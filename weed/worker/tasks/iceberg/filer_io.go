package iceberg

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"path"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3tables"
	util_http "github.com/seaweedfs/seaweedfs/weed/util/http"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// filerFileEntry holds a non-directory entry with its full directory path.
type filerFileEntry struct {
	Dir   string
	Entry *filer_pb.Entry
}

var initGlobalHTTPClientOnce sync.Once

type singleFilerClient struct {
	client filer_pb.SeaweedFilerClient
}

func (c singleFilerClient) WithFilerClient(_ bool, fn func(filer_pb.SeaweedFilerClient) error) error {
	return fn(c.client)
}

func (c singleFilerClient) AdjustedUrl(location *filer_pb.Location) string {
	if location == nil {
		return ""
	}
	if location.PublicUrl != "" {
		return location.PublicUrl
	}
	return location.Url
}

func (c singleFilerClient) GetDataCenter() string {
	return ""
}

// listFilerEntries lists all entries in a directory.
func listFilerEntries(ctx context.Context, client filer_pb.SeaweedFilerClient, dir, prefix string) ([]*filer_pb.Entry, error) {
	var entries []*filer_pb.Entry
	var lastFileName string
	limit := uint32(10000)

	for {
		resp, err := client.ListEntries(ctx, &filer_pb.ListEntriesRequest{
			Directory:          dir,
			Prefix:             prefix,
			StartFromFileName:  lastFileName,
			InclusiveStartFrom: lastFileName == "",
			Limit:              limit,
		})
		if err != nil {
			// Treat not-found as empty directory; propagate other errors.
			if status.Code(err) == codes.NotFound {
				return entries, nil
			}
			return entries, fmt.Errorf("list entries in %s: %w", dir, err)
		}

		count := 0
		for {
			entry, recvErr := resp.Recv()
			if recvErr != nil {
				if recvErr == io.EOF {
					break
				}
				return entries, fmt.Errorf("recv entry in %s: %w", dir, recvErr)
			}
			if entry.Entry != nil {
				entries = append(entries, entry.Entry)
				lastFileName = entry.Entry.Name
				count++
			}
		}

		if count < int(limit) {
			break
		}
	}

	return entries, nil
}

// walkFilerEntries recursively lists all non-directory entries under dir.
func walkFilerEntries(ctx context.Context, client filer_pb.SeaweedFilerClient, dir string) ([]filerFileEntry, error) {
	entries, err := listFilerEntries(ctx, client, dir, "")
	if err != nil {
		return nil, err
	}

	var result []filerFileEntry
	for _, entry := range entries {
		if entry.IsDirectory {
			subDir := path.Join(dir, entry.Name)
			subEntries, err := walkFilerEntries(ctx, client, subDir)
			if err != nil {
				glog.V(2).Infof("iceberg maintenance: cannot walk %s: %v", subDir, err)
				continue
			}
			result = append(result, subEntries...)
		} else {
			result = append(result, filerFileEntry{Dir: dir, Entry: entry})
		}
	}
	return result, nil
}

// loadCurrentMetadata loads and parses the current Iceberg metadata from the table entry's xattr.
func loadCurrentMetadata(ctx context.Context, client filer_pb.SeaweedFilerClient, bucketName, tablePath string) (*tableState, error) {
	dir := path.Join(s3tables.TablesPath, bucketName, path.Dir(tablePath))
	name := path.Base(tablePath)

	resp, err := filer_pb.LookupEntry(ctx, client, &filer_pb.LookupDirectoryEntryRequest{
		Directory: dir,
		Name:      name,
	})
	if err != nil {
		return nil, fmt.Errorf("lookup table entry %s/%s: %w", dir, name, err)
	}
	if resp == nil || resp.Entry == nil {
		return nil, fmt.Errorf("table entry not found: %s/%s", dir, name)
	}

	metadataBytes, ok := resp.Entry.Extended[s3tables.ExtendedKeyMetadata]
	if !ok || len(metadataBytes) == 0 {
		return nil, fmt.Errorf("no metadata xattr on table entry %s/%s", dir, name)
	}

	return parseTableMetadataEnvelope(metadataBytes, bucketName, tablePath)
}

// loadFileByIcebergPath loads a file from the filer given an Iceberg-style path.
// dataPath is the bucket-relative directory the table's files live in.
func loadFileByIcebergPath(ctx context.Context, client filer_pb.SeaweedFilerClient, bucketName, dataPath, icebergPath string) ([]byte, error) {
	fullPath, err := icebergFilerPath(bucketName, dataPath, icebergPath)
	if err != nil {
		return nil, err
	}
	dir, fileName := path.Dir(fullPath), path.Base(fullPath)

	resp, err := filer_pb.LookupEntry(ctx, client, &filer_pb.LookupDirectoryEntryRequest{
		Directory: dir,
		Name:      fileName,
	})
	if err != nil {
		return nil, fmt.Errorf("lookup %s/%s: %w", dir, fileName, err)
	}
	if resp == nil || resp.Entry == nil {
		return nil, fmt.Errorf("file not found: %s/%s", dir, fileName)
	}

	if len(resp.Entry.Content) > 0 || len(resp.Entry.Chunks) == 0 {
		return resp.Entry.Content, nil
	}

	initGlobalHTTPClientOnce.Do(util_http.InitGlobalHttpClient)
	reader := filer.NewFileReader(singleFilerClient{client: client}, resp.Entry)
	if closer, ok := reader.(io.Closer); ok {
		defer closer.Close()
	}
	data, err := io.ReadAll(reader)
	if err != nil {
		return nil, fmt.Errorf("read chunked file %s/%s: %w", dir, fileName, err)
	}
	return data, nil
}

// normalizeIcebergPath converts an Iceberg path (an S3 URL, an absolute filer
// path, or a path relative to the table root) into a path relative to the
// bucket root, e.g. "ns/table/metadata/snap-1.avro". Absolute references are
// resolved from the bucket root rather than from dataPath: an Iceberg table
// records where its files actually are, and a client writing through the REST
// catalog may place them outside the catalog path. The bucket-relative form is
// the canonical key for comparing references that mix both spellings.
func normalizeIcebergPath(icebergPath, bucketName, dataPath string) (string, error) {
	p := icebergPath
	absolute := false
	if idx := strings.Index(p, "://"); idx >= 0 {
		p = p[idx+3:]
		absolute = true
	} else if strings.HasPrefix(p, "/") {
		absolute = true
	}
	p = strings.TrimPrefix(p, "/")

	if absolute {
		rest, ok := cutPathPrefix(p, bucketName)
		if !ok {
			// Filer-style references carry the /buckets prefix ahead of the bucket.
			if withoutTablesPath, hasTablesPath := cutPathPrefix(p, strings.TrimPrefix(s3tables.TablesPath, "/")); hasTablesPath {
				rest, ok = cutPathPrefix(withoutTablesPath, bucketName)
			}
		}
		if !ok {
			return "", fmt.Errorf("iceberg path %q is outside bucket %q", icebergPath, bucketName)
		}
		p = rest
	} else {
		if clean := path.Clean(p); clean == ".." || strings.HasPrefix(clean, "../") {
			return "", fmt.Errorf("invalid iceberg path %q", icebergPath)
		}
		p = path.Join(dataPath, p)
	}

	p = path.Clean(p)
	if p == "." || p == ".." || strings.HasPrefix(p, "../") {
		return "", fmt.Errorf("invalid iceberg path %q", icebergPath)
	}
	return p, nil
}

// cutPathPrefix removes a leading path prefix at a path boundary.
func cutPathPrefix(p, segment string) (string, bool) {
	if segment == "" {
		return p, false
	}
	rest, ok := strings.CutPrefix(p, segment+"/")
	return rest, ok
}

// icebergFilerPath resolves an Iceberg path to the filer path holding the file.
func icebergFilerPath(bucketName, dataPath, icebergPath string) (string, error) {
	relPath, err := normalizeIcebergPath(icebergPath, bucketName, dataPath)
	if err != nil {
		return "", err
	}
	return path.Join(s3tables.TablesPath, bucketName, relPath), nil
}

// absoluteIcebergPath is the inverse of normalizeIcebergPath: it builds the
// absolute s3:// URI for a bucket-relative path. The Iceberg spec requires
// absolute locations in metadata — strict readers (Spark/Trino via S3FileIO)
// reject paths with no scheme.
func absoluteIcebergPath(bucketName string, elem ...string) string {
	return "s3://" + path.Join(append([]string{bucketName}, elem...)...)
}

// saveFilerFile saves a file to the filer.
func saveFilerFile(ctx context.Context, client filer_pb.SeaweedFilerClient, dir, fileName string, content []byte) error {
	resp, err := client.CreateEntry(ctx, &filer_pb.CreateEntryRequest{
		Directory: dir,
		Entry: &filer_pb.Entry{
			Name: fileName,
			Attributes: &filer_pb.FuseAttributes{
				Mtime:    time.Now().Unix(),
				Crtime:   time.Now().Unix(),
				FileMode: uint32(0644),
				FileSize: uint64(len(content)),
			},
			Content: content,
		},
	})
	if err != nil {
		return fmt.Errorf("create entry %s/%s: %w", dir, fileName, err)
	}
	if resp.Error != "" {
		return fmt.Errorf("create entry %s/%s: %s", dir, fileName, resp.Error)
	}
	return nil
}

// deleteFilerFile deletes a file from the filer.
func deleteFilerFile(ctx context.Context, client filer_pb.SeaweedFilerClient, dir, fileName string) error {
	return filer_pb.DoRemove(ctx, client, dir, fileName, true, false, true, false, nil)
}

// updateTableMetadataXattr updates the table entry's metadata xattr with
// the new Iceberg metadata. It verifies the stored metadataVersion before
// writing and passes the previous metadata xattr back to the filer as a
// server-side precondition so concurrent writers fail with a retryable
// metadata version conflict.
// newMetadataLocation is the absolute location of the new metadata file
// (e.g. "s3://bucket/ns/table/metadata/v3.metadata.json"), matching the form
// the S3 Tables catalog stores.
func updateTableMetadataXattr(ctx context.Context, client filer_pb.SeaweedFilerClient, tableDir string, expectedVersion int, newFullMetadata []byte, newMetadataLocation string) error {
	tableName := path.Base(tableDir)
	parentDir := path.Dir(tableDir)

	resp, err := filer_pb.LookupEntry(ctx, client, &filer_pb.LookupDirectoryEntryRequest{
		Directory: parentDir,
		Name:      tableName,
	})
	if err != nil {
		return fmt.Errorf("lookup table entry: %w", err)
	}
	if resp == nil || resp.Entry == nil {
		return fmt.Errorf("table entry not found")
	}

	existingXattr, ok := resp.Entry.Extended[s3tables.ExtendedKeyMetadata]
	if !ok {
		return fmt.Errorf("no metadata xattr on table entry")
	}

	// Parse existing xattr, update fullMetadata
	var internalMeta map[string]json.RawMessage
	if err := json.Unmarshal(existingXattr, &internalMeta); err != nil {
		return fmt.Errorf("unmarshal existing xattr: %w", err)
	}

	// Verify the stored metadataVersion matches what we expect before issuing
	// the conditional UpdateEntry request below.
	versionRaw, ok := internalMeta["metadataVersion"]
	if !ok {
		return fmt.Errorf("%w: metadataVersion field missing from xattr", errMetadataVersionConflict)
	}
	var storedVersion int
	if err := json.Unmarshal(versionRaw, &storedVersion); err != nil {
		return fmt.Errorf("%w: cannot parse metadataVersion: %v", errMetadataVersionConflict, err)
	}
	if storedVersion != expectedVersion {
		return fmt.Errorf("%w: expected version %d, found %d", errMetadataVersionConflict, expectedVersion, storedVersion)
	}

	// Update the metadata.fullMetadata field
	var metadataObj map[string]json.RawMessage
	if raw, ok := internalMeta["metadata"]; ok {
		if err := json.Unmarshal(raw, &metadataObj); err != nil {
			return fmt.Errorf("unmarshal metadata object: %w", err)
		}
	} else {
		metadataObj = make(map[string]json.RawMessage)
	}
	metadataObj["fullMetadata"] = newFullMetadata
	metadataJSON, err := json.Marshal(metadataObj)
	if err != nil {
		return fmt.Errorf("marshal metadata object: %w", err)
	}
	internalMeta["metadata"] = metadataJSON

	// Increment version
	newVersion := expectedVersion + 1
	versionJSON, _ := json.Marshal(newVersion)
	internalMeta["metadataVersion"] = versionJSON

	// Update modifiedAt
	modifiedAt, _ := json.Marshal(time.Now().Format(time.RFC3339Nano))
	internalMeta["modifiedAt"] = modifiedAt

	// Update metadataLocation to point to the new metadata file
	metaLocJSON, _ := json.Marshal(newMetadataLocation)
	internalMeta["metadataLocation"] = metaLocJSON

	// Regenerate versionToken for consistency with the S3 Tables catalog
	tokenJSON, _ := json.Marshal(generateIcebergVersionToken())
	internalMeta["versionToken"] = tokenJSON

	updatedXattr, err := json.Marshal(internalMeta)
	if err != nil {
		return fmt.Errorf("marshal updated xattr: %w", err)
	}

	expectedVersionXattr := resp.Entry.Extended[s3tables.ExtendedKeyMetadataVersion]
	resp.Entry.Extended[s3tables.ExtendedKeyMetadata] = updatedXattr
	resp.Entry.Extended[s3tables.ExtendedKeyMetadataVersion] = metadataVersionXattr(newVersion)
	_, err = client.UpdateEntry(ctx, &filer_pb.UpdateEntryRequest{
		Directory: parentDir,
		Entry:     resp.Entry,
		ExpectedExtended: map[string][]byte{
			s3tables.ExtendedKeyMetadataVersion: expectedVersionXattr,
		},
	})
	if err != nil {
		if status.Code(err) == codes.FailedPrecondition {
			return fmt.Errorf("%w: table metadata changed during update", errMetadataVersionConflict)
		}
		return fmt.Errorf("update table entry: %w", err)
	}
	return nil
}

func metadataVersionXattr(version int) []byte {
	return []byte(strconv.Itoa(version))
}

// generateIcebergVersionToken produces a random hex token, mirroring the
// logic in s3tables.generateVersionToken (which is unexported).
func generateIcebergVersionToken() string {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		return fmt.Sprintf("%x", time.Now().UnixNano())
	}
	return hex.EncodeToString(b)
}
