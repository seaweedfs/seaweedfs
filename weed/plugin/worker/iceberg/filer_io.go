package iceberg

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"path"
	"strings"
	"time"

	"github.com/apache/iceberg-go/table"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3tables"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// filerFileEntry holds a non-directory entry with its full directory path.
type filerFileEntry struct {
	Dir   string
	Entry *filer_pb.Entry
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
func loadCurrentMetadata(ctx context.Context, client filer_pb.SeaweedFilerClient, bucketName, tablePath string) (table.Metadata, string, error) {
	dir := path.Join(s3tables.TablesPath, bucketName, path.Dir(tablePath))
	name := path.Base(tablePath)

	resp, err := filer_pb.LookupEntry(ctx, client, &filer_pb.LookupDirectoryEntryRequest{
		Directory: dir,
		Name:      name,
	})
	if err != nil {
		return nil, "", fmt.Errorf("lookup table entry %s/%s: %w", dir, name, err)
	}
	if resp == nil || resp.Entry == nil {
		return nil, "", fmt.Errorf("table entry not found: %s/%s", dir, name)
	}

	metadataBytes, ok := resp.Entry.Extended[s3tables.ExtendedKeyMetadata]
	if !ok || len(metadataBytes) == 0 {
		return nil, "", fmt.Errorf("no metadata xattr on table entry %s/%s", dir, name)
	}

	// Parse internal metadata to extract FullMetadata
	var internalMeta struct {
		MetadataVersion  int    `json:"metadataVersion"`
		MetadataLocation string `json:"metadataLocation,omitempty"`
		Metadata         *struct {
			FullMetadata json.RawMessage `json:"fullMetadata,omitempty"`
		} `json:"metadata,omitempty"`
	}
	if err := json.Unmarshal(metadataBytes, &internalMeta); err != nil {
		return nil, "", fmt.Errorf("unmarshal internal metadata: %w", err)
	}
	if internalMeta.Metadata == nil || len(internalMeta.Metadata.FullMetadata) == 0 {
		return nil, "", fmt.Errorf("no fullMetadata in table xattr")
	}

	meta, err := table.ParseMetadataBytes(internalMeta.Metadata.FullMetadata)
	if err != nil {
		return nil, "", fmt.Errorf("parse iceberg metadata: %w", err)
	}

	// Use metadataLocation from xattr if available (includes nonce suffix),
	// otherwise fall back to the canonical name derived from metadataVersion.
	metadataFileName := path.Base(internalMeta.MetadataLocation)
	if metadataFileName == "" || metadataFileName == "." {
		metadataFileName = fmt.Sprintf("v%d.metadata.json", internalMeta.MetadataVersion)
	}
	return meta, metadataFileName, nil
}

// loadFileByIcebergPath loads a file from the filer given an Iceberg-style path.
// Paths may be absolute filer paths, relative (metadata/..., data/...), or
// location-based (s3://bucket/ns/table/metadata/...).
//
// The function normalises the path to a relative form under the table root
// (e.g. "metadata/snap-1.avro" or "data/region=us/file.parquet") and splits
// it into the correct filer directory + entry name, so nested sub-directories
// are resolved properly.
func loadFileByIcebergPath(ctx context.Context, client filer_pb.SeaweedFilerClient, bucketName, tablePath, icebergPath string) ([]byte, error) {
	relPath := path.Clean(normalizeIcebergPath(icebergPath, bucketName, tablePath))
	relPath = strings.TrimPrefix(relPath, "/")
	if relPath == "." || relPath == "" || strings.HasPrefix(relPath, "../") {
		return nil, fmt.Errorf("invalid iceberg path %q", icebergPath)
	}

	dir := path.Join(s3tables.TablesPath, bucketName, tablePath, path.Dir(relPath))
	fileName := path.Base(relPath)

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

	return resp.Entry.Content, nil
}

// normalizeIcebergPath converts an Iceberg path (which may be an S3 URL, an
// absolute filer path, or a plain relative path) into a relative path under the
// table root, e.g. "metadata/snap-1.avro" or "data/region=us/file.parquet".
func normalizeIcebergPath(icebergPath, bucketName, tablePath string) string {
	p := icebergPath

	// Strip scheme (e.g. "s3://bucket/ns/table/metadata/file" → "bucket/ns/table/metadata/file")
	if idx := strings.Index(p, "://"); idx >= 0 {
		p = p[idx+3:]
	}

	// Strip any leading slash
	p = strings.TrimPrefix(p, "/")

	// Strip bucket+tablePath prefix if present
	// e.g. "mybucket/ns/table/metadata/file" → "metadata/file"
	tablePrefix := path.Join(bucketName, tablePath) + "/"
	if strings.HasPrefix(p, tablePrefix) {
		return p[len(tablePrefix):]
	}

	// Strip filer TablesPath prefix if present
	// e.g. "buckets/mybucket/ns/table/metadata/file" → "metadata/file"
	filerPrefix := strings.TrimPrefix(s3tables.TablesPath, "/")
	fullPrefix := path.Join(filerPrefix, bucketName, tablePath) + "/"
	if strings.HasPrefix(p, fullPrefix) {
		return p[len(fullPrefix):]
	}

	// Already relative (e.g. "metadata/snap-1.avro")
	return p
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
// the new Iceberg metadata. It performs a compare-and-swap: if the stored
// metadataVersion does not match expectedVersion, it returns
// errMetadataVersionConflict so the caller can retry.
// newMetadataLocation is the table-relative path to the new metadata file
// (e.g. "metadata/v3.metadata.json").
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

	// Compare-and-swap: verify the stored metadataVersion matches what we expect.
	// NOTE: This is a client-side CAS — two workers could both read the same
	// version, pass this check, and race at UpdateEntry (last-write-wins).
	// The proper fix is server-side precondition support on UpdateEntryRequest
	// (e.g. expect-version or If-Match semantics). Until then, commitWithRetry
	// with exponential backoff mitigates but does not eliminate the race.
	// Avoid scheduling concurrent maintenance on the same table.
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

	resp.Entry.Extended[s3tables.ExtendedKeyMetadata] = updatedXattr
	_, err = client.UpdateEntry(ctx, &filer_pb.UpdateEntryRequest{
		Directory: parentDir,
		Entry:     resp.Entry,
	})
	if err != nil {
		return fmt.Errorf("update table entry: %w", err)
	}
	return nil
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
