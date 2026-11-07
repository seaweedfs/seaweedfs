package iceberg

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path"
	"strings"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3tables"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// stageTokenKey is the extended attribute stageOwnedMetadata tags a staged
// metadata file with: a value unique to the request that wrote it, which the
// request's own cleanup must find before it deletes the file. The internal
// prefix keeps it out of the headers S3 reads return.
const stageTokenKey = s3_constants.SeaweedFSInternalPrefix + "iceberg-stage-token"

// metadataDirPath returns the filer directory holding a table's metadata files.
func metadataDirPath(bucketName, tablePath string) string {
	dir := path.Join(s3tables.TablesPath, bucketName)
	if tablePath != "" {
		dir = path.Join(dir, tablePath)
	}
	return path.Join(dir, "metadata")
}

// saveMetadataFile saves the Iceberg metadata JSON file to the filer.
// It constructs the filer path from the S3 location components. With exclusive
// set the write fails with filer_pb.ErrEntryAlreadyExists rather than replacing
// a file that is already there: two commits racing off the same base pick the
// same v{N} name, and letting the second overwrite the first would leave the
// winner's catalog pointer aimed at the loser's metadata.
func (s *Server) saveMetadataFile(ctx context.Context, bucketName, tablePath, metadataFileName string, content []byte, exclusive bool) error {
	return s.saveMetadataBlob(ctx, bucketName, tablePath, metadataFileName, content, "application/json", exclusive, nil)
}

// saveMetadataBlob saves a file into the table's metadata directory, with
// extended stored on the entry alongside its mime type.
func (s *Server) saveMetadataBlob(ctx context.Context, bucketName, tablePath, metadataFileName string, content []byte, mimeType string, exclusive bool, extended map[string][]byte) error {
	attrs := map[string][]byte{"Mime-Type": []byte(mimeType)}
	for key, value := range extended {
		attrs[key] = value
	}

	// Create context with timeout for file operations
	opCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	return s.filerClient.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		bucketsPath := s3tables.TablesPath

		ensureDir := func(parent, name, errorContext string) error {
			_, err := filer_pb.LookupEntry(opCtx, client, &filer_pb.LookupDirectoryEntryRequest{
				Directory: parent,
				Name:      name,
			})
			if err == nil {
				return nil
			}
			if err != filer_pb.ErrNotFound {
				return fmt.Errorf("lookup %s failed: %w", errorContext, err)
			}

			// If lookup fails with ErrNotFound, try to create the directory.
			resp, createErr := client.CreateEntry(opCtx, &filer_pb.CreateEntryRequest{
				Directory: parent,
				Entry: &filer_pb.Entry{
					Name:        name,
					IsDirectory: true,
					Attributes: &filer_pb.FuseAttributes{
						Mtime:    time.Now().Unix(),
						Crtime:   time.Now().Unix(),
						FileMode: uint32(0755 | os.ModeDir),
					},
				},
			})
			if createErr != nil {
				return fmt.Errorf("failed to create %s: %w", errorContext, createErr)
			}
			if resp.ErrorCode != filer_pb.FilerError_OK && resp.ErrorCode != filer_pb.FilerError_ENTRY_ALREADY_EXISTS {
				return fmt.Errorf("failed to create %s: %s", errorContext, resp.Error)
			}
			return nil
		}

		bucketDir := path.Join(bucketsPath, bucketName)
		// 1. Ensure bucket directory exists: <bucketsPath>/<bucket>
		if err := ensureDir(bucketsPath, bucketName, "bucket directory"); err != nil {
			return err
		}

		// 2. Ensure table path exists under the bucket directory
		tableDir := bucketDir
		if tablePath != "" {
			segments := strings.Split(tablePath, "/")
			for _, segment := range segments {
				if segment == "" {
					continue
				}
				if err := ensureDir(tableDir, segment, "table directory"); err != nil {
					return err
				}
				tableDir = path.Join(tableDir, segment)
			}
		}

		// 3. Ensure metadata directory exists: <bucketsPath>/<bucket>/<tablePath>/metadata
		metadataDir := path.Join(tableDir, "metadata")
		if err := ensureDir(tableDir, "metadata", "metadata directory"); err != nil {
			return err
		}

		// 4. Write the file
		resp, err := client.CreateEntry(opCtx, &filer_pb.CreateEntryRequest{
			Directory: metadataDir,
			OExcl:     exclusive,
			Entry: &filer_pb.Entry{
				Name: metadataFileName,
				Attributes: &filer_pb.FuseAttributes{
					Mtime:    time.Now().Unix(),
					Crtime:   time.Now().Unix(),
					FileMode: uint32(0644),
					FileSize: uint64(len(content)),
				},
				Content:  content,
				Extended: attrs,
			},
		})
		if err != nil {
			return fmt.Errorf("failed to write metadata file: %w", err)
		}
		if resp.ErrorCode != filer_pb.FilerError_OK {
			if sentinel := filer_pb.FilerErrorToSentinel(resp.ErrorCode); sentinel != nil {
				return fmt.Errorf("failed to write metadata file: %w", sentinel)
			}
			return fmt.Errorf("failed to write metadata file: code=%v %s", resp.ErrorCode, resp.Error)
		}
		return nil
	})
}

func (s *Server) deleteMetadataFile(ctx context.Context, bucketName, tablePath, metadataFileName string) error {
	opCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	metadataDir := metadataDirPath(bucketName, tablePath)
	return s.filerClient.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		return filer_pb.DoRemove(opCtx, client, metadataDir, metadataFileName, true, false, true, false, nil)
	})
}

// errStagedFileReplaced reports that the entry at a staged file's path no
// longer carries the token it was staged with: it is another writer's file.
var errStagedFileReplaced = errors.New("staged metadata file was replaced")

// deleteStagedMetadataFile removes a file stageOwnedMetadata wrote, only while
// the entry at its path still carries token. The filer checks the token under
// the path lock the delete runs under, so a file that took the path since —
// the table was dropped and another create staged the same name — is refused
// with errStagedFileReplaced instead of deleted. A path that holds nothing is
// not an error.
func (s *Server) deleteStagedMetadataFile(ctx context.Context, bucketName, tablePath, metadataFileName, token string) error {
	if token == "" {
		return fmt.Errorf("%w: %s has no stage token", errStagedFileReplaced, metadataFileName)
	}
	opCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	metadataDir := metadataDirPath(bucketName, tablePath)
	return s.filerClient.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		_, err := filer_pb.DoRemoveRequest(opCtx, client, &filer_pb.DeleteEntryRequest{
			Directory:            metadataDir,
			Name:                 metadataFileName,
			IsDeleteData:         true,
			IgnoreRecursiveError: true,
			ExpectedExtended:     map[string][]byte{stageTokenKey: []byte(token)},
		})
		if status.Code(err) == codes.FailedPrecondition {
			return fmt.Errorf("%w: %s/%s: %v", errStagedFileReplaced, metadataDir, metadataFileName, err)
		}
		return err
	})
}

func (s *Server) loadMetadataFile(ctx context.Context, bucketName, tablePath, metadataFileName string) ([]byte, error) {
	opCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	metadataDir := metadataDirPath(bucketName, tablePath)

	var content []byte
	err := s.filerClient.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		resp, err := filer_pb.LookupEntry(opCtx, client, &filer_pb.LookupDirectoryEntryRequest{
			Directory: metadataDir,
			Name:      metadataFileName,
		})
		if err != nil {
			return err
		}
		if resp == nil || resp.Entry == nil {
			return fmt.Errorf("lookup returned nil entry for %s/%s", metadataDir, metadataFileName)
		}
		content = append([]byte(nil), resp.Entry.Content...)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return content, nil
}

// currentMetadataBytes returns the Iceberg metadata a table serves: the copy
// kept in its catalog record or, for a record that holds only a metadata
// location, the file at that location. Tables registered before the record
// kept their metadata look like that, and serving an empty placeholder for
// them hides every snapshot and fails every commit on a table UUID mismatch.
// It returns nil, for the caller's placeholder, when the record has neither
// or the file is gone.
func (s *Server) currentMetadataBytes(ctx context.Context, getResp s3tables.GetTableResponse, bucketName string) ([]byte, error) {
	if getResp.Metadata != nil && len(getResp.Metadata.FullMetadata) > 0 {
		return getResp.Metadata.FullMetadata, nil
	}
	if getResp.MetadataLocation == "" {
		return nil, nil
	}
	metadataBucket, tablePath, err := parseS3Location(tableLocationFromMetadataLocation(getResp.MetadataLocation))
	if err != nil {
		return nil, err
	}
	if err := confineMetadataLocation(metadataBucket, tablePath, bucketName); err != nil {
		return nil, err
	}
	content, err := s.loadTableMetadataBlob(ctx, metadataBucket, tablePath, path.Base(getResp.MetadataLocation))
	if errors.Is(err, filer_pb.ErrNotFound) {
		return nil, nil
	}
	return content, err
}
