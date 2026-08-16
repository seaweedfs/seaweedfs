package iceberg

import (
	"context"
	"fmt"
	"os"
	"path"
	"strings"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3tables"
)

// metadataDirPath returns the filer directory holding a table's metadata files.
func metadataDirPath(bucketName, tablePath string) string {
	dir := path.Join(s3tables.TablesPath, bucketName)
	if tablePath != "" {
		dir = path.Join(dir, tablePath)
	}
	return path.Join(dir, "metadata")
}

// saveMetadataFile saves the Iceberg metadata JSON file to the filer.
// It constructs the filer path from the S3 location components.
func (s *Server) saveMetadataFile(ctx context.Context, bucketName, tablePath, metadataFileName string, content []byte) error {
	return s.saveMetadataBlob(ctx, bucketName, tablePath, metadataFileName, content, "application/json")
}

// saveNewMetadataFile writes the metadata file of a new table version and
// fails with filer_pb.ErrEntryAlreadyExists if that version is already on
// disk. Two commits racing off the same base pick the same v{N} file name, and
// letting the second overwrite the first would leave the winner's catalog
// pointer aimed at the loser's metadata.
func (s *Server) saveNewMetadataFile(ctx context.Context, bucketName, tablePath, metadataFileName string, content []byte) error {
	return s.saveMetadataBlobExclusive(ctx, bucketName, tablePath, metadataFileName, content, "application/json", true)
}

// saveMetadataBlob saves a file into the table's metadata directory.
func (s *Server) saveMetadataBlob(ctx context.Context, bucketName, tablePath, metadataFileName string, content []byte, mimeType string) error {
	return s.saveMetadataBlobExclusive(ctx, bucketName, tablePath, metadataFileName, content, mimeType, false)
}

func (s *Server) saveMetadataBlobExclusive(ctx context.Context, bucketName, tablePath, metadataFileName string, content []byte, mimeType string, exclusive bool) error {

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
				Content: content,
				Extended: map[string][]byte{
					"Mime-Type": []byte(mimeType),
				},
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
