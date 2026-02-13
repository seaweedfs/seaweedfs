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

// saveMetadataFile saves the Iceberg metadata JSON file to the filer.
// It constructs the filer path from the S3 location components.
func (s *Server) saveMetadataFile(ctx context.Context, bucketName, tablePath, metadataFileName string, content []byte) error {

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
			if resp.Error != "" && !strings.Contains(resp.Error, "exist") {
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
					"Mime-Type": []byte("application/json"),
				},
			},
		})
		if err != nil {
			return fmt.Errorf("failed to write metadata file: %w", err)
		}
		if resp.Error != "" {
			return fmt.Errorf("failed to write metadata file: %s", resp.Error)
		}
		return nil
	})
}

func (s *Server) deleteMetadataFile(ctx context.Context, bucketName, tablePath, metadataFileName string) error {
	opCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	metadataDir := path.Join(s3tables.TablesPath, bucketName)
	if tablePath != "" {
		metadataDir = path.Join(metadataDir, tablePath)
	}
	metadataDir = path.Join(metadataDir, "metadata")
	return s.filerClient.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		return filer_pb.DoRemove(opCtx, client, metadataDir, metadataFileName, true, false, true, false, nil)
	})
}

func (s *Server) loadMetadataFile(ctx context.Context, bucketName, tablePath, metadataFileName string) ([]byte, error) {
	opCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	metadataDir := path.Join(s3tables.TablesPath, bucketName)
	if tablePath != "" {
		metadataDir = path.Join(metadataDir, tablePath)
	}
	metadataDir = path.Join(metadataDir, "metadata")

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
