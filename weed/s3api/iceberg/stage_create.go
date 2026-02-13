package iceberg

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"path"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3tables"
)

const (
	stageCreateMarkerDirName = ".iceberg_staged"
	// Keep staged markers long enough to avoid deleting in-progress create transactions.
	stageCreateMarkerTTL = 72 * time.Hour
)

type stageCreateMarker struct {
	TableUUID              string `json:"table_uuid"`
	Location               string `json:"location"`
	StagedMetadataLocation string `json:"staged_metadata_location,omitempty"`
	CreatedAt              string `json:"created_at"`
	ExpiresAt              string `json:"expires_at"`
}

func stageCreateMarkerNamespaceKey(namespace []string) string {
	return url.PathEscape(encodeNamespace(namespace))
}

func stageCreateMarkerDir(bucketName string, namespace []string, tableName string) string {
	return path.Join(s3tables.TablesPath, bucketName, stageCreateMarkerDirName, stageCreateMarkerNamespaceKey(namespace), tableName)
}

func stageCreateStagedTablePath(namespace []string, tableName string, tableUUID uuid.UUID) string {
	return path.Join(stageCreateMarkerDirName, stageCreateMarkerNamespaceKey(namespace), tableName, tableUUID.String())
}

func (s *Server) pruneExpiredStageCreateMarkers(ctx context.Context, bucketName string, namespace []string, tableName string) error {
	opCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	markerDir := stageCreateMarkerDir(bucketName, namespace, tableName)
	now := time.Now().UTC()

	return s.filerClient.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		stream, err := client.ListEntries(opCtx, &filer_pb.ListEntriesRequest{
			Directory: markerDir,
			Limit:     1024,
		})
		if err != nil {
			if errors.Is(err, filer_pb.ErrNotFound) || strings.Contains(strings.ToLower(err.Error()), "not found") {
				return nil
			}
			return err
		}

		for {
			resp, recvErr := stream.Recv()
			if recvErr == io.EOF {
				return nil
			}
			if recvErr != nil {
				return recvErr
			}
			if resp.Entry == nil || resp.Entry.IsDirectory || len(resp.Entry.Content) == 0 {
				continue
			}

			var marker stageCreateMarker
			if err := json.Unmarshal(resp.Entry.Content, &marker); err != nil {
				continue
			}
			expiresAt, err := time.Parse(time.RFC3339Nano, marker.ExpiresAt)
			if err != nil || !expiresAt.Before(now) {
				continue
			}

			if err := filer_pb.DoRemove(opCtx, client, markerDir, resp.Entry.Name, true, false, true, false, nil); err != nil && !errors.Is(err, filer_pb.ErrNotFound) {
				return err
			}
		}
	})
}

func (s *Server) loadLatestStageCreateMarker(ctx context.Context, bucketName string, namespace []string, tableName string) (*stageCreateMarker, error) {
	opCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	markerDir := stageCreateMarkerDir(bucketName, namespace, tableName)
	now := time.Now().UTC()

	var latestMarker *stageCreateMarker
	var latestCreatedAt time.Time

	err := s.filerClient.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		stream, err := client.ListEntries(opCtx, &filer_pb.ListEntriesRequest{
			Directory: markerDir,
			Limit:     1024,
		})
		if err != nil {
			if errors.Is(err, filer_pb.ErrNotFound) || strings.Contains(strings.ToLower(err.Error()), "not found") {
				return nil
			}
			return err
		}

		for {
			resp, recvErr := stream.Recv()
			if recvErr == io.EOF {
				return nil
			}
			if recvErr != nil {
				return recvErr
			}
			if resp.Entry == nil || resp.Entry.IsDirectory || len(resp.Entry.Content) == 0 {
				continue
			}

			var marker stageCreateMarker
			if err := json.Unmarshal(resp.Entry.Content, &marker); err != nil {
				continue
			}
			expiresAt, err := time.Parse(time.RFC3339Nano, marker.ExpiresAt)
			if err != nil || !expiresAt.After(now) {
				continue
			}

			createdAt, err := time.Parse(time.RFC3339Nano, marker.CreatedAt)
			if err != nil {
				createdAt = time.Time{}
			}
			if latestMarker == nil || createdAt.After(latestCreatedAt) {
				candidate := marker
				latestMarker = &candidate
				latestCreatedAt = createdAt
			}
		}
	})
	if err != nil {
		return nil, err
	}
	return latestMarker, nil
}

func (s *Server) writeStageCreateMarker(ctx context.Context, bucketName string, namespace []string, tableName string, tableUUID uuid.UUID, location string, stagedMetadataLocation string) error {
	opCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	marker := stageCreateMarker{
		TableUUID:              tableUUID.String(),
		Location:               location,
		StagedMetadataLocation: stagedMetadataLocation,
		CreatedAt:              time.Now().UTC().Format(time.RFC3339Nano),
		ExpiresAt:              time.Now().UTC().Add(stageCreateMarkerTTL).Format(time.RFC3339Nano),
	}
	content, err := json.Marshal(marker)
	if err != nil {
		return err
	}

	if err := s.pruneExpiredStageCreateMarkers(ctx, bucketName, namespace, tableName); err != nil {
		return err
	}

	return s.filerClient.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		ensureDir := func(parent, name, errorContext string) error {
			_, lookupErr := filer_pb.LookupEntry(opCtx, client, &filer_pb.LookupDirectoryEntryRequest{
				Directory: parent,
				Name:      name,
			})
			if lookupErr == nil {
				return nil
			}
			if lookupErr != filer_pb.ErrNotFound {
				return fmt.Errorf("lookup %s failed: %w", errorContext, lookupErr)
			}

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

		segments := []string{bucketName, stageCreateMarkerDirName, stageCreateMarkerNamespaceKey(namespace), tableName}
		currentDir := s3tables.TablesPath
		for _, segment := range segments {
			if segment == "" {
				continue
			}
			if err := ensureDir(currentDir, segment, "stage-create marker directory"); err != nil {
				return err
			}
			currentDir = path.Join(currentDir, segment)
		}

		entryName := tableUUID.String() + ".json"
		resp, createErr := client.CreateEntry(opCtx, &filer_pb.CreateEntryRequest{
			Directory: currentDir,
			Entry: &filer_pb.Entry{
				Name: entryName,
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
		if createErr != nil {
			return createErr
		}
		if resp.Error != "" {
			return errors.New(resp.Error)
		}
		return nil
	})
}

func (s *Server) deleteStageCreateMarkers(ctx context.Context, bucketName string, namespace []string, tableName string) error {
	opCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	parentDir := path.Dir(stageCreateMarkerDir(bucketName, namespace, tableName))
	targetName := path.Base(stageCreateMarkerDir(bucketName, namespace, tableName))

	return s.filerClient.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		err := filer_pb.DoRemove(opCtx, client, parentDir, targetName, true, true, true, false, nil)
		if errors.Is(err, filer_pb.ErrNotFound) {
			return nil
		}
		return err
	})
}

func isStageCreateEnabled() bool {
	value := strings.TrimSpace(strings.ToLower(os.Getenv("ICEBERG_ENABLE_STAGE_CREATE")))
	if value == "" {
		return true
	}
	switch value {
	case "0", "false", "no", "off":
		return false
	default:
		return true
	}
}
