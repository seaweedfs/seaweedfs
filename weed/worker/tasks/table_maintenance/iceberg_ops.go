package table_maintenance

import (
	"context"
	"encoding/json"
	"fmt"
	"path"
	"sort"
	"strings"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
)

// IcebergTableMetadata represents the Iceberg table metadata structure
type IcebergTableMetadata struct {
	FormatVersion int               `json:"format-version"`
	TableUUID     string            `json:"table-uuid"`
	Location      string            `json:"location"`
	Snapshots     []IcebergSnapshot `json:"snapshots,omitempty"`
	CurrentSnap   int64             `json:"current-snapshot-id"`
	Refs          map[string]SnapRef `json:"refs,omitempty"`
}

// IcebergSnapshot represents an Iceberg table snapshot
type IcebergSnapshot struct {
	SnapshotID       int64             `json:"snapshot-id"`
	TimestampMs      int64             `json:"timestamp-ms"`
	ManifestList     string            `json:"manifest-list"`
	Summary          map[string]string `json:"summary,omitempty"`
	ParentSnapshotID *int64            `json:"parent-snapshot-id,omitempty"`
}

// SnapRef represents a snapshot reference (branch or tag)
type SnapRef struct {
	SnapshotID int64  `json:"snapshot-id"`
	Type       string `json:"type"`
}

// IcebergManifestList represents an Iceberg manifest list
type IcebergManifestList struct {
	Manifests []IcebergManifestEntry `json:"manifests"`
}

// IcebergManifestEntry represents an entry in a manifest list
type IcebergManifestEntry struct {
	ManifestPath    string `json:"manifest_path"`
	ManifestLength  int64  `json:"manifest_length"`
	PartitionSpecID int    `json:"partition_spec_id"`
	AddedFilesCount int    `json:"added_files_count"`
	ExistingFiles   int    `json:"existing_files_count"`
	DeletedFiles    int    `json:"deleted_files_count"`
}

// IcebergManifest represents an Iceberg manifest file containing data file entries
type IcebergManifest struct {
	Entries []IcebergDataFileEntry `json:"entries"`
}

// IcebergDataFileEntry represents a data file entry in a manifest
type IcebergDataFileEntry struct {
	Status     int    `json:"status"` // 0=existing, 1=added, 2=deleted
	DataFile   DataFile `json:"data_file"`
}

// DataFile represents an Iceberg data file
type DataFile struct {
	FilePath        string `json:"file_path"`
	FileFormat      string `json:"file_format"`
	RecordCount     int64  `json:"record_count"`
	FileSizeInBytes int64  `json:"file_size_in_bytes"`
}

// TableMaintenanceContext provides context for table maintenance operations
type TableMaintenanceContext struct {
	FilerClient filer_pb.SeaweedFilerClient
	TablePath   string
	MetadataDir string
	DataDir     string
	Config      *Config
}

// NewTableMaintenanceContext creates a new maintenance context
func NewTableMaintenanceContext(client filer_pb.SeaweedFilerClient, tablePath string, config *Config) *TableMaintenanceContext {
	return &TableMaintenanceContext{
		FilerClient: client,
		TablePath:   tablePath,
		MetadataDir: path.Join(tablePath, "metadata"),
		DataDir:     path.Join(tablePath, "data"),
		Config:      config,
	}
}

// ReadTableMetadata reads the current Iceberg table metadata
func (mc *TableMaintenanceContext) ReadTableMetadata(ctx context.Context) (*IcebergTableMetadata, string, error) {
	// Find the latest metadata file (v*.metadata.json)
	metadataFiles, err := mc.listMetadataFiles(ctx)
	if err != nil {
		return nil, "", fmt.Errorf("failed to list metadata files: %w", err)
	}

	if len(metadataFiles) == 0 {
		return nil, "", fmt.Errorf("no metadata files found in %s", mc.MetadataDir)
	}

	// Sort to get the latest version
	sort.Strings(metadataFiles)
	latestMetadataFile := metadataFiles[len(metadataFiles)-1]
	metadataPath := path.Join(mc.MetadataDir, latestMetadataFile)

	// Read the metadata file content
	content, err := mc.readFileContent(ctx, metadataPath)
	if err != nil {
		return nil, "", fmt.Errorf("failed to read metadata file %s: %w", metadataPath, err)
	}

	var metadata IcebergTableMetadata
	if err := json.Unmarshal(content, &metadata); err != nil {
		return nil, "", fmt.Errorf("failed to parse metadata: %w", err)
	}

	return &metadata, metadataPath, nil
}

// listMetadataFiles lists all metadata files in the metadata directory
func (mc *TableMaintenanceContext) listMetadataFiles(ctx context.Context) ([]string, error) {
	return mc.listFilesWithPattern(ctx, mc.MetadataDir, "v", ".metadata.json")
}

// listFilesWithPattern lists files matching a prefix and suffix pattern
func (mc *TableMaintenanceContext) listFilesWithPattern(ctx context.Context, dir, prefix, suffix string) ([]string, error) {
	var files []string

	resp, err := mc.FilerClient.ListEntries(ctx, &filer_pb.ListEntriesRequest{
		Directory: dir,
		Limit:     1000,
	})
	if err != nil {
		return nil, err
	}

	for {
		entry, err := resp.Recv()
		if err != nil {
			break
		}
		name := entry.Entry.Name
		if strings.HasPrefix(name, prefix) && strings.HasSuffix(name, suffix) {
			files = append(files, name)
		}
	}

	return files, nil
}

// listAllFiles lists all files in a directory recursively
func (mc *TableMaintenanceContext) listAllFiles(ctx context.Context, dir string) ([]string, error) {
	var files []string

	resp, err := mc.FilerClient.ListEntries(ctx, &filer_pb.ListEntriesRequest{
		Directory: dir,
		Limit:     10000,
	})
	if err != nil {
		return nil, err
	}

	for {
		entry, err := resp.Recv()
		if err != nil {
			break
		}
		if entry.Entry.IsDirectory {
			// Recurse into subdirectory
			subFiles, err := mc.listAllFiles(ctx, path.Join(dir, entry.Entry.Name))
			if err != nil {
				glog.V(2).Infof("Failed to list subdirectory %s: %v", entry.Entry.Name, err)
				continue
			}
			files = append(files, subFiles...)
		} else {
			files = append(files, path.Join(dir, entry.Entry.Name))
		}
	}

	return files, nil
}

// readFileContent reads the content of a file
func (mc *TableMaintenanceContext) readFileContent(ctx context.Context, filePath string) ([]byte, error) {
	dir, name := splitPath(filePath)
	resp, err := filer_pb.LookupEntry(ctx, mc.FilerClient, &filer_pb.LookupDirectoryEntryRequest{
		Directory: dir,
		Name:      name,
	})
	if err != nil {
		return nil, err
	}

	// For small metadata files, the content may be inline
	// For larger files, we need to read chunks
	if len(resp.Entry.Content) > 0 {
		return resp.Entry.Content, nil
	}

	// For files with chunks, we need to read from volume servers
	// This is a simplified implementation - in production, use the full chunk reading logic
	return nil, fmt.Errorf("file %s requires chunk reading (not inline)", filePath)
}

// deleteFile deletes a single file
func (mc *TableMaintenanceContext) deleteFile(ctx context.Context, filePath string) error {
	dir, name := splitPath(filePath)
	_, err := mc.FilerClient.DeleteEntry(ctx, &filer_pb.DeleteEntryRequest{
		Directory:    dir,
		Name:         name,
		IsDeleteData: true,
	})
	return err
}

// GetReferencedFiles returns all files referenced by the current table metadata
func (mc *TableMaintenanceContext) GetReferencedFiles(ctx context.Context, metadata *IcebergTableMetadata) (map[string]bool, error) {
	referenced := make(map[string]bool)

	for _, snapshot := range metadata.Snapshots {
		// Add manifest list file
		if snapshot.ManifestList != "" {
			referenced[snapshot.ManifestList] = true
		}

		// TODO: Parse manifest list to get individual manifest files
		// TODO: Parse manifests to get data files
		// This requires reading Avro files, which is complex
		// For now, we mark the manifest list as referenced
	}

	return referenced, nil
}

// GetExpiredSnapshots returns snapshots older than the retention period
func (mc *TableMaintenanceContext) GetExpiredSnapshots(metadata *IcebergTableMetadata, retentionDays int) []IcebergSnapshot {
	var expired []IcebergSnapshot
	cutoffTime := time.Now().AddDate(0, 0, -retentionDays).UnixMilli()

	for _, snapshot := range metadata.Snapshots {
		// Never expire the current snapshot
		if snapshot.SnapshotID == metadata.CurrentSnap {
			continue
		}

		// Check if referenced by any branch/tag
		isReferenced := false
		for _, ref := range metadata.Refs {
			if ref.SnapshotID == snapshot.SnapshotID {
				isReferenced = true
				break
			}
		}

		if !isReferenced && snapshot.TimestampMs < cutoffTime {
			expired = append(expired, snapshot)
		}
	}

	return expired
}

// GetSmallDataFiles returns data files smaller than the target size
func (mc *TableMaintenanceContext) GetSmallDataFiles(ctx context.Context, targetSizeBytes int64) ([]string, error) {
	// List all files in the data directory
	dataFiles, err := mc.listAllFiles(ctx, mc.DataDir)
	if err != nil {
		return nil, err
	}

	var smallFiles []string
	for _, file := range dataFiles {
		dir, name := splitPath(file)
		resp, err := filer_pb.LookupEntry(ctx, mc.FilerClient, &filer_pb.LookupDirectoryEntryRequest{
			Directory: dir,
			Name:      name,
		})
		if err != nil {
			continue
		}

		if resp.Entry.Attributes != nil && resp.Entry.Attributes.FileSize < uint64(targetSizeBytes) {
			smallFiles = append(smallFiles, file)
		}
	}

	return smallFiles, nil
}

// splitPath splits a path into directory and name components
func splitPath(p string) (dir, name string) {
	dir = path.Dir(p)
	name = path.Base(p)
	return
}
