package s3tables

import (
	"context"
	"encoding/json"
	"regexp"
	"strings"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
)

// Iceberg file layout validation
// Apache Iceberg tables follow a specific file layout structure:
// - metadata/ directory containing metadata files (*.json, *.avro)
// - data/ directory containing data files (*.parquet, *.orc, *.avro)
//
// Valid file patterns include:
// - metadata/v*.metadata.json (table metadata)
// - metadata/snap-*.avro (snapshot manifest lists)
// - metadata/*.avro (manifest files)
// - data/*.parquet, data/*.orc, data/*.avro (data files)

var (
	// Allowed directories in an Iceberg table
	icebergAllowedDirs = map[string]bool{
		"metadata": true,
		"data":     true,
	}

	// Patterns for valid metadata files
	metadataFilePatterns = []*regexp.Regexp{
		regexp.MustCompile(`^v\d+\.metadata\.json$`),                                 // Table metadata: v1.metadata.json, v2.metadata.json
		regexp.MustCompile(`^snap-\d+-\d+-[a-f0-9-]+\.avro$`),                        // Snapshot manifests: snap-123-1-uuid.avro
		regexp.MustCompile(`^[a-f0-9-]+-m\d+\.avro$`),                                // Manifest files: uuid-m0.avro
		regexp.MustCompile(`^[a-f0-9-]+\.avro$`),                                     // General manifest files
		regexp.MustCompile(`^version-hint\.text$`),                                   // Version hint file
		regexp.MustCompile(`^[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}\.metadata\.json$`), // UUID-named metadata
	}

	// Patterns for valid data files
	dataFilePatterns = []*regexp.Regexp{
		regexp.MustCompile(`^[^/]+\.parquet$`), // Parquet files
		regexp.MustCompile(`^[^/]+\.orc$`),     // ORC files
		regexp.MustCompile(`^[^/]+\.avro$`),    // Avro files
	}

	// Data file partition path pattern (e.g., year=2024/month=01/)
	partitionPathPattern = regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_]*=[^/]+$`)
)

// IcebergLayoutValidator validates that files conform to Iceberg table layout
type IcebergLayoutValidator struct{}

// NewIcebergLayoutValidator creates a new Iceberg layout validator
func NewIcebergLayoutValidator() *IcebergLayoutValidator {
	return &IcebergLayoutValidator{}
}

// ValidateFilePath validates that a file path conforms to Iceberg layout
// The path should be relative to the table root (e.g., "metadata/v1.metadata.json" or "data/file.parquet")
func (v *IcebergLayoutValidator) ValidateFilePath(relativePath string) error {
	// Normalize path separators
	relativePath = strings.TrimPrefix(relativePath, "/")
	if relativePath == "" {
		return &IcebergLayoutError{
			Code:    ErrCodeInvalidIcebergLayout,
			Message: "empty file path",
		}
	}

	parts := strings.SplitN(relativePath, "/", 2)
	if len(parts) == 0 {
		return &IcebergLayoutError{
			Code:    ErrCodeInvalidIcebergLayout,
			Message: "invalid file path structure",
		}
	}

	topDir := parts[0]

	// Check if top-level directory is allowed
	if !icebergAllowedDirs[topDir] {
		return &IcebergLayoutError{
			Code:    ErrCodeInvalidIcebergLayout,
			Message: "files must be placed in 'metadata/' or 'data/' directories",
		}
	}

	// If it's just a directory creation, allow it
	if len(parts) == 1 {
		return nil
	}

	remainingPath := parts[1]

	switch topDir {
	case "metadata":
		return v.validateMetadataFile(remainingPath)
	case "data":
		return v.validateDataFile(remainingPath)
	}

	return nil
}

// validateMetadataFile validates files in the metadata/ directory
func (v *IcebergLayoutValidator) validateMetadataFile(path string) error {
	// Get the filename (last component)
	parts := strings.Split(path, "/")
	filename := parts[len(parts)-1]

	// Check against allowed metadata file patterns
	for _, pattern := range metadataFilePatterns {
		if pattern.MatchString(filename) {
			return nil
		}
	}

	return &IcebergLayoutError{
		Code:    ErrCodeInvalidIcebergLayout,
		Message: "invalid metadata file format: must be a valid Iceberg metadata, manifest, or snapshot file",
	}
}

// validateDataFile validates files in the data/ directory
func (v *IcebergLayoutValidator) validateDataFile(path string) error {
	parts := strings.Split(path, "/")
	filename := parts[len(parts)-1]

	// Validate partition directories if present
	if len(parts) > 1 {
		for i := 0; i < len(parts)-1; i++ {
			// Allow nested data directories and partition directories
			if !partitionPathPattern.MatchString(parts[i]) && parts[i] != "" {
				// Allow plain subdirectories (bucket ID directories, etc.)
				if !isValidSubdirectory(parts[i]) {
					return &IcebergLayoutError{
						Code:    ErrCodeInvalidIcebergLayout,
						Message: "invalid partition path format in data directory",
					}
				}
			}
		}
	}

	// Check against allowed data file patterns
	for _, pattern := range dataFilePatterns {
		if pattern.MatchString(filename) {
			return nil
		}
	}

	return &IcebergLayoutError{
		Code:    ErrCodeInvalidIcebergLayout,
		Message: "invalid data file format: must be .parquet, .orc, or .avro",
	}
}

// isValidSubdirectory checks if a path component is a valid subdirectory name
func isValidSubdirectory(name string) bool {
	// Allow alphanumeric, underscore, hyphen, and UUID-style directories
	validSubdir := regexp.MustCompile(`^[a-zA-Z0-9_-]+$`)
	return validSubdir.MatchString(name)
}

// IcebergLayoutError represents an Iceberg layout validation error
type IcebergLayoutError struct {
	Code    string
	Message string
}

func (e *IcebergLayoutError) Error() string {
	return e.Message
}

// Error code for Iceberg layout violations
const (
	ErrCodeInvalidIcebergLayout = "InvalidIcebergLayout"
)

// TableBucketFileValidator validates file uploads to table buckets
type TableBucketFileValidator struct {
	layoutValidator *IcebergLayoutValidator
}

// NewTableBucketFileValidator creates a new table bucket file validator
func NewTableBucketFileValidator() *TableBucketFileValidator {
	return &TableBucketFileValidator{
		layoutValidator: NewIcebergLayoutValidator(),
	}
}

// ValidateTableBucketUpload checks if a file upload to a table bucket conforms to Iceberg layout
// fullPath is the complete filer path (e.g., /table-buckets/mybucket/mynamespace/mytable/data/file.parquet)
// Returns nil if the path is not a table bucket path or if validation passes
// Returns an error if the file doesn't conform to Iceberg layout
func (v *TableBucketFileValidator) ValidateTableBucketUpload(fullPath string) error {
	// Check if this is a table bucket path
	if !strings.HasPrefix(fullPath, TablesPath+"/") {
		return nil // Not a table bucket, no validation needed
	}

	// Extract the path relative to table bucket root
	// Format: /table-buckets/{bucket}/{namespace}/{table}/{relative-path}
	relativePath := strings.TrimPrefix(fullPath, TablesPath+"/")
	parts := strings.SplitN(relativePath, "/", 4)

	// Need at least bucket/namespace/table/file
	if len(parts) < 4 {
		// Creating bucket, namespace, or table directories - allow
		return nil
	}

	// The last part is the path within the table (data/file.parquet or metadata/v1.json)
	tableRelativePath := parts[3]

	return v.layoutValidator.ValidateFilePath(tableRelativePath)
}

// IsTableBucketPath checks if a path is under the table-buckets directory
func IsTableBucketPath(fullPath string) bool {
	return strings.HasPrefix(fullPath, TablesPath+"/")
}

// GetTableInfoFromPath extracts bucket, namespace, and table names from a table bucket path
// Returns empty strings if the path doesn't contain enough components
func GetTableInfoFromPath(fullPath string) (bucket, namespace, table string) {
	if !strings.HasPrefix(fullPath, TablesPath+"/") {
		return "", "", ""
	}

	relativePath := strings.TrimPrefix(fullPath, TablesPath+"/")
	parts := strings.SplitN(relativePath, "/", 4)

	if len(parts) >= 1 {
		bucket = parts[0]
	}
	if len(parts) >= 2 {
		namespace = parts[1]
	}
	if len(parts) >= 3 {
		table = parts[2]
	}

	return bucket, namespace, table
}

// ValidateTableBucketUploadWithClient validates upload and checks that the table exists and is ICEBERG format
func (v *TableBucketFileValidator) ValidateTableBucketUploadWithClient(
	ctx context.Context,
	client filer_pb.SeaweedFilerClient,
	fullPath string,
) error {
	// First check basic layout
	if err := v.ValidateTableBucketUpload(fullPath); err != nil {
		return err
	}

	// If not a table bucket path, nothing more to check
	if !IsTableBucketPath(fullPath) {
		return nil
	}

	// Get table info and verify it exists
	bucket, namespace, table := GetTableInfoFromPath(fullPath)
	if bucket == "" || namespace == "" || table == "" {
		return nil // Not deep enough to need validation
	}

	// Verify the table exists and has ICEBERG format by checking its metadata
	tablePath := getTablePath(bucket, namespace, table)
	dir, name := splitPath(tablePath)

	resp, err := filer_pb.LookupEntry(ctx, client, &filer_pb.LookupDirectoryEntryRequest{
		Directory: dir,
		Name:      name,
	})
	if err != nil {
		// Table doesn't exist, reject the upload
		return &IcebergLayoutError{
			Code:    ErrCodeInvalidIcebergLayout,
			Message: "table does not exist",
		}
	}

	// Check if table has metadata indicating ICEBERG format
	if resp.Entry != nil && resp.Entry.Extended != nil {
		if metadataBytes, ok := resp.Entry.Extended[ExtendedKeyMetadata]; ok {
			var metadata tableMetadataInternal
			if err := json.Unmarshal(metadataBytes, &metadata); err == nil {
				if metadata.Format != "ICEBERG" {
					return &IcebergLayoutError{
						Code:    ErrCodeInvalidIcebergLayout,
						Message: "table is not in ICEBERG format",
					}
				}
			}
		}
	}

	return nil
}
