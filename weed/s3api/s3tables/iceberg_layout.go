package s3tables

import (
	"context"
	"encoding/json"
	"errors"
	pathpkg "path"
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

const uuidPattern = `[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}`

var (
	// Allowed directories in an Iceberg table
	icebergAllowedDirs = map[string]bool{
		"metadata": true,
		"data":     true,
	}

	// Patterns for valid metadata files
	metadataFilePatterns = []*regexp.Regexp{
		regexp.MustCompile(`^v\d+\.metadata\.json$`),                   // Table metadata: v1.metadata.json, v2.metadata.json
		regexp.MustCompile(`^snap-\d+-\d+-` + uuidPattern + `\.avro$`), // Snapshot manifests: snap-123-1-uuid.avro
		regexp.MustCompile(`^` + uuidPattern + `-m\d+\.avro$`),         // Manifest files: uuid-m0.avro
		regexp.MustCompile(`^` + uuidPattern + `\.avro$`),              // General manifest files
		regexp.MustCompile(`^version-hint\.text$`),                     // Version hint file
		regexp.MustCompile(`^` + uuidPattern + `\.metadata\.json$`),    // UUID-named metadata
	}

	// Patterns for valid data files
	dataFilePatterns = []*regexp.Regexp{
		regexp.MustCompile(`^[^/]+\.parquet$`), // Parquet files
		regexp.MustCompile(`^[^/]+\.orc$`),     // ORC files
		regexp.MustCompile(`^[^/]+\.avro$`),    // Avro files
	}

	// Data file partition path pattern (e.g., year=2024/month=01/)
	partitionPathPattern = regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_]*=[^/]+$`)

	// Pattern for valid subdirectory names (alphanumeric, underscore, hyphen, and UUID-style directories)
	validSubdirectoryPattern = regexp.MustCompile(`^[a-zA-Z0-9_-]+$`)
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

	topDir := parts[0]

	// Check if top-level directory is allowed
	if !icebergAllowedDirs[topDir] {
		return &IcebergLayoutError{
			Code:    ErrCodeInvalidIcebergLayout,
			Message: "files must be placed in 'metadata/' or 'data/' directories",
		}
	}

	// If it's just a bare top-level key (no trailing slash and no subpath), reject it
	if len(parts) == 1 {
		return &IcebergLayoutError{
			Code:    ErrCodeInvalidIcebergLayout,
			Message: "must be a directory (use trailing slash) or contain a subpath",
		}
	}

	remainingPath := parts[1]
	if remainingPath == "" {
		return nil // allow paths like "data/" or "metadata/"
	}

	switch topDir {
	case "metadata":
		return v.validateMetadataFile(remainingPath)
	case "data":
		return v.validateDataFile(remainingPath)
	}

	return nil
}

// validateDirectoryPath validates intermediate subdirectories in a path
// isMetadata indicates if we're in the metadata directory (true) or data directory (false)
func validateDirectoryPath(normalizedPath string, isMetadata bool) error {
	if normalizedPath == "" {
		return nil
	}
	// For metadata, reject any subdirectories (enforce flat structure under metadata/)
	if isMetadata {
		if strings.Contains(normalizedPath, "/") || normalizedPath != "" {
			return &IcebergLayoutError{
				Code:    ErrCodeInvalidIcebergLayout,
				Message: "metadata directory does not support subdirectories",
			}
		}
	}

	subdirs := strings.Split(normalizedPath, "/")
	for _, subdir := range subdirs {
		if subdir == "" {
			continue
		}
		// For data, allow both partitions and valid subdirectories
		if !partitionPathPattern.MatchString(subdir) && !isValidSubdirectory(subdir) {
			return &IcebergLayoutError{
				Code:    ErrCodeInvalidIcebergLayout,
				Message: "invalid partition or subdirectory format in data path",
			}
		}
	}
	return nil
}

// validateFilePatterns validates a filename against allowed patterns
// isMetadata indicates if we're validating metadata files (true) or data files (false)
func validateFilePatterns(filename string, isMetadata bool) error {
	var patterns []*regexp.Regexp
	var errorMsg string

	if isMetadata {
		patterns = metadataFilePatterns
		errorMsg = "invalid metadata file format: must be a valid Iceberg metadata, manifest, or snapshot file"
	} else {
		patterns = dataFilePatterns
		errorMsg = "invalid data file format: must be .parquet, .orc, or .avro"
	}

	for _, pattern := range patterns {
		if pattern.MatchString(filename) {
			return nil
		}
	}

	return &IcebergLayoutError{
		Code:    ErrCodeInvalidIcebergLayout,
		Message: errorMsg,
	}
}

// validateFile validates files with a unified logic for metadata and data directories
// isMetadata indicates whether we're validating metadata files (true) or data files (false)
// The logic is:
//  1. If path ends with "/", it's a directory - validate all parts and return nil
//  2. Otherwise, validate intermediate parts, then check the filename against patterns
func (v *IcebergLayoutValidator) validateFile(path string, isMetadata bool) error {
	// Detect if it's a directory (path ends with "/")
	if strings.HasSuffix(path, "/") {
		// Normalize by removing trailing slash
		normalizedPath := strings.TrimSuffix(path, "/")
		return validateDirectoryPath(normalizedPath, isMetadata)
	}

	filename := pathpkg.Base(path)

	// Validate intermediate subdirectories if present
	if dir := pathpkg.Dir(path); dir != "." {
		if err := validateDirectoryPath(dir, isMetadata); err != nil {
			return err
		}
	}

	// Check against allowed file patterns
	err := validateFilePatterns(filename, isMetadata)
	if err == nil {
		return nil
	}

	// Path could be for a directory without a trailing slash, e.g., "data/year=2024"
	if isMetadata {
		if isValidSubdirectory(filename) {
			return nil
		}
	} else {
		if partitionPathPattern.MatchString(filename) || isValidSubdirectory(filename) {
			return nil
		}
	}

	return err
}

// validateMetadataFile validates files in the metadata/ directory
// This is a thin wrapper that calls validateFile with isMetadata=true
func (v *IcebergLayoutValidator) validateMetadataFile(path string) error {
	return v.validateFile(path, true)
}

// validateDataFile validates files in the data/ directory
// This is a thin wrapper that calls validateFile with isMetadata=false
func (v *IcebergLayoutValidator) validateDataFile(path string) error {
	return v.validateFile(path, false)
}

// isValidSubdirectory checks if a path component is a valid subdirectory name
func isValidSubdirectory(name string) bool {
	// Allow alphanumeric, underscore, hyphen, and UUID-style directories
	return validSubdirectoryPattern.MatchString(name)
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
	if tableRelativePath == "" {
		return nil
	}

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

	return
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
		// Distinguish between "not found" and other errors
		if errors.Is(err, filer_pb.ErrNotFound) {
			return &IcebergLayoutError{
				Code:    ErrCodeInvalidIcebergLayout,
				Message: "table does not exist",
			}
		}
		return &IcebergLayoutError{
			Code:    ErrCodeInvalidIcebergLayout,
			Message: "failed to verify table existence: " + err.Error(),
		}
	}

	// Check if table has metadata indicating ICEBERG format
	if resp.Entry == nil || resp.Entry.Extended == nil {
		return &IcebergLayoutError{
			Code:    ErrCodeInvalidIcebergLayout,
			Message: "table is not a valid ICEBERG table (missing metadata)",
		}
	}

	metadataBytes, ok := resp.Entry.Extended[ExtendedKeyMetadata]
	if !ok {
		return &IcebergLayoutError{
			Code:    ErrCodeInvalidIcebergLayout,
			Message: "table is not in ICEBERG format (missing format metadata)",
		}
	}

	var metadata tableMetadataInternal
	if err := json.Unmarshal(metadataBytes, &metadata); err != nil {
		return &IcebergLayoutError{
			Code:    ErrCodeInvalidIcebergLayout,
			Message: "failed to parse table metadata: " + err.Error(),
		}
	}
	if metadata.Format != "ICEBERG" {
		return &IcebergLayoutError{
			Code:    ErrCodeInvalidIcebergLayout,
			Message: "table is not in ICEBERG format",
		}
	}

	return nil
}
