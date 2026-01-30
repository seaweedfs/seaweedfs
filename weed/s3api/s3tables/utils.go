package s3tables

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"net/url"
	"path"
	"regexp"
	"strings"
	"time"
)

const (
	bucketNamePatternStr     = `[a-z0-9-]+`
	tableNamespacePatternStr = `[a-z0-9_]+`
	tableNamePatternStr      = `[a-z0-9_]+`
)

var (
	bucketARNPattern = regexp.MustCompile(`^arn:aws:s3tables:[^:]*:[^:]*:bucket/(` + bucketNamePatternStr + `)$`)
	tableARNPattern  = regexp.MustCompile(`^arn:aws:s3tables:[^:]*:[^:]*:bucket/(` + bucketNamePatternStr + `)/table/(` + tableNamespacePatternStr + `)/(` + tableNamePatternStr + `)$`)
)

// ARN parsing functions

// parseBucketNameFromARN extracts bucket name from table bucket ARN
// ARN format: arn:aws:s3tables:{region}:{account}:bucket/{bucket-name}
func parseBucketNameFromARN(arn string) (string, error) {
	matches := bucketARNPattern.FindStringSubmatch(arn)
	if len(matches) != 2 {
		return "", fmt.Errorf("invalid bucket ARN: %s", arn)
	}
	bucketName := matches[1]
	if !isValidBucketName(bucketName) {
		return "", fmt.Errorf("invalid bucket name in ARN: %s", bucketName)
	}
	return bucketName, nil
}

// ParseBucketNameFromARN is a wrapper to validate bucket ARN for other packages.
func ParseBucketNameFromARN(arn string) (string, error) {
	return parseBucketNameFromARN(arn)
}

// parseTableFromARN extracts bucket name, namespace, and table name from ARN
// ARN format: arn:aws:s3tables:{region}:{account}:bucket/{bucket-name}/table/{namespace}/{table-name}
func parseTableFromARN(arn string) (bucketName, namespace, tableName string, err error) {
	// Updated regex to align with namespace validation (single-segment)
	matches := tableARNPattern.FindStringSubmatch(arn)
	if len(matches) != 4 {
		return "", "", "", fmt.Errorf("invalid table ARN: %s", arn)
	}

	// Validate bucket name
	bucketName = matches[1]
	if err := validateBucketName(bucketName); err != nil {
		return "", "", "", fmt.Errorf("invalid bucket name in ARN: %v", err)
	}

	// Namespace is already constrained by the regex; validate it directly.
	namespace = matches[2]
	_, err = validateNamespace([]string{namespace})
	if err != nil {
		return "", "", "", fmt.Errorf("invalid namespace in ARN: %v", err)
	}

	// URL decode and validate the table name from the ARN path component
	tableNameUnescaped, err := url.PathUnescape(matches[3])
	if err != nil {
		return "", "", "", fmt.Errorf("invalid table name encoding in ARN: %v", err)
	}
	if _, err := validateTableName(tableNameUnescaped); err != nil {
		return "", "", "", fmt.Errorf("invalid table name in ARN: %v", err)
	}
	return bucketName, namespace, tableNameUnescaped, nil
}

// Path helpers

// getTableBucketPath returns the filer path for a table bucket
func getTableBucketPath(bucketName string) string {
	return path.Join(TablesPath, bucketName)
}

// getNamespacePath returns the filer path for a namespace
func getNamespacePath(bucketName, namespace string) string {
	return path.Join(TablesPath, bucketName, namespace)
}

// getTablePath returns the filer path for a table
func getTablePath(bucketName, namespace, tableName string) string {
	return path.Join(TablesPath, bucketName, namespace, tableName)
}

// Metadata structures

type tableBucketMetadata struct {
	Name           string    `json:"name"`
	CreatedAt      time.Time `json:"createdAt"`
	OwnerAccountID string    `json:"ownerAccountId"`
}

// namespaceMetadata stores metadata for a namespace
type namespaceMetadata struct {
	Namespace      []string  `json:"namespace"`
	CreatedAt      time.Time `json:"createdAt"`
	OwnerAccountID string    `json:"ownerAccountId"`
}

// tableMetadataInternal stores metadata for a table
type tableMetadataInternal struct {
	Name             string         `json:"name"`
	Namespace        string         `json:"namespace"`
	Format           string         `json:"format"`
	CreatedAt        time.Time      `json:"createdAt"`
	ModifiedAt       time.Time      `json:"modifiedAt"`
	OwnerAccountID   string         `json:"ownerAccountId"`
	VersionToken     string         `json:"versionToken"`
	MetadataLocation string         `json:"metadataLocation,omitempty"`
	Metadata         *TableMetadata `json:"metadata,omitempty"`
}

// Utility functions

// validateBucketName validates bucket name and returns an error if invalid.
// Bucket names must contain only lowercase letters, numbers, and hyphens.
// Length must be between 3 and 63 characters.
// Must start and end with a letter or digit.
// Reserved prefixes/suffixes are rejected.
func validateBucketName(name string) error {
	if name == "" {
		return fmt.Errorf("bucket name is required")
	}

	if len(name) < 3 || len(name) > 63 {
		return fmt.Errorf("bucket name must be between 3 and 63 characters")
	}

	// Must start and end with a letter or digit
	start := name[0]
	end := name[len(name)-1]
	if !((start >= 'a' && start <= 'z') || (start >= '0' && start <= '9')) {
		return fmt.Errorf("bucket name must start with a letter or digit")
	}
	if !((end >= 'a' && end <= 'z') || (end >= '0' && end <= '9')) {
		return fmt.Errorf("bucket name must end with a letter or digit")
	}

	// Allowed characters: a-z, 0-9, -
	for i := 0; i < len(name); i++ {
		ch := name[i]
		if (ch >= 'a' && ch <= 'z') || (ch >= '0' && ch <= '9') || ch == '-' {
			continue
		}
		return fmt.Errorf("bucket name can only contain lowercase letters, numbers, and hyphens")
	}

	// Reserved prefixes
	reservedPrefixes := []string{"xn--", "sthree-", "amzn-s3-demo-", "aws"}
	for _, p := range reservedPrefixes {
		if strings.HasPrefix(name, p) {
			return fmt.Errorf("bucket name cannot start with reserved prefix: %s", p)
		}
	}

	// Reserved suffixes
	reservedSuffixes := []string{"-s3alias", "--ol-s3", "--x-s3", "--table-s3"}
	for _, s := range reservedSuffixes {
		if strings.HasSuffix(name, s) {
			return fmt.Errorf("bucket name cannot end with reserved suffix: %s", s)
		}
	}

	return nil
}

// ValidateBucketName validates bucket name and returns an error if invalid.
func ValidateBucketName(name string) error {
	return validateBucketName(name)
}

// isValidBucketName validates bucket name characters (kept for compatibility)
// Deprecated: use validateBucketName instead
func isValidBucketName(name string) bool {
	return validateBucketName(name) == nil
}

// generateVersionToken generates a unique, unpredictable version token
func generateVersionToken() string {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		// Fallback to timestamp if crypto/rand fails
		return fmt.Sprintf("%x", time.Now().UnixNano())
	}
	return hex.EncodeToString(b)
}

// splitPath splits a path into directory and name components using stdlib
func splitPath(p string) (dir, name string) {
	dir = path.Dir(p)
	name = path.Base(p)
	return
}

// validateNamespace validates that the namespace provided is supported (single-level)
func validateNamespace(namespace []string) (string, error) {
	if len(namespace) == 0 {
		return "", fmt.Errorf("namespace is required")
	}
	if len(namespace) > 1 {
		return "", fmt.Errorf("multi-level namespaces are not supported")
	}
	name := namespace[0]
	if len(name) < 1 || len(name) > 255 {
		return "", fmt.Errorf("namespace name must be between 1 and 255 characters")
	}

	// Prevent path traversal and multi-segment paths
	if name == "." || name == ".." {
		return "", fmt.Errorf("namespace name cannot be '.' or '..'")
	}
	if strings.Contains(name, "/") {
		return "", fmt.Errorf("namespace name cannot contain '/'")
	}

	// Must start and end with a letter or digit
	start := name[0]
	end := name[len(name)-1]
	if !((start >= 'a' && start <= 'z') || (start >= '0' && start <= '9')) {
		return "", fmt.Errorf("namespace name must start with a letter or digit")
	}
	if !((end >= 'a' && end <= 'z') || (end >= '0' && end <= '9')) {
		return "", fmt.Errorf("namespace name must end with a letter or digit")
	}

	// Allowed characters: a-z, 0-9, _
	for _, ch := range name {
		if (ch >= 'a' && ch <= 'z') || (ch >= '0' && ch <= '9') || ch == '_' {
			continue
		}
		return "", fmt.Errorf("invalid namespace name: only 'a-z', '0-9', and '_' are allowed")
	}

	// Reserved prefix
	if strings.HasPrefix(name, "aws") {
		return "", fmt.Errorf("namespace name cannot start with reserved prefix 'aws'")
	}

	return name, nil
}

// ValidateNamespace is a wrapper to validate namespace for other packages.
func ValidateNamespace(namespace []string) (string, error) {
	return validateNamespace(namespace)
}

// validateTableName validates a table name
func validateTableName(name string) (string, error) {
	if len(name) < 1 || len(name) > 255 {
		return "", fmt.Errorf("table name must be between 1 and 255 characters")
	}
	if name == "." || name == ".." || strings.Contains(name, "/") {
		return "", fmt.Errorf("invalid table name: cannot be '.', '..' or contain '/'")
	}

	// First character must be a letter or digit
	start := name[0]
	if !((start >= 'a' && start <= 'z') || (start >= '0' && start <= '9')) {
		return "", fmt.Errorf("table name must start with a letter or digit")
	}

	// Allowed characters: a-z, 0-9, _
	for _, ch := range name {
		if (ch >= 'a' && ch <= 'z') || (ch >= '0' && ch <= '9') || ch == '_' {
			continue
		}
		return "", fmt.Errorf("invalid table name: only 'a-z', '0-9', and '_' are allowed")
	}
	return name, nil
}

// ValidateTableName is a wrapper to validate table name for other packages.
func ValidateTableName(name string) (string, error) {
	return validateTableName(name)
}

// flattenNamespace joins namespace elements into a single string (using dots as per AWS S3 Tables)
func flattenNamespace(namespace []string) string {
	if len(namespace) == 0 {
		return ""
	}
	return strings.Join(namespace, ".")
}
