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

var (
	bucketARNPattern  = regexp.MustCompile(`^arn:aws:s3tables:[^:]*:[^:]*:bucket/([a-z0-9_-]+)$`)
	tableARNPattern   = regexp.MustCompile(`^arn:aws:s3tables:[^:]*:[^:]*:bucket/([a-z0-9_-]+)/table/([a-z0-9_-]+)/([a-z0-9_-]+)$`)
	bucketNamePattern = regexp.MustCompile(`^[a-z0-9_-]+$`)
)

// ARN parsing functions

// parseBucketNameFromARN extracts bucket name from table bucket ARN
// ARN format: arn:aws:s3tables:{region}:{account}:bucket/{bucket-name}
func parseBucketNameFromARN(arn string) (string, error) {
	matches := bucketARNPattern.FindStringSubmatch(arn)
	if len(matches) != 2 {
		return "", fmt.Errorf("invalid bucket ARN: %s", arn)
	}
	return matches[1], nil
}

// parseTableFromARN extracts bucket name, namespace, and table name from ARN
// ARN format: arn:aws:s3tables:{region}:{account}:bucket/{bucket-name}/table/{namespace}/{table-name}
func parseTableFromARN(arn string) (bucketName, namespace, tableName string, err error) {
	// Updated regex to align with namespace validation (single-segment)
	matches := tableARNPattern.FindStringSubmatch(arn)
	if len(matches) != 4 {
		return "", "", "", fmt.Errorf("invalid table ARN: %s", arn)
	}

	// URL decode the namespace from the ARN path component
	namespaceUnescaped, err := url.PathUnescape(matches[2])
	if err != nil {
		return "", "", "", fmt.Errorf("invalid namespace encoding in ARN: %v", err)
	}

	_, err = validateNamespace([]string{namespaceUnescaped})
	if err != nil {
		return "", "", "", fmt.Errorf("invalid namespace in ARN: %v", err)
	}

	return matches[1], namespaceUnescaped, matches[3], nil
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

// tableBucketMetadata stores metadata for a table bucket
type tableBucketMetadata struct {
	Name      string    `json:"name"`
	CreatedAt time.Time `json:"createdAt"`
	OwnerID   string    `json:"ownerAccountId"`
}

// namespaceMetadata stores metadata for a namespace
type namespaceMetadata struct {
	Namespace []string  `json:"namespace"`
	CreatedAt time.Time `json:"createdAt"`
	OwnerID   string    `json:"ownerAccountId"`
}

// tableMetadataInternal stores metadata for a table
type tableMetadataInternal struct {
	Name             string         `json:"name"`
	Namespace        string         `json:"namespace"`
	Format           string         `json:"format"`
	CreatedAt        time.Time      `json:"createdAt"`
	ModifiedAt       time.Time      `json:"modifiedAt"`
	OwnerID          string         `json:"ownerAccountId"`
	VersionToken     string         `json:"versionToken"`
	MetadataLocation string         `json:"metadataLocation,omitempty"`
	Schema           *TableMetadata `json:"metadata,omitempty"`
}

// Utility functions

// isValidBucketName validates bucket name characters
// Bucket names must contain only lowercase letters, numbers, hyphens, and underscores
func isValidBucketName(name string) bool {
	return bucketNamePattern.MatchString(name)
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

	// Prevent path traversal and multi-segment paths within a single namespace element.
	if name == "." || name == ".." {
		return "", fmt.Errorf("namespace name cannot be '.' or '..'")
	}
	if strings.Contains(name, "/") {
		return "", fmt.Errorf("namespace name cannot contain '/'")
	}

	// Enforce allowed character set consistent with table naming.
	for _, ch := range name {
		if (ch >= 'a' && ch <= 'z') || (ch >= '0' && ch <= '9') || ch == '_' || ch == '-' {
			continue
		}
		return "", fmt.Errorf("invalid namespace name: only 'a-z', '0-9', '_', and '-' are allowed")
	}

	return name, nil
}

// validateTableName validates a table name
func validateTableName(name string) (string, error) {
	if len(name) < 1 || len(name) > 255 {
		return "", fmt.Errorf("table name must be between 1 and 255 characters")
	}
	if name == "." || name == ".." || strings.Contains(name, "/") {
		return "", fmt.Errorf("invalid table name: cannot be '.', '..' or contain '/'")
	}
	for _, ch := range name {
		if (ch >= 'a' && ch <= 'z') || (ch >= '0' && ch <= '9') || ch == '_' || ch == '-' {
			continue
		}
		return "", fmt.Errorf("invalid table name: only 'a-z', '0-9', '_', and '-' are allowed")
	}
	return name, nil
}

// flattenNamespace joins namespace elements into a single string (using dots as per AWS S3 Tables)
func flattenNamespace(namespace []string) string {
	if len(namespace) == 0 {
		return ""
	}
	return strings.Join(namespace, ".")
}
