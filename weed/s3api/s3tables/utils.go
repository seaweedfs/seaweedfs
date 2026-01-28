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
	tableNamespacePatternStr = `[a-z0-9_-]+`
)

var (
	bucketARNPattern  = regexp.MustCompile(`^arn:aws:s3tables:[^:]*:[^:]*:bucket/(` + bucketNamePatternStr + `)$`)
	tableARNPattern   = regexp.MustCompile(`^arn:aws:s3tables:[^:]*:[^:]*:bucket/(` + bucketNamePatternStr + `)/table/(` + tableNamespacePatternStr + `)/(` + tableNamespacePatternStr + `)$`)
	bucketNamePattern = regexp.MustCompile(`^` + bucketNamePatternStr + `$`)
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
	Schema           *TableMetadata `json:"metadata,omitempty"`
}

// Utility functions

// isValidBucketName validates bucket name characters
// Bucket names must contain only lowercase letters, numbers, and hyphens.
// Length must be between 3 and 63 characters.
// Must start and end with a letter or digit.
// Reserved prefixes/suffixes are rejected.
func isValidBucketName(name string) bool {
	if len(name) < 3 || len(name) > 63 {
		return false
	}

	// Must start and end with a letter or digit
	start := name[0]
	end := name[len(name)-1]
	if !((start >= 'a' && start <= 'z') || (start >= '0' && start <= '9')) {
		return false
	}
	if !((end >= 'a' && end <= 'z') || (end >= '0' && end <= '9')) {
		return false
	}

	// Allowed characters: a-z, 0-9, -
	for i := 0; i < len(name); i++ {
		ch := name[i]
		if (ch >= 'a' && ch <= 'z') || (ch >= '0' && ch <= '9') || ch == '-' {
			continue
		}
		return false
	}

	// Reserved prefixes
	reservedPrefixes := []string{"xn--", "sthree-", "amzn-s3-demo-", "aws"}
	for _, p := range reservedPrefixes {
		if strings.HasPrefix(name, p) {
			return false
		}
	}

	// Reserved suffixes
	reservedSuffixes := []string{"-s3alias", "--ol-s3", "--x-s3", "--table-s3"}
	for _, s := range reservedSuffixes {
		if strings.HasSuffix(name, s) {
			return false
		}
	}

	return true
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

// flattenNamespace joins namespace elements into a single string (using dots as per AWS S3 Tables)
func flattenNamespace(namespace []string) string {
	if len(namespace) == 0 {
		return ""
	}
	return strings.Join(namespace, ".")
}
