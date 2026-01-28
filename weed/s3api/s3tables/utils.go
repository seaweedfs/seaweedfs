package s3tables

import (
	"fmt"
	"regexp"
	"time"
)

// ARN parsing functions

// parseBucketNameFromARN extracts bucket name from table bucket ARN
// ARN format: arn:aws:s3tables:{region}:{account}:bucket/{bucket-name}
func parseBucketNameFromARN(arn string) (string, error) {
	pattern := regexp.MustCompile(`^arn:aws:s3tables:[^:]*:[^:]*:bucket/([a-z0-9_-]+)$`)
	matches := pattern.FindStringSubmatch(arn)
	if len(matches) != 2 {
		return "", fmt.Errorf("invalid bucket ARN: %s", arn)
	}
	return matches[1], nil
}

// parseTableFromARN extracts bucket name, namespace, and table name from ARN
// ARN format: arn:aws:s3tables:{region}:{account}:bucket/{bucket-name}/table/{namespace}/{table-name}
func parseTableFromARN(arn string) (bucketName, namespace, tableName string, err error) {
	pattern := regexp.MustCompile(`^arn:aws:s3tables:[^:]*:[^:]*:bucket/([a-z0-9_-]+)/table/([a-z0-9_]+)/([a-z0-9_]+)$`)
	matches := pattern.FindStringSubmatch(arn)
	if len(matches) != 4 {
		return "", "", "", fmt.Errorf("invalid table ARN: %s", arn)
	}
	return matches[1], matches[2], matches[3], nil
}

// Path helpers

// getTableBucketPath returns the filer path for a table bucket
func getTableBucketPath(bucketName string) string {
	return fmt.Sprintf("%s/%s", TablesPath, bucketName)
}

// getNamespacePath returns the filer path for a namespace
func getNamespacePath(bucketName, namespace string) string {
	return fmt.Sprintf("%s/%s/namespaces/%s", TablesPath, bucketName, namespace)
}

// getTablePath returns the filer path for a table
func getTablePath(bucketName, namespace, tableName string) string {
	return fmt.Sprintf("%s/%s/namespaces/%s/%s", TablesPath, bucketName, namespace, tableName)
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
	pattern := regexp.MustCompile(`^[a-z0-9_-]+$`)
	return pattern.MatchString(name)
}

// generateVersionToken generates a unique version token
func generateVersionToken() string {
	return fmt.Sprintf("%d", time.Now().UnixNano())
}

// splitPath splits a path into directory and name components
func splitPath(path string) (dir, name string) {
	var idx int
	var i int

	// Remove trailing slash
	for i = len(path) - 1; i >= 0 && path[i] == '/'; i-- {
	}
	path = path[:i+1]

	// Find last separator
	idx = len(path) - 1
	for idx >= 0 && path[idx] != '/' {
		idx--
	}

	if idx == -1 {
		return "/", path
	}
	if idx == 0 {
		return "/", path[1:]
	}
	return path[:idx], path[idx+1:]
}
