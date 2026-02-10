package iceberg

import (
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"strconv"
	"strings"

	"github.com/gorilla/mux"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3tables"
)

// parseNamespace parses the namespace from path parameter.
// Iceberg uses unit separator (0x1F) for multi-level namespaces.
// Note: mux already decodes URL-encoded path parameters, so we only split by unit separator.
func parseNamespace(encoded string) []string {
	if encoded == "" {
		return nil
	}
	parts := strings.Split(encoded, "\x1F")
	// Filter empty parts
	result := make([]string, 0, len(parts))
	for _, p := range parts {
		if p != "" {
			result = append(result, p)
		}
	}
	return result
}

// encodeNamespace encodes namespace parts for response.
func encodeNamespace(parts []string) string {
	return strings.Join(parts, "\x1F")
}

func parseS3Location(location string) (bucketName, tablePath string, err error) {
	if !strings.HasPrefix(location, "s3://") {
		return "", "", fmt.Errorf("unsupported location: %s", location)
	}
	trimmed := strings.TrimPrefix(location, "s3://")
	trimmed = strings.TrimSuffix(trimmed, "/")
	if trimmed == "" {
		return "", "", fmt.Errorf("invalid location: %s", location)
	}
	parts := strings.SplitN(trimmed, "/", 2)
	bucketName = parts[0]
	if bucketName == "" {
		return "", "", fmt.Errorf("invalid location bucket: %s", location)
	}
	if len(parts) == 2 {
		tablePath = parts[1]
	}
	return bucketName, tablePath, nil
}

func tableLocationFromMetadataLocation(metadataLocation string) string {
	trimmed := strings.TrimSuffix(metadataLocation, "/")
	if idx := strings.LastIndex(trimmed, "/metadata/"); idx != -1 {
		return trimmed[:idx]
	}
	return trimmed
}

// writeJSON writes a JSON response.
func writeJSON(w http.ResponseWriter, status int, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if v != nil {
		data, err := json.Marshal(v)
		if err != nil {
			glog.Errorf("Iceberg: failed to encode response: %v", err)
			return
		}
		w.Write(data)
	}
}

// writeError writes an Iceberg error response.
func writeError(w http.ResponseWriter, status int, errType, message string) {
	resp := ErrorResponse{
		Error: ErrorModel{
			Message: message,
			Type:    errType,
			Code:    status,
		},
	}
	writeJSON(w, status, resp)
}

// getBucketFromPrefix extracts table bucket name from prefix parameter.
// For now, we use the prefix as the table bucket name.
func getBucketFromPrefix(r *http.Request) string {
	vars := mux.Vars(r)
	if prefix := vars["prefix"]; prefix != "" {
		return prefix
	}
	if bucket := os.Getenv("S3TABLES_DEFAULT_BUCKET"); bucket != "" {
		return bucket
	}
	// Default bucket if no prefix - use "warehouse" for Iceberg
	return "warehouse"
}

// buildTableBucketARN builds an ARN for a table bucket.
func buildTableBucketARN(bucketName string) string {
	arn, _ := s3tables.BuildBucketARN(s3tables.DefaultRegion, s3_constants.AccountAdminId, bucketName)
	return arn
}

const (
	defaultListPageSize = 1000
	maxListPageSize     = 1000
)

func getPaginationQueryParam(r *http.Request, primary, fallback string) string {
	if v := strings.TrimSpace(r.URL.Query().Get(primary)); v != "" {
		return v
	}
	return strings.TrimSpace(r.URL.Query().Get(fallback))
}

func parsePagination(r *http.Request) (pageToken string, pageSize int, err error) {
	pageToken = getPaginationQueryParam(r, "pageToken", "page-token")
	pageSize = defaultListPageSize

	pageSizeValue := getPaginationQueryParam(r, "pageSize", "page-size")
	if pageSizeValue == "" {
		return pageToken, pageSize, nil
	}

	parsedPageSize, parseErr := strconv.Atoi(pageSizeValue)
	if parseErr != nil || parsedPageSize <= 0 {
		return pageToken, 0, fmt.Errorf("invalid pageSize %q: must be a positive integer", pageSizeValue)
	}
	if parsedPageSize > maxListPageSize {
		return pageToken, 0, fmt.Errorf("invalid pageSize %q: must be <= %d", pageSizeValue, maxListPageSize)
	}

	return pageToken, parsedPageSize, nil
}

func normalizeNamespaceProperties(properties map[string]string) map[string]string {
	if properties == nil {
		return map[string]string{}
	}
	return properties
}
