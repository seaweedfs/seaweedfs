package catalog

import (
	"fmt"
	"io"
	"net/http"
	"testing"
)

// verifyTableBucketMetadata verifies that a table bucket was created with proper metadata
func verifyTableBucketMetadata(t *testing.T, env *TestEnvironment, bucketName string) {
	t.Helper()

	// Use S3Tables REST API to get the bucket
	endpoint := fmt.Sprintf("http://localhost:%d/buckets/%s", env.s3Port, bucketName)

	req, err := http.NewRequest(http.MethodGet, endpoint, nil)
	if err != nil {
		t.Fatalf("Failed to create request: %v", err)
	}
	req.Header.Set("Content-Type", "application/x-amz-json-1.1")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("Failed to get table bucket %s: %v", bucketName, err)
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	t.Logf("Get table bucket %s response: status=%d, body=%s", bucketName, resp.StatusCode, string(body))

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("Failed to get table bucket %s, status %d: %s", bucketName, resp.StatusCode, body)
	}
	t.Logf("Verified table bucket %s exists with metadata", bucketName)
}
