package s3api

import (
	"bytes"
	"io"
	"net/http"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/s3api/s3err"
)

// TestChunkedEncodingMixedFormat tests the fix for GitHub issue #6847
// where AWS SDKs send mixed format: unsigned streaming headers but signed chunk data
func TestChunkedEncodingMixedFormat(t *testing.T) {
	expectedContent := "hello world\n"

	// Create the problematic mixed format payload:
	// - Unsigned streaming headers (STREAMING-UNSIGNED-PAYLOAD-TRAILER)
	// - But chunk data contains chunk-signature headers
	mixedFormatPayload := "c;chunk-signature=347f6c62acd95b7c6ae18648776024a9e8cd6151184a5e777ea8e1d9b4e45b3c\r\n" +
		"hello world\n\r\n" +
		"0;chunk-signature=1a99b7790b8db0f4bfc048c8802056c3179d561e40c073167e79db5f1a6af4b2\r\n" +
		"x-amz-checksum-crc32:rwg7LQ==\r\n" +
		"\r\n"

	// Create HTTP request with unsigned streaming headers
	req, _ := http.NewRequest("PUT", "/test-bucket/test-object", bytes.NewReader([]byte(mixedFormatPayload)))
	req.Header.Set("x-amz-content-sha256", "STREAMING-UNSIGNED-PAYLOAD-TRAILER")
	req.Header.Set("x-amz-trailer", "x-amz-checksum-crc32")

	// Process through SeaweedFS chunked reader
	iam := setupTestIAM()
	reader, errCode := iam.newChunkedReader(req)

	if errCode != s3err.ErrNone {
		t.Fatalf("Failed to create chunked reader: %v", errCode)
	}

	// Read the content
	actualContent, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("Failed to read content: %v", err)
	}

	// Should correctly extract just the content, ignoring chunk signatures
	if string(actualContent) != expectedContent {
		t.Errorf("Mixed format handling failed. Expected: %q, Got: %q", expectedContent, string(actualContent))
	}
}

// setupTestIAM creates a test IAM instance using the same pattern as existing tests
func setupTestIAM() *IdentityAccessManagement {
	iam := &IdentityAccessManagement{}
	return iam
}
