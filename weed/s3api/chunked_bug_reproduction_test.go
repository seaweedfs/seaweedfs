package s3api

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"strings"
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

// createProblematicChunkedPayload creates the exact chunked payload from the GitHub issue
// This simulates what newer AWS SDKs actually send with STREAMING-UNSIGNED-PAYLOAD-TRAILER
func createProblematicChunkedPayload(content string) string {
	// For unsigned streaming, chunks should NOT have chunk-signature headers
	contentHex := fmt.Sprintf("%x", len(content))

	// First chunk with content (no chunk-signature for unsigned streaming)
	chunk1 := fmt.Sprintf("%s\r\n%s\r\n", contentHex, content)

	// Final chunk (0-length) with trailer
	finalChunk := "0\r\n" +
		"x-amz-checksum-crc32:rwg7LQ==\r\n" +
		"\r\n"

	return chunk1 + finalChunk
}

// createChunkedRequest creates an HTTP request with chunked encoding headers
func createChunkedRequest(payload string) *http.Request {
	req, _ := http.NewRequest("PUT", "/test-bucket/test-object", bytes.NewReader([]byte(payload)))

	// Set headers that trigger chunked processing
	req.Header.Set("x-amz-content-sha256", "STREAMING-UNSIGNED-PAYLOAD-TRAILER")
	req.Header.Set("x-amz-trailer", "x-amz-checksum-crc32")
	req.Header.Set("Content-Encoding", "aws-chunked")
	req.Header.Set("x-amz-decoded-content-length", "12") // length of "hello world\n"

	return req
}

// setupTestIAM creates a test IAM instance using the same pattern as existing tests
func setupTestIAM() *IdentityAccessManagement {
	iam := &IdentityAccessManagement{}
	return iam
}

// Standalone test to reproduce without relying on testing framework
func ReproduceChunkedEncodingBugStandalone() {
	fmt.Println("=== Reproducing Chunked Encoding Bug (Standalone) ===")

	// This simulates the exact payload that AWS SDK sends with chunked encoding
	// Based on the error message from the GitHub issue
	expectedContent := "hello world\n"

	// Create chunked payload similar to what AWS SDK sends
	chunkedPayload := createProblematicChunkedPayload(expectedContent)

	fmt.Printf("Input chunked payload:\n%s\n", chunkedPayload)
	fmt.Printf("Expected output: %q\n", expectedContent)

	// Create HTTP request that triggers the bug
	req := createChunkedRequest(chunkedPayload)

	// Process through SeaweedFS chunked reader
	iam := setupTestIAM()
	reader, errCode := iam.newChunkedReader(req)

	if errCode != s3err.ErrNone {
		fmt.Printf("ERROR: Failed to create chunked reader: %v\n", errCode)
		return
	}

	// Read the content
	actualContent, err := io.ReadAll(reader)
	if err != nil {
		fmt.Printf("ERROR: Failed to read content: %v\n", err)
		return
	}

	fmt.Printf("Actual output: %q\n", string(actualContent))

	// Check if the bug is present
	if string(actualContent) == expectedContent {
		fmt.Println("✅ PASS: Content correctly stripped of chunk headers")
	} else {
		fmt.Println("❌ FAIL: Bug reproduced - chunk headers included in content!")
		fmt.Printf("Expected: %q\n", expectedContent)
		fmt.Printf("Got:      %q\n", string(actualContent))

		if strings.Contains(string(actualContent), "chunk-signature") {
			fmt.Println("   🐛 Contains chunk-signature headers")
		}
		if strings.Contains(string(actualContent), "x-amz-checksum") {
			fmt.Println("   🐛 Contains x-amz-checksum headers")
		}
		if strings.Contains(string(actualContent), "x-amz-trailer-signature") {
			fmt.Println("   🐛 Contains x-amz-trailer-signature headers")
		}
	}
}
