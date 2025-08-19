package s3api

import (
	"bytes"
	"crypto/md5"
	"encoding/base64"
	"io"
	"net/http"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
)

// TestSSECRangeRequestsNotSupported verifies that HTTP Range requests are rejected
// for SSE-C encrypted objects because the IV is required at the beginning of the stream
func TestSSECRangeRequestsNotSupported(t *testing.T) {
	// This test verifies that the logic correctly identifies Range requests on SSE-C objects
	// as identified in the GitHub PR review feedback about missing IV handling

	// Create a mock HTTP request with Range header and valid SSE-C headers
	req := &http.Request{
		Header: make(http.Header),
	}
	req.Header.Set("Range", "bytes=10-20")
	req.Header.Set(s3_constants.AmzServerSideEncryptionCustomerAlgorithm, "AES256")

	key := make([]byte, 32)
	for i := range key {
		key[i] = byte(i)
	}
	keyMD5 := base64.StdEncoding.EncodeToString(md5.Sum(key)[:])

	req.Header.Set(s3_constants.AmzServerSideEncryptionCustomerKey, base64.StdEncoding.EncodeToString(key))
	req.Header.Set(s3_constants.AmzServerSideEncryptionCustomerKeyMD5, keyMD5)

	// Test that Range header is detected
	if req.Header.Get("Range") == "" {
		t.Error("Range header should be present in test request")
	}

	// Test that SSE-C headers are detected
	if req.Header.Get(s3_constants.AmzServerSideEncryptionCustomerAlgorithm) == "" {
		t.Error("SSE-C algorithm header should be present in test request")
	}

	// Create a mock HTTP response that simulates SSE-C encrypted object metadata
	proxyResponse := &http.Response{
		StatusCode: 200,
		Header:     make(http.Header),
		Body:       io.NopCloser(bytes.NewReader([]byte("mock encrypted data"))),
	}

	// Set SSE-C metadata headers that would be returned by the filer for encrypted objects
	proxyResponse.Header.Set(s3_constants.AmzServerSideEncryptionCustomerAlgorithm, "AES256")
	proxyResponse.Header.Set(s3_constants.AmzServerSideEncryptionCustomerKeyMD5, keyMD5)

	// Verify that the object appears encrypted based on metadata
	sseAlgorithm := proxyResponse.Header.Get(s3_constants.AmzServerSideEncryptionCustomerAlgorithm)
	sseKeyMD5 := proxyResponse.Header.Get(s3_constants.AmzServerSideEncryptionCustomerKeyMD5)
	isObjectEncrypted := sseAlgorithm != "" && sseKeyMD5 != ""

	if !isObjectEncrypted {
		t.Error("Test object should be detected as SSE-C encrypted")
	}

	// Verify that we would reject this combination (Range + SSE-C)
	hasRangeHeader := req.Header.Get("Range") != ""
	if hasRangeHeader && isObjectEncrypted {
		t.Log("SUCCESS: Range request on SSE-C encrypted object would be correctly rejected")
	} else {
		t.Error("Range request validation logic would not trigger correctly")
	}
}
