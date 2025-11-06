package s3api

import (
	"bytes"
	"crypto/md5"
	"encoding/base64"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gorilla/mux"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
)

// ResponseRecorder that also implements http.Flusher
type recorderFlusher struct{ *httptest.ResponseRecorder }

func (r recorderFlusher) Flush() {}

// TestSSECRangeRequestsSupported verifies that HTTP Range requests are now supported
// for SSE-C encrypted objects since the IV is stored in metadata and CTR mode allows seeking
func TestSSECRangeRequestsSupported(t *testing.T) {
	// Create a request with Range header and valid SSE-C headers
	req := httptest.NewRequest(http.MethodGet, "/b/o", nil)
	req.Header.Set("Range", "bytes=10-20")
	req.Header.Set(s3_constants.AmzServerSideEncryptionCustomerAlgorithm, "AES256")

	key := make([]byte, 32)
	for i := range key {
		key[i] = byte(i)
	}
	s := md5.Sum(key)
	keyMD5 := base64.StdEncoding.EncodeToString(s[:])

	req.Header.Set(s3_constants.AmzServerSideEncryptionCustomerKey, base64.StdEncoding.EncodeToString(key))
	req.Header.Set(s3_constants.AmzServerSideEncryptionCustomerKeyMD5, keyMD5)

	// Attach mux vars to avoid panic in error writer
	req = mux.SetURLVars(req, map[string]string{"bucket": "b", "object": "o"})

	// Create a mock HTTP response that simulates SSE-C encrypted object metadata
	proxyResponse := &http.Response{
		StatusCode: 200,
		Header:     make(http.Header),
		Body:       io.NopCloser(bytes.NewReader([]byte("mock encrypted data"))),
	}
	proxyResponse.Header.Set(s3_constants.AmzServerSideEncryptionCustomerAlgorithm, "AES256")
	proxyResponse.Header.Set(s3_constants.AmzServerSideEncryptionCustomerKeyMD5, keyMD5)

	// Call the function under test - should no longer reject range requests
	s3a := &S3ApiServer{
		option: &S3ApiServerOption{
			BucketsPath: "/buckets",
		},
	}
	rec := httptest.NewRecorder()
	w := recorderFlusher{rec}
	// Pass nil for entry since this test focuses on Range request handling
	statusCode, _ := s3a.handleSSECResponse(req, proxyResponse, w, nil)

	// Range requests should now be allowed to proceed (will be handled by filer layer)
	// The exact status code depends on the object existence and filer response
	if statusCode == http.StatusRequestedRangeNotSatisfiable {
		t.Fatalf("Range requests should no longer be rejected for SSE-C objects, got status %d", statusCode)
	}
}
