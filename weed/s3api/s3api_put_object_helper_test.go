package s3api

import (
	"bytes"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/credential"
	_ "github.com/seaweedfs/seaweedfs/weed/credential/memory"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3err"
)

func TestGetRequestDataReader_ChunkedEncodingWithoutIAM(t *testing.T) {
	// Create an S3ApiServer with IAM disabled
	s3a := &S3ApiServer{
		iam: NewIdentityAccessManagementWithStore(&S3ApiServerOption{}, nil, string(credential.StoreTypeMemory)),
	}
	// Ensure IAM is disabled for this test
	s3a.iam.isAuthEnabled = false

	tests := []struct {
		name          string
		contentSha256 string
		expectedError s3err.ErrorCode
		shouldProcess bool
		description   string
	}{
		{
			name:          "RegularRequest",
			contentSha256: "",
			expectedError: s3err.ErrNone,
			shouldProcess: false,
			description:   "Regular requests without chunked encoding should pass through unchanged",
		},
		{
			name:          "StreamingSignedWithoutIAM",
			contentSha256: "STREAMING-AWS4-HMAC-SHA256-PAYLOAD",
			expectedError: s3err.ErrAuthNotSetup,
			shouldProcess: false,
			description:   "Streaming signed requests should fail when IAM is disabled",
		},
		{
			name:          "StreamingUnsignedWithoutIAM",
			contentSha256: "STREAMING-UNSIGNED-PAYLOAD-TRAILER",
			expectedError: s3err.ErrNone,
			shouldProcess: true,
			description:   "Streaming unsigned requests should be processed even when IAM is disabled",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			body := strings.NewReader("test data")
			req, _ := http.NewRequest("PUT", "/bucket/key", body)

			if tt.contentSha256 != "" {
				req.Header.Set("x-amz-content-sha256", tt.contentSha256)
			}

			dataReader, errCode := getRequestDataReader(s3a, req)

			// Check error code
			if errCode != tt.expectedError {
				t.Errorf("Expected error code %v, got %v", tt.expectedError, errCode)
			}

			// For successful cases, check if processing occurred
			if errCode == s3err.ErrNone {
				if tt.shouldProcess {
					// For chunked requests, the reader should be different from the original body
					if dataReader == req.Body {
						t.Error("Expected dataReader to be processed by newChunkedReader, but got raw request body")
					}
				} else {
					// For regular requests, the reader should be the same as the original body
					if dataReader != req.Body {
						t.Error("Expected dataReader to be the same as request body for regular requests")
					}
				}
			}

			t.Logf("Test case: %s - %s", tt.name, tt.description)
		})
	}
}

func TestGetRequestDataReader_AuthTypeDetection(t *testing.T) {
	// Create an S3ApiServer with IAM disabled
	s3a := &S3ApiServer{
		iam: NewIdentityAccessManagementWithStore(&S3ApiServerOption{}, nil, string(credential.StoreTypeMemory)),
	}
	s3a.iam.isAuthEnabled = false

	// Test the specific case mentioned in the issue where chunked data
	// with checksum headers would be stored incorrectly
	t.Run("ChunkedDataWithChecksum", func(t *testing.T) {
		// Simulate a request with chunked data and checksum trailer
		body := strings.NewReader("test content")
		req, _ := http.NewRequest("PUT", "/bucket/key", body)
		req.Header.Set("x-amz-content-sha256", "STREAMING-UNSIGNED-PAYLOAD-TRAILER")
		req.Header.Set("x-amz-trailer", "x-amz-checksum-crc32")

		// Verify the auth type is detected correctly
		authType := getRequestAuthType(req)
		if authType != authTypeStreamingUnsigned {
			t.Errorf("Expected authTypeStreamingUnsigned, got %v", authType)
		}

		// Verify the request is processed correctly
		dataReader, errCode := getRequestDataReader(s3a, req)
		if errCode != s3err.ErrNone {
			t.Errorf("Expected no error, got %v", errCode)
		}

		// The dataReader should be processed by newChunkedReader
		if dataReader == req.Body {
			t.Error("Expected dataReader to be processed by newChunkedReader to handle chunked encoding")
		}
	})
}

func TestGetRequestDataReader_IAMEnabled(t *testing.T) {
	// Create an S3ApiServer with IAM enabled
	s3a := &S3ApiServer{
		iam: NewIdentityAccessManagementWithStore(&S3ApiServerOption{}, nil, string(credential.StoreTypeMemory)),
	}
	s3a.iam.isAuthEnabled = true

	t.Run("StreamingUnsignedWithIAMEnabled", func(t *testing.T) {
		body := strings.NewReader("test data")
		req, _ := http.NewRequest("PUT", "/bucket/key", body)
		req.Header.Set("x-amz-content-sha256", "STREAMING-UNSIGNED-PAYLOAD-TRAILER")

		dataReader, errCode := getRequestDataReader(s3a, req)

		// Should succeed and be processed
		if errCode != s3err.ErrNone {
			t.Errorf("Expected no error, got %v", errCode)
		}

		// Should be processed by newChunkedReader
		if dataReader == req.Body {
			t.Error("Expected dataReader to be processed by newChunkedReader")
		}
	})
}

// Test helper to verify auth type detection works correctly
func TestAuthTypeDetection(t *testing.T) {
	tests := []struct {
		name         string
		headers      map[string]string
		expectedType authType
	}{
		{
			name:         "StreamingUnsigned",
			headers:      map[string]string{"x-amz-content-sha256": "STREAMING-UNSIGNED-PAYLOAD-TRAILER"},
			expectedType: authTypeStreamingUnsigned,
		},
		{
			name:         "StreamingSigned",
			headers:      map[string]string{"x-amz-content-sha256": "STREAMING-AWS4-HMAC-SHA256-PAYLOAD"},
			expectedType: authTypeStreamingSigned,
		},
		{
			name:         "Regular",
			headers:      map[string]string{},
			expectedType: authTypeAnonymous,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req, _ := http.NewRequest("PUT", "/bucket/key", strings.NewReader("test"))
			for key, value := range tt.headers {
				req.Header.Set(key, value)
			}

			authType := getRequestAuthType(req)
			if authType != tt.expectedType {
				t.Errorf("Expected auth type %v, got %v", tt.expectedType, authType)
			}
		})
	}
}

func sha256Hex(t *testing.T, b []byte) string {
	t.Helper()
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

func TestExpectedContentSha256(t *testing.T) {
	good := sha256Hex(t, []byte("payload"))

	tests := []struct {
		name      string
		header    string
		wantValid bool
		wantNil   bool
	}{
		{"absent", "", true, true},
		{"unsigned", unsignedPayload, true, true},
		{"streaming signed", streamingContentSHA256, true, true},
		{"streaming trailer", streamingContentSHA256Trailer, true, true},
		{"streaming unsigned", streamingUnsignedPayload, true, true},
		{"hex", good, true, false},
		{"not hex", "nothex", false, true},
		{"short hex", "deadbeef", false, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := httptest.NewRequest("PUT", "/b/o", nil)
			if tt.header != "" {
				r.Header.Set("X-Amz-Content-Sha256", tt.header)
			}
			got, valid := expectedContentSha256(r)
			if valid != tt.wantValid {
				t.Fatalf("valid=%v want %v", valid, tt.wantValid)
			}
			if (got == nil) != tt.wantNil {
				t.Fatalf("expected nil=%v, got %x", tt.wantNil, got)
			}
		})
	}

	t.Run("base64 decodes to sha256", func(t *testing.T) {
		sum := sha256.Sum256([]byte("payload"))
		r := httptest.NewRequest("PUT", "/b/o", nil)
		r.Header.Set("X-Amz-Content-Sha256", base64.StdEncoding.EncodeToString(sum[:]))
		got, valid := expectedContentSha256(r)
		if !valid || !bytes.Equal(got, sum[:]) {
			t.Fatalf("valid=%v got=%x", valid, got)
		}
	})
}

func newVerifier(body string, expectedHex string) *contentSha256Verifier {
	expected, _ := hex.DecodeString(expectedHex)
	return &contentSha256Verifier{
		reader:   io.NopCloser(strings.NewReader(body)),
		hasher:   sha256.New(),
		expected: expected,
	}
}

func TestContentSha256VerifierMatch(t *testing.T) {
	v := newVerifier("hello", sha256Hex(t, []byte("hello")))
	got, err := io.ReadAll(v)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if string(got) != "hello" {
		t.Fatalf("got %q", got)
	}
}

func TestContentSha256VerifierMismatch(t *testing.T) {
	v := newVerifier("hello", sha256Hex(t, []byte("other")))
	_, err := io.ReadAll(v)
	if err == nil || !strings.Contains(err.Error(), "does not match") {
		t.Fatalf("expected mismatch error, got %v", err)
	}
	if _, err := v.Read(make([]byte, 8)); err == nil {
		t.Fatal("expected error to persist")
	}
}

// mimeDetect performs a single Read and ignores its error; the verifier must
// defer the mismatch so a dropped (n>0, err) result still surfaces later.
func TestContentSha256VerifierSurvivesDroppedError(t *testing.T) {
	v := newVerifier("hello", sha256Hex(t, []byte("other")))
	buf := make([]byte, 512)
	n, _ := v.Read(buf)
	rest := io.MultiReader(bytes.NewReader(buf[:n]), v)
	_, err := io.ReadAll(rest)
	if err == nil || !strings.Contains(err.Error(), "does not match") {
		t.Fatalf("expected deferred mismatch error, got %v", err)
	}
}

// A zero-length body never reaches the verifier, so the declared hash is
// compared against the empty-payload digest up front.
func TestGetRequestDataReaderEmptyBodyHash(t *testing.T) {
	s3a := &S3ApiServer{
		iam: NewIdentityAccessManagementWithStore(&S3ApiServerOption{}, nil, string(credential.StoreTypeMemory)),
	}
	s3a.iam.isAuthEnabled = false

	mismatch := httptest.NewRequest("PUT", "/b/dir/", http.NoBody)
	mismatch.Header.Set("X-Amz-Content-Sha256", sha256Hex(t, []byte("other")))
	if _, code := getRequestDataReader(s3a, mismatch); code != s3err.ErrContentSHA256Mismatch {
		t.Fatalf("empty body with wrong hash: code=%v", code)
	}

	malformed := httptest.NewRequest("PUT", "/b/o", strings.NewReader("x"))
	malformed.Header.Set("X-Amz-Content-Sha256", "nothex")
	if _, code := getRequestDataReader(s3a, malformed); code != s3err.ErrInvalidArgument {
		t.Fatalf("malformed sha256 header: code=%v", code)
	}

	emptySum := sha256.Sum256(nil)
	match := httptest.NewRequest("PUT", "/b/dir/", http.NoBody)
	match.Header.Set("X-Amz-Content-Sha256", hex.EncodeToString(emptySum[:]))
	if _, code := getRequestDataReader(s3a, match); code != s3err.ErrNone {
		t.Fatalf("empty body with empty hash: code=%v", code)
	}
}
