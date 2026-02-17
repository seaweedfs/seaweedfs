package s3api

import (
	"net/http"
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
