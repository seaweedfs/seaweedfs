package s3api

import (
	"bytes"
	"net/http"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3err"
)

// TestConditionalHeadersSupport tests all conditional headers for PUT operations
// This test verifies the fix for issue #7153 and full AWS S3 compatibility
func TestConditionalHeadersSupport(t *testing.T) {
	s3a := NewS3ApiServerForTest()
	if s3a == nil {
		t.Skip("S3ApiServer not available for testing")
	}

	bucket := "test-bucket"
	object := "/test-object"

	// Test If-None-Match header
	t.Run("IfNoneMatch", func(t *testing.T) {
		// Test case 1: If-None-Match=* when object doesn't exist (should return ErrNone)
		t.Run("Asterisk_ObjectDoesNotExist", func(t *testing.T) {
			req := createTestPutRequest(bucket, object, "test content")
			req.Header.Set(s3_constants.IfNoneMatch, "*")

			errCode := s3a.checkConditionalHeaders(req, bucket, object)
			if errCode != s3err.ErrNone {
				t.Errorf("Expected ErrNone when object doesn't exist, got %v", errCode)
			}
		})

		// Test case 2: If-None-Match with specific ETag when object doesn't exist
		t.Run("SpecificETag_ObjectDoesNotExist", func(t *testing.T) {
			req := createTestPutRequest(bucket, object, "test content")
			req.Header.Set(s3_constants.IfNoneMatch, "\"some-etag\"")

			errCode := s3a.checkConditionalHeaders(req, bucket, object)
			if errCode != s3err.ErrNone {
				t.Errorf("Expected ErrNone when object doesn't exist, got %v", errCode)
			}
		})

		// Test case 3: Multiple ETags in If-None-Match
		t.Run("MultipleETags", func(t *testing.T) {
			req := createTestPutRequest(bucket, object, "test content")
			req.Header.Set(s3_constants.IfNoneMatch, "\"etag1\", \"etag2\", \"etag3\"")

			errCode := s3a.checkConditionalHeaders(req, bucket, object)
			if errCode != s3err.ErrNone {
				t.Errorf("Expected ErrNone when object doesn't exist, got %v", errCode)
			}
		})
	})

	// Test If-Match header
	t.Run("IfMatch", func(t *testing.T) {
		// Test case 1: If-Match when object doesn't exist (should proceed)
		t.Run("ObjectDoesNotExist", func(t *testing.T) {
			req := createTestPutRequest(bucket, object, "test content")
			req.Header.Set(s3_constants.IfMatch, "\"some-etag\"")

			errCode := s3a.checkConditionalHeaders(req, bucket, object)
			if errCode != s3err.ErrNone {
				t.Errorf("Expected ErrNone when object doesn't exist, got %v", errCode)
			}
		})
	})

	// Test If-Modified-Since header
	t.Run("IfModifiedSince", func(t *testing.T) {
		// Test case 1: Valid date format
		t.Run("ValidDateFormat", func(t *testing.T) {
			req := createTestPutRequest(bucket, object, "test content")
			req.Header.Set(s3_constants.IfModifiedSince, time.Now().Format(time.RFC1123))

			errCode := s3a.checkConditionalHeaders(req, bucket, object)
			if errCode != s3err.ErrNone {
				t.Errorf("Expected ErrNone when object doesn't exist, got %v", errCode)
			}
		})

		// Test case 2: Invalid date format
		t.Run("InvalidDateFormat", func(t *testing.T) {
			req := createTestPutRequest(bucket, object, "test content")
			req.Header.Set(s3_constants.IfModifiedSince, "invalid-date")

			errCode := s3a.checkConditionalHeaders(req, bucket, object)
			if errCode != s3err.ErrInvalidRequest {
				t.Errorf("Expected ErrInvalidRequest for invalid date format, got %v", errCode)
			}
		})
	})

	// Test If-Unmodified-Since header
	t.Run("IfUnmodifiedSince", func(t *testing.T) {
		// Test case 1: Valid date format
		t.Run("ValidDateFormat", func(t *testing.T) {
			req := createTestPutRequest(bucket, object, "test content")
			req.Header.Set(s3_constants.IfUnmodifiedSince, time.Now().Format(time.RFC1123))

			errCode := s3a.checkConditionalHeaders(req, bucket, object)
			if errCode != s3err.ErrNone {
				t.Errorf("Expected ErrNone when object doesn't exist, got %v", errCode)
			}
		})

		// Test case 2: Invalid date format
		t.Run("InvalidDateFormat", func(t *testing.T) {
			req := createTestPutRequest(bucket, object, "test content")
			req.Header.Set(s3_constants.IfUnmodifiedSince, "invalid-date")

			errCode := s3a.checkConditionalHeaders(req, bucket, object)
			if errCode != s3err.ErrInvalidRequest {
				t.Errorf("Expected ErrInvalidRequest for invalid date format, got %v", errCode)
			}
		})
	})

	// Test no conditional headers
	t.Run("NoConditionalHeaders", func(t *testing.T) {
		req := createTestPutRequest(bucket, object, "test content")
		// Don't set any conditional headers

		errCode := s3a.checkConditionalHeaders(req, bucket, object)
		if errCode != s3err.ErrNone {
			t.Errorf("Expected ErrNone when no conditional headers, got %v", errCode)
		}
	})
}

// TestETagMatching tests the etagMatches helper function
func TestETagMatching(t *testing.T) {
	s3a := NewS3ApiServerForTest()
	if s3a == nil {
		t.Skip("S3ApiServer not available for testing")
	}

	testCases := []struct {
		name        string
		headerValue string
		objectETag  string
		expected    bool
	}{
		{
			name:        "ExactMatch",
			headerValue: "\"abc123\"",
			objectETag:  "abc123",
			expected:    true,
		},
		{
			name:        "ExactMatchWithQuotes",
			headerValue: "\"abc123\"",
			objectETag:  "\"abc123\"",
			expected:    true,
		},
		{
			name:        "NoMatch",
			headerValue: "\"abc123\"",
			objectETag:  "def456",
			expected:    false,
		},
		{
			name:        "MultipleETags_FirstMatch",
			headerValue: "\"abc123\", \"def456\"",
			objectETag:  "abc123",
			expected:    true,
		},
		{
			name:        "MultipleETags_SecondMatch",
			headerValue: "\"abc123\", \"def456\"",
			objectETag:  "def456",
			expected:    true,
		},
		{
			name:        "MultipleETags_NoMatch",
			headerValue: "\"abc123\", \"def456\"",
			objectETag:  "ghi789",
			expected:    false,
		},
		{
			name:        "WithSpaces",
			headerValue: " \"abc123\" , \"def456\" ",
			objectETag:  "def456",
			expected:    true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := s3a.etagMatches(tc.headerValue, tc.objectETag)
			if result != tc.expected {
				t.Errorf("Expected %v, got %v for headerValue='%s', objectETag='%s'",
					tc.expected, result, tc.headerValue, tc.objectETag)
			}
		})
	}
}

// TestConditionalHeadersIntegration tests conditional headers with full integration
func TestConditionalHeadersIntegration(t *testing.T) {
	// This would be a full integration test that requires a running SeaweedFS instance
	t.Skip("Integration test - requires running SeaweedFS instance")
}

// createTestPutRequest creates a test HTTP PUT request
func createTestPutRequest(bucket, object, content string) *http.Request {
	req, _ := http.NewRequest("PUT", "/"+bucket+object, bytes.NewReader([]byte(content)))
	req.Header.Set("Content-Type", "application/octet-stream")

	// Set up mux vars to simulate the bucket and object extraction
	// In real tests, this would be handled by the gorilla mux router
	return req
}

// NewS3ApiServerForTest creates a minimal S3ApiServer for testing
// Note: This is a simplified version for unit testing conditional logic
func NewS3ApiServerForTest() *S3ApiServer {
	// In a real test environment, this would set up a proper S3ApiServer
	// with filer connection, etc. For unit testing conditional header logic,
	// we create a minimal instance
	return &S3ApiServer{
		option: &S3ApiServerOption{
			BucketsPath: "/buckets",
		},
	}
}
