package s3api

import (
	"bytes"
	"encoding/hex"
	"fmt"
	"net/http"
	"net/url"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3err"
)

// TestConditionalHeadersWithExistingObjects tests conditional headers against existing objects
// This addresses the PR feedback about missing test coverage for object existence scenarios
func TestConditionalHeadersWithExistingObjects(t *testing.T) {
	bucket := "test-bucket"
	object := "/test-object"

	// Mock object with known ETag and modification time
	testObject := &filer_pb.Entry{
		Name: "test-object",
		Extended: map[string][]byte{
			s3_constants.ExtETagKey: []byte("\"abc123\""),
		},
		Attributes: &filer_pb.FuseAttributes{
			Mtime:    time.Date(2024, 6, 15, 12, 0, 0, 0, time.UTC).Unix(), // June 15, 2024
			FileSize: 1024,                                                 // Add file size
		},
		Chunks: []*filer_pb.FileChunk{
			// Add a mock chunk to make calculateETagFromChunks work
			{
				FileId: "test-file-id",
				Offset: 0,
				Size:   1024,
			},
		},
	}

	// Test If-None-Match with existing object
	t.Run("IfNoneMatch_ObjectExists", func(t *testing.T) {
		// Test case 1: If-None-Match=* when object exists (should fail)
		t.Run("Asterisk_ShouldFail", func(t *testing.T) {
			getter := createMockEntryGetter(testObject)
			req := createTestPutRequest(bucket, object, "test content")
			req.Header.Set(s3_constants.IfNoneMatch, "*")

			s3a := NewS3ApiServerForTest()
			errCode := s3a.checkConditionalHeadersWithGetter(getter, req, bucket, object)
			if errCode != s3err.ErrPreconditionFailed {
				t.Errorf("Expected ErrPreconditionFailed when object exists with If-None-Match=*, got %v", errCode)
			}
		})

		// Test case 2: If-None-Match with matching ETag (should fail)
		t.Run("MatchingETag_ShouldFail", func(t *testing.T) {
			getter := createMockEntryGetter(testObject)
			req := createTestPutRequest(bucket, object, "test content")
			req.Header.Set(s3_constants.IfNoneMatch, "\"abc123\"")

			s3a := NewS3ApiServerForTest()
			errCode := s3a.checkConditionalHeadersWithGetter(getter, req, bucket, object)
			if errCode != s3err.ErrPreconditionFailed {
				t.Errorf("Expected ErrPreconditionFailed when ETag matches, got %v", errCode)
			}
		})

		// Test case 3: If-None-Match with non-matching ETag (should succeed)
		t.Run("NonMatchingETag_ShouldSucceed", func(t *testing.T) {
			getter := createMockEntryGetter(testObject)
			req := createTestPutRequest(bucket, object, "test content")
			req.Header.Set(s3_constants.IfNoneMatch, "\"xyz789\"")

			s3a := NewS3ApiServerForTest()
			errCode := s3a.checkConditionalHeadersWithGetter(getter, req, bucket, object)
			if errCode != s3err.ErrNone {
				t.Errorf("Expected ErrNone when ETag doesn't match, got %v", errCode)
			}
		})

		// Test case 4: If-None-Match with multiple ETags, one matching (should fail)
		t.Run("MultipleETags_OneMatches_ShouldFail", func(t *testing.T) {
			getter := createMockEntryGetter(testObject)
			req := createTestPutRequest(bucket, object, "test content")
			req.Header.Set(s3_constants.IfNoneMatch, "\"xyz789\", \"abc123\", \"def456\"")

			s3a := NewS3ApiServerForTest()
			errCode := s3a.checkConditionalHeadersWithGetter(getter, req, bucket, object)
			if errCode != s3err.ErrPreconditionFailed {
				t.Errorf("Expected ErrPreconditionFailed when one ETag matches, got %v", errCode)
			}
		})

		// Test case 5: If-None-Match with multiple ETags, none matching (should succeed)
		t.Run("MultipleETags_NoneMatch_ShouldSucceed", func(t *testing.T) {
			getter := createMockEntryGetter(testObject)
			req := createTestPutRequest(bucket, object, "test content")
			req.Header.Set(s3_constants.IfNoneMatch, "\"xyz789\", \"def456\", \"ghi123\"")

			s3a := NewS3ApiServerForTest()
			errCode := s3a.checkConditionalHeadersWithGetter(getter, req, bucket, object)
			if errCode != s3err.ErrNone {
				t.Errorf("Expected ErrNone when no ETags match, got %v", errCode)
			}
		})
	})

	// Test If-Match with existing object
	t.Run("IfMatch_ObjectExists", func(t *testing.T) {
		// Test case 1: If-Match with matching ETag (should succeed)
		t.Run("MatchingETag_ShouldSucceed", func(t *testing.T) {
			getter := createMockEntryGetter(testObject)
			req := createTestPutRequest(bucket, object, "test content")
			req.Header.Set(s3_constants.IfMatch, "\"abc123\"")

			s3a := NewS3ApiServerForTest()
			errCode := s3a.checkConditionalHeadersWithGetter(getter, req, bucket, object)
			if errCode != s3err.ErrNone {
				t.Errorf("Expected ErrNone when ETag matches, got %v", errCode)
			}
		})

		// Test case 2: If-Match with non-matching ETag (should fail)
		t.Run("NonMatchingETag_ShouldFail", func(t *testing.T) {
			getter := createMockEntryGetter(testObject)
			req := createTestPutRequest(bucket, object, "test content")
			req.Header.Set(s3_constants.IfMatch, "\"xyz789\"")

			s3a := NewS3ApiServerForTest()
			errCode := s3a.checkConditionalHeadersWithGetter(getter, req, bucket, object)
			if errCode != s3err.ErrPreconditionFailed {
				t.Errorf("Expected ErrPreconditionFailed when ETag doesn't match, got %v", errCode)
			}
		})

		// Test case 3: If-Match with multiple ETags, one matching (should succeed)
		t.Run("MultipleETags_OneMatches_ShouldSucceed", func(t *testing.T) {
			getter := createMockEntryGetter(testObject)
			req := createTestPutRequest(bucket, object, "test content")
			req.Header.Set(s3_constants.IfMatch, "\"xyz789\", \"abc123\"")

			s3a := NewS3ApiServerForTest()
			errCode := s3a.checkConditionalHeadersWithGetter(getter, req, bucket, object)
			if errCode != s3err.ErrNone {
				t.Errorf("Expected ErrNone when one ETag matches, got %v", errCode)
			}
		})

		// Test case 4: If-Match with wildcard * (should succeed if object exists)
		t.Run("Wildcard_ShouldSucceed", func(t *testing.T) {
			getter := createMockEntryGetter(testObject)
			req := createTestPutRequest(bucket, object, "test content")
			req.Header.Set(s3_constants.IfMatch, "*")

			s3a := NewS3ApiServerForTest()
			errCode := s3a.checkConditionalHeadersWithGetter(getter, req, bucket, object)
			if errCode != s3err.ErrNone {
				t.Errorf("Expected ErrNone when If-Match=* and object exists, got %v", errCode)
			}
		})
	})

	// Test If-Modified-Since with existing object
	t.Run("IfModifiedSince_ObjectExists", func(t *testing.T) {
		// Test case 1: If-Modified-Since with date before object modification (should succeed)
		t.Run("DateBefore_ShouldSucceed", func(t *testing.T) {
			getter := createMockEntryGetter(testObject)
			req := createTestPutRequest(bucket, object, "test content")
			dateBeforeModification := time.Date(2024, 6, 14, 12, 0, 0, 0, time.UTC)
			req.Header.Set(s3_constants.IfModifiedSince, dateBeforeModification.Format(time.RFC1123))

			s3a := NewS3ApiServerForTest()
			errCode := s3a.checkConditionalHeadersWithGetter(getter, req, bucket, object)
			if errCode != s3err.ErrNone {
				t.Errorf("Expected ErrNone when object was modified after date, got %v", errCode)
			}
		})

		// Test case 2: If-Modified-Since with date after object modification (should fail)
		t.Run("DateAfter_ShouldFail", func(t *testing.T) {
			getter := createMockEntryGetter(testObject)
			req := createTestPutRequest(bucket, object, "test content")
			dateAfterModification := time.Date(2024, 6, 16, 12, 0, 0, 0, time.UTC)
			req.Header.Set(s3_constants.IfModifiedSince, dateAfterModification.Format(time.RFC1123))

			s3a := NewS3ApiServerForTest()
			errCode := s3a.checkConditionalHeadersWithGetter(getter, req, bucket, object)
			if errCode != s3err.ErrPreconditionFailed {
				t.Errorf("Expected ErrPreconditionFailed when object wasn't modified since date, got %v", errCode)
			}
		})

		// Test case 3: If-Modified-Since with exact modification date (should fail - not after)
		t.Run("ExactDate_ShouldFail", func(t *testing.T) {
			getter := createMockEntryGetter(testObject)
			req := createTestPutRequest(bucket, object, "test content")
			exactDate := time.Date(2024, 6, 15, 12, 0, 0, 0, time.UTC)
			req.Header.Set(s3_constants.IfModifiedSince, exactDate.Format(time.RFC1123))

			s3a := NewS3ApiServerForTest()
			errCode := s3a.checkConditionalHeadersWithGetter(getter, req, bucket, object)
			if errCode != s3err.ErrPreconditionFailed {
				t.Errorf("Expected ErrPreconditionFailed when object modification time equals header date, got %v", errCode)
			}
		})
	})

	// Test If-Unmodified-Since with existing object
	t.Run("IfUnmodifiedSince_ObjectExists", func(t *testing.T) {
		// Test case 1: If-Unmodified-Since with date after object modification (should succeed)
		t.Run("DateAfter_ShouldSucceed", func(t *testing.T) {
			getter := createMockEntryGetter(testObject)
			req := createTestPutRequest(bucket, object, "test content")
			dateAfterModification := time.Date(2024, 6, 16, 12, 0, 0, 0, time.UTC)
			req.Header.Set(s3_constants.IfUnmodifiedSince, dateAfterModification.Format(time.RFC1123))

			s3a := NewS3ApiServerForTest()
			errCode := s3a.checkConditionalHeadersWithGetter(getter, req, bucket, object)
			if errCode != s3err.ErrNone {
				t.Errorf("Expected ErrNone when object wasn't modified after date, got %v", errCode)
			}
		})

		// Test case 2: If-Unmodified-Since with date before object modification (should fail)
		t.Run("DateBefore_ShouldFail", func(t *testing.T) {
			getter := createMockEntryGetter(testObject)
			req := createTestPutRequest(bucket, object, "test content")
			dateBeforeModification := time.Date(2024, 6, 14, 12, 0, 0, 0, time.UTC)
			req.Header.Set(s3_constants.IfUnmodifiedSince, dateBeforeModification.Format(time.RFC1123))

			s3a := NewS3ApiServerForTest()
			errCode := s3a.checkConditionalHeadersWithGetter(getter, req, bucket, object)
			if errCode != s3err.ErrPreconditionFailed {
				t.Errorf("Expected ErrPreconditionFailed when object was modified after date, got %v", errCode)
			}
		})
	})
}

// TestConditionalHeadersForReads tests conditional headers for read operations (GET, HEAD)
// This implements AWS S3 conditional reads behavior where different conditions return different status codes
// See: https://docs.aws.amazon.com/AmazonS3/latest/userguide/conditional-reads.html
func TestConditionalHeadersForReads(t *testing.T) {
	bucket := "test-bucket"
	object := "/test-read-object"

	// Mock existing object to test conditional headers against
	existingObject := &filer_pb.Entry{
		Name: "test-read-object",
		Extended: map[string][]byte{
			s3_constants.ExtETagKey: []byte("\"read123\""),
		},
		Attributes: &filer_pb.FuseAttributes{
			Mtime:    time.Date(2024, 6, 15, 12, 0, 0, 0, time.UTC).Unix(),
			FileSize: 1024,
		},
		Chunks: []*filer_pb.FileChunk{
			{
				FileId: "read-file-id",
				Offset: 0,
				Size:   1024,
			},
		},
	}

	// Test conditional reads with existing object
	t.Run("ConditionalReads_ObjectExists", func(t *testing.T) {
		// Test If-None-Match with existing object (should return 304 Not Modified)
		t.Run("IfNoneMatch_ObjectExists_ShouldReturn304", func(t *testing.T) {
			getter := createMockEntryGetter(existingObject)

			req := createTestGetRequest(bucket, object)
			req.Header.Set(s3_constants.IfNoneMatch, "\"read123\"")

			s3a := NewS3ApiServerForTest()
			errCode := s3a.checkConditionalHeadersForReadsWithGetter(getter, req, bucket, object)
			if errCode.ErrorCode != s3err.ErrNotModified {
				t.Errorf("Expected ErrNotModified when If-None-Match matches, got %v", errCode)
			}
		})

		// Test If-None-Match=* with existing object (should return 304 Not Modified)
		t.Run("IfNoneMatchAsterisk_ObjectExists_ShouldReturn304", func(t *testing.T) {
			getter := createMockEntryGetter(existingObject)

			req := createTestGetRequest(bucket, object)
			req.Header.Set(s3_constants.IfNoneMatch, "*")

			s3a := NewS3ApiServerForTest()
			errCode := s3a.checkConditionalHeadersForReadsWithGetter(getter, req, bucket, object)
			if errCode.ErrorCode != s3err.ErrNotModified {
				t.Errorf("Expected ErrNotModified when If-None-Match=* with existing object, got %v", errCode)
			}
		})

		// Test If-None-Match with non-matching ETag (should succeed)
		t.Run("IfNoneMatch_NonMatchingETag_ShouldSucceed", func(t *testing.T) {
			getter := createMockEntryGetter(existingObject)

			req := createTestGetRequest(bucket, object)
			req.Header.Set(s3_constants.IfNoneMatch, "\"different-etag\"")

			s3a := NewS3ApiServerForTest()
			errCode := s3a.checkConditionalHeadersForReadsWithGetter(getter, req, bucket, object)
			if errCode.ErrorCode != s3err.ErrNone {
				t.Errorf("Expected ErrNone when If-None-Match doesn't match, got %v", errCode)
			}
		})

		// Test If-Match with matching ETag (should succeed)
		t.Run("IfMatch_MatchingETag_ShouldSucceed", func(t *testing.T) {
			getter := createMockEntryGetter(existingObject)

			req := createTestGetRequest(bucket, object)
			req.Header.Set(s3_constants.IfMatch, "\"read123\"")

			s3a := NewS3ApiServerForTest()
			errCode := s3a.checkConditionalHeadersForReadsWithGetter(getter, req, bucket, object)
			if errCode.ErrorCode != s3err.ErrNone {
				t.Errorf("Expected ErrNone when If-Match matches, got %v", errCode)
			}
		})

		// Test If-Match with non-matching ETag (should return 412 Precondition Failed)
		t.Run("IfMatch_NonMatchingETag_ShouldReturn412", func(t *testing.T) {
			getter := createMockEntryGetter(existingObject)

			req := createTestGetRequest(bucket, object)
			req.Header.Set(s3_constants.IfMatch, "\"different-etag\"")

			s3a := NewS3ApiServerForTest()
			errCode := s3a.checkConditionalHeadersForReadsWithGetter(getter, req, bucket, object)
			if errCode.ErrorCode != s3err.ErrPreconditionFailed {
				t.Errorf("Expected ErrPreconditionFailed when If-Match doesn't match, got %v", errCode)
			}
		})

		// Test If-Match=* with existing object (should succeed)
		t.Run("IfMatchAsterisk_ObjectExists_ShouldSucceed", func(t *testing.T) {
			getter := createMockEntryGetter(existingObject)

			req := createTestGetRequest(bucket, object)
			req.Header.Set(s3_constants.IfMatch, "*")

			s3a := NewS3ApiServerForTest()
			errCode := s3a.checkConditionalHeadersForReadsWithGetter(getter, req, bucket, object)
			if errCode.ErrorCode != s3err.ErrNone {
				t.Errorf("Expected ErrNone when If-Match=* with existing object, got %v", errCode)
			}
		})

		// Test If-Modified-Since (object modified after date - should succeed)
		t.Run("IfModifiedSince_ObjectModifiedAfter_ShouldSucceed", func(t *testing.T) {
			getter := createMockEntryGetter(existingObject)

			req := createTestGetRequest(bucket, object)
			req.Header.Set(s3_constants.IfModifiedSince, "Sat, 14 Jun 2024 12:00:00 GMT") // Before object mtime

			s3a := NewS3ApiServerForTest()
			errCode := s3a.checkConditionalHeadersForReadsWithGetter(getter, req, bucket, object)
			if errCode.ErrorCode != s3err.ErrNone {
				t.Errorf("Expected ErrNone when object modified after If-Modified-Since date, got %v", errCode)
			}
		})

		// Test If-Modified-Since (object not modified since date - should return 304)
		t.Run("IfModifiedSince_ObjectNotModified_ShouldReturn304", func(t *testing.T) {
			getter := createMockEntryGetter(existingObject)

			req := createTestGetRequest(bucket, object)
			req.Header.Set(s3_constants.IfModifiedSince, "Sun, 16 Jun 2024 12:00:00 GMT") // After object mtime

			s3a := NewS3ApiServerForTest()
			errCode := s3a.checkConditionalHeadersForReadsWithGetter(getter, req, bucket, object)
			if errCode.ErrorCode != s3err.ErrNotModified {
				t.Errorf("Expected ErrNotModified when object not modified since If-Modified-Since date, got %v", errCode)
			}
		})

		// Test If-Unmodified-Since (object not modified since date - should succeed)
		t.Run("IfUnmodifiedSince_ObjectNotModified_ShouldSucceed", func(t *testing.T) {
			getter := createMockEntryGetter(existingObject)

			req := createTestGetRequest(bucket, object)
			req.Header.Set(s3_constants.IfUnmodifiedSince, "Sun, 16 Jun 2024 12:00:00 GMT") // After object mtime

			s3a := NewS3ApiServerForTest()
			errCode := s3a.checkConditionalHeadersForReadsWithGetter(getter, req, bucket, object)
			if errCode.ErrorCode != s3err.ErrNone {
				t.Errorf("Expected ErrNone when object not modified since If-Unmodified-Since date, got %v", errCode)
			}
		})

		// Test If-Unmodified-Since (object modified since date - should return 412)
		t.Run("IfUnmodifiedSince_ObjectModified_ShouldReturn412", func(t *testing.T) {
			getter := createMockEntryGetter(existingObject)

			req := createTestGetRequest(bucket, object)
			req.Header.Set(s3_constants.IfUnmodifiedSince, "Fri, 14 Jun 2024 12:00:00 GMT") // Before object mtime

			s3a := NewS3ApiServerForTest()
			errCode := s3a.checkConditionalHeadersForReadsWithGetter(getter, req, bucket, object)
			if errCode.ErrorCode != s3err.ErrPreconditionFailed {
				t.Errorf("Expected ErrPreconditionFailed when object modified since If-Unmodified-Since date, got %v", errCode)
			}
		})
	})

	// Test conditional reads with non-existent object
	t.Run("ConditionalReads_ObjectNotExists", func(t *testing.T) {
		// Test If-None-Match with non-existent object (should succeed)
		t.Run("IfNoneMatch_ObjectNotExists_ShouldSucceed", func(t *testing.T) {
			getter := createMockEntryGetter(nil) // No object

			req := createTestGetRequest(bucket, object)
			req.Header.Set(s3_constants.IfNoneMatch, "\"any-etag\"")

			s3a := NewS3ApiServerForTest()
			errCode := s3a.checkConditionalHeadersForReadsWithGetter(getter, req, bucket, object)
			if errCode.ErrorCode != s3err.ErrNone {
				t.Errorf("Expected ErrNone when object doesn't exist with If-None-Match, got %v", errCode)
			}
		})

		// Test If-Match with non-existent object (should return 412)
		t.Run("IfMatch_ObjectNotExists_ShouldReturn412", func(t *testing.T) {
			getter := createMockEntryGetter(nil) // No object

			req := createTestGetRequest(bucket, object)
			req.Header.Set(s3_constants.IfMatch, "\"any-etag\"")

			s3a := NewS3ApiServerForTest()
			errCode := s3a.checkConditionalHeadersForReadsWithGetter(getter, req, bucket, object)
			if errCode.ErrorCode != s3err.ErrPreconditionFailed {
				t.Errorf("Expected ErrPreconditionFailed when object doesn't exist with If-Match, got %v", errCode)
			}
		})

		// Test If-Modified-Since with non-existent object (should succeed)
		t.Run("IfModifiedSince_ObjectNotExists_ShouldSucceed", func(t *testing.T) {
			getter := createMockEntryGetter(nil) // No object

			req := createTestGetRequest(bucket, object)
			req.Header.Set(s3_constants.IfModifiedSince, "Sat, 15 Jun 2024 12:00:00 GMT")

			s3a := NewS3ApiServerForTest()
			errCode := s3a.checkConditionalHeadersForReadsWithGetter(getter, req, bucket, object)
			if errCode.ErrorCode != s3err.ErrNone {
				t.Errorf("Expected ErrNone when object doesn't exist with If-Modified-Since, got %v", errCode)
			}
		})

		// Test If-Unmodified-Since with non-existent object (should return 412)
		t.Run("IfUnmodifiedSince_ObjectNotExists_ShouldReturn412", func(t *testing.T) {
			getter := createMockEntryGetter(nil) // No object

			req := createTestGetRequest(bucket, object)
			req.Header.Set(s3_constants.IfUnmodifiedSince, "Sat, 15 Jun 2024 12:00:00 GMT")

			s3a := NewS3ApiServerForTest()
			errCode := s3a.checkConditionalHeadersForReadsWithGetter(getter, req, bucket, object)
			if errCode.ErrorCode != s3err.ErrPreconditionFailed {
				t.Errorf("Expected ErrPreconditionFailed when object doesn't exist with If-Unmodified-Since, got %v", errCode)
			}
		})
	})
}

// Helper function to create a GET request for testing
func createTestGetRequest(bucket, object string) *http.Request {
	return &http.Request{
		Method: "GET",
		Header: make(http.Header),
		URL: &url.URL{
			Path: fmt.Sprintf("/%s/%s", bucket, object),
		},
	}
}

// TestConditionalHeadersWithNonExistentObjects tests the original scenarios (object doesn't exist)
func TestConditionalHeadersWithNonExistentObjects(t *testing.T) {
	s3a := NewS3ApiServerForTest()
	if s3a == nil {
		t.Skip("S3ApiServer not available for testing")
	}

	bucket := "test-bucket"
	object := "/test-object"

	// Test If-None-Match header when object doesn't exist
	t.Run("IfNoneMatch_ObjectDoesNotExist", func(t *testing.T) {
		// Test case 1: If-None-Match=* when object doesn't exist (should return ErrNone)
		t.Run("Asterisk_ShouldSucceed", func(t *testing.T) {
			getter := createMockEntryGetter(nil) // No object exists
			req := createTestPutRequest(bucket, object, "test content")
			req.Header.Set(s3_constants.IfNoneMatch, "*")

			s3a := NewS3ApiServerForTest()
			errCode := s3a.checkConditionalHeadersWithGetter(getter, req, bucket, object)
			if errCode != s3err.ErrNone {
				t.Errorf("Expected ErrNone when object doesn't exist, got %v", errCode)
			}
		})

		// Test case 2: If-None-Match with specific ETag when object doesn't exist
		t.Run("SpecificETag_ShouldSucceed", func(t *testing.T) {
			getter := createMockEntryGetter(nil) // No object exists
			req := createTestPutRequest(bucket, object, "test content")
			req.Header.Set(s3_constants.IfNoneMatch, "\"some-etag\"")

			s3a := NewS3ApiServerForTest()
			errCode := s3a.checkConditionalHeadersWithGetter(getter, req, bucket, object)
			if errCode != s3err.ErrNone {
				t.Errorf("Expected ErrNone when object doesn't exist, got %v", errCode)
			}
		})
	})

	// Test If-Match header when object doesn't exist
	t.Run("IfMatch_ObjectDoesNotExist", func(t *testing.T) {
		// Test case 1: If-Match with specific ETag when object doesn't exist (should fail - critical bug fix)
		t.Run("SpecificETag_ShouldFail", func(t *testing.T) {
			getter := createMockEntryGetter(nil) // No object exists
			req := createTestPutRequest(bucket, object, "test content")
			req.Header.Set(s3_constants.IfMatch, "\"some-etag\"")

			s3a := NewS3ApiServerForTest()
			errCode := s3a.checkConditionalHeadersWithGetter(getter, req, bucket, object)
			if errCode != s3err.ErrPreconditionFailed {
				t.Errorf("Expected ErrPreconditionFailed when object doesn't exist with If-Match header, got %v", errCode)
			}
		})

		// Test case 2: If-Match with wildcard * when object doesn't exist (should fail)
		t.Run("Wildcard_ShouldFail", func(t *testing.T) {
			getter := createMockEntryGetter(nil) // No object exists
			req := createTestPutRequest(bucket, object, "test content")
			req.Header.Set(s3_constants.IfMatch, "*")

			s3a := NewS3ApiServerForTest()
			errCode := s3a.checkConditionalHeadersWithGetter(getter, req, bucket, object)
			if errCode != s3err.ErrPreconditionFailed {
				t.Errorf("Expected ErrPreconditionFailed when object doesn't exist with If-Match=*, got %v", errCode)
			}
		})
	})

	// Test date format validation (works regardless of object existence)
	t.Run("DateFormatValidation", func(t *testing.T) {
		// Test case 1: Valid If-Modified-Since date format
		t.Run("IfModifiedSince_ValidFormat", func(t *testing.T) {
			getter := createMockEntryGetter(nil) // No object exists
			req := createTestPutRequest(bucket, object, "test content")
			req.Header.Set(s3_constants.IfModifiedSince, time.Now().Format(time.RFC1123))

			s3a := NewS3ApiServerForTest()
			errCode := s3a.checkConditionalHeadersWithGetter(getter, req, bucket, object)
			if errCode != s3err.ErrNone {
				t.Errorf("Expected ErrNone with valid date format, got %v", errCode)
			}
		})

		// Test case 2: Invalid If-Modified-Since date format
		t.Run("IfModifiedSince_InvalidFormat", func(t *testing.T) {
			getter := createMockEntryGetter(nil) // No object exists
			req := createTestPutRequest(bucket, object, "test content")
			req.Header.Set(s3_constants.IfModifiedSince, "invalid-date")

			s3a := NewS3ApiServerForTest()
			errCode := s3a.checkConditionalHeadersWithGetter(getter, req, bucket, object)
			if errCode != s3err.ErrInvalidRequest {
				t.Errorf("Expected ErrInvalidRequest for invalid date format, got %v", errCode)
			}
		})

		// Test case 3: Invalid If-Unmodified-Since date format
		t.Run("IfUnmodifiedSince_InvalidFormat", func(t *testing.T) {
			getter := createMockEntryGetter(nil) // No object exists
			req := createTestPutRequest(bucket, object, "test content")
			req.Header.Set(s3_constants.IfUnmodifiedSince, "invalid-date")

			s3a := NewS3ApiServerForTest()
			errCode := s3a.checkConditionalHeadersWithGetter(getter, req, bucket, object)
			if errCode != s3err.ErrInvalidRequest {
				t.Errorf("Expected ErrInvalidRequest for invalid date format, got %v", errCode)
			}
		})
	})

	// Test no conditional headers
	t.Run("NoConditionalHeaders", func(t *testing.T) {
		getter := createMockEntryGetter(nil) // No object exists
		req := createTestPutRequest(bucket, object, "test content")
		// Don't set any conditional headers

		s3a := NewS3ApiServerForTest()
		errCode := s3a.checkConditionalHeadersWithGetter(getter, req, bucket, object)
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

// TestGetObjectETagWithMd5AndChunks tests the fix for issue #7274
// When an object has both Attributes.Md5 and multiple chunks, getObjectETag should
// prefer Attributes.Md5 to match the behavior of HeadObject and filer.ETag
func TestGetObjectETagWithMd5AndChunks(t *testing.T) {
	s3a := NewS3ApiServerForTest()
	if s3a == nil {
		t.Skip("S3ApiServer not available for testing")
	}

	// Create an object with both Md5 and multiple chunks (like in issue #7274)
	// Md5: ZjcmMwrCVGNVgb4HoqHe9g== (base64) = 663726330ac254635581be07a2a1def6 (hex)
	md5HexString := "663726330ac254635581be07a2a1def6"
	md5Bytes, err := hex.DecodeString(md5HexString)
	if err != nil {
		t.Fatalf("failed to decode md5 hex string: %v", err)
	}

	entry := &filer_pb.Entry{
		Name: "test-multipart-object",
		Attributes: &filer_pb.FuseAttributes{
			Mtime:    time.Now().Unix(),
			FileSize: 5597744,
			Md5:      md5Bytes,
		},
		// Two chunks - if we only used ETagChunks, it would return format "hash-2"
		Chunks: []*filer_pb.FileChunk{
			{
				FileId: "chunk1",
				Offset: 0,
				Size:   4194304,
				ETag:   "9+yCD2DGwMG5uKwAd+y04Q==",
			},
			{
				FileId: "chunk2",
				Offset: 4194304,
				Size:   1403440,
				ETag:   "cs6SVSTgZ8W3IbIrAKmklg==",
			},
		},
	}

	// getObjectETag should return the Md5 in hex with quotes
	expectedETag := "\"" + md5HexString + "\""
	actualETag := s3a.getObjectETag(entry)

	if actualETag != expectedETag {
		t.Errorf("Expected ETag %s, got %s", expectedETag, actualETag)
	}

	// Now test that conditional headers work with this ETag
	bucket := "test-bucket"
	object := "/test-object"

	// Test If-Match with the Md5-based ETag (should succeed)
	t.Run("IfMatch_WithMd5BasedETag_ShouldSucceed", func(t *testing.T) {
		getter := createMockEntryGetter(entry)
		req := createTestGetRequest(bucket, object)
		// Client sends the ETag from HeadObject (without quotes)
		req.Header.Set(s3_constants.IfMatch, md5HexString)

		result := s3a.checkConditionalHeadersForReadsWithGetter(getter, req, bucket, object)
		if result.ErrorCode != s3err.ErrNone {
			t.Errorf("Expected ErrNone when If-Match uses Md5-based ETag, got %v (ETag was %s)", result.ErrorCode, actualETag)
		}
	})

	// Test If-Match with chunk-based ETag format (should fail - this was the old incorrect behavior)
	t.Run("IfMatch_WithChunkBasedETag_ShouldFail", func(t *testing.T) {
		getter := createMockEntryGetter(entry)
		req := createTestGetRequest(bucket, object)
		// If we incorrectly calculated ETag from chunks, it would be in format "hash-2"
		req.Header.Set(s3_constants.IfMatch, "123294de680f28bde364b81477549f7d-2")

		result := s3a.checkConditionalHeadersForReadsWithGetter(getter, req, bucket, object)
		if result.ErrorCode != s3err.ErrPreconditionFailed {
			t.Errorf("Expected ErrPreconditionFailed when If-Match uses chunk-based ETag format, got %v", result.ErrorCode)
		}
	})
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

// MockEntryGetter implements the simplified EntryGetter interface for testing
// Only mocks the data access dependency - tests use production getObjectETag and etagMatches
type MockEntryGetter struct {
	mockEntry *filer_pb.Entry
}

// Implement only the simplified EntryGetter interface
func (m *MockEntryGetter) getEntry(parentDirectoryPath, entryName string) (*filer_pb.Entry, error) {
	if m.mockEntry != nil {
		return m.mockEntry, nil
	}
	return nil, filer_pb.ErrNotFound
}

// createMockEntryGetter creates a mock EntryGetter for testing
func createMockEntryGetter(mockEntry *filer_pb.Entry) *MockEntryGetter {
	return &MockEntryGetter{
		mockEntry: mockEntry,
	}
}

// TestConditionalHeadersMultipartUpload tests conditional headers with multipart uploads
// This verifies AWS S3 compatibility where conditional headers only apply to CompleteMultipartUpload
func TestConditionalHeadersMultipartUpload(t *testing.T) {
	bucket := "test-bucket"
	object := "/test-multipart-object"

	// Mock existing object to test conditional headers against
	existingObject := &filer_pb.Entry{
		Name: "test-multipart-object",
		Extended: map[string][]byte{
			s3_constants.ExtETagKey: []byte("\"existing123\""),
		},
		Attributes: &filer_pb.FuseAttributes{
			Mtime:    time.Date(2024, 6, 15, 12, 0, 0, 0, time.UTC).Unix(),
			FileSize: 2048,
		},
		Chunks: []*filer_pb.FileChunk{
			{
				FileId: "existing-file-id",
				Offset: 0,
				Size:   2048,
			},
		},
	}

	// Test CompleteMultipartUpload with If-None-Match: * (should fail when object exists)
	t.Run("CompleteMultipartUpload_IfNoneMatchAsterisk_ObjectExists_ShouldFail", func(t *testing.T) {
		getter := createMockEntryGetter(existingObject)

		// Create a mock CompleteMultipartUpload request with If-None-Match: *
		req := &http.Request{
			Method: "POST",
			Header: make(http.Header),
			URL: &url.URL{
				RawQuery: "uploadId=test-upload-id",
			},
		}
		req.Header.Set(s3_constants.IfNoneMatch, "*")

		s3a := NewS3ApiServerForTest()
		errCode := s3a.checkConditionalHeadersWithGetter(getter, req, bucket, object)
		if errCode != s3err.ErrPreconditionFailed {
			t.Errorf("Expected ErrPreconditionFailed when object exists with If-None-Match=*, got %v", errCode)
		}
	})

	// Test CompleteMultipartUpload with If-None-Match: * (should succeed when object doesn't exist)
	t.Run("CompleteMultipartUpload_IfNoneMatchAsterisk_ObjectNotExists_ShouldSucceed", func(t *testing.T) {
		getter := createMockEntryGetter(nil) // No existing object

		req := &http.Request{
			Method: "POST",
			Header: make(http.Header),
			URL: &url.URL{
				RawQuery: "uploadId=test-upload-id",
			},
		}
		req.Header.Set(s3_constants.IfNoneMatch, "*")

		s3a := NewS3ApiServerForTest()
		errCode := s3a.checkConditionalHeadersWithGetter(getter, req, bucket, object)
		if errCode != s3err.ErrNone {
			t.Errorf("Expected ErrNone when object doesn't exist with If-None-Match=*, got %v", errCode)
		}
	})

	// Test CompleteMultipartUpload with If-Match (should succeed when ETag matches)
	t.Run("CompleteMultipartUpload_IfMatch_ETagMatches_ShouldSucceed", func(t *testing.T) {
		getter := createMockEntryGetter(existingObject)

		req := &http.Request{
			Method: "POST",
			Header: make(http.Header),
			URL: &url.URL{
				RawQuery: "uploadId=test-upload-id",
			},
		}
		req.Header.Set(s3_constants.IfMatch, "\"existing123\"")

		s3a := NewS3ApiServerForTest()
		errCode := s3a.checkConditionalHeadersWithGetter(getter, req, bucket, object)
		if errCode != s3err.ErrNone {
			t.Errorf("Expected ErrNone when ETag matches, got %v", errCode)
		}
	})

	// Test CompleteMultipartUpload with If-Match (should fail when object doesn't exist)
	t.Run("CompleteMultipartUpload_IfMatch_ObjectNotExists_ShouldFail", func(t *testing.T) {
		getter := createMockEntryGetter(nil) // No existing object

		req := &http.Request{
			Method: "POST",
			Header: make(http.Header),
			URL: &url.URL{
				RawQuery: "uploadId=test-upload-id",
			},
		}
		req.Header.Set(s3_constants.IfMatch, "\"any-etag\"")

		s3a := NewS3ApiServerForTest()
		errCode := s3a.checkConditionalHeadersWithGetter(getter, req, bucket, object)
		if errCode != s3err.ErrPreconditionFailed {
			t.Errorf("Expected ErrPreconditionFailed when object doesn't exist with If-Match, got %v", errCode)
		}
	})

	// Test CompleteMultipartUpload with If-Match wildcard (should succeed when object exists)
	t.Run("CompleteMultipartUpload_IfMatchWildcard_ObjectExists_ShouldSucceed", func(t *testing.T) {
		getter := createMockEntryGetter(existingObject)

		req := &http.Request{
			Method: "POST",
			Header: make(http.Header),
			URL: &url.URL{
				RawQuery: "uploadId=test-upload-id",
			},
		}
		req.Header.Set(s3_constants.IfMatch, "*")

		s3a := NewS3ApiServerForTest()
		errCode := s3a.checkConditionalHeadersWithGetter(getter, req, bucket, object)
		if errCode != s3err.ErrNone {
			t.Errorf("Expected ErrNone when object exists with If-Match=*, got %v", errCode)
		}
	})
}
