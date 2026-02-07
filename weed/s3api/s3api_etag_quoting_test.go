package s3api

import (
	"fmt"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3err"
)

// TestReproIfMatchMismatch tests specifically for the scenario where internal ETag
// is unquoted (common in SeaweedFS) but client sends quoted ETag in If-Match.
func TestReproIfMatchMismatch(t *testing.T) {
	bucket := "test-bucket"
	object := "/test-key"
	etagValue := "37b51d194a7513e45b56f6524f2d51f2"

	// Scenario 1: Internal ETag is UNQUOTED (stored in Extended), Client sends QUOTED If-Match
	// This mirrors the behavior we enforced in filer_multipart.go
	t.Run("UnquotedInternal_QuotedHeader", func(t *testing.T) {
		entry := &filer_pb.Entry{
			Name: "test-key",
			Extended: map[string][]byte{
				s3_constants.ExtETagKey: []byte(etagValue), // Unquoted
			},
			Attributes: &filer_pb.FuseAttributes{
				Mtime:    time.Now().Unix(),
				FileSize: 1024,
			},
		}

		getter := &MockEntryGetter{mockEntry: entry}
		req := createTestGetRequest(bucket, object)
		// Client sends quoted ETag
		req.Header.Set(s3_constants.IfMatch, "\""+etagValue+"\"")

		s3a := NewS3ApiServerForTest()
		result := s3a.checkConditionalHeadersForReadsWithGetter(getter, req, bucket, object)

		if result.ErrorCode != s3err.ErrNone {
			t.Errorf("Expected success (ErrNone) for unquoted internal ETag and quoted header, got %v. Internal ETag: %s", result.ErrorCode, string(entry.Extended[s3_constants.ExtETagKey]))
		}
	})

	// Scenario 2: Internal ETag is QUOTED (stored in Extended), Client sends QUOTED If-Match
	// This handles legacy or mixed content
	t.Run("QuotedInternal_QuotedHeader", func(t *testing.T) {
		entry := &filer_pb.Entry{
			Name: "test-key",
			Extended: map[string][]byte{
				s3_constants.ExtETagKey: []byte("\"" + etagValue + "\""), // Quoted
			},
			Attributes: &filer_pb.FuseAttributes{
				Mtime:    time.Now().Unix(),
				FileSize: 1024,
			},
		}

		getter := &MockEntryGetter{mockEntry: entry}
		req := createTestGetRequest(bucket, object)
		req.Header.Set(s3_constants.IfMatch, "\""+etagValue+"\"")

		s3a := NewS3ApiServerForTest()
		result := s3a.checkConditionalHeadersForReadsWithGetter(getter, req, bucket, object)

		if result.ErrorCode != s3err.ErrNone {
			t.Errorf("Expected success (ErrNone) for quoted internal ETag and quoted header, got %v", result.ErrorCode)
		}
	})

	// Scenario 3: Internal ETag is from Md5 (QUOTED by getObjectETag), Client sends QUOTED If-Match
	t.Run("Md5Internal_QuotedHeader", func(t *testing.T) {
		// Mock Md5 attribute (16 bytes)
		md5Bytes := make([]byte, 16)
		copy(md5Bytes, []byte("1234567890123456")) // This doesn't match the hex string below, but getObjectETag formats it as hex

		// Expected ETag from Md5 is hex string of bytes
		expectedHex := fmt.Sprintf("%x", md5Bytes)

		entry := &filer_pb.Entry{
			Name: "test-key",
			Attributes: &filer_pb.FuseAttributes{
				Mtime:    time.Now().Unix(),
				FileSize: 1024,
				Md5:      md5Bytes,
			},
		}

		getter := &MockEntryGetter{mockEntry: entry}
		req := createTestGetRequest(bucket, object)
		req.Header.Set(s3_constants.IfMatch, "\""+expectedHex+"\"")

		s3a := NewS3ApiServerForTest()
		result := s3a.checkConditionalHeadersForReadsWithGetter(getter, req, bucket, object)

		if result.ErrorCode != s3err.ErrNone {
			t.Errorf("Expected success (ErrNone) for Md5 internal ETag and quoted header, got %v", result.ErrorCode)
		}
	})

	// Test getObjectETag specifically ensuring it returns quoted strings
	t.Run("getObjectETag_ShouldReturnQuoted", func(t *testing.T) {
		entry := &filer_pb.Entry{
			Name: "test-key",
			Extended: map[string][]byte{
				s3_constants.ExtETagKey: []byte("unquoted-etag"),
			},
		}

		s3a := NewS3ApiServerForTest()
		etag := s3a.getObjectETag(entry)

		expected := "\"unquoted-etag\""
		if etag != expected {
			t.Errorf("Expected quoted ETag %s, got %s", expected, etag)
		}
	})

	// Test getObjectETag fallback when Extended ETag is present but empty
	t.Run("getObjectETag_EmptyExtended_ShouldFallback", func(t *testing.T) {
		md5Bytes := []byte("1234567890123456")
		expectedHex := fmt.Sprintf("\"%x\"", md5Bytes)

		entry := &filer_pb.Entry{
			Name: "test-key-fallback",
			Extended: map[string][]byte{
				s3_constants.ExtETagKey: []byte(""), // Present but empty
			},
			Attributes: &filer_pb.FuseAttributes{
				Mtime:    time.Now().Unix(),
				FileSize: 1024,
				Md5:      md5Bytes,
			},
		}

		s3a := NewS3ApiServerForTest()
		etag := s3a.getObjectETag(entry)

		if etag != expectedHex {
			t.Errorf("Expected fallback ETag %s, got %s", expectedHex, etag)
		}
	})

	// Test newListEntry ETag behavior
	t.Run("newListEntry_ShouldReturnQuoted", func(t *testing.T) {
		entry := &filer_pb.Entry{
			Name: "test-key",
			Extended: map[string][]byte{
				s3_constants.ExtETagKey: []byte("unquoted-etag"),
			},
			Attributes: &filer_pb.FuseAttributes{
				Mtime:    time.Now().Unix(),
				FileSize: 1024,
			},
		}

		s3a := NewS3ApiServerForTest()
		listEntry := newListEntry(s3a, entry, "", "bucket/dir", "test-key", "bucket/", false, false, false)

		expected := "\"unquoted-etag\""
		if listEntry.ETag != expected {
			t.Errorf("Expected quoted ETag %s, got %s", expected, listEntry.ETag)
		}
	})
}
