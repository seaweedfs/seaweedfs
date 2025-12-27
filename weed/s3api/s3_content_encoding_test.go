package s3api

import (
	"bytes"
	"net/http/httptest"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestContentEncodingPreservation tests that Content-Encoding and Content-Language headers
// are preserved during S3 PUT and GET operations.
// This is a regression test for issue #7894
func TestContentEncodingPreservation(t *testing.T) {
	testCases := []struct {
		name            string
		contentEncoding string
		contentLanguage string
		body            string
	}{
		{
			name:            "gzip encoding",
			contentEncoding: "gzip",
			contentLanguage: "",
			body:            "Hello, SeaweedFS with gzip!",
		},
		{
			name:            "zstd encoding",
			contentEncoding: "zstd",
			contentLanguage: "",
			body:            "Hello, SeaweedFS with zstd!",
		},
		{
			name:            "deflate encoding",
			contentEncoding: "deflate",
			contentLanguage: "",
			body:            "Hello, SeaweedFS with deflate!",
		},
		{
			name:            "br (Brotli) encoding",
			contentEncoding: "br",
			contentLanguage: "",
			body:            "Hello, SeaweedFS with Brotli!",
		},
		{
			name:            "multiple encodings",
			contentEncoding: "gzip, deflate",
			contentLanguage: "",
			body:            "Hello, SeaweedFS with multiple encodings!",
		},
		{
			name:            "encoding with language",
			contentEncoding: "gzip",
			contentLanguage: "en-US",
			body:            "Hello, SeaweedFS with language!",
		},
		{
			name:            "language only",
			contentEncoding: "",
			contentLanguage: "fr-FR",
			body:            "Bonjour, SeaweedFS!",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Create a mock S3 API server
			s3a := &S3ApiServer{
				option: &S3ApiServerOption{
					BucketsPath: "/tmp/test-buckets",
				},
			}

			bucket := "test-bucket"
			key := "test-object.txt"

			// Create PUT request with Content-Encoding and/or Content-Language headers
			putReq := httptest.NewRequest("PUT", "/"+bucket+"/"+key, bytes.NewBufferString(tc.body))
			putReq.Header.Set("Content-Type", "text/plain")
			if tc.contentEncoding != "" {
				putReq.Header.Set("Content-Encoding", tc.contentEncoding)
			}
			if tc.contentLanguage != "" {
				putReq.Header.Set("Content-Language", tc.contentLanguage)
			}

			// Test that ParseS3Metadata correctly extracts the headers
			metadata, errCode := ParseS3Metadata(putReq, nil, false)
			require.Equal(t, 0, int(errCode), "ParseS3Metadata should succeed")

			// Verify Content-Encoding is stored in metadata
			if tc.contentEncoding != "" {
				assert.Equal(t, []byte(tc.contentEncoding), metadata["Content-Encoding"],
					"Content-Encoding should be stored in metadata")
			} else {
				assert.NotContains(t, metadata, "Content-Encoding",
					"Content-Encoding should not be in metadata when not provided")
			}

			// Verify Content-Language is stored in metadata
			if tc.contentLanguage != "" {
				assert.Equal(t, []byte(tc.contentLanguage), metadata["Content-Language"],
					"Content-Language should be stored in metadata")
			} else {
				assert.NotContains(t, metadata, "Content-Language",
					"Content-Language should not be in metadata when not provided")
			}

			// Simulate GET response - verify headers are set correctly
			getResp := httptest.NewRecorder()
			getReq := httptest.NewRequest("GET", "/"+bucket+"/"+key, nil)

			// Create a mock entry with the metadata
			entry := &filer_pb.Entry{
				Name: key,
				Attributes: &filer_pb.FuseAttributes{
					Mtime:    1234567890,
					FileSize: uint64(len(tc.body)),
					Mime:     "text/plain",
				},
				Extended: metadata,
			}

			// Call setResponseHeaders to set headers from metadata
			s3a.setResponseHeaders(getResp, getReq, entry, int64(len(tc.body)))

			// Verify Content-Encoding header is returned
			if tc.contentEncoding != "" {
				actualEncoding := getResp.Header().Get("Content-Encoding")
				assert.Equal(t, tc.contentEncoding, actualEncoding,
					"Content-Encoding header should be preserved in GET response")
			} else {
				assert.Empty(t, getResp.Header().Get("Content-Encoding"),
					"Content-Encoding should not be set when not provided")
			}

			// Verify Content-Language header is returned
			if tc.contentLanguage != "" {
				actualLanguage := getResp.Header().Get("Content-Language")
				assert.Equal(t, tc.contentLanguage, actualLanguage,
					"Content-Language header should be preserved in GET response")
			} else {
				assert.Empty(t, getResp.Header().Get("Content-Language"),
					"Content-Language should not be set when not provided")
			}
		})
	}
}

// TestContentEncodingWithOtherHeaders verifies that Content-Encoding works
// correctly alongside other standard headers
func TestContentEncodingWithOtherHeaders(t *testing.T) {
	s3a := &S3ApiServer{
		option: &S3ApiServerOption{
			BucketsPath: "/tmp/test-buckets",
		},
	}

	bucket := "test-bucket"
	key := "test-object.txt"
	body := "Test content"

	// Create PUT request with multiple headers
	putReq := httptest.NewRequest("PUT", "/"+bucket+"/"+key, bytes.NewBufferString(body))
	putReq.Header.Set("Content-Type", "text/plain")
	putReq.Header.Set("Content-Encoding", "gzip")
	putReq.Header.Set("Content-Language", "en-US")
	putReq.Header.Set("Cache-Control", "max-age=3600")
	putReq.Header.Set("Content-Disposition", "attachment; filename=test.txt")

	// Parse metadata
	metadata, errCode := ParseS3Metadata(putReq, nil, false)
	require.Equal(t, 0, int(errCode))

	// Verify all headers are stored
	assert.Equal(t, []byte("gzip"), metadata["Content-Encoding"])
	assert.Equal(t, []byte("en-US"), metadata["Content-Language"])
	assert.Equal(t, []byte("max-age=3600"), metadata["Cache-Control"])
	assert.Equal(t, []byte("attachment; filename=test.txt"), metadata["Content-Disposition"])

	// Simulate GET response
	getResp := httptest.NewRecorder()
	getReq := httptest.NewRequest("GET", "/"+bucket+"/"+key, nil)

	entry := &filer_pb.Entry{
		Name: key,
		Attributes: &filer_pb.FuseAttributes{
			Mtime:    1234567890,
			FileSize: uint64(len(body)),
			Mime:     "text/plain",
		},
		Extended: metadata,
	}

	s3a.setResponseHeaders(getResp, getReq, entry, int64(len(body)))

	// Verify all headers are returned
	assert.Equal(t, "gzip", getResp.Header().Get("Content-Encoding"))
	assert.Equal(t, "en-US", getResp.Header().Get("Content-Language"))
	assert.Equal(t, "max-age=3600", getResp.Header().Get("Cache-Control"))
	assert.Equal(t, "attachment; filename=test.txt", getResp.Header().Get("Content-Disposition"))
}
