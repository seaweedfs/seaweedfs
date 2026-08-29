package s3api

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3err"
)

// A precondition can only fail against an object that exists. GET/HEAD of a missing
// key must stay a missing-key answer even when If-Match or If-Unmodified-Since is sent.
func TestValidateConditionalHeadersForReadsMissingObject(t *testing.T) {
	s3a := &S3ApiServer{}

	existing := &filer_pb.Entry{
		Attributes: &filer_pb.FuseAttributes{Mtime: time.Now().Unix()},
		Extended:   map[string][]byte{s3_constants.ExtETagKey: []byte("d41d8cd98f00b204e9800998ecf8427e")},
	}
	deleteMarker := &filer_pb.Entry{
		Attributes: &filer_pb.FuseAttributes{Mtime: time.Now().Unix()},
		Extended:   map[string][]byte{s3_constants.ExtDeleteMarkerKey: []byte("true")},
	}
	future := time.Now().Add(24 * time.Hour).UTC().Format(http.TimeFormat)

	testCases := []struct {
		name   string
		header string
		value  string
		entry  *filer_pb.Entry
		want   s3err.ErrorCode
	}{
		{"if-match on missing object", s3_constants.IfMatch, "0000", nil, s3err.ErrNoSuchKey},
		{"if-match star on missing object", s3_constants.IfMatch, "*", nil, s3err.ErrNoSuchKey},
		{"if-unmodified-since on missing object", s3_constants.IfUnmodifiedSince, future, nil, s3err.ErrNoSuchKey},
		{"if-match on delete marker", s3_constants.IfMatch, "0000", deleteMarker, s3err.ErrNoSuchKey},
		{"if-none-match on missing object", s3_constants.IfNoneMatch, "*", nil, s3err.ErrNone},
		{"if-modified-since on missing object", s3_constants.IfModifiedSince, future, nil, s3err.ErrNone},
		{"if-match mismatch on existing object", s3_constants.IfMatch, "0000", existing, s3err.ErrPreconditionFailed},
		{"if-match hit on existing object", s3_constants.IfMatch, "d41d8cd98f00b204e9800998ecf8427e", existing, s3err.ErrNone},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			r := httptest.NewRequest(http.MethodGet, "/bucket/object", nil)
			r.Header.Set(tc.header, tc.value)
			headers, errCode := parseConditionalHeaders(r)
			if errCode != s3err.ErrNone {
				t.Fatalf("parseConditionalHeaders: %v", errCode)
			}
			result := s3a.validateConditionalHeadersForReads(r, headers, tc.entry, "bucket", "object")
			if result.ErrorCode != tc.want {
				t.Errorf("got %v, want %v", result.ErrorCode, tc.want)
			}
		})
	}
}
