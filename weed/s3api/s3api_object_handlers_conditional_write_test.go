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

// A conditional write with If-Match has nothing to compare against when the key has
// no current version; AWS answers 404 there, not 412.
func TestValidateConditionalHeadersMissingObject(t *testing.T) {
	s3a := &S3ApiServer{}

	existing := &filer_pb.Entry{
		Attributes: &filer_pb.FuseAttributes{Mtime: time.Now().Unix()},
		Extended:   map[string][]byte{s3_constants.ExtETagKey: []byte("d41d8cd98f00b204e9800998ecf8427e")},
	}
	deleteMarker := &filer_pb.Entry{
		Attributes: &filer_pb.FuseAttributes{Mtime: time.Now().Unix()},
		Extended:   map[string][]byte{s3_constants.ExtDeleteMarkerKey: []byte("true")},
	}

	testCases := []struct {
		name   string
		header string
		value  string
		entry  *filer_pb.Entry
		want   s3err.ErrorCode
	}{
		{"if-match on missing object", s3_constants.IfMatch, "d41d8cd98f00b204e9800998ecf8427e", nil, s3err.ErrNoSuchKey},
		{"if-match star on missing object", s3_constants.IfMatch, "*", nil, s3err.ErrNoSuchKey},
		{"if-match on delete marker", s3_constants.IfMatch, "d41d8cd98f00b204e9800998ecf8427e", deleteMarker, s3err.ErrNoSuchKey},
		{"if-match mismatch on existing object", s3_constants.IfMatch, "0000", existing, s3err.ErrPreconditionFailed},
		{"if-match hit on existing object", s3_constants.IfMatch, "d41d8cd98f00b204e9800998ecf8427e", existing, s3err.ErrNone},
		{"if-none-match star on missing object", s3_constants.IfNoneMatch, "*", nil, s3err.ErrNone},
		{"if-none-match star on delete marker", s3_constants.IfNoneMatch, "*", deleteMarker, s3err.ErrNone},
		{"if-none-match star on existing object", s3_constants.IfNoneMatch, "*", existing, s3err.ErrPreconditionFailed},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			r := httptest.NewRequest(http.MethodPut, "/bucket/object", nil)
			r.Header.Set(tc.header, tc.value)
			headers, errCode := parseConditionalHeaders(r)
			if errCode != s3err.ErrNone {
				t.Fatalf("parseConditionalHeaders: %v", errCode)
			}
			if got := s3a.validateConditionalHeaders(r, headers, tc.entry, "bucket", "object"); got != tc.want {
				t.Errorf("got %v, want %v", got, tc.want)
			}
		})
	}
}
