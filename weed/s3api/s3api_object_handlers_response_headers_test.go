package s3api

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
)

// GetObject/HeadObject must expose only response headers AWS defines: user
// metadata, standard entity headers and x-amz-tagging-count. Tag values live
// behind GetObjectTagging and ACL/owner/lock state behind its own APIs; the
// generic Extended echo must not hand them to any caller that can read the
// object.
func TestSetResponseHeadersFiltersInternalExtendedKeys(t *testing.T) {
	s3a := &S3ApiServer{}
	entry := &filer_pb.Entry{
		Name:       "object",
		Attributes: &filer_pb.FuseAttributes{Mtime: 100},
		Extended: map[string][]byte{
			s3_constants.AmzObjectTaggingPrefix + "project": []byte("alpha"),
			s3_constants.AmzObjectTaggingPrefix + "owner":   []byte("y"),
			"x-amz-tagging-sneaky":                        []byte("z"),
			"seaweed-x-amz-acl":                           []byte("z"),
			s3_constants.ExtAmzAclKey:                       []byte(`[{"Permission":"FULL_CONTROL"}]`),
			s3_constants.ExtAmzOwnerKey:                     []byte("admin"),
			s3_constants.ExtVersionIdKey:                    []byte("version-1"),
			s3_constants.ExtObjectLockModeKey:               []byte("COMPLIANCE"),
			s3_constants.ExtETagKey:                         []byte("d41d8cd98f00b204e9800998ecf8427e"),
			s3_constants.SeaweedFSWriteId:                   []byte("write-id"),
			"X-Amz-Meta-Foo":                               []byte("bar"),
			"Cache-Control":                                []byte("no-cache"),
			"xattr-user.note":                              []byte("n"),
		},
	}

	for _, method := range []string{http.MethodGet, http.MethodHead} {
		w := httptest.NewRecorder()
		s3a.setResponseHeaders(w, httptest.NewRequest(method, "/bucket/object", nil), entry, 8)

		for key := range w.Header() {
			lowerKey := strings.ToLower(key)
			if lowerKey == s3_constants.AmzTagCount {
				continue
			}
			if strings.HasPrefix(lowerKey, strings.ToLower(s3_constants.ExtAmzPrefix)) {
				t.Errorf("%s: internal header %q leaked into response", method, key)
			}
			if strings.HasPrefix(lowerKey, strings.ToLower(s3_constants.AmzObjectTaggingPrefix)) {
				t.Errorf("%s: object tag value %q leaked into response", method, key)
			}
		}
		if got := w.Header().Get(s3_constants.AmzTagCount); got != "2" {
			t.Errorf("%s: tag count = %q, want 2", method, got)
		}
		if got := w.Header()["X-Amz-Meta-foo"]; len(got) != 1 || got[0] != "bar" {
			t.Errorf("%s: user metadata dropped, X-Amz-Meta-foo = %v", method, got)
		}
		if got := w.Header().Get("Cache-Control"); got != "no-cache" {
			t.Errorf("%s: Cache-Control = %q", method, got)
		}
	}
}
