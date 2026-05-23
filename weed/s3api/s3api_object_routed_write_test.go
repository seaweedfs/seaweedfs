package s3api

import (
	"net/http"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
)

func reqWith(headers map[string]string) *http.Request {
	r, _ := http.NewRequest(http.MethodPut, "/b/o", nil)
	for k, v := range headers {
		r.Header.Set(k, v)
	}
	return r
}

func TestBuildWriteCondition(t *testing.T) {
	cases := []struct {
		name    string
		headers map[string]string
		wantOk  bool
		want    filer_pb.WriteCondition_Kind
		wantTag string
	}{
		{"none", nil, true, filer_pb.WriteCondition_NONE, ""},
		{"ifnonematch-star", map[string]string{s3_constants.IfNoneMatch: "*"}, true, filer_pb.WriteCondition_IF_NOT_EXISTS, ""},
		{"ifmatch-star", map[string]string{s3_constants.IfMatch: "*"}, true, filer_pb.WriteCondition_IF_EXISTS, ""},
		{"ifmatch-etag", map[string]string{s3_constants.IfMatch: `"abc"`}, true, filer_pb.WriteCondition_IF_ETAG_MATCH, "abc"},
		{"ifnonematch-etag", map[string]string{s3_constants.IfNoneMatch: `"abc"`}, true, filer_pb.WriteCondition_IF_ETAG_NOT_MATCH, "abc"},
		// Cases that must fall back to the lock path:
		{"both", map[string]string{s3_constants.IfMatch: "*", s3_constants.IfNoneMatch: "*"}, false, 0, ""},
		{"etag-list", map[string]string{s3_constants.IfMatch: `"a","b"`}, false, 0, ""},
		{"weak-etag", map[string]string{s3_constants.IfMatch: `W/"abc"`}, false, 0, ""},
		{"time-based", map[string]string{s3_constants.IfUnmodifiedSince: "Wed, 21 Oct 2015 07:28:00 GMT"}, false, 0, ""},
	}
	for _, tc := range cases {
		cond, ok := buildWriteCondition(reqWith(tc.headers))
		if ok != tc.wantOk {
			t.Errorf("%s: ok=%v want %v", tc.name, ok, tc.wantOk)
			continue
		}
		if ok {
			if cond.Kind != tc.want {
				t.Errorf("%s: kind=%v want %v", tc.name, cond.Kind, tc.want)
			}
			if cond.Etag != tc.wantTag {
				t.Errorf("%s: etag=%q want %q", tc.name, cond.Etag, tc.wantTag)
			}
		}
	}
}

func TestBuildDeleteCondition(t *testing.T) {
	cases := []struct {
		name    string
		ifMatch string
		wantOk  bool
		want    filer_pb.WriteCondition_Kind
		wantTag string
	}{
		{"none", "", true, filer_pb.WriteCondition_NONE, ""},
		{"star", "*", true, filer_pb.WriteCondition_IF_EXISTS, ""},
		{"etag", `"abc"`, true, filer_pb.WriteCondition_IF_ETAG_MATCH, "abc"},
		{"list", `"a","b"`, false, 0, ""},
		{"weak", `W/"abc"`, false, 0, ""},
	}
	for _, tc := range cases {
		h := map[string]string{}
		if tc.ifMatch != "" {
			h[s3_constants.IfMatch] = tc.ifMatch
		}
		cond, ok := buildDeleteCondition(reqWith(h))
		if ok != tc.wantOk {
			t.Errorf("%s: ok=%v want %v", tc.name, ok, tc.wantOk)
			continue
		}
		if ok && (cond.Kind != tc.want || cond.Etag != tc.wantTag) {
			t.Errorf("%s: got kind=%v etag=%q want kind=%v etag=%q", tc.name, cond.Kind, cond.Etag, tc.want, tc.wantTag)
		}
	}
}
