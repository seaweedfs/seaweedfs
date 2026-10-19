package s3api

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
)

func TestParseAndValidateRangeRejectsMalformedNumericOffsets(t *testing.T) {
	s3a := &S3ApiServer{}
	entry := &filer_pb.Entry{}

	tests := []struct {
		name        string
		rangeHeader string
	}{
		{"invalid start", "bytes=abc-5"},
		{"invalid end", "bytes=5-abc"},
		{"invalid open ended start", "bytes=abc-"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, "/bucket/object", nil)
			req.Header.Set("Range", tt.rangeHeader)
			rec := httptest.NewRecorder()

			_, _, _, streamErr := s3a.parseAndValidateRange(rec, req, entry, 100, "bucket", "object")
			if streamErr == nil {
				t.Fatalf("parseAndValidateRange(%q) error = nil, want invalid range error", tt.rangeHeader)
			}
			if rec.Code != http.StatusRequestedRangeNotSatisfiable {
				t.Fatalf("parseAndValidateRange(%q) status = %d, want %d", tt.rangeHeader, rec.Code, http.StatusRequestedRangeNotSatisfiable)
			}
			if got := rec.Header().Get("Content-Range"); got != "bytes */100" {
				t.Fatalf("parseAndValidateRange(%q) Content-Range = %q, want %q", tt.rangeHeader, got, "bytes */100")
			}
		})
	}
}

func TestHeadObjectRange(t *testing.T) {
	s3a, f := newAnonymousReadTestServer(t)
	f.entries["/buckets/b/o"] = anonymousReadEntry(anonymousReadGrant("READ"))
	for _, tc := range []struct {
		rangeHeader, contentRange, contentLength string
		code                                     int
	}{
		{"", "", "11", http.StatusOK},
		{"bytes=1-4", "bytes 1-4/11", "4", http.StatusPartialContent},
		{"bytes=-3", "bytes 8-10/11", "3", http.StatusPartialContent},
		{"bytes=5-", "bytes 5-10/11", "6", http.StatusPartialContent},
		{"bytes=20-", "bytes */11", "", http.StatusRequestedRangeNotSatisfiable},
	} {
		headers := map[string]string{}
		if tc.rangeHeader != "" {
			headers["Range"] = tc.rangeHeader
		}
		rr := serveAnonymousRead(s3a, http.MethodHead, "o", "", headers)
		if rr.Code != tc.code {
			t.Errorf("Range %q: status %d, want %d", tc.rangeHeader, rr.Code, tc.code)
		}
		if got := rr.Header().Get("Content-Range"); got != tc.contentRange {
			t.Errorf("Range %q: Content-Range %q, want %q", tc.rangeHeader, got, tc.contentRange)
		}
		if got := rr.Header().Get("Content-Length"); tc.contentLength != "" && got != tc.contentLength {
			t.Errorf("Range %q: Content-Length %q, want %q", tc.rangeHeader, got, tc.contentLength)
		}
	}
}
