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
