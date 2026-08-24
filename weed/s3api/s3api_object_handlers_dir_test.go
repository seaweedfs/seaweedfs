package s3api

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
)

func TestServeDirectoryContentContentType(t *testing.T) {
	s3a := &S3ApiServer{}

	tests := []struct {
		name     string
		entry    *filer_pb.Entry
		wantType string
	}{
		{
			name:     "bare directory answers the directory marker type",
			entry:    &filer_pb.Entry{Name: "savepoint", IsDirectory: true, Attributes: &filer_pb.FuseAttributes{}},
			wantType: "application/x-directory",
		},
		{
			name:     "promoted file with data keeps octet-stream",
			entry:    &filer_pb.Entry{Name: "promoted", IsDirectory: true, Content: []byte("data"), Attributes: &filer_pb.FuseAttributes{FileSize: 4}},
			wantType: "application/octet-stream",
		},
		{
			name:     "stored mime is echoed verbatim",
			entry:    &filer_pb.Entry{Name: "marker", IsDirectory: true, Attributes: &filer_pb.FuseAttributes{Mime: "httpd/unix-directory"}},
			wantType: "httpd/unix-directory",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodHead, "/bucket/dir/", nil)
			rec := httptest.NewRecorder()
			s3a.serveDirectoryContent(rec, req, tt.entry)
			if rec.Code != http.StatusOK {
				t.Fatalf("status = %d, want 200", rec.Code)
			}
			if got := rec.Header().Get("Content-Type"); got != tt.wantType {
				t.Fatalf("Content-Type = %q, want %q", got, tt.wantType)
			}
		})
	}
}

func TestIsBareDirectory(t *testing.T) {
	tests := []struct {
		name  string
		entry *filer_pb.Entry
		want  bool
	}{
		{"nothing there", nil, false},
		{"file", &filer_pb.Entry{Attributes: &filer_pb.FuseAttributes{}}, false},
		{"directory a listing stands for", &filer_pb.Entry{IsDirectory: true, Attributes: &filer_pb.FuseAttributes{}}, true},
		{"promoted file with data", &filer_pb.Entry{IsDirectory: true, Content: []byte("data"), Attributes: &filer_pb.FuseAttributes{FileSize: 4}}, false},
		{
			// The key is real even at zero bytes, so it answers rather than 404s.
			name: "empty prefix object",
			entry: &filer_pb.Entry{IsDirectory: true, Attributes: &filer_pb.FuseAttributes{},
				Extended: map[string][]byte{s3_constants.SeaweedFSPrefixObject: []byte("true")}},
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := isBareDirectory(tt.entry); got != tt.want {
				t.Errorf("isBareDirectory() = %v, want %v", got, tt.want)
			}
		})
	}
}
