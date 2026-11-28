package s3api

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	util_http "github.com/seaweedfs/seaweedfs/weed/util/http"
)

// TestUploadTransformedChunkData_ContentLocation checks that re-encrypted and
// decrypted copies (SSE paths) record the destination object path.
func TestUploadTransformedChunkData_ContentLocation(t *testing.T) {
	util_http.InitGlobalHttpClient()

	var got string
	dst := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got = r.Header.Get("Seaweed-Content-Location")
		_, _ = io.Copy(io.Discard, r.Body)
		w.WriteHeader(http.StatusCreated)
		_, _ = io.WriteString(w, `{"size":5}`)
	}))
	defer dst.Close()

	assign := &filer_pb.AssignVolumeResponse{
		FileId:   "3,01",
		Location: &filer_pb.Location{Url: strings.TrimPrefix(dst.URL, "http://")},
	}
	s3a := &S3ApiServer{}
	if _, err := s3a.uploadTransformedChunkData([]byte("hello"), assign, "/buckets/b/a b"); err != nil {
		t.Fatalf("upload: %v", err)
	}
	if got != "/buckets/b/a%20b" {
		t.Errorf("Seaweed-Content-Location = %q, want %q", got, "/buckets/b/a%20b")
	}
}
