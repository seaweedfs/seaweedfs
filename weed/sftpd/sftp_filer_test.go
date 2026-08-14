package sftpd

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb"
)

// TestPutFileEscapesQueryInjection ensures a filename containing "?" cannot be
// reinterpreted by the filer as a query string that injects cp.from/mv.from
// commands, which would let an SFTP user escape their home directory.
func TestPutFileEscapesQueryInjection(t *testing.T) {
	var gotPath, gotRawQuery string
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		gotRawQuery = r.URL.RawQuery
		if _, err := w.Write([]byte(`{}`)); err != nil {
			t.Errorf("write response: %v", err)
		}
	}))
	defer ts.Close()

	fs := &SftpServer{filerAddr: pb.ServerAddress(strings.TrimPrefix(ts.URL, "http://"))}

	malicious := "/home/alice/steal?cp.from=/home/bob/secret.txt"
	if err := fs.putFile(malicious, strings.NewReader("dummy"), nil); err != nil {
		t.Fatalf("putFile: %v", err)
	}

	if gotRawQuery != "" {
		t.Errorf("filename leaked into query string: RawQuery=%q", gotRawQuery)
	}
	if gotPath != malicious {
		t.Errorf("path not delivered literally: got %q, want %q", gotPath, malicious)
	}
}
