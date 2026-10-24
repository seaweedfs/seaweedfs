package weed_server

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/filer"
)

func TestCheckPreconditionsETagLists(t *testing.T) {
	entry := &filer.Entry{Attr: filer.Attr{Mtime: time.Unix(1700000000, 0), Md5: []byte{0xb1, 0x94}}}
	const etag = `"b194"`

	tests := []struct {
		header, value string
		want          int
	}{
		{"If-None-Match", etag, http.StatusNotModified},
		{"If-None-Match", `*`, http.StatusNotModified},
		{"If-None-Match", `W/` + etag, http.StatusNotModified},
		{"If-None-Match", `"x", ` + etag, http.StatusNotModified},
		{"If-None-Match", `"x"`, http.StatusOK},
		{"If-Match", etag, http.StatusOK},
		{"If-Match", `*`, http.StatusOK},
		{"If-Match", `"x", ` + etag, http.StatusOK},
		{"If-Match", `W/` + etag, http.StatusPreconditionFailed},
		{"If-Match", `"x"`, http.StatusPreconditionFailed},
	}
	for _, tt := range tests {
		r := httptest.NewRequest(http.MethodGet, "/d/f.txt", nil)
		r.Header.Set(tt.header, tt.value)
		w := httptest.NewRecorder()
		got := http.StatusOK
		if checkPreconditions(w, r, entry) {
			got = w.Code
		}
		if got != tt.want {
			t.Errorf("%s: %s -> %d, want %d", tt.header, tt.value, got, tt.want)
		}
	}
}
