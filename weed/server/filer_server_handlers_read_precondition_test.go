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
		header string
		values []string
		want   int
	}{
		{"If-None-Match", []string{etag}, http.StatusNotModified},
		{"If-None-Match", []string{`*`}, http.StatusNotModified},
		{"If-None-Match", []string{`W/` + etag}, http.StatusNotModified},
		{"If-None-Match", []string{`"x", ` + etag}, http.StatusNotModified},
		{"If-None-Match", []string{`"x"`, etag}, http.StatusNotModified},
		{"If-None-Match", []string{`"x"`}, http.StatusOK},
		{"If-None-Match", []string{`"b194,old"`}, http.StatusOK},
		{"If-Match", []string{etag}, http.StatusOK},
		{"If-Match", []string{`*`}, http.StatusOK},
		{"If-Match", []string{`"x", ` + etag}, http.StatusOK},
		{"If-Match", []string{`"x"`, etag}, http.StatusOK},
		{"If-Match", []string{`W/` + etag}, http.StatusPreconditionFailed},
		{"If-Match", []string{`"x"`}, http.StatusPreconditionFailed},
		{"If-Match", []string{`"b194,old"`}, http.StatusPreconditionFailed},
	}
	for _, tt := range tests {
		r := httptest.NewRequest(http.MethodGet, "/d/f.txt", nil)
		for _, v := range tt.values {
			r.Header.Add(tt.header, v)
		}
		w := httptest.NewRecorder()
		got := http.StatusOK
		if checkPreconditions(w, r, entry) {
			got = w.Code
		}
		if got != tt.want {
			t.Errorf("%s: %q -> %d, want %d", tt.header, tt.values, got, tt.want)
		}
	}
}
