package weed_server

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
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

func TestCheckPreconditionsPresentButEmptyHeaders(t *testing.T) {
	entry := &filer.Entry{Attr: filer.Attr{Mtime: time.Unix(1700000000, 0), Md5: []byte{0xb1, 0x94}}}
	before := time.Unix(1700000000-60, 0).UTC().Format(http.TimeFormat)
	after := time.Unix(1700000000+60, 0).UTC().Format(http.TimeFormat)

	// An empty If-Match line is still an If-Match: it matches nothing, and
	// If-Unmodified-Since stays ignored.
	r := httptest.NewRequest(http.MethodGet, "/d/f.txt", nil)
	r.Header.Set("If-Match", "")
	r.Header.Set("If-Unmodified-Since", after)
	if w := httptest.NewRecorder(); !checkPreconditions(w, r, entry) || w.Code != http.StatusPreconditionFailed {
		t.Errorf("empty If-Match -> %d, want 412", w.Code)
	}

	// An empty If-None-Match line matches nothing and suppresses
	// If-Modified-Since, so the request proceeds.
	r = httptest.NewRequest(http.MethodGet, "/d/f.txt", nil)
	r.Header.Set("If-None-Match", "")
	r.Header.Set("If-Modified-Since", after)
	if checkPreconditions(httptest.NewRecorder(), r, entry) {
		t.Error("empty If-None-Match with matching If-Modified-Since -> 304, want 200")
	}

	r = httptest.NewRequest(http.MethodGet, "/d/f.txt", nil)
	r.Header.Set("If-Modified-Since", before)
	if checkPreconditions(httptest.NewRecorder(), r, entry) {
		t.Error("old If-Modified-Since alone -> 304, want 200")
	}
}

func TestCheckPreconditionsIgnoresEmptyETagListElements(t *testing.T) {
	entry := &filer.Entry{
		Attr:   filer.Attr{Mtime: time.Unix(1700000000, 0)},
		Remote: &filer_pb.RemoteEntry{RemoteSize: 1},
	}

	tests := []struct {
		header string
		values []string
		want   int
	}{
		{"If-None-Match", []string{`"x",`}, http.StatusOK},
		{"If-None-Match", []string{`"x",,"y"`}, http.StatusOK},
		{"If-None-Match", []string{`""`}, http.StatusNotModified},
		{"If-None-Match", []string{`W/""`}, http.StatusNotModified},
		{"If-Match", []string{`"x",`}, http.StatusPreconditionFailed},
		{"If-Match", []string{`""`}, http.StatusOK},
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
