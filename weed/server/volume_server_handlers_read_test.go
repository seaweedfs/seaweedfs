package weed_server

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestInternalErrorWritesTheReadError(t *testing.T) {
	readErr := errors.New("invalid CRC for needle 2a (got 0a1b2c3d, want 4e5f6071), data on disk corrupted: needle data corrupted")
	w := httptest.NewRecorder()
	InternalError(w, httptest.NewRequest(http.MethodGet, "/1,abc", nil), readErr)
	if w.Code != http.StatusInternalServerError {
		t.Fatalf("status %d, want 500", w.Code)
	}
	if body := strings.TrimSpace(w.Body.String()); body != `{"error":"`+readErr.Error()+`"}` {
		t.Fatalf("body %q", body)
	}

	w = httptest.NewRecorder()
	InternalError(w, httptest.NewRequest(http.MethodGet, "/1,abc", nil), nil)
	if w.Code != http.StatusInternalServerError || w.Body.Len() != 0 {
		t.Fatalf("without an error: status %d body %q, want a bare 500", w.Code, w.Body.String())
	}

	w = httptest.NewRecorder()
	InternalError(w, httptest.NewRequest(http.MethodHead, "/1,abc", nil), readErr)
	if w.Code != http.StatusInternalServerError || w.Body.Len() != 0 {
		t.Fatalf("on a HEAD request: status %d body %q, want a bare 500", w.Code, w.Body.String())
	}
}

// A HEAD only attempts a metadata-only read, like a GET: a needle up to
// PagedReadLimit is read whole. Demanding metadata only would turn that one
// read into a header read followed by a trailer read.
func TestShouldAttemptStreamWrite(t *testing.T) {
	for _, tc := range []struct {
		name         string
		method       string
		target       string
		ext          string
		hasVolume    bool
		wantAttempt  bool
		wantMustMeta bool
	}{
		{"HEAD", http.MethodHead, "/1,abc.jpg", ".jpg", true, true, false},
		{"GET", http.MethodGet, "/1,abc.jpg", ".jpg", true, true, false},
		{"GET of a resized image", http.MethodGet, "/1,abc.jpg?width=100", ".jpg", true, false, false},
		{"HEAD of a resized image", http.MethodHead, "/1,abc.jpg?width=100", ".jpg", true, false, false},
		{"HEAD of a cropped image", http.MethodHead, "/1,abc.jpg?crop_x1=0&crop_y1=0&crop_x2=100&crop_y2=100", ".jpg", true, false, false},
		{"HEAD without a local volume", http.MethodHead, "/1,abc.jpg", ".jpg", false, false, false},
	} {
		r := httptest.NewRequest(tc.method, tc.target, nil)
		attempt, mustMeta := shouldAttemptStreamWrite(tc.hasVolume, tc.ext, r)
		if attempt != tc.wantAttempt || mustMeta != tc.wantMustMeta {
			t.Errorf("%s: attempt %v, must be metadata only %v; want %v, %v", tc.name, attempt, mustMeta, tc.wantAttempt, tc.wantMustMeta)
		}
	}
}

func TestNotModifiedChecksIfNoneMatchFirst(t *testing.T) {
	const etag = "353dd8be"
	const lastModified = 1700000000
	before := time.Unix(lastModified-60, 0).UTC().Format(http.TimeFormat)
	after := time.Unix(lastModified+60, 0).UTC().Format(http.TimeFormat)

	tests := []struct {
		name    string
		headers [][2]string
		want    bool
	}{
		{"matching etag", [][2]string{{"If-None-Match", `"353dd8be"`}}, true},
		{"weak etag", [][2]string{{"If-None-Match", `W/"353dd8be"`}}, true},
		{"etag in a list", [][2]string{{"If-None-Match", `"x", "353dd8be"`}}, true},
		{"star", [][2]string{{"If-None-Match", `*`}}, true},
		{"other etag", [][2]string{{"If-None-Match", `"x"`}}, false},
		{"etag across repeated lines", [][2]string{{"If-None-Match", `"x"`}, {"If-None-Match", `"353dd8be"`}}, true},
		{"comma inside quotes", [][2]string{{"If-None-Match", `"353dd8be,old"`}}, false},
		{"other etag ignores If-Modified-Since", [][2]string{{"If-None-Match", `"x"`}, {"If-Modified-Since", after}}, false},
		{"empty If-None-Match ignores If-Modified-Since", [][2]string{{"If-None-Match", ``}, {"If-Modified-Since", after}}, false},
		{"not modified since", [][2]string{{"If-Modified-Since", after}}, true},
		{"modified since", [][2]string{{"If-Modified-Since", before}}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := httptest.NewRequest(http.MethodGet, "/3,01637037d6", nil)
			for _, kv := range tt.headers {
				r.Header.Add(kv[0], kv[1])
			}
			if got := notModified(r, etag, lastModified); got != tt.want {
				t.Errorf("notModified = %v, want %v", got, tt.want)
			}
		})
	}
}
