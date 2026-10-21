package weed_server

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
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
