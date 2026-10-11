package weed_server

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

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
		{"HEAD without a local volume", http.MethodHead, "/1,abc.jpg", ".jpg", false, false, false},
	} {
		r := httptest.NewRequest(tc.method, tc.target, nil)
		attempt, mustMeta := shouldAttemptStreamWrite(tc.hasVolume, tc.ext, r)
		if attempt != tc.wantAttempt || mustMeta != tc.wantMustMeta {
			t.Errorf("%s: attempt %v, must be metadata only %v; want %v, %v", tc.name, attempt, mustMeta, tc.wantAttempt, tc.wantMustMeta)
		}
	}
}
