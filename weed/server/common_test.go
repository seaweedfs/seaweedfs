package weed_server

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestParseURL(t *testing.T) {
	if vid, fid, _, _, _ := parseURLPath("/1,06dfa8a684"); true {
		if vid != "1" {
			t.Errorf("fail to parse vid: %s", vid)
		}
		if fid != "06dfa8a684" {
			t.Errorf("fail to parse fid: %s", fid)
		}
	}
	if vid, fid, _, _, _ := parseURLPath("/1,06dfa8a684_1"); true {
		if vid != "1" {
			t.Errorf("fail to parse vid: %s", vid)
		}
		if fid != "06dfa8a684_1" {
			t.Errorf("fail to parse fid: %s", fid)
		}
		if sepIndex := strings.LastIndex(fid, "_"); sepIndex > 0 {
			fid = fid[:sepIndex]
		}
		if fid != "06dfa8a684" {
			t.Errorf("fail to parse fid: %s", fid)
		}
	}
}

func TestWriteJsonNoJSONP(t *testing.T) {
	// callback= must be ignored; response is always application/json with nosniff.
	cases := []string{"", "myCb", "<script>alert(1)</script>"}
	for _, cb := range cases {
		t.Run("callback="+cb, func(t *testing.T) {
			url := "/x"
			if cb != "" {
				url += "?callback=" + cb
			}
			r := httptest.NewRequest(http.MethodGet, url, nil)
			w := httptest.NewRecorder()
			if err := writeJson(w, r, http.StatusOK, map[string]string{"k": "v"}); err != nil {
				t.Fatalf("writeJson: %v", err)
			}
			if w.Code != http.StatusOK {
				t.Errorf("status: got %d want 200", w.Code)
			}
			if got := w.Header().Get("Content-Type"); got != "application/json" {
				t.Errorf("Content-Type: got %q want application/json", got)
			}
			if got := w.Header().Get("X-Content-Type-Options"); got != "nosniff" {
				t.Errorf("X-Content-Type-Options: got %q want nosniff", got)
			}
			if got := w.Body.String(); got != `{"k":"v"}` {
				t.Errorf("body: got %q want %q", got, `{"k":"v"}`)
			}
		})
	}
}
