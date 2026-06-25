package weed_server

import (
	"bytes"
	"io"
	"mime/multipart"
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

func TestWriteJsonPrettyDoesNotReadMultipartBody(t *testing.T) {
	var form bytes.Buffer
	mw := multipart.NewWriter(&form)
	if err := mw.WriteField("pretty", "1"); err != nil {
		t.Fatalf("write pretty field: %v", err)
	}
	part, err := mw.CreateFormFile("file", "test.txt")
	if err != nil {
		t.Fatalf("create form file: %v", err)
	}
	if _, err := part.Write([]byte("hello")); err != nil {
		t.Fatalf("write form file: %v", err)
	}
	if err := mw.Close(); err != nil {
		t.Fatalf("close multipart writer: %v", err)
	}

	body := &countingReadCloser{Reader: bytes.NewReader(form.Bytes())}
	r := httptest.NewRequest(http.MethodPost, "/x", body)
	r.Header.Set("Content-Type", mw.FormDataContentType())
	w := httptest.NewRecorder()

	if err := writeJson(w, r, http.StatusTooManyRequests, map[string]string{"error": "busy"}); err != nil {
		t.Fatalf("writeJson: %v", err)
	}

	if body.reads != 0 {
		t.Fatalf("writeJson read multipart body %d times", body.reads)
	}
	if got, want := w.Body.String(), `{"error":"busy"}`; got != want {
		t.Fatalf("body: got %q want %q", got, want)
	}
}

type countingReadCloser struct {
	io.Reader
	reads int
}

func (c *countingReadCloser) Read(p []byte) (int, error) {
	c.reads++
	return c.Reader.Read(p)
}

func (c *countingReadCloser) Close() error {
	return nil
}
