package weed_server

import (
	"bytes"
	"io"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/filer"
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

// A POST to a path with a doubled slash must reach the handler at the cleaned,
// still-decoded path. Without CleanPathHandler, http.ServeMux answers with a
// redirect whose Location percent-encodes the already-escaped path a second
// time (golang/go#79897), and a client following it creates directories
// literally named "%E8%B4%9F..." (seaweedfs#11125).
func TestCleanPathHandlerServesCleanedPathWithoutRedirect(t *testing.T) {
	tests := []struct {
		name        string
		target      string
		wantPath    string
		wantEscaped string
	}{
		{
			name:        "double slash before non-ascii segments",
			target:      "/Image/2026-09-04//负极全景/OK渲染图/test123_残片正光_拼图_105525641.jpg",
			wantPath:    "/Image/2026-09-04/负极全景/OK渲染图/test123_残片正光_拼图_105525641.jpg",
			wantEscaped: "/Image/2026-09-04/%E8%B4%9F%E6%9E%81%E5%85%A8%E6%99%AF/OK%E6%B8%B2%E6%9F%93%E5%9B%BE/test123_%E6%AE%8B%E7%89%87%E6%AD%A3%E5%85%89_%E6%8B%BC%E5%9B%BE_105525641.jpg",
		},
		{
			name:        "already percent-encoded by the client",
			target:      "/Image//%E8%B4%9F%E6%9E%81%E5%85%A8%E6%99%AF/a.jpg",
			wantPath:    "/Image/负极全景/a.jpg",
			wantEscaped: "/Image/%E8%B4%9F%E6%9E%81%E5%85%A8%E6%99%AF/a.jpg",
		},
		{
			name:        "trailing slash marks a directory and is kept",
			target:      "/Image//负极全景/",
			wantPath:    "/Image/负极全景/",
			wantEscaped: "/Image/%E8%B4%9F%E6%9E%81%E5%85%A8%E6%99%AF/",
		},
		{
			name:        "dot segments are resolved",
			target:      "/Image/./tmp/../负极全景/a.jpg",
			wantPath:    "/Image/负极全景/a.jpg",
			wantEscaped: "/Image/%E8%B4%9F%E6%9E%81%E5%85%A8%E6%99%AF/a.jpg",
		},
		{
			name:        "canonical path is passed through untouched",
			target:      "/Image/负极全景/a.jpg",
			wantPath:    "/Image/负极全景/a.jpg",
			wantEscaped: "/Image/%E8%B4%9F%E6%9E%81%E5%85%A8%E6%99%AF/a.jpg",
		},
		{
			name:        "encoded slash is not a separator and is preserved",
			target:      "/Image//a%2F%2Fb/c.jpg",
			wantPath:    "/Image/a//b/c.jpg",
			wantEscaped: "/Image/a%2F%2Fb/c.jpg",
		},
		{
			name:        "root",
			target:      "//",
			wantPath:    "/",
			wantEscaped: "/",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var gotPath, gotEscaped string
			mux := http.NewServeMux()
			mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
				gotPath, gotEscaped = r.URL.Path, r.URL.EscapedPath()
			})

			w := httptest.NewRecorder()
			CleanPathHandler(mux).ServeHTTP(w, httptest.NewRequest(http.MethodPost, tc.target, strings.NewReader("data")))

			if w.Code != http.StatusOK {
				t.Fatalf("status: got %d (Location %q), want 200 with no redirect", w.Code, w.Header().Get("Location"))
			}
			if gotPath != tc.wantPath {
				t.Errorf("r.URL.Path: got %q want %q", gotPath, tc.wantPath)
			}
			if gotEscaped != tc.wantEscaped {
				t.Errorf("r.URL.EscapedPath(): got %q want %q", gotEscaped, tc.wantEscaped)
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

func TestProcessRangeRequestRanges(t *testing.T) {
	data := []byte("0123456789")
	serve := func(rangeHeader string) (*httptest.ResponseRecorder, error) {
		r := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/test.txt", nil)
		r.Header.Set("Range", rangeHeader)
		w := httptest.NewRecorder()
		err := ProcessRangeRequest(r, w, int64(len(data)), "text/plain", func(offset int64, size int64) (filer.DoStreamContent, error) {
			return func(writer io.Writer) error {
				_, err := writer.Write(data[offset : offset+size])
				return err
			}, nil
		})
		return w, err
	}

	tests := []struct {
		rangeHeader string
		wantCode    int
		wantRange   string
		wantBody    string
	}{
		{"bytes=0-1", http.StatusPartialContent, "bytes 0-1/10", "01"},
		{"bytes=5-100", http.StatusPartialContent, "bytes 5-9/10", "56789"},
		{"bytes=10-", http.StatusRequestedRangeNotSatisfiable, "bytes */10", ""},
		{"bytes=100-", http.StatusRequestedRangeNotSatisfiable, "bytes */10", ""},
		{"bytes=10-,0-1", http.StatusPartialContent, "bytes 0-1/10", "01"},
	}
	for _, tt := range tests {
		w, err := serve(tt.rangeHeader)
		if wantErr := tt.wantCode == http.StatusRequestedRangeNotSatisfiable; (err != nil) != wantErr {
			t.Errorf("%s: error = %v, want an error only for 416", tt.rangeHeader, err)
		}
		if w.Code != tt.wantCode {
			t.Errorf("%s: status %d, want %d", tt.rangeHeader, w.Code, tt.wantCode)
		}
		if got := w.Header().Get("Content-Range"); got != tt.wantRange {
			t.Errorf("%s: Content-Range %q, want %q", tt.rangeHeader, got, tt.wantRange)
		}
		if tt.wantBody != "" && w.Body.String() != tt.wantBody {
			t.Errorf("%s: body %q, want %q", tt.rangeHeader, w.Body.String(), tt.wantBody)
		}
	}
}
