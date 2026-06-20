package admin

import (
	"bytes"
	"compress/gzip"
	"io"
	"io/fs"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestStaticGzMirror fails when static_gz/ is out of sync with static/.
// Regenerate with: go generate ./weed/admin
func TestStaticGzMirror(t *testing.T) {
	sources := make(map[string][]byte)
	err := filepath.WalkDir("static", func(p string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return err
		}
		data, err := os.ReadFile(p)
		if err != nil {
			return err
		}
		rel, err := filepath.Rel("static", p)
		if err != nil {
			return err
		}
		sources[filepath.ToSlash(rel)] = data
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(sources) == 0 {
		t.Fatal("no files under static/")
	}

	mirrored := 0
	err = filepath.WalkDir("static_gz", func(p string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return err
		}
		rel, err := filepath.Rel("static_gz", p)
		if err != nil {
			return err
		}
		key := strings.TrimSuffix(filepath.ToSlash(rel), ".gz")
		want, ok := sources[key]
		if !ok {
			t.Errorf("static_gz/%s has no source in static/", rel)
			return nil
		}
		mirrored++
		gz, err := os.ReadFile(p)
		if err != nil {
			return err
		}
		zr, err := gzip.NewReader(bytes.NewReader(gz))
		if err != nil {
			return err
		}
		defer zr.Close()
		got, err := io.ReadAll(zr)
		if err != nil {
			return err
		}
		if !bytes.Equal(got, want) {
			t.Errorf("static_gz/%s does not match static/%s", rel, key)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if mirrored != len(sources) {
		t.Errorf("static_gz mirrors %d of %d files under static/", mirrored, len(sources))
	}
}

func fetchStatic(t *testing.T, path string, header map[string]string) *http.Response {
	t.Helper()
	req := httptest.NewRequest("GET", path, nil)
	for k, v := range header {
		req.Header.Set(k, v)
	}
	w := httptest.NewRecorder()
	StaticHandler().ServeHTTP(w, req)
	return w.Result()
}

func TestStaticHandlerGzip(t *testing.T) {
	resp := fetchStatic(t, "/css/bootstrap.min.css", map[string]string{"Accept-Encoding": "gzip, deflate, br"})
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status %d", resp.StatusCode)
	}
	if got := resp.Header.Get("Content-Encoding"); got != "gzip" {
		t.Fatalf("Content-Encoding %q", got)
	}
	if resp.Header.Get("Cache-Control") != staticCacheControl {
		t.Errorf("Cache-Control %q", resp.Header.Get("Cache-Control"))
	}
	if resp.Header.Get("Vary") != "Accept-Encoding" {
		t.Errorf("Vary %q", resp.Header.Get("Vary"))
	}
	if resp.Header.Get("ETag") == "" {
		t.Error("missing ETag")
	}
	if got := resp.Header.Get("Content-Type"); !strings.HasPrefix(got, "text/css") {
		t.Errorf("Content-Type %q", got)
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	if resp.Header.Get("Content-Length") == "" {
		t.Error("missing Content-Length")
	}
	zr, err := gzip.NewReader(bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	defer zr.Close()
	got, err := io.ReadAll(zr)
	if err != nil {
		t.Fatal(err)
	}
	want, err := os.ReadFile("static/css/bootstrap.min.css")
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, want) {
		t.Error("gzip body does not decompress to the source file")
	}
}

func TestStaticHandlerIdentity(t *testing.T) {
	resp := fetchStatic(t, "/css/bootstrap.min.css", nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status %d", resp.StatusCode)
	}
	if got := resp.Header.Get("Content-Encoding"); got != "" {
		t.Fatalf("Content-Encoding %q", got)
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	want, err := os.ReadFile("static/css/bootstrap.min.css")
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(body, want) {
		t.Error("identity body does not match the source file")
	}
}

func TestStaticHandlerNotModified(t *testing.T) {
	first := fetchStatic(t, "/css/admin.css", map[string]string{"Accept-Encoding": "gzip"})
	etag := first.Header.Get("ETag")
	if etag == "" {
		t.Fatal("missing ETag")
	}
	resp := fetchStatic(t, "/css/admin.css", map[string]string{"Accept-Encoding": "gzip", "If-None-Match": etag})
	if resp.StatusCode != http.StatusNotModified {
		t.Fatalf("status %d", resp.StatusCode)
	}
	if resp.Header.Get("Content-Length") != "" {
		t.Errorf("304 carries Content-Length %q", resp.Header.Get("Content-Length"))
	}
}

func TestStaticHandlerVariantETags(t *testing.T) {
	gz := fetchStatic(t, "/css/admin.css", map[string]string{"Accept-Encoding": "gzip"})
	id := fetchStatic(t, "/css/admin.css", nil)
	if gz.Header.Get("ETag") == id.Header.Get("ETag") {
		t.Error("gzip and identity variants share an ETag")
	}
}

func TestStaticHandlerNotFound(t *testing.T) {
	for _, p := range []string{"/css/missing.css", "/css/", "/", "/../static_embed.go"} {
		if resp := fetchStatic(t, p, nil); resp.StatusCode != http.StatusNotFound {
			t.Errorf("%s: status %d", p, resp.StatusCode)
		}
	}
}

func TestAcceptsGzip(t *testing.T) {
	cases := []struct {
		header string
		want   bool
	}{
		{"", false},
		{"gzip", true},
		{"gzip, deflate, br", true},
		{"br;q=1.0, gzip;q=0.8", true},
		{"gzip;q=0", false},
		{"gzip;q=0.0", false},
		{"deflate", false},
		{"x-gzip-like", false},
	}
	for _, c := range cases {
		r := httptest.NewRequest("GET", "/", nil)
		if c.header != "" {
			r.Header.Set("Accept-Encoding", c.header)
		}
		if got := acceptsGzip(r); got != c.want {
			t.Errorf("acceptsGzip(%q) = %v, want %v", c.header, got, c.want)
		}
	}
}
