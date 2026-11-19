package testutil

import (
	"crypto/sha256"
	"encoding/hex"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
)

func TestDownloadVerified(t *testing.T) {
	body := []byte("jar contents")
	sum := sha256.Sum256(body)
	want := hex.EncodeToString(sum[:])
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/a.jar" {
			http.NotFound(w, r)
			return
		}
		w.Write(body)
	}))
	defer srv.Close()

	dir := t.TempDir()
	path := filepath.Join(dir, "a.jar")

	if err := downloadVerified(srv.URL+"/missing.jar", path, want); err == nil {
		t.Fatal("a 404 should fail")
	}
	if err := downloadVerified(srv.URL+"/a.jar", path, "00"); err == nil {
		t.Fatal("a checksum mismatch should fail")
	}
	if entries, _ := os.ReadDir(dir); len(entries) != 0 {
		t.Fatalf("failed downloads left %d files behind", len(entries))
	}

	if err := downloadVerified(srv.URL+"/a.jar", path, want); err != nil {
		t.Fatalf("download: %v", err)
	}
	if got := fileSHA256(path); got != want {
		t.Fatalf("sha256 %s, want %s", got, want)
	}
}
