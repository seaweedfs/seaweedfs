package glog

import (
	"compress/gzip"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestCompressRotated_Toggle(t *testing.T) {
	if IsCompressRotated() {
		t.Error("compression should be off by default")
	}

	SetCompressRotated(true)
	if !IsCompressRotated() {
		t.Error("compression should be on after SetCompressRotated(true)")
	}

	SetCompressRotated(false)
	if IsCompressRotated() {
		t.Error("compression should be off after SetCompressRotated(false)")
	}
}

func TestCompressFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.log")

	content := "line 1\nline 2\nline 3\n"
	if err := os.WriteFile(path, []byte(content), 0644); err != nil {
		t.Fatalf("failed to write test file: %v", err)
	}

	compressFile(path)

	// Original should be deleted
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Error("original file should be deleted after compression")
	}

	// .gz should exist
	gzPath := path + ".gz"
	if _, err := os.Stat(gzPath); err != nil {
		t.Fatalf(".gz file should exist: %v", err)
	}

	// Verify .gz content
	f, err := os.Open(gzPath)
	if err != nil {
		t.Fatalf("failed to open .gz: %v", err)
	}
	defer f.Close()

	gz, err := gzip.NewReader(f)
	if err != nil {
		t.Fatalf("failed to create gzip reader: %v", err)
	}
	defer gz.Close()

	decompressed, err := io.ReadAll(gz)
	if err != nil {
		t.Fatalf("failed to decompress: %v", err)
	}

	if string(decompressed) != content {
		t.Errorf("decompressed content mismatch: got %q, want %q", string(decompressed), content)
	}
}

func TestCompressFile_AlreadyGz(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.log.gz")

	if err := os.WriteFile(path, []byte("fake gz"), 0644); err != nil {
		t.Fatal(err)
	}

	// Should skip .gz files
	compressFile(path)

	// File should still exist unchanged
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != "fake gz" {
		t.Error("should not modify .gz files")
	}

	// No .gz.gz should exist
	if _, err := os.Stat(path + ".gz"); !os.IsNotExist(err) {
		t.Error(".gz.gz should not be created")
	}
}

func TestCompressFile_NonExistent(t *testing.T) {
	// Should not panic on missing file
	missing := filepath.Join(t.TempDir(), "does-not-exist.log")
	compressFile(missing)
}

func TestCompressFile_EmptyFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "empty.log")

	if err := os.WriteFile(path, []byte{}, 0644); err != nil {
		t.Fatal(err)
	}

	compressFile(path)

	// .gz should exist even for empty files
	gzPath := path + ".gz"
	if _, err := os.Stat(gzPath); err != nil {
		t.Errorf(".gz should exist for empty file: %v", err)
	}

	// Original should be removed
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Error("original empty file should be deleted")
	}
}

func TestCompressFile_LargeContent(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "large.log")

	// Write 1MB of repetitive content (compresses well)
	content := strings.Repeat("I0319 12:00:00.000000 12345 server.go:42] repeated log line\n", 15000)
	if err := os.WriteFile(path, []byte(content), 0644); err != nil {
		t.Fatal(err)
	}

	origSize := int64(len(content))
	compressFile(path)

	gzPath := path + ".gz"
	info, err := os.Stat(gzPath)
	if err != nil {
		t.Fatalf(".gz missing: %v", err)
	}

	// Compressed should be significantly smaller
	ratio := float64(info.Size()) / float64(origSize) * 100
	if ratio > 20 {
		t.Errorf("compression ratio too high: %.1f%% (expected < 20%% for repetitive logs)", ratio)
	}
}
