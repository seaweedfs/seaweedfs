package parquet_pushdown

import (
	"context"
	"os"
	"path/filepath"
	"testing"
)

func TestLocalLoader_Open(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "a.bin")
	want := []byte("hello pushdown")
	if err := os.WriteFile(path, want, 0o600); err != nil {
		t.Fatalf("write: %v", err)
	}

	h, err := LocalLoader{}.Open(context.Background(), path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer h.Close()

	if h.Size() != int64(len(want)) {
		t.Errorf("Size = %d, want %d", h.Size(), len(want))
	}
	got := make([]byte, len(want))
	n, err := h.ReadAt(got, 0)
	if err != nil {
		t.Fatalf("ReadAt: %v", err)
	}
	if n != len(want) || string(got) != string(want) {
		t.Errorf("got %q (n=%d), want %q", got[:n], n, want)
	}
}

func TestLocalLoader_RejectsMissing(t *testing.T) {
	if _, err := (LocalLoader{}).Open(context.Background(), filepath.Join(t.TempDir(), "missing.bin")); err == nil {
		t.Error("expected error opening missing file")
	}
}

func TestLocalLoader_RejectsDirectory(t *testing.T) {
	dir := t.TempDir()
	if _, err := (LocalLoader{}).Open(context.Background(), dir); err == nil {
		t.Error("expected error opening a directory")
	}
}
