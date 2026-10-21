package operation

import (
	"os"
	"path/filepath"
	"testing"
)

func TestNewFilePartsClosesOnError(t *testing.T) {
	fdsBefore, err := os.ReadDir("/proc/self/fd")
	if err != nil {
		t.Skip("fd counting requires /proc")
	}
	dir := t.TempDir()
	good := filepath.Join(dir, "good.txt")
	if err := os.WriteFile(good, []byte("x"), 0644); err != nil {
		t.Fatal(err)
	}
	if _, err := NewFileParts([]string{good, filepath.Join(dir, "missing.txt")}); err == nil {
		t.Fatal("expected error for missing file")
	}
	fdsAfter, err := os.ReadDir("/proc/self/fd")
	if err != nil {
		t.Skip("fd counting requires /proc")
	}
	if len(fdsAfter) != len(fdsBefore) {
		t.Errorf("fd count changed by %d: a file part leaked its handle", len(fdsAfter)-len(fdsBefore))
	}
}
