package operation

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/glog"
)

func TestNewFilePartsClosesOnError(t *testing.T) {
	dir := t.TempDir()
	missing := filepath.Join(dir, "missing.txt")
	if _, err := NewFileParts([]string{missing}); err == nil {
		t.Skip("missing file unexpectedly opened")
	}
	glog.Flush()

	fdsBefore, err := os.ReadDir("/proc/self/fd")
	if err != nil {
		t.Skip("fd counting requires /proc")
	}
	good := filepath.Join(dir, "good.txt")
	if err := os.WriteFile(good, []byte("x"), 0644); err != nil {
		t.Fatal(err)
	}
	if _, err := NewFileParts([]string{good, missing}); err == nil {
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
