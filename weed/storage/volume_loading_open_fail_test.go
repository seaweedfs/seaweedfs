package storage

import (
	"os"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/storage/super_block"
)

// TestLoad_DatOpenFail_NoNilPanic verifies that when the .dat file cannot be
// opened (simulated by replacing it with a directory, which fails open even
// as root), load returns a non-nil error rather than panicking on a nil
// *os.File inside backend.NewDiskFile.  Reproduces seaweedfs/seaweedfs#11615.
func TestLoad_DatOpenFail_NoNilPanic(t *testing.T) {
	dir := t.TempDir()

	// Create a healthy volume so the .dat/.idx files exist.
	v, err := NewVolume(dir, dir, "", 1, NeedleMapInMemory, &super_block.ReplicaPlacement{}, &needle.TTL{}, 0, needle.GetCurrentVersion(), 0, 0)
	if err != nil {
		t.Fatalf("create volume: %v", err)
	}
	v.Close()

	// Replace the .dat file with a directory; opening a directory with
	// O_RDWR|O_CREATE fails with EISDIR even when the process runs as root.
	datPath := VolumeFileName(dir, "", 1) + ".dat"
	if err := os.Remove(datPath); err != nil {
		t.Fatalf("remove .dat: %v", err)
	}
	if err := os.Mkdir(datPath, 0755); err != nil {
		t.Fatalf("mkdir .dat: %v", err)
	}

	// Before the fix this panicked inside backend.NewDiskFile(nil).
	v2, err := NewVolume(dir, dir, "", 1, NeedleMapInMemory, &super_block.ReplicaPlacement{}, &needle.TTL{}, 0, needle.GetCurrentVersion(), 0, 0)
	if err == nil {
		v2.Close()
		t.Fatal("expected error when .dat cannot be opened, got nil")
	}
}
