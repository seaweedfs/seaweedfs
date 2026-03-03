package blockvol

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
)

// TestResize_ExpandWorks grows a 1MB volume to 2MB, writes to the new region,
// and reads back to verify.
func TestResize_ExpandWorks(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "expand.blk")

	vol, err := CreateBlockVol(path, CreateOptions{
		VolumeSize: 1024 * 1024, // 1MB
		BlockSize:  4096,
		WALSize:    64 * 1024, // 64KB (small for test)
	})
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	defer vol.Close()

	// Write to last block of original size.
	lastLBA := uint64((1024*1024)/4096 - 1)
	data := make([]byte, 4096)
	for i := range data {
		data[i] = 0xAA
	}
	if err := vol.WriteLBA(lastLBA, data); err != nil {
		t.Fatalf("write last block: %v", err)
	}

	// Expand to 2MB.
	newSize := uint64(2 * 1024 * 1024)
	if err := vol.Expand(newSize); err != nil {
		t.Fatalf("expand: %v", err)
	}

	// Verify volume size updated.
	if vol.Info().VolumeSize != newSize {
		t.Fatalf("expected VolumeSize=%d, got %d", newSize, vol.Info().VolumeSize)
	}

	// Write to a block in the new region.
	newLBA := uint64((1024 * 1024) / 4096) // first block in expanded region
	data2 := make([]byte, 4096)
	for i := range data2 {
		data2[i] = 0xBB
	}
	if err := vol.WriteLBA(newLBA, data2); err != nil {
		t.Fatalf("write new region: %v", err)
	}

	// Read back old data.
	got, err := vol.ReadLBA(lastLBA, 4096)
	if err != nil {
		t.Fatalf("read old block: %v", err)
	}
	if !bytes.Equal(got, data) {
		t.Fatal("old block data mismatch after expand")
	}

	// Read back new data.
	got2, err := vol.ReadLBA(newLBA, 4096)
	if err != nil {
		t.Fatalf("read new block: %v", err)
	}
	if !bytes.Equal(got2, data2) {
		t.Fatal("new block data mismatch after expand")
	}

	// Verify file size on disk.
	fi, _ := os.Stat(path)
	extentStart := vol.super.WALOffset + vol.super.WALSize
	expected := int64(extentStart + newSize)
	if fi.Size() != expected {
		t.Fatalf("file size: expected %d, got %d", expected, fi.Size())
	}
}

// TestResize_ShrinkRejected verifies that attempting to shrink returns an error.
func TestResize_ShrinkRejected(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "shrink.blk")

	vol, err := CreateBlockVol(path, CreateOptions{
		VolumeSize: 1024 * 1024,
		BlockSize:  4096,
		WALSize:    64 * 1024,
	})
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	defer vol.Close()

	err = vol.Expand(512 * 1024) // shrink
	if err != ErrShrinkNotSupported {
		t.Fatalf("expected ErrShrinkNotSupported, got %v", err)
	}

	// Same size = no-op.
	if err := vol.Expand(1024 * 1024); err != nil {
		t.Fatalf("same-size expand should be no-op: %v", err)
	}
}

// TestResize_WithSnapshotsRejected verifies that resize is blocked when
// snapshots are active.
func TestResize_WithSnapshotsRejected(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "snap-resize.blk")

	vol, err := CreateBlockVol(path, CreateOptions{
		VolumeSize: 1024 * 1024,
		BlockSize:  4096,
		WALSize:    64 * 1024,
	})
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	defer vol.Close()

	// Create a snapshot.
	if err := vol.CreateSnapshot(1); err != nil {
		t.Fatalf("create snapshot: %v", err)
	}

	// Try to expand -- should fail.
	err = vol.Expand(2 * 1024 * 1024)
	if err != ErrSnapshotsPreventResize {
		t.Fatalf("expected ErrSnapshotsPreventResize, got %v", err)
	}

	// Delete snapshot, then expand should work.
	if err := vol.DeleteSnapshot(1); err != nil {
		t.Fatalf("delete snapshot: %v", err)
	}
	if err := vol.Expand(2 * 1024 * 1024); err != nil {
		t.Fatalf("expand after snapshot delete: %v", err)
	}
}
