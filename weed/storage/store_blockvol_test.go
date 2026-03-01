package storage

import (
	"path/filepath"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/storage/blockvol"
)

func createTestBlockVol(t *testing.T, dir, name string) string {
	t.Helper()
	path := filepath.Join(dir, name)
	vol, err := blockvol.CreateBlockVol(path, blockvol.CreateOptions{
		VolumeSize: 1024 * 4096,
		BlockSize:  4096,
		ExtentSize: 65536,
		WALSize:    1 << 20,
	})
	if err != nil {
		t.Fatal(err)
	}
	vol.Close()
	return path
}

func TestStoreAddBlockVolume(t *testing.T) {
	dir := t.TempDir()
	path := createTestBlockVol(t, dir, "vol1.blk")

	bs := NewBlockVolumeStore()
	defer bs.Close()

	vol, err := bs.AddBlockVolume(path)
	if err != nil {
		t.Fatal(err)
	}
	if vol == nil {
		t.Fatal("expected non-nil volume")
	}

	// Duplicate add should fail.
	_, err = bs.AddBlockVolume(path)
	if err == nil {
		t.Fatal("expected error on duplicate add")
	}

	// Should be listed.
	paths := bs.ListBlockVolumes()
	if len(paths) != 1 || paths[0] != path {
		t.Fatalf("ListBlockVolumes: got %v", paths)
	}

	// Should be gettable.
	got, ok := bs.GetBlockVolume(path)
	if !ok || got != vol {
		t.Fatal("GetBlockVolume failed")
	}
}

func TestStoreRemoveBlockVolume(t *testing.T) {
	dir := t.TempDir()
	path := createTestBlockVol(t, dir, "vol1.blk")

	bs := NewBlockVolumeStore()
	defer bs.Close()

	if _, err := bs.AddBlockVolume(path); err != nil {
		t.Fatal(err)
	}

	if err := bs.RemoveBlockVolume(path); err != nil {
		t.Fatal(err)
	}

	// Should be gone.
	if _, ok := bs.GetBlockVolume(path); ok {
		t.Fatal("volume should have been removed")
	}

	// Remove again should fail.
	if err := bs.RemoveBlockVolume(path); err == nil {
		t.Fatal("expected error on double remove")
	}
}

func TestStoreCloseAllBlockVolumes(t *testing.T) {
	dir := t.TempDir()
	path1 := createTestBlockVol(t, dir, "vol1.blk")
	path2 := createTestBlockVol(t, dir, "vol2.blk")

	bs := NewBlockVolumeStore()

	if _, err := bs.AddBlockVolume(path1); err != nil {
		t.Fatal(err)
	}
	if _, err := bs.AddBlockVolume(path2); err != nil {
		t.Fatal(err)
	}

	if len(bs.ListBlockVolumes()) != 2 {
		t.Fatalf("expected 2 volumes, got %d", len(bs.ListBlockVolumes()))
	}

	bs.Close()

	if len(bs.ListBlockVolumes()) != 0 {
		t.Fatalf("expected 0 volumes after close, got %d", len(bs.ListBlockVolumes()))
	}
}
