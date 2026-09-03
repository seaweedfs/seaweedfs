package storage

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/storage/super_block"
)

// Pointing -dir.idx at a directory with no .idx used to abort the whole volume
// server in checkIdxFile; the index is derivable from the .dat, so it must be
// rebuilt in place instead.
func TestLoad_MovedIdxDirectory_RebuildsIdx(t *testing.T) {
	root := t.TempDir()
	dataDir := filepath.Join(root, "data")
	oldIdxDir := filepath.Join(root, "idxA")
	newIdxDir := filepath.Join(root, "idxB")
	for _, dir := range []string{dataDir, oldIdxDir, newIdxDir} {
		if err := os.MkdirAll(dir, 0755); err != nil {
			t.Fatalf("mkdir %s: %v", dir, err)
		}
	}

	v, err := NewVolume(dataDir, oldIdxDir, "", 1, NeedleMapInMemory, &super_block.ReplicaPlacement{}, &needle.TTL{}, 0, needle.GetCurrentVersion(), 0, 0)
	if err != nil {
		t.Fatalf("create volume: %v", err)
	}
	for id := uint64(1); id <= 3; id++ {
		if _, _, _, err := v.writeNeedle2(newRandomNeedle(id), true, false, false); err != nil {
			t.Fatalf("seed write %d: %v", id, err)
		}
	}
	if _, err := v.deleteNeedle2(newRandomNeedle(2)); err != nil {
		t.Fatalf("seed delete: %v", err)
	}
	wantCount, wantDeleted := v.nm.FileCount(), v.nm.DeletedCount()
	v.Close()

	if _, err := os.Stat(filepath.Join(oldIdxDir, "1.idx")); err != nil {
		t.Fatalf("seeded idx should live in the old idx dir: %v", err)
	}

	v2, err := NewVolume(dataDir, newIdxDir, "", 1, NeedleMapInMemory, &super_block.ReplicaPlacement{}, &needle.TTL{}, 0, needle.GetCurrentVersion(), 0, 0)
	if err != nil {
		t.Fatalf("reload against the new idx dir: %v", err)
	}
	defer v2.Close()

	if _, err := os.Stat(filepath.Join(newIdxDir, "1.idx")); err != nil {
		t.Fatalf("idx not rebuilt in the new idx dir: %v", err)
	}
	if got := v2.nm.FileCount(); got != wantCount {
		t.Errorf("file count = %d, want %d", got, wantCount)
	}
	if got := v2.nm.DeletedCount(); got != wantDeleted {
		t.Errorf("deleted count = %d, want %d", got, wantDeleted)
	}
	old, err := os.ReadFile(filepath.Join(oldIdxDir, "1.idx"))
	if err != nil {
		t.Fatalf("read the seeded idx: %v", err)
	}
	rebuilt, err := os.ReadFile(filepath.Join(newIdxDir, "1.idx"))
	if err != nil {
		t.Fatalf("read the rebuilt idx: %v", err)
	}
	if !bytes.Equal(old, rebuilt) {
		t.Errorf("rebuilt idx (%d bytes) differs from the one the server wrote (%d bytes)", len(rebuilt), len(old))
	}
	if v2.noWriteOrDelete {
		t.Errorf("volume marked read-only after the rebuild")
	}
}
