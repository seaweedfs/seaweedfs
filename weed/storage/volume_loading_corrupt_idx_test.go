package storage

import (
	"os"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/storage/super_block"
)

// Corrupt .idx made NewSortedFileNeedleMap return a typed-nil into v.nm;
// the post-load MaxNeedleEnd check then segfaulted on the nil receiver.
func TestLoad_CorruptIdx_NoSegfault(t *testing.T) {
	dir := t.TempDir()

	v, err := NewVolume(dir, dir, "", 1, NeedleMapInMemory, &super_block.ReplicaPlacement{}, &needle.TTL{}, 0, needle.GetCurrentVersion(), 0, 0)
	if err != nil {
		t.Fatalf("create volume: %v", err)
	}
	if _, _, _, err := v.writeNeedle2(newRandomNeedle(1), true, false); err != nil {
		t.Fatalf("seed write: %v", err)
	}
	v.PersistReadOnly(true) // reload goes through SortedFileNeedleMap
	v.Close()

	// Truncate .idx to a non-aligned size so the walk rejects it.
	idxPath := VolumeFileName(dir, "", 1) + ".idx"
	st, err := os.Stat(idxPath)
	if err != nil {
		t.Fatalf("stat idx: %v", err)
	}
	if err := os.Truncate(idxPath, st.Size()-1); err != nil {
		t.Fatalf("truncate idx: %v", err)
	}

	// Pre-fix this panicked inside (*mapMetric).MaxNeedleEnd.
	v2, err := NewVolume(dir, dir, "", 1, NeedleMapInMemory, &super_block.ReplicaPlacement{}, &needle.TTL{}, 0, needle.GetCurrentVersion(), 0, 0)
	if err == nil {
		v2.Close()
	}
}
