package storage

import (
	"os"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/storage/super_block"
)

// A read-only volume whose .idx fails the size-multiple check made
// NewSortedFileNeedleMap return a typed-nil pointer. The multi-value
// assignment to the v.nm NeedleMapper interface stored that as a non-nil
// interface wrapping a nil concrete value, so the post-load MaxNeedleEnd
// structural check passed its v.nm != nil guard and then segfaulted
// dereferencing the embedded mapMetric.
func TestLoad_CorruptIdx_NoSegfault(t *testing.T) {
	dir := t.TempDir()

	v, err := NewVolume(dir, dir, "", 1, NeedleMapInMemory, &super_block.ReplicaPlacement{}, &needle.TTL{}, 0, needle.GetCurrentVersion(), 0, 0)
	if err != nil {
		t.Fatalf("create volume: %v", err)
	}
	if _, _, _, err := v.writeNeedle2(newRandomNeedle(1), true, false); err != nil {
		t.Fatalf("seed write: %v", err)
	}
	// Persist read-only so the reload picks the SortedFileNeedleMap branch.
	v.PersistReadOnly(true)
	v.Close()

	// Truncate .idx to a non-multiple of NeedleMapEntrySize. The walk rejects
	// that with "unexpected file size", so NewSortedFileNeedleMap returns
	// (nil, err) — the typed-nil shape that triggered the segfault.
	idxPath := VolumeFileName(dir, "", 1) + ".idx"
	st, err := os.Stat(idxPath)
	if err != nil {
		t.Fatalf("stat idx: %v", err)
	}
	if err := os.Truncate(idxPath, st.Size()-1); err != nil {
		t.Fatalf("truncate idx: %v", err)
	}

	// Pre-fix this panicked inside (*mapMetric).MaxNeedleEnd. Either an
	// error or a clean reload is fine; what matters is no crash.
	v2, err := NewVolume(dir, dir, "", 1, NeedleMapInMemory, &super_block.ReplicaPlacement{}, &needle.TTL{}, 0, needle.GetCurrentVersion(), 0, 0)
	if err == nil {
		v2.Close()
	}
}
