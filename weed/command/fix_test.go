package command

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/storage/idx"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle_map"
	"github.com/seaweedfs/seaweedfs/weed/storage/types"
)

// TestSaveToIdxWritesOffsetOrder guards that weed fix rebuilds the .idx as an
// append-ordered log (sorted by .dat offset), not sorted by key. A key-sorted
// .idx puts the highest-key needle last instead of the .dat-tail needle, which
// flipped volumes read-only on load (issue #9688). It must also carry every
// tombstone — including one whose live needle is gone — so the last entry is
// the real .dat tail.
func TestSaveToIdxWritesOffsetOrder(t *testing.T) {
	nm := needle_map.NewMemDb()
	defer nm.Close()
	nmDeleted := needle_map.NewMemDb()
	defer nmDeleted.Close()

	// Live needles with high keys at low offsets (as if written first).
	for _, e := range []struct {
		key    uint64
		offset int64
	}{{30, 8}, {20, 128}, {10, 256}} {
		if err := nm.Set(types.Uint64ToNeedleId(e.key), types.ToOffset(e.offset), types.Size(100)); err != nil {
			t.Fatalf("nm.Set: %v", err)
		}
	}
	// A tombstone at the .dat tail whose live needle is gone; the old SaveToIdx
	// dropped these because the key was absent from the live map.
	if err := nmDeleted.Set(types.Uint64ToNeedleId(5), types.ToOffset(384), types.TombstoneFileSize); err != nil {
		t.Fatalf("nmDeleted.Set: %v", err)
	}

	scanner := &VolumeFileScanner4Fix{nm: nm, nmDeleted: nmDeleted, includeDeleted: true}

	idxPath := filepath.Join(t.TempDir(), "v.idx")
	if err := SaveToIdx(scanner, idxPath); err != nil {
		t.Fatalf("SaveToIdx: %v", err)
	}

	f, err := os.Open(idxPath)
	if err != nil {
		t.Fatalf("open idx: %v", err)
	}
	defer f.Close()

	var offsets []int64
	var lastKey types.NeedleId
	if err := idx.WalkIndexFile(f, 0, func(key types.NeedleId, offset types.Offset, size types.Size) error {
		offsets = append(offsets, offset.ToActualOffset())
		lastKey = key
		return nil
	}); err != nil {
		t.Fatalf("walk idx: %v", err)
	}

	if len(offsets) != 4 {
		t.Fatalf("expected 4 entries (3 live + 1 tombstone), got %d", len(offsets))
	}
	for i := 1; i < len(offsets); i++ {
		if offsets[i] < offsets[i-1] {
			t.Fatalf("entries not in offset order: %v", offsets)
		}
	}
	// The .dat-tail needle (the orphan tombstone at the highest offset) must be last.
	if lastKey != types.Uint64ToNeedleId(5) {
		t.Errorf("last entry should be the .dat-tail tombstone (key 5), got key %v", lastKey)
	}
}
