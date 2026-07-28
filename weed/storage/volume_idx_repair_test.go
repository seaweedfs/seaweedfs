package storage

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb/volume_server_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle_map"
	"github.com/seaweedfs/seaweedfs/weed/storage/super_block"
	"github.com/seaweedfs/seaweedfs/weed/storage/types"
	"github.com/seaweedfs/seaweedfs/weed/storage/volume_info"
)

// clobberIdxHead reproduces the damage a delete on a tiered read-only volume
// used to do: it writes (key, offset 0, tombstone) rows over the front of .idx
// instead of appending them.
func clobberIdxHead(t *testing.T, idxPath string, keys []uint64) {
	t.Helper()
	f, err := os.OpenFile(idxPath, os.O_WRONLY, 0644)
	if err != nil {
		t.Fatalf("open idx: %v", err)
	}
	defer f.Close()
	for i, key := range keys {
		row := needle_map.ToBytes(types.Uint64ToNeedleId(key), types.Offset{}, types.TombstoneFileSize)
		if _, err := f.WriteAt(row, int64(i*types.NeedleMapEntrySize)); err != nil {
			t.Fatalf("clobber row %d: %v", i, err)
		}
	}
}

func writeTestVolume(t *testing.T, dir string, needleCount int) map[uint64]*needle.Needle {
	t.Helper()
	v, err := NewVolume(dir, dir, "", 1, NeedleMapInMemory, &super_block.ReplicaPlacement{}, &needle.TTL{}, 0, needle.GetCurrentVersion(), 0, 0)
	if err != nil {
		t.Fatalf("volume creation: %v", err)
	}
	written := make(map[uint64]*needle.Needle)
	for i := 1; i <= needleCount; i++ {
		n := newRandomNeedle(uint64(i))
		if _, _, _, err := v.writeNeedle2(n, true, false); err != nil {
			v.Close()
			t.Fatalf("write needle %d: %v", i, err)
		}
		written[uint64(i)] = n
	}
	v.Close()
	return written
}

func mustReload(t *testing.T, dir string) *Volume {
	t.Helper()
	v, err := loadVolumeWithoutWorker(dir, dir, "", 1, NeedleMapInMemory, 0)
	if err != nil {
		t.Fatalf("reload volume: %v", err)
	}
	return v
}

func mustRead(t *testing.T, v *Volume, id uint64) []byte {
	t.Helper()
	n := newEmptyNeedle(id)
	if _, err := v.readNeedle(n, nil, nil); err != nil {
		t.Fatalf("read needle %d: %v", id, err)
	}
	return n.Data
}

// TestRepairIdxHeadTombstones_RestoresClobberedRows checks that a volume whose
// .idx head was overwritten with offset-0 tombstones serves its first needles
// again after a reload, without a full weed fix rebuild.
func TestRepairIdxHeadTombstones_RestoresClobberedRows(t *testing.T) {
	dir := t.TempDir()
	const needleCount = 12
	const clobbered = 4

	written := writeTestVolume(t, dir, needleCount)
	idxPath := filepath.Join(dir, "1.idx")
	sizeBefore := fileSize(t, idxPath)

	// Deletes against needles 9..12 land on the front of .idx and take the
	// rows indexing needles 1..4 with them.
	clobberIdxHead(t, idxPath, []uint64{9, 10, 11, 12})

	v := mustReload(t, dir)
	defer v.Close()

	for i := uint64(1); i <= needleCount; i++ {
		if i >= 9 {
			// genuinely deleted by the tombstones that did the damage
			continue
		}
		if got, want := mustRead(t, v, i), written[i].Data; string(got) != string(want) {
			t.Fatalf("needle %d: data mismatch after recovery", i)
		}
	}

	if got, want := fileSize(t, idxPath), sizeBefore+int64(clobbered*types.NeedleMapEntrySize); got != want {
		t.Fatalf("idx size after recovery: got %d, want %d", got, want)
	}

	// The recovered rows go back in front, so .idx is in .dat append order
	// again: the fingerprint is gone and the last row is still the .dat tail.
	entries := readAllIdxEntries(t, idxPath)
	if entries[0].offset.ToActualOffset() != int64(v.SuperBlock.BlockSize()) {
		t.Fatalf("first row does not index the first needle in .dat: %+v", entries[0])
	}
	for i := 1; i < clobbered; i++ {
		if entries[i-1].offset.ToActualOffset() >= entries[i].offset.ToActualOffset() {
			t.Fatalf("recovered rows are not in .dat order: %+v then %+v", entries[i-1], entries[i])
		}
	}

	// The recovery is idempotent: a second load finds nothing left to restore.
	v.Close()
	v = mustReload(t, dir)
	if got, want := fileSize(t, idxPath), sizeBefore+int64(clobbered*types.NeedleMapEntrySize); got != want {
		t.Fatalf("idx grew on second load: got %d, want %d", got, want)
	}
}

// TestRepairIdxHeadTombstones_ReadOnlyVolume covers the shape the damage
// actually occurs in: a read-only volume, whose reads go through the sorted
// needle map. The recovery has to land before .sdx is regenerated, or the
// restored rows never reach the map that answers the read.
func TestRepairIdxHeadTombstones_ReadOnlyVolume(t *testing.T) {
	dir := t.TempDir()
	written := writeTestVolume(t, dir, 8)
	clobberIdxHead(t, filepath.Join(dir, "1.idx"), []uint64{7, 8})

	if err := volume_info.SaveVolumeInfo(filepath.Join(dir, "1.vif"), &volume_server_pb.VolumeInfo{
		Version:  uint32(needle.GetCurrentVersion()),
		ReadOnly: true,
	}); err != nil {
		t.Fatalf("save vif: %v", err)
	}

	v := mustReload(t, dir)
	defer v.Close()

	if _, isSorted := v.nm.(*SortedFileNeedleMap); !isSorted {
		t.Fatalf("expected a sorted needle map, got %T", v.nm)
	}
	for i := uint64(1); i <= 2; i++ {
		if got, want := mustRead(t, v, i), written[i].Data; string(got) != string(want) {
			t.Fatalf("needle %d: data mismatch after recovery", i)
		}
	}
}

// TestRepairIdxHeadTombstones_LeavesHealthyIdxAlone guards the fingerprint: a
// volume with ordinary deletes must not be rewritten.
func TestRepairIdxHeadTombstones_LeavesHealthyIdxAlone(t *testing.T) {
	dir := t.TempDir()
	writeTestVolume(t, dir, 6)
	idxPath := filepath.Join(dir, "1.idx")

	v := mustReload(t, dir)
	if _, err := v.doDeleteRequest(newEmptyNeedle(3)); err != nil {
		t.Fatalf("delete needle 3: %v", err)
	}
	v.Close()

	sizeBefore := fileSize(t, idxPath)
	entriesBefore := readAllIdxEntries(t, idxPath)

	v = mustReload(t, dir)
	defer v.Close()

	if got := fileSize(t, idxPath); got != sizeBefore {
		t.Fatalf("healthy idx was rewritten: got %d, want %d", got, sizeBefore)
	}
	if got := readAllIdxEntries(t, idxPath); len(got) != len(entriesBefore) {
		t.Fatalf("healthy idx row count changed: got %d, want %d", len(got), len(entriesBefore))
	}
	if _, err := v.readNeedle(newEmptyNeedle(3), nil, nil); err != ErrorDeleted {
		t.Fatalf("needle 3 should stay deleted, got %v", err)
	}
}

// TestRepairIdxHeadTombstones_KeepsDeletedNeedlesDeleted checks that a needle
// deleted before the damage is not resurrected: its tombstone row survives at
// the tail of .idx, so the key is still indexed and stays out of the recovery.
// It stays unreadable either way -- as deleted if its Put row survived, as
// not-found if the tombstone is all the .idx has left of it.
func TestRepairIdxHeadTombstones_KeepsDeletedNeedlesDeleted(t *testing.T) {
	dir := t.TempDir()
	writeTestVolume(t, dir, 8)
	idxPath := filepath.Join(dir, "1.idx")

	v := mustReload(t, dir)
	if _, err := v.doDeleteRequest(newEmptyNeedle(2)); err != nil {
		t.Fatalf("delete needle 2: %v", err)
	}
	v.Close()

	clobberIdxHead(t, idxPath, []uint64{7, 8})

	v = mustReload(t, dir)
	defer v.Close()

	if _, err := v.readNeedle(newEmptyNeedle(2), nil, nil); err != ErrorDeleted && err != ErrorNotFound {
		t.Fatalf("needle 2 was resurrected, got %v", err)
	}
	if _, err := v.readNeedle(newEmptyNeedle(1), nil, nil); err != nil {
		t.Fatalf("needle 1 should have been recovered: %v", err)
	}
}
