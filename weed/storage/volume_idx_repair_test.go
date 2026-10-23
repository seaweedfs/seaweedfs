package storage

import (
	"bytes"
	"errors"
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
		if _, _, _, err := v.writeNeedle2(n, true, false, false); err != nil {
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

	// The rewrite replaces .idx wholesale, so it must not widen the mode.
	if err := os.Chmod(idxPath, 0600); err != nil {
		t.Fatalf("chmod idx: %v", err)
	}

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

	if st, err := os.Stat(idxPath); err != nil {
		t.Fatalf("stat idx: %v", err)
	} else if got := st.Mode().Perm(); got != 0600 {
		t.Fatalf("idx mode after recovery: got %o, want 600", got)
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

// TestStaleIndexEntry_HealsOnWrite covers a damaged index entry that resolves
// to a different needle: reads must miss instead of serving the wrong needle,
// and the next write for the key must re-index it rather than be rejected as
// a cookie conflict.
func TestStaleIndexEntry_HealsOnWrite(t *testing.T) {
	dir := t.TempDir()
	written := writeTestVolume(t, dir, 5)

	v := mustReload(t, dir)
	defer v.Close()

	// point key 9 at needle 2's record with a size that does not match the
	// record, as a corrupt index entry would look
	const staleKey = 9
	nv, ok := v.nm.Get(types.Uint64ToNeedleId(2))
	if !ok {
		t.Fatal("needle 2 missing from index")
	}
	if err := v.nm.Put(types.Uint64ToNeedleId(staleKey), nv.Offset, nv.Size+1); err != nil {
		t.Fatalf("plant stale index entry: %v", err)
	}

	if _, err := v.readNeedle(newEmptyNeedle(staleKey), nil, nil); err == nil {
		t.Fatal("stale-index read served needle 2's data")
	}

	stream := newEmptyNeedle(staleKey)
	stream.DataSize = written[2].DataSize
	if err := v.readNeedleDataInto(stream, &ReadOption{ReadBufferSize: 1024}, &bytes.Buffer{}, 0, int64(stream.DataSize)); err != ErrorNotFound {
		t.Fatalf("stale-index stream: want ErrorNotFound, got %v", err)
	}

	n := newRandomNeedle(staleKey)
	n.Cookie = 0xABCD
	if _, _, _, err := v.writeNeedle2(n, true, false, false); err != nil {
		t.Fatalf("write over stale index entry: %v", err)
	}
	if got := mustRead(t, v, staleKey); !bytes.Equal(got, n.Data) {
		t.Fatalf("needle %d after re-index: got %d bytes, want %d", staleKey, len(got), len(n.Data))
	}
}

// TestStaleIndexEntry_SurvivesFailedWrite checks that a stale index entry is
// only replaced once the new record commits: a durable write rolled back by
// a failed fsync leaves the index exactly as it found it.
func TestStaleIndexEntry_SurvivesFailedWrite(t *testing.T) {
	v, counting := newCountingVolume(t)

	existing := fixedNeedle(2, "existing")
	if _, _, _, err := v.writeNeedle2(existing, true, true, true); err != nil {
		t.Fatalf("write needle 2: %v", err)
	}
	nv, ok := v.nm.Get(existing.Id)
	if !ok {
		t.Fatal("needle 2 missing from index")
	}

	staleId := types.Uint64ToNeedleId(9)
	if err := v.nm.Put(staleId, nv.Offset, nv.Size+1); err != nil {
		t.Fatalf("plant stale index entry: %v", err)
	}

	counting.syncErr = errors.New("batch fsync failed")
	if _, _, _, err := v.writeNeedle2(fixedNeedle(9, "replacement"), true, true, false); err == nil {
		t.Fatal("a write whose fsync failed must not be acknowledged")
	}

	entry, ok := v.nm.Get(staleId)
	if !ok {
		t.Fatal("a failed write dropped the index entry it meant to replace")
	}
	if entry.Offset != nv.Offset || entry.Size != nv.Size+1 {
		t.Fatalf("index entry changed under a failed write: %+v, want %+v", entry, nv)
	}
}

// TestStaleIndexEntry_KeepsCookieConflictForSameKey checks that an entry
// resolving to the same needle id still enforces the cookie check: only
// cross-key corruption is dropped.
func TestStaleIndexEntry_KeepsCookieConflictForSameKey(t *testing.T) {
	dir := t.TempDir()
	writeTestVolume(t, dir, 3)

	v := mustReload(t, dir)
	defer v.Close()

	n := newRandomNeedle(2)
	n.Cookie = 0xABCD
	if _, _, _, err := v.writeNeedle2(n, true, false, false); err == nil {
		t.Fatal("write with a foreign cookie over an existing key should be rejected")
	}
}
