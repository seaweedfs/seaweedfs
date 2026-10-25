package storage

import (
	"errors"
	"os"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/storage/super_block"
)

// A volume booted with .vif ReadOnly=true used to come back stuck: load()
// opens .idx as O_RDONLY and builds a SortedFileNeedleMap whose Put returns
// os.ErrInvalid, and MarkVolumeWritable only flipped the in-memory flag and
// rewrote .vif. Subsequent writes failed at v.nm.Put.
func TestMarkVolumeWritable_ReopensPersistedReadOnly(t *testing.T) {
	dir := t.TempDir()

	v, err := NewVolume(dir, dir, "", 1, NeedleMapInMemory, &super_block.ReplicaPlacement{}, &needle.TTL{}, 0, needle.GetCurrentVersion(), 0, 0)
	if err != nil {
		t.Fatalf("create volume: %v", err)
	}

	if _, _, _, err := v.writeNeedle2(newRandomNeedle(1), true, false, false); err != nil {
		t.Fatalf("initial write: %v", err)
	}

	// Persist read-only state into .vif, then simulate a server restart by
	// closing and re-opening the volume from the same directory.
	if err := v.PersistReadOnly(true, false); err != nil {
		t.Fatalf("persist read-only: %v", err)
	}
	v.Close()

	v2, err := NewVolume(dir, dir, "", 1, NeedleMapInMemory, &super_block.ReplicaPlacement{}, &needle.TTL{}, 0, needle.GetCurrentVersion(), 0, 0)
	if err != nil {
		t.Fatalf("reload volume: %v", err)
	}
	defer v2.Close()

	if !v2.noWriteOrDelete {
		t.Fatalf("reloaded volume should be in noWriteOrDelete=true after .vif ReadOnly=true")
	}
	if _, ok := v2.nm.(*SortedFileNeedleMap); !ok {
		t.Fatalf("reloaded readonly volume should use SortedFileNeedleMap, got %T", v2.nm)
	}

	// Pre-fix behaviour: SortedFileNeedleMap.Put returns os.ErrInvalid even
	// once noWriteOrDelete is cleared. Confirm the failure mode the issue
	// describes — flipping only the flag is not enough.
	v2.noWriteOrDelete = false
	_, _, _, writeErr := v2.writeNeedle2(newRandomNeedle(2), true, false, false)
	if !errors.Is(writeErr, os.ErrInvalid) {
		t.Fatalf("expected write through SortedFileNeedleMap to fail with os.ErrInvalid, got %v", writeErr)
	}
	v2.noWriteOrDelete = true // restore so reopenIdxForWrite reflects the real entry condition

	if err := v2.reopenIdxForWrite(); err != nil {
		t.Fatalf("reopenIdxForWrite: %v", err)
	}
	if _, stillSorted := v2.nm.(*SortedFileNeedleMap); stillSorted {
		t.Fatalf("reopenIdxForWrite left SortedFileNeedleMap in place")
	}

	v2.noWriteOrDelete = false
	if _, _, _, err := v2.writeNeedle2(newRandomNeedle(3), true, false, false); err != nil {
		t.Fatalf("write after reopen: %v", err)
	}
}

// A remote-tiered volume marked writable keeps the delete-only mode load()
// derives — the remote .dat cannot take appends, so "writable" resolves to
// can-delete even when the mark left noWriteOrDelete without noWriteCanDelete.
func TestMarkVolumeWritableOnATieredVolumeKeepsDeletesOnly(t *testing.T) {
	b := newLocalDirBackend(t)
	registerTestBackend(t, b)

	v, _ := tierUpVolumeLive(t, t.TempDir(), 86, b)
	defer v.Close()
	// the upload-time mark as a refused MarkTieredDeletable leaves it
	v.noWriteOrDelete = true
	v.noWriteCanDelete = false

	location := newTestLocation(10, false, 0)
	location.SetVolume(86, v)
	s := &Store{Locations: []*DiskLocation{location}}
	if err := s.MarkVolumeWritable(86); err != nil {
		t.Fatalf("mark writable: %v", err)
	}

	readOnly, orDelete, canDelete, _ := v.ReadOnlyReasons()
	if orDelete || !canDelete || !readOnly {
		t.Fatalf("a tiered volume resolves writable to delete-only, got readOnly=%v orDelete=%v canDelete=%v",
			readOnly, orDelete, canDelete)
	}
}

// reopenIdxForWrite must be a no-op when the volume already has a writable
// needle map — otherwise repeated MarkVolumeWritable calls would churn the
// index file handle for no reason.
func TestReopenIdxForWrite_NoopWhenAlreadyWritable(t *testing.T) {
	dir := t.TempDir()
	v, err := NewVolume(dir, dir, "", 2, NeedleMapInMemory, &super_block.ReplicaPlacement{}, &needle.TTL{}, 0, needle.GetCurrentVersion(), 0, 0)
	if err != nil {
		t.Fatalf("create volume: %v", err)
	}
	defer v.Close()

	before := v.nm
	if err := v.reopenIdxForWrite(); err != nil {
		t.Fatalf("reopenIdxForWrite on writable volume: %v", err)
	}
	if v.nm != before {
		t.Fatalf("reopenIdxForWrite replaced nm on a writable volume (before=%p after=%p)", before, v.nm)
	}
}
