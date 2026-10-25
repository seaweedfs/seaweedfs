package storage

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/storage/idx"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	. "github.com/seaweedfs/seaweedfs/weed/storage/types"
)

// TestSortedFileNeedleMap_DeleteAppendsTombstone guards against a regression
// where Delete() overwrote the front of .idx with tombstones because
// NewSortedFileNeedleMap forgot to seed baseNeedleMapper.indexFileOffset.
func TestSortedFileNeedleMap_DeleteAppendsTombstone(t *testing.T) {
	dir := t.TempDir()
	baseName := filepath.Join(dir, "v1")
	idxPath := baseName + ".idx"

	const putCount = 16

	// Populate .idx with putCount Put records via the in-memory needle map.
	idxFile, err := os.Create(idxPath)
	if err != nil {
		t.Fatalf("create idx: %v", err)
	}
	writer := NewCompactNeedleMap(idxFile)
	for i := 0; i < putCount; i++ {
		key := Uint64ToNeedleId(uint64(i + 1))
		off := Uint32ToOffset(uint32((i + 1) * 8))
		if err := writer.Put(key, off, Size(1024)); err != nil {
			writer.Close()
			t.Fatalf("put %d: %v", i, err)
		}
	}
	writer.Close()

	wantIdxSize := int64(putCount * NeedleMapEntrySize)
	if got := fileSize(t, idxPath); got != wantIdxSize {
		t.Fatalf("idx size after seed: got %d, want %d", got, wantIdxSize)
	}

	// Re-open and construct the sorted-file needle map, which is what a
	// read-only volume load goes through.
	idxFile, err = os.OpenFile(idxPath, os.O_RDWR, 0644)
	if err != nil {
		t.Fatalf("reopen idx: %v", err)
	}
	m, err := NewSortedFileNeedleMap(baseName, idxFile, needle.GetCurrentVersion())
	if err != nil {
		t.Fatalf("NewSortedFileNeedleMap: %v", err)
	}
	defer m.Close()

	if m.indexFileOffset != wantIdxSize {
		t.Fatalf("indexFileOffset after open: got %d, want %d", m.indexFileOffset, wantIdxSize)
	}

	// Delete one of the live needles. This should append a tombstone to the
	// tail of .idx, not overwrite the head.
	deletedKey := Uint64ToNeedleId(7)
	deletedOff := Uint32ToOffset(uint32(7 * 8))
	if err := m.Delete(deletedKey, deletedOff); err != nil {
		t.Fatalf("Delete: %v", err)
	}

	if got, want := fileSize(t, idxPath), wantIdxSize+int64(NeedleMapEntrySize); got != want {
		t.Fatalf("idx size after delete: got %d, want %d (front-overwrite regression)", got, want)
	}

	// Walk .idx and verify all original Put records are intact and the
	// tombstone landed at the tail.
	entries := readAllIdxEntries(t, idxPath)
	if len(entries) != putCount+1 {
		t.Fatalf("entry count: got %d, want %d", len(entries), putCount+1)
	}
	for i := 0; i < putCount; i++ {
		wantKey := Uint64ToNeedleId(uint64(i + 1))
		wantOff := Uint32ToOffset(uint32((i + 1) * 8))
		wantSize := Size(1024)
		if entries[i].key != wantKey {
			t.Fatalf("entry[%d].key: got %d, want %d (front of idx was overwritten)", i, entries[i].key, wantKey)
		}
		if entries[i].offset != wantOff {
			t.Fatalf("entry[%d].offset: got %s, want %s", i, entries[i].offset, wantOff)
		}
		if entries[i].size != wantSize {
			t.Fatalf("entry[%d].size: got %d, want %d", i, entries[i].size, wantSize)
		}
		if entries[i].size.IsDeleted() {
			t.Fatalf("entry[%d] is unexpectedly a tombstone (front-overwrite regression)", i)
		}
	}
	tail := entries[putCount]
	if tail.key != deletedKey {
		t.Fatalf("tombstone key: got %d, want %d", tail.key, deletedKey)
	}
	if !tail.size.IsDeleted() {
		t.Fatalf("tail entry is not a tombstone: size=%d", tail.size)
	}
}

type idxEntry struct {
	key    NeedleId
	offset Offset
	size   Size
}

func readAllIdxEntries(t *testing.T, path string) []idxEntry {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		t.Fatalf("open idx: %v", err)
	}
	defer f.Close()
	var out []idxEntry
	if err := idx.WalkIndexFile(f, 0, func(key NeedleId, offset Offset, size Size) error {
		out = append(out, idxEntry{key: key, offset: offset, size: size})
		return nil
	}); err != nil {
		t.Fatalf("walk idx: %v", err)
	}
	return out
}

func fileSize(t *testing.T, path string) int64 {
	t.Helper()
	st, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat %s: %v", path, err)
	}
	return st.Size()
}
