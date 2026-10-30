package storage

import (
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/storage/super_block"
	"github.com/seaweedfs/seaweedfs/weed/storage/types"
)

func TestBinarySearch(t *testing.T) {
	var testInput []int
	testInput = []int{-1, 0, 3, 5, 9, 12}

	if 3 != binarySearchForLargerThanTarget(testInput, 4) {
		t.Errorf("failed to find target %d", 4)
	}
	if 3 != binarySearchForLargerThanTarget(testInput, 3) {
		t.Errorf("failed to find target %d", 3)
	}
	if 6 != binarySearchForLargerThanTarget(testInput, 12) {
		t.Errorf("failed to find target %d", 12)
	}
	if 1 != binarySearchForLargerThanTarget(testInput, -1) {
		t.Errorf("failed to find target %d", -1)
	}
	if 0 != binarySearchForLargerThanTarget(testInput, -2) {
		t.Errorf("failed to find target %d", -2)
	}

}

func binarySearchForLargerThanTarget(nums []int, target int) int {
	l := 0
	h := len(nums)
	for l < h {
		m := (l + h) / 2
		if nums[m] <= target {
			l = m + 1
		} else {
			h = m
		}
	}
	return l
}

func TestResumeByLastEntry(t *testing.T) {
	dir := t.TempDir()

	v, err := NewVolume(dir, dir, "", 1, NeedleMapInMemory, &super_block.ReplicaPlacement{}, &needle.TTL{}, 0, needle.GetCurrentVersion(), 0, 0)
	if err != nil {
		t.Fatalf("volume creation: %v", err)
	}
	defer v.Close()

	for i := 1; i <= 5; i++ {
		if _, _, _, err := v.writeNeedle2(newRandomNeedle(uint64(i)), true, false, false); err != nil {
			t.Fatalf("write needle %d: %v", i, err)
		}
	}

	entryAt := func(m int64) (key types.NeedleId, cookie uint32, size types.Size) {
		key, offset, size, err := v.readIndexEntry(m)
		if err != nil {
			t.Fatalf("readIndexEntry %d: %v", m, err)
		}
		n, err := v.readNeedleAt(offset)
		if err != nil {
			t.Fatalf("readNeedleAt %d: %v", m, err)
		}
		return key, uint32(n.Cookie), size
	}

	prefixHash := func(count uint64) uint64 {
		h, err := v.hashIndexEntries(count)
		if err != nil {
			t.Fatalf("hashIndexEntries %d: %v", count, err)
		}
		return h
	}

	// a copy whose tail is the replica's tail is already up to date
	key, cookie, size := entryAt(4)
	if _, upToDate, err := v.ResumeByLastEntry(5, key, cookie, size, prefixHash(5), v.IndexFileSize()); err != nil || !upToDate {
		t.Fatalf("tail cursor: upToDate=%v err=%v", upToDate, err)
	}

	// a copy ending at an earlier record resumes from the next entry
	key, cookie, size = entryAt(2)
	offset, upToDate, err := v.ResumeByLastEntry(3, key, cookie, size, prefixHash(3), v.IndexFileSize())
	if err != nil || upToDate {
		t.Fatalf("mid cursor: upToDate=%v err=%v", upToDate, err)
	}
	if want, _ := v.readOffsetFromIndex(3); offset != want {
		t.Fatalf("resume offset %v, want %v", offset, want)
	}

	// a copy ahead of the replica diverged
	if _, _, err = v.ResumeByLastEntry(6, key, cookie, size, prefixHash(3), v.IndexFileSize()); err != ErrIncrementalCopyDiverged {
		t.Fatalf("ahead cursor: %v", err)
	}

	// a copy whose tail record or earlier records differ diverged
	key, cookie, size = entryAt(4)
	if _, _, err = v.ResumeByLastEntry(5, key+1, cookie, size, prefixHash(5), v.IndexFileSize()); err != ErrIncrementalCopyDiverged {
		t.Fatalf("key mismatch: %v", err)
	}
	if _, _, err = v.ResumeByLastEntry(5, key, cookie+1, size, prefixHash(5), v.IndexFileSize()); err != ErrIncrementalCopyDiverged {
		t.Fatalf("cookie mismatch: %v", err)
	}
	if _, _, err = v.ResumeByLastEntry(5, key, cookie, size+1, prefixHash(5), v.IndexFileSize()); err != ErrIncrementalCopyDiverged {
		t.Fatalf("size mismatch: %v", err)
	}
	if _, _, err = v.ResumeByLastEntry(5, key, cookie, size, prefixHash(5)+1, v.IndexFileSize()); err != ErrIncrementalCopyDiverged {
		t.Fatalf("prefix hash mismatch: %v", err)
	}

	// a remote-tier tombstone has no .dat record; a copy ending on it is a
	// valid boundary identified by key and size alone
	deletedKey, _, _ := entryAt(3)
	if err := v.nm.Delete(deletedKey, types.ToOffset(0)); err != nil {
		t.Fatalf("zero-offset tombstone: %v", err)
	}
	if _, upToDate, err := v.ResumeByLastEntry(6, deletedKey, 0, types.TombstoneFileSize, prefixHash(6), v.IndexFileSize()); err != nil || !upToDate {
		t.Fatalf("tombstone tail cursor: upToDate=%v err=%v", upToDate, err)
	}

	// index-only rows are skipped when locating the .dat resume offset
	if _, _, _, err := v.writeNeedle2(newRandomNeedle(6), true, false, false); err != nil {
		t.Fatalf("write needle 6: %v", err)
	}
	key, cookie, size = entryAt(4)
	offset, upToDate, err = v.ResumeByLastEntry(5, key, cookie, size, prefixHash(5), v.IndexFileSize())
	if err != nil || upToDate {
		t.Fatalf("cursor before tombstone: upToDate=%v err=%v", upToDate, err)
	}
	if want, _ := v.readOffsetFromIndex(6); offset != want {
		t.Fatalf("resume offset %v, want %v", offset, want)
	}

	// a copy ahead of only tombstones resumes with no .dat left to send
	if err := v.nm.Delete(deletedKey+1, types.ToOffset(0)); err != nil {
		t.Fatalf("second zero-offset tombstone: %v", err)
	}
	key, cookie, size = entryAt(6)
	offset, upToDate, err = v.ResumeByLastEntry(7, key, cookie, size, prefixHash(7), v.IndexFileSize())
	if err != nil || upToDate {
		t.Fatalf("cursor before trailing tombstone: upToDate=%v err=%v", upToDate, err)
	}
	if !offset.IsZero() {
		t.Fatalf("resume offset %v, want zero", offset)
	}
}
