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

	// a copy whose tail is the replica's tail is already up to date
	key, cookie, size := entryAt(4)
	if _, upToDate, err := v.ResumeByLastEntry(5, key, cookie, size); err != nil || !upToDate {
		t.Fatalf("tail cursor: upToDate=%v err=%v", upToDate, err)
	}

	// a copy ending at an earlier record resumes from the next entry
	key, cookie, size = entryAt(2)
	offset, upToDate, err := v.ResumeByLastEntry(3, key, cookie, size)
	if err != nil || upToDate {
		t.Fatalf("mid cursor: upToDate=%v err=%v", upToDate, err)
	}
	if want, _ := v.readOffsetFromIndex(3); offset != want {
		t.Fatalf("resume offset %v, want %v", offset, want)
	}

	// a copy ahead of the replica, or with a different tail record, diverged
	if _, _, err = v.ResumeByLastEntry(6, key, cookie, size); err != ErrIncrementalCopyDiverged {
		t.Fatalf("ahead cursor: %v", err)
	}
	if _, _, err = v.ResumeByLastEntry(5, key+1, cookie, size); err != ErrIncrementalCopyDiverged {
		t.Fatalf("key mismatch: %v", err)
	}
	if _, _, err = v.ResumeByLastEntry(5, key, cookie+1, size); err != ErrIncrementalCopyDiverged {
		t.Fatalf("cookie mismatch: %v", err)
	}
	if _, _, err = v.ResumeByLastEntry(5, key, cookie, size+1); err != ErrIncrementalCopyDiverged {
		t.Fatalf("size mismatch: %v", err)
	}
}
