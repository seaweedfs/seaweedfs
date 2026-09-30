package storage

import (
	"errors"
	"path/filepath"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/storage/super_block"
	"github.com/seaweedfs/seaweedfs/weed/storage/types"
)

// newVolumeWithGarbage writes live+deleted needles and deletes the first
// deleted ones, so the volume carries that much garbage on disk.
func newVolumeWithGarbage(t *testing.T, live, deleted int) *Volume {
	t.Helper()
	dir := t.TempDir()
	v, err := NewVolume(dir, dir, "", 1, NeedleMapInMemory, &super_block.ReplicaPlacement{}, &needle.TTL{}, 0, needle.GetCurrentVersion(), 0, 0)
	if err != nil {
		t.Fatalf("NewVolume: %v", err)
	}
	t.Cleanup(v.Close)
	for i := 1; i <= live+deleted; i++ {
		if _, _, _, err := v.writeNeedle2(newRandomNeedle(uint64(i)), true, false, false); err != nil {
			t.Fatalf("write needle %d: %v", i, err)
		}
	}
	for i := 1; i <= deleted; i++ {
		if _, err := v.deleteNeedle2(newEmptyNeedle(uint64(i))); err != nil {
			t.Fatalf("delete needle %d: %v", i, err)
		}
	}
	return v
}

func stubCompactionDiskFree(t *testing.T, free uint64) {
	t.Helper()
	prev := compactionDiskFree
	compactionDiskFree = func(string) uint64 { return free }
	t.Cleanup(func() { compactionDiskFree = prev })
}

func TestCompactionSpaceNeeded_CountsLiveBytesNotVolumeSize(t *testing.T) {
	v := newVolumeWithGarbage(t, 20, 2000)
	datSize, idxSize, _ := v.FileStat()
	liveContent := int64(v.ContentSize() - v.DeletedSize())

	dataBytes, indexBytes := compactionSpaceNeeded(v, 0)

	if dataBytes < liveContent+super_block.SuperBlockSize {
		t.Fatalf("data estimate %d does not cover the %d live content bytes plus the superblock", dataBytes, liveContent)
	}
	if indexBytes < 20*types.NeedleMapEntrySize {
		t.Fatalf("index estimate %d does not cover 20 live entries", indexBytes)
	}
	if dataBytes+indexBytes >= int64(datSize+idxSize) {
		t.Fatalf("space needed %d is not below the current volume size %d: a mostly-garbage volume must not require its own size to compact", dataBytes+indexBytes, datSize+idxSize)
	}
}

func TestCompactionSpaceNeeded_AllGarbageNeedsAlmostNothing(t *testing.T) {
	v := newVolumeWithGarbage(t, 0, 2000)
	datSize, _, _ := v.FileStat()

	dataBytes, indexBytes := compactionSpaceNeeded(v, 0)

	if needed := dataBytes + indexBytes; needed > int64(datSize)/10 {
		t.Fatalf("an all-garbage volume of %d bytes still asks for %d bytes", datSize, needed)
	}
}

func TestCompactionSpaceNeeded_NeverAboveCurrentVolume(t *testing.T) {
	// Nothing deleted: the estimate may not exceed what is already on disk.
	v := newVolumeWithGarbage(t, 200, 0)
	datSize, idxSize, _ := v.FileStat()

	dataBytes, indexBytes := compactionSpaceNeeded(v, 0)

	if dataBytes > int64(datSize) || indexBytes > int64(idxSize) {
		t.Fatalf("estimate data=%d index=%d exceeds the current files data=%d index=%d", dataBytes, indexBytes, datSize, idxSize)
	}
}

func TestCompactionSpaceNeeded_PreallocateWinsForDataOnly(t *testing.T) {
	// The new .dat is preallocated to this size; the rebuilt index is a
	// separate file and still needs its own room.
	v := newVolumeWithGarbage(t, 5, 5)
	const preallocate = int64(1) << 30

	dataBytes, indexBytes := compactionSpaceNeeded(v, preallocate)

	if dataBytes != preallocate {
		t.Fatalf("data estimate %d, want the preallocate size %d", dataBytes, preallocate)
	}
	if indexBytes < 5*types.NeedleMapEntrySize {
		t.Fatalf("index estimate %d does not cover 5 live entries", indexBytes)
	}
}

func TestEnsureCompactVolumeSpace_FullDiskWithGarbage(t *testing.T) {
	// The disk-full case from #11516: free space is far below the volume's
	// size, but well above what compacting its live needles will write.
	v := newVolumeWithGarbage(t, 20, 2000)
	datSize, idxSize, _ := v.FileStat()
	dataBytes, indexBytes := compactionSpaceNeeded(v, 0)
	needed := dataBytes + indexBytes
	if uint64(needed) >= datSize+idxSize {
		t.Fatalf("test setup: estimate %d is not below volume size %d", needed, datSize+idxSize)
	}

	stubCompactionDiskFree(t, uint64(needed))
	if err := ensureCompactVolumeSpace(v, 0); err != nil {
		t.Fatalf("free %d covers the estimate %d, volume is %d: unexpected %v", needed, needed, datSize+idxSize, err)
	}

	stubCompactionDiskFree(t, uint64(needed)-1)
	err := ensureCompactVolumeSpace(v, 0)
	if !errors.Is(err, ErrInsufficientSpace) {
		t.Fatalf("free %d below the estimate %d: got %v, want ErrInsufficientSpace", needed-1, needed, err)
	}
}

func TestEnsureCompactVolumeSpace_SeparateIndexDisk(t *testing.T) {
	dataDir, idxDir := t.TempDir(), t.TempDir()
	v, err := NewVolume(dataDir, idxDir, "", 1, NeedleMapInMemory, &super_block.ReplicaPlacement{}, &needle.TTL{}, 0, needle.GetCurrentVersion(), 0, 0)
	if err != nil {
		t.Fatalf("NewVolume: %v", err)
	}
	t.Cleanup(v.Close)
	for i := 1; i <= 50; i++ {
		if _, _, _, err := v.writeNeedle2(newRandomNeedle(uint64(i)), true, false, false); err != nil {
			t.Fatalf("write needle %d: %v", i, err)
		}
	}
	dataBytes, indexBytes := compactionSpaceNeeded(v, 0)

	free := map[string]uint64{dataDir: uint64(dataBytes), idxDir: uint64(indexBytes)}
	prevFree, prevSame := compactionDiskFree, compactionSameFilesystem
	compactionDiskFree = func(dir string) uint64 { return free[dir] }
	compactionSameFilesystem = func(string, string) bool { return false }
	t.Cleanup(func() { compactionDiskFree, compactionSameFilesystem = prevFree, prevSame })

	if err := ensureCompactVolumeSpace(v, 0); err != nil {
		t.Fatalf("each disk covers its own share: unexpected %v", err)
	}
	// Plenty of room on the data disk cannot make up for a full index disk.
	free[dataDir] = uint64(dataBytes) * 10
	free[idxDir] = uint64(indexBytes) - 1
	if err := ensureCompactVolumeSpace(v, 0); !errors.Is(err, ErrInsufficientSpace) {
		t.Fatalf("full index disk: got %v, want ErrInsufficientSpace", err)
	}

	// Two directories on one filesystem draw on the same free space, so the
	// data and the index estimates must be covered together.
	compactionSameFilesystem = func(string, string) bool { return true }
	free[dataDir] = uint64(dataBytes+indexBytes) - 1
	if err := ensureCompactVolumeSpace(v, 0); !errors.Is(err, ErrInsufficientSpace) {
		t.Fatalf("shared filesystem short of the sum: got %v, want ErrInsufficientSpace", err)
	}
	free[dataDir] = uint64(dataBytes + indexBytes)
	if err := ensureCompactVolumeSpace(v, 0); err != nil {
		t.Fatalf("shared filesystem covering the sum: unexpected %v", err)
	}
}

func TestSameFilesystem(t *testing.T) {
	dir := t.TempDir()
	if !sameFilesystem(dir, dir) {
		t.Fatal("a directory is on its own filesystem")
	}
	if !sameFilesystem(dir, filepath.Join(dir, "missing")) {
		t.Fatal("an unreadable path must be treated as shared, so the check asks for the sum")
	}
}
