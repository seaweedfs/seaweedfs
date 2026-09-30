package storage

import (
	"errors"
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

	needed, liveBytes, indexBytes := compactionSpaceNeeded(v, 0)

	if liveBytes < liveContent+super_block.SuperBlockSize {
		t.Fatalf("live estimate %d does not cover the %d live content bytes plus the superblock", liveBytes, liveContent)
	}
	if indexBytes < 20*types.NeedleMapEntrySize {
		t.Fatalf("index estimate %d does not cover 20 live entries", indexBytes)
	}
	if needed < liveBytes+indexBytes {
		t.Fatalf("space needed %d is below live %d + index %d", needed, liveBytes, indexBytes)
	}
	if needed >= int64(datSize+idxSize) {
		t.Fatalf("space needed %d is not below the current volume size %d: a mostly-garbage volume must not require its own size to compact", needed, datSize+idxSize)
	}
}

func TestCompactionSpaceNeeded_AllGarbageNeedsAlmostNothing(t *testing.T) {
	v := newVolumeWithGarbage(t, 0, 2000)
	datSize, _, _ := v.FileStat()

	needed, _, _ := compactionSpaceNeeded(v, 0)

	if needed > int64(datSize)/10 {
		t.Fatalf("an all-garbage volume of %d bytes still asks for %d bytes", datSize, needed)
	}
}

func TestCompactionSpaceNeeded_NeverAboveCurrentVolume(t *testing.T) {
	// Nothing deleted: the estimate may not exceed what is already on disk.
	v := newVolumeWithGarbage(t, 200, 0)
	datSize, idxSize, _ := v.FileStat()

	needed, _, _ := compactionSpaceNeeded(v, 0)

	if needed > int64(datSize+idxSize) {
		t.Fatalf("space needed %d exceeds the current volume %d", needed, datSize+idxSize)
	}
}

func TestCompactionSpaceNeeded_PreallocateWins(t *testing.T) {
	v := newVolumeWithGarbage(t, 5, 5)
	const preallocate = int64(1) << 30

	needed, _, _ := compactionSpaceNeeded(v, preallocate)

	if needed != preallocate {
		t.Fatalf("space needed %d, want the preallocate size %d", needed, preallocate)
	}
}

func TestEnsureCompactVolumeSpace_FullDiskWithGarbage(t *testing.T) {
	// The disk-full case from #11516: free space is far below the volume's
	// size, but well above what compacting its live needles will write.
	v := newVolumeWithGarbage(t, 20, 2000)
	datSize, idxSize, _ := v.FileStat()
	needed, _, _ := compactionSpaceNeeded(v, 0)
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

// Compaction writes live needles only, so the space check must be measured
// against the live size, not the .dat the garbage occupies — a full disk
// needs the estimate to shrink or it can never reclaim.
func TestEstimatedCompactedSizeCountsLiveNeedles(t *testing.T) {
	dir := t.TempDir()

	v, err := NewVolume(dir, dir, "", 1, NeedleMapInMemory, &super_block.ReplicaPlacement{}, &needle.TTL{}, 0, needle.GetCurrentVersion(), 0, 0)
	if err != nil {
		t.Fatalf("volume creation: %v", err)
	}
	defer v.Close()

	const count = 20
	for i := 1; i <= count; i++ {
		if _, _, _, err := v.writeNeedle2(newRandomNeedle(uint64(i)), true, false, false); err != nil {
			t.Fatalf("write needle %d: %v", i, err)
		}
	}
	datSize, _, _ := v.FileStat()

	fullEstimate := estimatedCompactedSize(v)
	if fullEstimate <= super_block.SuperBlockSize {
		t.Fatalf("estimate for all-live volume = %d, want > superblock", fullEstimate)
	}

	for i := 1; i < count; i++ {
		if _, err := v.doDeleteRequest(newEmptyNeedle(uint64(i))); err != nil {
			t.Fatalf("delete needle %d: %v", i, err)
		}
	}

	estimate := estimatedCompactedSize(v)
	if estimate >= int64(datSize) {
		t.Fatalf("estimate %d not below .dat size %d with 19/20 needles deleted", estimate, datSize)
	}
	if estimate <= super_block.SuperBlockSize {
		t.Fatalf("estimate %d lost the one live needle", estimate)
	}
	live := int64(v.FileCount()-v.DeletedCount())*types.NeedleMapEntrySize + super_block.SuperBlockSize
	if estimate < live {
		t.Fatalf("estimate %d below superblock + live index entries %d", estimate, live)
	}

	if _, err := v.doDeleteRequest(newEmptyNeedle(uint64(count))); err != nil {
		t.Fatalf("delete last needle: %v", err)
	}
	if estimate := estimatedCompactedSize(v); estimate != super_block.SuperBlockSize {
		t.Fatalf("all-deleted estimate = %d, want superblock only (%d)", estimate, super_block.SuperBlockSize)
	}
}

// The estimate must cover what compaction writes on disk: each live needle's
// content plus its header, checksum, timestamp and padding. An all-live
// volume's compacted .dat is byte-for-byte its current one, so the estimate
// may not fall below the current file.
func TestEstimatedCompactedSizeCoversNeedleFraming(t *testing.T) {
	dir := t.TempDir()

	v, err := NewVolume(dir, dir, "", 1, NeedleMapInMemory, &super_block.ReplicaPlacement{}, &needle.TTL{}, 0, needle.GetCurrentVersion(), 0, 0)
	if err != nil {
		t.Fatalf("volume creation: %v", err)
	}
	defer v.Close()

	for i := 1; i <= 100; i++ {
		if _, _, _, err := v.writeNeedle2(newRandomNeedle(uint64(i)), true, false, false); err != nil {
			t.Fatalf("write needle %d: %v", i, err)
		}
	}
	datSize, _, _ := v.FileStat()

	if estimate := estimatedCompactedSize(v); estimate < int64(datSize) {
		t.Fatalf("estimate %d below .dat size %d for an all-live volume: missing per-needle framing", estimate, datSize)
	}
}

// disk_space_low is only reported when low space is the sole read-only cause,
// so a volume also marked read-only by an operator or quarantined by failed
// I/O stays out of the sweep.
func TestCheckCompactVolumeDiskLowSoleCauseOnly(t *testing.T) {
	dir := t.TempDir()
	store := newSingleDirStore(t, dir)
	defer store.Close()
	const vid = needle.VolumeId(7)
	require.NoError(t, store.AddVolume(vid, "", NeedleMapInMemory, "000", "", 0, needle.GetCurrentVersion(), 0, types.HardDriveType, 0))

	_, low, err := store.CheckCompactVolume(vid)
	require.NoError(t, err)
	require.False(t, low)

	store.Locations[0].isDiskSpaceLow.Store(true)
	_, low, err = store.CheckCompactVolume(vid)
	require.NoError(t, err)
	require.True(t, low)

	require.NoError(t, store.MarkVolumeReadonly(vid, false, false))
	_, low, err = store.CheckCompactVolume(vid)
	require.NoError(t, err)
	require.False(t, low)
}
