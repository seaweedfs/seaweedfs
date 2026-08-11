package storage

import (
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/seaweedfs/seaweedfs/weed/stats"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/storage/super_block"
	"github.com/seaweedfs/seaweedfs/weed/storage/types"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

func TestSearchVolumesWithDeletedNeedles(t *testing.T) {
	dir := t.TempDir()

	v, err := NewVolume(dir, dir, "", 1, NeedleMapInMemory, &super_block.ReplicaPlacement{}, &needle.TTL{}, 0, needle.GetCurrentVersion(), 0, 0)
	if err != nil {
		t.Fatalf("volume creation: %v", err)
	}
	defer v.Close()

	count := 20

	for i := 1; i < count; i++ {
		n := newRandomNeedle(uint64(i))
		_, _, _, err := v.writeNeedle2(n, true, false)
		if err != nil {
			t.Fatalf("write needle %d: %v", i, err)
		}
	}

	for i := 1; i < 15; i++ {
		n := newEmptyNeedle(uint64(i))
		err := v.nm.Put(n.Id, types.Offset{}, types.TombstoneFileSize)
		if err != nil {
			t.Fatalf("delete needle %d: %v", i, err)
		}
	}

	ts1 := time.Now().UnixNano()

	for i := 15; i < count; i++ {
		n := newEmptyNeedle(uint64(i))
		_, err := v.doDeleteRequest(n)
		if err != nil {
			t.Fatalf("delete needle %d: %v", i, err)
		}
	}

	offset, isLast, err := v.BinarySearchByAppendAtNs(uint64(ts1))
	if err != nil {
		t.Fatalf("lookup by ts: %v", err)
	}
	fmt.Printf("offset: %v, isLast: %v\n", offset.ToActualOffset(), isLast)

}

func isFileExist(path string) (bool, error) {
	if _, err := os.Stat(path); err == nil {
		return true, nil
	} else if errors.Is(err, os.ErrNotExist) {
		return false, nil
	} else {
		return false, err
	}
}

func assertFileExist(t *testing.T, expected bool, path string) {
	exist, err := isFileExist(path)
	if err != nil {
		t.Fatalf("isFileExist: %v", err)
	}
	assert.Equal(t, expected, exist)
}

func TestDestroyEmptyVolumeWithOnlyEmpty(t *testing.T) {
	dir := t.TempDir()

	v, err := NewVolume(dir, dir, "", 1, NeedleMapInMemory, &super_block.ReplicaPlacement{}, &needle.TTL{}, 0, needle.GetCurrentVersion(), 0, 0)
	if err != nil {
		t.Fatalf("volume creation: %v", err)
	}
	path := v.DataBackend.Name()

	// should can Destroy empty volume with onlyEmpty
	assertFileExist(t, true, path)
	err = v.Destroy(true, false)
	if err != nil {
		t.Fatalf("destroy volume: %v", err)
	}
	assertFileExist(t, false, path)
}

func TestDestroyEmptyVolumeWithoutOnlyEmpty(t *testing.T) {
	dir := t.TempDir()

	v, err := NewVolume(dir, dir, "", 1, NeedleMapInMemory, &super_block.ReplicaPlacement{}, &needle.TTL{}, 0, needle.GetCurrentVersion(), 0, 0)
	if err != nil {
		t.Fatalf("volume creation: %v", err)
	}
	path := v.DataBackend.Name()

	// should can Destroy empty volume without onlyEmpty
	assertFileExist(t, true, path)
	err = v.Destroy(false, false)
	if err != nil {
		t.Fatalf("destroy volume: %v", err)
	}
	assertFileExist(t, false, path)
}

func TestDestroyNonemptyVolumeWithOnlyEmpty(t *testing.T) {
	dir := t.TempDir()

	v, err := NewVolume(dir, dir, "", 1, NeedleMapInMemory, &super_block.ReplicaPlacement{}, &needle.TTL{}, 0, needle.GetCurrentVersion(), 0, 0)
	if err != nil {
		t.Fatalf("volume creation: %v", err)
	}
	defer v.Close()
	path := v.DataBackend.Name()

	// should return "volume not empty" error and do not delete file when Destroy non-empty volume
	_, _, _, err = v.writeNeedle2(newRandomNeedle(1), true, false)
	if err != nil {
		t.Fatalf("write needle: %v", err)
	}
	assert.Equal(t, uint64(1), v.FileCount())

	assertFileExist(t, true, path)
	err = v.Destroy(true, false)
	assert.EqualError(t, err, "volume not empty")
	assertFileExist(t, true, path)

	// should keep working after "volume not empty"
	_, _, _, err = v.writeNeedle2(newRandomNeedle(2), true, false)
	if err != nil {
		t.Fatalf("write needle: %v", err)
	}

	assert.Equal(t, uint64(2), v.FileCount())
}

func TestDestroyNonemptyVolumeWithoutOnlyEmpty(t *testing.T) {
	dir := t.TempDir()

	v, err := NewVolume(dir, dir, "", 1, NeedleMapInMemory, &super_block.ReplicaPlacement{}, &needle.TTL{}, 0, needle.GetCurrentVersion(), 0, 0)
	if err != nil {
		t.Fatalf("volume creation: %v", err)
	}
	path := v.DataBackend.Name()

	// should can Destroy non-empty volume without onlyEmpty
	_, _, _, err = v.writeNeedle2(newRandomNeedle(1), true, false)
	if err != nil {
		t.Fatalf("write needle: %v", err)
	}
	assert.Equal(t, uint64(1), v.FileCount())

	assertFileExist(t, true, path)
	err = v.Destroy(false, false)
	if err != nil {
		t.Fatalf("destroy volume: %v", err)
	}
	assertFileExist(t, false, path)
}

// Pre-fix: the blob was appended to .dat, then rejected by SortedFileNeedleMap.Put.
func TestWriteNeedleBlobRejectedOnReadOnlyVolume(t *testing.T) {
	dir := t.TempDir()

	v, err := NewVolume(dir, dir, "", 7, NeedleMapInMemory, &super_block.ReplicaPlacement{}, &needle.TTL{}, 0, needle.GetCurrentVersion(), 0, 0)
	if err != nil {
		t.Fatalf("volume creation: %v", err)
	}
	n := newRandomNeedle(1)
	offset, _, _, err := v.writeNeedle2(n, true, false)
	if err != nil {
		t.Fatalf("write needle: %v", err)
	}
	blob, err := v.ReadNeedleBlob(int64(offset), n.Size)
	if err != nil {
		t.Fatalf("read needle blob: %v", err)
	}
	v.PersistReadOnly(true, false)
	v.Close()

	v, err = NewVolume(dir, dir, "", 7, NeedleMapInMemory, &super_block.ReplicaPlacement{}, &needle.TTL{}, 0, needle.GetCurrentVersion(), 0, 0)
	if err != nil {
		t.Fatalf("volume reload: %v", err)
	}
	defer v.Close()
	if _, ok := v.nm.(*SortedFileNeedleMap); !ok {
		t.Fatalf("reloaded read-only volume should use SortedFileNeedleMap, got %T", v.nm)
	}

	datSizeBefore, _, _ := v.DataBackend.GetStat()

	err = v.WriteNeedleBlob(types.Uint64ToNeedleId(2), blob, n.Size)
	if err == nil {
		t.Fatalf("expected WriteNeedleBlob to be rejected on a read-only volume")
	}
	if errors.Is(err, os.ErrInvalid) {
		t.Errorf("WriteNeedleBlob should fail with a read-only error, not the needle map's os.ErrInvalid: %v", err)
	}

	datSizeAfter, _, _ := v.DataBackend.GetStat()
	if datSizeAfter != datSizeBefore {
		t.Errorf("read-only volume .dat grew from %d to %d, leaving an unindexed needle", datSizeBefore, datSizeAfter)
	}
}

// A size disagreeing with the blob's own header indexes the needle at the wrong
// length and, on v3, stamps the append timestamp into the middle of the needle.
func TestWriteNeedleBlobRejectsSizeMismatch(t *testing.T) {
	dir := t.TempDir()
	location := NewDiskLocation(dir, 10, util.MinFreeSpace{}, dir, "", nil, stats.DefaultDiskIOProbeConfig())
	defer location.Close()

	v, err := NewVolume(dir, dir, "", 7, NeedleMapInMemory, &super_block.ReplicaPlacement{}, &needle.TTL{}, 0, needle.GetCurrentVersion(), 0, 0)
	if err != nil {
		t.Fatalf("volume creation: %v", err)
	}
	defer v.Close()
	location.SetVolume(7, v)

	n := newRandomNeedle(1)
	offset, _, _, err := v.writeNeedle2(n, true, false)
	if err != nil {
		t.Fatalf("write needle: %v", err)
	}
	blob, err := v.ReadNeedleBlob(int64(offset), n.Size)
	if err != nil {
		t.Fatalf("read needle blob: %v", err)
	}

	datSizeBefore, _, _ := v.DataBackend.GetStat()

	// types.Size(n.DataSize) is what needle.Append reports, and what a caller
	// following the payload-size convention would send.
	if err = v.WriteNeedleBlob(types.Uint64ToNeedleId(2), blob, types.Size(n.DataSize)); err == nil {
		t.Fatal("expected WriteNeedleBlob to reject a size that disagrees with the blob header")
	}

	datSizeAfter, _, _ := v.DataBackend.GetStat()
	if datSizeAfter != datSizeBefore {
		t.Errorf(".dat grew from %d to %d on a rejected blob", datSizeBefore, datSizeAfter)
	}

	if err = v.WriteNeedleBlob(types.Uint64ToNeedleId(2), blob, n.Size); err != nil {
		t.Fatalf("write needle blob with the header size: %v", err)
	}
}
