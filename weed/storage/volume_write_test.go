package storage

import (
	"bytes"
	"errors"
	"fmt"
	"math"
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
		_, _, _, err := v.writeNeedle2(n, true, false, false)
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
	_, _, _, err = v.writeNeedle2(newRandomNeedle(1), true, false, false)
	if err != nil {
		t.Fatalf("write needle: %v", err)
	}
	assert.Equal(t, uint64(1), v.FileCount())

	assertFileExist(t, true, path)
	err = v.Destroy(true, false)
	assert.EqualError(t, err, "volume not empty")
	assertFileExist(t, true, path)

	// should keep working after "volume not empty"
	_, _, _, err = v.writeNeedle2(newRandomNeedle(2), true, false, false)
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
	_, _, _, err = v.writeNeedle2(newRandomNeedle(1), true, false, false)
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
	offset, _, _, err := v.writeNeedle2(n, true, false, false)
	if err != nil {
		t.Fatalf("write needle: %v", err)
	}
	blob, err := v.ReadNeedleBlob(int64(offset), n.Size)
	if err != nil {
		t.Fatalf("read needle blob: %v", err)
	}
	if err := v.PersistReadOnly(true, false); err != nil {
		t.Fatalf("persist read-only: %v", err)
	}
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
	offset, _, _, err := v.writeNeedle2(n, true, false, false)
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

// A negative size reaches make() in needle.ReadNeedleBlob, and the blob RPCs
// have no recover, so one request took down the volume server.
func TestReadNeedleBlobRejectsNegativeSize(t *testing.T) {
	dir := t.TempDir()
	v, err := NewVolume(dir, dir, "", 7, NeedleMapInMemory, &super_block.ReplicaPlacement{}, &needle.TTL{}, 0, needle.GetCurrentVersion(), 0, 0)
	if err != nil {
		t.Fatalf("volume creation: %v", err)
	}
	defer v.Close()

	n := newRandomNeedle(1)
	offset, _, _, err := v.writeNeedle2(n, true, false, false)
	if err != nil {
		t.Fatalf("write needle: %v", err)
	}

	for _, size := range []types.Size{types.TombstoneFileSize, -100, math.MinInt32} {
		t.Run(fmt.Sprintf("size %d", size), func(t *testing.T) {
			defer func() {
				if r := recover(); r != nil {
					t.Errorf("ReadNeedleBlob panicked: %v", r)
				}
			}()
			if _, err := v.ReadNeedleBlob(int64(offset), size); err == nil {
				t.Error("expected ReadNeedleBlob to reject a negative size")
			}
		})
	}

	// Only negative sizes are rejected: size 0 is what a delete record carries.
	for _, size := range []types.Size{0, n.Size} {
		if _, err := v.ReadNeedleBlob(int64(offset), size); err != nil {
			t.Errorf("ReadNeedleBlob with size %d: %v", size, err)
		}
	}
}

// The blob header carries the same negative size, so only the sign is wrong.
func TestWriteNeedleBlobRejectsNegativeSize(t *testing.T) {
	for _, size := range []types.Size{types.TombstoneFileSize, -5} {
		t.Run(fmt.Sprintf("size %d", size), func(t *testing.T) {
			dir := t.TempDir()
			v, err := NewVolume(dir, dir, "", 7, NeedleMapInMemory, &super_block.ReplicaPlacement{}, &needle.TTL{}, 0, needle.GetCurrentVersion(), 0, 0)
			if err != nil {
				t.Fatalf("volume creation: %v", err)
			}
			defer v.Close()

			n := newRandomNeedle(1)
			offset, _, _, err := v.writeNeedle2(n, true, false, false)
			if err != nil {
				t.Fatalf("write needle: %v", err)
			}
			blob, err := v.ReadNeedleBlob(int64(offset), n.Size)
			if err != nil {
				t.Fatalf("read needle blob: %v", err)
			}
			// Make the header agree with the size, so only the sign is wrong.
			types.SizeToBytes(blob[types.NeedleHeaderSize-types.SizeSize:types.NeedleHeaderSize], size)

			datSizeBefore, _, _ := v.DataBackend.GetStat()

			if err = v.WriteNeedleBlob(types.Uint64ToNeedleId(2), blob, size); err == nil {
				t.Error("expected WriteNeedleBlob to reject a negative size")
			}

			datSizeAfter, _, _ := v.DataBackend.GetStat()
			if datSizeAfter != datSizeBefore {
				t.Errorf(".dat grew from %d to %d on a rejected blob", datSizeBefore, datSizeAfter)
			}
			if nv, ok := v.nm.Get(types.Uint64ToNeedleId(2)); ok {
				t.Errorf("needle 2 was indexed with size %d", nv.Size)
			}
		})
	}
}

// A blob shorter or longer than its record size leaves .dat off the record grid:
// later writes index at truncated offsets, or a scan reads the leftover bytes as
// the next record.
func TestWriteNeedleBlobRejectsLengthMismatch(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mutate func(blob []byte) []byte
	}{
		{"one byte too long", func(blob []byte) []byte { return append(blob, 0) }},
		{"one byte short", func(blob []byte) []byte { return blob[:len(blob)-1] }},
		{"8 bytes too long", func(blob []byte) []byte { return append(blob, make([]byte, 8)...) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			v, err := NewVolume(dir, dir, "", 7, NeedleMapInMemory, &super_block.ReplicaPlacement{}, &needle.TTL{}, 0, needle.GetCurrentVersion(), 0, 0)
			if err != nil {
				t.Fatalf("volume creation: %v", err)
			}
			defer v.Close()

			n := newRandomNeedle(1)
			offset, _, _, err := v.writeNeedle2(n, true, false, false)
			if err != nil {
				t.Fatalf("write needle: %v", err)
			}
			blob, err := v.ReadNeedleBlob(int64(offset), n.Size)
			if err != nil {
				t.Fatalf("read needle blob: %v", err)
			}
			datSizeBefore, _, _ := v.DataBackend.GetStat()

			if err = v.WriteNeedleBlob(types.Uint64ToNeedleId(2), tc.mutate(blob), n.Size); err == nil {
				t.Error("expected WriteNeedleBlob to reject a blob whose length does not match its size")
			}

			datSizeAfter, _, _ := v.DataBackend.GetStat()
			if datSizeAfter != datSizeBefore {
				t.Errorf(".dat grew from %d to %d on a rejected blob", datSizeBefore, datSizeAfter)
			}

			next := newRandomNeedle(3)
			if _, _, _, err = v.writeNeedle2(next, true, false, false); err != nil {
				t.Fatalf("write needle 3: %v", err)
			}
			got := newEmptyNeedle(3)
			if _, err = v.readNeedle(got, nil, nil); err != nil {
				t.Fatalf("read back needle 3: %v", err)
			}
			if !bytes.Equal(got.Data, next.Data) {
				t.Error("needle 3 read back with different data")
			}
		})
	}
}

// The checks must pass what the real callers send: a needle's own record and the
// size-0 record a delete leaves.
func TestWriteNeedleBlobRoundTrip(t *testing.T) {
	dir := t.TempDir()
	v, err := NewVolume(dir, dir, "", 7, NeedleMapInMemory, &super_block.ReplicaPlacement{}, &needle.TTL{}, 0, needle.GetCurrentVersion(), 0, 0)
	if err != nil {
		t.Fatalf("volume creation: %v", err)
	}
	defer v.Close()

	n := newRandomNeedle(1)
	offset, _, _, err := v.writeNeedle2(n, true, false, false)
	if err != nil {
		t.Fatalf("write needle: %v", err)
	}
	blob, err := v.ReadNeedleBlob(int64(offset), n.Size)
	if err != nil {
		t.Fatalf("read needle blob: %v", err)
	}
	if err = v.WriteNeedleBlob(types.Uint64ToNeedleId(2), blob, n.Size); err != nil {
		t.Fatalf("write needle blob: %v", err)
	}
	got := newEmptyNeedle(2)
	if _, err = v.readNeedle(got, nil, nil); err != nil {
		t.Fatalf("read back needle 2: %v", err)
	}
	if !bytes.Equal(got.Data, n.Data) {
		t.Error("needle 2 read back with different data")
	}

	deleteOffset, _, _ := v.DataBackend.GetStat()
	if _, err = v.doDeleteRequest(newEmptyNeedle(1)); err != nil {
		t.Fatalf("delete needle 1: %v", err)
	}
	deleteRecord, err := v.ReadNeedleBlob(deleteOffset, 0)
	if err != nil {
		t.Fatalf("read delete record: %v", err)
	}
	if err = v.WriteNeedleBlob(types.Uint64ToNeedleId(3), deleteRecord, 0); err != nil {
		t.Fatalf("write size-0 needle blob: %v", err)
	}

	next := newRandomNeedle(4)
	if _, _, _, err = v.writeNeedle2(next, true, false, false); err != nil {
		t.Fatalf("write needle 4: %v", err)
	}
	got = newEmptyNeedle(4)
	if _, err = v.readNeedle(got, nil, nil); err != nil {
		t.Fatalf("read back needle 4: %v", err)
	}
	if !bytes.Equal(got.Data, next.Data) {
		t.Error("needle 4 read back with different data")
	}
}
