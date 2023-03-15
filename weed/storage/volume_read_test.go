package storage

import (
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/storage/super_block"
	"github.com/seaweedfs/seaweedfs/weed/storage/types"
	"github.com/stretchr/testify/assert"
)

func TestReadNeedMetaWithWritesAndUpdates(t *testing.T) {
	dir := t.TempDir()

	v, err := NewVolume(dir, dir, "", 1, NeedleMapInMemory, &super_block.ReplicaPlacement{}, &needle.TTL{}, 0, 0, 0)
	if err != nil {
		t.Fatalf("volume creation: %v", err)
	}
	type WriteInfo struct {
		offset int64
		size   int32
	}
	writeInfos := make([]WriteInfo, 30)
	mockLastUpdateTime := uint64(1000000000000)
	// initialize 20 needles then update first 10 needles
	for i := 1; i <= 30; i++ {
		n := newRandomNeedle(uint64(i % 20))
		n.Flags = 0x08
		n.LastModified = mockLastUpdateTime
		mockLastUpdateTime += 2000
		offset, _, _, err := v.writeNeedle2(n, true, false)
		if err != nil {
			t.Fatalf("write needle %d: %v", i, err)
		}
		writeInfos[i-1] = WriteInfo{offset: int64(offset), size: int32(n.Size)}
	}
	expectedLastUpdateTime := uint64(1000000000000)
	for i := 0; i < 30; i++ {
		testNeedle := new(needle.Needle)
		testNeedle.Id = types.Uint64ToNeedleId(uint64(i + 1%20))
		testNeedle.Flags = 0x08
		v.readNeedleMetaAt(testNeedle, writeInfos[i].offset, writeInfos[i].size)
		actualLastModifiedTime := testNeedle.LastModified
		if writeInfos[i].size != 0 {
			assert.Equal(t, expectedLastUpdateTime, actualLastModifiedTime, "The two words should be the same.")
		}
		expectedLastUpdateTime += 2000
	}
}

func TestReadNeedMetaWithDeletesThenWrites(t *testing.T) {
	dir := t.TempDir()

	v, err := NewVolume(dir, dir, "", 1, NeedleMapInMemory, &super_block.ReplicaPlacement{}, &needle.TTL{}, 0, 0, 0)
	if err != nil {
		t.Fatalf("volume creation: %v", err)
	}
	type WriteInfo struct {
		offset int64
		size   int32
	}
	writeInfos := make([]WriteInfo, 10)
	mockLastUpdateTime := uint64(1000000000000)
	for i := 1; i <= 10; i++ {
		n := newRandomNeedle(uint64(i % 5))
		n.Flags = 0x08
		n.LastModified = mockLastUpdateTime
		mockLastUpdateTime += 2000
		offset, _, _, err := v.writeNeedle2(n, true, false)
		if err != nil {
			t.Fatalf("write needle %d: %v", i, err)
		}
		if i < 5 {
			size, err := v.deleteNeedle2(n)
			if err != nil {
				t.Fatalf("delete needle %d: %v", i, err)
			}
			writeInfos[i-1] = WriteInfo{offset: int64(offset), size: int32(size)}
		} else {
			writeInfos[i-1] = WriteInfo{offset: int64(offset), size: int32(n.Size)}
		}
	}

	expectedLastUpdateTime := uint64(1000000000000)
	for i := 0; i < 10; i++ {
		testNeedle := new(needle.Needle)
		testNeedle.Id = types.Uint64ToNeedleId(uint64(i + 1%5))
		testNeedle.Flags = 0x08
		v.readNeedleMetaAt(testNeedle, writeInfos[i].offset, writeInfos[i].size)
		actualLastModifiedTime := testNeedle.LastModified
		if writeInfos[i].size != 0 {
			assert.Equal(t, expectedLastUpdateTime, actualLastModifiedTime, "The two words should be the same.")
		}
		expectedLastUpdateTime += 2000
	}
}
