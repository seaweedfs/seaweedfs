package storage

import (
	"os"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/storage/idx"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/storage/super_block"
	"github.com/seaweedfs/seaweedfs/weed/storage/types"
	"github.com/stretchr/testify/assert"
)

func TestFirstInvalidIndex(t *testing.T) {
	dir := t.TempDir()

	v, err := NewVolume(dir, dir, "", 1, NeedleMapInMemory, &super_block.ReplicaPlacement{}, &needle.TTL{}, 0, 0, 0)
	if err != nil {
		t.Fatalf("volume creation: %v", err)
	}
	type WriteInfo struct {
		offset int64
		size   int32
	}
	// initialize 20 needles then update first 10 needles
	for i := 1; i <= 30; i++ {
		n := newRandomNeedle(uint64(i))
		n.Flags = 0x08
		_, _, _, err := v.writeNeedle2(n, true, false)
		if err != nil {
			t.Fatalf("write needle %d: %v", i, err)
		}
	}
	b, err := os.ReadFile(v.IndexFileName() + ".idx")
	if err != nil {
		t.Fatal(err)
	}
	// base case every record is valid -> nothing is filtered
	index, err := idx.FirstInvalidIndex(b, func(key types.NeedleId, offset types.Offset, size types.Size) (bool, error) {
		return true, nil
	})
	if err != nil {
		t.Fatalf("failed to complete binary search %v", err)
	}
	assert.Equal(t, 30, index, "when every record is valid nothing should be filtered from binary search")
	index, err = idx.FirstInvalidIndex(b, func(key types.NeedleId, offset types.Offset, size types.Size) (bool, error) {
		return false, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, 0, index, "when every record is invalid everything should be filtered from binary search")
	index, err = idx.FirstInvalidIndex(b, func(key types.NeedleId, offset types.Offset, size types.Size) (bool, error) {
		return key < 20, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	// needle key range from 1 to 30 so < 20 means 19 keys are valid and cutoff the bytes at 19 * 16 = 304
	assert.Equal(t, 19, index, "when every record is invalid everything should be filtered from binary search")

	index, err = idx.FirstInvalidIndex(b, func(key types.NeedleId, offset types.Offset, size types.Size) (bool, error) {
		return key <= 1, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	// needle key range from 1 to 30 so <=1 1 means 1 key is valid and cutoff the bytes at 1 * 16 = 16
	assert.Equal(t, 1, index, "when every record is invalid everything should be filtered from binary search")
}
