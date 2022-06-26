package storage

import (
	"fmt"
	"testing"
	"time"

	"github.com/chrislusf/seaweedfs/weed/storage/needle"
	"github.com/chrislusf/seaweedfs/weed/storage/super_block"
	"github.com/chrislusf/seaweedfs/weed/storage/types"
)

func TestSearchVolumesWithDeletedNeedles(t *testing.T) {
	dir := t.TempDir()

	v, err := NewVolume(dir, dir, "", 1, NeedleMapInMemory, &super_block.ReplicaPlacement{}, &needle.TTL{}, 0, 0)
	if err != nil {
		t.Fatalf("volume creation: %v", err)
	}

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
