package storage

import (
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/storage/super_block"
	"github.com/seaweedfs/seaweedfs/weed/storage/types"
)

// The /status handler builds VolumeInfo via collectStatForOneVolume, which
// used to leave ModifiedAtSecond at 0; only the heartbeat path filled it in.
func TestCollectStatForOneVolumeModifiedAtSecond(t *testing.T) {
	dir := t.TempDir()

	v, err := NewVolume(dir, dir, "", 1, NeedleMapInMemory, &super_block.ReplicaPlacement{}, &needle.TTL{}, 0, needle.GetCurrentVersion(), 0, 0)
	if err != nil {
		t.Fatalf("create volume: %v", err)
	}
	defer v.Close()
	v.location = &DiskLocation{Directory: dir, DiskType: types.HardDriveType}

	if _, _, _, err := v.writeNeedle2(newRandomNeedle(1), true, false); err != nil {
		t.Fatalf("write: %v", err)
	}

	s := collectStatForOneVolume(v.Id, v)
	if s.ModifiedAtSecond <= 0 {
		t.Fatalf("ModifiedAtSecond = %d, want > 0", s.ModifiedAtSecond)
	}
}
