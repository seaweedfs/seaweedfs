package shell

import (
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage/types"
)

// countFreeShardSlots must report zero for a physically near-full disk even
// though its slot math (MaxVolumeCount - VolumeCount) says it has room, so EC
// placement never piles shards onto a full disk. Mirrors the volume #10160 gate.
func TestCountFreeShardSlotsPhysicalDiskGate(t *testing.T) {
	const gb = uint64(1) << 30

	mk := func(total, free uint64) *master_pb.DataNodeInfo {
		return &master_pb.DataNodeInfo{
			Id: "n1",
			DiskInfos: map[string]*master_pb.DiskInfo{
				"": {MaxVolumeCount: 100, VolumeCount: 0, DiskTotalBytes: total, DiskFreeBytes: free},
			},
		}
	}

	// Physically empty: slot math applies, positive free slots.
	if got := countFreeShardSlots(mk(1000*gb, 900*gb), types.HardDriveType); got <= 0 {
		t.Errorf("physically empty disk free shard slots = %d, want > 0", got)
	}
	// Physically 96% full: gated to zero regardless of slot room.
	if got := countFreeShardSlots(mk(1000*gb, 40*gb), types.HardDriveType); got != 0 {
		t.Errorf("physically full disk free shard slots = %d, want 0", got)
	}
	// No byte report (older server): fall back to slot math, positive.
	if got := countFreeShardSlots(mk(0, 0), types.HardDriveType); got <= 0 {
		t.Errorf("unreported-bytes disk free shard slots = %d, want > 0 (slot fallback)", got)
	}
}
