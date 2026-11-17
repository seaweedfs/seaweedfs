package topology

import (
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/storage/types"
)

// An over-committed node (more volumes registered than it has slots for) must
// report zero free slots, not a negative count that drags the cluster total
// below the slots healthy servers can still offer. See issue #6700.
func TestFreeVolumeCountNeverNegative(t *testing.T) {
	topo := NewTopology("free-slots", nil, 32*1024*1024*1024, 5, false)
	dn := topo.GetOrCreateDataCenter("dc1").GetOrCreateRack("rack1").
		GetOrCreateDataNode("10.0.0.1", 8080, 18080, "", "", map[string]uint32{"": 5})
	// 8 volumes registered on a server configured for 5: 5 + 0 - 8 = -3.
	dn.getOrCreateDisk("").UpAdjustDiskUsageDelta("", &DiskUsageCounts{volumeCount: 8})

	option := &VolumeGrowOption{DiskType: types.HardDriveType}
	if got := dn.AvailableSpaceFor(option); got != 0 {
		t.Errorf("data node AvailableSpaceFor = %d, want 0 (over-committed slots must not go negative)", got)
	}
	if got := topo.AvailableSpaceFor(option); got != 0 {
		t.Errorf("topology AvailableSpaceFor = %d, want 0", got)
	}
	if got := dn.diskUsages.FreeSpace(); got != 0 {
		t.Errorf("diskUsages.FreeSpace = %d, want 0", got)
	}
	if got := dn.diskUsages.ToDiskInfo()[""].FreeVolumeCount; got != 0 {
		t.Errorf("disk info FreeVolumeCount = %d, want 0", got)
	}
}

func TestFreeVolumeCountPositiveCaseUnchanged(t *testing.T) {
	topo := NewTopology("free-slots", nil, 32*1024*1024*1024, 5, false)
	dn := topo.GetOrCreateDataCenter("dc1").GetOrCreateRack("rack1").
		GetOrCreateDataNode("10.0.0.1", 8080, 18080, "", "", map[string]uint32{"": 5})
	dn.getOrCreateDisk("").UpAdjustDiskUsageDelta("", &DiskUsageCounts{volumeCount: 2})

	option := &VolumeGrowOption{DiskType: types.HardDriveType}
	if got := dn.AvailableSpaceFor(option); got != 3 {
		t.Errorf("AvailableSpaceFor = %d, want 3", got)
	}
	if got := dn.diskUsages.ToDiskInfo()[""].FreeVolumeCount; got != 3 {
		t.Errorf("disk info FreeVolumeCount = %d, want 3", got)
	}
}
