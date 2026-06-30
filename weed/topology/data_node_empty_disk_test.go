package topology

import (
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/sequence"
)

// TestToDataNodeInfoReportsEmptyPhysicalDisks verifies the master lists every
// physical disk a volume server reports via DiskTags — including disks holding
// no volumes or EC shards — so empty disks are no longer invisible to
// per-physical-disk consumers (cluster.status, volume.list, the admin topology).
func TestToDataNodeInfoReportsEmptyPhysicalDisks(t *testing.T) {
	topo := NewTopology("weedfs", sequence.NewMemorySequencer(), 32*1024, 5, false)
	dc := topo.GetOrCreateDataCenter("dc1")
	rack := dc.GetOrCreateRack("rack1")
	// HDD ("" type) per-type aggregate; three physical disks share it.
	dn := rack.GetOrCreateDataNode("127.0.0.1", 34534, 0, "127.0.0.1", "", map[string]uint32{"": 1000})
	dn.AdjustMaxVolumeCounts(map[string]uint32{"": 1000})

	dn.UpdateDiskTags([]*master_pb.DiskTag{
		{DiskId: 0, Type: "", MaxVolumeCount: 350},
		{DiskId: 1, Type: "", MaxVolumeCount: 350},
		{DiskId: 2, Type: "", MaxVolumeCount: 300}, // empty disk, never held a volume
	})

	info := dn.ToDataNodeInfo()
	di, ok := info.DiskInfos[""]
	if !ok {
		t.Fatalf("missing HDD disk info")
	}
	if len(di.PhysicalDisks) != 3 {
		t.Fatalf("want 3 physical disks reported, got %d", len(di.PhysicalDisks))
	}

	split := di.SplitByPhysicalDisk()
	if len(split) != 3 {
		t.Fatalf("SplitByPhysicalDisk: want 3 disks including the empty one, got %d", len(split))
	}
	byID := map[uint32]*master_pb.DiskInfo{}
	for _, d := range split {
		byID[d.DiskId] = d
	}
	if byID[2] == nil {
		t.Fatalf("empty disk 2 not surfaced")
	}
	if byID[2].MaxVolumeCount != 300 || byID[2].VolumeCount != 0 {
		t.Errorf("empty disk 2: want max 300 / 0 volumes, got max %d / %d volumes",
			byID[2].MaxVolumeCount, byID[2].VolumeCount)
	}
}
