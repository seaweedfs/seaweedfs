package topology

import (
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/sequence"
)

// A disk with no volumes/shards surfaces via DiskTags.
func TestToDataNodeInfoReportsEmptyPhysicalDisks(t *testing.T) {
	topo := NewTopology("weedfs", sequence.NewMemorySequencer(), 32*1024, 5, false)
	dc := topo.GetOrCreateDataCenter("dc1")
	rack := dc.GetOrCreateRack("rack1")
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
	if len(di.MaxVolumeCountByDisk) != 3 {
		t.Fatalf("want 3 physical disks reported, got %d", len(di.MaxVolumeCountByDisk))
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

// A max-0 (unavailable) disk stays listed when the node reports capacity.
func TestToDataNodeInfoKeepsZeroCapacityDisk(t *testing.T) {
	topo := NewTopology("weedfs", sequence.NewMemorySequencer(), 32*1024, 5, false)
	dc := topo.GetOrCreateDataCenter("dc1")
	rack := dc.GetOrCreateRack("rack1")
	dn := rack.GetOrCreateDataNode("127.0.0.1", 34534, 0, "127.0.0.1", "", map[string]uint32{"": 700})
	dn.AdjustMaxVolumeCounts(map[string]uint32{"": 700})

	dn.UpdateDiskTags([]*master_pb.DiskTag{
		{DiskId: 0, Type: "", MaxVolumeCount: 350},
		{DiskId: 1, Type: "", MaxVolumeCount: 350},
		{DiskId: 2, Type: "", MaxVolumeCount: 0}, // unavailable disk
	})

	di := dn.ToDataNodeInfo().DiskInfos[""]
	if len(di.MaxVolumeCountByDisk) != 3 {
		t.Fatalf("want 3 physical disks (incl the zero-capacity one), got %d", len(di.MaxVolumeCountByDisk))
	}
}

// An older server sending no per-disk capacity leaves the map empty.
func TestToDataNodeInfoFallsBackWhenNoCapacityReported(t *testing.T) {
	topo := NewTopology("weedfs", sequence.NewMemorySequencer(), 32*1024, 5, false)
	dc := topo.GetOrCreateDataCenter("dc1")
	rack := dc.GetOrCreateRack("rack1")
	dn := rack.GetOrCreateDataNode("127.0.0.1", 34534, 0, "127.0.0.1", "", map[string]uint32{"": 700})
	dn.AdjustMaxVolumeCounts(map[string]uint32{"": 700})

	// Older server: DiskTags carry disk_id (+tags) but no type/max.
	dn.UpdateDiskTags([]*master_pb.DiskTag{
		{DiskId: 0},
		{DiskId: 1},
	})

	di := dn.ToDataNodeInfo().DiskInfos[""]
	if len(di.MaxVolumeCountByDisk) != 0 {
		t.Fatalf("want no per-disk max (fallback), got %d", len(di.MaxVolumeCountByDisk))
	}
}
