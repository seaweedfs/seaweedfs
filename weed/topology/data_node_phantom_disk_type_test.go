package topology

import (
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/sequence"
	"github.com/seaweedfs/seaweedfs/weed/storage/types"
)

// Asking whether a node has room for a disk type it does not have must not
// make the node report that disk type. Volume growth walks every data center,
// rack and data node with AvailableSpaceFor; before the fix that walk left an
// empty DiskInfo (MaxVolumeCount 0) of the asked type on every node, so an
// HDD-only server advertised an "ssd" disk and an SSD-only one an "hdd" disk.
// Planners that read DiskInfos then took such a node for the emptiest
// destination of that type.
func TestCapacityQueriesDoNotCreateDiskTypes(t *testing.T) {
	topo := NewTopology("weedfs", sequence.NewMemorySequencer(), 32*1024, 5, false)
	dc := topo.GetOrCreateDataCenter("dc1")
	rack := dc.GetOrCreateRack("rack1")
	hddNode := rack.GetOrCreateDataNode("127.0.0.1", 34534, 0, "127.0.0.1", "", map[string]uint32{"": 10})
	hddNode.AdjustMaxVolumeCounts(map[string]uint32{"": 10})
	ssdNode := rack.GetOrCreateDataNode("127.0.0.2", 34534, 0, "127.0.0.2", "", map[string]uint32{"ssd": 4})
	ssdNode.AdjustMaxVolumeCounts(map[string]uint32{"ssd": 4})

	hdd := &VolumeGrowOption{DiskType: types.HardDriveType}
	ssd := &VolumeGrowOption{DiskType: types.SsdType}
	nvme := &VolumeGrowOption{DiskType: types.ToDiskType("nvme")}

	// What growing a volume of each type does: reserve a slot by walking the
	// tree, plus the assign path's capacity checks on the topology root.
	for _, option := range []*VolumeGrowOption{hdd, ssd, nvme} {
		_, _ = topo.ReserveOneVolume(0, option)
		_ = topo.AvailableSpaceFor(option)
		_ = topo.CapacityFor(option)
		for _, dn := range []*DataNode{hddNode, ssdNode} {
			_ = dn.AvailableSpaceForReservation(option)
			_ = dn.CapacityFor(option)
		}
	}

	if got := hddNode.AvailableSpaceFor(ssd); got != 0 {
		t.Errorf("hdd node AvailableSpaceFor(ssd) = %d, want 0", got)
	}
	if got := topo.CapacityFor(nvme); got != 0 {
		t.Errorf("topology CapacityFor(nvme) = %d, want 0: no server has that disk type", got)
	}
	if got := topo.AvailableSpaceFor(hdd); got != 10 {
		t.Errorf("topology AvailableSpaceFor(hdd) = %d, want 10", got)
	}
	if got := topo.AvailableSpaceFor(ssd); got != 4 {
		t.Errorf("topology AvailableSpaceFor(ssd) = %d, want 4", got)
	}

	check := func(name string, dn *DataNode, want string) {
		t.Helper()
		info := dn.ToDataNodeInfo(VolumeFilter{})
		if len(info.DiskInfos) != 1 {
			keys := make([]string, 0, len(info.DiskInfos))
			for k := range info.DiskInfos {
				keys = append(keys, "\""+k+"\"")
			}
			t.Errorf("%s reports disk types %v, want only %q", name, keys, want)
		}
		if di := info.DiskInfos[want]; di == nil || di.MaxVolumeCount == 0 {
			t.Errorf("%s lost its real %q disk: %+v", name, want, di)
		}
	}
	check("hdd node", hddNode, "")
	check("ssd node", ssdNode, "ssd")

	// A usage entry created by a pre-fix capacity walk has no disk child: the
	// node stops advertising it without waiting for a restart.
	hddNode.diskUsages.getOrCreateDisk(types.SsdType)
	check("hdd node", hddNode, "")
}
