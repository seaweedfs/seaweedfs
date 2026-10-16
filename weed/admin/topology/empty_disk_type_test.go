package topology

import (
	"fmt"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
)

// A master that answered a capacity question for a disk type a node lacks
// published an empty DiskInfo of that type for the node: no capacity, no
// volumes, DiskId 0 -- the same id as the node's real first disk. Disks are
// keyed by node and disk id here, so whichever DiskInfo the map iteration
// visited last won disk 0: an SSD-only node sometimes looked like an empty
// HDD disk (and a balance destination for HDD volumes), and an HDD node
// sometimes lost its volumes behind an empty "ssd" disk.
func TestUpdateTopologyIgnoresEmptyDiskTypeEntries(t *testing.T) {
	topo := &master_pb.TopologyInfo{
		Id: "phantom_disk_types",
		DataCenterInfos: []*master_pb.DataCenterInfo{{
			Id: "dc1",
			RackInfos: []*master_pb.RackInfo{{
				Id: "rack1",
				DataNodeInfos: []*master_pb.DataNodeInfo{
					{
						Id:      "ssd-node",
						Address: "ssd-node:8080",
						DiskInfos: map[string]*master_pb.DiskInfo{
							"ssd": {Type: "ssd", MaxVolumeCount: 200, VolumeCount: 1,
								VolumeInfos: []*master_pb.VolumeInformationMessage{{Id: 7, DiskType: "ssd"}}},
							"": {Type: ""}, // the empty entry the master made up
						},
					},
					{
						Id:      "hdd-node",
						Address: "hdd-node:8080",
						DiskInfos: map[string]*master_pb.DiskInfo{
							"": {Type: "", MaxVolumeCount: 100, VolumeCount: 1,
								VolumeInfos: []*master_pb.VolumeInformationMessage{{Id: 3}}},
							"ssd": {Type: "ssd"},
						},
					},
					{
						// An empty disk of unknown capacity with nothing else on the
						// node is a disk, not a leftover: it must stay.
						Id:        "new-node",
						Address:   "new-node:8080",
						DiskInfos: map[string]*master_pb.DiskInfo{"": {Type: ""}},
					},
				},
			}},
		}},
	}

	at := NewActiveTopology(0)
	// Map iteration order is random; repeat so a shadowed disk would show.
	for i := 0; i < 64; i++ {
		if err := at.UpdateTopology(topo); err != nil {
			t.Fatalf("UpdateTopology: %v", err)
		}
		for node, want := range map[string]string{"ssd-node": "ssd", "hdd-node": "", "new-node": ""} {
			disks := at.GetNodeDisks(node)
			if len(disks) != 1 || disks[0].DiskType != want || (node != "new-node" && disks[0].DiskInfo.MaxVolumeCount == 0) {
				var got []string
				for _, d := range disks {
					got = append(got, "\""+d.DiskType+"\"")
				}
				t.Fatalf("pass %d: %s disks %v, want exactly its real %q disk", i, node, got, want)
			}
		}
	}
	if _, _, disks := CountTopologyResources(topo); disks != 3 {
		t.Errorf("CountTopologyResources disks = %d, want 3 (a made-up empty entry is not a disk)", disks)
	}
}

// The volume index is built from the per-disk map, so a shadowed disk 0 also
// hid its volumes: planners failed to find a volume on the server that holds
// it ("Could not find volume N on source server").
func TestUpdateTopologyKeepsVolumesBehindEmptyDiskTypeEntries(t *testing.T) {
	topo := &master_pb.TopologyInfo{
		Id: "phantom_disk_types_index",
		DataCenterInfos: []*master_pb.DataCenterInfo{{
			Id: "dc1",
			RackInfos: []*master_pb.RackInfo{{
				Id: "rack1",
				DataNodeInfos: []*master_pb.DataNodeInfo{{
					Id:      "hdd-node",
					Address: "hdd-node:8080",
					DiskInfos: map[string]*master_pb.DiskInfo{
						"": {Type: "", MaxVolumeCount: 100, VolumeCount: 1,
							VolumeInfos: []*master_pb.VolumeInformationMessage{{Id: 3, Collection: "c1"}}},
						"ssd": {Type: "ssd"},
					},
				}},
			}},
		}},
	}

	at := NewActiveTopology(0)
	for i := 0; i < 64; i++ {
		if err := at.UpdateTopology(topo); err != nil {
			t.Fatalf("UpdateTopology: %v", err)
		}
		locations := at.GetVolumeLocations(3, "c1")
		if len(locations) != 1 || locations[0].ServerID != "hdd-node" {
			t.Fatalf("pass %d: volume 3 locations %+v, want it on hdd-node", i, locations)
		}
	}
}

// The made-up entry always carries DiskId 0, so it need not collide with a
// real disk: here each node's only usable disk is location 1 (location 0 is
// unavailable, so the volume server does not report it). The entry must still
// not count as a disk, nor reach planners as an empty disk of unknown
// capacity on a node that has no disk of its type.
func TestUpdateTopologyIgnoresEmptyDiskTypeEntriesAtAnyDiskId(t *testing.T) {
	topo := &master_pb.TopologyInfo{
		Id: "phantom_disk_types_no_collision",
		DataCenterInfos: []*master_pb.DataCenterInfo{{
			Id: "dc1",
			RackInfos: []*master_pb.RackInfo{{
				Id: "rack1",
				DataNodeInfos: []*master_pb.DataNodeInfo{
					{
						Id:      "ssd-node",
						Address: "ssd-node:8080",
						DiskInfos: map[string]*master_pb.DiskInfo{
							"ssd": {Type: "ssd", DiskId: 1, MaxVolumeCount: 200, VolumeCount: 1,
								MaxVolumeCountByDisk: map[uint32]int64{1: 200},
								VolumeInfos:          []*master_pb.VolumeInformationMessage{{Id: 7, DiskType: "ssd", DiskId: 1}}},
							"": {Type: ""},
						},
					},
					{
						Id:      "hdd-node",
						Address: "hdd-node:8080",
						DiskInfos: map[string]*master_pb.DiskInfo{
							"": {Type: "", DiskId: 1, MaxVolumeCount: 100, VolumeCount: 1,
								MaxVolumeCountByDisk: map[uint32]int64{1: 100},
								VolumeInfos:          []*master_pb.VolumeInformationMessage{{Id: 3, DiskId: 1}}},
							"ssd": {Type: "ssd"},
						},
					},
				},
			}},
		}},
	}

	at := NewActiveTopology(0)
	for i := 0; i < 16; i++ {
		if err := at.UpdateTopology(topo); err != nil {
			t.Fatalf("UpdateTopology: %v", err)
		}
		for node, want := range map[string]string{"ssd-node": "ssd", "hdd-node": ""} {
			disks := at.GetNodeDisks(node)
			if len(disks) != 1 || disks[0].DiskType != want || disks[0].DiskID != 1 {
				var got []string
				for _, d := range disks {
					got = append(got, fmt.Sprintf("%q@%d", d.DiskType, d.DiskID))
				}
				t.Fatalf("pass %d: %s disks %v, want only its real %q disk 1", i, node, got, want)
			}
		}
	}
	for _, disk := range at.GetDisksWithEffectiveCapacity(TaskTypeBalance, "", 1) {
		if disk.DiskID == 0 {
			t.Errorf("planners offered %s disk 0 (%q), which does not exist", disk.NodeID, disk.DiskType)
		}
	}
	if _, _, disks := CountTopologyResources(topo); disks != 2 {
		t.Errorf("CountTopologyResources disks = %d, want 2", disks)
	}
}
