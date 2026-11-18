package topology

import (
	"fmt"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/storage/super_block"
	"github.com/seaweedfs/seaweedfs/weed/storage/types"
)

// growSpreadVolumes places count volumes one by one the way a grow request
// does: each one is registered in its layout before the next is placed.
// It returns how many replicas of the layout each data node received.
func growSpreadVolumes(t *testing.T, topo *Topology, option *VolumeGrowOption, count int) map[string]int {
	t.Helper()
	vg := NewDefaultVolumeGrowth()
	perNode := make(map[string]int)
	for i := 0; i < count; i++ {
		servers, _, err := vg.findEmptySlotsForOneVolume(topo, option, false)
		if err != nil {
			t.Fatalf("volume %d: %v", i, err)
		}
		vi := storage.VolumeInfo{
			Id:               needle.VolumeId(i + 1),
			Collection:       option.Collection,
			ReplicaPlacement: option.ReplicaPlacement,
			Ttl:              option.Ttl,
			Version:          needle.GetCurrentVersion(),
		}
		for _, dn := range servers {
			dn.AddOrUpdateVolume(vi)
			topo.RegisterVolumeLayout(vi, dn)
			perNode[string(dn.Id())]++
		}
	}
	return perNode
}

// newSpreadTopology builds dcs data centers with one rack each, holding
// nodesPerDc single-disk servers, and returns the racks.
func newSpreadTopology(dcs, nodesPerDc int, maxVol int64) (*Topology, []*Rack) {
	topo, _ := newMaintenanceTestTopology()
	topo.UnlinkChildNode("dc1")
	var racks []*Rack
	for d := 0; d < dcs; d++ {
		dc := NewDataCenter(fmt.Sprintf("dc%d", d))
		topo.LinkChildNode(dc)
		rack := NewRack("rack1")
		dc.LinkChildNode(rack)
		racks = append(racks, rack)
		for n := 0; n < nodesPerDc; n++ {
			addMaintenanceTestNode(rack, fmt.Sprintf("dc%d-server%d", d, n), maxVol)
		}
	}
	return topo, racks
}

// A grow request smaller than the number of servers must not put two writable
// volumes of the layout on one server. Free slots alone would pick servers
// independently at random and collide.
func TestVolumeGrowthSpreadsWritableVolumesOverServers(t *testing.T) {
	rp, _ := super_block.NewReplicaPlacementFromString("000")
	option := &VolumeGrowOption{Collection: "spread", ReplicaPlacement: rp, Ttl: needle.EMPTY_TTL, DiskType: types.HardDriveType}
	for trial := 0; trial < 20; trial++ {
		topo, _ := newSpreadTopology(1, 16, 100)
		perNode := growSpreadVolumes(t, topo, option, 16)
		for node, n := range perNode {
			if n != 1 {
				t.Fatalf("trial %d: %s holds %d writable volumes of the layout, want 1: %v", trial, node, n, perNode)
			}
		}
	}
}

// Replicas placed in another data center go through the same rule.
func TestVolumeGrowthSpreadsReplicasInOtherDataCenters(t *testing.T) {
	rp, _ := super_block.NewReplicaPlacementFromString("100")
	option := &VolumeGrowOption{Collection: "spread", ReplicaPlacement: rp, Ttl: needle.EMPTY_TTL, DiskType: types.HardDriveType}
	for trial := 0; trial < 20; trial++ {
		topo, _ := newSpreadTopology(2, 8, 100)
		perNode := growSpreadVolumes(t, topo, option, 8)
		if len(perNode) != 16 {
			t.Fatalf("trial %d: replicas on %d servers, want all 16: %v", trial, len(perNode), perNode)
		}
	}
}

// A server with several disks takes a share of the layout's writable volumes
// in proportion to its disks: load is counted per disk, not per server.
func TestVolumeGrowthCountsLoadPerDisk(t *testing.T) {
	rp, _ := super_block.NewReplicaPlacementFromString("000")
	option := &VolumeGrowOption{Collection: "spread", ReplicaPlacement: rp, Ttl: needle.EMPTY_TTL, DiskType: types.HardDriveType}
	for trial := 0; trial < 20; trial++ {
		topo, racks := newSpreadTopology(1, 3, 100)
		multi := addMaintenanceTestNode(racks[0], "multi", 300)
		multi.UpdateDiskTags([]*master_pb.DiskTag{{DiskId: 0, MaxVolumeCount: 100}, {DiskId: 1, MaxVolumeCount: 100}, {DiskId: 2, MaxVolumeCount: 100}})
		perNode := growSpreadVolumes(t, topo, option, 6)
		if perNode["multi"] != 3 {
			t.Fatalf("trial %d: the 3-disk server got %d volumes of 6, want 3: %v", trial, perNode["multi"], perNode)
		}
	}
}

// Disks that report no volume slots (unavailable, or low on space) do not
// count: a server with one usable disk of four takes one disk's share.
func TestVolumeGrowthSkipsDisksWithoutSlots(t *testing.T) {
	rp, _ := super_block.NewReplicaPlacementFromString("000")
	option := &VolumeGrowOption{Collection: "spread", ReplicaPlacement: rp, Ttl: needle.EMPTY_TTL, DiskType: types.HardDriveType}
	for trial := 0; trial < 20; trial++ {
		topo, racks := newSpreadTopology(1, 3, 100)
		damaged := addMaintenanceTestNode(racks[0], "damaged", 100)
		damaged.UpdateDiskTags([]*master_pb.DiskTag{{DiskId: 0, MaxVolumeCount: 100}, {DiskId: 1}, {DiskId: 2}, {DiskId: 3}})
		perNode := growSpreadVolumes(t, topo, option, 8)
		if perNode["damaged"] != 2 {
			t.Fatalf("trial %d: the server with one usable disk got %d volumes of 8, want 2: %v", trial, perNode["damaged"], perNode)
		}
	}
}

// A server that refused an allocation in this grow request offers no slots
// for the rest of it, so the retry places the volume elsewhere.
func TestVolumeGrowthSkipsRefusingServer(t *testing.T) {
	rp, _ := super_block.NewReplicaPlacementFromString("000")
	option := &VolumeGrowOption{Collection: "spread", ReplicaPlacement: rp, Ttl: needle.EMPTY_TTL, DiskType: types.HardDriveType}
	topo, racks := newSpreadTopology(1, 4, 100)
	refusing := racks[0].Children()[0].(*DataNode)
	placed := option.withLayoutLoad(topo)
	placed.refused = map[*DataNode]bool{refusing: true}
	vg := NewDefaultVolumeGrowth()
	for i := 0; i < 50; i++ {
		servers, _, err := vg.findEmptySlotsForOneVolume(topo, placed, false)
		if err != nil {
			t.Fatal(err)
		}
		if servers[0] == refusing {
			t.Fatalf("attempt %d picked %s, which refused an allocation", i, refusing.Id())
		}
	}
}

// The refusing server's free slots also leave the rack and data center
// weights, so a much emptier rack holding only that server is not picked.
func TestVolumeGrowthSkipsRefusingServerInOtherRack(t *testing.T) {
	rp, _ := super_block.NewReplicaPlacementFromString("000")
	option := &VolumeGrowOption{Collection: "spread", ReplicaPlacement: rp, Ttl: needle.EMPTY_TTL, DiskType: types.HardDriveType}
	topo, racks := newSpreadTopology(1, 0, 0)
	refusing := addMaintenanceTestNode(racks[0], "refusing", 100)
	otherRack := NewRack("rack2")
	racks[0].Parent().LinkChildNode(otherRack)
	healthy := addMaintenanceTestNode(otherRack, "healthy", 1)
	placed := option.withLayoutLoad(topo)
	placed.refused = map[*DataNode]bool{refusing: true}
	vg := NewDefaultVolumeGrowth()
	for i := 0; i < 50; i++ {
		servers, _, err := vg.findEmptySlotsForOneVolume(topo, placed, false)
		if err != nil {
			t.Fatal(err)
		}
		if servers[0] != healthy {
			t.Fatalf("attempt %d picked %s, want %s", i, servers[0].Id(), healthy.Id())
		}
	}
}

func TestVolumeGrowthSkipsRefusingServerWhenItsRackKeepsOneServer(t *testing.T) {
	rp, _ := super_block.NewReplicaPlacementFromString("011")
	option := &VolumeGrowOption{Collection: "spread", ReplicaPlacement: rp, Ttl: needle.EMPTY_TTL, DiskType: types.HardDriveType}
	topo, racks := newSpreadTopology(1, 0, 0)
	refusing := addMaintenanceTestNode(racks[0], "refusing", 100)
	addMaintenanceTestNode(racks[0], "healthy1", 100)
	otherRack := NewRack("rack2")
	racks[0].Parent().LinkChildNode(otherRack)
	addMaintenanceTestNode(otherRack, "healthy2", 100)
	addMaintenanceTestNode(otherRack, "healthy3", 100)
	placed := option.withLayoutLoad(topo)
	placed.refused = map[*DataNode]bool{refusing: true}
	vg := NewDefaultVolumeGrowth()
	for i := 0; i < 50; i++ {
		servers, _, err := vg.findEmptySlotsForOneVolume(topo, placed, false)
		if err != nil {
			t.Fatalf("attempt %d: %v", i, err)
		}
		for _, s := range servers {
			if s == refusing {
				t.Fatalf("attempt %d picked the refusing server", i)
			}
		}
	}
}
