package topology

import (
	"slices"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/sequence"
	"github.com/seaweedfs/seaweedfs/weed/storage"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/storage/super_block"
	"github.com/seaweedfs/seaweedfs/weed/storage/types"
)

// addMaintenanceTestNode links a data node with maxVol free slots under rack.
func addMaintenanceTestNode(rack *Rack, id string, maxVol int64) *DataNode {
	dn := NewDataNode(id)
	dn.Ip = id
	rack.LinkChildNode(dn)
	dn.getOrCreateDisk("").UpAdjustDiskUsageDelta("", &DiskUsageCounts{maxVolumeCount: maxVol})
	return dn
}

func newMaintenanceTestTopology() (*Topology, *DataCenter) {
	topo := NewTopology("weedfs", sequence.NewMemorySequencer(), 32*1024, 5, false)
	dc := NewDataCenter("dc1")
	topo.LinkChildNode(dc)
	return topo, dc
}

func TestMaintenanceModeReportsNoFreeSlots(t *testing.T) {
	rack := NewRack("rack1")
	dn := addMaintenanceTestNode(rack, "server1", 10)
	option := &VolumeGrowOption{DiskType: types.HardDriveType}

	if got := dn.AvailableSpaceFor(option); got != 10 {
		t.Fatalf("AvailableSpaceFor = %d, want 10", got)
	}
	if !dn.SetMaintenanceMode(true) {
		t.Fatal("first switch should report a change")
	}
	if dn.SetMaintenanceMode(true) {
		t.Fatal("repeating the same value should not report a change")
	}
	if !dn.InMaintenanceMode() {
		t.Fatal("InMaintenanceMode = false after switching on")
	}
	if got := dn.AvailableSpaceFor(option); got != 0 {
		t.Errorf("AvailableSpaceFor in maintenance = %d, want 0: no new volumes go to a server in maintenance", got)
	}
	if got := dn.AvailableSpaceForReservation(option); got != 0 {
		t.Errorf("AvailableSpaceForReservation in maintenance = %d, want 0", got)
	}
	if _, ok := dn.TryReserveCapacity(types.HardDriveType, 1); ok {
		t.Error("reserved capacity on a server in maintenance")
	}
	if got := rack.AvailableSpaceFor(option); got != 10 {
		t.Errorf("rack AvailableSpaceFor = %d, want 10: the node's capacity still rolls up to its parents", got)
	}

	if !dn.SetMaintenanceMode(false) {
		t.Fatal("switching off should report a change")
	}
	if got := dn.AvailableSpaceFor(option); got != 10 {
		t.Errorf("AvailableSpaceFor after maintenance = %d, want 10", got)
	}
}

// A server in maintenance mode is the one being evacuated, so volume growth in
// its rack must land every replica on its siblings even when it has the most
// free slots by far.
func TestVolumeGrowthSkipsMaintenanceNodeInSameRack(t *testing.T) {
	topo, dc := newMaintenanceTestTopology()
	rack := NewRack("rack1")
	dc.LinkChildNode(rack)
	busy := addMaintenanceTestNode(rack, "busy", 1000)
	busy.SetMaintenanceMode(true)
	addMaintenanceTestNode(rack, "small1", 2)
	addMaintenanceTestNode(rack, "small2", 2)

	vg := NewDefaultVolumeGrowth()
	rp, _ := super_block.NewReplicaPlacementFromString("001")
	option := &VolumeGrowOption{ReplicaPlacement: rp, DiskType: types.HardDriveType}
	for i := 0; i < 50; i++ {
		servers, _, err := vg.findEmptySlotsForOneVolume(topo, option, false)
		if err != nil {
			t.Fatalf("iteration %d: %v", i, err)
		}
		if len(servers) != 2 {
			t.Fatalf("iteration %d: got %d servers, want 2", i, len(servers))
		}
		for _, s := range servers {
			if s.Id() == busy.Id() {
				t.Fatalf("iteration %d: picked the server in maintenance mode", i)
			}
		}
	}

	// Pinning the request to the maintenance node must fail rather than
	// create a volume there.
	rp0, _ := super_block.NewReplicaPlacementFromString("000")
	pinned := &VolumeGrowOption{ReplicaPlacement: rp0, DiskType: types.HardDriveType, DataNode: "busy"}
	if _, _, err := vg.findEmptySlotsForOneVolume(topo, pinned, false); err == nil {
		t.Fatal("a volume pinned to a server in maintenance mode must not be created")
	}
}

// The replica on another rack is chosen by walking a random offset through the
// rack's rolled-up free slots, which still count a node in maintenance. The walk
// has to land on an eligible sibling every time, not fall off the end.
func TestVolumeGrowthSkipsMaintenanceNodeInOtherRack(t *testing.T) {
	topo, dc := newMaintenanceTestTopology()
	rack1 := NewRack("rack1")
	dc.LinkChildNode(rack1)
	addMaintenanceTestNode(rack1, "r1n1", 100)
	rack2 := NewRack("rack2")
	dc.LinkChildNode(rack2)
	busy := addMaintenanceTestNode(rack2, "r2busy", 1000)
	busy.SetMaintenanceMode(true)
	ok := addMaintenanceTestNode(rack2, "r2ok", 1)

	vg := NewDefaultVolumeGrowth()
	rp, _ := super_block.NewReplicaPlacementFromString("010")
	option := &VolumeGrowOption{ReplicaPlacement: rp, DiskType: types.HardDriveType}
	for i := 0; i < 50; i++ {
		servers, _, err := vg.findEmptySlotsForOneVolume(topo, option, false)
		if err != nil {
			t.Fatalf("iteration %d: %v", i, err)
		}
		if len(servers) != 2 {
			t.Fatalf("iteration %d: got %d servers, want 2", i, len(servers))
		}
		onSibling := 0
		for _, s := range servers {
			if s.Id() == busy.Id() {
				t.Fatalf("iteration %d: picked the server in maintenance mode", i)
			}
			if s.Id() == ok.Id() {
				onSibling++
			}
		}
		if onSibling != 1 {
			t.Fatalf("iteration %d: the other-rack replica must land on the eligible sibling, got %v", i, servers)
		}
	}
}

// A replica on a server in maintenance mode takes no writes, so its volume
// leaves the writable list while the mode is on and returns once it is off.
func TestMaintenanceModeTogglesVolumeWritability(t *testing.T) {
	topo, dc := newMaintenanceTestTopology()
	rack := NewRack("rack1")
	dc.LinkChildNode(rack)
	dn1 := addMaintenanceTestNode(rack, "dn1", 10)
	dn2 := addMaintenanceTestNode(rack, "dn2", 10)

	rp, _ := super_block.NewReplicaPlacementFromString("001")
	shared := storage.VolumeInfo{Id: 1, Size: 100, ReplicaPlacement: rp, Ttl: needle.EMPTY_TTL, Version: needle.GetCurrentVersion()}
	dn1.AddOrUpdateVolume(shared)
	dn2.AddOrUpdateVolume(shared)
	topo.RegisterVolumeLayout(shared, dn1)
	topo.RegisterVolumeLayout(shared, dn2)

	rp0, _ := super_block.NewReplicaPlacementFromString("000")
	alone := storage.VolumeInfo{Id: 2, Size: 100, ReplicaPlacement: rp0, Ttl: needle.EMPTY_TTL, Version: needle.GetCurrentVersion()}
	dn2.AddOrUpdateVolume(alone)
	topo.RegisterVolumeLayout(alone, dn2)

	vlShared := topo.GetVolumeLayout("", rp, needle.EMPTY_TTL, types.HardDriveType)
	vlAlone := topo.GetVolumeLayout("", rp0, needle.EMPTY_TTL, types.HardDriveType)
	expectWritables := func(step string, vl *VolumeLayout, want ...needle.VolumeId) {
		t.Helper()
		got := vl.CloneWritableVolumes()
		slices.Sort(got)
		if !slices.Equal(got, want) {
			t.Errorf("%s: writables = %v, want %v", step, got, want)
		}
	}
	expectWritables("initial shared", vlShared, 1)
	expectWritables("initial alone", vlAlone, 2)

	if !topo.SetDataNodeMaintenanceMode(dn1, true) {
		t.Fatal("switching dn1 on should report a change")
	}
	expectWritables("dn1 in maintenance: a volume with a replica there takes no writes", vlShared)
	expectWritables("dn1 in maintenance: volumes elsewhere are unaffected", vlAlone, 2)
	if locations := vlShared.Lookup(1); len(locations) != 2 {
		t.Errorf("reads must still see every replica, got %v", locations)
	}

	if topo.SetDataNodeMaintenanceMode(dn1, true) {
		t.Error("repeating the same state should not report a change")
	}

	if !topo.SetDataNodeMaintenanceMode(dn1, false) {
		t.Fatal("switching dn1 off should report a change")
	}
	expectWritables("dn1 back: the volume is writable again", vlShared, 1)

	topo.SetDataNodeMaintenanceMode(dn2, true)
	expectWritables("dn2 in maintenance: shared", vlShared)
	expectWritables("dn2 in maintenance: alone", vlAlone)
}
