package topology

import (
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/storage/types"
)

// An over-committed node (more volumes registered than it has slots for) must
// report zero free slots, not a negative count that drags the cluster total
// below the slots healthy servers can still offer.
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
	// Planners add refunds and moves to the published count, so it keeps the deficit.
	if got := dn.diskUsages.ToDiskInfo()[""].FreeVolumeCount; got != -3 {
		t.Errorf("disk info FreeVolumeCount = %d, want -3", got)
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

func TestFreeVolumeCountSkipsOverCommittedServer(t *testing.T) {
	topo := NewTopology("free-slots", nil, 32*1024*1024*1024, 5, false)
	dc1 := topo.GetOrCreateDataCenter("dc1")
	rack1 := dc1.GetOrCreateRack("rack1")
	over := rack1.GetOrCreateDataNode("10.0.0.1", 8080, 18080, "", "", map[string]uint32{"": 5})
	over.getOrCreateDisk("").UpAdjustDiskUsageDelta("", &DiskUsageCounts{volumeCount: 20})
	healthy := rack1.GetOrCreateDataNode("10.0.0.2", 8080, 18080, "", "", map[string]uint32{"": 10})
	dc2 := topo.GetOrCreateDataCenter("dc2")
	other := dc2.GetOrCreateRack("rack1").GetOrCreateDataNode("10.0.0.3", 8080, 18080, "", "", map[string]uint32{"": 4})
	other.getOrCreateDisk("").UpAdjustDiskUsageDelta("", &DiskUsageCounts{volumeCount: 1})

	option := &VolumeGrowOption{DiskType: types.HardDriveType}
	for _, c := range []struct {
		name string
		node Node
		want int64
	}{
		{"rack1", rack1, 10},
		{"dc1", dc1, 10},
		{"dc2", dc2, 3},
		{"topology", topo, 13},
	} {
		if got := c.node.AvailableSpaceFor(option); got != c.want {
			t.Errorf("%s AvailableSpaceFor = %d, want %d", c.name, got, c.want)
		}
	}

	for i := 0; i < 20; i++ {
		first, rest, err := topo.PickNodesByWeight(2, option, func(Node) error { return nil })
		if err != nil {
			t.Fatalf("topology PickNodesByWeight: %v", err)
		}
		if picked := map[Node]bool{first: true, rest[0]: true}; !picked[dc1] || !picked[dc2] {
			t.Fatalf("topology picked %s and %s, want dc1 and dc2", first.Id(), rest[0].Id())
		}
		first, _, err = rack1.PickNodesByWeight(1, option, func(Node) error { return nil })
		if err != nil {
			t.Fatalf("rack1 PickNodesByWeight: %v", err)
		}
		if first != Node(healthy) {
			t.Fatalf("rack1 picked %s, want %s", first.Id(), healthy.Id())
		}
	}

	assigned := map[*DataNode]int{}
	for r := int64(0); r < 13; r++ {
		dn, err := topo.ReserveOneVolume(r, option)
		if err != nil {
			t.Fatalf("ReserveOneVolume(%d): %v", r, err)
		}
		assigned[dn]++
	}
	if assigned[over] != 0 || assigned[healthy] == 0 {
		t.Errorf("ReserveOneVolume spread over=%d healthy=%d other=%d, want the healthy servers only",
			assigned[over], assigned[healthy], assigned[other])
	}
}
