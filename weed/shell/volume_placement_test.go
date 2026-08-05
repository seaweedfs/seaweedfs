package shell

import (
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage/types"
)

func placementNode(id string, maxVolumes, used int64, totalBytes, freeBytes uint64) *master_pb.DataNodeInfo {
	return &master_pb.DataNodeInfo{Id: id, DiskInfos: map[string]*master_pb.DiskInfo{
		"ssd": {
			MaxVolumeCount: maxVolumes, VolumeCount: used,
			DiskTotalBytes: totalBytes, DiskFreeBytes: freeBytes,
		},
	}}
}

// placementTopo builds a one-datacenter topology, taking each node's first
// character as its rack so the tests read compactly.
func placementTopo(nodes ...*master_pb.DataNodeInfo) *master_pb.TopologyInfo {
	byRack := map[string][]*master_pb.DataNodeInfo{}
	var order []string
	for _, n := range nodes {
		rack := n.Id[:1]
		if _, seen := byRack[rack]; !seen {
			order = append(order, rack)
		}
		byRack[rack] = append(byRack[rack], n)
	}
	var racks []*master_pb.RackInfo
	for _, r := range order {
		racks = append(racks, &master_pb.RackInfo{Id: r, DataNodeInfos: byRack[r]})
	}
	return &master_pb.TopologyInfo{DataCenterInfos: []*master_pb.DataCenterInfo{{Id: "dc1", RackInfos: racks}}}
}

func TestPickTargetPrefersTheEmptiestByFreeBytes(t *testing.T) {
	topo := placementTopo(
		placementNode("a1", 10, 1, 1000, 200),
		placementNode("a2", 10, 1, 1000, 900),
		placementNode("a3", 10, 1, 1000, 500),
	)
	got := PickTarget(topo, PlacementPreference{Source: "z9", DiskType: types.SsdType})
	if got.GetId() != "a2" {
		t.Fatalf("got %q, want the node with the most free bytes (a2)", got.GetId())
	}
}

func TestPickTargetFallsBackToSlotsWithoutReportedBytes(t *testing.T) {
	// A volume server too old to report filesystem bytes must still be ordered
	// sensibly rather than read as having zero free.
	topo := placementTopo(
		placementNode("a1", 10, 9, 0, 0),
		placementNode("a2", 10, 2, 0, 0),
	)
	got := PickTarget(topo, PlacementPreference{Source: "z9", DiskType: types.SsdType})
	if got.GetId() != "a2" {
		t.Fatalf("got %q, want the node with the most free slots (a2)", got.GetId())
	}
}

func TestPickTargetSkipsSourceExcludedAndFull(t *testing.T) {
	topo := placementTopo(
		placementNode("a1", 10, 1, 1000, 900), // the source itself
		placementNode("a2", 10, 1, 1000, 800), // already spoken for by the plan
		placementNode("a3", 5, 5, 1000, 700),  // no free slot
		placementNode("a4", 10, 1, 1000, 100),
	)
	got := PickTarget(topo, PlacementPreference{
		Source: "a1", DiskType: types.SsdType, Exclude: map[string]bool{"a2": true},
	})
	if got.GetId() != "a4" {
		t.Fatalf("got %q, want a4", got.GetId())
	}
}

func TestPickTargetReturnsNilWhenNothingFits(t *testing.T) {
	topo := placementTopo(placementNode("a1", 5, 5, 1000, 900))
	if got := PickTarget(topo, PlacementPreference{Source: "z9", DiskType: types.SsdType}); got != nil {
		t.Fatalf("got %q, want nil", got.GetId())
	}
}

func TestPickTargetHonoursAnchorDataCenter(t *testing.T) {
	topo := &master_pb.TopologyInfo{DataCenterInfos: []*master_pb.DataCenterInfo{
		{Id: "dc1", RackInfos: []*master_pb.RackInfo{{Id: "r1", DataNodeInfos: []*master_pb.DataNodeInfo{
			placementNode("a1", 10, 1, 1000, 100),
		}}}},
		{Id: "dc2", RackInfos: []*master_pb.RackInfo{{Id: "r2", DataNodeInfos: []*master_pb.DataNodeInfo{
			placementNode("b1", 10, 1, 1000, 900),
		}}}},
	}}
	got := PickTarget(topo, PlacementPreference{
		Source: "z9", DiskType: types.SsdType, AnchorDataCenter: "dc1",
	})
	if got.GetId() != "a1" {
		t.Fatalf("got %q, want a1 -- the anchor outranks an emptier node elsewhere", got.GetId())
	}
}

func TestPickTargetPrefersTheSourceRackOverAnEmptierStranger(t *testing.T) {
	topo := placementTopo(
		placementNode("a1", 10, 1, 1000, 500), // source, rack a
		placementNode("a2", 10, 1, 1000, 100), // same rack, tighter
		placementNode("b1", 10, 1, 1000, 900), // other rack, emptiest
	)
	got := PickTarget(topo, PlacementPreference{Source: "a1", DiskType: types.SsdType})
	if got.GetId() != "a2" {
		t.Fatalf("got %q, want a2 -- same rack outranks emptier", got.GetId())
	}
}

func TestPickTargetLeavesTheRackWhenItCannotHoldTheVolume(t *testing.T) {
	// Locality is a preference over candidates, never a reason to overfill.
	topo := placementTopo(
		placementNode("a1", 10, 1, 1000, 500), // source
		placementNode("a2", 5, 5, 1000, 900),  // same rack, no free slot
		placementNode("b1", 10, 1, 1000, 100),
	)
	got := PickTarget(topo, PlacementPreference{Source: "a1", DiskType: types.SsdType})
	if got.GetId() != "b1" {
		t.Fatalf("got %q, want b1 -- a full same-rack node is not a candidate", got.GetId())
	}
}

func TestPickTargetPrefersSameDataCenterOverAnother(t *testing.T) {
	topo := &master_pb.TopologyInfo{DataCenterInfos: []*master_pb.DataCenterInfo{
		{Id: "dc1", RackInfos: []*master_pb.RackInfo{
			{Id: "r1", DataNodeInfos: []*master_pb.DataNodeInfo{placementNode("a1", 10, 1, 1000, 500)}},
			{Id: "r2", DataNodeInfos: []*master_pb.DataNodeInfo{placementNode("a2", 10, 1, 1000, 100)}},
		}},
		{Id: "dc2", RackInfos: []*master_pb.RackInfo{
			{Id: "r3", DataNodeInfos: []*master_pb.DataNodeInfo{placementNode("b1", 10, 1, 1000, 900)}},
		}},
	}}
	got := PickTarget(topo, PlacementPreference{Source: "a1", DiskType: types.SsdType})
	if got.GetId() != "a2" {
		t.Fatalf("got %q, want a2 -- another rack in the same dc beats another dc", got.GetId())
	}
}

func TestPickTargetFallsBackToCapacityForAnUnknownSource(t *testing.T) {
	// A source that is not in the topology has no rack to be near, so ordering
	// degrades to capacity rather than to topology order.
	topo := placementTopo(
		placementNode("a1", 10, 1, 1000, 100),
		placementNode("b1", 10, 1, 1000, 900),
	)
	got := PickTarget(topo, PlacementPreference{Source: "gone:8080", DiskType: types.SsdType})
	if got.GetId() != "b1" {
		t.Fatalf("got %q, want b1", got.GetId())
	}
}

func TestPickTargetSpendsCapacitySoABatchSpreads(t *testing.T) {
	// The reservation this exists for: planning several moves from one snapshot
	// must not stack them all on whichever node started emptiest.
	topo := placementTopo(
		placementNode("a1", 4, 0, 4000, 4000),
		placementNode("a2", 4, 0, 4000, 3000),
	)
	seen := map[string]int{}
	for i := 0; i < 4; i++ {
		got := PickTarget(topo, PlacementPreference{Source: "z9", DiskType: types.SsdType})
		if got == nil {
			t.Fatalf("pick %d returned nil", i)
		}
		seen[got.GetId()]++
	}
	if seen["a1"] == 4 || seen["a2"] == 4 {
		t.Fatalf("all four picks landed on one node: %v", seen)
	}
	if seen["a1"]+seen["a2"] != 4 {
		t.Fatalf("unexpected spread: %v", seen)
	}
}

func TestPickTargetStopsWhenTheSnapshotIsSpent(t *testing.T) {
	// Reservation has to make a node stop being a candidate, not merely rank it
	// lower, or a batch could overcommit a tier.
	topo := placementTopo(placementNode("a1", 2, 0, 2000, 2000))
	for i := 0; i < 2; i++ {
		if got := PickTarget(topo, PlacementPreference{Source: "z9", DiskType: types.SsdType}); got == nil {
			t.Fatalf("pick %d returned nil while slots remained", i)
		}
	}
	if got := PickTarget(topo, PlacementPreference{Source: "z9", DiskType: types.SsdType}); got != nil {
		t.Fatalf("got %q, want nil once the snapshot is spent", got.GetId())
	}
}

func TestPickTargetUsesOneMetricWhenReportingIsMixed(t *testing.T) {
	// b1 has the most free bytes but the fewest slots; a2 the reverse. With a3
	// reporting no bytes at all, every candidate must be ordered on slots, or
	// the comparator is intransitive and the winner depends on sort order.
	topo := placementTopo(
		placementNode("a1", 10, 1, 1000, 500), // source
		placementNode("a2", 10, 1, 1000, 100),
		placementNode("a3", 10, 2, 0, 0), // too old to report bytes
		placementNode("a4", 10, 8, 1000, 900),
	)
	got := PickTarget(topo, PlacementPreference{Source: "a1", DiskType: types.SsdType})
	if got.GetId() != "a2" {
		t.Fatalf("got %q, want a2 -- the most free slots once bytes are unusable", got.GetId())
	}
}

func TestPickTargetReservesTheVolumeSize(t *testing.T) {
	// A volume far bigger than the tier average must be charged at its real
	// size, or a batch of them overcommits the destination.
	topo := placementTopo(
		placementNode("a1", 10, 0, 1000, 1000),
		placementNode("a2", 10, 0, 1000, 900),
	)
	first := PickTarget(topo, PlacementPreference{Source: "z9", DiskType: types.SsdType, VolumeBytes: 800})
	if first.GetId() != "a1" {
		t.Fatalf("first pick %q, want a1", first.GetId())
	}
	// a1 now reports 200 free against a2's 900, so the next move must go there
	// rather than to the node that merely started emptiest.
	second := PickTarget(topo, PlacementPreference{Source: "z9", DiskType: types.SsdType, VolumeBytes: 800})
	if second.GetId() != "a2" {
		t.Fatalf("second pick %q, want a2 after 800 bytes were spent on a1", second.GetId())
	}
}
