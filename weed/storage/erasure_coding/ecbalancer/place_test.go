package ecbalancer

import (
	"fmt"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/storage/erasure_coding"
	"github.com/seaweedfs/seaweedfs/weed/storage/super_block"
)

// buildPlaceTopo makes a topology of racks x nodesPerRack, each node one disk with
// perDiskFree free EC shard slots.
func buildPlaceTopo(racks, nodesPerRack, perDiskFree int) *Topology {
	topo := NewTopology()
	for r := 0; r < racks; r++ {
		rackKey := fmt.Sprintf("dc1:rack%d", r)
		for n := 0; n < nodesPerRack; n++ {
			id := fmt.Sprintf("10.0.%d.%d:8080", r, n)
			node := topo.AddNode(id, "dc1", rackKey, perDiskFree)
			node.AddDisk(0, "", perDiskFree, 0)
		}
	}
	return topo
}

func allShards() []int {
	out := make([]int, erasure_coding.TotalShardsCount)
	for i := range out {
		out[i] = i
	}
	return out
}

// TestPlaceStrictSpreadAndCaps places a fresh 10+4 volume and checks every shard
// lands on a distinct node and no rack exceeds the even per-type cap.
func TestPlaceStrictSpreadAndCaps(t *testing.T) {
	const racks = 4
	topo := buildPlaceTopo(racks, 4, 50)

	res, err := topo.Place(1, "c1", allShards(), Constraints{}, PlaceStrict)
	if err != nil {
		t.Fatalf("Place: %v", err)
	}
	if len(res.Destinations) != erasure_coding.TotalShardsCount {
		t.Fatalf("placed %d shards, want %d", len(res.Destinations), erasure_coding.TotalShardsCount)
	}

	usedNodes := map[string]bool{}
	dataPerRack := map[string]int{}
	parityPerRack := map[string]int{}
	for sid, d := range res.Destinations {
		if usedNodes[d.Node] {
			t.Errorf("node %s reused for shard %d (expected distinct nodes with ample capacity)", d.Node, sid)
		}
		usedNodes[d.Node] = true
		if sid < erasure_coding.DataShardsCount {
			dataPerRack[d.Rack]++
		} else {
			parityPerRack[d.Rack]++
		}
	}
	dataCap := ceilDivide(erasure_coding.DataShardsCount, racks)
	parityCap := ceilDivide(erasure_coding.ParityShardsCount, racks)
	for rk, n := range dataPerRack {
		if n > dataCap {
			t.Errorf("rack %s holds %d data shards, cap %d", rk, n, dataCap)
		}
	}
	for rk, n := range parityPerRack {
		if n > parityCap {
			t.Errorf("rack %s holds %d parity shards, cap %d", rk, n, parityCap)
		}
	}
}

// TestPlaceStrictFailsAndRollsBack: a single tiny disk cannot hold 14 shards, so
// strict Place fails and leaves the snapshot untouched.
func TestPlaceStrictFailsAndRollsBack(t *testing.T) {
	topo := buildPlaceTopo(1, 1, 2) // one node, room for 2 shards
	node := topo.nodes["10.0.0.0:8080"]
	freeBefore := node.freeSlots
	diskFreeBefore := node.disks[0].freeSlots

	_, err := topo.Place(1, "c1", allShards(), Constraints{}, PlaceStrict)
	if err == nil {
		t.Fatal("expected Place to fail on insufficient capacity")
	}
	if info, ok := node.shards[volKey{collection: "c1", vid: 1}]; ok && info.shardBits.Count() != 0 {
		t.Errorf("volume shard bits left on node after failed strict Place (rollback incomplete): %b", info.shardBits)
	}
	if node.freeSlots != freeBefore {
		t.Errorf("node freeSlots = %d after rollback, want %d", node.freeSlots, freeBefore)
	}
	if node.disks[0].freeSlots != diskFreeBefore {
		t.Errorf("disk freeSlots = %d after rollback, want %d", node.disks[0].freeSlots, diskFreeBefore)
	}
}

// TestPlaceDurabilityFirstRelaxesRP: a ReplicaPlacement rack limit too tight for
// the shard count makes strict fail, while durability-first relaxes RP to place
// everything and reports the relaxation.
func TestPlaceDurabilityFirstRelaxesRP(t *testing.T) {
	rp := &super_block.ReplicaPlacement{DiffRackCount: 3} // <=3 shards per rack
	topo := buildPlaceTopo(2, 8, 50)                      // 2 racks: 2*3=6 < 14 under RP

	if _, err := topo.Place(1, "c1", allShards(), Constraints{ReplicaPlacement: rp}, PlaceStrict); err == nil {
		t.Fatal("strict Place should fail when RP rack limit cannot fit all shards")
	}

	topo = buildPlaceTopo(2, 8, 50)
	res, err := topo.Place(1, "c1", allShards(), Constraints{ReplicaPlacement: rp}, PlaceDurabilityFirst)
	if err != nil {
		t.Fatalf("durability-first Place: %v", err)
	}
	if len(res.Destinations) != erasure_coding.TotalShardsCount {
		t.Fatalf("placed %d shards, want %d", len(res.Destinations), erasure_coding.TotalShardsCount)
	}
	relaxedRP := false
	for _, r := range res.Relaxed {
		if r == "replica-placement" {
			relaxedRP = true
		}
	}
	if !relaxedRP {
		t.Errorf("expected replica-placement relaxation, got %v", res.Relaxed)
	}
}
