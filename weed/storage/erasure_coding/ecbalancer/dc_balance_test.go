package ecbalancer

import (
	"fmt"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/storage/erasure_coding"
	"github.com/seaweedfs/seaweedfs/weed/storage/super_block"
)

// TestDetectCrossDCImbalanceDrainsOverCapDC: a volume with all its shards in one
// data center is drained down to DiffDataCenterCount, with the excess moved to the
// other DC.
func TestDetectCrossDCImbalanceDrainsOverCapDC(t *testing.T) {
	vk := volKey{collection: "c1", vid: 1}
	topo := NewTopology()
	for dc := 0; dc < 2; dc++ {
		dcID := fmt.Sprintf("dc%d", dc)
		for r := 0; r < 10; r++ {
			rackKey := fmt.Sprintf("%s:rack%d", dcID, r)
			n := topo.AddNode(fmt.Sprintf("%s-n%d:8080", dcID, r), dcID, rackKey, 50)
			n.AddDisk(0, "", 50, 0)
			if dc == 0 {
				n.AddShards(1, "c1", 0, erasure_coding.ShardBits(uint32(1)<<uint(r))) // 10 shards, all in dc0
			}
		}
	}

	rp := &super_block.ReplicaPlacement{DiffDataCenterCount: 7}
	moves := detectCrossDCImbalance(vk, topo.nodes, buildRacks(topo.nodes), "", erasure_coding.DataShardsCount, erasure_coding.ParityShardsCount, rp)
	if len(moves) != 3 { // 10 - 7
		t.Fatalf("expected 3 cross-DC moves, got %d", len(moves))
	}
	for _, m := range moves {
		if m.phase != "cross_dc" {
			t.Errorf("unexpected phase %q", m.phase)
		}
	}

	// detectCrossDCImbalance mutates the snapshot inline, so recount.
	perDC := map[string]int{}
	for _, n := range topo.nodes {
		if info, ok := n.shards[vk]; ok {
			perDC[n.dc] += info.shardBits.Count()
		}
	}
	if perDC["dc0"] > rp.DiffDataCenterCount {
		t.Errorf("dc0 still over cap: %d > %d", perDC["dc0"], rp.DiffDataCenterCount)
	}
	if perDC["dc0"]+perDC["dc1"] != 10 {
		t.Errorf("shard count changed during rebalance: %v", perDC)
	}
}

// TestDetectCrossDCImbalanceBoundedProportional: with a loose DiffDataCenterCount,
// the phase spreads shards evenly across DCs (down to the durability floor) rather
// than only draining to the cap. 14 shards in one DC, 2 DCs, cap=10: bounded target
// is min(10, max(ceil(14/2)=7, parity=4)) = 7, so it drains 14->7 (7 moves -> 7/7),
// not 14->10 (4 moves -> 10/4).
func TestDetectCrossDCImbalanceBoundedProportional(t *testing.T) {
	vk := volKey{collection: "c1", vid: 1}
	topo := NewTopology()
	for dc := 0; dc < 2; dc++ {
		dcID := fmt.Sprintf("dc%d", dc)
		for r := 0; r < erasure_coding.TotalShardsCount; r++ {
			rackKey := fmt.Sprintf("%s:rack%d", dcID, r)
			n := topo.AddNode(fmt.Sprintf("%s-n%d:8080", dcID, r), dcID, rackKey, 50)
			n.AddDisk(0, "", 50, 0)
			if dc == 0 {
				n.AddShards(1, "c1", 0, erasure_coding.ShardBits(uint32(1)<<uint(r))) // all 14 shards in dc0
			}
		}
	}

	rp := &super_block.ReplicaPlacement{DiffDataCenterCount: 10} // loose cap (10 of 14)
	moves := detectCrossDCImbalance(vk, topo.nodes, buildRacks(topo.nodes), "", erasure_coding.DataShardsCount, erasure_coding.ParityShardsCount, rp)
	if len(moves) != 7 {
		t.Fatalf("expected 7 cross-DC moves (bounded 14->7), got %d", len(moves))
	}

	perDC := map[string]int{}
	for _, n := range topo.nodes {
		if info, ok := n.shards[vk]; ok {
			perDC[n.dc] += info.shardBits.Count()
		}
	}
	if perDC["dc0"] != 7 || perDC["dc1"] != 7 {
		t.Errorf("expected even 7/7 spread under the loose cap, got dc0=%d dc1=%d", perDC["dc0"], perDC["dc1"])
	}
}

// TestDetectCrossDCImbalanceNoOp: without a DC constraint the phase proposes
// nothing (so the balancer's behavior is unchanged for non-DC placements).
func TestDetectCrossDCImbalanceNoOp(t *testing.T) {
	vk := volKey{collection: "c1", vid: 1}
	topo := NewTopology()
	for r := 0; r < 4; r++ {
		n := topo.AddNode(fmt.Sprintf("n%d:8080", r), "dc1", fmt.Sprintf("dc1:rack%d", r), 50)
		n.AddDisk(0, "", 50, 0)
		n.AddShards(1, "c1", 0, erasure_coding.ShardBits(uint32(1)<<uint(r)))
	}

	if moves := detectCrossDCImbalance(vk, topo.nodes, buildRacks(topo.nodes), "", erasure_coding.DataShardsCount, erasure_coding.ParityShardsCount, nil); moves != nil {
		t.Errorf("expected no moves with nil ReplicaPlacement, got %d", len(moves))
	}
	rp := &super_block.ReplicaPlacement{DiffRackCount: 2} // no DC digit
	if moves := detectCrossDCImbalance(vk, topo.nodes, buildRacks(topo.nodes), "", erasure_coding.DataShardsCount, erasure_coding.ParityShardsCount, rp); moves != nil {
		t.Errorf("expected no moves when DiffDataCenterCount is 0, got %d", len(moves))
	}
}

// TestBalanceCrossRackAllowsSameDCMoveAtDCCap: a DC sitting exactly at its
// DiffDataCenterCount cap must still permit intra-DC cross-rack rebalancing — a
// same-DC move leaves the DC total unchanged. Regression test for the per-DC cap
// predicate previously rejecting every target rack in a capped DC, which blocked
// legal within-DC spreads after detectCrossDCImbalance drained both DCs to the cap.
func TestBalanceCrossRackAllowsSameDCMoveAtDCCap(t *testing.T) {
	vk := volKey{collection: "c1", vid: 1}
	topo := NewTopology()
	mk := func(node, dc, rack string, bits uint32) {
		n := topo.AddNode(node, dc, rack, 50)
		n.AddDisk(0, "", 50, 0)
		if bits != 0 {
			n.AddShards(1, "c1", 0, erasure_coding.ShardBits(bits))
		}
	}
	// Both DCs hold 5 shards (the cap), each concentrated on one rack with an empty
	// peer rack in the same DC. The only legal targets are the same-DC empty racks.
	mk("dc0-n0:8080", "dc0", "dc0:rack0", 0x1F)  // shards 0..4
	mk("dc0-n1:8080", "dc0", "dc0:rack1", 0)     // empty, same DC as rack0
	mk("dc1-n0:8080", "dc1", "dc1:rack2", 0x3E0) // shards 5..9
	mk("dc1-n1:8080", "dc1", "dc1:rack3", 0)     // empty, same DC as rack2

	racks := buildRacks(topo.nodes)
	rp := &super_block.ReplicaPlacement{DiffDataCenterCount: 5}
	shardsPerRack, _ := shardsByGroup(vk, topo.nodes, erasure_coding.DataShardsCount, func(n *Node) string { return n.rack })
	rackShardCount := countShardsByRack(vk, topo.nodes)

	// maxPerRack=2 forces each over-full rack to shed into its empty same-DC peer.
	moves := balanceShardTypeAcrossRacks(vk, topo.nodes, racks, "", erasure_coding.DataShardsCount, erasure_coding.ParityShardsCount,
		shardsPerRack, rackShardCount, 2, nil, rp)
	if len(moves) == 0 {
		t.Fatal("expected same-DC cross-rack moves when the DC is at its DC cap, got none")
	}
	for _, m := range moves {
		if m.source.dc != m.target.dc {
			t.Errorf("expected same-DC move, got %s -> %s", m.source.dc, m.target.dc)
		}
	}
}
