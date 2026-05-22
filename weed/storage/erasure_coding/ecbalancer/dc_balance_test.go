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
	moves := detectCrossDCImbalance(vk, topo.nodes, buildRacks(topo.nodes), "", erasure_coding.DataShardsCount, rp)
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

	if moves := detectCrossDCImbalance(vk, topo.nodes, buildRacks(topo.nodes), "", erasure_coding.DataShardsCount, nil); moves != nil {
		t.Errorf("expected no moves with nil ReplicaPlacement, got %d", len(moves))
	}
	rp := &super_block.ReplicaPlacement{DiffRackCount: 2} // no DC digit
	if moves := detectCrossDCImbalance(vk, topo.nodes, buildRacks(topo.nodes), "", erasure_coding.DataShardsCount, rp); moves != nil {
		t.Errorf("expected no moves when DiffDataCenterCount is 0, got %d", len(moves))
	}
}
