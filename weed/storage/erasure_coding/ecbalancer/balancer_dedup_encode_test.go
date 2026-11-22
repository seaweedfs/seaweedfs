package ecbalancer

import "testing"

// staleDupTopology models a retried encode: node "fresh" holds all 14 shards
// from run 200, node "stale" keeps shards 0 and 8 from a failed run 100 and has
// more free slots, so the free-slot rule alone would keep its copies.
func staleDupTopology(staleTs, freshTs int64) *Topology {
	topo := NewTopology()
	fresh := topo.AddNode("fresh", "dc1", "dc1:rack1", 5)
	fresh.AddDisk(1, "", 5, 14)
	fresh.AddShards(100, "c", 1, allBits(14))
	fresh.SetEncodeTs(100, "c", 1, freshTs)
	stale := topo.AddNode("stale", "dc1", "dc1:rack2", 50)
	stale.AddDisk(1, "", 50, 2)
	stale.AddShards(100, "c", 1, bits(0, 8))
	stale.SetEncodeTs(100, "c", 1, staleTs)
	return topo
}

func dedupMoves(topo *Topology) []Move {
	var out []Move
	for _, m := range Plan(topo, Options{ImbalanceThreshold: 0.01, Ratio: ratio(10, 4)}) {
		if m.Phase == "dedup" {
			out = append(out, m)
		}
	}
	return out
}

func TestPlanDedupKeepsMajorityEncodeRun(t *testing.T) {
	moves := dedupMoves(staleDupTopology(100, 200))
	if len(moves) != 2 {
		t.Fatalf("got %d dedup moves, want 2: %+v", len(moves), moves)
	}
	for _, m := range moves {
		if m.SourceNode != "stale" || m.KeepNode != "fresh" {
			t.Errorf("dedup move %+v: want the stale run's copy deleted and the majority run's copy kept", m)
		}
	}
}

func TestPlanDedupSameEncodeRunUsesFreeSlots(t *testing.T) {
	moves := dedupMoves(staleDupTopology(200, 200))
	if len(moves) != 2 {
		t.Fatalf("got %d dedup moves, want 2: %+v", len(moves), moves)
	}
	for _, m := range moves {
		if m.SourceNode != "fresh" || m.KeepNode != "stale" {
			t.Errorf("dedup move %+v: copies of one run keep the node with more free slots", m)
		}
	}
}

func TestPlanDedupRefusesWithoutMajority(t *testing.T) {
	topo := NewTopology()
	a := topo.AddNode("a", "dc1", "dc1:rack1", 5)
	a.AddDisk(1, "", 5, 7)
	a.AddShards(100, "c", 1, bits(0, 1, 2, 3, 4, 5, 6))
	a.SetEncodeTs(100, "c", 1, 100)
	b := topo.AddNode("b", "dc1", "dc1:rack2", 50)
	b.AddDisk(1, "", 50, 7)
	b.AddShards(100, "c", 1, bits(0, 7, 8, 9, 10, 11, 12))
	b.SetEncodeTs(100, "c", 1, 200)
	if moves := dedupMoves(topo); len(moves) != 0 {
		t.Fatalf("two runs tie for the majority, so no copy may be chosen; got %+v", moves)
	}
}

func TestPlanDedupRefusesWhenMajorityIsUnreported(t *testing.T) {
	if moves := dedupMoves(staleDupTopology(200, 0)); len(moves) != 0 {
		t.Fatalf("the majority run carries no identity, so copies cannot be compared; got %+v", moves)
	}
}
