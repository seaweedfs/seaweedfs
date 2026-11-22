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

func planVolume(topo *Topology, vid uint32) (dedup, other []Move) {
	for _, m := range Plan(topo, Options{ImbalanceThreshold: 0.01, Ratio: ratio(10, 4)}) {
		switch {
		case m.VolumeID != vid:
		case m.Phase == "dedup":
			dedup = append(dedup, m)
		default:
			other = append(other, m)
		}
	}
	return dedup, other
}

func TestPlanDedupKeepsCompleteEncodeRun(t *testing.T) {
	dedup, _ := planVolume(staleDupTopology(100, 200), 100)
	if len(dedup) != 2 {
		t.Fatalf("got %d dedup moves, want 2: %+v", len(dedup), dedup)
	}
	for _, m := range dedup {
		if m.SourceNode != "stale" || m.KeepNode != "fresh" {
			t.Errorf("dedup move %+v: want the stale run's copy deleted and the complete run's copy kept", m)
		}
	}
}

func TestPlanDedupSameEncodeRunUsesFreeSlots(t *testing.T) {
	dedup, _ := planVolume(staleDupTopology(200, 200), 100)
	if len(dedup) != 2 {
		t.Fatalf("got %d dedup moves, want 2: %+v", len(dedup), dedup)
	}
	for _, m := range dedup {
		if m.SourceNode != "fresh" || m.KeepNode != "stale" {
			t.Errorf("dedup move %+v: copies of one run keep the node with more free slots", m)
		}
	}
}

func TestPlanDedupRefusesWhenCompleteRunIsUnreported(t *testing.T) {
	if dedup, other := planVolume(staleDupTopology(200, 0), 100); len(dedup)+len(other) != 0 {
		t.Fatalf("the complete run carries no identity, so copies cannot be compared; got %+v %+v", dedup, other)
	}
}

// An unreported copy may be the newer one, so a second complete run that does
// carry an identity cannot win over it.
func TestPlanDedupRefusesWhenUnreportedRunIsAlsoComplete(t *testing.T) {
	topo := staleDupTopology(200, 0)
	stale := topo.nodes["stale"]
	stale.AddDisk(2, "", 50, 14)
	stale.AddShards(100, "c", 2, allBits(14))
	stale.SetEncodeTs(100, "c", 2, 200)
	if dedup, other := planVolume(topo, 100); len(dedup)+len(other) != 0 {
		t.Fatalf("two complete runs, one unreported, must leave the volume alone; got %+v %+v", dedup, other)
	}
}

// A node holding one shard on two disks of different runs has no duplicate on
// another node, yet a move of that shard could take the stale copy and delete
// the fresh one.
func TestPlanDedupFreezesVolumeWithMixedRunsOnOneNode(t *testing.T) {
	topo := NewTopology()
	a := topo.AddNode("a", "dc1", "dc1:rack1", 50)
	a.AddDisk(1, "", 50, 14)
	a.AddDisk(2, "", 50, 1)
	a.AddShards(100, "c", 1, allBits(14))
	a.SetEncodeTs(100, "c", 1, 200)
	a.AddShards(100, "c", 2, bits(0))
	a.SetEncodeTs(100, "c", 2, 100)
	addEmptyNode(topo, "e1", "dc1:rack2")
	addEmptyNode(topo, "e2", "dc1:rack3")
	if dedup, other := planVolume(topo, 100); len(dedup)+len(other) != 0 {
		t.Fatalf("a volume whose shard copies on one node disagree must not be touched; got dedup %+v, moves %+v", dedup, other)
	}
}

// Neither run is complete in the snapshot (a filter may hide the rest of
// either), so no copy may be chosen, and no later phase may move a copy of the
// shared shard onto the other.
func TestPlanDedupFreezesVolumeWithoutCompleteRun(t *testing.T) {
	topo := NewTopology()
	a := topo.AddNode("a", "dc1", "dc1:rack1", 50)
	a.AddDisk(1, "", 50, 7)
	a.AddShards(100, "c", 1, bits(0, 1, 2, 3, 4, 5, 6))
	a.SetEncodeTs(100, "c", 1, 100)
	b := topo.AddNode("b", "dc1", "dc1:rack1", 50)
	b.AddDisk(1, "", 50, 7)
	b.AddShards(100, "c", 1, bits(0, 7, 8, 9, 10, 11, 12))
	b.SetEncodeTs(100, "c", 1, 200)
	addEmptyNode(topo, "e1", "dc1:rack2")
	addEmptyNode(topo, "e2", "dc1:rack3")
	if dedup, other := planVolume(topo, 100); len(dedup)+len(other) != 0 {
		t.Fatalf("a volume with unresolved run conflicts must not be touched; got dedup %+v, moves %+v", dedup, other)
	}
}

// A node holding the complete run on one disk and the stale run on another is
// counted per disk, so the complete run is still found.
func TestPlanDedupCountsRunsPerDisk(t *testing.T) {
	topo := NewTopology()
	a := topo.AddNode("a", "dc1", "dc1:rack1", 5)
	a.AddDisk(1, "", 5, 12)
	a.AddDisk(2, "", 5, 14)
	a.AddShards(100, "c", 1, bits(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12))
	a.SetEncodeTs(100, "c", 1, 100)
	a.AddShards(100, "c", 2, bits(0, 13))
	a.SetEncodeTs(100, "c", 2, 200)
	b := topo.AddNode("b", "dc1", "dc1:rack2", 10)
	b.AddDisk(1, "", 10, 12)
	b.AddShards(100, "c", 1, bits(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12))
	b.SetEncodeTs(100, "c", 1, 200)
	c := topo.AddNode("c", "dc1", "dc1:rack3", 50)
	c.AddDisk(1, "", 50, 1)
	c.AddShards(100, "c", 1, bits(0))
	c.SetEncodeTs(100, "c", 1, 100)
	dedup, _ := planVolume(topo, 100)
	for _, m := range dedup {
		if m.ShardID == 0 && m.SourceNode != "c" {
			t.Errorf("dedup move %+v: shard 0 must keep a's run-200 copy and drop c's run-100 copy", m)
		}
	}
	var dropsC bool
	for _, m := range dedup {
		dropsC = dropsC || (m.ShardID == 0 && m.SourceNode == "c")
	}
	if !dropsC {
		t.Errorf("want c's run-100 shard 0 removed, got %+v", dedup)
	}
}

// A frozen volume's shards stay out of every move but still count toward the
// node's load: B holds 4 movable plus 8 frozen shards against A's 10, so the
// global phase must not move A's shards onto B.
func TestPlanFrozenShardsCountTowardLoad(t *testing.T) {
	topo := NewTopology()
	a := topo.AddNode("a", "dc1", "dc1:rack1", 20)
	a.AddDisk(1, "", 20, 10)
	b := topo.AddNode("b", "dc1", "dc1:rack1", 18)
	b.AddDisk(1, "", 18, 12)
	for vid := uint32(1); vid <= 10; vid++ {
		a.AddShards(vid, "c", 1, bits(0))
	}
	for vid := uint32(11); vid <= 14; vid++ {
		b.AddShards(vid, "c", 1, bits(0))
	}
	b.AddShards(100, "c", 1, bits(0, 1, 2, 3, 4, 5, 6, 7))
	b.Freeze(100, "c")
	for _, m := range Plan(topo, Options{ImbalanceThreshold: 0.01, Ratio: ratio(10, 4), GlobalUtilizationBased: true}) {
		if m.VolumeID == 100 {
			t.Errorf("frozen volume moved: %+v", m)
		}
		if m.SourceNode == "a" && m.TargetNode == "b" {
			t.Errorf("moved %+v onto the node whose frozen shards make it the fuller one", m)
		}
	}
}
