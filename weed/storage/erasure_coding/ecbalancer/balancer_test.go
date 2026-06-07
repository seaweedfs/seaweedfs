package ecbalancer

import (
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/storage/erasure_coding"
	"github.com/seaweedfs/seaweedfs/weed/storage/super_block"
)

func bits(ids ...int) erasure_coding.ShardBits {
	var b erasure_coding.ShardBits
	for _, id := range ids {
		b = b.Set(erasure_coding.ShardId(id))
	}
	return b
}

// addEmptyNode adds an EC-empty destination node with six disks and capacity.
func addEmptyNode(t *Topology, id, rackKey string) {
	n := t.AddNode(id, "dc1", rackKey, 600)
	for d := uint32(1); d <= 6; d++ {
		n.AddDisk(d, "", 100, 0)
	}
}

func ratio(d, p int) func(string) (int, int) {
	return func(string) (int, int) { return d, p }
}

func TestPickBestDiskOnNode(t *testing.T) {
	const vid = uint32(100)
	const ds = erasure_coding.DataShardsCount
	vk := volKey{collection: "c", vid: vid}

	t.Run("skips disks with no free slots", func(t *testing.T) {
		topo := NewTopology()
		n := topo.AddNode("n1", "dc1", "dc1:r1", 100)
		n.AddDisk(1, "", 0, 0)
		n.AddDisk(2, "", 10, 0)
		if got := pickBestDiskOnNode(n, vk, "", 0, ds); got != 2 {
			t.Errorf("got disk %d, want 2", got)
		}
	})

	t.Run("spreads a volume's shards across disks", func(t *testing.T) {
		topo := NewTopology()
		n := topo.AddNode("n1", "dc1", "dc1:r1", 100)
		n.AddDisk(1, "", 10, 1)
		n.AddDisk(2, "", 10, 0)
		n.AddShards(vid, "c", 1, bits(0))
		if got := pickBestDiskOnNode(n, vk, "", 5, ds); got != 2 {
			t.Errorf("got disk %d, want 2 (disk 1 already holds this volume)", got)
		}
	})

	t.Run("data shard avoids disk holding parity", func(t *testing.T) {
		topo := NewTopology()
		n := topo.AddNode("n1", "dc1", "dc1:r1", 100)
		n.AddDisk(1, "", 10, 1)
		n.AddDisk(2, "", 10, 0)
		n.AddShards(vid, "c", 1, bits(ds)) // parity on disk 1
		if got := pickBestDiskOnNode(n, vk, "", 0, ds); got != 2 {
			t.Errorf("got disk %d, want 2 (anti-affinity)", got)
		}
	})

	t.Run("anti-affinity follows the supplied ratio boundary", func(t *testing.T) {
		topo := NewTopology()
		n := topo.AddNode("n1", "dc1", "dc1:r1", 100)
		n.AddDisk(1, "", 10, 1)
		n.AddDisk(2, "", 10, 2)
		n.AddShards(vid, "c", 1, bits(7)) // parity at 6+3
		n.AddShards(vid, "c", 2, bits(2)) // data
		if got := pickBestDiskOnNode(n, vk, "", 1, 6); got != 2 {
			t.Errorf("ratio 6: got disk %d, want 2", got)
		}
		if got := pickBestDiskOnNode(n, vk, "", 1, erasure_coding.DataShardsCount); got != 1 {
			t.Errorf("boundary 10: got disk %d, want 1", got)
		}
	})

	t.Run("only matching disk type when set", func(t *testing.T) {
		topo := NewTopology()
		n := topo.AddNode("n1", "dc1", "dc1:r1", 100)
		n.AddDisk(1, "ssd", 10, 0)
		n.AddDisk(2, "hdd", 10, 0)
		if got := pickBestDiskOnNode(n, vk, "hdd", 0, ds); got != 2 {
			t.Errorf("got disk %d, want 2 (only hdd)", got)
		}
	})
}

func TestPlanSourceDiskAttribution(t *testing.T) {
	shardsByDisk := map[uint32][]int{0: {0, 1, 2}, 1: {3, 4, 5}, 2: {6, 7}, 3: {8, 9}, 4: {10, 11}, 5: {12, 13}}
	shardToDisk := map[int]uint32{}
	for d, ss := range shardsByDisk {
		for _, s := range ss {
			shardToDisk[s] = d
		}
	}
	topo := NewTopology()
	src := topo.AddNode("node1", "dc1", "dc1:rack1", 0)
	for d, ss := range shardsByDisk {
		src.AddDisk(d, "", 0, len(ss))
		src.AddShards(100, "col1", d, bits(ss...))
	}
	addEmptyNode(topo, "node2", "dc1:rack2")

	moves := Plan(topo, Options{ImbalanceThreshold: 0.01, Ratio: ratio(10, 4)})
	if len(moves) == 0 {
		t.Fatal("expected moves")
	}
	for _, m := range moves {
		if m.Phase != "cross_rack" {
			continue
		}
		if want := shardToDisk[m.ShardID]; m.SourceDisk != want {
			t.Errorf("shard %d: source disk %d, want %d", m.ShardID, m.SourceDisk, want)
		}
	}
}

func TestPlanSpreadsAcrossDestinationDisks(t *testing.T) {
	topo := NewTopology()
	src := topo.AddNode("node1", "dc1", "dc1:rack1", 0)
	src.AddDisk(0, "", 0, 14)
	src.AddShards(100, "col1", 0, bits(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13))
	addEmptyNode(topo, "node2", "dc1:rack2")

	moves := Plan(topo, Options{ImbalanceThreshold: 0.01, Ratio: ratio(10, 4)})
	distinct := map[uint32]bool{}
	crossRack := 0
	for _, m := range moves {
		if m.Phase == "cross_rack" {
			crossRack++
			distinct[m.TargetDisk] = true
		}
	}
	if crossRack != 7 {
		t.Fatalf("got %d cross-rack moves, want 7", crossRack)
	}
	if len(distinct) != 6 {
		t.Errorf("cross-rack moves used %d distinct disks, want 6: %v", len(distinct), distinct)
	}
}

func TestPlanCrossRackParityAntiAffinity(t *testing.T) {
	topo := NewTopology()
	src := topo.AddNode("node1", "dc1", "dc1:rack1", 0)
	src.AddDisk(0, "", 0, 3)
	src.AddShards(100, "col1", 0, bits(0, 1, 2)) // 1 data + 2 parity at ratio 1+2
	addEmptyNode(topo, "node2", "dc1:rack2")
	addEmptyNode(topo, "node3", "dc1:rack3")

	moves := Plan(topo, Options{ImbalanceThreshold: 0.01, Ratio: ratio(1, 2)})
	if len(moves) == 0 {
		t.Fatal("expected parity moves across racks")
	}
	for _, m := range moves {
		if m.ShardID < 1 {
			t.Errorf("data shard %d moved; it fits in rack1", m.ShardID)
		}
		if m.TargetNode == "node1" {
			t.Errorf("parity shard %d moved onto data-bearing node1", m.ShardID)
		}
	}
}

func TestWithinRackParityAntiAffinity(t *testing.T) {
	// Test the within-rack phase in isolation (the global phase, which balances
	// total load, would otherwise also act on this single rack).
	topo := NewTopology()
	src := topo.AddNode("node1", "dc1", "dc1:rack1", 600)
	src.AddDisk(0, "", 600, 3)
	src.AddShards(100, "col1", 0, bits(0, 1, 2))
	addEmptyNode(topo, "node2", "dc1:rack1")
	addEmptyNode(topo, "node3", "dc1:rack1")

	racks := buildRacks(topo.nodes)
	moves := detectWithinRackImbalance(volKey{collection: "col1", vid: 100}, topo.nodes, racks, "", 1, 2, nil)
	if len(moves) == 0 {
		t.Fatal("expected parity moves within rack")
	}
	for _, m := range moves {
		if m.shardID < 1 {
			t.Errorf("data shard %d moved; it fits on node1", m.shardID)
		}
		if m.target.id == "node1" {
			t.Errorf("parity shard %d moved onto data-bearing node1", m.shardID)
		}
	}
}

func TestPlanReplicaPlacementCapsPerRack(t *testing.T) {
	build := func() *Topology {
		topo := NewTopology()
		src := topo.AddNode("node1", "dc1", "dc1:rack1", 0)
		src.AddDisk(0, "", 0, 6)
		src.AddShards(100, "col1", 0, bits(0, 1, 2, 3, 4, 5)) // all data at ratio 6+0
		addEmptyNode(topo, "node2", "dc1:rack2")
		addEmptyNode(topo, "node3", "dc1:rack3")
		return topo
	}

	countCross := func(moves []Move) int {
		n := 0
		for _, m := range moves {
			if m.Phase == "cross_rack" {
				n++
			}
		}
		return n
	}

	if got := countCross(Plan(build(), Options{ImbalanceThreshold: 0.01, Ratio: ratio(6, 0)})); got != 4 {
		t.Fatalf("without replica placement: %d cross-rack moves, want 4", got)
	}
	rp := &super_block.ReplicaPlacement{DiffRackCount: 1}
	if got := countCross(Plan(build(), Options{ImbalanceThreshold: 0.01, ReplicaPlacement: rp, Ratio: ratio(6, 0)})); got != 2 {
		t.Errorf("with DiffRackCount=1: %d cross-rack moves, want 2", got)
	}
}

func TestPlanDedup(t *testing.T) {
	topo := NewTopology()
	n1 := topo.AddNode("node1", "dc1", "dc1:rack1", 5)
	n1.AddDisk(0, "", 5, 2)
	n1.AddShards(100, "col1", 0, bits(0, 1))
	n2 := topo.AddNode("node2", "dc1", "dc1:rack2", 10)
	n2.AddDisk(0, "", 10, 1)
	n2.AddShards(100, "col1", 0, bits(0)) // shard 0 duplicated on node2

	var dedup []Move
	for _, m := range Plan(topo, Options{ImbalanceThreshold: 0.01, Ratio: ratio(10, 4)}) {
		if m.Phase == "dedup" {
			dedup = append(dedup, m)
		}
	}
	if len(dedup) != 1 {
		t.Fatalf("got %d dedup moves, want 1", len(dedup))
	}
	if dedup[0].ShardID != 0 || dedup[0].SourceNode != "node1" || dedup[0].TargetNode != "node1" {
		t.Errorf("dedup move = %+v, want shard 0 deleted on node1 (fewer free slots)", dedup[0])
	}
}

func TestCeilDivide(t *testing.T) {
	for _, tc := range []struct{ a, b, want int }{{14, 3, 5}, {10, 3, 4}, {0, 5, 0}, {5, 0, 0}} {
		if got := ceilDivide(tc.a, tc.b); got != tc.want {
			t.Errorf("ceilDivide(%d,%d)=%d want %d", tc.a, tc.b, got, tc.want)
		}
	}
}

func allBits(n int) erasure_coding.ShardBits {
	var b erasure_coding.ShardBits
	for i := 0; i < n; i++ {
		b = b.Set(erasure_coding.ShardId(i))
	}
	return b
}

func TestGlobalImbalanceMovesFromFullToEmpty(t *testing.T) {
	topo := NewTopology()
	n1 := topo.AddNode("node1", "dc1", "dc1:rack1", 5)
	n1.AddShards(100, "col1", 0, allBits(14))
	n1.AddShards(200, "col1", 0, allBits(6))
	n2 := topo.AddNode("node2", "dc1", "dc1:rack1", 30)
	n2.AddShards(300, "col1", 0, allBits(2))

	moves := detectGlobalImbalance(topo.nodes, buildRacks(topo.nodes), "", 0.01, nil, nil, 0, true)
	if len(moves) == 0 {
		t.Fatal("expected global balance moves")
	}
	for _, m := range moves {
		if m.phase != "global" || m.source.id != "node1" || m.target.id != "node2" {
			t.Errorf("move = %+v, want global node1->node2", m)
		}
	}
}

// TestGlobalImbalanceHeterogeneousCapacity: node1 holds more shards but is less
// utilized (high capacity); moves must drain the more-utilized node2.
func TestGlobalImbalanceHeterogeneousCapacity(t *testing.T) {
	topo := NewTopology()
	n1 := topo.AddNode("node1", "dc1", "dc1:rack1", 90)
	n1.AddShards(100, "col1", 0, allBits(10))
	n2 := topo.AddNode("node2", "dc1", "dc1:rack1", 2)
	n2.AddShards(200, "col1", 0, allBits(3))

	moves := detectGlobalImbalance(topo.nodes, buildRacks(topo.nodes), "", 0.01, nil, nil, 0, true)
	if len(moves) == 0 {
		t.Fatal("expected moves from high-util node2 to low-util node1")
	}
	seen := map[[2]int]bool{}
	for _, m := range moves {
		if m.source.id != "node2" || m.target.id != "node1" {
			t.Errorf("move = %+v, want node2->node1", m)
		}
		key := [2]int{int(m.volumeID), m.shardID}
		if seen[key] {
			t.Errorf("duplicate move for volume %d shard %d", m.volumeID, m.shardID)
		}
		seen[key] = true
	}
}

func TestGlobalImbalanceSkipsFullNodes(t *testing.T) {
	topo := NewTopology()
	n1 := topo.AddNode("node1", "dc1", "dc1:rack1", 10)
	n1.AddShards(100, "col1", 0, allBits(14))
	n2 := topo.AddNode("node2", "dc1", "dc1:rack1", 0) // full, cannot receive
	n2.AddShards(200, "col1", 0, allBits(2))

	if moves := detectGlobalImbalance(topo.nodes, buildRacks(topo.nodes), "", 0.01, nil, nil, 0, true); len(moves) != 0 {
		t.Fatalf("expected 0 moves (node2 full), got %d", len(moves))
	}
}

// TestPlanBalancesSkewedDataParityWithEvenTotals guards the per-type gate: two
// racks hold equal shard totals (7 each) but the data shards are skewed (7 vs 3).
// A total-count gate would skip balancing; the per-type gate must still act.
func TestPlanBalancesSkewedDataParityWithEvenTotals(t *testing.T) {
	topo := NewTopology()
	n1 := topo.AddNode("node1", "dc1", "dc1:rack1", 100)
	n1.AddDisk(0, "", 100, 7)
	n1.AddShards(100, "col1", 0, bits(0, 1, 2, 3, 4, 5, 6)) // 7 data shards
	n2 := topo.AddNode("node2", "dc1", "dc1:rack2", 100)
	n2.AddDisk(0, "", 100, 7)
	n2.AddShards(100, "col1", 0, bits(7, 8, 9, 10, 11, 12, 13)) // 3 data + 4 parity

	moves := Plan(topo, Options{ImbalanceThreshold: 0, Ratio: ratio(10, 4)})

	crossRack, dataMoved := 0, 0
	for _, m := range moves {
		if m.Phase == "cross_rack" {
			crossRack++
			if m.ShardID < 10 {
				dataMoved++
			}
		}
	}
	if crossRack == 0 {
		t.Fatal("even totals masked a data/parity skew: no cross-rack moves produced")
	}
	if dataMoved == 0 {
		t.Error("expected skewed data shards to rebalance across racks")
	}
}

// TestGlobalPrefersVolumeAbsentFromDestination guards the global phase's
// volume-diversity preference: when draining a node, move a shard of a volume the
// destination does not hold at all before piling a second shard of an
// already-present volume onto it. node1 (full) holds vol100 and vol200; node2
// (empty) holds only vol100, so the first global move should be a vol200 shard.
func TestGlobalPrefersVolumeAbsentFromDestination(t *testing.T) {
	topo := NewTopology()
	n1 := topo.AddNode("node1", "dc1", "dc1:rack1", 0)
	n1.AddShards(100, "col1", 0, bits(0, 1))
	n1.AddShards(200, "col1", 0, bits(0, 1))
	n2 := topo.AddNode("node2", "dc1", "dc1:rack1", 3)
	n2.AddShards(100, "col1", 0, bits(2))

	moves := detectGlobalImbalance(topo.nodes, buildRacks(topo.nodes), "", 0.01, nil, nil, 0, true)
	if len(moves) == 0 {
		t.Fatal("expected a global move from the full node")
	}
	if moves[0].volumeID != 200 {
		t.Errorf("first global move is volume %d, want 200 (the volume absent from node2)", moves[0].volumeID)
	}
	for _, m := range moves {
		if m.source.id != "node1" || m.target.id != "node2" {
			t.Errorf("move %+v, want node1->node2", m)
		}
	}
}

// TestPlanKeepsCollectionsWithSameVolumeIdDistinct guards EC identity: a numeric
// volume id reused across collections must not be merged. (A,5) on node1 and
// (B,5) on node2 are different volumes, so neither dedup nor any move should
// treat them as copies of one another.
func TestPlanKeepsCollectionsWithSameVolumeIdDistinct(t *testing.T) {
	topo := NewTopology()
	n1 := topo.AddNode("node1", "dc1", "dc1:rack1", 100)
	n1.AddDisk(0, "", 100, 1)
	n1.AddShards(5, "A", 0, bits(0))
	n2 := topo.AddNode("node2", "dc1", "dc1:rack2", 100)
	n2.AddDisk(0, "", 100, 1)
	n2.AddShards(5, "B", 0, bits(0))

	for _, m := range Plan(topo, Options{ImbalanceThreshold: 0.01, Ratio: ratio(10, 4)}) {
		if m.Phase == "dedup" {
			t.Errorf("dedup %+v: (A,5) and (B,5) are different volumes and must not be deduped", m)
		}
	}
}

// TestDedupFreesCapacityForLaterPhases checks that capacity opened by deleting a
// duplicate is usable by a later phase in the same Plan. node2 is full (0 free)
// but holds a duplicate of node1's shard 0; node1 is roomier, so dedup deletes
// node2's copy, freeing a slot. The within-rack phase must then be able to move a
// shard onto node2.
func TestDedupFreesCapacityForLaterPhases(t *testing.T) {
	topo := NewTopology()
	n1 := topo.AddNode("node1", "dc1", "dc1:rack1", 5)
	n1.AddDisk(0, "", 5, 7)
	n1.AddShards(100, "col1", 0, bits(0, 1, 2, 3, 4, 5, 6))
	n2 := topo.AddNode("node2", "dc1", "dc1:rack1", 0) // full
	n2.AddDisk(0, "", 0, 1)
	n2.AddShards(100, "col1", 0, bits(0)) // duplicate of node1's shard 0

	moves := Plan(topo, Options{ImbalanceThreshold: 0.01, Ratio: ratio(10, 4)})

	dedup := false
	toNode2 := 0
	for _, m := range moves {
		if m.Phase == "dedup" {
			dedup = true
			continue
		}
		if m.TargetNode == "node2" {
			toNode2++
		}
	}
	if !dedup {
		t.Fatal("expected a dedup move for the duplicated shard 0")
	}
	if toNode2 == 0 {
		t.Error("slot freed by dedup on node2 was not usable by a later phase")
	}
}

// TestWithinRackSpreadsAcrossMachines: a 10+4 volume's 14 shards all sit on boxA
// (two of its servers), with three other machines free. Four machines is enough for
// the spread to stay within parity, so the within-rack phase must move shards out
// until no machine holds more than ceil(14/4)=4, even though they looked spread
// across boxA's two nodes.
func TestWithinRackSpreadsAcrossMachines(t *testing.T) {
	topo := NewTopology()
	mk := func(id, host string) *Node {
		n := topo.AddNode(id, "dc1", "dc1:rack1", 100)
		n.SetHost(host)
		n.AddDisk(0, "", 100, 0)
		return n
	}
	a1 := mk("a1", "boxA")
	a2 := mk("a2", "boxA")
	mk("b1", "boxB")
	mk("c1", "boxC")
	mk("d1", "boxD")
	a1.AddShards(100, "col1", 0, bits(0, 1, 2, 3, 4, 5, 6))     // 7 shards on boxA
	a2.AddShards(100, "col1", 0, bits(7, 8, 9, 10, 11, 12, 13)) // 7 shards on boxA (all 14)

	detectWithinRackImbalance(volKey{"col1", 100}, topo.nodes, buildRacks(topo.nodes), "", 10, 4, nil)

	perHost := map[string]int{}
	for _, n := range topo.nodes {
		if info, ok := n.shards[volKey{"col1", 100}]; ok {
			perHost[n.host] += info.shardBits.Count()
		}
	}
	if len(perHost) < 3 {
		t.Fatalf("shards not spread across machines: %v", perHost)
	}
	for h, c := range perHost {
		if c > 4 {
			t.Errorf("machine %s holds %d shards, want <=4 (ceil(14/4), within parity)", h, c)
		}
	}
}

// TestWithinRackMachineSpreadBalancesCombinedOccupancy: a 5+5 volume on two machines
// where each shard type is already within its own per-type cap (boxA 3 data + 3
// parity = 6, boxB 2 data + 2 parity = 4). Spreading data and parity independently
// would leave boxA at 6, past parity; balancing the combined count must move one
// shard to reach 5/5 so a machine loss stays recoverable.
func TestWithinRackMachineSpreadBalancesCombinedOccupancy(t *testing.T) {
	topo := NewTopology()
	mk := func(id, host string) *Node {
		n := topo.AddNode(id, "dc1", "dc1:rack1", 100)
		n.SetHost(host)
		n.AddDisk(0, "", 100, 0)
		return n
	}
	a1 := mk("a1", "boxA")
	a2 := mk("a2", "boxA") // two nodes on boxA -> numMachines(2) < numNodes(3)
	b1 := mk("b1", "boxB")
	a1.AddShards(100, "col1", 0, bits(0, 1, 2))    // 3 data
	a2.AddShards(100, "col1", 0, bits(5, 6, 7))    // 3 parity (ids >= 5)
	b1.AddShards(100, "col1", 0, bits(3, 4, 8, 9)) // 2 data + 2 parity

	// dataShards=5, parityShards=5: feasibility ceil(10/2)=5 <= 5, so machine spread runs.
	detectWithinRackImbalance(volKey{"col1", 100}, topo.nodes, buildRacks(topo.nodes), "", 5, 5, nil)

	perHost := map[string]int{}
	for _, n := range topo.nodes {
		if info, ok := n.shards[volKey{"col1", 100}]; ok {
			perHost[n.host] += info.shardBits.Count()
		}
	}
	if perHost["boxA"] > 5 {
		t.Errorf("boxA holds %d shards, want <=5 (parity); data and parity spread independently", perHost["boxA"])
	}
}

// TestWithinRackMachineSpreadActsOnExactlyHalfSkew: a 5/4/3 machine layout for a 10+4
// volume is only 50% skewed ((5-3)/4 = 0.5), which a 0.5 relative-imbalance threshold
// would skip -- but the 5-shard machine is already past parity. The spread must act
// regardless (the even cap, not a skew threshold, is the bound) and bring every
// machine to <=ceil(12/3)=4.
func TestWithinRackMachineSpreadActsOnExactlyHalfSkew(t *testing.T) {
	topo := NewTopology()
	mk := func(id, host string) *Node {
		n := topo.AddNode(id, "dc1", "dc1:rack1", 100)
		n.SetHost(host)
		n.AddDisk(0, "", 100, 0)
		return n
	}
	a1 := mk("a1", "boxA")
	a2 := mk("a2", "boxA")
	b1 := mk("b1", "boxB")
	b2 := mk("b2", "boxB")
	c1 := mk("c1", "boxC")
	c2 := mk("c2", "boxC")
	a1.AddShards(100, "col1", 0, bits(0, 1, 2))
	a2.AddShards(100, "col1", 0, bits(3, 4)) // boxA = 5
	b1.AddShards(100, "col1", 0, bits(5, 6))
	b2.AddShards(100, "col1", 0, bits(7, 8)) // boxB = 4
	c1.AddShards(100, "col1", 0, bits(9, 10))
	c2.AddShards(100, "col1", 0, bits(11)) // boxC = 3

	detectWithinRackImbalance(volKey{"col1", 100}, topo.nodes, buildRacks(topo.nodes), "", 10, 4, nil)

	perHost := map[string]int{}
	for _, n := range topo.nodes {
		if info, ok := n.shards[volKey{"col1", 100}]; ok {
			perHost[n.host] += info.shardBits.Count()
		}
	}
	for h, c := range perHost {
		if c > 4 {
			t.Errorf("machine %s holds %d shards, want <=4 (ceil(12/3)); a 50%% skew was skipped", h, c)
		}
	}
}

// TestWithinRackSpreadUsesRackLocalShardCount: a rack holds only 7 shards of a 10+4
// volume (cross-rack spreading moved the rest to other racks). Two machines can hold
// those 7 within parity (ceil(7/2)=4), so machine spread must apply -- gating on the
// full 14-shard total would wrongly fall back to node spread and pile 6 onto boxA's
// two nodes, exceeding parity.
func TestWithinRackSpreadUsesRackLocalShardCount(t *testing.T) {
	topo := NewTopology()
	mk := func(id, host string) *Node {
		n := topo.AddNode(id, "dc1", "dc1:rack1", 100)
		n.SetHost(host)
		n.AddDisk(0, "", 100, 0)
		return n
	}
	a1 := mk("a1", "boxA")
	a2 := mk("a2", "boxA")
	mk("b1", "boxB")
	a1.AddShards(100, "col1", 0, bits(0, 1, 2, 3))
	a2.AddShards(100, "col1", 0, bits(4, 5, 6)) // boxA holds all 7 of this rack's shards

	detectWithinRackImbalance(volKey{"col1", 100}, topo.nodes, buildRacks(topo.nodes), "", 10, 4, nil)

	perHost := map[string]int{}
	for _, n := range topo.nodes {
		if info, ok := n.shards[volKey{"col1", 100}]; ok {
			perHost[n.host] += info.shardBits.Count()
		}
	}
	if perHost["boxA"] > 4 {
		t.Errorf("boxA holds %d shards, want <=4 (parity); rack-local feasibility not used", perHost["boxA"])
	}
	if perHost["boxB"] == 0 {
		t.Error("no shards moved to boxB; machine spread did not apply")
	}
}

// TestWithinRackSpreadDefaultsToNodes: with no SetHost each node is its own
// machine, so the within-rack phase still spreads a volume off an overloaded node
// exactly as before (machine grouping reduces to node grouping).
func TestWithinRackSpreadDefaultsToNodes(t *testing.T) {
	topo := NewTopology()
	mk := func(id string) *Node {
		n := topo.AddNode(id, "dc1", "dc1:rack1", 100)
		n.AddDisk(0, "", 100, 0)
		return n
	}
	n1 := mk("n1")
	mk("n2")
	mk("n3")
	n1.AddShards(100, "col1", 0, allBits(14)) // all 14 piled on one node

	detectWithinRackImbalance(volKey{"col1", 100}, topo.nodes, buildRacks(topo.nodes), "", 10, 4, nil)

	perNode := map[string]int{}
	for id, n := range topo.nodes {
		if info, ok := n.shards[volKey{"col1", 100}]; ok {
			perNode[id] = info.shardBits.Count()
		}
	}
	if perNode["n1"] == 14 {
		t.Fatal("no within-rack spread happened with one server per host")
	}
	if len(perNode) < 3 {
		t.Errorf("shards not spread across all three nodes: %v", perNode)
	}
	// Per-type even caps: ceil(10/3) data + ceil(4/3) parity = 4+2 per node.
	for id, c := range perNode {
		if c > 6 {
			t.Errorf("node %s holds %d shards, want <=6 (dataCap+parityCap)", id, c)
		}
	}
}

// TestGlobalDoesNotConcentrateVolumeAcrossMachines: when the volume's machine spread
// is achievable (here 2 machines, a 2+2 volume, one machine can hold <= parity), a
// load move must not raise a machine's shard count of the volume past the source's.
// boxA's only volume is also on boxB, so pass 0 finds nothing and the sole pass-1
// option is the cross-machine boxA->boxB move, which must be rejected (boxB already
// holds as many shards as boxA). Same-machine node rebalancing would still be fine.
func TestGlobalDoesNotConcentrateVolumeAcrossMachines(t *testing.T) {
	topo := NewTopology()
	a1 := topo.AddNode("a1", "dc1", "dc1:rack1", 0) // full -> high util, the max node
	a1.SetHost("boxA")
	a1.AddShards(100, "col1", 0, bits(0, 1))
	b1 := topo.AddNode("b1", "dc1", "dc1:rack1", 0) // full -> cannot receive
	b1.SetHost("boxB")
	b1.AddShards(100, "col1", 0, bits(2, 3))
	b2 := topo.AddNode("b2", "dc1", "dc1:rack1", 10) // empty -> low util, the min node
	b2.SetHost("boxB")

	data := map[string]int{"col1": 2}
	parity := map[string]int{"col1": 2}
	for _, m := range detectGlobalImbalance(topo.nodes, buildRacks(topo.nodes), "", 0.01, data, parity, 0, true) {
		if m.source.host != m.target.host {
			t.Errorf("cross-machine global move %d.%d from %s to %s concentrates the volume on a machine",
				m.volumeID, m.shardID, m.source.host, m.target.host)
		}
	}
}

// TestWithinRackMachineSkipsCappedMachine: a machine whose only node is already at
// the per-node SameRackCount cap is not a viable target, and the within-rack spread
// must move shards to the next machine that is, rather than picking the capped one
// and skipping the move. Four machines (boxA has two nodes) make the spread active;
// boxB's node holds two parity shards (== SameRackCount), so boxA's over-concentrated
// data shards must land on the viable boxC/boxD.
func TestWithinRackMachineSkipsCappedMachine(t *testing.T) {
	rp, _ := super_block.NewReplicaPlacementFromString("002") // SameRackCount=2
	topo := NewTopology()
	mk := func(id, host string) *Node {
		n := topo.AddNode(id, "dc1", "dc1:rack1", 100)
		n.SetHost(host)
		n.AddDisk(0, "", 100, 0)
		return n
	}
	a1 := mk("a1", "boxA")
	mk("a2", "boxA") // boxA's second node makes numMachines(4) < numNodes(5)
	b1 := mk("b1", "boxB")
	c1 := mk("c1", "boxC")
	d1 := mk("d1", "boxD")
	a1.AddShards(100, "col1", 0, bits(0, 1, 2, 3)) // 4 data shards, over-concentrated on boxA
	b1.AddShards(100, "col1", 0, bits(10, 11))     // 2 parity -> b1 at SameRackCount cap

	detectWithinRackImbalance(volKey{"col1", 100}, topo.nodes, buildRacks(topo.nodes), "", 10, 4, rp)

	moved := 0
	for _, n := range []*Node{c1, d1} {
		if info, ok := n.shards[volKey{"col1", 100}]; ok {
			moved += info.shardBits.Count()
		}
	}
	if moved == 0 {
		t.Error("data shards were not moved to the viable machines boxC/boxD; the capped boxB blocked the move")
	}
}

// TestGlobalPrefersVolumeAbsentFromDestinationMachine: the global load phase must
// judge "volume already present" at the machine level. boxB's sibling node holds
// vol100, so draining boxA onto boxB's empty node should move a vol200 shard first
// (vol200 is absent from boxB) rather than piling a second vol100 copy onto boxB.
func TestGlobalPrefersVolumeAbsentFromDestinationMachine(t *testing.T) {
	topo := NewTopology()
	n1 := topo.AddNode("n1", "dc1", "dc1:rack1", 0)
	n1.SetHost("boxA")
	n1.AddShards(100, "col1", 0, bits(0, 1, 2, 3))
	n1.AddShards(200, "col1", 0, bits(0, 1, 2, 3))
	n2 := topo.AddNode("n2", "dc1", "dc1:rack1", 5)
	n2.SetHost("boxB")
	n2.AddShards(100, "col1", 0, bits(4)) // vol100 already on boxB (sibling node)
	n3 := topo.AddNode("n3", "dc1", "dc1:rack1", 5)
	n3.SetHost("boxB") // empty destination node, but its machine holds vol100

	moves := detectGlobalImbalance(topo.nodes, buildRacks(topo.nodes), "", 0.01, nil, nil, 0, true)
	if len(moves) == 0 {
		t.Fatal("expected a global move from the full node")
	}
	if moves[0].volumeID != 200 {
		t.Errorf("first global move is volume %d, want 200 (vol100 already on the destination machine)", moves[0].volumeID)
	}
}
