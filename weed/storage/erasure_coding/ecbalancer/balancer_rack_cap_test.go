package ecbalancer

import (
	"fmt"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/storage/erasure_coding"
	"github.com/seaweedfs/seaweedfs/weed/storage/super_block"
)

// Total-shards-per-rack cap for Plan.
//
// The per-type caps (ceil(data/racks) data, ceil(parity/racks) parity) spread
// each type evenly on their own, but together they allow e.g. 2 data + 1 parity
// on one rack. When a rack is one physical disk, losing two such racks loses 6
// of a 10+4 volume's shards. Plan also caps the TOTAL per rack, sized like
// Place's rackTotalCap: ceil(shards/racks) unless the racks' capacity forces it
// higher.

const capTestVid = uint32(1)

var capTestVk = volKey{collection: "c1", vid: capTestVid}

// rackCapThresholds are the imbalance thresholds of the shell (0) and the
// worker's default (0.2); both reach Plan.
var rackCapThresholds = []float64{0, 0.2}

// addCapNode adds a node with one disk of `free` slots, before its shards.
func addCapNode(topo *Topology, id, rackKey string, free int) *Node {
	n := topo.AddNode(id, "dc1", rackKey, free)
	n.AddDisk(0, "", free, 0)
	return n
}

// putShards places the volume's shards on the node's disk 0 and takes their slots.
func putShards(n *Node, ids ...int) {
	n.AddShards(capTestVid, "c1", 0, bits(ids...))
	n.freeSlots -= len(ids)
	n.disks[0].freeSlots -= len(ids)
	n.disks[0].shardCount += len(ids)
}

func shardTotalsPerRack(topo *Topology) map[string]int {
	totals := map[string]int{}
	for _, n := range topo.nodes {
		totals[n.rack] += volumeShardCount(n, capTestVk)
	}
	return totals
}

// planAndCheckRackTotals runs Plan, then checks every shard is still placed,
// that no rack holds more than maxPerRack, and that a second Plan is a no-op.
func planAndCheckRackTotals(t *testing.T, topo *Topology, opts Options, maxPerRack int) {
	t.Helper()
	Plan(topo, opts)
	totals := shardTotalsPerRack(topo)
	t.Logf("total shards per rack: %v", totals)
	sum := 0
	for rk, n := range totals {
		sum += n
		if n > maxPerRack {
			t.Errorf("rack %s holds %d shards, want at most %d", rk, n, maxPerRack)
		}
	}
	if sum != erasure_coding.TotalShardsCount {
		t.Errorf("%d shards placed, want %d", sum, erasure_coding.TotalShardsCount)
	}
	if again := Plan(topo, opts); len(again) != 0 {
		t.Errorf("second Plan is not a no-op: %+v", again)
	}
}

// raisedCaps records Options.RackTotalCapRaised reports.
type raisedCaps []string

func (r *raisedCaps) record(collection string, vid uint32, rackCap, evenCap int) {
	*r = append(*r, fmt.Sprintf("%s/%d cap=%d even=%d", collection, vid, rackCap, evenCap))
}

func forEachThreshold(t *testing.T, run func(t *testing.T, threshold float64)) {
	for _, th := range rackCapThresholds {
		t.Run(fmt.Sprintf("threshold=%v", th), func(t *testing.T) { run(t, th) })
	}
}

// TestPlanTotalShardsPerRackCap: 10+4 encoded onto one of 8 racks spreads to at
// most ceil(14/8) = 2 shards per rack, not 2 data + 1 parity.
func TestPlanTotalShardsPerRackCap(t *testing.T) {
	forEachThreshold(t, func(t *testing.T, th float64) {
		topo := NewTopology()
		putShards(addCapNode(topo, "n1", "dc1:rack1", 100), 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13)
		for r := 2; r <= 8; r++ {
			addCapNode(topo, fmt.Sprintf("n%d", r), fmt.Sprintf("dc1:rack%d", r), 100)
		}
		planAndCheckRackTotals(t, topo, Options{ImbalanceThreshold: th, Ratio: ratio(10, 4)}, 2)
	})
}

// TestPlanTotalShardsPerRackCapTwoNodesPerRack: the cap is per rack, not per
// node, so two volume servers sharing a rack (one physical disk) hold at most 2
// between them.
func TestPlanTotalShardsPerRackCapTwoNodesPerRack(t *testing.T) {
	forEachThreshold(t, func(t *testing.T, th float64) {
		topo := NewTopology()
		for r := 1; r <= 8; r++ {
			for i := 0; i < 2; i++ {
				n := addCapNode(topo, fmt.Sprintf("n%d-%d", r, i), fmt.Sprintf("dc1:rack%d", r), 100)
				if r == 1 && i == 0 {
					putShards(n, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13)
				}
			}
		}
		planAndCheckRackTotals(t, topo, Options{ImbalanceThreshold: th, Ratio: ratio(10, 4)}, 2)
	})
}

// TestPlanTotalCapFromClumpedStart: data 2,2,2,2,2,0,0,0 with all parity on
// rack1. Racks 6-8 take one parity each, after which no rack is under the parity
// cap of 1; the last parity must still leave rack1 (2 data + 1 parity) for a
// data-bearing rack under the total cap rather than stay put.
func TestPlanTotalCapFromClumpedStart(t *testing.T) {
	forEachThreshold(t, func(t *testing.T, th float64) {
		topo := NewTopology()
		layout := [][]int{{0, 1, 10, 11, 12, 13}, {2, 3}, {4, 5}, {6, 7}, {8, 9}, nil, nil, nil}
		for r, ids := range layout {
			n := addCapNode(topo, fmt.Sprintf("n%d", r+1), fmt.Sprintf("dc1:rack%d", r+1), 100)
			if len(ids) > 0 {
				putShards(n, ids...)
			}
		}
		planAndCheckRackTotals(t, topo, Options{ImbalanceThreshold: th, Ratio: ratio(10, 4)}, 2)
	})
}

// TestPlanTotalCapAllRacksBearData: every rack holds data, so parity has to
// share a rack with data. Anti-affinity stays a preference and the plan still
// reaches at most 2 per rack.
func TestPlanTotalCapAllRacksBearData(t *testing.T) {
	forEachThreshold(t, func(t *testing.T, th float64) {
		topo := NewTopology()
		layout := [][]int{{0, 1, 10, 11, 12, 13}, {2, 3}, {4}, {5}, {6}, {7}, {8}, {9}}
		for r, ids := range layout {
			putShards(addCapNode(topo, fmt.Sprintf("n%d", r+1), fmt.Sprintf("dc1:rack%d", r+1), 100), ids...)
		}
		planAndCheckRackTotals(t, topo, Options{ImbalanceThreshold: th, Ratio: ratio(10, 4)}, 2)
	})
}

// TestPlanTotalCapParityOnDataRacks: each type is within its own cap (2 data,
// 1 parity per rack) yet four racks hold 3. The non-overflow parity shards that
// may only move to a data-free rack find just two of those; the over-cap ones
// that remain must go to a data-bearing rack under the total cap.
func TestPlanTotalCapParityOnDataRacks(t *testing.T) {
	forEachThreshold(t, func(t *testing.T, th float64) {
		topo := NewTopology()
		layout := [][]int{{0, 1, 10}, {2, 3, 11}, {4, 5, 12}, {6, 7, 13}, {8}, {9}, nil, nil}
		for r, ids := range layout {
			n := addCapNode(topo, fmt.Sprintf("n%d", r+1), fmt.Sprintf("dc1:rack%d", r+1), 100)
			if len(ids) > 0 {
				putShards(n, ids...)
			}
		}
		planAndCheckRackTotals(t, topo, Options{ImbalanceThreshold: th, Ratio: ratio(10, 4)}, 2)
	})
}

// TestPlanTotalCapIgnoresImbalanceThreshold: a rack above the total cap is a
// durability problem, so a threshold high enough to pass both per-type checks
// must not stop Plan from fixing it.
func TestPlanTotalCapIgnoresImbalanceThreshold(t *testing.T) {
	topo := NewTopology()
	layout := [][]int{{0, 1, 10}, {2, 3, 11}, {4, 5}, {6, 7}, {8}, {9}, {12}, {13}}
	for r, ids := range layout {
		putShards(addCapNode(topo, fmt.Sprintf("n%d", r+1), fmt.Sprintf("dc1:rack%d", r+1), 100), ids...)
	}
	planAndCheckRackTotals(t, topo, Options{ImbalanceThreshold: 100, Ratio: ratio(10, 4)}, 2)
}

// TestPlanTotalCapWithNearlyFullRack: 8 racks of two volume servers each, plus
// a small, nearly full SSD rack already holding one shard. The SSD rack's lack
// of room must not raise the cap above ceil(14/9) = 2 for the HDD racks. The
// start is the layout Place produced before it capped the total per rack.
func TestPlanTotalCapWithNearlyFullRack(t *testing.T) {
	forEachThreshold(t, func(t *testing.T, th float64) {
		topo := NewTopology()
		layout := [][]int{{0, 1, 10}, {2, 3, 11}, {4, 5}, {6, 7}, {8}, {9}, {12}, nil}
		for r, ids := range layout {
			for i := 0; i < 2; i++ {
				n := addCapNode(topo, fmt.Sprintf("n%d-%d", r, i), fmt.Sprintf("dc1:hdd%d", r), 100)
				if i == 0 && len(ids) > 0 {
					putShards(n, ids...)
				}
			}
		}
		putShards(addCapNode(topo, "nvme", "dc1:ssd", 2), 13)
		var raised raisedCaps
		planAndCheckRackTotals(t, topo, Options{ImbalanceThreshold: th, Ratio: ratio(10, 4), RackTotalCapRaised: raised.record}, 2)
		if len(raised) != 0 {
			t.Errorf("cap reported as raised: %v", raised)
		}
	})
}

// TestPlanTotalCapRisesWhenRacksAreFull: two of 8 racks are full, so the other
// six can hold at best 3 each. A cap fixed at ceil(14/8) = 2 would block every
// destination and leave all four parity shards on rack1; the capacity-sized cap
// of 3 lets them spread as far as the room allows.
func TestPlanTotalCapRisesWhenRacksAreFull(t *testing.T) {
	forEachThreshold(t, func(t *testing.T, th float64) {
		topo := NewTopology()
		layout := [][]int{{10, 11, 12, 13}, {0, 1}, {2, 3}, {4, 5}, {6, 7}, {8, 9}}
		for r, ids := range layout {
			putShards(addCapNode(topo, fmt.Sprintf("n%d", r+1), fmt.Sprintf("dc1:rack%d", r+1), 100), ids...)
		}
		addCapNode(topo, "full7", "dc1:rack7", 0)
		addCapNode(topo, "full8", "dc1:rack8", 0)
		var raised raisedCaps
		planAndCheckRackTotals(t, topo, Options{ImbalanceThreshold: th, Ratio: ratio(10, 4), RackTotalCapRaised: raised.record}, 3)
		// Reported on every Plan (two here) while the cap stays raised.
		want := "c1/1 cap=3 even=2"
		if len(raised) != 2 || raised[0] != want || raised[1] != want {
			t.Errorf("raised-cap reports = %v, want [%s %s]", raised, want, want)
		}
	})
}

// TestPlanRackTotalCap: Plan may move every shard, so shards already on a rack
// count as room there, not as held in place. All 14 on one rack still gives 2.
func TestPlanRackTotalCap(t *testing.T) {
	build := func(free []int, held map[int]int) (map[string]*rack, map[string]int) {
		topo := NewTopology()
		next := 0
		for r, f := range free {
			n := addCapNode(topo, fmt.Sprintf("n%d", r), fmt.Sprintf("dc1:rack%d", r), f)
			for i := 0; i < held[r]; i++ {
				n.AddShards(capTestVid, "c1", 0, bits(next))
				next++
			}
		}
		return buildRacks(topo.nodes), countShardsByRack(capTestVk, topo.nodes)
	}
	cases := []struct {
		name string
		free []int
		held map[int]int
		want int
	}{
		{name: "all on one rack", free: []int{0, 50, 50, 50, 50, 50, 50, 50}, held: map[int]int{0: 14}, want: 2},
		{name: "even over 5", free: []int{50, 50, 50, 50, 50}, held: map[int]int{0: 14}, want: 3},
		{name: "two full empty racks", free: []int{50, 50, 50, 50, 50, 50, 0, 0}, held: map[int]int{0: 14}, want: 3},
		{name: "small nearly full ninth rack", free: []int{50, 50, 50, 50, 50, 50, 50, 50, 1}, held: map[int]int{0: 13, 8: 1}, want: 2},
		{name: "only the holding rack has room", free: []int{0, 0}, held: map[int]int{0: 14}, want: 14},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			racks, held := build(tc.free, tc.held)
			if got := planRackTotalCap(capTestVk, racks, held, erasure_coding.TotalShardsCount, nil); got != tc.want {
				t.Errorf("planRackTotalCap = %d, want %d", got, tc.want)
			}
		})
	}
}

// TestPlanTotalCapDataRackWaitsForParitySlot: the data pass runs out of
// destinations, and the only slots left for rack5's surplus data open up when
// the parity pass moves parity off rack3 and rack4. One Plan must still bring
// every rack within the cap (3), using those freed slots, without moving any
// shard twice.
func TestPlanTotalCapDataRackWaitsForParitySlot(t *testing.T) {
	forEachThreshold(t, func(t *testing.T, th float64) {
		topo := NewTopology()
		layout := [][]int{{0, 1, 2}, {3, 4}, {10, 11}, {12}, {5, 6, 7, 8, 9, 13}}
		free := []int{0, 4, 1, 1, 4}
		for r, ids := range layout {
			n := addCapNode(topo, fmt.Sprintf("n%d", r+1), fmt.Sprintf("dc1:rack%d", r+1), free[r]+len(ids))
			putShards(n, ids...)
		}
		moves := Plan(topo, Options{ImbalanceThreshold: th, Ratio: ratio(10, 4)})
		seen := map[int]bool{}
		for _, m := range moves {
			if seen[m.ShardID] {
				t.Errorf("shard %d moved twice in one plan: %+v", m.ShardID, moves)
			}
			seen[m.ShardID] = true
		}
		totals := shardTotalsPerRack(topo)
		t.Logf("total shards per rack: %v", totals)
		for rk, n := range totals {
			if n > 3 {
				t.Errorf("rack %s holds %d shards, want at most 3", rk, n)
			}
		}
	})
}

// TestPlanTotalCapCountsSameRackCount: with SameRackCount=1 a node takes at
// most one shard of the volume, so racks of one node that already hold a shard
// have no room however many free slots they report. Rack A's seven shards can't
// spread, the cap is 7, and Plan reports it as raised.
func TestPlanTotalCapCountsSameRackCount(t *testing.T) {
	topo := NewTopology()
	for i := 0; i < 7; i++ {
		putShards(addCapNode(topo, fmt.Sprintf("a%d", i), "dc1:rackA", 100), i)
	}
	for i, rk := range []string{"B", "C", "D", "E", "F", "G", "H"} {
		putShards(addCapNode(topo, "n"+rk, "dc1:rack"+rk, 100), 7+i)
	}
	var raised raisedCaps
	rp := &super_block.ReplicaPlacement{SameRackCount: 1}
	Plan(topo, Options{ReplicaPlacement: rp, Ratio: ratio(10, 4), RackTotalCapRaised: raised.record})
	if want := "c1/1 cap=7 even=2"; len(raised) != 1 || raised[0] != want {
		t.Errorf("raised-cap reports = %v, want [%s]", raised, want)
	}
}
