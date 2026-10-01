package ecbalancer

import (
	"fmt"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/storage/erasure_coding"
)

// Total-shards-per-rack cap for Place / PlaceDurabilityFirst.
//
// The per-type caps (ceil(data/racks) data, ceil(parity/racks) parity) spread
// each type evenly on their own, but together they allow e.g. 2 data + 1 parity
// on one rack. Place also caps the TOTAL per rack at the lowest value the racks'
// free capacity allows (rackTotalCap), under both modes and with a nil
// ReplicaPlacement.

// placedTotalsPerRack checks every shard was placed and returns the number of
// shards per rack.
func placedTotalsPerRack(t *testing.T, res *PlaceResult) map[string]int {
	t.Helper()
	if len(res.Destinations) != erasure_coding.TotalShardsCount {
		t.Fatalf("placed %d shards, want %d", len(res.Destinations), erasure_coding.TotalShardsCount)
	}
	totals := map[string]int{}
	for _, d := range res.Destinations {
		totals[d.Rack]++
	}
	t.Logf("total shards per rack: %v", totals)
	return totals
}

func assertMaxPerRack(t *testing.T, totals map[string]int, maxAllowed int) {
	t.Helper()
	for rk, n := range totals {
		if n > maxAllowed {
			t.Errorf("rack %s holds %d shards, want at most %d", rk, n, maxAllowed)
		}
	}
}

func assertNotRelaxed(t *testing.T, res *PlaceResult, name string) {
	t.Helper()
	for _, r := range res.Relaxed {
		if r == name {
			t.Errorf("unexpected %q relaxation, got %v", name, res.Relaxed)
		}
	}
}

// setRackFreeSlots leaves the rack with `slots` free slots, all on the first
// node's disk 0 (buildPlaceTopo gives each node one disk). Node and disk free
// slots are kept consistent, since rack capacity is derived from them.
func setRackFreeSlots(topo *Topology, rackKey string, slots int) {
	first := true
	for _, id := range sortedNodeKeys(topo.nodes) {
		n := topo.nodes[id]
		if n.rack != rackKey {
			continue
		}
		free := 0
		if first {
			free, first = slots, false
		}
		for _, d := range n.disks {
			d.freeSlots = free
		}
		n.freeSlots = free
	}
}

// TestPlaceTotalShardsPerRackCap: 10+4 over 8 racks of two nodes each places at
// most ceil(14/8) = 2 shards per rack, in both modes. Before the cap this was
// 3,3,2,2,1,1,1,1: two racks with 3 each.
func TestPlaceTotalShardsPerRackCap(t *testing.T) {
	modes := []struct {
		name string
		mode PlacementMode
	}{{"strict", PlaceStrict}, {"durability-first", PlaceDurabilityFirst}}
	for _, m := range modes {
		t.Run(m.name, func(t *testing.T) {
			topo := buildPlaceTopo(8, 2, 50)
			res, err := topo.Place(1, "c1", allShards(), Constraints{}, m.mode)
			if err != nil {
				t.Fatalf("Place: %v", err)
			}
			assertMaxPerRack(t, placedTotalsPerRack(t, res), 2)
		})
	}
}

// TestPlaceTotalCapWithStarvedRack: a rack with a single free slot still takes
// one shard and the other seven racks keep to 2 each.
func TestPlaceTotalCapWithStarvedRack(t *testing.T) {
	topo := buildPlaceTopo(8, 2, 50)
	setRackFreeSlots(topo, "dc1:rack0", 1)

	res, err := topo.Place(1, "c1", allShards(), Constraints{}, PlaceDurabilityFirst)
	if err != nil {
		t.Fatalf("Place: %v", err)
	}
	totals := placedTotalsPerRack(t, res)
	assertMaxPerRack(t, totals, 2)
	assertNotRelaxed(t, res, "rack-total-cap")
}

// TestPlaceTotalCapRisesForNearlyFullRacks: when nearly full racks cannot take
// an even share, the cap rises so the roomy racks absorb it instead of failing.
// A plain ceil(14/eligibleRacks) cap failed both cases.
func TestPlaceTotalCapRisesForNearlyFullRacks(t *testing.T) {
	cases := []struct {
		name     string
		racks    int
		starved  map[string]int // rack -> free slots
		wantCap  int
		wantHeld map[string]int // exact shard count expected on starved racks
	}{
		{
			// ceil(14/3) = 5 would fit only 5+5+1 = 11.
			name:     "3 racks, one with 1 slot",
			racks:    3,
			starved:  map[string]int{"dc1:rack2": 1},
			wantCap:  7,
			wantHeld: map[string]int{"rack2": 1},
		},
		{
			// ceil(14/8) = 2 would fit only 2+7 = 9.
			name:  "8 racks, seven with 1 slot",
			racks: 8,
			starved: map[string]int{
				"dc1:rack1": 1, "dc1:rack2": 1, "dc1:rack3": 1, "dc1:rack4": 1,
				"dc1:rack5": 1, "dc1:rack6": 1, "dc1:rack7": 1,
			},
			wantCap: 7,
			wantHeld: map[string]int{
				"rack1": 1, "rack2": 1, "rack3": 1, "rack4": 1,
				"rack5": 1, "rack6": 1, "rack7": 1,
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			topo := buildPlaceTopo(tc.racks, 2, 50)
			for rk, slots := range tc.starved {
				setRackFreeSlots(topo, rk, slots)
			}
			res, err := topo.Place(1, "c1", allShards(), Constraints{}, PlaceDurabilityFirst)
			if err != nil {
				t.Fatalf("Place: %v", err)
			}
			totals := placedTotalsPerRack(t, res)
			assertMaxPerRack(t, totals, tc.wantCap)
			for rk, want := range tc.wantHeld {
				if totals[rk] != want {
					t.Errorf("rack %s holds %d shards, want %d", rk, totals[rk], want)
				}
			}
			assertNotRelaxed(t, res, "rack-total-cap")
		})
	}
}

// TestPlaceTotalCapKeepsPreferredTagTier: a preferred-tag tier whose racks have
// uneven room must still place the whole volume on the tagged disks rather than
// spill to untagged ones.
func TestPlaceTotalCapKeepsPreferredTagTier(t *testing.T) {
	topo := NewTopology()
	for r := 0; r < 3; r++ {
		for n := 0; n < 2; n++ {
			taggedFree := 50
			if r == 2 {
				taggedFree = 0
				if n == 0 {
					taggedFree = 2
				}
			}
			node := topo.AddNode(fmt.Sprintf("10.0.%d.%d:8080", r, n), "dc1", fmt.Sprintf("dc1:rack%d", r), taggedFree+50)
			node.AddDisk(0, "", taggedFree, 0)
			node.AddDiskTags(0, []string{"ec-pool"})
			node.AddDisk(1, "", 50, 0)
		}
	}

	res, err := topo.Place(1, "c1", allShards(), Constraints{PreferredTags: []string{"ec-pool"}}, PlaceDurabilityFirst)
	if err != nil {
		t.Fatalf("Place: %v", err)
	}
	if res.SpilledOutsidePreferredTags {
		t.Fatal("placement spilled outside the preferred tags although the tagged disks can hold every shard")
	}
	for sid, d := range res.Destinations {
		if d.DiskID != 0 {
			t.Errorf("shard %d landed on untagged disk %d of %s", sid, d.DiskID, d.Node)
		}
	}
	placedTotalsPerRack(t, res)
}

// TestPlaceSkipsRackWithFullDisks: once a rack's disks hit the per-disk cap
// (parityShards shards of the volume), Place moves on to a rack that still has
// room instead of failing the shard.
func TestPlaceSkipsRackWithFullDisks(t *testing.T) {
	topo := buildPlaceTopo(1, 3, 50) // rack0: 3 nodes
	small := topo.AddNode("10.0.1.0:8080", "dc1", "dc1:rack1", 50)
	small.AddDisk(0, "", 50, 0) // rack1: a single disk, so at most 4 shards

	res, err := topo.Place(1, "c1", allShards(), Constraints{}, PlaceDurabilityFirst)
	if err != nil {
		t.Fatalf("Place: %v", err)
	}
	totals := placedTotalsPerRack(t, res)
	if totals["rack1"] > erasure_coding.ParityShardsCount {
		t.Errorf("rack1 holds %d shards on one disk, want at most %d", totals["rack1"], erasure_coding.ParityShardsCount)
	}
}

func TestRackTotalCap(t *testing.T) {
	keys := func(n int) []string {
		out := make([]string, n)
		for i := range out {
			out[i] = fmt.Sprintf("r%d", i)
		}
		return out
	}
	uniform := func(n, room int) map[string]int {
		out := map[string]int{}
		for _, k := range keys(n) {
			out[k] = room
		}
		return out
	}
	cases := []struct {
		name  string
		racks int
		held  map[string]int
		room  map[string]int
		total int
		want  int
	}{
		{name: "even over 8", racks: 8, room: uniform(8, 50), total: 14, want: 2},
		{name: "even over 5", racks: 5, room: uniform(5, 50), total: 14, want: 3},
		{name: "one rack with 1 slot", racks: 3, room: map[string]int{"r0": 50, "r1": 50, "r2": 1}, total: 14, want: 7},
		{name: "one rack full", racks: 3, room: map[string]int{"r0": 50, "r1": 50}, total: 14, want: 7},
		// Repair: r0 already holds 5 survivors; the other racks share the rest.
		{name: "held above even share", racks: 3, held: map[string]int{"r0": 5}, room: uniform(3, 50), total: 14, want: 5},
		{name: "not enough room", racks: 2, room: uniform(2, 3), total: 14, want: 14},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := rackTotalCap(keys(tc.racks), tc.held, tc.room, tc.total); got != tc.want {
				t.Errorf("rackTotalCap = %d, want %d", got, tc.want)
			}
		})
	}
}
