package ecbalancer

import (
	"fmt"
	"math"
	"sort"
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

// TestPlaceDistributesEvenlyAcrossManyVolumes: placing many volumes against the shared
// encode snapshot must not pile shards onto the sorted-first machine.
func TestPlaceDistributesEvenlyAcrossManyVolumes(t *testing.T) {
	topo := NewTopology()
	hosts := []string{"10.0.0.1", "10.0.0.2", "10.0.0.3", "10.0.0.4", "10.0.0.5", "10.0.0.6"}
	for _, h := range hosts {
		for _, port := range []string{":8080", ":8081"} {
			n := topo.AddNode(h+port, "dc1", "dc1:rack1", 1000)
			n.SetHost(h)
			n.AddDisk(0, "", 1000, 0)
		}
	}

	const volumes = 120
	for v := 0; v < volumes; v++ {
		if _, err := topo.Place(uint32(v), "c1", allShards(), Constraints{}, PlaceDurabilityFirst); err != nil {
			t.Fatalf("Place volume %d: %v", v, err)
		}
	}

	perMachine := map[string]int{}
	for _, n := range topo.nodes {
		for _, d := range n.disks {
			perMachine[n.host] += d.shardCount
		}
	}
	min, max := 1<<30, 0
	for _, c := range perMachine {
		if c < min {
			min = c
		}
		if c > max {
			max = c
		}
	}
	// Even distribution is 120*14/6 = 280 shards/machine. Catch a lopsided pileup
	// (the production symptom was the first machine at ~1.7x the rest).
	if float64(max) > 1.25*float64(min) {
		t.Errorf("EC shards piled unevenly across machines: min=%d max=%d %v", min, max, perMachine)
	}
}

// TestPlaceSpreadsAcrossMachines: one rack runs two physical machines (each with
// four volume servers). Placing a 10+4 volume must spread its shards across both
// machines so no single machine holds more than ceil(14/2)=7, otherwise losing one
// box would take out more shards than EC can recover even though they look spread
// across distinct nodes.
func TestPlaceSpreadsAcrossMachines(t *testing.T) {
	topo := NewTopology()
	host := map[string]string{}
	add := func(id, h string) {
		n := topo.AddNode(id, "dc1", "dc1:rack0", 50)
		n.SetHost(h)
		n.AddDisk(0, "", 50, 0)
		host[id] = h
	}
	for i := 0; i < 4; i++ {
		add(fmt.Sprintf("a%d", i), "10.0.0.1")
		add(fmt.Sprintf("b%d", i), "10.0.0.2")
	}

	res, err := topo.Place(1, "c1", allShards(), Constraints{}, PlaceStrict)
	if err != nil {
		t.Fatalf("Place: %v", err)
	}
	perMachine := map[string]int{}
	for _, d := range res.Destinations {
		perMachine[host[d.Node]]++
	}
	if len(perMachine) < 2 {
		t.Fatalf("shards not spread across machines: %v", perMachine)
	}
	maxPerMachine := ceilDivide(erasure_coding.TotalShardsCount, 2)
	for h, c := range perMachine {
		if c > maxPerMachine {
			t.Errorf("machine %s holds %d shards, want <=%d (ceil(14/2))", h, c, maxPerMachine)
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

// TestPlaceSameRackCountIsDirectPerNodeCap: the 3rd ReplicaPlacement digit
// (SameRackCount) caps shards per node directly (max == digit), matching the
// per-rack DiffRackCount cap rather than allowing digit+1 per node.
func TestPlaceSameRackCountIsDirectPerNodeCap(t *testing.T) {
	rp := &super_block.ReplicaPlacement{SameRackCount: 2} // <=2 shards per node

	// 5 single-node racks: 5 nodes * 2 = 10 < 14, so a strict 10+4 placement
	// cannot satisfy the per-node cap and must fail. Under the old digit+1 reading
	// the cap would be 3/node => 15 slots and this would have wrongly succeeded.
	topo := buildPlaceTopo(5, 1, 50)
	if _, err := topo.Place(1, "c1", allShards(), Constraints{ReplicaPlacement: rp}, PlaceStrict); err == nil {
		t.Fatal("strict Place should fail: 5 nodes cannot hold 14 shards at <=2 per node")
	}

	// Durability-first relaxes the unsatisfiable per-node cap, still places every
	// shard, and reports the relaxation so it isn't silently weakened.
	topo = buildPlaceTopo(5, 1, 50)
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

// TestPlaceDiskTypeHardFilter: with DiskType set, shards land only on disks of
// that type, even though the snapshot also contains other-typed disks.
func TestPlaceDiskTypeHardFilter(t *testing.T) {
	topo := NewTopology()
	for r := 0; r < 4; r++ {
		rackKey := fmt.Sprintf("dc1:rack%d", r)
		ssd := topo.AddNode(fmt.Sprintf("ssd-%d:8080", r), "dc1", rackKey, 50)
		ssd.AddDisk(0, "ssd", 50, 0)
		hdd := topo.AddNode(fmt.Sprintf("hdd-%d:8080", r), "dc1", rackKey, 50)
		hdd.AddDisk(0, "hdd", 50, 0)
	}

	res, err := topo.Place(1, "c1", allShards(), Constraints{DiskType: "ssd", DiskTypePolicy: DiskTypeRequire}, PlaceStrict)
	if err != nil {
		t.Fatalf("Place ssd: %v", err)
	}
	for sid, d := range res.Destinations {
		node := topo.nodes[d.Node]
		disk := node.disks[d.DiskID]
		if disk == nil || disk.diskType != "ssd" {
			t.Errorf("shard %d placed on non-ssd disk: node=%s diskID=%d", sid, d.Node, d.DiskID)
		}
	}
}

// TestPlaceDiskTypeUnavailableFails: a request for a disk type with no matching
// disks fails rather than silently placing on the wrong tier.
func TestPlaceDiskTypeUnavailableFails(t *testing.T) {
	topo := NewTopology()
	for r := 0; r < 4; r++ {
		n := topo.AddNode(fmt.Sprintf("hdd-%d:8080", r), "dc1", fmt.Sprintf("dc1:rack%d", r), 50)
		n.AddDisk(0, "hdd", 50, 0)
	}
	if _, err := topo.Place(1, "c1", allShards(), Constraints{DiskType: "ssd", DiskTypePolicy: DiskTypeRequire}, PlaceStrict); err == nil {
		t.Fatal("expected Place to fail when no disks of the requested type exist")
	}
}

// TestPlaceHDDRequestMatchesEmptyTypeDisks: a "hdd" request normalizes to
// HardDriveType ("") and must land on the HDD disk (reported as ""), never the SSD
// disk, even on nodes that have both.
func TestPlaceHDDRequestMatchesEmptyTypeDisks(t *testing.T) {
	topo := NewTopology()
	for r := 0; r < 6; r++ {
		n := topo.AddNode(fmt.Sprintf("n%d:8080", r), "dc1", fmt.Sprintf("dc1:rack%d", r), 100)
		n.AddDisk(0, "", 50, 0)    // HDD (HardDriveType, reported as "")
		n.AddDisk(1, "ssd", 50, 0) // SSD
	}

	res, err := topo.Place(1, "c1", allShards(), Constraints{DiskType: "hdd", DiskTypePolicy: DiskTypeRequire}, PlaceStrict)
	if err != nil {
		t.Fatalf("Place hdd: %v", err)
	}
	for sid, d := range res.Destinations {
		if d.DiskID != 0 { // disk 0 is the HDD disk on every node
			t.Errorf("shard %d placed on disk %d (expected HDD disk 0) on node %s", sid, d.DiskID, d.Node)
		}
	}
}

// TestPlaceDurabilityCapRejectsSkewed: in a near-full cluster where only one disk
// has spare room, Place must not pile more than parityShards shards onto it (losing
// it would then lose more than EC can recover). It fails instead, so the caller
// leaves the volume unencoded rather than minting an unrecoverable layout.
func TestPlaceDurabilityCapRejectsSkewed(t *testing.T) {
	topo := NewTopology()
	// One spacious node plus four nearly-full ones, all in a single rack.
	a := topo.AddNode("a:8080", "dc1", "dc1:rack0", 100)
	a.AddDisk(0, "", 100, 0)
	for i := 0; i < 4; i++ {
		n := topo.AddNode(fmt.Sprintf("b%d:8080", i), "dc1", "dc1:rack0", 1)
		n.AddDisk(0, "", 1, 0)
	}

	// 14 shards, parity 4: node a is capped at 4, the others hold 1 each -> at most
	// 4+4=8 placeable without exceeding parityShards on a disk, so Place must fail.
	// (Without the per-disk cap, a would greedily absorb 10 shards and "succeed".)
	if _, err := topo.Place(1, "c1", allShards(), Constraints{}, PlaceDurabilityFirst); err == nil {
		t.Fatal("expected Place to fail rather than pile >parityShards shards on one disk")
	}
}

// TestReleaseVolumeShards: removes all of a volume's shards from the snapshot and
// credits the freed capacity at BOTH disk and node level (rack capacity sums node
// freeSlots), mirroring how FromActiveTopology accounts stale shards.
func TestReleaseVolumeShards(t *testing.T) {
	topo := NewTopology()
	// Node total 20 = two disks of 10. Two stale shards occupy one slot each, so the
	// snapshot would show disk0/disk1 at 9 and node at 18 (as FromActiveTopology does).
	n := topo.AddNode("n0:8080", "dc1", "dc1:rack0", 20)
	n.AddDisk(0, "", 10, 0)
	n.AddDisk(1, "", 10, 0)
	vk := volKey{collection: "c1", vid: 1}
	n.AddShards(1, "c1", 0, erasure_coding.ShardBits(uint32(1)<<3))
	n.AddShards(1, "c1", 1, erasure_coding.ShardBits(uint32(1)<<7))
	n.disks[0].freeSlots = 9
	n.disks[1].freeSlots = 9
	n.freeSlots = 18

	topo.ReleaseVolumeShards("c1", 1)

	if _, ok := n.shards[vk]; ok {
		t.Error("volume shards should be gone after ReleaseVolumeShards")
	}
	if n.disks[0].freeSlots != 10 || n.disks[1].freeSlots != 10 {
		t.Errorf("disk freeSlots not restored: disk0=%d, disk1=%d (want 10, 10)", n.disks[0].freeSlots, n.disks[1].freeSlots)
	}
	if n.freeSlots != 20 {
		t.Errorf("node freeSlots = %d, want 20 (must be credited at node level too)", n.freeSlots)
	}
}

// TestClearShardAccounting: dropping one disk's copy of a shard preserves a kept
// copy of the same shard on another disk of the same node, and credits no capacity.
func TestClearShardAccounting(t *testing.T) {
	topo := NewTopology()
	n := topo.AddNode("n0:8080", "dc1", "dc1:rack0", 50)
	n.AddDisk(0, "", 50, 0)
	n.AddDisk(1, "", 50, 0)
	vk := volKey{collection: "c1", vid: 1}
	// Shard 3 lives on disk 0 (keep) and disk 1 (duplicate to delete).
	n.AddShards(1, "c1", 0, erasure_coding.ShardBits(uint32(1)<<3))
	n.AddShards(1, "c1", 1, erasure_coding.ShardBits(uint32(1)<<3))
	if got := n.shards[vk].shardBits.Count(); got != 1 {
		t.Fatalf("union count = %d, want 1", got)
	}
	freeBefore := n.disks[1].freeSlots

	clearShardAccounting(n, vk, 3, 1)

	if !n.shards[vk].shardBits.Has(erasure_coding.ShardId(3)) {
		t.Error("kept copy of shard 3 (disk 0) lost from the node-level union")
	}
	if n.shards[vk].diskShardBits[1].Has(erasure_coding.ShardId(3)) {
		t.Error("disk-1 copy of shard 3 was not cleared")
	}
	if n.disks[1].freeSlots != freeBefore {
		t.Errorf("freeSlots changed %d -> %d; clearShardAccounting must not credit capacity", freeBefore, n.disks[1].freeSlots)
	}
}

// TestPlaceDiskTypePreferSpills: DiskTypePrefer fills the preferred type first and
// spills the remainder to other types, reporting SpilledToOtherDiskType. SSD is
// scarce (one tiny SSD per node) so the volume must spill to HDD, but there are
// enough disks to keep each within the parityShards durability cap.
func TestPlaceDiskTypePreferSpills(t *testing.T) {
	topo := NewTopology()
	for r := 0; r < 8; r++ {
		n := topo.AddNode(fmt.Sprintf("n%d:8080", r), "dc1", fmt.Sprintf("dc1:rack%d", r), 100)
		n.AddDisk(0, "ssd", 1, 0) // tiny SSD: 1 shard
		n.AddDisk(1, "", 50, 0)   // roomy HDD
	}

	res, err := topo.Place(1, "c1", allShards(), Constraints{DiskType: "ssd", DiskTypePolicy: DiskTypePrefer}, PlaceDurabilityFirst)
	if err != nil {
		t.Fatalf("Place: %v", err)
	}
	if len(res.Destinations) != erasure_coding.TotalShardsCount {
		t.Fatalf("placed %d, want %d", len(res.Destinations), erasure_coding.TotalShardsCount)
	}
	ssd, hdd := 0, 0
	for _, d := range res.Destinations {
		if topo.nodes[d.Node].disks[d.DiskID].diskType == "ssd" {
			ssd++
		} else {
			hdd++
		}
	}
	if ssd == 0 || hdd == 0 {
		t.Errorf("expected prefer-then-spill: some shards on SSD and some on HDD, got ssd=%d hdd=%d", ssd, hdd)
	}
	if !res.SpilledToOtherDiskType {
		t.Error("expected SpilledToOtherDiskType when SSD cannot hold every shard")
	}
}

// TestPlacePreferredTagsUseTaggedDisks: when tagged disks can hold the whole plan,
// every shard lands on a tagged disk and no spill is reported.
func TestPlacePreferredTagsUseTaggedDisks(t *testing.T) {
	topo := NewTopology()
	for r := 0; r < 4; r++ {
		rackKey := fmt.Sprintf("dc1:rack%d", r)
		fast := topo.AddNode(fmt.Sprintf("fast-%d:8080", r), "dc1", rackKey, 50)
		fast.AddDisk(0, "", 50, 0)
		fast.AddDiskTags(0, []string{"fast"})
		topo.AddNode(fmt.Sprintf("slow-%d:8080", r), "dc1", rackKey, 50).AddDisk(0, "", 50, 0)
	}

	res, err := topo.Place(1, "c1", allShards(), Constraints{PreferredTags: []string{"fast"}}, PlaceStrict)
	if err != nil {
		t.Fatalf("Place: %v", err)
	}
	for sid, d := range res.Destinations {
		if !diskHasAnyTag(topo.nodes[d.Node].disks[d.DiskID], []string{"fast"}) {
			t.Errorf("shard %d placed on an untagged disk (node %s)", sid, d.Node)
		}
	}
	if res.SpilledOutsidePreferredTags {
		t.Error("did not expect tag spill when tagged disks suffice")
	}
}

// TestPlacePreferredTagsSpillWhenInsufficient: when the tagged tier cannot hold the
// whole plan, Place falls back to all disks and reports SpilledOutsidePreferredTags.
func TestPlacePreferredTagsSpillWhenInsufficient(t *testing.T) {
	topo := NewTopology()
	for r := 0; r < 4; r++ {
		n := topo.AddNode(fmt.Sprintf("n-%d:8080", r), "dc1", fmt.Sprintf("dc1:rack%d", r), 50)
		if r == 0 {
			n.AddDisk(0, "", 5, 0) // the only tagged disk, too small for 14 shards
			n.AddDiskTags(0, []string{"fast"})
		} else {
			n.AddDisk(0, "", 50, 0)
		}
	}

	res, err := topo.Place(1, "c1", allShards(), Constraints{PreferredTags: []string{"fast"}}, PlaceStrict)
	if err != nil {
		t.Fatalf("Place: %v", err)
	}
	if len(res.Destinations) != erasure_coding.TotalShardsCount {
		t.Fatalf("placed %d, want %d", len(res.Destinations), erasure_coding.TotalShardsCount)
	}
	if !res.SpilledOutsidePreferredTags {
		t.Error("expected SpilledOutsidePreferredTags when the single fast disk cannot hold the plan")
	}
}

// TestPlaceStrictCapsCountEligibleRacks: with DiskTypeRequire, the even per-rack
// cap divides by racks that have a matching disk, not all racks, so SSDs in only
// some racks still place successfully.
func TestPlaceStrictCapsCountEligibleRacks(t *testing.T) {
	topo := NewTopology()
	// SSDs live in only 2 of 4 racks, with several SSD nodes per rack so 14 shards
	// fit at <= parityShards per disk. The even per-rack cap must divide by the 2
	// eligible racks (ceil(10/2)=5 data/rack), not all 4 (ceil(10/4)=3 -> infeasible).
	for r := 0; r < 4; r++ {
		rackKey := fmt.Sprintf("dc1:rack%d", r)
		topo.AddNode(fmt.Sprintf("hdd-%d:8080", r), "dc1", rackKey, 50).AddDisk(0, "", 50, 0)
		if r < 2 {
			for n := 0; n < 4; n++ {
				topo.AddNode(fmt.Sprintf("ssd-%d-%d:8080", r, n), "dc1", rackKey, 50).AddDisk(0, "ssd", 50, 0)
			}
		}
	}

	res, err := topo.Place(1, "c1", allShards(), Constraints{DiskType: "ssd", DiskTypePolicy: DiskTypeRequire}, PlaceStrict)
	if err != nil {
		t.Fatalf("Place ssd in 2/4 racks: %v", err)
	}
	if len(res.Destinations) != erasure_coding.TotalShardsCount {
		t.Fatalf("placed %d, want %d", len(res.Destinations), erasure_coding.TotalShardsCount)
	}
	for sid, d := range res.Destinations {
		if disk := topo.nodes[d.Node].disks[d.DiskID]; disk == nil || disk.diskType != "ssd" {
			t.Errorf("shard %d not on an SSD disk: node=%s disk=%d", sid, d.Node, d.DiskID)
		}
	}
}

// TestPlaceRotatesExtraRackShardsAcrossVolumes: 10+4 onto 4 equal racks is
// always 4+4+3+3. Sorted-id tie-breaks pinned the two fours on the first rack
// ids every volume. Free-slot tie-breaks must walk the extra shards so every
// rack is in the "4" set across a short series.
func TestPlaceRotatesExtraRackShardsAcrossVolumes(t *testing.T) {
	topo := buildPlaceTopo(4, 2, 200)
	const volumes = 8
	gotFour := map[string]int{}
	for v := 0; v < volumes; v++ {
		res, err := topo.Place(uint32(v+1), "c1", allShards(), Constraints{}, PlaceStrict)
		if err != nil {
			t.Fatalf("Place volume %d: %v", v+1, err)
		}
		perRack := map[string]int{}
		for _, d := range res.Destinations {
			perRack[d.Rack]++
		}
		if len(perRack) != 4 {
			t.Fatalf("volume %d racks %v, want 4", v+1, perRack)
		}
		counts := make([]int, 0, 4)
		for rack, n := range perRack {
			counts = append(counts, n)
			if n != 3 && n != 4 {
				t.Errorf("volume %d rack %s has %d shards, want 3 or 4", v+1, rack, n)
			}
			if n == 4 {
				gotFour[rack]++
			}
		}
		sort.Ints(counts)
		if counts[0] != 3 || counts[3] != 4 {
			t.Errorf("volume %d rack counts %v, want 3,3,4,4", v+1, perRack)
		}
	}
	if len(gotFour) < 4 {
		t.Errorf("extra (4-shard) racks did not rotate; hit %v over %d volumes", gotFour, volumes)
	}
}

func twoDiskRackRP() *super_block.ReplicaPlacement {
	return &super_block.ReplicaPlacement{DiffRackCount: 4, SameRackCount: 2}
}

func placeNeed(t *testing.T, topo *Topology, vid uint32, n int, rp *super_block.ReplicaPlacement) map[string]int {
	t.Helper()
	need := make([]int, n)
	for i := range need {
		need[i] = i
	}
	res, err := topo.Place(vid, "c1", need, Constraints{
		ReplicaPlacement: rp,
		Ratio:            func(string) (int, int) { return 10, 4 },
	}, PlaceDurabilityFirst)
	if err != nil {
		t.Fatalf("Place vid=%d: %v", vid, err)
	}
	got := map[string]int{}
	for _, d := range res.Destinations {
		got[d.Node]++
	}
	return got
}

// TestWithinRackPaysDownShardDebt3: 2 VS, same disk cap, live 100 vs 135,
// 3 shards, cap 2. freeSlots = cap−shards so the thinner also has more
// remaining slots; extras pay the debt — not 50/50 from vol 1.
// Equal remaining slots (same free, different shards) are the liveShards
// tie-break; see TestEcVolumeCountIsNotScore.
func TestWithinRackPaysDownShardDebt3(t *testing.T) {
	topo := NewTopology()
	a := topo.AddNode("a:8080", "dc1", "dc1:rack0", 4900)
	a.AddDisk(0, "", 4900, 100)
	b := topo.AddNode("b:8081", "dc1", "dc1:rack0", 4865)
	b.AddDisk(0, "", 4865, 135)
	rp := twoDiskRackRP()

	got := placeNeed(t, topo, 1, 3, rp)
	if got["a:8080"] != 2 || got["b:8081"] != 1 {
		t.Fatalf("first volume %v, want a=2 b=1 (pair on thinner)", got)
	}

	const rounds = 35
	for v := uint32(2); v <= rounds; v++ {
		beforeA, beforeB := nodeLiveShardCount(a), nodeLiveShardCount(b)
		got = placeNeed(t, topo, v, 3, rp)
		if beforeA+2 <= beforeB && (got["a:8080"] != 2 || got["b:8081"] != 1) {
			t.Fatalf("volume %d %v before A=%d B=%d; debt≥2 must stay 2+1 on A",
				v, got, beforeA, beforeB)
		}
	}
	if ga, gb := nodeLiveShardCount(a), nodeLiveShardCount(b); ga != 170 || gb != 170 {
		t.Errorf("after %d×(2+1) live A=%d B=%d, want 170/170", rounds, ga, gb)
	}
}

// TestWithinRackFourShardsStayEven: 4 shards / 2 disks is always 2+2.
// Remainder is 0 — packing 3+1 would put 3 shards on one disk.
func TestWithinRackFourShardsStayEven(t *testing.T) {
	topo := NewTopology()
	a := topo.AddNode("a:8080", "dc1", "dc1:rack0", 5000)
	a.AddDisk(0, "", 5000, 100)
	b := topo.AddNode("b:8081", "dc1", "dc1:rack0", 5000)
	b.AddDisk(0, "", 5000, 130)
	rp := twoDiskRackRP()

	got := placeNeed(t, topo, 1, 4, rp)
	if got["a:8080"] != 2 || got["b:8081"] != 2 {
		t.Fatalf("first volume %v, want 2+2 (no 3+1)", got)
	}
	if ga, gb := nodeLiveShardCount(a), nodeLiveShardCount(b); ga != 102 || gb != 132 {
		t.Fatalf("after 2+2 live A=%d B=%d, want 102/132 (gap frozen)", ga, gb)
	}
	got = placeNeed(t, topo, 2, 4, rp)
	if got["a:8080"] != 2 || got["b:8081"] != 2 {
		t.Fatalf("second volume %v, want 2+2", got)
	}
}

// TestEcVolumeCountIsNotScore: both VS show the same "EC volumes" cardinality;
// the pair still goes to the lower shard count.
func TestEcVolumeCountIsNotScore(t *testing.T) {
	topo := NewTopology()
	a := topo.AddNode("a:8080", "dc1", "dc1:rack0", 5000)
	a.AddDisk(0, "", 5000, 2528)
	b := topo.AddNode("b:8081", "dc1", "dc1:rack0", 5000)
	b.AddDisk(0, "", 5000, 2563)
	for i := uint32(1); i <= 1000; i++ {
		a.AddShards(i, "other", 0, erasure_coding.ShardBits(1))
		b.AddShards(i, "other", 0, erasure_coding.ShardBits(1))
	}
	got := placeNeed(t, topo, 2000, 3, twoDiskRackRP())
	if got["a:8080"] != 2 || got["b:8081"] != 1 {
		t.Fatalf("got %v; score must be shard count (2528 vs 2563), not 1000 EC volumes", got)
	}
}

// TestFreeSlotsWinsAsymmetricExtras: more freeSlots on the fatter disk takes
// the remainder even if that disk already has more live shards. 4 shards stay
// 2+2 (remainder 0; no 3+1).
func TestFreeSlotsWinsAsymmetricExtras(t *testing.T) {
	topo := NewTopology()
	a := topo.AddNode("a:8080", "dc1", "dc1:rack0", 100)
	a.AddDisk(0, "", 100, 100)
	b := topo.AddNode("b:8081", "dc1", "dc1:rack0", 200)
	b.AddDisk(0, "", 200, 135)
	rp := twoDiskRackRP()
	got := placeNeed(t, topo, 1, 3, rp)
	if got["a:8080"] != 1 || got["b:8081"] != 2 {
		t.Fatalf("3 shards %v, want 2+1 on B (more freeSlots) even though B has more shards", got)
	}

	topo = NewTopology()
	a = topo.AddNode("a:8080", "dc1", "dc1:rack0", 100)
	a.AddDisk(0, "", 100, 100)
	b = topo.AddNode("b:8081", "dc1", "dc1:rack0", 200)
	b.AddDisk(0, "", 200, 135)
	got = placeNeed(t, topo, 1, 4, rp)
	if got["a:8080"] != 2 || got["b:8081"] != 2 {
		t.Fatalf("4 shards %v, want 2+2 (remainder 0; freeSlots must not pack 3+1)", got)
	}
}

// TestReleaseVolumeShardsKeepsOtherLiveCounts: encode's ReleaseVolumeShards
// must not zero disk.shardCount for other volumes (that was the green-field bug).
func TestReleaseVolumeShardsKeepsOtherLiveCounts(t *testing.T) {
	topo := NewTopology()
	n := topo.AddNode("n0:8080", "dc1", "dc1:rack0", 100)
	n.AddDisk(0, "", 90, 10)
	n.AddShards(1, "c1", 0, erasure_coding.ShardBits(1<<0))
	n.AddShards(2, "c1", 0, erasure_coding.ShardBits(1<<1|1<<2))
	n.disks[0].shardCount = 10
	n.disks[0].freeSlots = 90

	topo.ReleaseVolumeShards("c1", 1)

	if nodeLiveShardCount(n) != 9 {
		t.Errorf("live shardCount = %d after releasing 1 of 10, want 9 (must not zero the snapshot)", nodeLiveShardCount(n))
	}
	if _, ok := n.shards[volKey{collection: "c1", vid: 2}]; !ok {
		t.Error("other volume shards were dropped")
	}
}

const imbalanceLiveBase = 200
const imbalanceDiskCap = 5000

// buildImbalancedTopo is racks × machines × disks: one VS per machine, disk
// ids 0..D-1. Earlier (rack, machine, disk) ids are fatter — more liveShards,
// fewer freeSlots (cap−live). Sorted-id extras would pile on those first ids;
// freeSlots-then-liveShards extras must walk the other way.
func buildImbalancedTopo(racks, machines, disks int) *Topology {
	topo := NewTopology()
	idx := 0
	for r := 0; r < racks; r++ {
		rackKey := fmt.Sprintf("dc1:rack%d", r)
		for m := 0; m < machines; m++ {
			host := fmt.Sprintf("10.0.%d.%d", r, m)
			lives := make([]int, disks)
			frees := make([]int, disks)
			nodeFree := 0
			for d := 0; d < disks; d++ {
				lives[d] = imbalanceLiveBase - idx
				idx++
				frees[d] = imbalanceDiskCap - lives[d]
				nodeFree += frees[d]
			}
			n := topo.AddNode(host+":8080", "dc1", rackKey, nodeFree)
			n.SetHost(host)
			for d := 0; d < disks; d++ {
				n.AddDisk(uint32(d), "", frees[d], lives[d])
			}
		}
	}
	return topo
}

type diskKey struct {
	node   string
	diskID uint32
}

func snapshotDiskLive(topo *Topology) map[diskKey]int {
	out := make(map[diskKey]int)
	for _, n := range topo.nodes {
		for id, d := range n.disks {
			out[diskKey{n.id, id}] = d.shardCount
		}
	}
	return out
}

func snapshotRackLive(topo *Topology) map[string]int {
	out := make(map[string]int)
	for _, n := range topo.nodes {
		out[n.rack] += nodeLiveShardCount(n)
	}
	return out
}

func place104Counts(t *testing.T, topo *Topology) (perDisk map[diskKey]int, perRack map[string]int) {
	t.Helper()
	res, err := topo.Place(1, "c1", allShards(), Constraints{}, PlaceStrict)
	if err != nil {
		t.Fatalf("Place: %v", err)
	}
	if len(res.Destinations) != erasure_coding.TotalShardsCount {
		t.Fatalf("placed %d shards, want %d", len(res.Destinations), erasure_coding.TotalShardsCount)
	}
	perDisk = map[diskKey]int{}
	perRack = map[string]int{}
	for _, d := range res.Destinations {
		perDisk[diskKey{d.Node, d.DiskID}]++
		perRack[topo.nodes[d.Node].rack]++
	}
	return perDisk, perRack
}

func assertTailsPreferThinDisks(t *testing.T, topo *Topology, before map[diskKey]int, got map[diskKey]int) {
	t.Helper()
	for _, n := range topo.nodes {
		type row struct {
			live int
			got  int
			id   uint32
		}
		rows := make([]row, 0, len(n.disks))
		for id := range n.disks {
			k := diskKey{n.id, id}
			rows = append(rows, row{live: before[k], got: got[k], id: id})
		}
		sort.Slice(rows, func(i, j int) bool {
			if rows[i].live != rows[j].live {
				return rows[i].live > rows[j].live
			}
			return rows[i].id < rows[j].id
		})
		for i := 1; i < len(rows); i++ {
			if rows[i-1].got > rows[i].got {
				t.Errorf("node %s: fatter disk %d (live=%d) got %d of this volume, thinner disk %d (live=%d) got %d; remainder tails must prefer thin disks, not first ids",
					n.id, rows[i-1].id, rows[i-1].live, rows[i-1].got, rows[i].id, rows[i].live, rows[i].got)
			}
		}
	}
}

func assertRackExtrasWalkThin(t *testing.T, before map[string]int, got map[string]int) {
	t.Helper()
	type row struct {
		rack string
		live int
		got  int
	}
	rows := make([]row, 0, len(before))
	for rk, live := range before {
		rows = append(rows, row{rack: rk, live: live, got: got[rk]})
	}
	sort.Slice(rows, func(i, j int) bool {
		if rows[i].live != rows[j].live {
			return rows[i].live > rows[j].live
		}
		return rows[i].rack < rows[j].rack
	})
	for i := 1; i < len(rows); i++ {
		if rows[i-1].got > rows[i].got {
			t.Errorf("fatter rack %s (live=%d) got %d, thinner rack %s (live=%d) got %d; extras must walk to thinner racks",
				rows[i-1].rack, rows[i-1].live, rows[i-1].got, rows[i].rack, rows[i].live, rows[i].got)
		}
	}
}

func assertLiveGapFrozen(t *testing.T, before, after map[diskKey]int, placed map[diskKey]int) {
	t.Helper()
	keys := make([]diskKey, 0, len(before))
	for k := range before {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool {
		if keys[i].node != keys[j].node {
			return keys[i].node < keys[j].node
		}
		return keys[i].diskID < keys[j].diskID
	})
	for i := 0; i < len(keys); i++ {
		for j := i + 1; j < len(keys); j++ {
			a, b := keys[i], keys[j]
			if before[a]-before[b] != after[a]-after[b] {
				t.Errorf("live gap %s/%d vs %s/%d changed %d → %d (placed %d vs %d); remainder 0 must not close the gap",
					a.node, a.diskID, b.node, b.diskID, before[a]-before[b], after[a]-after[b], placed[a], placed[b])
			}
		}
	}
}

// TestPlace10Plus4ImbalanceTiebreak: 10+4 onto seeded liveShards/freeSlots
// skew. Remainder tails pay thin disks/racks, not sorted-first ids. Remainder
// 0 (1-1-7, 7-1-2) is a no-op on the live gap — that is the contract.
// Existing 4-1-2 / within-rack 2-disk cases stay in the tests above.
func TestPlace10Plus4ImbalanceTiebreak(t *testing.T) {
	cases := []struct {
		name              string
		racks, machines, disks int
		remainderZero     bool
		fatFirstGetsZero  bool
		rackExtras        bool
		fatRacksTwoPlusOne bool
	}{
		{name: "1-1-4", racks: 1, machines: 1, disks: 4},
		{name: "1-1-6", racks: 1, machines: 1, disks: 6},
		{name: "1-1-8", racks: 1, machines: 1, disks: 8},
		{name: "1-1-15", racks: 1, machines: 1, disks: 15, fatFirstGetsZero: true},
		{name: "1-1-7", racks: 1, machines: 1, disks: 7, remainderZero: true},
		{name: "7-1-2", racks: 7, machines: 1, disks: 2, remainderZero: true},
		{name: "4-2-3", racks: 4, machines: 2, disks: 3, rackExtras: true},
		{name: "5-2-2", racks: 5, machines: 2, disks: 2, rackExtras: true},
		{name: "6-1-2", racks: 6, machines: 1, disks: 2, rackExtras: true, fatRacksTwoPlusOne: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			topo := buildImbalancedTopo(tc.racks, tc.machines, tc.disks)
			beforeDisk := snapshotDiskLive(topo)
			beforeRack := snapshotRackLive(topo)

			gotDisk, gotRack := place104Counts(t, topo)

			if tc.fatFirstGetsZero {
				var zero []diskKey
				for _, n := range topo.nodes {
					for id := range n.disks {
						k := diskKey{n.id, id}
						if gotDisk[k] == 0 {
							zero = append(zero, k)
						}
					}
				}
				if len(zero) != 1 {
					t.Fatalf("want exactly one disk with 0 shards of this volume, got %v (placed %v)", zero, gotDisk)
				}
				if zero[0].diskID != 0 {
					t.Fatalf("zero-of-this-volume disk is %s/%d; must be the fat first-id disk, not the last sorted id", zero[0].node, zero[0].diskID)
				}
			}

			assertTailsPreferThinDisks(t, topo, beforeDisk, gotDisk)

			if tc.rackExtras || tc.remainderZero && tc.racks > 1 {
				assertRackExtrasWalkThin(t, beforeRack, gotRack)
			}

			if tc.fatRacksTwoPlusOne {
				for _, n := range topo.nodes {
					if gotRack[n.rack] != 3 {
						continue
					}
					counts := make([]int, 0, len(n.disks))
					for id := range n.disks {
						counts = append(counts, gotDisk[diskKey{n.id, id}])
					}
					sort.Ints(counts)
					if len(counts) != 2 || counts[0] != 1 || counts[1] != 2 {
						t.Errorf("rack %s has 3 shards on disks %v, want 2+1", n.rack, counts)
					}
				}
			}

			if tc.remainderZero {
				after := snapshotDiskLive(topo)
				assertLiveGapFrozen(t, beforeDisk, after, gotDisk)
			}
		})
	}
}

func TestVolumeDeadbandToShardSlots(t *testing.T) {
	if got := volumeDeadbandToShardSlots(0, 10); got != 0 {
		t.Fatalf("M=0 → %d, want 0", got)
	}
	if got := volumeDeadbandToShardSlots(1, 10); got != 10 {
		t.Fatalf("M=1 at 10+4 → %d, want 10", got)
	}
	if got := volumeDeadbandToShardSlots(1, 8); got != 8 {
		t.Fatalf("M=1 at 8+4 → %d, want 8", got)
	}
	if got := volumeDeadbandToShardSlots(50, 10); got != 500 {
		t.Fatalf("M=50 at 10+4 → %d, want 500", got)
	}
	if got := volumeDeadbandToShardSlots(-3, 10); got != 0 {
		t.Fatalf("negative M → %d, want 0", got)
	}
}

func TestAbsIntDiff(t *testing.T) {
	if got := absIntDiff(105, 100); got != 5 {
		t.Fatalf("absIntDiff(105, 100)=%d, want 5", got)
	}
	if got := absIntDiff(100, 105); got != 5 {
		t.Fatalf("absIntDiff(100, 105)=%d, want 5", got)
	}
	if got := absIntDiff(100, 100); got != 0 {
		t.Fatalf("absIntDiff(100, 100)=%d, want 0", got)
	}
	if got := absIntDiff(math.MaxInt, math.MaxInt-5); got != 5 {
		t.Fatalf("absIntDiff(MaxInt, MaxInt-5)=%d, want 5", got)
	}
	if got := absIntDiff(math.MaxInt-5, math.MaxInt); got != 5 {
		t.Fatalf("absIntDiff(MaxInt-5, MaxInt)=%d, want 5", got)
	}
	if got := absIntDiff(math.MinInt+5, math.MinInt); got != 5 {
		t.Fatalf("absIntDiff(MinInt+5, MinInt)=%d, want 5", got)
	}
	if got := absIntDiff(math.MinInt, math.MinInt+5); got != 5 {
		t.Fatalf("absIntDiff(MinInt, MinInt+5)=%d, want 5", got)
	}
}

func placeFirstShardNode(t *testing.T, topo *Topology, m int) string {
	t.Helper()
	res, err := topo.Place(1, "c1", []int{0}, Constraints{FreeSlotsDeadband: m}, PlaceStrict)
	if err != nil {
		t.Fatalf("Place M=%d: %v", m, err)
	}
	d, ok := res.Destinations[0]
	if !ok {
		t.Fatal("missing shard 0")
	}
	return d.Node
}

// TestFreeSlotsDeadbandDefersToLiveShards: two nodes, empty this-volume.
// b has slightly more freeSlots but more live shards. M=0 follows freeSlots
// (b); M=1 volume (×10 shard slots at 10+4) covers the gap and follows
// liveShards (a). Sorted-id would always pick a.
func TestFreeSlotsDeadbandDefersToLiveShards(t *testing.T) {
	build := func() *Topology {
		topo := NewTopology()
		a := topo.AddNode("a:8080", "dc1", "dc1:rack0", 100)
		a.AddDisk(0, "", 100, 10)
		b := topo.AddNode("b:8081", "dc1", "dc1:rack0", 105)
		b.AddDisk(0, "", 105, 50)
		return topo
	}
	if got := placeFirstShardNode(t, build(), 0); got != "b:8081" {
		t.Fatalf("M=0: got %s, want b (strict freeSlots)", got)
	}
	if got := placeFirstShardNode(t, build(), 1); got != "a:8080" {
		t.Fatalf("M=1 volume: got %s, want a (liveShards remainder)", got)
	}
}

// TestFreeSlotsDeadbandZeroExactTieUsesLiveShards: equal freeSlots, M=0
// remainder is liveShards, not the lower sorted id.
func TestFreeSlotsDeadbandZeroExactTieUsesLiveShards(t *testing.T) {
	topo := NewTopology()
	a := topo.AddNode("a:8080", "dc1", "dc1:rack0", 100)
	a.AddDisk(0, "", 100, 50)
	b := topo.AddNode("b:8081", "dc1", "dc1:rack0", 100)
	b.AddDisk(0, "", 100, 10)
	if got := placeFirstShardNode(t, topo, 0); got != "b:8081" {
		t.Fatalf("equal freeSlots M=0: got %s, want b (lower liveShards)", got)
	}
}

// TestFreeSlotsDeadbandStillHonorsLargeGap: gap above M still follows freeSlots
// even if that node is fatter on liveShards.
func TestFreeSlotsDeadbandStillHonorsLargeGap(t *testing.T) {
	topo := NewTopology()
	a := topo.AddNode("a:8080", "dc1", "dc1:rack0", 100)
	a.AddDisk(0, "", 100, 10)
	b := topo.AddNode("b:8081", "dc1", "dc1:rack0", 200)
	b.AddDisk(0, "", 200, 80)
	if got := placeFirstShardNode(t, topo, 1); got != "b:8081" {
		t.Fatalf("M=1 volume gap=100 shards: got %s, want b (freeSlots still wins)", got)
	}
}

// TestFreeSlotsDeadbandScalesWithRatio: M=1 volume is 8 shard slots at 8+4
// and 10 at default 10+4. Gap of 9 shards: 8+4 still follows freeSlots; 10+4
// ties and follows liveShards.
func TestFreeSlotsDeadbandScalesWithRatio(t *testing.T) {
	build := func() *Topology {
		topo := NewTopology()
		a := topo.AddNode("a:8080", "dc1", "dc1:rack0", 100)
		a.AddDisk(0, "", 100, 10)
		b := topo.AddNode("b:8081", "dc1", "dc1:rack0", 109)
		b.AddDisk(0, "", 109, 50)
		return topo
	}
	place := func(topo *Topology, ratio func(string) (int, int)) string {
		t.Helper()
		res, err := topo.Place(1, "c1", []int{0}, Constraints{FreeSlotsDeadband: 1, Ratio: ratio}, PlaceStrict)
		if err != nil {
			t.Fatalf("Place: %v", err)
		}
		d, ok := res.Destinations[0]
		if !ok {
			t.Fatal("missing shard 0")
		}
		return d.Node
	}
	if got := place(build(), func(string) (int, int) { return 8, 4 }); got != "b:8081" {
		t.Fatalf("8+4 M=1 gap=9: got %s, want b (deadband 8, freeSlots)", got)
	}
	if got := place(build(), nil); got != "a:8080" {
		t.Fatalf("10+4 M=1 gap=9: got %s, want a (deadband 10, liveShards)", got)
	}
}

func rackExtraTopo(thinFree, thinLive, fatFree, fatLive int) *Topology {
	topo := NewTopology()
	add := func(rack, id string, free, live int) {
		n := topo.AddNode(id, "dc1", rack, free)
		n.AddDisk(0, "", free, live)
	}
	add("dc1:rack0", "thin:8080", thinFree, thinLive)
	add("dc1:rack1", "fat1:8080", fatFree, fatLive)
	add("dc1:rack2", "fat2:8080", fatFree, fatLive)
	add("dc1:rack3", "fat3:8080", fatFree, fatLive)
	return topo
}

func place104PerRack(t *testing.T, topo *Topology, m int) map[string]int {
	t.Helper()
	res, err := topo.Place(1, "c1", allShards(), Constraints{FreeSlotsDeadband: m}, PlaceStrict)
	if err != nil {
		t.Fatalf("Place M=%d: %v", m, err)
	}
	got := map[string]int{}
	for _, d := range res.Destinations {
		got[d.Rack]++
	}
	return got
}

// TestRackDeadbandDefersToLiveShards: 10+4 extras are 4+4+3+3. Thin rack has
// fewer freeSlots (gap 5) but fewer liveShards. M=1 volume is 10 shard slots,
// so the gap is a tie and extras go to the thinner live rack.
func TestRackDeadbandDefersToLiveShards(t *testing.T) {
	got := place104PerRack(t, rackExtraTopo(100, 10, 105, 50), 1)
	if got["rack0"] != 4 {
		t.Fatalf("M=1 gap=5: thin rack0 got %d, want 4 (liveShards remainder); per-rack %v", got["rack0"], got)
	}
}

// TestRackDeadbandStillHonorsLargeGap: same thin-vs-fat live, but freeSlots
// gap 100 is above M=1 (10 shard slots), so extras still follow freeSlots.
func TestRackDeadbandStillHonorsLargeGap(t *testing.T) {
	got := place104PerRack(t, rackExtraTopo(100, 10, 200, 50), 1)
	if got["rack0"] != 3 {
		t.Fatalf("M=1 gap=100: thin rack0 got %d, want 3 (freeSlots still wins); per-rack %v", got["rack0"], got)
	}
}

// Greptile chain at M=1 volume (10 shard slots): 120/300, 110/200, 100/100.
// Pairwise vs the current winner walks 120→110→100 or 100→110→120 depending
// on scan order. maxFree=120 keeps |Δ|≤10, so 110 wins — not 100 (out of
// band) and not 120 (fatter live).
func TestNodeDeadbandAnchorsOnMaxFree(t *testing.T) {
	run := func(t *testing.T, frees, lives [3]int, want string) {
		t.Helper()
		topo := NewTopology()
		ids := [3]string{"n0:8080", "n1:8081", "n2:8082"}
		for i, id := range ids {
			n := topo.AddNode(id, "dc1", "dc1:rack0", frees[i])
			n.AddDisk(0, "", frees[i], lives[i])
		}
		if got := placeFirstShardNode(t, topo, 1); got != want {
			t.Fatalf("got %s, want %s (110 in-band, not 100/120)", got, want)
		}
	}
	t.Run("high-to-low", func(t *testing.T) {
		run(t, [3]int{120, 110, 100}, [3]int{300, 200, 100}, "n1:8081")
	})
	t.Run("low-to-high", func(t *testing.T) {
		run(t, [3]int{100, 110, 120}, [3]int{100, 200, 300}, "n1:8081")
	})
}

func TestRackDeadbandAnchorsOnMaxFree(t *testing.T) {
	run := func(t *testing.T, frees, lives [3]int, want string) {
		t.Helper()
		topo := NewTopology()
		for i := 0; i < 3; i++ {
			n := topo.AddNode(fmt.Sprintf("n%d:8080", i), "dc1", fmt.Sprintf("dc1:rack%d", i), frees[i])
			n.AddDisk(0, "", frees[i], lives[i])
		}
		res, err := topo.Place(1, "c1", []int{0}, Constraints{FreeSlotsDeadband: 1}, PlaceStrict)
		if err != nil {
			t.Fatalf("Place: %v", err)
		}
		d, ok := res.Destinations[0]
		if !ok {
			t.Fatal("missing shard 0")
		}
		if d.Rack != want {
			t.Fatalf("got rack %s, want %s (110 in-band, not 100/120)", d.Rack, want)
		}
	}
	t.Run("high-to-low", func(t *testing.T) {
		run(t, [3]int{120, 110, 100}, [3]int{300, 200, 100}, "rack1")
	})
	t.Run("low-to-high", func(t *testing.T) {
		run(t, [3]int{100, 110, 120}, [3]int{100, 200, 300}, "rack1")
	})
}
