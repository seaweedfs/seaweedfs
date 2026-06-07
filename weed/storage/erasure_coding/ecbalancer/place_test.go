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

// TestPlaceDistributesEvenlyAcrossManyVolumes: encode places each volume on the
// shared snapshot (Place reserves into it), so node selection must account for the
// load already placed. Otherwise the sorted-first machine wins the first shard of
// every volume and accumulates far more total shards than the rest.
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
