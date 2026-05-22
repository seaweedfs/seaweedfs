package ecbalancer

import (
	"fmt"
	"sort"

	"github.com/seaweedfs/seaweedfs/weed/storage/erasure_coding"
	"github.com/seaweedfs/seaweedfs/weed/storage/super_block"
	storagetypes "github.com/seaweedfs/seaweedfs/weed/storage/types"
)

// Constraints configures a Place call. Ratio resolves a collection's
// (dataShards, parityShards); nil uses the standard scheme. ReplicaPlacement,
// when non-nil, caps shards per DC (DiffDataCenterCount), per rack
// (DiffRackCount), and per node within a rack (SameRackCount).
//
// FilterDiskType selects how DiskType is used: false means any disk type is
// eligible; true means only disks whose type matches DiskType (compared after
// normalization, so "" and "hdd" both mean HardDriveType). The separate flag is
// required because HardDriveType normalizes to "", which would otherwise be
// indistinguishable from "any".
type Constraints struct {
	DiskType         string
	FilterDiskType   bool
	ReplicaPlacement *super_block.ReplicaPlacement
	Ratio            func(collection string) (dataShards, parityShards int)
}

// diskTypeMatches reports whether disk type dt satisfies a request for want when
// filtering is enabled; types are normalized so "" and "hdd" are equivalent.
func diskTypeMatches(dt, want string, filter bool) bool {
	if !filter {
		return true
	}
	return storagetypes.ToDiskType(dt).String() == storagetypes.ToDiskType(want).String()
}

// Destination is a chosen target for one shard.
type Destination struct {
	Node   string
	DiskID uint32
	Rack   string // composite "dc:rack"
}

// PlaceResult holds the chosen destinations and which constraints (if any) had to
// be relaxed to place every shard (durability-first only).
type PlaceResult struct {
	Destinations map[int]Destination
	Relaxed      []string
}

// PlacementMode selects the strictness/relaxation policy.
type PlacementMode int

const (
	// PlaceStrict (encode): caps and ReplicaPlacement are hard. Place fails rather
	// than violate them, so the caller can leave the volume unencoded and retry.
	PlaceStrict PlacementMode = iota
	// PlaceDurabilityFirst (repair): relax per-type caps -> data/parity
	// anti-affinity -> ReplicaPlacement, in that order, until each shard lands.
	// Fail only if no disk has free capacity at all.
	PlaceDurabilityFirst
)

// relaxation controls which placement-quality constraints are enforced on an
// attempt. preferring fresh nodes (repair's "avoid surviving-shard nodes") is not
// listed: pickNodeInRack already selects the node with the fewest shards of the
// volume, so survivors are deprioritized with built-in fallback.
type relaxation struct {
	caps         bool
	antiAffinity bool
	rp           bool
}

func (r relaxation) relaxedNames() []string {
	var n []string
	if !r.caps {
		n = append(n, "caps")
	}
	if !r.antiAffinity {
		n = append(n, "anti-affinity")
	}
	if !r.rp {
		n = append(n, "replica-placement")
	}
	return n
}

var strictAttempts = []relaxation{{caps: true, antiAffinity: true, rp: true}}

var durabilityAttempts = []relaxation{
	{caps: true, antiAffinity: true, rp: true},
	{caps: false, antiAffinity: true, rp: true},
	{caps: false, antiAffinity: false, rp: true},
	{caps: false, antiAffinity: false, rp: false},
}

type placedEntry struct {
	node    *Node
	sid     int
	rackKey string
}

// Place assigns destinations for the `need` shard ids of volume (collection,vid),
// reading the volume's already-placed shards from the snapshot (so encode passes
// an empty-for-this-volume snapshot, repair passes one seeded with the surviving
// shards). Data shards are placed first, then parity with anti-affinity to the
// data-bearing racks (and vice-versa), each capped to an even per-rack share.
// Reservations are journaled and rolled back if any shard cannot be placed.
func (t *Topology) Place(vid uint32, collection string, need []int, c Constraints, mode PlacementMode) (*PlaceResult, error) {
	result := &PlaceResult{Destinations: make(map[int]Destination, len(need))}
	if len(need) == 0 {
		return result, nil
	}

	vk := volKey{collection: collection, vid: vid}
	dataShards, parityShards := erasure_coding.DataShardsCount, erasure_coding.ParityShardsCount
	if c.Ratio != nil {
		if d, p := c.Ratio(collection); d > 0 && p > 0 {
			dataShards, parityShards = d, p
		}
	}

	racks := buildRacks(t.nodes)
	if len(racks) == 0 {
		return nil, fmt.Errorf("no racks available for EC placement")
	}
	rackKeys := sortedKeys(racks)
	numRacks := len(racks)

	// dcOfRack maps each rack key to its data center (for the ReplicaPlacement DC
	// limit). Built from every node so all racks are covered, not just occupied ones.
	dcOfRack := make(map[string]string, numRacks)
	for _, n := range t.nodes {
		dcOfRack[n.rack] = n.dc
	}

	// Per-type shard ids per rack (for even caps), total shard count per rack
	// (DiffRackCount) and per DC (DiffDataCenterCount), and the racks bearing each
	// type (anti-affinity) — all seeded from the volume's existing shards.
	shardsPerRack := map[bool]map[string][]int{true: {}, false: {}}
	rackShardCount := map[string]int{}
	dcShardCount := map[string]int{}
	bearing := map[bool]map[string]bool{true: {}, false: {}}
	for _, n := range t.nodes {
		info, ok := n.shards[vk]
		if !ok {
			continue
		}
		for s := 0; s < erasure_coding.MaxShardCount; s++ {
			if !info.shardBits.Has(erasure_coding.ShardId(s)) {
				continue
			}
			isData := s < dataShards
			shardsPerRack[isData][n.rack] = append(shardsPerRack[isData][n.rack], s)
			rackShardCount[n.rack]++
			dcShardCount[n.dc]++
			bearing[isData][n.rack] = true
		}
	}

	attempts := strictAttempts
	if mode == PlaceDurabilityFirst {
		attempts = durabilityAttempts
	}

	var journal []placedEntry
	relaxedSeen := map[string]bool{}

	placeShard := func(sid int, isData bool) bool {
		typeTotal := dataShards
		if !isData {
			typeTotal = parityShards
		}
		for _, rl := range attempts {
			node, diskID, ok := t.chooseShardDest(vk, sid, isData, dataShards, typeTotal, numRacks, racks, rackKeys, c, shardsPerRack[isData], rackShardCount, dcShardCount, dcOfRack, bearing, rl)
			if !ok {
				continue
			}
			reserveShard(node, vk, sid, diskID)
			node.freeSlots--
			racks[node.rack].freeSlots--
			shardsPerRack[isData][node.rack] = append(shardsPerRack[isData][node.rack], sid)
			rackShardCount[node.rack]++
			dcShardCount[dcOfRack[node.rack]]++
			bearing[isData][node.rack] = true
			journal = append(journal, placedEntry{node: node, sid: sid, rackKey: node.rack})
			result.Destinations[sid] = Destination{Node: node.id, DiskID: diskID, Rack: node.rack}
			for _, name := range rl.relaxedNames() {
				relaxedSeen[name] = true
			}
			return true
		}
		return false
	}

	// Data shards first, then parity, so parity can avoid data-bearing racks.
	for _, isData := range []bool{true, false} {
		for _, sid := range shardsOfType(need, isData, dataShards) {
			if placeShard(sid, isData) {
				continue
			}
			for _, e := range journal {
				releaseShard(e.node, vk, e.sid)
				e.node.freeSlots++
				racks[e.rackKey].freeSlots++
			}
			return nil, fmt.Errorf("cannot place EC shard %d of volume %d (collection %q): no disk with capacity", sid, vid, collection)
		}
	}

	for name := range relaxedSeen {
		result.Relaxed = append(result.Relaxed, name)
	}
	sort.Strings(result.Relaxed)
	return result, nil
}

// chooseShardDest selects a (node, disk) for one shard at the given relaxation
// level: pick a rack (even per-type cap + ReplicaPlacement rack limit + two-pass
// anti-affinity to the opposite type), then the least-loaded node in that rack,
// then the best disk on that node. Returns ok=false when no rack/node/disk fits.
func (t *Topology) chooseShardDest(vk volKey, sid int, isData bool, dataShards, typeTotal, numRacks int, racks map[string]*rack, rackKeys []string, c Constraints, shardsPerRackType map[string][]int, rackShardCount, dcShardCount map[string]int, dcOfRack map[string]string, bearing map[bool]map[string]bool, rl relaxation) (*Node, uint32, bool) {
	maxPerRack := numRacks*typeTotal + 1 // effectively unlimited when caps are relaxed
	if rl.caps {
		if maxPerRack = ceilDivide(typeTotal, numRacks); maxPerRack < 1 {
			maxPerRack = 1
		}
	}

	var anti map[string]bool
	if rl.antiAffinity {
		anti = bearing[!isData] // racks already holding the opposite shard type
	}

	rp := c.ReplicaPlacement
	if !rl.rp {
		rp = nil
	}
	// A rack is eligible only if it is under both the per-rack (DiffRackCount) and
	// per-DC (DiffDataCenterCount) shard caps. The DC cap is what spreads shards
	// across data centers; it is enforced only when set, so non-DC placements are
	// unchanged.
	withinLimit := func(r string) bool {
		if rp == nil {
			return true
		}
		if rp.DiffRackCount > 0 && rackShardCount[r] >= rp.DiffRackCount {
			return false
		}
		if rp.DiffDataCenterCount > 0 && dcShardCount[dcOfRack[r]] >= rp.DiffDataCenterCount {
			return false
		}
		return true
	}

	destRack, ok := pickTarget(rackKeys, shardsPerRackType, maxPerRack, anti,
		func(r string) bool {
			return racks[r].freeSlots > 0 && rackHasFreeDiskOfType(racks[r], c.DiskType, c.FilterDiskType)
		},
		withinLimit)
	if !ok {
		return nil, 0, false
	}
	node := pickNodeInRackWithDisk(racks[destRack], vk, rp, c.DiskType, c.FilterDiskType)
	if node == nil {
		return nil, 0, false
	}
	diskID, ok := pickBestDiskOfTypeOnNode(node, vk, c.DiskType, c.FilterDiskType, sid, dataShards)
	if !ok {
		return nil, 0, false
	}
	return node, diskID, true
}

// nodeHasFreeDiskOfType reports whether the node has a free disk matching the
// disk-type request (see diskTypeMatches; filter=false means any type).
func nodeHasFreeDiskOfType(n *Node, diskType string, filter bool) bool {
	for _, d := range n.disks {
		if !diskTypeMatches(d.diskType, diskType, filter) {
			continue
		}
		if d.freeSlots > 0 {
			return true
		}
	}
	return false
}

// rackHasFreeDiskOfType reports whether any node in the rack has a matching free disk.
func rackHasFreeDiskOfType(r *rack, diskType string, filter bool) bool {
	for _, n := range r.nodes {
		if n.freeSlots > 0 && nodeHasFreeDiskOfType(n, diskType, filter) {
			return true
		}
	}
	return false
}

// pickNodeInRackWithDisk is pickNodeInRack restricted to nodes that actually have
// a matching free disk. FromActiveTopology keeps all disk types in the snapshot,
// so without this a node with free volume slots but no disk of the requested type
// could be chosen.
func pickNodeInRackWithDisk(r *rack, vk volKey, rp *super_block.ReplicaPlacement, diskType string, filter bool) *Node {
	var best *Node
	bestCount := -1
	for _, id := range sortedNodeKeys(r.nodes) {
		node := r.nodes[id]
		if node.freeSlots <= 0 {
			continue
		}
		if !nodeHasFreeDiskOfType(node, diskType, filter) {
			continue
		}
		count := volumeShardCount(node, vk)
		if rp != nil && rp.SameRackCount > 0 && count >= rp.SameRackCount+1 {
			continue
		}
		if best == nil || count < bestCount {
			best, bestCount = node, count
		}
	}
	return best
}

// pickBestDiskOfTypeOnNode is pickBestDiskOnNode with a strict disk-type filter
// (so a HardDriveType request does not fall through to other tiers) and an
// explicit found result. Returns ok=false when the node has no matching disk with
// capacity.
func pickBestDiskOfTypeOnNode(node *Node, vk volKey, diskType string, filter bool, shardID, dataShardCount int) (uint32, bool) {
	isDataShard := dataShardCount > 0 && shardID < dataShardCount
	info := node.shards[vk]
	var bestDiskID uint32
	bestScore := -1
	for _, diskID := range sortedDiskKeys(node.disks) {
		d := node.disks[diskID]
		if !diskTypeMatches(d.diskType, diskType, filter) {
			continue
		}
		if d.freeSlots <= 0 {
			continue
		}
		existingShards := 0
		hasData := false
		hasParity := false
		if info != nil {
			bits := info.diskShardBits[diskID]
			existingShards = bits.Count()
			if dataShardCount > 0 {
				for s := 0; s < erasure_coding.MaxShardCount; s++ {
					if !bits.Has(erasure_coding.ShardId(s)) {
						continue
					}
					if s < dataShardCount {
						hasData = true
					} else {
						hasParity = true
					}
				}
			}
		}
		score := d.shardCount*10 + existingShards*100
		if dataShardCount > 0 {
			if isDataShard && hasParity {
				score += 1000
			} else if !isDataShard && hasData {
				score += 1000
			}
		}
		if bestScore == -1 || score < bestScore {
			bestScore = score
			bestDiskID = diskID
		}
	}
	return bestDiskID, bestScore != -1
}

// clearShardAccounting removes one shard copy of a volume from the snapshot's
// per-domain accounting (the volume's shard bits) WITHOUT crediting disk capacity.
// It clears only the given physical disk's bit, then recomputes the node-level
// union from the remaining disk bits, so a kept copy of the same shard on another
// disk of the same node still counts toward caps / ReplicaPlacement / anti-affinity.
//
// Repair uses this to drop the duplicate/mismatched copies it plans to delete
// before placing missing shards, so those copies do not inflate placement
// accounting. Capacity is deliberately NOT credited: the deletes run only after
// the rebuilt shards are distributed, so the slots are not free at plan time. This
// is distinct from releaseShard, which credits freeSlots and clears the union.
func clearShardAccounting(node *Node, vk volKey, shardID int, diskID uint32) {
	info, ok := node.shards[vk]
	if !ok {
		return
	}
	sid := erasure_coding.ShardId(shardID)
	if bits, ok := info.diskShardBits[diskID]; ok {
		info.diskShardBits[diskID] = bits.Clear(sid)
	}
	var union erasure_coding.ShardBits
	for _, b := range info.diskShardBits {
		union |= b
	}
	info.shardBits = union
}

// shardsOfType returns the sorted subset of need that are data shards (id <
// dataShards) when isData, else the parity subset.
func shardsOfType(need []int, isData bool, dataShards int) []int {
	var out []int
	for _, s := range need {
		if (s < dataShards) == isData {
			out = append(out, s)
		}
	}
	sort.Ints(out)
	return out
}
