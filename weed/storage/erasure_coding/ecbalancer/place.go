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
// DiskTypePolicy controls how DiskType constrains placement (Any / Prefer /
// Require). PreferredTags drives whole-plan tag tiering: Place tries disks
// carrying the earliest tags first and widens to all disks only if a tier cannot
// place every shard.
type Constraints struct {
	DiskType         string
	DiskTypePolicy   DiskTypePolicy
	PreferredTags    []string
	ReplicaPlacement *super_block.ReplicaPlacement
	Ratio            func(collection string) (dataShards, parityShards int)
}

// DiskTypePolicy controls how Constraints.DiskType constrains placement.
type DiskTypePolicy int

const (
	DiskTypeAny     DiskTypePolicy = iota // any disk type
	DiskTypePrefer                        // prefer DiskType, spill to other types if needed
	DiskTypeRequire                       // only DiskType (HardDriveType when "")
)

// diskTypeEqual compares disk types after normalization, so "" and "hdd" (both
// HardDriveType) are equal.
func diskTypeEqual(a, b string) bool {
	return storagetypes.ToDiskType(a).String() == storagetypes.ToDiskType(b).String()
}

// diskHasAnyTag reports whether the disk carries any of the given tags.
func diskHasAnyTag(d *disk, tags []string) bool {
	for _, want := range tags {
		for _, have := range d.tags {
			if have == want {
				return true
			}
		}
	}
	return false
}

// Destination is a chosen target for one shard.
type Destination struct {
	Node   string
	DiskID uint32
	Rack   string // composite "dc:rack"
}

// PlaceResult holds the chosen destinations, which constraints had to be relaxed
// (durability-first only), and whether placement spilled outside the preferred
// disk type or tag tiers (for parity with today's logging).
type PlaceResult struct {
	Destinations                map[int]Destination
	Relaxed                     []string
	SpilledToOtherDiskType      bool
	SpilledOutsidePreferredTags bool
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
// shards).
//
// Tag tiering (whole-plan retry): it tries the preferred-tag tiers in order, each
// a complete candidate set, and returns the first tier that places every shard;
// only when it falls through to the no-tag tier does it set
// SpilledOutsidePreferredTags. Within a tier, disk-type Prefer spills to other
// types per shard (SpilledToOtherDiskType); Require filters strictly.
func (t *Topology) Place(vid uint32, collection string, need []int, c Constraints, mode PlacementMode) (*PlaceResult, error) {
	if len(need) == 0 {
		return &PlaceResult{Destinations: map[int]Destination{}}, nil
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
	dcOfRack := make(map[string]string, len(racks))
	for _, n := range t.nodes {
		dcOfRack[n.rack] = n.dc
	}

	// Disk-type eligibility (Require filters; Any/Prefer admit all) and the soft
	// type preference applied in scoring under Prefer.
	typeEligible := func(d *disk) bool {
		if c.DiskTypePolicy == DiskTypeRequire {
			return diskTypeEqual(d.diskType, c.DiskType)
		}
		return true
	}
	var prefer func(*disk) bool
	if c.DiskTypePolicy == DiskTypePrefer {
		prefer = func(d *disk) bool { return diskTypeEqual(d.diskType, c.DiskType) }
	}

	// Whole-plan retry over preferred-tag tiers; the first tier that places every
	// shard wins. Reaching the no-tag tier means we spilled outside the tags.
	tiers := tagTiers(c.PreferredTags)
	var lastErr error
	for _, tierTags := range tiers {
		tt := tierTags
		eligible := func(d *disk) bool {
			return typeEligible(d) && (len(tt) == 0 || diskHasAnyTag(d, tt))
		}
		res, err := t.tryPlace(vk, need, dataShards, parityShards, racks, rackKeys, dcOfRack, mode, c.ReplicaPlacement, eligible, prefer)
		if err != nil {
			lastErr = err
			continue
		}
		if len(c.PreferredTags) > 0 && len(tierTags) == 0 {
			res.SpilledOutsidePreferredTags = true
		}
		return res, nil
	}
	return nil, lastErr
}

// tagTiers returns the eligibility tag-sets in increasing breadth, ending with an
// empty set ("any disk"). Empty preferredTags yields a single any-disk tier.
func tagTiers(preferredTags []string) [][]string {
	if len(preferredTags) == 0 {
		return [][]string{nil}
	}
	tiers := make([][]string, 0, len(preferredTags)+1)
	for k := range preferredTags {
		tiers = append(tiers, append([]string(nil), preferredTags[:k+1]...))
	}
	return append(tiers, nil)
}

// tryPlace runs one whole-plan placement attempt restricted to disks satisfying
// `eligible`, with `prefer` (may be nil) ranking soft-preferred disks first. It
// journals reservations and rolls them all back if any shard cannot be placed, so
// a failed tier leaves the snapshot unchanged for the next attempt.
func (t *Topology) tryPlace(vk volKey, need []int, dataShards, parityShards int, racks map[string]*rack, rackKeys []string, dcOfRack map[string]string, mode PlacementMode, rp *super_block.ReplicaPlacement, eligible func(*disk) bool, prefer func(*disk) bool) (*PlaceResult, error) {
	result := &PlaceResult{Destinations: make(map[int]Destination, len(need))}

	// Per-type shard ids per rack (even caps), total shard count per rack
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
		for sid := range info.shardBits.All() {
			s := int(sid)
			isData := s < dataShards
			shardsPerRack[isData][n.rack] = append(shardsPerRack[isData][n.rack], s)
			rackShardCount[n.rack]++
			dcShardCount[n.dc]++
			bearing[isData][n.rack] = true
		}
	}

	// Even per-rack caps divide by racks that actually have an eligible free disk,
	// not all racks (the snapshot keeps every disk type/tag), so a valid tiered
	// cluster — e.g. SSDs in only 2 of 4 racks — is not capped impossibly low.
	numEligibleRacks := 0
	for _, rk := range rackKeys {
		if rackHasFreeDisk(racks[rk], eligible) {
			numEligibleRacks++
		}
	}
	if numEligibleRacks < 1 {
		numEligibleRacks = 1
	}

	attempts := strictAttempts
	if mode == PlaceDurabilityFirst {
		attempts = durabilityAttempts
	}

	var journal []placedEntry
	relaxedSeen := map[string]bool{}
	spilledType := false

	placeShard := func(sid int, isData bool) bool {
		typeTotal := dataShards
		if !isData {
			typeTotal = parityShards
		}
		for _, rl := range attempts {
			node, diskID, spilled, ok := chooseShardDest(vk, sid, isData, dataShards, typeTotal, numEligibleRacks, racks, rackKeys, rp, eligible, prefer, shardsPerRack[isData], rackShardCount, dcShardCount, dcOfRack, bearing, rl)
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
			if spilled {
				spilledType = true
			}
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
			return nil, fmt.Errorf("cannot place EC shard %d of volume %d (collection %q)", sid, vk.vid, vk.collection)
		}
	}

	result.SpilledToOtherDiskType = spilledType
	for name := range relaxedSeen {
		result.Relaxed = append(result.Relaxed, name)
	}
	sort.Strings(result.Relaxed)
	return result, nil
}

// chooseShardDest selects a (node, disk) for one shard at the given relaxation
// level: pick a rack (even per-type cap + ReplicaPlacement caps + two-pass
// anti-affinity to the opposite type), then the least-loaded eligible node, then
// the best eligible disk. The third return reports whether the disk spilled off
// the soft-preferred type. ok=false when no rack/node/disk fits.
func chooseShardDest(vk volKey, sid int, isData bool, dataShards, typeTotal, numEligibleRacks int, racks map[string]*rack, rackKeys []string, rp *super_block.ReplicaPlacement, eligible func(*disk) bool, prefer func(*disk) bool, shardsPerRackType map[string][]int, rackShardCount, dcShardCount map[string]int, dcOfRack map[string]string, bearing map[bool]map[string]bool, rl relaxation) (*Node, uint32, bool, bool) {
	maxPerRack := numEligibleRacks*typeTotal + 1 // effectively unlimited when caps are relaxed
	if rl.caps {
		if maxPerRack = ceilDivide(typeTotal, numEligibleRacks); maxPerRack < 1 {
			maxPerRack = 1
		}
	}

	var anti map[string]bool
	if rl.antiAffinity {
		anti = bearing[!isData] // racks already holding the opposite shard type
	}

	if !rl.rp {
		rp = nil
	}
	// A rack is eligible only if it is under both the per-rack (DiffRackCount) and
	// per-DC (DiffDataCenterCount) shard caps. The DC cap spreads shards across data
	// centers; it is enforced only when set, so non-DC placements are unchanged.
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
		func(r string) bool { return racks[r].freeSlots > 0 && rackHasFreeDisk(racks[r], eligible) },
		withinLimit)
	if !ok {
		return nil, 0, false, false
	}
	node := pickNodeInRackEligible(racks[destRack], vk, rp, eligible)
	if node == nil {
		return nil, 0, false, false
	}
	diskID, ok, spilled := pickBestDiskEligible(node, vk, eligible, prefer, sid, dataShards)
	if !ok {
		return nil, 0, false, false
	}
	return node, diskID, spilled, true
}

// nodeHasFreeDisk reports whether the node has a free disk satisfying eligible.
func nodeHasFreeDisk(n *Node, eligible func(*disk) bool) bool {
	for _, d := range n.disks {
		if d.freeSlots > 0 && eligible(d) {
			return true
		}
	}
	return false
}

// rackHasFreeDisk reports whether any node in the rack has a free eligible disk.
func rackHasFreeDisk(r *rack, eligible func(*disk) bool) bool {
	for _, n := range r.nodes {
		if n.freeSlots > 0 && nodeHasFreeDisk(n, eligible) {
			return true
		}
	}
	return false
}

// pickNodeInRackEligible is pickNodeInRack restricted to nodes that have a free
// eligible disk. FromActiveTopology keeps all disk types/tags in the snapshot, so
// without this a node with free volume slots but no eligible disk could be chosen.
func pickNodeInRackEligible(r *rack, vk volKey, rp *super_block.ReplicaPlacement, eligible func(*disk) bool) *Node {
	var best *Node
	bestCount := -1
	for _, id := range sortedNodeKeys(r.nodes) {
		node := r.nodes[id]
		if node.freeSlots <= 0 {
			continue
		}
		if !nodeHasFreeDisk(node, eligible) {
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

// pickBestDiskEligible chooses the best eligible disk on a node, ranking
// soft-preferred disks (prefer != nil && prefer(d)) ahead of others so disk-type
// Prefer uses the preferred type when available but spills otherwise. Returns the
// disk id, whether one was found, and whether the chosen disk spilled off the
// preferred type.
func pickBestDiskEligible(node *Node, vk volKey, eligible func(*disk) bool, prefer func(*disk) bool, shardID, dataShardCount int) (uint32, bool, bool) {
	isDataShard := dataShardCount > 0 && shardID < dataShardCount
	info := node.shards[vk]
	var bestDiskID uint32
	bestScore := -1
	bestPreferred := false
	for _, diskID := range sortedDiskKeys(node.disks) {
		d := node.disks[diskID]
		if !eligible(d) || d.freeSlots <= 0 {
			continue
		}
		existingShards := 0
		hasData := false
		hasParity := false
		if info != nil {
			bits := info.diskShardBits[diskID]
			existingShards = bits.Count()
			if dataShardCount > 0 {
				for sid := range bits.All() {
					if int(sid) < dataShardCount {
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
		preferred := prefer == nil || prefer(d)
		if !preferred {
			score += 100000 // strongly deprioritize spilling to a non-preferred type
		}
		if bestScore == -1 || score < bestScore {
			bestScore = score
			bestDiskID = diskID
			bestPreferred = preferred
		}
	}
	if bestScore == -1 {
		return 0, false, false
	}
	return bestDiskID, true, prefer != nil && !bestPreferred
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

// ClearShardAccounting drops one shard copy of a volume from placement accounting
// without crediting capacity (see clearShardAccounting). Repair calls it for each
// copy it plans to delete before placing missing shards, so those copies do not
// inflate caps/RP/anti-affinity. No-op for an unknown node.
func (t *Topology) ClearShardAccounting(nodeID, collection string, vid uint32, shardID int, diskID uint32) {
	n, ok := t.nodes[nodeID]
	if !ok {
		return
	}
	clearShardAccounting(n, volKey{collection: collection, vid: vid}, shardID, diskID)
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
