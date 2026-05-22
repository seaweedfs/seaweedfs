package ecbalancer

import (
	"sort"

	"github.com/seaweedfs/seaweedfs/weed/storage/erasure_coding"
	"github.com/seaweedfs/seaweedfs/weed/storage/super_block"
)

// detectCrossDCImbalance enforces the ReplicaPlacement data-center limit by moving
// a volume's shards out of any data center holding more than DiffDataCenterCount of
// them into data centers under the cap. It is a no-op unless DiffDataCenterCount is
// set, so non-DC placements (and the balancer's output for them) are unaffected.
//
// Like the cross-rack phase, it mutates the snapshot inline (release on the source,
// reserve on the target) and adjusts rack/node/DC capacity; Plan re-asserts the
// shard bits via applyMovesToTopology.
// boundedMaxPerDC returns the per-DC shard cap to balance toward: the operator's
// DiffDataCenterCount, but tightened so shards spread proportionally across the
// available DCs (ceil(total/numDCs)) instead of merely staying under the cap. The
// proportional target is floored at parityShards — once each DC holds at most
// parityShards a single-DC loss is already recoverable, so spreading tighter only
// adds cross-DC (WAN) traffic for no durability gain. Returns 0 when no DC cap
// applies (rp unset, DiffDataCenterCount 0, or a single DC).
func boundedMaxPerDC(rp *super_block.ReplicaPlacement, dataShards, parityShards, numDCs int) int {
	if rp == nil || rp.DiffDataCenterCount <= 0 || numDCs <= 1 {
		return 0
	}
	limit := rp.DiffDataCenterCount
	target := ceilDivide(dataShards+parityShards, numDCs)
	if target < parityShards {
		target = parityShards
	}
	if target < limit {
		limit = target
	}
	return limit
}

func detectCrossDCImbalance(vk volKey, nodes map[string]*Node, racks map[string]*rack, diskType string, dataShards, parityShards int, rp *super_block.ReplicaPlacement) []*move {
	if rp == nil || rp.DiffDataCenterCount <= 0 {
		return nil
	}

	// Group racks by data center.
	dcOfRack := make(map[string]string, len(racks))
	dcRacks := map[string][]string{}
	for _, rackID := range sortedKeys(racks) {
		for _, n := range racks[rackID].nodes {
			dcOfRack[rackID] = n.dc
			break
		}
		dcRacks[dcOfRack[rackID]] = append(dcRacks[dcOfRack[rackID]], rackID)
	}
	dcKeys := sortedKeys(dcRacks)
	if len(dcKeys) <= 1 {
		return nil
	}
	maxPerDC := boundedMaxPerDC(rp, dataShards, parityShards, len(dcKeys))

	// This volume's shard ids per DC.
	dcShards := map[string][]int{}
	for _, n := range nodes {
		info, ok := n.shards[vk]
		if !ok {
			continue
		}
		for sid := range info.shardBits.All() {
			dcShards[n.dc] = append(dcShards[n.dc], int(sid))
		}
	}

	// Shards to evict from over-cap DCs (lowest ids first, deterministic).
	type pending struct {
		shardID int
		src     *Node
	}
	var toMove []pending
	for _, dc := range dcKeys {
		excess := len(dcShards[dc]) - maxPerDC
		if excess <= 0 {
			continue
		}
		shards := append([]int(nil), dcShards[dc]...)
		sort.Ints(shards)
		for i := 0; i < excess && i < len(shards); i++ {
			if src := dcNodeHoldingShard(racks, dcRacks[dc], vk, shards[i]); src != nil {
				toMove = append(toMove, pending{shards[i], src})
			}
		}
	}
	if len(toMove) == 0 {
		return nil
	}

	rackShardCount := countShardsByRack(vk, nodes)
	// No per-rack cap unless ReplicaPlacement sets one. A volume has at most
	// MaxShardCount shards, so MaxShardCount+1 is effectively unlimited and lets a
	// DC's single rack absorb that DC's whole allocation (len(racks)+1 was too
	// small: a 2-rack cluster wrongly capped each rack at 3).
	rackCap := erasure_coding.MaxShardCount + 1
	if rp.DiffRackCount > 0 {
		rackCap = rp.DiffRackCount
	}
	// Balance uses the same convention as pickBestDiskOnNode: a non-empty disk type
	// is a filter, "" means any (the balance snapshot is pre-filtered by type).
	eligible := func(d *disk) bool {
		return diskType == "" || diskTypeEqual(d.diskType, diskType)
	}

	var moves []*move
	for _, pm := range toMove {
		// Target DC: fewest shards of this volume, under the DC cap, with room.
		destDC, ok := pickTarget(dcKeys, dcShards, maxPerDC, nil,
			func(dc string) bool { return dcHasFreeDisk(racks, dcRacks[dc], eligible) },
			func(string) bool { return true })
		if !ok {
			continue
		}
		// Target rack within the DC: fewest shards, under the per-rack cap.
		destRack, ok := pickTarget(dcRacks[destDC], shardsPerRackList(vk, racks, dcRacks[destDC]), rackCap, nil,
			func(r string) bool {
				return racks[r].freeSlots > 0 && rackHasFreeDisk(racks[r], eligible)
			},
			func(r string) bool {
				if rp.DiffRackCount > 0 {
					return rackShardCount[r] < rp.DiffRackCount
				}
				return true
			})
		if !ok {
			continue
		}
		destNode := pickNodeInRackEligible(racks[destRack], vk, rp, eligible)
		if destNode == nil {
			continue
		}
		destDisk, ok, _ := pickBestDiskEligible(destNode, vk, eligible, nil, pm.shardID, dataShards)
		if !ok {
			continue
		}
		moves = append(moves, &move{
			volumeID:   vk.vid,
			shardID:    pm.shardID,
			collection: vk.collection,
			source:     pm.src,
			sourceDisk: shardDiskID(pm.src, vk, pm.shardID),
			target:     destNode,
			targetDisk: destDisk,
			phase:      "cross_dc",
		})
		releaseShard(pm.src, vk, pm.shardID)
		reserveShard(destNode, vk, pm.shardID, destDisk)
		srcDC := pm.src.dc
		srcRack := pm.src.rack
		dcShards[destDC] = append(dcShards[destDC], pm.shardID)
		dcShards[srcDC] = removeInt(dcShards[srcDC], pm.shardID)
		rackShardCount[destRack]++
		rackShardCount[srcRack]--
		racks[destRack].freeSlots--
		racks[srcRack].freeSlots++
		destNode.freeSlots--
		pm.src.freeSlots++
	}
	return moves
}

// dcNodeHoldingShard returns a node in the given DC's racks that holds the shard.
func dcNodeHoldingShard(racks map[string]*rack, rackKeys []string, vk volKey, shardID int) *Node {
	sid := erasure_coding.ShardId(shardID)
	for _, rk := range rackKeys {
		for _, id := range sortedNodeKeys(racks[rk].nodes) {
			n := racks[rk].nodes[id]
			if info, ok := n.shards[vk]; ok && info.shardBits.Has(sid) {
				return n
			}
		}
	}
	return nil
}

// dcHasFreeDisk reports whether any rack in the DC has a node with a free eligible
// disk.
func dcHasFreeDisk(racks map[string]*rack, rackKeys []string, eligible func(*disk) bool) bool {
	for _, rk := range rackKeys {
		if racks[rk].freeSlots > 0 && rackHasFreeDisk(racks[rk], eligible) {
			return true
		}
	}
	return false
}

// shardsPerRackList returns this volume's shard ids per rack for the given racks.
func shardsPerRackList(vk volKey, racks map[string]*rack, rackKeys []string) map[string][]int {
	out := map[string][]int{}
	for _, rk := range rackKeys {
		for _, n := range racks[rk].nodes {
			info, ok := n.shards[vk]
			if !ok {
				continue
			}
			for sid := range info.shardBits.All() {
				out[rk] = append(out[rk], int(sid))
			}
		}
	}
	return out
}
