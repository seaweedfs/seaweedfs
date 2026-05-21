package ec_balance

import (
	"context"
	"fmt"
	"math"
	"sort"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/worker_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage/erasure_coding"
	"github.com/seaweedfs/seaweedfs/weed/storage/super_block"
	"github.com/seaweedfs/seaweedfs/weed/util/wildcard"
	"github.com/seaweedfs/seaweedfs/weed/worker/tasks/base"
	"github.com/seaweedfs/seaweedfs/weed/worker/types"
)

// ecNodeInfo represents a volume server with EC shard information for detection
type ecNodeInfo struct {
	nodeID    string
	address   string
	dc        string
	rack      string // dc:rack composite key
	freeSlots int
	// volumeID -> shardBits (bitmask of shard IDs present on this node)
	ecShards map[uint32]*ecVolumeInfo
	// physical disk_id -> per-disk capacity/occupancy, used to pick a good
	// destination disk for a moved shard (see pickBestDiskOnNode).
	disks map[uint32]*ecDiskInfo
}

// ecDiskInfo is a per-physical-disk view of a node. Same-type disks collapse
// into one DiskInfo on the wire, so free capacity is only known per node; it is
// split evenly across the node's physical disks (matching the shell ec.balance
// heuristic). ecShardCount is the exact total of EC shards on the disk.
type ecDiskInfo struct {
	diskID       uint32
	diskType     string
	freeSlots    int // approximate per-disk free EC slots
	ecShardCount int // total EC shards on this disk, across all volumes
}

type ecVolumeInfo struct {
	collection string
	shardBits  erasure_coding.ShardBits // every shard of this volume on the node (all disks)
	// diskShardBits maps physical disk_id -> the shards of this volume on that
	// disk. A node keys its DiskInfos by disk type, so several physical disks
	// collapse into one DiskInfo; this preserves which disk actually holds each
	// shard so a move reports the correct source disk.
	diskShardBits map[uint32]erasure_coding.ShardBits
}

// ecRackInfo represents a rack with EC node information
type ecRackInfo struct {
	nodes     map[string]*ecNodeInfo
	freeSlots int
}

// shardMove represents a proposed EC shard move
type shardMove struct {
	volumeID   uint32
	shardID    int
	collection string
	source     *ecNodeInfo
	sourceDisk uint32
	target     *ecNodeInfo
	targetDisk uint32
	phase      string // "dedup", "cross_rack", "within_rack", "global"
}

// Detection implements the multi-phase EC shard balance detection algorithm.
// It analyzes EC shard distribution and proposes moves to achieve even distribution.
func Detection(
	ctx context.Context,
	metrics []*types.VolumeHealthMetrics,
	clusterInfo *types.ClusterInfo,
	config base.TaskConfig,
	maxResults int,
) ([]*types.TaskDetectionResult, bool, error) {
	if !config.IsEnabled() {
		return nil, false, nil
	}

	ecConfig := config.(*Config)
	if maxResults < 0 {
		maxResults = 0
	}

	if clusterInfo == nil || clusterInfo.ActiveTopology == nil {
		return nil, false, fmt.Errorf("active topology not available for EC balance detection")
	}

	topoInfo := clusterInfo.ActiveTopology.GetTopologyInfo()
	if topoInfo == nil {
		return nil, false, fmt.Errorf("topology info not available")
	}

	// Build EC topology view
	nodes, racks := buildECTopology(topoInfo, ecConfig)

	if len(nodes) < ecConfig.MinServerCount {
		glog.V(1).Infof("EC balance: only %d servers, need at least %d", len(nodes), ecConfig.MinServerCount)
		return nil, false, nil
	}

	// Collect all EC volumes grouped by collection
	collections := collectECCollections(nodes, ecConfig)
	if len(collections) == 0 {
		glog.V(1).Infof("EC balance: no EC volumes found matching filters")
		return nil, false, nil
	}

	threshold := ecConfig.ImbalanceThreshold
	var allMoves []*shardMove

	// Optional EC shard replica placement constraint (empty = even spread).
	var replicaPlacement *super_block.ReplicaPlacement
	if ecConfig.ReplicaPlacement != "" {
		rp, rpErr := super_block.NewReplicaPlacementFromString(ecConfig.ReplicaPlacement)
		if rpErr != nil {
			glog.Warningf("EC balance: ignoring invalid replica_placement %q: %v", ecConfig.ReplicaPlacement, rpErr)
		} else if rp.HasReplication() {
			replicaPlacement = rp
		}
	}

	// Build set of allowed collections for global phase filtering, and resolve
	// each collection's EC ratio once (data/parity boundary for the two-pass
	// balancing and disk anti-affinity).
	allowedVids := make(map[uint32]bool)
	dataShardsByCollection := make(map[string]int)
	for collection, volumeIDs := range collections {
		dataShards, _ := resolveECRatio(clusterInfo, collection)
		dataShardsByCollection[collection] = dataShards
		for _, vid := range volumeIDs {
			allowedVids[vid] = true
		}
	}

	for collection, volumeIDs := range collections {
		if ctx != nil {
			if err := ctx.Err(); err != nil {
				return nil, false, err
			}
		}

		dataShards, parityShards := resolveECRatio(clusterInfo, collection)

		// Phase 1: Detect duplicate shards (always run, duplicates are errors not imbalance)
		for _, vid := range volumeIDs {
			moves := detectDuplicateShards(vid, collection, nodes, ecConfig.DiskType)
			applyMovesToTopology(moves)
			allMoves = append(allMoves, moves...)
		}

		// Phase 2: Balance shards across racks (operates on updated topology from phase 1)
		for _, vid := range volumeIDs {
			moves := detectCrossRackImbalance(vid, collection, nodes, racks, ecConfig.DiskType, threshold, dataShards, parityShards, replicaPlacement)
			applyMovesToTopology(moves)
			allMoves = append(allMoves, moves...)
		}

		// Phase 3: Balance shards within racks (operates on updated topology from phases 1-2)
		for _, vid := range volumeIDs {
			moves := detectWithinRackImbalance(vid, collection, nodes, racks, ecConfig.DiskType, threshold, dataShards, parityShards, replicaPlacement)
			applyMovesToTopology(moves)
			allMoves = append(allMoves, moves...)
		}
	}

	// Phase 4: Global node balance across racks (only for volumes in allowed collections)
	globalMoves := detectGlobalImbalance(nodes, racks, ecConfig, allowedVids, dataShardsByCollection)
	allMoves = append(allMoves, globalMoves...)

	// Cap results
	hasMore := false
	if maxResults > 0 && len(allMoves) > maxResults {
		allMoves = allMoves[:maxResults]
		hasMore = true
	}

	// Convert moves to TaskDetectionResults
	now := time.Now()
	results := make([]*types.TaskDetectionResult, 0, len(allMoves))
	for i, move := range allMoves {
		// Include loop index and source/target in TaskID for uniqueness
		taskID := fmt.Sprintf("ec_balance_%d_%d_%s_%s_%d_%d",
			move.volumeID, move.shardID,
			move.source.nodeID, move.target.nodeID,
			now.UnixNano(), i)

		result := &types.TaskDetectionResult{
			TaskID:     taskID,
			TaskType:   types.TaskTypeECBalance,
			VolumeID:   move.volumeID,
			Server:     move.source.nodeID,
			Collection: move.collection,
			Priority:   movePhasePriority(move.phase),
			Reason: fmt.Sprintf("EC shard %d.%d %s: %s → %s (%s)",
				move.volumeID, move.shardID, move.phase,
				move.source.nodeID, move.target.nodeID, move.phase),
			ScheduleAt: now,
			TypedParams: &worker_pb.TaskParams{
				TaskId:     taskID,
				VolumeId:   move.volumeID,
				Collection: move.collection,
				Sources: []*worker_pb.TaskSource{{
					Node:     move.source.address,
					DiskId:   move.sourceDisk,
					Rack:     move.source.rack,
					ShardIds: []uint32{uint32(move.shardID)},
				}},
				Targets: []*worker_pb.TaskTarget{{
					Node:     move.target.address,
					DiskId:   move.targetDisk,
					Rack:     move.target.rack,
					ShardIds: []uint32{uint32(move.shardID)},
				}},
				TaskParams: &worker_pb.TaskParams_EcBalanceParams{
					EcBalanceParams: &worker_pb.EcBalanceTaskParams{
						DiskType:       ecConfig.DiskType,
						TimeoutSeconds: 600,
					},
				},
			},
		}
		results = append(results, result)
	}

	glog.V(1).Infof("EC balance detection: %d moves proposed across %d collections",
		len(results), len(collections))

	return results, hasMore, nil
}

// buildECTopology constructs EC node and rack structures from topology info.
// Rack keys are dc:rack composites to avoid cross-DC name collisions.
// Only racks with eligible nodes (matching disk type, having EC shards or capacity) are included.
func buildECTopology(topoInfo *master_pb.TopologyInfo, config *Config) (map[string]*ecNodeInfo, map[string]*ecRackInfo) {
	nodes := make(map[string]*ecNodeInfo)
	racks := make(map[string]*ecRackInfo)

	for _, dc := range topoInfo.DataCenterInfos {
		if config.DataCenterFilter != "" {
			matchers := wildcard.CompileWildcardMatchers(config.DataCenterFilter)
			if !wildcard.MatchesAnyWildcard(matchers, dc.Id) {
				continue
			}
		}

		for _, rack := range dc.RackInfos {
			// Use dc:rack composite key to avoid cross-DC name collisions
			rackKey := dc.Id + ":" + rack.Id

			for _, dn := range rack.DataNodeInfos {
				node := &ecNodeInfo{
					nodeID:   dn.Id,
					address:  dn.Id,
					dc:       dc.Id,
					rack:     rackKey,
					ecShards: make(map[uint32]*ecVolumeInfo),
					disks:    make(map[uint32]*ecDiskInfo),
				}

				hasMatchingDisk := false
				for diskType, diskInfo := range dn.DiskInfos {
					if config.DiskType != "" && diskType != config.DiskType {
						continue
					}
					hasMatchingDisk = true

					freeSlots := int(diskInfo.MaxVolumeCount-diskInfo.VolumeCount)*erasure_coding.DataShardsCount - countEcShards(diskInfo.EcShardInfos)
					if freeSlots > 0 {
						node.freeSlots += freeSlots
					}

					ensureDisk := func(diskID uint32) *ecDiskInfo {
						d, ok := node.disks[diskID]
						if !ok {
							d = &ecDiskInfo{diskID: diskID, diskType: diskType}
							node.disks[diskID] = d
						}
						return d
					}
					// Discover physical disks from regular volumes too, so an
					// EC-empty disk is still a candidate destination.
					for _, vi := range diskInfo.VolumeInfos {
						ensureDisk(vi.DiskId)
					}

					for _, ecShardInfo := range diskInfo.EcShardInfos {
						ensureDisk(ecShardInfo.DiskId).ecShardCount += erasure_coding.GetShardCount(ecShardInfo)

						vid := ecShardInfo.Id
						existing, ok := node.ecShards[vid]
						if !ok {
							existing = &ecVolumeInfo{
								collection:    ecShardInfo.Collection,
								diskShardBits: make(map[uint32]erasure_coding.ShardBits),
							}
							node.ecShards[vid] = existing
						}
						bits := erasure_coding.ShardBits(ecShardInfo.EcIndexBits)
						existing.shardBits |= bits
						existing.diskShardBits[ecShardInfo.DiskId] |= bits
					}
				}

				if !hasMatchingDisk {
					continue
				}

				// The wire collapses same-type disks, so per-disk free capacity
				// is unknown; split the node total evenly across its disks.
				if diskCount := len(node.disks); diskCount > 0 && node.freeSlots > 0 {
					perDisk := node.freeSlots / diskCount
					for _, d := range node.disks {
						d.freeSlots = perDisk
					}
				}

				nodes[dn.Id] = node

				// Only create rack entry when we have an eligible node
				if _, ok := racks[rackKey]; !ok {
					racks[rackKey] = &ecRackInfo{nodes: make(map[string]*ecNodeInfo)}
				}
				racks[rackKey].nodes[dn.Id] = node
				racks[rackKey].freeSlots += node.freeSlots
			}
		}
	}

	return nodes, racks
}

// collectECCollections groups EC volume IDs by collection, applying filters
func collectECCollections(nodes map[string]*ecNodeInfo, config *Config) map[string][]uint32 {
	allowedCollections := wildcard.CompileWildcardMatchers(config.CollectionFilter)

	// Collect unique volume IDs per collection
	collectionVids := make(map[string]map[uint32]bool)
	for _, node := range nodes {
		for vid, info := range node.ecShards {
			if len(allowedCollections) > 0 && !wildcard.MatchesAnyWildcard(allowedCollections, info.collection) {
				continue
			}
			if _, ok := collectionVids[info.collection]; !ok {
				collectionVids[info.collection] = make(map[uint32]bool)
			}
			collectionVids[info.collection][vid] = true
		}
	}

	// Convert to sorted slices
	result := make(map[string][]uint32, len(collectionVids))
	for collection, vids := range collectionVids {
		vidSlice := make([]uint32, 0, len(vids))
		for vid := range vids {
			vidSlice = append(vidSlice, vid)
		}
		sort.Slice(vidSlice, func(i, j int) bool { return vidSlice[i] < vidSlice[j] })
		result[collection] = vidSlice
	}

	return result
}

// detectDuplicateShards finds shards that exist on multiple nodes.
// Duplicates are always returned regardless of threshold since they are data errors.
func detectDuplicateShards(vid uint32, collection string, nodes map[string]*ecNodeInfo, diskType string) []*shardMove {
	// Build shard -> list of nodes mapping
	shardLocations := make(map[int][]*ecNodeInfo)
	for _, node := range nodes {
		info, ok := node.ecShards[vid]
		if !ok {
			continue
		}
		for shardID := 0; shardID < erasure_coding.MaxShardCount; shardID++ {
			if info.shardBits.Has(erasure_coding.ShardId(shardID)) {
				shardLocations[shardID] = append(shardLocations[shardID], node)
			}
		}
	}

	var moves []*shardMove
	for shardID, locs := range shardLocations {
		if len(locs) <= 1 {
			continue
		}
		// Keep the copy on the node with most free slots (ascending sort, keep last)
		sort.Slice(locs, func(i, j int) bool { return locs[i].freeSlots < locs[j].freeSlots })

		// Propose deletion of all other copies (skip the keeper at the end).
		// Set target=source so isDedupPhase() recognizes this as unmount+delete only.
		for _, node := range locs[:len(locs)-1] {
			moves = append(moves, &shardMove{
				volumeID:   vid,
				shardID:    shardID,
				collection: collection,
				source:     node,
				sourceDisk: ecShardDiskIDForShard(node, vid, shardID),
				target:     node,
				targetDisk: ecShardDiskIDForShard(node, vid, shardID),
				phase:      "dedup",
			})
		}
	}

	return moves
}

// detectCrossRackImbalance spreads a volume's shards across racks. It balances
// data shards and parity shards in two separate passes so each type spreads
// evenly, and gives parity shards anti-affinity to racks already holding data
// shards (mirrors the shell ec.balance doBalanceEcShardsAcrossRacks). Returns
// nil if the overall distribution is below the imbalance threshold.
func detectCrossRackImbalance(vid uint32, collection string, nodes map[string]*ecNodeInfo, racks map[string]*ecRackInfo, diskType string, threshold float64, dataShards, parityShards int, rp *super_block.ReplicaPlacement) []*shardMove {
	numRacks := len(racks)
	if numRacks <= 1 {
		return nil
	}

	rackShardCount := countShardsByRackForVolume(vid, nodes)
	totalShards := 0
	for _, c := range rackShardCount {
		totalShards += c
	}
	if totalShards == 0 || !exceedsImbalanceThreshold(rackShardCount, totalShards, numRacks, threshold) {
		return nil
	}

	var moves []*shardMove

	// Pass 1: data shards, no anti-affinity.
	dataPerRack, _ := shardsByGroup(vid, nodes, dataShards, func(n *ecNodeInfo) string { return n.rack })
	moves = append(moves, balanceShardTypeAcrossRacks(vid, collection, nodes, racks, diskType, dataShards,
		dataPerRack, rackShardCount, ceilDivide(dataShards, numRacks), nil, rp)...)

	// Pass 2: parity shards, avoiding racks that hold data shards (recompute from
	// the now-updated placement).
	dataPerRack, parityPerRack := shardsByGroup(vid, nodes, dataShards, func(n *ecNodeInfo) string { return n.rack })
	antiAffinityRacks := make(map[string]bool)
	for rackID, shards := range dataPerRack {
		if len(shards) > 0 {
			antiAffinityRacks[rackID] = true
		}
	}
	moves = append(moves, balanceShardTypeAcrossRacks(vid, collection, nodes, racks, diskType, dataShards,
		parityPerRack, rackShardCount, ceilDivide(parityShards, numRacks), antiAffinityRacks, rp)...)

	return moves
}

// balanceShardTypeAcrossRacks moves the excess shards of one type (data or
// parity, given by shardsPerRack) out of racks holding more than maxPerRack of
// that type, into racks chosen by pickTarget (honoring anti-affinity and the
// replica placement's DiffRackCount). The destination node within the rack and
// the destination disk are chosen for even spread.
func balanceShardTypeAcrossRacks(vid uint32, collection string, nodes map[string]*ecNodeInfo, racks map[string]*ecRackInfo, diskType string, dataShards int, shardsPerRack map[string][]int, rackShardCount map[string]int, maxPerRack int, antiAffinityRacks map[string]bool, rp *super_block.ReplicaPlacement) []*shardMove {
	if maxPerRack < 1 {
		maxPerRack = 1
	}
	rackKeys := sortedRackKeys(racks)

	type pending struct {
		shardID int
		src     *ecNodeInfo
	}
	var toMove []pending
	for _, rackID := range rackKeys {
		shards := append([]int(nil), shardsPerRack[rackID]...)
		if len(shards) <= maxPerRack {
			continue
		}
		sort.Ints(shards)
		for i := 0; i < len(shards)-maxPerRack; i++ {
			if src := nodeInRackHoldingShard(nodes, rackID, vid, shards[i]); src != nil {
				toMove = append(toMove, pending{shards[i], src})
			}
		}
	}

	var moves []*shardMove
	for _, pm := range toMove {
		destRack, ok := pickTarget(rackKeys, shardsPerRack, maxPerRack, antiAffinityRacks,
			func(r string) bool { return racks[r].freeSlots > 0 },
			func(r string) bool {
				if rp != nil && rp.DiffRackCount > 0 {
					return rackShardCount[r] < rp.DiffRackCount
				}
				return true
			})
		if !ok {
			continue
		}
		destNode := pickNodeInRack(racks[destRack], vid, rp)
		if destNode == nil {
			continue
		}
		destDisk := pickBestDiskOnNode(destNode, vid, diskType, pm.shardID, dataShards)
		moves = append(moves, &shardMove{
			volumeID:   vid,
			shardID:    pm.shardID,
			collection: collection,
			source:     pm.src,
			sourceDisk: ecShardDiskIDForShard(pm.src, vid, pm.shardID),
			target:     destNode,
			targetDisk: destDisk,
			phase:      "cross_rack",
		})
		releaseShardFromNode(pm.src, vid, pm.shardID)
		reserveShardOnDisk(destNode, vid, collection, pm.shardID, destDisk)
		srcRack := pm.src.rack
		shardsPerRack[destRack] = append(shardsPerRack[destRack], pm.shardID)
		shardsPerRack[srcRack] = removeInt(shardsPerRack[srcRack], pm.shardID)
		rackShardCount[destRack]++
		rackShardCount[srcRack]--
		racks[destRack].freeSlots--
		racks[srcRack].freeSlots++
	}
	return moves
}

// pickNodeInRack chooses the node in a rack to receive a new shard of the
// volume: the one with free slots and the fewest shards of this volume, honoring
// the replica placement's SameRackCount limit. Deterministic by node id.
func pickNodeInRack(rack *ecRackInfo, vid uint32, rp *super_block.ReplicaPlacement) *ecNodeInfo {
	var best *ecNodeInfo
	bestCount := -1
	for _, id := range sortedNodeKeys(rack.nodes) {
		node := rack.nodes[id]
		if node.freeSlots <= 0 {
			continue
		}
		count := volumeShardCount(node, vid)
		if rp != nil && rp.SameRackCount > 0 && count >= rp.SameRackCount+1 {
			continue
		}
		if best == nil || count < bestCount {
			best, bestCount = node, count
		}
	}
	return best
}

// detectWithinRackImbalance spreads a volume's shards evenly across the nodes of
// each rack. Like the cross-rack phase it runs separate data and parity passes,
// giving parity shards anti-affinity to nodes already holding data shards
// (mirrors the shell ec.balance doBalanceEcShardsWithinOneRack).
func detectWithinRackImbalance(vid uint32, collection string, nodes map[string]*ecNodeInfo, racks map[string]*ecRackInfo, diskType string, threshold float64, dataShards, parityShards int, rp *super_block.ReplicaPlacement) []*shardMove {
	var moves []*shardMove

	for _, rackID := range sortedRackKeys(racks) {
		rack := racks[rackID]
		if len(rack.nodes) <= 1 {
			continue
		}

		nodeShardCount := countShardsByNodeForVolume(vid, rack.nodes)
		totalInRack := 0
		for _, c := range nodeShardCount {
			totalInRack += c
		}
		if totalInRack == 0 || !exceedsImbalanceThreshold(nodeShardCount, totalInRack, len(rack.nodes), threshold) {
			continue
		}
		numNodes := len(rack.nodes)

		// Pass 1: data shards across nodes.
		dataPerNode, _ := shardsByGroup(vid, rack.nodes, dataShards, func(n *ecNodeInfo) string { return n.nodeID })
		moves = append(moves, balanceShardTypeAcrossNodes(vid, collection, rack, diskType, dataShards,
			dataPerNode, nodeShardCount, ceilDivide(sumLens(dataPerNode), numNodes), nil, rp)...)

		// Pass 2: parity shards across nodes, avoiding nodes that hold data shards.
		dataPerNode, parityPerNode := shardsByGroup(vid, rack.nodes, dataShards, func(n *ecNodeInfo) string { return n.nodeID })
		antiAffinityNodes := make(map[string]bool)
		for nodeID, shards := range dataPerNode {
			if len(shards) > 0 {
				antiAffinityNodes[nodeID] = true
			}
		}
		moves = append(moves, balanceShardTypeAcrossNodes(vid, collection, rack, diskType, dataShards,
			parityPerNode, nodeShardCount, ceilDivide(sumLens(parityPerNode), numNodes), antiAffinityNodes, rp)...)
	}

	return moves
}

// balanceShardTypeAcrossNodes moves excess shards of one type out of nodes
// holding more than maxPerNode of it, into nodes chosen by pickTarget (honoring
// anti-affinity and the replica placement's SameRackCount).
func balanceShardTypeAcrossNodes(vid uint32, collection string, rack *ecRackInfo, diskType string, dataShards int, shardsPerNode map[string][]int, nodeShardCount map[string]int, maxPerNode int, antiAffinityNodes map[string]bool, rp *super_block.ReplicaPlacement) []*shardMove {
	if maxPerNode < 1 {
		maxPerNode = 1
	}
	nodeKeys := sortedNodeKeys(rack.nodes)

	type pending struct {
		shardID int
		src     *ecNodeInfo
	}
	var toMove []pending
	for _, nodeID := range nodeKeys {
		shards := append([]int(nil), shardsPerNode[nodeID]...)
		if len(shards) <= maxPerNode {
			continue
		}
		sort.Ints(shards)
		src := rack.nodes[nodeID]
		for i := 0; i < len(shards)-maxPerNode; i++ {
			toMove = append(toMove, pending{shards[i], src})
		}
	}

	var moves []*shardMove
	for _, pm := range toMove {
		destID, ok := pickTarget(nodeKeys, shardsPerNode, maxPerNode, antiAffinityNodes,
			func(n string) bool { return n != pm.src.nodeID && rack.nodes[n].freeSlots > 0 },
			func(n string) bool {
				if rp != nil && rp.SameRackCount > 0 {
					return nodeShardCount[n] < rp.SameRackCount+1
				}
				return true
			})
		if !ok {
			continue
		}
		destNode := rack.nodes[destID]
		destDisk := pickBestDiskOnNode(destNode, vid, diskType, pm.shardID, dataShards)
		moves = append(moves, &shardMove{
			volumeID:   vid,
			shardID:    pm.shardID,
			collection: collection,
			source:     pm.src,
			sourceDisk: ecShardDiskIDForShard(pm.src, vid, pm.shardID),
			target:     destNode,
			targetDisk: destDisk,
			phase:      "within_rack",
		})
		releaseShardFromNode(pm.src, vid, pm.shardID)
		reserveShardOnDisk(destNode, vid, collection, pm.shardID, destDisk)
		shardsPerNode[destID] = append(shardsPerNode[destID], pm.shardID)
		shardsPerNode[pm.src.nodeID] = removeInt(shardsPerNode[pm.src.nodeID], pm.shardID)
		nodeShardCount[destID]++
		nodeShardCount[pm.src.nodeID]--
		pm.src.freeSlots++
		destNode.freeSlots--
	}
	return moves
}

// detectGlobalImbalance detects total shard count imbalance across nodes in each rack.
// Respects ImbalanceThreshold from config. Only considers volumes in allowedVids.
func detectGlobalImbalance(nodes map[string]*ecNodeInfo, racks map[string]*ecRackInfo, config *Config, allowedVids map[uint32]bool, dataShardsByCollection map[string]int) []*shardMove {
	var moves []*shardMove

	for _, rack := range racks {
		if len(rack.nodes) <= 1 {
			continue
		}

		// Count total EC shards per node (only for allowed volumes)
		nodeShardCounts := make(map[string]int)
		totalShards := 0
		for nodeID, node := range rack.nodes {
			count := 0
			for vid, info := range node.ecShards {
				if len(allowedVids) > 0 && !allowedVids[vid] {
					continue
				}
				count += info.shardBits.Count()
			}
			nodeShardCounts[nodeID] = count
			totalShards += count
		}

		if totalShards == 0 {
			continue
		}

		// Snapshot each node's total shard capacity (current shards from allowed
		// volumes plus any remaining free slots). Capacity is fixed for the
		// duration of this loop — moves conserve total shards across the rack,
		// so the denominator does not change as nodeShardCounts shift.
		nodeCapacity := make(map[string]int, len(rack.nodes))
		for nodeID, count := range nodeShardCounts {
			nodeCapacity[nodeID] = count + rack.nodes[nodeID].freeSlots
		}

		// Check if imbalance exceeds threshold using utilization ratios
		// (count/capacity), not raw shard counts. Raw counts would say a
		// cluster is imbalanced whenever a large-capacity node holds more
		// shards than a small-capacity node, even when both are at the
		// same fractional fullness.
		if !exceedsUtilImbalanceThreshold(nodeShardCounts, nodeCapacity, config.ImbalanceThreshold) {
			continue
		}

		// Iteratively move shards from most-utilized to least-utilized
		for i := 0; i < 10; i++ { // cap iterations to avoid infinite loops
			// Find min and max nodes by utilization ratio. Min must have free
			// slots so it can receive a shard; max can be any node with shards
			// (we move shards out of it). Utilization-based selection is
			// critical on heterogeneous racks: a large-capacity node with many
			// shards in absolute terms may still be the LEAST utilized, and
			// moving shards into it from a small, nearly-full node is the
			// correct direction even though raw counts would suggest otherwise.
			var minNode, maxNode *ecNodeInfo
			minUtil := math.Inf(1)
			maxUtil := -1.0
			var minCount, maxCount int
			for nodeID, count := range nodeShardCounts {
				node := rack.nodes[nodeID]
				cap := nodeCapacity[nodeID]
				if cap <= 0 {
					continue
				}
				util := float64(count) / float64(cap)
				if util < minUtil && node.freeSlots > 0 {
					minUtil = util
					minCount = count
					minNode = node
				}
				if util > maxUtil {
					maxUtil = util
					maxCount = count
					maxNode = rack.nodes[nodeID]
				}
			}

			if maxNode == nil || minNode == nil || maxNode.nodeID == minNode.nodeID {
				break
			}

			// Per-move convergence guard: reject any move where the
			// destination's post-move utilization would strictly exceed the
			// source's post-move utilization. This mirrors the guard in
			// weed/worker/tasks/balance/detection.go and terminates the loop
			// once no further beneficial move exists, preventing oscillation
			// and overshoot on heterogeneous racks.
			maxCap := nodeCapacity[maxNode.nodeID]
			minCap := nodeCapacity[minNode.nodeID]
			if maxCap <= 0 || minCap <= 0 {
				break
			}
			newSrcUtil := float64(maxCount-1) / float64(maxCap)
			newDstUtil := float64(minCount+1) / float64(minCap)
			if newDstUtil > newSrcUtil {
				break
			}

			// Pick a shard from maxNode that doesn't already exist on minNode
			moved := false
			for vid, info := range maxNode.ecShards {
				if moved {
					break
				}
				if len(allowedVids) > 0 && !allowedVids[vid] {
					continue
				}
				// Check minNode doesn't have this volume's shards already (avoid same-volume overlap)
				minInfo := minNode.ecShards[vid]
				for shardID := 0; shardID < erasure_coding.TotalShardsCount; shardID++ {
					if !info.shardBits.Has(erasure_coding.ShardId(shardID)) {
						continue
					}
					// Skip if destination already has this shard
					if minInfo != nil && minInfo.shardBits.Has(erasure_coding.ShardId(shardID)) {
						continue
					}

					dataShards := dataShardsByCollection[info.collection]
					if dataShards <= 0 {
						dataShards = erasure_coding.DataShardsCount
					}
					destDisk := pickBestDiskOnNode(minNode, vid, config.DiskType, shardID, dataShards)
					moves = append(moves, &shardMove{
						volumeID:   vid,
						shardID:    shardID,
						collection: info.collection,
						source:     maxNode,
						sourceDisk: ecShardDiskIDForShard(maxNode, vid, shardID),
						target:     minNode,
						targetDisk: destDisk,
						phase:      "global",
					})
					// Update in-memory shard placement so the next iteration of the
					// outer loop sees the move: drop the shard from the source and
					// record it on the chosen destination disk.
					sid := erasure_coding.ShardId(shardID)
					info.shardBits = info.shardBits.Clear(sid)
					for diskID := range info.diskShardBits {
						info.diskShardBits[diskID] = info.diskShardBits[diskID].Clear(sid)
					}
					reserveShardOnDisk(minNode, vid, info.collection, shardID, destDisk)
					nodeShardCounts[maxNode.nodeID]--
					nodeShardCounts[minNode.nodeID]++
					maxNode.freeSlots++
					minNode.freeSlots--
					moved = true
					break
				}
			}
			if !moved {
				break
			}
		}
	}

	return moves
}

// resolveECRatio returns the (dataShards, parityShards) for a collection.
// Custom EC ratios are an enterprise feature; OSS always uses the standard
// scheme, so the collection and cluster snapshot are not consulted here.
func resolveECRatio(_ *types.ClusterInfo, _ string) (int, int) {
	return normalizeECShardCounts(0, 0)
}

func normalizeECShardCounts(dataShards, parityShards int) (int, int) {
	if dataShards <= 0 {
		dataShards = erasure_coding.DataShardsCount
	}
	if parityShards <= 0 {
		parityShards = erasure_coding.ParityShardsCount
	}
	return dataShards, parityShards
}

// shardsByGroup classifies a volume's shards into data (id < dataShards) and
// parity buckets, grouped by key(node). Returns map[groupKey] -> []shardID.
func shardsByGroup(vid uint32, nodes map[string]*ecNodeInfo, dataShards int, key func(*ecNodeInfo) string) (dataPer, parityPer map[string][]int) {
	dataPer = make(map[string][]int)
	parityPer = make(map[string][]int)
	for _, node := range nodes {
		info, ok := node.ecShards[vid]
		if !ok {
			continue
		}
		k := key(node)
		for s := 0; s < erasure_coding.MaxShardCount; s++ {
			if !info.shardBits.Has(erasure_coding.ShardId(s)) {
				continue
			}
			if s < dataShards {
				dataPer[k] = append(dataPer[k], s)
			} else {
				parityPer[k] = append(parityPer[k], s)
			}
		}
	}
	return
}

// pickTarget selects a destination key (rack or node) with room for another
// shard of a type, in two passes: first excluding anti-affinity targets, then
// falling back to any valid target. Among valid targets it prefers the one
// holding the fewest shards of this type; ties break on the (sorted) key order
// of candidates, so selection is deterministic.
func pickTarget(candidates []string, shardsPerTarget map[string][]int, maxPerTarget int, antiAffinity map[string]bool, hasFreeSlots, withinLimit func(string) bool) (string, bool) {
	try := func(skipAnti bool) (string, bool) {
		best := ""
		bestCount := maxPerTarget + 1
		for _, c := range candidates {
			if skipAnti && antiAffinity[c] {
				continue
			}
			if !hasFreeSlots(c) {
				continue
			}
			if len(shardsPerTarget[c]) >= maxPerTarget {
				continue
			}
			if !withinLimit(c) {
				continue
			}
			if cnt := len(shardsPerTarget[c]); cnt < bestCount {
				best, bestCount = c, cnt
			}
		}
		return best, best != ""
	}
	if len(antiAffinity) > 0 {
		if t, ok := try(true); ok {
			return t, true
		}
	}
	return try(false)
}

// releaseShardFromNode removes a shard of the volume from a node's in-memory
// model (shardBits, the holding disk's bits, and that disk's occupancy/free
// slots), so a two-pass run sees the source give up the shard.
func releaseShardFromNode(node *ecNodeInfo, vid uint32, shardID int) {
	info, ok := node.ecShards[vid]
	if !ok {
		return
	}
	sid := erasure_coding.ShardId(shardID)
	for diskID, bits := range info.diskShardBits {
		if bits.Has(sid) {
			info.diskShardBits[diskID] = bits.Clear(sid)
			if disk, ok := node.disks[diskID]; ok {
				disk.ecShardCount--
				disk.freeSlots++
			}
		}
	}
	info.shardBits = info.shardBits.Clear(sid)
}

func volumeShardCount(node *ecNodeInfo, vid uint32) int {
	if info, ok := node.ecShards[vid]; ok {
		return info.shardBits.Count()
	}
	return 0
}

// nodeInRackHoldingShard returns the (deterministically lowest-id) node in the
// rack that holds the given shard of the volume.
func nodeInRackHoldingShard(nodes map[string]*ecNodeInfo, rackID string, vid uint32, shardID int) *ecNodeInfo {
	sid := erasure_coding.ShardId(shardID)
	for _, id := range sortedNodeKeys(nodes) {
		node := nodes[id]
		if node.rack != rackID {
			continue
		}
		if info, ok := node.ecShards[vid]; ok && info.shardBits.Has(sid) {
			return node
		}
	}
	return nil
}

func countShardsByRackForVolume(vid uint32, nodes map[string]*ecNodeInfo) map[string]int {
	m := make(map[string]int)
	for _, node := range nodes {
		if info, ok := node.ecShards[vid]; ok {
			m[node.rack] += info.shardBits.Count()
		}
	}
	return m
}

func countShardsByNodeForVolume(vid uint32, nodes map[string]*ecNodeInfo) map[string]int {
	m := make(map[string]int)
	for id, node := range nodes {
		if info, ok := node.ecShards[vid]; ok {
			m[id] = info.shardBits.Count()
		}
	}
	return m
}

func sortedRackKeys(racks map[string]*ecRackInfo) []string {
	keys := make([]string, 0, len(racks))
	for k := range racks {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

func sortedNodeKeys(nodes map[string]*ecNodeInfo) []string {
	keys := make([]string, 0, len(nodes))
	for k := range nodes {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

func sumLens(m map[string][]int) int {
	total := 0
	for _, v := range m {
		total += len(v)
	}
	return total
}

func removeInt(s []int, v int) []int {
	for i, x := range s {
		if x == v {
			return append(s[:i], s[i+1:]...)
		}
	}
	return s
}

// exceedsImbalanceThreshold checks if the distribution of counts exceeds the threshold.
// numGroups is the total number of groups (including those with 0 shards that aren't in the map).
// imbalanceRatio = (maxCount - minCount) / avgCount
func exceedsImbalanceThreshold(counts map[string]int, total int, numGroups int, threshold float64) bool {
	if numGroups <= 1 || total == 0 {
		return false
	}

	minCount := 0 // groups not in map have 0 shards
	if len(counts) >= numGroups {
		// All groups have entries; find actual min
		minCount = total + 1
		for _, count := range counts {
			if count < minCount {
				minCount = count
			}
		}
	}

	maxCount := -1
	for _, count := range counts {
		if count > maxCount {
			maxCount = count
		}
	}

	avg := float64(total) / float64(numGroups)
	if avg == 0 {
		return false
	}

	imbalanceRatio := float64(maxCount-minCount) / avg
	return imbalanceRatio > threshold
}

// exceedsUtilImbalanceThreshold checks whether the per-node utilization ratio
// (shard count / shard slot capacity) is skewed beyond the given threshold.
// Unlike exceedsImbalanceThreshold, it compares fractional fullness rather
// than raw counts so that racks with heterogeneous MaxVolumeCount are
// evaluated correctly — a large-capacity node holding more shards than a
// small-capacity node is not considered imbalanced if both are at the same
// fractional fullness. Nodes with zero capacity are skipped.
func exceedsUtilImbalanceThreshold(counts map[string]int, capacities map[string]int, threshold float64) bool {
	minUtil := math.Inf(1)
	maxUtil := -1.0
	seen := 0
	for nodeID, count := range counts {
		cap := capacities[nodeID]
		if cap <= 0 {
			continue
		}
		util := float64(count) / float64(cap)
		if util < minUtil {
			minUtil = util
		}
		if util > maxUtil {
			maxUtil = util
		}
		seen++
	}
	if seen < 2 || maxUtil <= 0 {
		return false
	}
	avg := (maxUtil + minUtil) / 2
	if avg == 0 {
		return false
	}
	return (maxUtil-minUtil)/avg > threshold
}

// applyMovesToTopology simulates planned moves on the in-memory topology
// so subsequent detection phases see updated shard placement.
func applyMovesToTopology(moves []*shardMove) {
	for _, move := range moves {
		sid := erasure_coding.ShardId(move.shardID)

		// Remove shard from source, including its per-disk record so a later
		// phase that re-sources this node attributes the right disk.
		if srcInfo, ok := move.source.ecShards[move.volumeID]; ok {
			srcInfo.shardBits = srcInfo.shardBits.Clear(sid)
			for diskID := range srcInfo.diskShardBits {
				srcInfo.diskShardBits[diskID] = srcInfo.diskShardBits[diskID].Clear(sid)
			}
		}

		// For non-dedup moves, add shard to target
		if move.source.nodeID != move.target.nodeID {
			dstInfo, ok := move.target.ecShards[move.volumeID]
			if !ok {
				dstInfo = &ecVolumeInfo{
					collection:    move.collection,
					diskShardBits: make(map[uint32]erasure_coding.ShardBits),
				}
				move.target.ecShards[move.volumeID] = dstInfo
			}
			if dstInfo.diskShardBits == nil {
				dstInfo.diskShardBits = make(map[uint32]erasure_coding.ShardBits)
			}
			dstInfo.shardBits = dstInfo.shardBits.Set(sid)
			dstInfo.diskShardBits[move.targetDisk] = dstInfo.diskShardBits[move.targetDisk].Set(sid)
		}
	}
}

// Helper functions

func countEcShards(ecShardInfos []*master_pb.VolumeEcShardInformationMessage) int {
	count := 0
	for _, eci := range ecShardInfos {
		count += erasure_coding.GetShardCount(eci)
	}
	return count
}

// ecShardDiskIDForShard returns the physical disk_id that actually holds the
// given shard of the volume on this node. Multiple physical disks of one node
// collapse into a single DiskInfo on the wire, so a node-wide disk id is not
// enough to attribute an individual shard to its disk — capacity reservations
// and move bookkeeping key off the source disk, and a wrong disk drifts the
// per-disk capacity model the EC placement planner relies on. Returns 0 if the
// shard is not present (shardBits and diskShardBits are built together, so this
// only happens for a shard the node does not hold).
func ecShardDiskIDForShard(node *ecNodeInfo, vid uint32, shardID int) uint32 {
	info, ok := node.ecShards[vid]
	if !ok {
		return 0
	}
	sid := erasure_coding.ShardId(shardID)
	for diskID, bits := range info.diskShardBits {
		if bits.Has(sid) {
			return diskID
		}
	}
	return 0
}

// pickBestDiskOnNode chooses the physical disk on a node to place a new EC shard
// of the given volume, mirroring the shell ec.balance heuristic. It considers
// only disks of the matching type (diskType == "" matches any) with free
// capacity, and prefers, by ascending score:
//   - fewer total EC shards on the disk (spread load across disks),
//   - far fewer shards of this same volume on the disk (spread a volume's shards
//     across disks for fault tolerance, weighted heavily), and
//   - data/parity anti-affinity: a data shard avoids disks already holding this
//     volume's parity shards, and a parity shard avoids disks holding its data
//     shards.
//
// Returns 0 ("let the volume server pick a disk with free space") when no disk
// info is available or no disk of the right type has capacity.
func pickBestDiskOnNode(node *ecNodeInfo, vid uint32, diskType string, shardID, dataShardCount int) uint32 {
	if len(node.disks) == 0 {
		return 0
	}

	isDataShard := dataShardCount > 0 && shardID < dataShardCount
	info := node.ecShards[vid]

	var bestDiskID uint32
	bestScore := -1
	for diskID, disk := range node.disks {
		if diskType != "" && disk.diskType != diskType {
			continue
		}
		if disk.freeSlots <= 0 {
			continue
		}

		existingShards := 0
		hasDataShards := false
		hasParityShards := false
		if info != nil {
			bits := info.diskShardBits[diskID]
			existingShards = bits.Count()
			if dataShardCount > 0 {
				for s := 0; s < erasure_coding.MaxShardCount; s++ {
					if !bits.Has(erasure_coding.ShardId(s)) {
						continue
					}
					if s < dataShardCount {
						hasDataShards = true
					} else {
						hasParityShards = true
					}
				}
			}
		}

		// Lower score is better. Shards of this volume dominate (weight 100) so
		// the shard spreads across disks; total occupancy (weight 10) breaks ties.
		score := disk.ecShardCount*10 + existingShards*100
		if dataShardCount > 0 {
			if isDataShard && hasParityShards {
				score += 1000
			} else if !isDataShard && hasDataShards {
				score += 1000
			}
		}

		if bestScore == -1 || score < bestScore {
			bestScore = score
			bestDiskID = diskID
		}
	}

	return bestDiskID
}

// reserveShardOnDisk records a just-planned shard placement on a destination
// node's in-memory model so later picks in the same detection run spread shards
// across disks (and across nodes) instead of repeatedly choosing disk 0. It is
// idempotent with applyMovesToTopology, which re-asserts the same shard bit.
func reserveShardOnDisk(node *ecNodeInfo, vid uint32, collection string, shardID int, diskID uint32) {
	info, ok := node.ecShards[vid]
	if !ok {
		info = &ecVolumeInfo{
			collection:    collection,
			diskShardBits: make(map[uint32]erasure_coding.ShardBits),
		}
		node.ecShards[vid] = info
	}
	if info.diskShardBits == nil {
		info.diskShardBits = make(map[uint32]erasure_coding.ShardBits)
	}
	sid := erasure_coding.ShardId(shardID)
	info.shardBits = info.shardBits.Set(sid)
	info.diskShardBits[diskID] = info.diskShardBits[diskID].Set(sid)
	if disk, ok := node.disks[diskID]; ok {
		disk.ecShardCount++
		if disk.freeSlots > 0 {
			disk.freeSlots--
		}
	}
}

func ceilDivide(a, b int) int {
	if b == 0 {
		return 0
	}
	return (a + b - 1) / b
}

func movePhasePriority(phase string) types.TaskPriority {
	switch phase {
	case "dedup":
		return types.TaskPriorityHigh
	case "cross_rack":
		return types.TaskPriorityMedium
	default:
		return types.TaskPriorityLow
	}
}
