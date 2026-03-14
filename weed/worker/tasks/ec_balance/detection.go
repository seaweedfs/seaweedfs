package ec_balance

import (
	"context"
	"fmt"
	"sort"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/worker_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage/erasure_coding"
	"github.com/seaweedfs/seaweedfs/weed/util/wildcard"
	"github.com/seaweedfs/seaweedfs/weed/worker/tasks/base"
	"github.com/seaweedfs/seaweedfs/weed/worker/types"
)

// ecNodeInfo represents a volume server with EC shard information for detection
type ecNodeInfo struct {
	nodeID    string
	address   string
	dc        string
	rack      string
	freeSlots int
	// volumeID -> shardBits (bitmask of shard IDs present on this node)
	ecShards map[uint32]*ecVolumeInfo
}

type ecVolumeInfo struct {
	collection string
	shardBits  uint32 // bitmask
	diskID     uint32
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

	var allMoves []*shardMove
	now := time.Now()

	for collection, volumeIDs := range collections {
		if ctx != nil {
			if err := ctx.Err(); err != nil {
				return nil, false, err
			}
		}

		// Phase 1: Detect duplicate shards
		for _, vid := range volumeIDs {
			moves := detectDuplicateShards(vid, collection, nodes, ecConfig.DiskType)
			allMoves = append(allMoves, moves...)
		}

		// Phase 2: Balance shards across racks
		for _, vid := range volumeIDs {
			moves := detectCrossRackImbalance(vid, collection, nodes, racks, ecConfig.DiskType)
			allMoves = append(allMoves, moves...)
		}

		// Phase 3: Balance shards within racks
		for _, vid := range volumeIDs {
			moves := detectWithinRackImbalance(vid, collection, nodes, racks, ecConfig.DiskType)
			allMoves = append(allMoves, moves...)
		}
	}

	// Phase 4: Global node balance across racks
	globalMoves := detectGlobalImbalance(nodes, racks, ecConfig)
	allMoves = append(allMoves, globalMoves...)

	// Cap results
	hasMore := false
	if maxResults > 0 && len(allMoves) > maxResults {
		allMoves = allMoves[:maxResults]
		hasMore = true
	}

	// Convert moves to TaskDetectionResults
	results := make([]*types.TaskDetectionResult, 0, len(allMoves))
	for _, move := range allMoves {
		taskID := fmt.Sprintf("ec_balance_%d_%d_%d", move.volumeID, move.shardID, now.UnixNano())

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

	glog.V(1).Infof("EC balance detection: %d moves proposed across %d collections (%d dedup, cross_rack, within_rack, global)",
		len(results), len(collections), len(results))

	return results, hasMore, nil
}

// buildECTopology constructs EC node and rack structures from topology info
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
			rackID := rack.Id
			if _, ok := racks[rackID]; !ok {
				racks[rackID] = &ecRackInfo{nodes: make(map[string]*ecNodeInfo)}
			}

			for _, dn := range rack.DataNodeInfos {
				node := &ecNodeInfo{
					nodeID:   dn.Id,
					address:  dn.Id, // same as ID in topology
					dc:       dc.Id,
					rack:     rackID,
					ecShards: make(map[uint32]*ecVolumeInfo),
				}

				for diskType, diskInfo := range dn.DiskInfos {
					if config.DiskType != "" && diskType != config.DiskType {
						continue
					}

					freeSlots := int(diskInfo.MaxVolumeCount-diskInfo.VolumeCount)*erasure_coding.DataShardsCount - countEcShards(diskInfo.EcShardInfos)
					if freeSlots > 0 {
						node.freeSlots += freeSlots
					}

					for _, ecShardInfo := range diskInfo.EcShardInfos {
						vid := ecShardInfo.Id
						existing, ok := node.ecShards[vid]
						if !ok {
							existing = &ecVolumeInfo{
								collection: ecShardInfo.Collection,
								diskID:     ecShardInfo.DiskId,
							}
							node.ecShards[vid] = existing
						}
						existing.shardBits |= ecShardInfo.EcIndexBits
					}
				}

				nodes[dn.Id] = node
				racks[rackID].nodes[dn.Id] = node
				racks[rackID].freeSlots += node.freeSlots
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

// detectDuplicateShards finds shards that exist on multiple nodes
func detectDuplicateShards(vid uint32, collection string, nodes map[string]*ecNodeInfo, diskType string) []*shardMove {
	// Build shard -> list of nodes mapping
	shardLocations := make(map[int][]*ecNodeInfo)
	for _, node := range nodes {
		info, ok := node.ecShards[vid]
		if !ok {
			continue
		}
		for shardID := 0; shardID < erasure_coding.MaxShardCount; shardID++ {
			if info.shardBits&(1<<uint(shardID)) != 0 {
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
		keeper := locs[len(locs)-1]

		// Propose deletion of all other copies
		for _, node := range locs[:len(locs)-1] {
			moves = append(moves, &shardMove{
				volumeID:   vid,
				shardID:    shardID,
				collection: collection,
				source:     node,
				sourceDisk: ecShardDiskID(node, vid),
				target:     keeper, // target is the keeper for dedup (we delete from source)
				targetDisk: ecShardDiskID(keeper, vid),
				phase:      "dedup",
			})
		}
	}

	return moves
}

// detectCrossRackImbalance detects shards that should be moved across racks for even distribution
func detectCrossRackImbalance(vid uint32, collection string, nodes map[string]*ecNodeInfo, racks map[string]*ecRackInfo, diskType string) []*shardMove {
	numRacks := len(racks)
	if numRacks <= 1 {
		return nil
	}

	// Count shards per rack for this volume
	rackShardCount := make(map[string]int)
	rackShardNodes := make(map[string][]*ecNodeInfo)
	totalShards := 0

	for _, node := range nodes {
		info, ok := node.ecShards[vid]
		if !ok {
			continue
		}
		count := shardBitCount(info.shardBits)
		if count > 0 {
			rackShardCount[node.rack] += count
			rackShardNodes[node.rack] = append(rackShardNodes[node.rack], node)
			totalShards += count
		}
	}

	if totalShards == 0 {
		return nil
	}

	maxPerRack := ceilDivide(totalShards, numRacks)

	var moves []*shardMove

	// Find over-loaded racks and move excess shards to under-loaded racks
	for rackID, count := range rackShardCount {
		if count <= maxPerRack {
			continue
		}
		excess := count - maxPerRack
		movedFromRack := 0

		// Find shards to move from this rack
		for _, node := range rackShardNodes[rackID] {
			if movedFromRack >= excess {
				break
			}
			info := node.ecShards[vid]
			for shardID := 0; shardID < erasure_coding.TotalShardsCount; shardID++ {
				if movedFromRack >= excess {
					break
				}
				if info.shardBits&(1<<uint(shardID)) == 0 {
					continue
				}

				// Find destination: rack with fewest shards of this volume
				destNode := findDestNodeInUnderloadedRack(vid, racks, rackShardCount, maxPerRack, rackID, nodes)
				if destNode == nil {
					continue
				}

				moves = append(moves, &shardMove{
					volumeID:   vid,
					shardID:    shardID,
					collection: collection,
					source:     node,
					sourceDisk: ecShardDiskID(node, vid),
					target:     destNode,
					targetDisk: ecShardDiskID(destNode, vid),
					phase:      "cross_rack",
				})
				movedFromRack++
			}
		}
	}

	return moves
}

// detectWithinRackImbalance detects shards that should be moved within racks for even node distribution
func detectWithinRackImbalance(vid uint32, collection string, nodes map[string]*ecNodeInfo, racks map[string]*ecRackInfo, diskType string) []*shardMove {
	var moves []*shardMove

	for rackID, rack := range racks {
		if len(rack.nodes) <= 1 {
			continue
		}

		// Count shards per node in this rack for this volume
		nodeShardCount := make(map[string]int)
		totalInRack := 0
		for nodeID, node := range rack.nodes {
			info, ok := node.ecShards[vid]
			if !ok {
				continue
			}
			count := shardBitCount(info.shardBits)
			nodeShardCount[nodeID] = count
			totalInRack += count
		}

		if totalInRack == 0 {
			continue
		}

		maxPerNode := ceilDivide(totalInRack, len(rack.nodes))

		// Find over-loaded nodes and move excess
		for nodeID, count := range nodeShardCount {
			if count <= maxPerNode {
				continue
			}
			excess := count - maxPerNode
			node := rack.nodes[nodeID]
			info := node.ecShards[vid]
			moved := 0

			for shardID := 0; shardID < erasure_coding.TotalShardsCount; shardID++ {
				if moved >= excess {
					break
				}
				if info.shardBits&(1<<uint(shardID)) == 0 {
					continue
				}

				// Find least-loaded node in same rack
				destNode := findLeastLoadedNodeInRack(vid, rack, nodeID, nodeShardCount, maxPerNode)
				if destNode == nil {
					continue
				}

				moves = append(moves, &shardMove{
					volumeID:   vid,
					shardID:    shardID,
					collection: collection,
					source:     node,
					sourceDisk: ecShardDiskID(node, vid),
					target:     destNode,
					targetDisk: 0,
					phase:      "within_rack",
				})
				moved++
				nodeShardCount[nodeID]--
				nodeShardCount[destNode.nodeID]++
			}
		}
		_ = rackID
	}

	return moves
}

// detectGlobalImbalance detects total shard count imbalance across nodes in each rack
func detectGlobalImbalance(nodes map[string]*ecNodeInfo, racks map[string]*ecRackInfo, config *Config) []*shardMove {
	var moves []*shardMove

	for _, rack := range racks {
		if len(rack.nodes) <= 1 {
			continue
		}

		// Count total EC shards per node (across all volumes)
		nodeShardCounts := make(map[string]int)
		totalShards := 0
		for nodeID, node := range rack.nodes {
			count := 0
			for _, info := range node.ecShards {
				count += shardBitCount(info.shardBits)
			}
			nodeShardCounts[nodeID] = count
			totalShards += count
		}

		if totalShards == 0 {
			continue
		}

		avgShards := ceilDivide(totalShards, len(rack.nodes))

		// Iteratively move shards from most-loaded to least-loaded
		for i := 0; i < 10; i++ { // cap iterations to avoid infinite loops
			// Find min and max nodes
			var minNode, maxNode *ecNodeInfo
			minCount, maxCount := totalShards+1, -1
			for nodeID, count := range nodeShardCounts {
				if count < minCount {
					minCount = count
					minNode = rack.nodes[nodeID]
				}
				if count > maxCount {
					maxCount = count
					maxNode = rack.nodes[nodeID]
				}
			}

			if maxNode == nil || minNode == nil || maxNode.nodeID == minNode.nodeID {
				break
			}
			if maxCount <= avgShards || minCount+1 > avgShards {
				break
			}
			if maxCount-minCount <= 1 {
				break
			}

			// Pick a shard from maxNode that doesn't already exist on minNode
			moved := false
			for vid, info := range maxNode.ecShards {
				if moved {
					break
				}
				// Check minNode doesn't have this volume's shards already (avoid same-volume overlap)
				minInfo := minNode.ecShards[vid]
				for shardID := 0; shardID < erasure_coding.TotalShardsCount; shardID++ {
					if info.shardBits&(1<<uint(shardID)) == 0 {
						continue
					}
					// Skip if destination already has this shard
					if minInfo != nil && minInfo.shardBits&(1<<uint(shardID)) != 0 {
						continue
					}

					moves = append(moves, &shardMove{
						volumeID:   vid,
						shardID:    shardID,
						collection: info.collection,
						source:     maxNode,
						sourceDisk: info.diskID,
						target:     minNode,
						targetDisk: 0,
						phase:      "global",
					})
					nodeShardCounts[maxNode.nodeID]--
					nodeShardCounts[minNode.nodeID]++
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

// findDestNodeInUnderloadedRack finds a node in a rack that has fewer than maxPerRack shards
func findDestNodeInUnderloadedRack(vid uint32, racks map[string]*ecRackInfo, rackShardCount map[string]int, maxPerRack int, excludeRack string, nodes map[string]*ecNodeInfo) *ecNodeInfo {
	var bestNode *ecNodeInfo
	bestFreeSlots := -1

	for rackID, rack := range racks {
		if rackID == excludeRack {
			continue
		}
		if rackShardCount[rackID] >= maxPerRack {
			continue
		}
		if rack.freeSlots <= 0 {
			continue
		}
		for _, node := range rack.nodes {
			if node.freeSlots <= 0 {
				continue
			}
			if node.freeSlots > bestFreeSlots {
				bestFreeSlots = node.freeSlots
				bestNode = node
			}
		}
	}

	return bestNode
}

// findLeastLoadedNodeInRack finds the node with fewest shards in a rack
func findLeastLoadedNodeInRack(vid uint32, rack *ecRackInfo, excludeNode string, nodeShardCount map[string]int, maxPerNode int) *ecNodeInfo {
	var bestNode *ecNodeInfo
	bestCount := maxPerNode + 1

	for nodeID, node := range rack.nodes {
		if nodeID == excludeNode {
			continue
		}
		if node.freeSlots <= 0 {
			continue
		}
		count := nodeShardCount[nodeID]
		if count >= maxPerNode {
			continue
		}
		if count < bestCount {
			bestCount = count
			bestNode = node
		}
	}

	return bestNode
}

// Helper functions

func countEcShards(ecShardInfos []*master_pb.VolumeEcShardInformationMessage) int {
	count := 0
	for _, eci := range ecShardInfos {
		count += erasure_coding.GetShardCount(eci)
	}
	return count
}

func shardBitCount(bits uint32) int {
	count := 0
	for bits != 0 {
		count += int(bits & 1)
		bits >>= 1
	}
	return count
}

func ecShardDiskID(node *ecNodeInfo, vid uint32) uint32 {
	if info, ok := node.ecShards[vid]; ok {
		return info.diskID
	}
	return 0
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
