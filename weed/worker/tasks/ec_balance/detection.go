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
	// dataShards is this volume's EC data-shard count, reported per shard message
	// (0 means the reporting volume server predates custom ratios). It sets the
	// data/parity boundary for anti-affinity instead of assuming the 10+4 default.
	dataShards int
}

// dataShardCount returns the volume's EC data-shard count, falling back to the
// standard default when the volume server did not report a custom ratio.
func (info *ecVolumeInfo) dataShardCount() int {
	if info != nil && info.dataShards > 0 {
		return info.dataShards
	}
	return erasure_coding.DataShardsCount
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

	// Build set of allowed collections for global phase filtering
	allowedVids := make(map[uint32]bool)
	for _, volumeIDs := range collections {
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

		// Phase 1: Detect duplicate shards (always run, duplicates are errors not imbalance)
		for _, vid := range volumeIDs {
			moves := detectDuplicateShards(vid, collection, nodes, ecConfig.DiskType)
			applyMovesToTopology(moves)
			allMoves = append(allMoves, moves...)
		}

		// Phase 2: Balance shards across racks (operates on updated topology from phase 1)
		for _, vid := range volumeIDs {
			moves := detectCrossRackImbalance(vid, collection, nodes, racks, ecConfig.DiskType, threshold)
			applyMovesToTopology(moves)
			allMoves = append(allMoves, moves...)
		}

		// Phase 3: Balance shards within racks (operates on updated topology from phases 1-2)
		for _, vid := range volumeIDs {
			moves := detectWithinRackImbalance(vid, collection, nodes, racks, ecConfig.DiskType, threshold)
			applyMovesToTopology(moves)
			allMoves = append(allMoves, moves...)
		}
	}

	// Phase 4: Global node balance across racks (only for volumes in allowed collections)
	globalMoves := detectGlobalImbalance(nodes, racks, ecConfig, allowedVids)
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

// detectCrossRackImbalance detects shards that should be moved across racks for even distribution.
// Returns nil if imbalance is below the threshold.
func detectCrossRackImbalance(vid uint32, collection string, nodes map[string]*ecNodeInfo, racks map[string]*ecRackInfo, diskType string, threshold float64) []*shardMove {
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
		count := info.shardBits.Count()
		if count > 0 {
			rackShardCount[node.rack] += count
			rackShardNodes[node.rack] = append(rackShardNodes[node.rack], node)
			totalShards += count
		}
	}

	if totalShards == 0 {
		return nil
	}

	// Check if imbalance exceeds threshold
	if !exceedsImbalanceThreshold(rackShardCount, totalShards, numRacks, threshold) {
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
				if !info.shardBits.Has(erasure_coding.ShardId(shardID)) {
					continue
				}

				// Find destination: rack with fewest shards of this volume
				destNode := findDestNodeInUnderloadedRack(vid, racks, rackShardCount, maxPerRack, rackID, nodes)
				if destNode == nil {
					continue
				}

				// Spread the shard across the destination's disks (data/parity
				// anti-affinity), rather than letting it land on disk 0.
				destDisk := pickBestDiskOnNode(destNode, vid, diskType, shardID, info.dataShardCount())
				moves = append(moves, &shardMove{
					volumeID:   vid,
					shardID:    shardID,
					collection: collection,
					source:     node,
					sourceDisk: ecShardDiskIDForShard(node, vid, shardID),
					target:     destNode,
					targetDisk: destDisk,
					phase:      "cross_rack",
				})
				reserveShardOnDisk(destNode, vid, collection, shardID, destDisk)
				movedFromRack++

				// Reserve capacity on destination so it isn't picked again,
				// and release one slot on the source so later volumes in this
				// same detection run see its true available capacity.
				rackShardCount[destNode.rack]++
				rackShardCount[rackID]--
				node.freeSlots++
				destNode.freeSlots--
			}
		}
	}

	return moves
}

// detectWithinRackImbalance detects shards that should be moved within racks for even node distribution.
// Returns nil if imbalance is below the threshold.
func detectWithinRackImbalance(vid uint32, collection string, nodes map[string]*ecNodeInfo, racks map[string]*ecRackInfo, diskType string, threshold float64) []*shardMove {
	var moves []*shardMove

	for _, rack := range racks {
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
			count := info.shardBits.Count()
			nodeShardCount[nodeID] = count
			totalInRack += count
		}

		if totalInRack == 0 {
			continue
		}

		// Check if imbalance exceeds threshold
		if !exceedsImbalanceThreshold(nodeShardCount, totalInRack, len(rack.nodes), threshold) {
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
				if !info.shardBits.Has(erasure_coding.ShardId(shardID)) {
					continue
				}

				// Find least-loaded node in same rack
				destNode := findLeastLoadedNodeInRack(vid, rack, nodeID, nodeShardCount, maxPerNode)
				if destNode == nil {
					continue
				}

				destDisk := pickBestDiskOnNode(destNode, vid, diskType, shardID, info.dataShardCount())
				moves = append(moves, &shardMove{
					volumeID:   vid,
					shardID:    shardID,
					collection: collection,
					source:     node,
					sourceDisk: ecShardDiskIDForShard(node, vid, shardID),
					target:     destNode,
					targetDisk: destDisk,
					phase:      "within_rack",
				})
				reserveShardOnDisk(destNode, vid, collection, shardID, destDisk)
				moved++
				nodeShardCount[nodeID]--
				nodeShardCount[destNode.nodeID]++
				node.freeSlots++
				destNode.freeSlots--
			}
		}
	}

	return moves
}

// detectGlobalImbalance detects total shard count imbalance across nodes in each rack.
// Respects ImbalanceThreshold from config. Only considers volumes in allowedVids.
func detectGlobalImbalance(nodes map[string]*ecNodeInfo, racks map[string]*ecRackInfo, config *Config, allowedVids map[uint32]bool) []*shardMove {
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

					destDisk := pickBestDiskOnNode(minNode, vid, config.DiskType, shardID, info.dataShardCount())
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
