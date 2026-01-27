package erasure_coding

import (
	"fmt"
	"strings"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/admin/topology"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/worker_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage/erasure_coding"
	"github.com/seaweedfs/seaweedfs/weed/storage/erasure_coding/placement"
	"github.com/seaweedfs/seaweedfs/weed/worker/tasks/base"
	"github.com/seaweedfs/seaweedfs/weed/worker/tasks/util"
	"github.com/seaweedfs/seaweedfs/weed/worker/types"
)

// Detection implements the detection logic for erasure coding tasks
func Detection(metrics []*types.VolumeHealthMetrics, clusterInfo *types.ClusterInfo, config base.TaskConfig) ([]*types.TaskDetectionResult, error) {
	if !config.IsEnabled() {
		return nil, nil
	}

	ecConfig := config.(*Config)
	var results []*types.TaskDetectionResult
	now := time.Now()
	quietThreshold := time.Duration(ecConfig.QuietForSeconds) * time.Second
	minSizeBytes := uint64(ecConfig.MinSizeMB) * 1024 * 1024 // Configurable minimum

	debugCount := 0
	skippedAlreadyEC := 0
	skippedTooSmall := 0
	skippedCollectionFilter := 0
	skippedQuietTime := 0
	skippedFullness := 0

	// Group metrics by VolumeID to handle replicas and select canonical server
	volumeGroups := make(map[uint32][]*types.VolumeHealthMetrics)
	for _, metric := range metrics {
		volumeGroups[metric.VolumeID] = append(volumeGroups[metric.VolumeID], metric)
	}

	// Iterate over groups to check criteria and creation tasks
	for _, groupMetrics := range volumeGroups {
		// Find canonical metric (lowest Server ID) to ensure consistent task deduplication
		metric := groupMetrics[0]
		for _, m := range groupMetrics {
			if m.Server < metric.Server {
				metric = m
			}
		}

		// Skip if already EC volume
		if metric.IsECVolume {
			skippedAlreadyEC++
			continue
		}

		// Check minimum size requirement
		if metric.Size < minSizeBytes {
			skippedTooSmall++
			continue
		}

		// Check collection filter if specified
		if ecConfig.CollectionFilter != "" {
			// Parse comma-separated collections
			allowedCollections := make(map[string]bool)
			for _, collection := range strings.Split(ecConfig.CollectionFilter, ",") {
				allowedCollections[strings.TrimSpace(collection)] = true
			}
			// Skip if volume's collection is not in the allowed list
			if !allowedCollections[metric.Collection] {
				skippedCollectionFilter++
				continue
			}
		}

		// Check quiet duration and fullness criteria
		if metric.Age >= quietThreshold && metric.FullnessRatio >= ecConfig.FullnessRatio {
			glog.Infof("EC Detection: Volume %d meets all criteria, attempting to create task", metric.VolumeID)

			// Generate task ID for ActiveTopology integration
			taskID := fmt.Sprintf("ec_vol_%d_%d", metric.VolumeID, now.Unix())

			result := &types.TaskDetectionResult{
				TaskID:     taskID, // Link to ActiveTopology pending task
				TaskType:   types.TaskTypeErasureCoding,
				VolumeID:   metric.VolumeID,
				Server:     metric.Server,
				Collection: metric.Collection,
				Priority:   types.TaskPriorityLow, // EC is not urgent
				Reason: fmt.Sprintf("Volume meets EC criteria: quiet for %.1fs (>%ds), fullness=%.1f%% (>%.1f%%), size=%.1fMB (>%dMB)",
					metric.Age.Seconds(), ecConfig.QuietForSeconds, metric.FullnessRatio*100, ecConfig.FullnessRatio*100,
					float64(metric.Size)/(1024*1024), ecConfig.MinSizeMB),
				ScheduleAt: now,
			}

			// Plan EC destinations if ActiveTopology is available
			if clusterInfo.ActiveTopology != nil {
				// Check if ANY task already exists in ActiveTopology for this volume
				if clusterInfo.ActiveTopology.HasAnyTask(metric.VolumeID) {
					glog.V(2).Infof("EC Detection: Skipping volume %d, task already exists in ActiveTopology", metric.VolumeID)
					continue
				}

				glog.Infof("EC Detection: ActiveTopology available, planning destinations for volume %d", metric.VolumeID)
				multiPlan, err := planECDestinations(clusterInfo.ActiveTopology, metric, ecConfig)
				if err != nil {
					glog.Warningf("Failed to plan EC destinations for volume %d: %v", metric.VolumeID, err)
					continue // Skip this volume if destination planning fails
				}
				glog.Infof("EC Detection: Successfully planned %d destinations for volume %d", len(multiPlan.Plans), metric.VolumeID)

				// Calculate expected shard size for EC operation
				// Each data shard will be approximately volumeSize / dataShards
				expectedShardSize := uint64(metric.Size) / uint64(erasure_coding.DataShardsCount)

				// Add pending EC shard task to ActiveTopology for capacity management

				// Extract shard destinations from multiPlan
				var shardDestinations []string
				var shardDiskIDs []uint32
				for _, plan := range multiPlan.Plans {
					shardDestinations = append(shardDestinations, plan.TargetNode)
					shardDiskIDs = append(shardDiskIDs, plan.TargetDisk)
				}

				// Find all volume replica locations (server + disk) from topology
				glog.Infof("EC Detection: Looking for replica locations for volume %d", metric.VolumeID)
				replicaLocations := findVolumeReplicaLocations(clusterInfo.ActiveTopology, metric.VolumeID, metric.Collection)
				if len(replicaLocations) == 0 {
					glog.Warningf("No replica locations found for volume %d, skipping EC", metric.VolumeID)
					continue
				}
				glog.Infof("EC Detection: Found %d replica locations for volume %d", len(replicaLocations), metric.VolumeID)

				// Find existing EC shards from previous failed attempts
				existingECShards := findExistingECShards(clusterInfo.ActiveTopology, metric.VolumeID, metric.Collection)

				// Combine volume replicas and existing EC shards for cleanup
				var sources []topology.TaskSourceSpec

				// Add volume replicas (will free volume slots)
				for _, replica := range replicaLocations {
					sources = append(sources, topology.TaskSourceSpec{
						ServerID:    replica.ServerID,
						DiskID:      replica.DiskID,
						DataCenter:  replica.DataCenter,
						Rack:        replica.Rack,
						CleanupType: topology.CleanupVolumeReplica,
					})
				}

				// Add existing EC shards (will free shard slots)
				duplicateCheck := make(map[string]bool)
				for _, replica := range replicaLocations {
					key := fmt.Sprintf("%s:%d", replica.ServerID, replica.DiskID)
					duplicateCheck[key] = true
				}

				for _, shard := range existingECShards {
					key := fmt.Sprintf("%s:%d", shard.ServerID, shard.DiskID)
					if !duplicateCheck[key] { // Avoid duplicates if EC shards are on same disk as volume replicas
						sources = append(sources, topology.TaskSourceSpec{
							ServerID:    shard.ServerID,
							DiskID:      shard.DiskID,
							DataCenter:  shard.DataCenter,
							Rack:        shard.Rack,
							CleanupType: topology.CleanupECShards,
						})
						duplicateCheck[key] = true
					}
				}

				glog.V(2).Infof("Found %d volume replicas and %d existing EC shards for volume %d (total %d cleanup sources)",
					len(replicaLocations), len(existingECShards), metric.VolumeID, len(sources))

				// Convert shard destinations to TaskDestinationSpec
				destinations := make([]topology.TaskDestinationSpec, len(shardDestinations))
				shardImpact := topology.CalculateECShardStorageImpact(1, int64(expectedShardSize)) // 1 shard per destination
				shardSize := int64(expectedShardSize)
				for i, dest := range shardDestinations {
					destinations[i] = topology.TaskDestinationSpec{
						ServerID:      dest,
						DiskID:        shardDiskIDs[i],
						StorageImpact: &shardImpact,
						EstimatedSize: &shardSize,
					}
				}

				err = clusterInfo.ActiveTopology.AddPendingTask(topology.TaskSpec{
					TaskID:       taskID,
					TaskType:     topology.TaskTypeErasureCoding,
					VolumeID:     metric.VolumeID,
					VolumeSize:   int64(metric.Size),
					Sources:      sources,
					Destinations: destinations,
				})
				if err != nil {
					glog.Warningf("Failed to add pending EC shard task to ActiveTopology for volume %d: %v", metric.VolumeID, err)
					continue // Skip this volume if topology task addition fails
				}

				glog.V(2).Infof("Added pending EC shard task %s to ActiveTopology for volume %d with %d cleanup sources and %d shard destinations",
					taskID, metric.VolumeID, len(sources), len(multiPlan.Plans))

				// Convert sources
				sourcesProto, err := convertTaskSourcesToProtobuf(sources, metric.VolumeID, clusterInfo.ActiveTopology)
				if err != nil {
					glog.Warningf("Failed to convert sources for EC task on volume %d: %v, skipping", metric.VolumeID, err)
					continue
				}

				// Create unified sources and targets for EC task
				result.TypedParams = &worker_pb.TaskParams{
					TaskId:     taskID, // Link to ActiveTopology pending task
					VolumeId:   metric.VolumeID,
					Collection: metric.Collection,
					VolumeSize: metric.Size, // Store original volume size for tracking changes

					// Unified sources - all sources that will be processed/cleaned up
					Sources: sourcesProto,

					// Unified targets - all EC shard destinations
					Targets: createECTargets(multiPlan),

					TaskParams: &worker_pb.TaskParams_ErasureCodingParams{
						ErasureCodingParams: createECTaskParams(multiPlan),
					},
				}

				glog.V(1).Infof("Planned EC destinations for volume %d: %d shards across %d racks, %d DCs",
					metric.VolumeID, len(multiPlan.Plans), multiPlan.SuccessfulRack, multiPlan.SuccessfulDCs)
			} else {
				glog.Warningf("No ActiveTopology available for destination planning in EC detection")
				continue // Skip this volume if no topology available
			}

			glog.Infof("EC Detection: Successfully created EC task for volume %d, adding to results", metric.VolumeID)
			results = append(results, result)
		} else {
			// Count debug reasons
			if debugCount < 5 { // Limit to avoid spam
				if metric.Age < quietThreshold {
					skippedQuietTime++
				}
				if metric.FullnessRatio < ecConfig.FullnessRatio {
					skippedFullness++
				}
			}
			debugCount++
		}
	}

	// Log debug summary if no tasks were created
	if len(results) == 0 && len(metrics) > 0 {
		totalVolumes := len(metrics)
		glog.V(1).Infof("EC detection: No tasks created for %d volumes (skipped: %d already EC, %d too small, %d filtered, %d not quiet, %d not full)",
			totalVolumes, skippedAlreadyEC, skippedTooSmall, skippedCollectionFilter, skippedQuietTime, skippedFullness)

		// Show details for first few volumes
		for i, metric := range metrics {
			if i >= 3 || metric.IsECVolume { // Limit to first 3 non-EC volumes
				continue
			}
			sizeMB := float64(metric.Size) / (1024 * 1024)
			glog.Infof("ERASURE CODING: Volume %d: size=%.1fMB (need ≥%dMB), age=%s (need ≥%s), fullness=%.1f%% (need ≥%.1f%%)",
				metric.VolumeID, sizeMB, ecConfig.MinSizeMB, metric.Age.Truncate(time.Minute), quietThreshold.Truncate(time.Minute),
				metric.FullnessRatio*100, ecConfig.FullnessRatio*100)
		}
	}

	return results, nil
}

// planECDestinations plans the destinations for erasure coding operation
// This function implements EC destination planning logic directly in the detection phase
func planECDestinations(activeTopology *topology.ActiveTopology, metric *types.VolumeHealthMetrics, ecConfig *Config) (*topology.MultiDestinationPlan, error) {
	// Calculate expected shard size for EC operation
	expectedShardSize := uint64(metric.Size) / uint64(erasure_coding.DataShardsCount)

	// Get source node information from topology
	var sourceRack, sourceDC string

	// Extract rack and DC from topology info
	topologyInfo := activeTopology.GetTopologyInfo()
	if topologyInfo != nil {
		for _, dc := range topologyInfo.DataCenterInfos {
			for _, rack := range dc.RackInfos {
				for _, dataNodeInfo := range rack.DataNodeInfos {
					if dataNodeInfo.Id == metric.Server {
						sourceDC = dc.Id
						sourceRack = rack.Id
						break
					}
				}
				if sourceRack != "" {
					break
				}
			}
			if sourceDC != "" {
				break
			}
		}
	}

	// Get available disks for EC placement with effective capacity consideration (includes pending tasks)
	// For EC, we typically need 1 volume slot per shard, so use minimum capacity of 1
	// For EC, we need at least 1 available volume slot on a disk to consider it for placement.
	// Note: We don't exclude the source server since the original volume will be deleted after EC conversion
	availableDisks := activeTopology.GetDisksWithEffectiveCapacity(topology.TaskTypeErasureCoding, "", 1)
	if len(availableDisks) < erasure_coding.MinTotalDisks {
		return nil, fmt.Errorf("insufficient disks for EC placement: need %d, have %d (considering pending/active tasks)", erasure_coding.MinTotalDisks, len(availableDisks))
	}

	// Select best disks for EC placement with rack/DC diversity
	selectedDisks := selectBestECDestinations(availableDisks, sourceRack, sourceDC, erasure_coding.TotalShardsCount)
	if len(selectedDisks) < erasure_coding.MinTotalDisks {
		return nil, fmt.Errorf("found %d disks, but could not find %d suitable destinations for EC placement", len(selectedDisks), erasure_coding.MinTotalDisks)
	}

	var plans []*topology.DestinationPlan
	rackCount := make(map[string]int)
	dcCount := make(map[string]int)

	for _, disk := range selectedDisks {
		// Get the target server address
		targetAddress, err := util.ResolveServerAddress(disk.NodeID, activeTopology)
		if err != nil {
			return nil, fmt.Errorf("failed to resolve address for target server %s: %v", disk.NodeID, err)
		}

		plan := &topology.DestinationPlan{
			TargetNode:     disk.NodeID,
			TargetAddress:  targetAddress,
			TargetDisk:     disk.DiskID,
			TargetRack:     disk.Rack,
			TargetDC:       disk.DataCenter,
			ExpectedSize:   expectedShardSize, // Set calculated EC shard size
			PlacementScore: calculateECScore(disk, sourceRack, sourceDC),
		}
		plans = append(plans, plan)

		// Count rack and DC diversity
		rackKey := fmt.Sprintf("%s:%s", disk.DataCenter, disk.Rack)
		rackCount[rackKey]++
		dcCount[disk.DataCenter]++
	}

	// Log capacity utilization information using ActiveTopology's encapsulated logic
	totalEffectiveCapacity := int64(0)
	for _, plan := range plans {
		effectiveCapacity := activeTopology.GetEffectiveAvailableCapacity(plan.TargetNode, plan.TargetDisk)
		totalEffectiveCapacity += effectiveCapacity
	}

	glog.V(1).Infof("Planned EC destinations for volume %d (size=%d bytes): expected shard size=%d bytes, %d shards across %d racks, %d DCs, total effective capacity=%d slots",
		metric.VolumeID, metric.Size, expectedShardSize, len(plans), len(rackCount), len(dcCount), totalEffectiveCapacity)

	// Log storage impact for EC task (source only - EC has multiple targets handled individually)
	sourceChange, _ := topology.CalculateTaskStorageImpact(topology.TaskTypeErasureCoding, int64(metric.Size))
	glog.V(2).Infof("EC task capacity management: source_reserves_with_zero_impact={VolumeSlots:%d, ShardSlots:%d}, %d_targets_will_receive_shards, estimated_size=%d",
		sourceChange.VolumeSlots, sourceChange.ShardSlots, len(plans), metric.Size)
	glog.V(2).Infof("EC source reserves capacity but with zero StorageSlotChange impact")

	return &topology.MultiDestinationPlan{
		Plans:          plans,
		TotalShards:    len(plans),
		SuccessfulRack: len(rackCount),
		SuccessfulDCs:  len(dcCount),
	}, nil
}

// createECTargets creates unified TaskTarget structures from the multi-destination plan
// with proper shard ID assignment during planning phase
func createECTargets(multiPlan *topology.MultiDestinationPlan) []*worker_pb.TaskTarget {
	var targets []*worker_pb.TaskTarget
	numTargets := len(multiPlan.Plans)

	// Create shard assignment arrays for each target (round-robin distribution)
	targetShards := make([][]uint32, numTargets)
	for i := range targetShards {
		targetShards[i] = make([]uint32, 0)
	}

	// Distribute shards in round-robin fashion to spread both data and parity shards
	// This ensures each target gets a mix of data shards (0-9) and parity shards (10-13)
	for shardId := uint32(0); shardId < uint32(erasure_coding.TotalShardsCount); shardId++ {
		targetIndex := int(shardId) % numTargets
		targetShards[targetIndex] = append(targetShards[targetIndex], shardId)
	}

	// Create targets with assigned shard IDs
	for i, plan := range multiPlan.Plans {
		target := &worker_pb.TaskTarget{
			Node:          plan.TargetAddress,
			DiskId:        plan.TargetDisk,
			Rack:          plan.TargetRack,
			DataCenter:    plan.TargetDC,
			ShardIds:      targetShards[i], // Round-robin assigned shards
			EstimatedSize: plan.ExpectedSize,
		}
		targets = append(targets, target)

		// Log shard assignment with data/parity classification
		dataShards := make([]uint32, 0)
		parityShards := make([]uint32, 0)
		for _, shardId := range targetShards[i] {
			if shardId < uint32(erasure_coding.DataShardsCount) {
				dataShards = append(dataShards, shardId)
			} else {
				parityShards = append(parityShards, shardId)
			}
		}
		glog.V(2).Infof("EC planning: target %s assigned shards %v (data: %v, parity: %v)",
			plan.TargetNode, targetShards[i], dataShards, parityShards)
	}

	glog.V(1).Infof("EC planning: distributed %d shards across %d targets using round-robin (data shards 0-%d, parity shards %d-%d)",
		erasure_coding.TotalShardsCount, numTargets,
		erasure_coding.DataShardsCount-1, erasure_coding.DataShardsCount, erasure_coding.TotalShardsCount-1)
	return targets
}

// convertTaskSourcesToProtobuf converts topology.TaskSourceSpec to worker_pb.TaskSource
func convertTaskSourcesToProtobuf(sources []topology.TaskSourceSpec, volumeID uint32, activeTopology *topology.ActiveTopology) ([]*worker_pb.TaskSource, error) {
	var protobufSources []*worker_pb.TaskSource

	for _, source := range sources {
		serverAddress, err := util.ResolveServerAddress(source.ServerID, activeTopology)
		if err != nil {
			return nil, fmt.Errorf("failed to resolve address for source server %s: %v", source.ServerID, err)
		}

		pbSource := &worker_pb.TaskSource{
			Node:       serverAddress,
			DiskId:     source.DiskID,
			DataCenter: source.DataCenter,
			Rack:       source.Rack,
		}

		// Convert storage impact to estimated size
		if source.EstimatedSize != nil {
			pbSource.EstimatedSize = uint64(*source.EstimatedSize)
		}

		// Set appropriate volume ID or shard IDs based on cleanup type
		switch source.CleanupType {
		case topology.CleanupVolumeReplica:
			// This is a volume replica, use the actual volume ID
			pbSource.VolumeId = volumeID
		case topology.CleanupECShards:
			// This is EC shards, also use the volume ID for consistency
			pbSource.VolumeId = volumeID
			// Note: ShardIds would need to be passed separately if we need specific shard info
		}

		protobufSources = append(protobufSources, pbSource)
	}

	return protobufSources, nil
}

// createECTaskParams creates clean EC task parameters (destinations now in unified targets)
func createECTaskParams(multiPlan *topology.MultiDestinationPlan) *worker_pb.ErasureCodingTaskParams {
	return &worker_pb.ErasureCodingTaskParams{
		DataShards:   erasure_coding.DataShardsCount,   // Standard data shards
		ParityShards: erasure_coding.ParityShardsCount, // Standard parity shards
	}
}

// selectBestECDestinations selects multiple disks for EC shard placement with diversity
// Uses the consolidated placement package for proper rack/server/disk spreading
func selectBestECDestinations(disks []*topology.DiskInfo, sourceRack, sourceDC string, shardsNeeded int) []*topology.DiskInfo {
	if len(disks) == 0 {
		return nil
	}

	// Convert topology.DiskInfo to placement.DiskCandidate
	candidates := diskInfosToCandidates(disks)
	if len(candidates) == 0 {
		return nil
	}

	// Configure placement for EC shards
	config := placement.PlacementRequest{
		ShardsNeeded:           shardsNeeded,
		MaxShardsPerServer:     0, // No hard limit, but prefer spreading
		MaxShardsPerRack:       0, // No hard limit, but prefer spreading
		MaxTaskLoad:            topology.MaxTaskLoadForECPlacement,
		PreferDifferentServers: true,
		PreferDifferentRacks:   true,
	}

	// Use the shared placement algorithm
	result, err := placement.SelectDestinations(candidates, config)
	if err != nil {
		glog.V(2).Infof("EC placement failed: %v", err)
		return nil
	}

	// Convert back to topology.DiskInfo
	return candidatesToDiskInfos(result.SelectedDisks, disks)
}

// diskInfosToCandidates converts topology.DiskInfo slice to placement.DiskCandidate slice
func diskInfosToCandidates(disks []*topology.DiskInfo) []*placement.DiskCandidate {
	var candidates []*placement.DiskCandidate
	for _, disk := range disks {
		if disk.DiskInfo == nil {
			continue
		}

		// Calculate free slots (using default max if not set)
		freeSlots := int(disk.DiskInfo.MaxVolumeCount - disk.DiskInfo.VolumeCount)
		if freeSlots < 0 {
			freeSlots = 0
		}

		// Calculate EC shard count for this specific disk
		// EcShardInfos contains all shards, so we need to filter by DiskId and sum actual shard counts
		ecShardCount := 0
		if disk.DiskInfo.EcShardInfos != nil {
			for _, shardInfo := range disk.DiskInfo.EcShardInfos {
				if shardInfo.DiskId == disk.DiskID {
					ecShardCount += erasure_coding.GetShardCount(shardInfo)
				}
			}
		}

		candidates = append(candidates, &placement.DiskCandidate{
			NodeID:         disk.NodeID,
			DiskID:         disk.DiskID,
			DataCenter:     disk.DataCenter,
			Rack:           disk.Rack,
			VolumeCount:    disk.DiskInfo.VolumeCount,
			MaxVolumeCount: disk.DiskInfo.MaxVolumeCount,
			ShardCount:     ecShardCount,
			FreeSlots:      freeSlots,
			LoadCount:      disk.LoadCount,
		})
	}
	return candidates
}

// candidatesToDiskInfos converts placement results back to topology.DiskInfo
func candidatesToDiskInfos(candidates []*placement.DiskCandidate, originalDisks []*topology.DiskInfo) []*topology.DiskInfo {
	// Create a map for quick lookup
	diskMap := make(map[string]*topology.DiskInfo)
	for _, disk := range originalDisks {
		key := fmt.Sprintf("%s:%d", disk.NodeID, disk.DiskID)
		diskMap[key] = disk
	}

	var result []*topology.DiskInfo
	for _, candidate := range candidates {
		key := fmt.Sprintf("%s:%d", candidate.NodeID, candidate.DiskID)
		if disk, ok := diskMap[key]; ok {
			result = append(result, disk)
		}
	}
	return result
}

// calculateECScore calculates placement score for EC operations
// Used for logging and plan metadata
func calculateECScore(disk *topology.DiskInfo, sourceRack, sourceDC string) float64 {
	if disk.DiskInfo == nil {
		return 0.0
	}

	score := 0.0

	// Prefer disks with available capacity (primary factor)
	if disk.DiskInfo.MaxVolumeCount > 0 {
		utilization := float64(disk.DiskInfo.VolumeCount) / float64(disk.DiskInfo.MaxVolumeCount)
		score += (1.0 - utilization) * 60.0 // Up to 60 points for available capacity
	}

	// Consider current load (secondary factor)
	score += (10.0 - float64(disk.LoadCount)) // Up to 10 points for low load

	return score
}

// isDiskSuitableForEC checks if a disk is suitable for EC placement
// Note: This is kept for backward compatibility but the placement package
// handles filtering internally
func isDiskSuitableForEC(disk *topology.DiskInfo) bool {
	if disk.DiskInfo == nil {
		return false
	}

	// Check if disk is not overloaded with tasks
	if disk.LoadCount > topology.MaxTaskLoadForECPlacement {
		return false
	}

	return true
}

// findVolumeReplicaLocations finds all replica locations (server + disk) for the specified volume
// Uses O(1) indexed lookup for optimal performance on large clusters.
func findVolumeReplicaLocations(activeTopology *topology.ActiveTopology, volumeID uint32, collection string) []topology.VolumeReplica {
	if activeTopology == nil {
		return nil
	}
	return activeTopology.GetVolumeLocations(volumeID, collection)
}

// findExistingECShards finds existing EC shards for a volume (from previous failed EC attempts)
// Uses O(1) indexed lookup for optimal performance on large clusters.
func findExistingECShards(activeTopology *topology.ActiveTopology, volumeID uint32, collection string) []topology.VolumeReplica {
	if activeTopology == nil {
		return nil
	}
	return activeTopology.GetECShardLocations(volumeID, collection)
}

// findVolumeReplicas finds all servers that have replicas of the specified volume
func findVolumeReplicas(activeTopology *topology.ActiveTopology, volumeID uint32, collection string) []string {
	if activeTopology == nil {
		return []string{}
	}

	topologyInfo := activeTopology.GetTopologyInfo()
	if topologyInfo == nil {
		return []string{}
	}

	var replicaServers []string

	// Iterate through all nodes to find volume replicas
	for _, dc := range topologyInfo.DataCenterInfos {
		for _, rack := range dc.RackInfos {
			for _, nodeInfo := range rack.DataNodeInfos {
				for _, diskInfo := range nodeInfo.DiskInfos {
					for _, volumeInfo := range diskInfo.VolumeInfos {
						if volumeInfo.Id == volumeID && volumeInfo.Collection == collection {
							replicaServers = append(replicaServers, nodeInfo.Id)
							break // Found volume on this node, move to next node
						}
					}
				}
			}
		}
	}

	return replicaServers
}
