package erasure_coding

import (
	"fmt"
	"strings"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/admin/topology"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/worker_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage/erasure_coding"
	"github.com/seaweedfs/seaweedfs/weed/worker/tasks/base"
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

	for _, metric := range metrics {
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
				multiPlan, err := planECDestinations(clusterInfo.ActiveTopology, metric, ecConfig)
				if err != nil {
					glog.Warningf("Failed to plan EC destinations for volume %d: %v", metric.VolumeID, err)
					continue // Skip this volume if destination planning fails
				}

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
				replicaLocations := findVolumeReplicaLocations(clusterInfo.ActiveTopology, metric.VolumeID, metric.Collection)
				if len(replicaLocations) == 0 {
					glog.Warningf("No replica locations found for volume %d, skipping EC", metric.VolumeID)
					continue
				}

				// Convert to TaskSourceLocation format
				sourceLocations := make([]topology.TaskSourceLocation, len(replicaLocations))
				for i, replica := range replicaLocations {
					sourceLocations[i] = topology.TaskSourceLocation{
						ServerID: replica.ServerID,
						DiskID:   replica.DiskID,
					}
				}

				glog.V(2).Infof("Found %d replica locations for volume %d: %+v", len(replicaLocations), metric.VolumeID, replicaLocations)

				err = clusterInfo.ActiveTopology.AddPendingECShardTask(
					taskID,
					metric.VolumeID,
					sourceLocations,
					shardDestinations,
					shardDiskIDs,
					int32(len(multiPlan.Plans)),
					int64(expectedShardSize),
					int64(metric.Size),
				)
				if err != nil {
					glog.Warningf("Failed to add pending EC shard task to ActiveTopology for volume %d: %v", metric.VolumeID, err)
					continue // Skip this volume if topology task addition fails
				}

				glog.V(2).Infof("Added pending EC shard task %s to ActiveTopology for volume %d with %d source replicas and %d shard destinations",
					taskID, metric.VolumeID, len(sourceLocations), len(multiPlan.Plans))

				// Find all volume replicas from topology (for legacy worker compatibility)
				replicas := findVolumeReplicas(clusterInfo.ActiveTopology, metric.VolumeID, metric.Collection)
				glog.V(1).Infof("Found %d replicas for volume %d: %v", len(replicas), metric.VolumeID, replicas)

				// Create typed parameters with EC destination information and replicas
				result.TypedParams = &worker_pb.TaskParams{
					TaskId:     taskID, // Link to ActiveTopology pending task
					VolumeId:   metric.VolumeID,
					Server:     metric.Server,
					Collection: metric.Collection,
					VolumeSize: metric.Size, // Store original volume size for tracking changes
					Replicas:   replicas,    // Include all volume replicas for deletion
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
		plan := &topology.DestinationPlan{
			TargetNode:     disk.NodeID,
			TargetDisk:     disk.DiskID,
			TargetRack:     disk.Rack,
			TargetDC:       disk.DataCenter,
			ExpectedSize:   expectedShardSize, // Set calculated EC shard size
			PlacementScore: calculateECScore(disk, sourceRack, sourceDC),
			Conflicts:      checkECPlacementConflicts(disk, sourceRack, sourceDC),
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

// createECTaskParams creates EC task parameters from the multi-destination plan
func createECTaskParams(multiPlan *topology.MultiDestinationPlan) *worker_pb.ErasureCodingTaskParams {
	var destinations []*worker_pb.ECDestination

	for _, plan := range multiPlan.Plans {
		destination := &worker_pb.ECDestination{
			Node:           plan.TargetNode,
			DiskId:         plan.TargetDisk,
			Rack:           plan.TargetRack,
			DataCenter:     plan.TargetDC,
			PlacementScore: plan.PlacementScore,
		}
		destinations = append(destinations, destination)
	}

	// Collect placement conflicts from all destinations
	var placementConflicts []string
	for _, plan := range multiPlan.Plans {
		placementConflicts = append(placementConflicts, plan.Conflicts...)
	}

	return &worker_pb.ErasureCodingTaskParams{
		Destinations:       destinations,
		DataShards:         erasure_coding.DataShardsCount,   // Standard data shards
		ParityShards:       erasure_coding.ParityShardsCount, // Standard parity shards
		PlacementConflicts: placementConflicts,
	}
}

// selectBestECDestinations selects multiple disks for EC shard placement with diversity
func selectBestECDestinations(disks []*topology.DiskInfo, sourceRack, sourceDC string, shardsNeeded int) []*topology.DiskInfo {
	if len(disks) == 0 {
		return nil
	}

	// Group disks by rack and DC for diversity
	rackGroups := make(map[string][]*topology.DiskInfo)
	for _, disk := range disks {
		rackKey := fmt.Sprintf("%s:%s", disk.DataCenter, disk.Rack)
		rackGroups[rackKey] = append(rackGroups[rackKey], disk)
	}

	var selected []*topology.DiskInfo
	usedRacks := make(map[string]bool)

	// First pass: select one disk from each rack for maximum diversity
	for rackKey, rackDisks := range rackGroups {
		if len(selected) >= shardsNeeded {
			break
		}

		// Select best disk from this rack
		bestDisk := selectBestFromRack(rackDisks, sourceRack, sourceDC)
		if bestDisk != nil {
			selected = append(selected, bestDisk)
			usedRacks[rackKey] = true
		}
	}

	// Second pass: if we need more disks, select from racks we've already used
	if len(selected) < shardsNeeded {
		for _, disk := range disks {
			if len(selected) >= shardsNeeded {
				break
			}

			// Skip if already selected
			alreadySelected := false
			for _, sel := range selected {
				if sel.NodeID == disk.NodeID && sel.DiskID == disk.DiskID {
					alreadySelected = true
					break
				}
			}

			if !alreadySelected && isDiskSuitableForEC(disk) {
				selected = append(selected, disk)
			}
		}
	}

	return selected
}

// selectBestFromRack selects the best disk from a rack for EC placement
func selectBestFromRack(disks []*topology.DiskInfo, sourceRack, sourceDC string) *topology.DiskInfo {
	if len(disks) == 0 {
		return nil
	}

	var bestDisk *topology.DiskInfo
	bestScore := -1.0

	for _, disk := range disks {
		if !isDiskSuitableForEC(disk) {
			continue
		}

		score := calculateECScore(disk, sourceRack, sourceDC)
		if score > bestScore {
			bestScore = score
			bestDisk = disk
		}
	}

	return bestDisk
}

// calculateECScore calculates placement score for EC operations
func calculateECScore(disk *topology.DiskInfo, sourceRack, sourceDC string) float64 {
	if disk.DiskInfo == nil {
		return 0.0
	}

	score := 0.0

	// Prefer disks with available capacity
	if disk.DiskInfo.MaxVolumeCount > 0 {
		utilization := float64(disk.DiskInfo.VolumeCount) / float64(disk.DiskInfo.MaxVolumeCount)
		score += (1.0 - utilization) * 50.0 // Up to 50 points for available capacity
	}

	// Prefer different racks for better distribution
	if disk.Rack != sourceRack {
		score += 30.0
	}

	// Prefer different data centers for better distribution
	if disk.DataCenter != sourceDC {
		score += 20.0
	}

	// Consider current load
	score += (10.0 - float64(disk.LoadCount)) // Up to 10 points for low load

	return score
}

// calculateECScoreWithSize calculates placement score for EC operations with shard size consideration
func calculateECScoreWithSize(disk *topology.DiskInfo, sourceRack, sourceDC string, expectedShardSize uint64) float64 {
	baseScore := calculateECScore(disk, sourceRack, sourceDC)

	// Additional scoring based on effective available space vs expected shard size
	if disk.DiskInfo != nil && expectedShardSize > 0 {
		// Use effective available capacity (MaxVolumeCount - VolumeCount already accounts for tasks)
		// The GetDisksWithEffectiveCapacity method adjusts VolumeCount to reflect effective capacity
		effectiveAvailableSlots := disk.DiskInfo.MaxVolumeCount - disk.DiskInfo.VolumeCount
		if effectiveAvailableSlots > 0 {
			// Bonus for having plenty of space for the expected shard
			// This is a heuristic - each volume slot can theoretically hold any size
			baseScore += float64(effectiveAvailableSlots) * 2.0 // Up to 2 points per available slot
		}

		// Additional penalty if load count is high (many pending/active tasks)
		if disk.LoadCount > 1 {
			baseScore -= float64(disk.LoadCount) * 1.0 // 1 point penalty per high load
		}
	}

	return baseScore
}

// isDiskSuitableForEC checks if a disk is suitable for EC placement
func isDiskSuitableForEC(disk *topology.DiskInfo) bool {
	if disk.DiskInfo == nil {
		return false
	}

	// Check if disk has effective capacity (accounting for pending/active tasks)
	// The GetDisksWithEffectiveCapacity method already adjusts VolumeCount to reflect effective capacity
	effectiveAvailableSlots := disk.DiskInfo.MaxVolumeCount - disk.DiskInfo.VolumeCount
	if effectiveAvailableSlots <= 0 {
		return false
	}

	// Check if disk is not overloaded with tasks
	if disk.LoadCount > 10 { // Arbitrary threshold for total task load
		return false
	}

	return true
}

// checkECPlacementConflicts checks for placement rule conflicts in EC operations
func checkECPlacementConflicts(disk *topology.DiskInfo, sourceRack, sourceDC string) []string {
	var conflicts []string

	// For EC, being on the same rack as source is often acceptable
	// but we note it as potential conflict for monitoring
	if disk.Rack == sourceRack && disk.DataCenter == sourceDC {
		conflicts = append(conflicts, "same_rack_as_source")
	}

	return conflicts
}

// findVolumeDisk finds the disk ID where a specific volume is located on a given server
func findVolumeDisk(activeTopology *topology.ActiveTopology, volumeID uint32, collection string, serverID string) uint32 {
	if activeTopology == nil {
		return 0
	}

	topologyInfo := activeTopology.GetTopologyInfo()
	if topologyInfo == nil {
		return 0
	}

	// Iterate through all nodes to find the specific server and volume
	for _, dc := range topologyInfo.DataCenterInfos {
		for _, rack := range dc.RackInfos {
			for _, nodeInfo := range rack.DataNodeInfos {
				if nodeInfo.Id == serverID {
					for _, diskInfo := range nodeInfo.DiskInfos {
						for _, volumeInfo := range diskInfo.VolumeInfos {
							if volumeInfo.Id == volumeID && volumeInfo.Collection == collection {
								return diskInfo.DiskId
							}
						}
					}
				}
			}
		}
	}

	// Default to disk 0 if not found (common case for single-disk nodes)
	return 0
}

// VolumeReplica represents a replica location with server and disk information
type VolumeReplica struct {
	ServerID string
	DiskID   uint32
}

// findVolumeReplicaLocations finds all replica locations (server + disk) for the specified volume
func findVolumeReplicaLocations(activeTopology *topology.ActiveTopology, volumeID uint32, collection string) []VolumeReplica {
	if activeTopology == nil {
		return []VolumeReplica{}
	}

	topologyInfo := activeTopology.GetTopologyInfo()
	if topologyInfo == nil {
		return []VolumeReplica{}
	}

	var replicas []VolumeReplica

	// Iterate through all nodes to find volume replicas
	for _, dc := range topologyInfo.DataCenterInfos {
		for _, rack := range dc.RackInfos {
			for _, nodeInfo := range rack.DataNodeInfos {
				for _, diskInfo := range nodeInfo.DiskInfos {
					for _, volumeInfo := range diskInfo.VolumeInfos {
						if volumeInfo.Id == volumeID && volumeInfo.Collection == collection {
							replicas = append(replicas, VolumeReplica{
								ServerID: nodeInfo.Id,
								DiskID:   diskInfo.DiskId,
							})
							break // Found volume on this disk, move to next disk
						}
					}
				}
			}
		}
	}

	return replicas
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
