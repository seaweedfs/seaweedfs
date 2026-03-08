package balance

import (
	"fmt"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/admin/topology"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/worker_pb"
	"github.com/seaweedfs/seaweedfs/weed/worker/tasks/base"
	"github.com/seaweedfs/seaweedfs/weed/worker/tasks/util"
	"github.com/seaweedfs/seaweedfs/weed/worker/types"
)

// Detection implements the detection logic for balance tasks.
// maxResults limits how many balance operations are returned per invocation.
func Detection(metrics []*types.VolumeHealthMetrics, clusterInfo *types.ClusterInfo, config base.TaskConfig, maxResults int) ([]*types.TaskDetectionResult, error) {
	if !config.IsEnabled() {
		return nil, nil
	}

	balanceConfig := config.(*Config)

	if maxResults <= 0 {
		maxResults = 1
	}

	// Group volumes by disk type to ensure we compare apples to apples
	volumesByDiskType := make(map[string][]*types.VolumeHealthMetrics)
	for _, metric := range metrics {
		volumesByDiskType[metric.DiskType] = append(volumesByDiskType[metric.DiskType], metric)
	}

	var allParams []*types.TaskDetectionResult

	for diskType, diskMetrics := range volumesByDiskType {
		remaining := maxResults - len(allParams)
		if remaining <= 0 {
			break
		}
		tasks := detectForDiskType(diskType, diskMetrics, balanceConfig, clusterInfo, remaining)
		allParams = append(allParams, tasks...)
	}

	return allParams, nil
}

// detectForDiskType performs balance detection for a specific disk type,
// returning up to maxResults balance tasks.
func detectForDiskType(diskType string, diskMetrics []*types.VolumeHealthMetrics, balanceConfig *Config, clusterInfo *types.ClusterInfo, maxResults int) []*types.TaskDetectionResult {
	// Skip if cluster segment is too small
	minVolumeCount := 2 // More reasonable for small clusters
	if len(diskMetrics) < minVolumeCount {
		// Only log at verbose level to avoid spamming for small/empty disk types
		glog.V(1).Infof("BALANCE [%s]: No tasks created - cluster too small (%d volumes, need ≥%d)", diskType, len(diskMetrics), minVolumeCount)
		return nil
	}

	// Analyze volume distribution across servers
	serverVolumeCounts := make(map[string]int)
	for _, metric := range diskMetrics {
		serverVolumeCounts[metric.Server]++
	}

	if len(serverVolumeCounts) < balanceConfig.MinServerCount {
		glog.V(1).Infof("BALANCE [%s]: No tasks created - too few servers (%d servers, need ≥%d)", diskType, len(serverVolumeCounts), balanceConfig.MinServerCount)
		return nil
	}

	// Track effective adjustments as we plan moves in this detection run
	adjustments := make(map[string]int)

	var results []*types.TaskDetectionResult

	for len(results) < maxResults {
		// Compute effective volume counts with adjustments from planned moves
		effectiveCounts := make(map[string]int, len(serverVolumeCounts))
		totalVolumes := 0
		for server, count := range serverVolumeCounts {
			effective := count + adjustments[server]
			if effective < 0 {
				effective = 0
			}
			effectiveCounts[server] = effective
			totalVolumes += effective
		}
		// Include any destination servers not in the original disk metrics
		for server, adj := range adjustments {
			if _, exists := serverVolumeCounts[server]; !exists && adj > 0 {
				effectiveCounts[server] = adj
				totalVolumes += adj
			}
		}

		avgVolumesPerServer := float64(totalVolumes) / float64(len(effectiveCounts))

		maxVolumes := 0
		minVolumes := totalVolumes
		maxServer := ""
		minServer := ""

		for server, count := range effectiveCounts {
			if count > maxVolumes {
				maxVolumes = count
				maxServer = server
			}
			if count < minVolumes {
				minVolumes = count
				minServer = server
			}
		}

		// Check if imbalance exceeds threshold
		imbalanceRatio := float64(maxVolumes-minVolumes) / avgVolumesPerServer
		if imbalanceRatio <= balanceConfig.ImbalanceThreshold {
			if len(results) == 0 {
				glog.Infof("BALANCE [%s]: No tasks created - cluster well balanced. Imbalance=%.1f%% (threshold=%.1f%%). Max=%d volumes on %s, Min=%d on %s, Avg=%.1f",
					diskType, imbalanceRatio*100, balanceConfig.ImbalanceThreshold*100, maxVolumes, maxServer, minVolumes, minServer, avgVolumesPerServer)
			} else {
				glog.Infof("BALANCE [%s]: Created %d task(s), cluster now balanced. Imbalance=%.1f%% (threshold=%.1f%%)",
					diskType, len(results), imbalanceRatio*100, balanceConfig.ImbalanceThreshold*100)
			}
			break
		}

		// Select a volume from the overloaded server for balance
		var selectedVolume *types.VolumeHealthMetrics
		for _, metric := range diskMetrics {
			if metric.Server == maxServer {
				// Skip volumes that already have a task in ActiveTopology
				if clusterInfo.ActiveTopology != nil && clusterInfo.ActiveTopology.HasAnyTask(metric.VolumeID) {
					continue
				}
				selectedVolume = metric
				break
			}
		}

		if selectedVolume == nil {
			glog.V(1).Infof("BALANCE [%s]: No more eligible volumes on overloaded server %s", diskType, maxServer)
			break
		}

		// Plan destination and create task
		task := createBalanceTask(diskType, selectedVolume, clusterInfo)
		if task == nil {
			break
		}

		results = append(results, task)

		// Adjust effective counts for the next iteration
		adjustments[maxServer]--
		if task.TypedParams != nil && len(task.TypedParams.Targets) > 0 {
			// Find the destination server ID from the planned task
			destAddress := task.TypedParams.Targets[0].Node
			destServer := findServerIDByAddress(diskMetrics, destAddress, clusterInfo)
			if destServer != "" {
				adjustments[destServer]++
			}
		}
	}

	return results
}

// findServerIDByAddress resolves a server address back to its server ID.
func findServerIDByAddress(diskMetrics []*types.VolumeHealthMetrics, address string, clusterInfo *types.ClusterInfo) string {
	// Check metrics first for a direct match
	for _, m := range diskMetrics {
		if m.ServerAddress == address || m.Server == address {
			return m.Server
		}
	}
	// Fall back to topology lookup
	if clusterInfo.ActiveTopology != nil {
		topologyInfo := clusterInfo.ActiveTopology.GetTopologyInfo()
		if topologyInfo != nil {
			for _, dc := range topologyInfo.DataCenterInfos {
				for _, rack := range dc.RackInfos {
					for _, dn := range rack.DataNodeInfos {
						if dn.Address == address || dn.Id == address {
							return dn.Id
						}
					}
				}
			}
		}
	}
	return ""
}

// createBalanceTask creates a single balance task for the selected volume.
// Returns nil if destination planning fails.
func createBalanceTask(diskType string, selectedVolume *types.VolumeHealthMetrics, clusterInfo *types.ClusterInfo) *types.TaskDetectionResult {
	taskID := fmt.Sprintf("balance_vol_%d_%d", selectedVolume.VolumeID, time.Now().Unix())

	task := &types.TaskDetectionResult{
		TaskID:     taskID,
		TaskType:   types.TaskTypeBalance,
		VolumeID:   selectedVolume.VolumeID,
		Server:     selectedVolume.Server,
		Collection: selectedVolume.Collection,
		Priority:   types.TaskPriorityNormal,
		Reason: fmt.Sprintf("Cluster imbalance detected for %s disk type",
			diskType),
		ScheduleAt: time.Now(),
	}

	// Plan destination if ActiveTopology is available
	if clusterInfo.ActiveTopology == nil {
		glog.Warningf("No ActiveTopology available for destination planning in balance detection")
		return nil
	}

	// Check if ANY task already exists in ActiveTopology for this volume
	if clusterInfo.ActiveTopology.HasAnyTask(selectedVolume.VolumeID) {
		glog.V(2).Infof("BALANCE [%s]: Skipping volume %d, task already exists in ActiveTopology", diskType, selectedVolume.VolumeID)
		return nil
	}

	destinationPlan, err := planBalanceDestination(clusterInfo.ActiveTopology, selectedVolume)
	if err != nil {
		glog.Warningf("Failed to plan balance destination for volume %d: %v", selectedVolume.VolumeID, err)
		return nil
	}

	// Find the actual disk containing the volume on the source server
	sourceDisk, found := base.FindVolumeDisk(clusterInfo.ActiveTopology, selectedVolume.VolumeID, selectedVolume.Collection, selectedVolume.Server)
	if !found {
		glog.Warningf("BALANCE [%s]: Could not find volume %d (collection: %s) on source server %s - unable to create balance task",
			diskType, selectedVolume.VolumeID, selectedVolume.Collection, selectedVolume.Server)
		return nil
	}

	// Update reason with full details now that we have destination info
	task.Reason = fmt.Sprintf("Cluster imbalance detected for %s: move volume %d from %s to %s",
		diskType, selectedVolume.VolumeID, selectedVolume.Server, destinationPlan.TargetNode)

	// Create typed parameters with unified source and target information
	task.TypedParams = &worker_pb.TaskParams{
		TaskId:     taskID,
		VolumeId:   selectedVolume.VolumeID,
		Collection: selectedVolume.Collection,
		VolumeSize: selectedVolume.Size,

		Sources: []*worker_pb.TaskSource{
			{
				Node:          selectedVolume.ServerAddress,
				DiskId:        sourceDisk,
				VolumeId:      selectedVolume.VolumeID,
				EstimatedSize: selectedVolume.Size,
				DataCenter:    selectedVolume.DataCenter,
				Rack:          selectedVolume.Rack,
			},
		},
		Targets: []*worker_pb.TaskTarget{
			{
				Node:          destinationPlan.TargetAddress,
				DiskId:        destinationPlan.TargetDisk,
				VolumeId:      selectedVolume.VolumeID,
				EstimatedSize: destinationPlan.ExpectedSize,
				DataCenter:    destinationPlan.TargetDC,
				Rack:          destinationPlan.TargetRack,
			},
		},

		TaskParams: &worker_pb.TaskParams_BalanceParams{
			BalanceParams: &worker_pb.BalanceTaskParams{
				ForceMove:      false,
				TimeoutSeconds: 600, // 10 minutes default
			},
		},
	}

	glog.V(1).Infof("Planned balance destination for volume %d: %s -> %s",
		selectedVolume.VolumeID, selectedVolume.Server, destinationPlan.TargetNode)

	// Add pending balance task to ActiveTopology for capacity management
	targetDisk := destinationPlan.TargetDisk

	err = clusterInfo.ActiveTopology.AddPendingTask(topology.TaskSpec{
		TaskID:     taskID,
		TaskType:   topology.TaskTypeBalance,
		VolumeID:   selectedVolume.VolumeID,
		VolumeSize: int64(selectedVolume.Size),
		Sources: []topology.TaskSourceSpec{
			{ServerID: selectedVolume.Server, DiskID: sourceDisk},
		},
		Destinations: []topology.TaskDestinationSpec{
			{ServerID: destinationPlan.TargetNode, DiskID: targetDisk},
		},
	})
	if err != nil {
		glog.Warningf("BALANCE [%s]: Failed to add pending task for volume %d: %v", diskType, selectedVolume.VolumeID, err)
		return nil
	}

	glog.V(2).Infof("Added pending balance task %s to ActiveTopology for volume %d: %s:%d -> %s:%d",
		taskID, selectedVolume.VolumeID, selectedVolume.Server, sourceDisk, destinationPlan.TargetNode, targetDisk)

	return task
}

// planBalanceDestination plans the destination for a balance operation
// This function implements destination planning logic directly in the detection phase
func planBalanceDestination(activeTopology *topology.ActiveTopology, selectedVolume *types.VolumeHealthMetrics) (*topology.DestinationPlan, error) {
	// Get source node information from topology
	var sourceRack, sourceDC string

	// Extract rack and DC from topology info
	topologyInfo := activeTopology.GetTopologyInfo()
	if topologyInfo != nil {
		for _, dc := range topologyInfo.DataCenterInfos {
			for _, rack := range dc.RackInfos {
				for _, dataNodeInfo := range rack.DataNodeInfos {
					if dataNodeInfo.Id == selectedVolume.Server {
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

	// Get available disks, excluding the source node
	availableDisks := activeTopology.GetAvailableDisks(topology.TaskTypeBalance, selectedVolume.Server)
	if len(availableDisks) == 0 {
		return nil, fmt.Errorf("no available disks for balance operation")
	}

	// Find the best destination disk based on balance criteria
	var bestDisk *topology.DiskInfo
	bestScore := -1.0

	for _, disk := range availableDisks {
		// Ensure disk type matches
		if disk.DiskType != selectedVolume.DiskType {
			continue
		}

		score := calculateBalanceScore(disk, sourceRack, sourceDC, selectedVolume.Size)
		if score > bestScore {
			bestScore = score
			bestDisk = disk
		}
	}

	if bestDisk == nil {
		return nil, fmt.Errorf("no suitable destination found for balance operation")
	}

	// Get the target server address
	targetAddress, err := util.ResolveServerAddress(bestDisk.NodeID, activeTopology)
	if err != nil {
		return nil, fmt.Errorf("failed to resolve address for target server %s: %v", bestDisk.NodeID, err)
	}

	return &topology.DestinationPlan{
		TargetNode:     bestDisk.NodeID,
		TargetAddress:  targetAddress,
		TargetDisk:     bestDisk.DiskID,
		TargetRack:     bestDisk.Rack,
		TargetDC:       bestDisk.DataCenter,
		ExpectedSize:   selectedVolume.Size,
		PlacementScore: bestScore,
	}, nil
}

// calculateBalanceScore calculates placement score for balance operations
func calculateBalanceScore(disk *topology.DiskInfo, sourceRack, sourceDC string, volumeSize uint64) float64 {
	if disk.DiskInfo == nil {
		return 0.0
	}

	score := 0.0

	// Prefer disks with lower current volume count (better for balance)
	if disk.DiskInfo.MaxVolumeCount > 0 {
		utilization := float64(disk.DiskInfo.VolumeCount) / float64(disk.DiskInfo.MaxVolumeCount)
		score += (1.0 - utilization) * 40.0 // Up to 40 points for low utilization
	}

	// Prefer different racks for better distribution
	if disk.Rack != sourceRack {
		score += 30.0
	}

	// Prefer different data centers for better distribution
	if disk.DataCenter != sourceDC {
		score += 20.0
	}

	// Prefer disks with lower current load
	score += (10.0 - float64(disk.LoadCount)) // Up to 10 points for low load

	return score
}
