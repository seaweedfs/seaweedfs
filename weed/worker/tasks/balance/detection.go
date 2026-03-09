package balance

import (
	"fmt"
	"math"
	"sort"
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
// A non-positive maxResults means no explicit limit (uses a large default).
// The returned truncated flag is true when detection stopped because it hit
// maxResults rather than running out of work.
func Detection(metrics []*types.VolumeHealthMetrics, clusterInfo *types.ClusterInfo, config base.TaskConfig, maxResults int) ([]*types.TaskDetectionResult, bool, error) {
	if !config.IsEnabled() {
		return nil, false, nil
	}
	if clusterInfo == nil {
		return nil, false, nil
	}

	balanceConfig := config.(*Config)

	if maxResults <= 0 {
		maxResults = math.MaxInt32
	}

	// Group volumes by disk type to ensure we compare apples to apples
	volumesByDiskType := make(map[string][]*types.VolumeHealthMetrics)
	for _, metric := range metrics {
		volumesByDiskType[metric.DiskType] = append(volumesByDiskType[metric.DiskType], metric)
	}

	// Sort disk types for deterministic iteration order when maxResults
	// spans multiple disk types.
	diskTypes := make([]string, 0, len(volumesByDiskType))
	for dt := range volumesByDiskType {
		diskTypes = append(diskTypes, dt)
	}
	sort.Strings(diskTypes)

	var allParams []*types.TaskDetectionResult
	truncated := false

	for _, diskType := range diskTypes {
		remaining := maxResults - len(allParams)
		if remaining <= 0 {
			truncated = true
			break
		}
		tasks, diskTruncated := detectForDiskType(diskType, volumesByDiskType[diskType], balanceConfig, clusterInfo, remaining)
		allParams = append(allParams, tasks...)
		if diskTruncated {
			truncated = true
		}
	}

	return allParams, truncated, nil
}

// detectForDiskType performs balance detection for a specific disk type,
// returning up to maxResults balance tasks and whether it was truncated by the limit.
func detectForDiskType(diskType string, diskMetrics []*types.VolumeHealthMetrics, balanceConfig *Config, clusterInfo *types.ClusterInfo, maxResults int) ([]*types.TaskDetectionResult, bool) {
	// Skip if cluster segment is too small
	minVolumeCount := 2 // More reasonable for small clusters
	if len(diskMetrics) < minVolumeCount {
		// Only log at verbose level to avoid spamming for small/empty disk types
		glog.V(1).Infof("BALANCE [%s]: No tasks created - cluster too small (%d volumes, need ≥%d)", diskType, len(diskMetrics), minVolumeCount)
		return nil, false
	}

	// Analyze volume distribution across servers.
	// Seed from ActiveTopology so servers with matching disk type but zero
	// volumes are included in the count and imbalance calculation.
	serverVolumeCounts := make(map[string]int)
	if clusterInfo.ActiveTopology != nil {
		topologyInfo := clusterInfo.ActiveTopology.GetTopologyInfo()
		if topologyInfo != nil {
			for _, dc := range topologyInfo.DataCenterInfos {
				for _, rack := range dc.RackInfos {
					for _, node := range rack.DataNodeInfos {
						for diskTypeName := range node.DiskInfos {
							if diskTypeName == diskType {
								serverVolumeCounts[node.Id] = 0
							}
						}
					}
				}
			}
		}
	}
	for _, metric := range diskMetrics {
		serverVolumeCounts[metric.Server]++
	}

	if len(serverVolumeCounts) < balanceConfig.MinServerCount {
		glog.V(1).Infof("BALANCE [%s]: No tasks created - too few servers (%d servers, need ≥%d)", diskType, len(serverVolumeCounts), balanceConfig.MinServerCount)
		return nil, false
	}

	// Seed adjustments from existing pending/assigned balance tasks so that
	// effectiveCounts reflects in-flight moves and prevents over-scheduling.
	var adjustments map[string]int
	if clusterInfo.ActiveTopology != nil {
		adjustments = clusterInfo.ActiveTopology.GetTaskServerAdjustments(topology.TaskTypeBalance)
	}
	if adjustments == nil {
		adjustments = make(map[string]int)
	}
	// Servers where we can no longer find eligible volumes or plan destinations
	exhaustedServers := make(map[string]bool)

	// Sort servers for deterministic iteration and tie-breaking
	sortedServers := make([]string, 0, len(serverVolumeCounts))
	for server := range serverVolumeCounts {
		sortedServers = append(sortedServers, server)
	}
	sort.Strings(sortedServers)

	// Pre-index volumes by server with cursors to avoid O(maxResults * volumes) scanning.
	// Sort each server's volumes by VolumeID for deterministic selection.
	volumesByServer := make(map[string][]*types.VolumeHealthMetrics, len(serverVolumeCounts))
	for _, metric := range diskMetrics {
		volumesByServer[metric.Server] = append(volumesByServer[metric.Server], metric)
	}
	for _, vols := range volumesByServer {
		sort.Slice(vols, func(i, j int) bool {
			return vols[i].VolumeID < vols[j].VolumeID
		})
	}
	serverCursors := make(map[string]int, len(serverVolumeCounts))

	var results []*types.TaskDetectionResult
	balanced := false

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
		avgVolumesPerServer := float64(totalVolumes) / float64(len(effectiveCounts))

		maxVolumes := 0
		minVolumes := totalVolumes
		maxServer := ""
		minServer := ""

		for _, server := range sortedServers {
			count := effectiveCounts[server]
			// Min is calculated across all servers for an accurate imbalance ratio
			if count < minVolumes {
				minVolumes = count
				minServer = server
			}
			// Max is only among non-exhausted servers since we can only move from them
			if exhaustedServers[server] {
				continue
			}
			if count > maxVolumes {
				maxVolumes = count
				maxServer = server
			}
		}

		if maxServer == "" {
			// All servers exhausted
			glog.V(1).Infof("BALANCE [%s]: All overloaded servers exhausted after %d task(s)", diskType, len(results))
			break
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
			balanced = true
			break
		}

		// Select a volume from the overloaded server using per-server cursor
		var selectedVolume *types.VolumeHealthMetrics
		serverVols := volumesByServer[maxServer]
		cursor := serverCursors[maxServer]
		for cursor < len(serverVols) {
			metric := serverVols[cursor]
			cursor++
			// Skip volumes that already have a task in ActiveTopology
			if clusterInfo.ActiveTopology != nil && clusterInfo.ActiveTopology.HasAnyTask(metric.VolumeID) {
				continue
			}
			selectedVolume = metric
			break
		}
		serverCursors[maxServer] = cursor

		if selectedVolume == nil {
			glog.V(1).Infof("BALANCE [%s]: No more eligible volumes on overloaded server %s, trying other servers", diskType, maxServer)
			exhaustedServers[maxServer] = true
			continue
		}

		// Plan destination and create task.
		// On failure, continue to the next volume on the same server rather
		// than exhausting the entire server — the failure may be per-volume
		// (e.g., volume not found in topology, AddPendingTask failed).
		task, destServerID := createBalanceTask(diskType, selectedVolume, clusterInfo)
		if task == nil {
			glog.V(1).Infof("BALANCE [%s]: Cannot plan task for volume %d on server %s, trying next volume", diskType, selectedVolume.VolumeID, maxServer)
			continue
		}

		results = append(results, task)

		// Adjust effective counts for the next iteration
		adjustments[maxServer]--
		if destServerID != "" {
			adjustments[destServerID]++
		}
	}

	// Truncated only if we hit maxResults and detection didn't naturally finish
	truncated := len(results) >= maxResults && !balanced
	return results, truncated
}

// createBalanceTask creates a single balance task for the selected volume.
// Returns (nil, "") if destination planning fails.
// On success, returns the task result and the canonical destination server ID.
func createBalanceTask(diskType string, selectedVolume *types.VolumeHealthMetrics, clusterInfo *types.ClusterInfo) (*types.TaskDetectionResult, string) {
	taskID := fmt.Sprintf("balance_vol_%d_%d", selectedVolume.VolumeID, time.Now().UnixNano())

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
		return nil, ""
	}

	destinationPlan, err := planBalanceDestination(clusterInfo.ActiveTopology, selectedVolume)
	if err != nil {
		glog.Warningf("Failed to plan balance destination for volume %d: %v", selectedVolume.VolumeID, err)
		return nil, ""
	}

	// Find the actual disk containing the volume on the source server
	sourceDisk, found := base.FindVolumeDisk(clusterInfo.ActiveTopology, selectedVolume.VolumeID, selectedVolume.Collection, selectedVolume.Server)
	if !found {
		glog.Warningf("BALANCE [%s]: Could not find volume %d (collection: %s) on source server %s - unable to create balance task",
			diskType, selectedVolume.VolumeID, selectedVolume.Collection, selectedVolume.Server)
		return nil, ""
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
		return nil, ""
	}

	glog.V(2).Infof("Added pending balance task %s to ActiveTopology for volume %d: %s:%d -> %s:%d",
		taskID, selectedVolume.VolumeID, selectedVolume.Server, sourceDisk, destinationPlan.TargetNode, targetDisk)

	return task, destinationPlan.TargetNode
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

	// Sort available disks by NodeID then DiskID for deterministic tie-breaking
	sort.Slice(availableDisks, func(i, j int) bool {
		if availableDisks[i].NodeID != availableDisks[j].NodeID {
			return availableDisks[i].NodeID < availableDisks[j].NodeID
		}
		return availableDisks[i].DiskID < availableDisks[j].DiskID
	})

	// Find the best destination disk based on balance criteria
	var bestDisk *topology.DiskInfo
	bestScore := math.Inf(-1)

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

// calculateBalanceScore calculates placement score for balance operations.
// LoadCount reflects pending+assigned tasks on the disk, so we factor it into
// the utilization estimate to avoid stacking multiple moves onto the same target.
func calculateBalanceScore(disk *topology.DiskInfo, sourceRack, sourceDC string, volumeSize uint64) float64 {
	if disk.DiskInfo == nil {
		return 0.0
	}

	score := 0.0

	// Prefer disks with lower effective volume count (current + pending moves).
	// LoadCount is included so that disks already targeted by planned moves
	// appear more utilized, naturally spreading work across targets.
	if disk.DiskInfo.MaxVolumeCount > 0 {
		effectiveVolumeCount := float64(disk.DiskInfo.VolumeCount) + float64(disk.LoadCount)
		utilization := effectiveVolumeCount / float64(disk.DiskInfo.MaxVolumeCount)
		score += (1.0 - utilization) * 50.0 // Up to 50 points for low utilization
	}

	// Prefer different racks for better distribution
	if disk.Rack != sourceRack {
		score += 30.0
	}

	// Prefer different data centers for better distribution
	if disk.DataCenter != sourceDC {
		score += 20.0
	}

	return score
}
