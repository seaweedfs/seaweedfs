package balance

import (
	"fmt"
	"math"
	"sort"
	"strings"
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
	// Also collect MaxVolumeCount per server to compute utilization ratios.
	serverVolumeCounts := make(map[string]int)
	serverMaxVolumes := make(map[string]int64)
	if clusterInfo.ActiveTopology != nil {
		topologyInfo := clusterInfo.ActiveTopology.GetTopologyInfo()
		if topologyInfo != nil {
			var rackFilterSet, nodeFilterSet map[string]bool
			if balanceConfig.RackFilter != "" {
				rackFilterSet = parseCSVSet(balanceConfig.RackFilter)
			}
			if balanceConfig.NodeFilter != "" {
				nodeFilterSet = parseCSVSet(balanceConfig.NodeFilter)
			}
			for _, dc := range topologyInfo.DataCenterInfos {
				if balanceConfig.DataCenterFilter != "" && dc.Id != balanceConfig.DataCenterFilter {
					continue
				}
				for _, rack := range dc.RackInfos {
					if rackFilterSet != nil && !rackFilterSet[rack.Id] {
						continue
					}
					for _, node := range rack.DataNodeInfos {
						if nodeFilterSet != nil && !nodeFilterSet[node.Id] {
							continue
						}
						for diskTypeName, diskInfo := range node.DiskInfos {
							if diskTypeName == diskType {
								serverVolumeCounts[node.Id] = 0
								serverMaxVolumes[node.Id] += diskInfo.MaxVolumeCount
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

	// Decide upfront whether all servers have MaxVolumeCount info.
	// If any server is missing it, fall back to raw counts for ALL servers
	// to avoid mixing utilization ratios (0.0–1.0) with raw counts.
	allServersHaveMaxInfo := true
	for _, server := range sortedServers {
		if maxVol, ok := serverMaxVolumes[server]; !ok || maxVol <= 0 {
			allServersHaveMaxInfo = false
			glog.V(1).Infof("BALANCE [%s]: Server %s is missing MaxVolumeCount info, falling back to raw volume counts for balancing", diskType, server)
			break
		}
	}

	var serverUtilization func(server string, effectiveCount int) float64
	if allServersHaveMaxInfo {
		serverUtilization = func(server string, effectiveCount int) float64 {
			return float64(effectiveCount) / float64(serverMaxVolumes[server])
		}
	} else {
		serverUtilization = func(_ string, effectiveCount int) float64 {
			return float64(effectiveCount)
		}
	}

	for len(results) < maxResults {
		// Compute effective volume counts with adjustments from planned moves
		effectiveCounts := make(map[string]int, len(serverVolumeCounts))
		for server, count := range serverVolumeCounts {
			effective := count + adjustments[server]
			if effective < 0 {
				effective = 0
			}
			effectiveCounts[server] = effective
		}

		// Find the most and least utilized servers using utilization ratio
		// (volumes / maxVolumes) so that servers with higher capacity are
		// expected to hold proportionally more volumes.
		maxUtilization := -1.0
		minUtilization := math.Inf(1)
		maxServer := ""
		minServer := ""

		for _, server := range sortedServers {
			count := effectiveCounts[server]
			util := serverUtilization(server, count)
			// Min is calculated across all servers for an accurate imbalance ratio
			if util < minUtilization {
				minUtilization = util
				minServer = server
			}
			// Max is only among non-exhausted servers since we can only move from them
			if exhaustedServers[server] {
				continue
			}
			if util > maxUtilization {
				maxUtilization = util
				maxServer = server
			}
		}

		if maxServer == "" {
			// All servers exhausted
			glog.V(1).Infof("BALANCE [%s]: All overloaded servers exhausted after %d task(s)", diskType, len(results))
			break
		}

		// Check if utilization imbalance exceeds threshold.
		// imbalanceRatio is the difference between the most and least utilized
		// servers, expressed as a fraction of mean utilization.
		avgUtilization := (maxUtilization + minUtilization) / 2.0
		var imbalanceRatio float64
		if avgUtilization > 0 {
			imbalanceRatio = (maxUtilization - minUtilization) / avgUtilization
		}
		if imbalanceRatio <= balanceConfig.ImbalanceThreshold {
			if len(results) == 0 {
				glog.Infof("BALANCE [%s]: No tasks created - cluster well balanced. Imbalance=%.1f%% (threshold=%.1f%%). MaxUtil=%.1f%% on %s, MinUtil=%.1f%% on %s",
					diskType, imbalanceRatio*100, balanceConfig.ImbalanceThreshold*100, maxUtilization*100, maxServer, minUtilization*100, minServer)
			} else {
				glog.Infof("BALANCE [%s]: Created %d task(s), cluster now balanced. Imbalance=%.1f%% (threshold=%.1f%%)",
					diskType, len(results), imbalanceRatio*100, balanceConfig.ImbalanceThreshold*100)
			}
			balanced = true
			break
		}

		// When the global max and min effective counts differ by at most 1,
		// no single move can improve balance — it would just swap which server
		// is min vs max. Stop here to avoid infinite oscillation when the
		// threshold is unachievable (e.g., 11 vols across 4 servers: best is
		// 3/3/3/2, imbalance=36%). We scan ALL servers' effective counts so the
		// check works regardless of whether utilization or raw counts are used.
		globalMaxCount, globalMinCount := 0, math.MaxInt
		for _, c := range effectiveCounts {
			if c > globalMaxCount {
				globalMaxCount = c
			}
			if c < globalMinCount {
				globalMinCount = c
			}
		}
		if globalMaxCount-globalMinCount <= 1 {
			if len(results) == 0 {
				glog.Infof("BALANCE [%s]: No tasks created - cluster as balanced as possible. Imbalance=%.1f%% (threshold=%.1f%%), but max-min diff is %d",
					diskType, imbalanceRatio*100, balanceConfig.ImbalanceThreshold*100, globalMaxCount-globalMinCount)
			} else {
				glog.Infof("BALANCE [%s]: Created %d task(s), cluster as balanced as possible. Imbalance=%.1f%% (threshold=%.1f%%), max-min diff=%d",
					diskType, len(results), imbalanceRatio*100, balanceConfig.ImbalanceThreshold*100, globalMaxCount-globalMinCount)
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

		// Create task targeting minServer — the greedy algorithm's natural choice.
		// Using minServer instead of letting planBalanceDestination independently
		// pick a destination ensures that the detection loop's effective counts
		// and the destination selection stay in sync. Without this, the topology's
		// LoadCount-based scoring can diverge from the adjustment-based effective
		// counts, causing moves to pile onto one server or oscillate (A→B, B→A).
		task, destServerID := createBalanceTask(diskType, selectedVolume, clusterInfo, minServer)
		if task == nil {
			glog.V(1).Infof("BALANCE [%s]: Cannot plan task for volume %d on server %s, trying next volume", diskType, selectedVolume.VolumeID, maxServer)
			continue
		}

		results = append(results, task)

		// Adjust effective counts for the next iteration.
		adjustments[maxServer]--
		if destServerID != "" {
			adjustments[destServerID]++
			// If the destination server wasn't in serverVolumeCounts (e.g., a
			// server with 0 volumes not seeded from topology), add it so
			// subsequent iterations include it in effective/average/min/max.
			if _, exists := serverVolumeCounts[destServerID]; !exists {
				serverVolumeCounts[destServerID] = 0
				sortedServers = append(sortedServers, destServerID)
				sort.Strings(sortedServers)
			}
		}
	}

	// Truncated only if we hit maxResults and detection didn't naturally finish
	truncated := len(results) >= maxResults && !balanced
	return results, truncated
}

// createBalanceTask creates a single balance task for the selected volume.
// targetServer is the server ID chosen by the detection loop's greedy algorithm.
// Returns (nil, "") if destination planning fails.
// On success, returns the task result and the canonical destination server ID.
func createBalanceTask(diskType string, selectedVolume *types.VolumeHealthMetrics, clusterInfo *types.ClusterInfo, targetServer string) (*types.TaskDetectionResult, string) {
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

	// Resolve the target server chosen by the detection loop's effective counts.
	// This keeps destination selection in sync with the greedy algorithm rather
	// than relying on topology LoadCount which can diverge across iterations.
	destinationPlan, err := resolveBalanceDestination(clusterInfo.ActiveTopology, selectedVolume, targetServer)
	if err != nil {
		// Fall back to score-based planning if the preferred target can't be resolved
		glog.V(1).Infof("BALANCE [%s]: Cannot resolve target %s for volume %d, falling back to score-based planning: %v",
			diskType, targetServer, selectedVolume.VolumeID, err)
		destinationPlan, err = planBalanceDestination(clusterInfo.ActiveTopology, selectedVolume)
		if err != nil {
			glog.Warningf("Failed to plan balance destination for volume %d: %v", selectedVolume.VolumeID, err)
			return nil, ""
		}
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

// resolveBalanceDestination resolves the destination for a balance operation
// when the target server is already known (chosen by the detection loop's
// effective volume counts). It finds the appropriate disk and address for the
// target server in the topology.
func resolveBalanceDestination(activeTopology *topology.ActiveTopology, selectedVolume *types.VolumeHealthMetrics, targetServer string) (*topology.DestinationPlan, error) {
	topologyInfo := activeTopology.GetTopologyInfo()
	if topologyInfo == nil {
		return nil, fmt.Errorf("no topology info available")
	}

	// Find the target node in the topology and get its disk info
	for _, dc := range topologyInfo.DataCenterInfos {
		for _, rack := range dc.RackInfos {
			for _, node := range rack.DataNodeInfos {
				if node.Id != targetServer {
					continue
				}
				// Find an available disk matching the volume's disk type
				for diskTypeName, diskInfo := range node.DiskInfos {
					if diskTypeName != selectedVolume.DiskType {
						continue
					}
					if diskInfo.MaxVolumeCount > 0 && diskInfo.VolumeCount >= diskInfo.MaxVolumeCount {
						continue // disk is full
					}
					targetAddress, err := util.ResolveServerAddress(node.Id, activeTopology)
					if err != nil {
						return nil, fmt.Errorf("failed to resolve address for target server %s: %v", node.Id, err)
					}
					return &topology.DestinationPlan{
						TargetNode:    node.Id,
						TargetAddress: targetAddress,
						TargetDisk:    diskInfo.DiskId,
						TargetRack:    rack.Id,
						TargetDC:      dc.Id,
						ExpectedSize:  selectedVolume.Size,
					}, nil
				}
				return nil, fmt.Errorf("target server %s has no available disk of type %s", targetServer, selectedVolume.DiskType)
			}
		}
	}
	return nil, fmt.Errorf("target server %s not found in topology", targetServer)
}

// planBalanceDestination plans the destination for a balance operation using
// score-based selection. Used as a fallback when the preferred target cannot
// be resolved, and for single-move scenarios outside the detection loop.
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

// parseCSVSet splits a comma-separated string into a set of trimmed, non-empty values.
func parseCSVSet(csv string) map[string]bool {
	set := make(map[string]bool)
	for _, item := range strings.Split(csv, ",") {
		trimmed := strings.TrimSpace(item)
		if trimmed != "" {
			set[trimmed] = true
		}
	}
	return set
}
