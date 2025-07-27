package task

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/wdclient"
	"github.com/seaweedfs/seaweedfs/weed/worker/tasks/erasure_coding"
	"github.com/seaweedfs/seaweedfs/weed/worker/tasks/vacuum"
	"github.com/seaweedfs/seaweedfs/weed/worker/types"
)

// MasterSynchronizer handles periodic synchronization with the master server
type MasterSynchronizer struct {
	masterClient       *wdclient.MasterClient
	volumeStateManager *VolumeStateManager
	adminServer        *AdminServer
	syncInterval       time.Duration
	stopCh             chan struct{}
	volumeSizeLimitMB  uint64 // Volume size limit from master in MB
}

// NewMasterSynchronizer creates a new master synchronizer
func NewMasterSynchronizer(masterClient *wdclient.MasterClient, vsm *VolumeStateManager, admin *AdminServer) *MasterSynchronizer {
	return &MasterSynchronizer{
		masterClient:       masterClient,
		volumeStateManager: vsm,
		adminServer:        admin,
		syncInterval:       30 * time.Second, // Default 30 second sync interval
		stopCh:             make(chan struct{}),
	}
}

// Start begins the periodic master synchronization
func (ms *MasterSynchronizer) Start() {
	glog.Infof("Starting master synchronization with interval %v", ms.syncInterval)

	go func() {
		// Immediate sync on startup
		ms.performSync()

		ticker := time.NewTicker(ms.syncInterval)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				ms.performSync()
			case <-ms.stopCh:
				glog.Infof("Master synchronization stopped")
				return
			}
		}
	}()
}

// Stop stops the master synchronization
func (ms *MasterSynchronizer) Stop() {
	close(ms.stopCh)
}

// performSync executes a single synchronization cycle
func (ms *MasterSynchronizer) performSync() {
	glog.V(1).Infof("Starting master sync cycle")
	startTime := time.Now()

	// Get volume list from master
	volumeData, err := ms.getVolumeListFromMaster()
	if err != nil {
		glog.Errorf("Failed to get volume list from master: %v", err)
		return
	}

	// Update volume size limit from master
	if volumeData.VolumeSizeLimitMb > 0 {
		ms.volumeSizeLimitMB = volumeData.VolumeSizeLimitMb
		glog.V(2).Infof("Updated volume size limit to %d MB from master", ms.volumeSizeLimitMB)
	}

	// Merge data into volume state manager
	err = ms.mergeVolumeData(volumeData)
	if err != nil {
		glog.Errorf("Failed to merge volume data: %v", err)
		return
	}

	// Detect volumes needing work
	candidates := ms.detectMaintenanceCandidates(volumeData)

	// Process candidates for task assignment
	ms.processCandidates(candidates)

	duration := time.Since(startTime)
	glog.V(1).Infof("Master sync completed in %v, found %d maintenance candidates",
		duration, len(candidates))
}

// getVolumeListFromMaster retrieves the current volume topology from master
func (ms *MasterSynchronizer) getVolumeListFromMaster() (*master_pb.VolumeListResponse, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	err := ms.masterClient.WithClient(false, func(client master_pb.SeaweedClient) error {
		req := &master_pb.VolumeListRequest{}
		response, err := client.VolumeList(ctx, req)
		if err != nil {
			return fmt.Errorf("VolumeList RPC failed: %v", err)
		}
		volumeData = response
		return nil
	})

	if err != nil {
		return nil, err
	}

	return volumeData, nil
}

// VolumeMaintenanceCandidate represents a volume that needs maintenance
type VolumeMaintenanceCandidate struct {
	VolumeID    uint32
	Server      string
	TaskType    string
	Priority    TaskPriority
	Reason      string
	VolumeInfo  *VolumeInfo
	ECShardInfo map[int]*ShardInfo
}

// mergeVolumeData merges master volume data into the volume state manager
func (ms *MasterSynchronizer) mergeVolumeData(data *master_pb.VolumeListResponse) error {
	if data.TopologyInfo == nil {
		return fmt.Errorf("empty topology info from master")
	}

	volumes := make(map[uint32]*VolumeInfo)
	ecShards := make(map[uint32]map[int]*ShardInfo)
	serverCapacity := make(map[string]*CapacityInfo)

	// Extract volume information from topology
	ms.extractVolumesFromTopology(data.TopologyInfo, volumes, ecShards, serverCapacity)

	// Update volume state manager
	err := ms.volumeStateManager.SyncWithMasterData(volumes, ecShards, serverCapacity)
	if err != nil {
		return fmt.Errorf("failed to sync with volume state manager: %v", err)
	}

	glog.V(2).Infof("Synced %d volumes, %d EC volume groups, %d servers",
		len(volumes), len(ecShards), len(serverCapacity))

	return nil
}

// extractVolumesFromTopology extracts volume and capacity data from master topology
func (ms *MasterSynchronizer) extractVolumesFromTopology(
	topology *master_pb.TopologyInfo,
	volumes map[uint32]*VolumeInfo,
	ecShards map[uint32]map[int]*ShardInfo,
	serverCapacity map[string]*CapacityInfo) {

	for _, dcInfo := range topology.DataCenterInfos {
		for _, rackInfo := range dcInfo.RackInfos {
			for _, nodeInfo := range rackInfo.DataNodeInfos {
				serverID := fmt.Sprintf("%s:%d", nodeInfo.Id, nodeInfo.GrpcPort)

				// Initialize server capacity info
				if serverCapacity[serverID] == nil {
					serverCapacity[serverID] = &CapacityInfo{
						Server: serverID,
					}
				}

				// Process disk information
				for diskType, diskInfo := range nodeInfo.DiskInfos {
					ms.processDiskInfo(diskInfo, diskType, serverID, volumes, ecShards, serverCapacity)
				}
			}
		}
	}
}

// processDiskInfo processes disk information for a specific server
func (ms *MasterSynchronizer) processDiskInfo(
	diskInfo *master_pb.DiskInfo,
	diskType string,
	serverID string,
	volumes map[uint32]*VolumeInfo,
	ecShards map[uint32]map[int]*ShardInfo,
	serverCapacity map[string]*CapacityInfo) {

	// Update capacity information
	capacity := serverCapacity[serverID]
	volumeSizeBytes := int64(ms.volumeSizeLimitMB) * 1024 * 1024 // Convert MB to bytes
	capacity.TotalCapacity += int64(diskInfo.MaxVolumeCount) * volumeSizeBytes
	capacity.UsedCapacity += int64(diskInfo.ActiveVolumeCount) * volumeSizeBytes

	// Process regular volumes
	for _, volInfo := range diskInfo.VolumeInfos {
		volumes[volInfo.Id] = &VolumeInfo{
			ID:               volInfo.Id,
			Size:             volInfo.Size,
			Collection:       volInfo.Collection,
			FileCount:        volInfo.FileCount,
			DeleteCount:      volInfo.DeleteCount,
			DeletedByteCount: volInfo.DeletedByteCount,
			ReadOnly:         volInfo.ReadOnly,
			Server:           serverID,
			DiskType:         diskType,
			ModifiedAtSecond: volInfo.ModifiedAtSecond,
		}
	}

	// Process EC shards
	for _, shardInfo := range diskInfo.EcShardInfos {
		volumeID := shardInfo.Id
		if ecShards[volumeID] == nil {
			ecShards[volumeID] = make(map[int]*ShardInfo)
		}

		// Extract shard IDs from ec_index_bits
		for shardID := 0; shardID < 14; shardID++ {
			if (shardInfo.EcIndexBits & (1 << uint(shardID))) != 0 {
				ecShards[volumeID][shardID] = &ShardInfo{
					ShardID: shardID,
					Server:  serverID,
					Status:  ShardStatusExists,
					Size:    0, // Size not available in shard info
				}
			}
		}
	}
}

// detectMaintenanceCandidates identifies volumes that need maintenance
func (ms *MasterSynchronizer) detectMaintenanceCandidates(data *master_pb.VolumeListResponse) []*VolumeMaintenanceCandidate {
	var candidates []*VolumeMaintenanceCandidate

	// Get current volume states
	currentVolumes := ms.volumeStateManager.GetAllVolumeStates()

	for volumeID, volumeState := range currentVolumes {
		// Skip volumes with in-progress tasks
		if len(volumeState.InProgressTasks) > 0 {
			continue
		}

		// Check for EC encoding candidates
		if candidate := ms.checkECEncodingCandidate(volumeID, volumeState); candidate != nil {
			candidates = append(candidates, candidate)
		}

		// Check for vacuum candidates
		if candidate := ms.checkVacuumCandidate(volumeID, volumeState); candidate != nil {
			candidates = append(candidates, candidate)
		}

		// Check for EC rebuild candidates
		if candidate := ms.checkECRebuildCandidate(volumeID, volumeState); candidate != nil {
			candidates = append(candidates, candidate)
		}
	}

	return candidates
}

// EC encoding criteria - using configuration from EC detector
func (ms *MasterSynchronizer) checkECEncodingCandidate(volumeID uint32, state *VolumeState) *VolumeMaintenanceCandidate {
	volume := state.CurrentState
	if volume == nil {
		return nil
	}

	// Get the current configuration from the EC detector
	ecDetector, _ := erasure_coding.GetSharedInstances()
	if ecDetector == nil || !ecDetector.IsEnabled() {
		return nil
	}

	// Get configuration values from the detector
	fullnessThreshold := ecDetector.GetFullnessRatio()
	quietForSeconds := ecDetector.GetQuietForSeconds()
	collectionFilter := ecDetector.GetCollectionFilter()

	// EC encoding criteria:
	// 1. Volume meets fullness ratio threshold
	// 2. Volume has been quiet for required duration
	// 3. Collection filter matches (if specified)
	// 4. Not already EC encoded

	// Check fullness ratio (if we have size info)
	if volume.Size == 0 {
		return nil
	}

	// Calculate fullness ratio (assuming total capacity is close to actual size for near-full volumes)
	// For a more accurate calculation, we'd need the volume's max capacity
	fullnessRatio := float64(volume.Size-volume.DeletedByteCount) / float64(volume.Size)
	if fullnessRatio < fullnessThreshold {
		return nil
	}

	// Check collection filter if specified
	if collectionFilter != "" {
		// Parse comma-separated collections
		allowedCollections := make(map[string]bool)
		for _, collection := range strings.Split(collectionFilter, ",") {
			allowedCollections[strings.TrimSpace(collection)] = true
		}
		// Skip if volume's collection is not in the allowed list
		if !allowedCollections[volume.Collection] {
			return nil
		}
	}

	// Check quiet duration using volume's last modification time
	now := time.Now()
	lastModified := time.Unix(volume.ModifiedAtSecond, 0)
	timeSinceModification := now.Sub(lastModified)

	if timeSinceModification < time.Duration(quietForSeconds)*time.Second {
		return nil // Volume hasn't been quiet long enough
	}

	return &VolumeMaintenanceCandidate{
		VolumeID:   volumeID,
		Server:     volume.Server,
		TaskType:   "ec_encode",
		Priority:   types.TaskPriorityLow, // EC is typically low priority
		Reason:     fmt.Sprintf("Volume meets EC criteria: fullness=%.1f%% (>%.1f%%), quiet for %s (>%ds), collection='%s'", fullnessRatio*100, fullnessThreshold*100, timeSinceModification.Truncate(time.Second), quietForSeconds, volume.Collection),
		VolumeInfo: volume,
	}
}

// checkVacuumCandidate checks if a volume is a candidate for vacuum
func (ms *MasterSynchronizer) checkVacuumCandidate(volumeID uint32, state *VolumeState) *VolumeMaintenanceCandidate {
	volume := state.CurrentState
	if volume == nil || volume.ReadOnly {
		return nil
	}

	// Get the current configuration from the vacuum detector
	vacuumDetector, _ := vacuum.GetSharedInstances()
	if vacuumDetector == nil || !vacuumDetector.IsEnabled() {
		return nil
	}

	// Get configuration values from the detector
	garbageThreshold := vacuumDetector.GetGarbageThreshold()
	minVolumeAge := vacuumDetector.GetMinVolumeAge()

	// Vacuum criteria:
	// 1. Volume meets garbage threshold
	// 2. Volume is old enough (respects minimum age)
	// 3. Volume has sufficient size

	// Check minimum volume size (avoid vacuum on tiny volumes)
	if volume.Size == 0 {
		return nil
	}

	// Check garbage ratio
	deletedRatio := float64(volume.DeletedByteCount) / float64(volume.Size)
	if deletedRatio < garbageThreshold {
		return nil
	}

	// Check minimum volume age using volume's last modification time
	now := time.Now()
	lastModified := time.Unix(volume.ModifiedAtSecond, 0)
	volumeAge := now.Sub(lastModified)

	if volumeAge < minVolumeAge {
		return nil // Volume is too new for vacuum
	}

	// Determine priority based on garbage ratio
	priority := types.TaskPriorityNormal
	if deletedRatio > 0.6 { // High garbage ratio gets higher priority
		priority = types.TaskPriorityHigh
	}

	return &VolumeMaintenanceCandidate{
		VolumeID: volumeID,
		Server:   volume.Server,
		TaskType: "vacuum",
		Priority: priority,
		Reason: fmt.Sprintf("Volume meets vacuum criteria: garbage=%.1f%% (>%.1f%%), age=%s (>%s)",
			deletedRatio*100, garbageThreshold*100, volumeAge.Truncate(time.Second), minVolumeAge.Truncate(time.Second)),
		VolumeInfo: volume,
	}
}

// checkECRebuildCandidate checks if an EC volume needs shard rebuilding
func (ms *MasterSynchronizer) checkECRebuildCandidate(volumeID uint32, state *VolumeState) *VolumeMaintenanceCandidate {
	// For now, skip EC rebuild detection as it requires more complex shard state tracking
	// This would be implemented when the volume state manager provides proper EC shard access
	return nil
}

// processCandidates attempts to assign tasks for maintenance candidates
func (ms *MasterSynchronizer) processCandidates(candidates []*VolumeMaintenanceCandidate) {
	for _, candidate := range candidates {
		// Check if we can assign this task
		if !ms.canAssignCandidate(candidate) {
			glog.V(2).Infof("Cannot assign task for volume %d: insufficient capacity or no workers",
				candidate.VolumeID)
			continue
		}

		// Create and queue the task
		task := ms.createTaskFromCandidate(candidate)
		if task != nil {
			ms.adminServer.QueueTask(task)
			glog.V(1).Infof("Queued %s task for volume %d on server %s: %s",
				candidate.TaskType, candidate.VolumeID, candidate.Server, candidate.Reason)
		}
	}
}

// canAssignCandidate checks if a candidate can be assigned (capacity, workers available)
func (ms *MasterSynchronizer) canAssignCandidate(candidate *VolumeMaintenanceCandidate) bool {
	// Check if server has capacity for the task
	if candidate.TaskType == "ec_encode" {
		// EC encoding requires significant temporary space
		requiredSpace := int64(candidate.VolumeInfo.Size * 2) // Estimate 2x volume size needed
		if !ms.volumeStateManager.CanAssignVolumeToServer(requiredSpace, candidate.Server) {
			return false
		}
	}

	// Check if we have workers capable of this task type
	availableWorkers := ms.adminServer.GetAvailableWorkers(candidate.TaskType)
	if len(availableWorkers) == 0 {
		return false
	}

	return true
}

// createTaskFromCandidate creates a task from a maintenance candidate
func (ms *MasterSynchronizer) createTaskFromCandidate(candidate *VolumeMaintenanceCandidate) *Task {
	now := time.Now()

	task := &Task{
		ID:        generateTaskID(),
		Type:      TaskType(candidate.TaskType),
		VolumeID:  candidate.VolumeID,
		Priority:  candidate.Priority,
		Status:    types.TaskStatusPending,
		CreatedAt: now,
		Parameters: map[string]interface{}{
			"volume_id": fmt.Sprintf("%d", candidate.VolumeID),
			"server":    candidate.Server,
			"reason":    candidate.Reason,
		},
	}

	// Add task-specific parameters
	switch candidate.TaskType {
	case "ec_encode":
		task.Parameters["replication"] = "001" // Default replication for EC
		task.Parameters["collection"] = candidate.VolumeInfo.Collection
	case "vacuum":
		// Get the current garbage threshold from the vacuum detector
		vacuumDetector, _ := vacuum.GetSharedInstances()
		var garbageThreshold float64 = 0.3 // Default fallback
		if vacuumDetector != nil {
			garbageThreshold = vacuumDetector.GetGarbageThreshold()
		}
		task.Parameters["garbage_threshold"] = strconv.FormatFloat(garbageThreshold, 'f', -1, 64)
	case "ec_rebuild":
		// Add info about which shards need rebuilding
	}

	return task
}

// Global variable to hold the master volume data
var volumeData *master_pb.VolumeListResponse
