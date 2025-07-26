package task

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/wdclient"
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
	capacity.TotalCapacity += int64(diskInfo.MaxVolumeCount) * (32 * 1024 * 1024 * 1024) // Assume 32GB per volume
	capacity.UsedCapacity += int64(diskInfo.ActiveVolumeCount) * (32 * 1024 * 1024 * 1024)

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

// EC encoding criteria - using size limit from master
func (ms *MasterSynchronizer) checkECEncodingCandidate(volumeID uint32, state *VolumeState) *VolumeMaintenanceCandidate {
	volume := state.CurrentState
	if volume == nil {
		return nil
	}

	// Skip EC encoding detection if no volume size limit is set from master
	if ms.volumeSizeLimitMB <= 0 {
		return nil
	}

	// EC encoding criteria:
	// 1. Volume is read-only or large enough
	// 2. Not already EC encoded
	// 3. Size threshold met

	// Convert MB to bytes and use a fraction for EC threshold (e.g., 50% of size limit)
	ecSizeThreshold := (ms.volumeSizeLimitMB * 1024 * 1024) / 2

	// Check if volume is already EC encoded by checking if we have any EC shards for this volume
	// For simplicity, assume no EC encoding for now since we don't have direct access to EC shard state
	isCandidate := volume.Size > ecSizeThreshold &&
		volume.Size > 1024*1024 // At least 1MB

	if !isCandidate {
		return nil
	}

	return &VolumeMaintenanceCandidate{
		VolumeID:   volumeID,
		Server:     volume.Server,
		TaskType:   "ec_encode",
		Priority:   types.TaskPriorityNormal,
		Reason:     fmt.Sprintf("Volume size %d bytes exceeds EC threshold %d", volume.Size, ecSizeThreshold),
		VolumeInfo: volume,
	}
}

// checkVacuumCandidate checks if a volume is a candidate for vacuum
func (ms *MasterSynchronizer) checkVacuumCandidate(volumeID uint32, state *VolumeState) *VolumeMaintenanceCandidate {
	volume := state.CurrentState
	if volume == nil || volume.ReadOnly {
		return nil
	}

	// Get the current garbage threshold from the vacuum detector
	vacuumDetector, _ := vacuum.GetSharedInstances()
	var vacuumThresholdPercent float64 = 0.3 // Default fallback
	if vacuumDetector != nil {
		vacuumThresholdPercent = vacuumDetector.GetGarbageThreshold()
	}

	// Vacuum criteria:
	// 1. Significant deleted bytes (> configured threshold or > 1GB)
	// 2. Not currently being written to heavily

	const vacuumMinBytes = 1024 * 1024 * 1024 // 1GB

	deletedRatio := float64(volume.DeletedByteCount) / float64(volume.Size)
	isCandidate := (deletedRatio > vacuumThresholdPercent || volume.DeletedByteCount > vacuumMinBytes) &&
		volume.Size > 0

	if !isCandidate {
		return nil
	}

	return &VolumeMaintenanceCandidate{
		VolumeID: volumeID,
		Server:   volume.Server,
		TaskType: "vacuum",
		Priority: types.TaskPriorityNormal,
		Reason: fmt.Sprintf("Deleted bytes %d (%.1f%%) exceed vacuum threshold (%.1f%%)",
			volume.DeletedByteCount, deletedRatio*100, vacuumThresholdPercent*100),
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
