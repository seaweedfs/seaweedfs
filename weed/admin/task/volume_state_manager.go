package task

import (
	"context"
	"sync"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage/erasure_coding"
	"github.com/seaweedfs/seaweedfs/weed/wdclient"
	"github.com/seaweedfs/seaweedfs/weed/worker/types"
)

// VolumeStateManager provides comprehensive tracking of all volume and shard states
type VolumeStateManager struct {
	masterClient      *wdclient.MasterClient
	volumes           map[uint32]*VolumeState
	ecShards          map[uint32]*ECShardState     // Key: VolumeID
	inProgressTasks   map[string]*TaskImpact       // Key: TaskID
	plannedOperations map[string]*PlannedOperation // Key: OperationID
	capacityCache     map[string]*CapacityInfo     // Key: Server address
	lastMasterSync    time.Time
	mutex             sync.RWMutex
}

// VolumeState tracks comprehensive state of a volume
type VolumeState struct {
	VolumeID         uint32
	CurrentState     *VolumeInfo         // Current state from master
	InProgressTasks  []*TaskImpact       // Tasks currently affecting this volume
	PlannedChanges   []*PlannedOperation // Future operations planned
	PredictedState   *VolumeInfo         // Predicted state after all operations
	LastMasterUpdate time.Time
	Inconsistencies  []StateInconsistency
}

// ECShardState tracks EC shard information
type ECShardState struct {
	VolumeID        uint32
	CurrentShards   map[int]*ShardInfo    // Current shards from master (0-13)
	InProgressTasks []*TaskImpact         // Tasks affecting shards
	PlannedShards   map[int]*PlannedShard // Planned shard operations
	PredictedShards map[int]*ShardInfo    // Predicted final state
	LastUpdate      time.Time
}

// ShardInfo represents information about an EC shard
type ShardInfo struct {
	ShardID    int
	Server     string
	Size       uint64
	Status     ShardStatus
	LastUpdate time.Time
}

// ShardStatus represents the status of a shard
type ShardStatus string

const (
	ShardStatusExists    ShardStatus = "exists"
	ShardStatusCreating  ShardStatus = "creating"
	ShardStatusDeleting  ShardStatus = "deleting"
	ShardStatusMissing   ShardStatus = "missing"
	ShardStatusCorrupted ShardStatus = "corrupted"
)

// TaskImpact describes how a task affects volume/shard state
type TaskImpact struct {
	TaskID       string
	TaskType     types.TaskType
	VolumeID     uint32
	WorkerID     string
	StartedAt    time.Time
	EstimatedEnd time.Time

	// Volume impacts
	VolumeChanges *VolumeChanges

	// Shard impacts
	ShardChanges map[int]*ShardChange // Key: ShardID

	// Capacity impacts
	CapacityDelta map[string]int64 // Key: Server, Value: capacity change
}

// VolumeChanges describes changes to a volume
type VolumeChanges struct {
	SizeChange         int64
	WillBeDeleted      bool
	WillBeCreated      bool
	WillBecomeReadOnly bool
	CollectionChange   string
	DiskTypeChange     string
}

// ShardChange describes changes to a shard
type ShardChange struct {
	ShardID       int
	WillBeCreated bool
	WillBeDeleted bool
	TargetServer  string
	SizeChange    int64
}

// PlannedOperation represents a future operation
type PlannedOperation struct {
	OperationID   string
	Type          OperationType
	VolumeID      uint32
	ScheduledAt   time.Time
	Priority      types.TaskPriority
	Prerequisites []string // Other operation IDs that must complete first
	Impact        *TaskImpact
}

// OperationType represents different types of planned operations
type OperationType string

const (
	OperationECEncode     OperationType = "ec_encode"
	OperationECRebuild    OperationType = "ec_rebuild"
	OperationECBalance    OperationType = "ec_balance"
	OperationVacuum       OperationType = "vacuum"
	OperationVolumeMove   OperationType = "volume_move"
	OperationShardMove    OperationType = "shard_move"
	OperationVolumeDelete OperationType = "volume_delete"
)

// CapacityInfo tracks server capacity information
type CapacityInfo struct {
	Server           string
	TotalCapacity    int64
	UsedCapacity     int64
	ReservedCapacity int64 // Capacity reserved for in-progress tasks
	PredictedUsage   int64 // Predicted usage after all operations
	LastUpdate       time.Time
}

// StateInconsistency represents detected inconsistencies
type StateInconsistency struct {
	Type        InconsistencyType
	Description string
	DetectedAt  time.Time
	Severity    SeverityLevel
	VolumeID    uint32
	ShardID     *int
}

// InconsistencyType represents different types of state inconsistencies
type InconsistencyType string

const (
	InconsistencyVolumeMissing    InconsistencyType = "volume_missing"
	InconsistencyVolumeUnexpected InconsistencyType = "volume_unexpected"
	InconsistencyShardMissing     InconsistencyType = "shard_missing"
	InconsistencyShardUnexpected  InconsistencyType = "shard_unexpected"
	InconsistencyCapacityMismatch InconsistencyType = "capacity_mismatch"
	InconsistencyTaskOrphaned     InconsistencyType = "task_orphaned"
	InconsistencyDuplicateTask    InconsistencyType = "duplicate_task"
)

// SeverityLevel represents the severity of an inconsistency
type SeverityLevel string

const (
	SeverityLow      SeverityLevel = "low"
	SeverityMedium   SeverityLevel = "medium"
	SeverityHigh     SeverityLevel = "high"
	SeverityCritical SeverityLevel = "critical"
)

// NewVolumeStateManager creates a new volume state manager
func NewVolumeStateManager(masterClient *wdclient.MasterClient) *VolumeStateManager {
	return &VolumeStateManager{
		masterClient:      masterClient,
		volumes:           make(map[uint32]*VolumeState),
		ecShards:          make(map[uint32]*ECShardState),
		inProgressTasks:   make(map[string]*TaskImpact),
		plannedOperations: make(map[string]*PlannedOperation),
		capacityCache:     make(map[string]*CapacityInfo),
	}
}

// SyncWithMaster synchronizes state with the master server
func (vsm *VolumeStateManager) SyncWithMaster() error {
	vsm.mutex.Lock()
	defer vsm.mutex.Unlock()

	glog.V(2).Infof("Syncing volume state with master")

	// Get current volume list from master
	masterVolumes, masterShards, err := vsm.fetchMasterState()
	if err != nil {
		return err
	}

	// Update volume states
	vsm.updateVolumeStates(masterVolumes)

	// Update shard states
	vsm.updateShardStates(masterShards)

	// Detect inconsistencies
	vsm.detectInconsistencies()

	// Update capacity information
	vsm.updateCapacityInfo()

	// Recalculate predicted states
	vsm.recalculatePredictedStates()

	vsm.lastMasterSync = time.Now()
	glog.V(2).Infof("Master sync completed, tracking %d volumes, %d EC volumes",
		len(vsm.volumes), len(vsm.ecShards))

	return nil
}

// RegisterTaskImpact registers the impact of a new task
func (vsm *VolumeStateManager) RegisterTaskImpact(taskID string, impact *TaskImpact) {
	vsm.mutex.Lock()
	defer vsm.mutex.Unlock()

	vsm.inProgressTasks[taskID] = impact

	// Update volume state
	if volumeState, exists := vsm.volumes[impact.VolumeID]; exists {
		volumeState.InProgressTasks = append(volumeState.InProgressTasks, impact)
	}

	// Update shard state for EC operations
	if impact.TaskType == types.TaskTypeErasureCoding {
		if shardState, exists := vsm.ecShards[impact.VolumeID]; exists {
			shardState.InProgressTasks = append(shardState.InProgressTasks, impact)
		}
	}

	// Update capacity reservations
	for server, capacityDelta := range impact.CapacityDelta {
		if capacity, exists := vsm.capacityCache[server]; exists {
			capacity.ReservedCapacity += capacityDelta
		}
	}

	// Recalculate predicted states
	vsm.recalculatePredictedStates()

	glog.V(2).Infof("Registered task impact: %s for volume %d", taskID, impact.VolumeID)
}

// UnregisterTaskImpact removes a completed task's impact
func (vsm *VolumeStateManager) UnregisterTaskImpact(taskID string) {
	vsm.mutex.Lock()
	defer vsm.mutex.Unlock()

	impact, exists := vsm.inProgressTasks[taskID]
	if !exists {
		return
	}

	delete(vsm.inProgressTasks, taskID)

	// Remove from volume state
	if volumeState, exists := vsm.volumes[impact.VolumeID]; exists {
		vsm.removeTaskFromVolume(volumeState, taskID)
	}

	// Remove from shard state
	if shardState, exists := vsm.ecShards[impact.VolumeID]; exists {
		vsm.removeTaskFromShards(shardState, taskID)
	}

	// Update capacity reservations
	for server, capacityDelta := range impact.CapacityDelta {
		if capacity, exists := vsm.capacityCache[server]; exists {
			capacity.ReservedCapacity -= capacityDelta
		}
	}

	// Recalculate predicted states
	vsm.recalculatePredictedStates()

	glog.V(2).Infof("Unregistered task impact: %s", taskID)
}

// GetAccurateCapacity returns accurate capacity information for a server
func (vsm *VolumeStateManager) GetAccurateCapacity(server string) *CapacityInfo {
	vsm.mutex.RLock()
	defer vsm.mutex.RUnlock()

	if capacity, exists := vsm.capacityCache[server]; exists {
		// Return a copy to avoid external modifications
		return &CapacityInfo{
			Server:           capacity.Server,
			TotalCapacity:    capacity.TotalCapacity,
			UsedCapacity:     capacity.UsedCapacity,
			ReservedCapacity: capacity.ReservedCapacity,
			PredictedUsage:   capacity.PredictedUsage,
			LastUpdate:       capacity.LastUpdate,
		}
	}
	return nil
}

// GetVolumeState returns the current state of a volume
func (vsm *VolumeStateManager) GetVolumeState(volumeID uint32) *VolumeState {
	vsm.mutex.RLock()
	defer vsm.mutex.RUnlock()

	if state, exists := vsm.volumes[volumeID]; exists {
		// Return a copy to avoid external modifications
		return vsm.copyVolumeState(state)
	}
	return nil
}

// GetECShardState returns the current state of EC shards for a volume
func (vsm *VolumeStateManager) GetECShardState(volumeID uint32) *ECShardState {
	vsm.mutex.RLock()
	defer vsm.mutex.RUnlock()

	if state, exists := vsm.ecShards[volumeID]; exists {
		return vsm.copyECShardState(state)
	}
	return nil
}

// CanAssignVolumeToServer checks if a volume can be assigned to a server
func (vsm *VolumeStateManager) CanAssignVolumeToServer(volumeSize int64, server string) bool {
	vsm.mutex.RLock()
	defer vsm.mutex.RUnlock()

	capacity := vsm.capacityCache[server]
	if capacity == nil {
		return false
	}

	// Calculate available capacity: Total - Used - Reserved
	availableCapacity := capacity.TotalCapacity - capacity.UsedCapacity - capacity.ReservedCapacity
	return availableCapacity >= volumeSize
}

// PlanOperation schedules a future operation
func (vsm *VolumeStateManager) PlanOperation(operation *PlannedOperation) {
	vsm.mutex.Lock()
	defer vsm.mutex.Unlock()

	vsm.plannedOperations[operation.OperationID] = operation

	// Add to volume planned changes
	if volumeState, exists := vsm.volumes[operation.VolumeID]; exists {
		volumeState.PlannedChanges = append(volumeState.PlannedChanges, operation)
	}

	glog.V(2).Infof("Planned operation: %s for volume %d", operation.OperationID, operation.VolumeID)
}

// GetPendingChange returns pending change for a volume
func (vsm *VolumeStateManager) GetPendingChange(volumeID uint32) *VolumeChange {
	vsm.mutex.RLock()
	defer vsm.mutex.RUnlock()

	// Look for pending changes in volume state
	if volumeState, exists := vsm.volumes[volumeID]; exists {
		// Return the most recent pending change
		if len(volumeState.PlannedChanges) > 0 {
			latestOp := volumeState.PlannedChanges[len(volumeState.PlannedChanges)-1]
			if latestOp.Impact != nil && latestOp.Impact.VolumeChanges != nil {
				return &VolumeChange{
					VolumeID:         volumeID,
					ChangeType:       ChangeType(latestOp.Type),
					OldCapacity:      int64(volumeState.CurrentState.Size),
					NewCapacity:      int64(volumeState.CurrentState.Size) + latestOp.Impact.VolumeChanges.SizeChange,
					TaskID:           latestOp.Impact.TaskID,
					CompletedAt:      time.Time{}, // Not completed yet
					ReportedToMaster: false,
				}
			}
		}
	}

	return nil
}

// fetchMasterState retrieves current state from master
func (vsm *VolumeStateManager) fetchMasterState() (map[uint32]*VolumeInfo, map[uint32]map[int]*ShardInfo, error) {
	volumes := make(map[uint32]*VolumeInfo)
	shards := make(map[uint32]map[int]*ShardInfo)

	err := vsm.masterClient.WithClient(false, func(client master_pb.SeaweedClient) error {
		// Fetch volume list
		resp, err := client.VolumeList(context.Background(), &master_pb.VolumeListRequest{})
		if err != nil {
			return err
		}

		// Process topology info
		if resp.TopologyInfo != nil {
			for _, dc := range resp.TopologyInfo.DataCenterInfos {
				for _, rack := range dc.RackInfos {
					for _, node := range rack.DataNodeInfos {
						for _, diskInfo := range node.DiskInfos {
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
									Server:           node.Id,
									DataCenter:       dc.Id,
									Rack:             rack.Id,
									DiskType:         volInfo.DiskType,
									ModifiedAtSecond: volInfo.ModifiedAtSecond,
									RemoteStorageKey: volInfo.RemoteStorageKey,
								}
							}

							// Process EC shards
							for _, ecShardInfo := range diskInfo.EcShardInfos {
								volumeID := ecShardInfo.Id
								if shards[volumeID] == nil {
									shards[volumeID] = make(map[int]*ShardInfo)
								}

								// Decode shard bits
								for shardID := 0; shardID < erasure_coding.TotalShardsCount; shardID++ {
									if (ecShardInfo.EcIndexBits & (1 << uint(shardID))) != 0 {
										shards[volumeID][shardID] = &ShardInfo{
											ShardID:    shardID,
											Server:     node.Id,
											Size:       0, // Size would need to be fetched separately
											Status:     ShardStatusExists,
											LastUpdate: time.Now(),
										}
									}
								}
							}
						}
					}
				}
			}
		}

		return nil
	})

	return volumes, shards, err
}

// updateVolumeStates updates volume states based on master data
func (vsm *VolumeStateManager) updateVolumeStates(masterVolumes map[uint32]*VolumeInfo) {
	now := time.Now()

	// Update existing volumes and add new ones
	for volumeID, masterVolume := range masterVolumes {
		if volumeState, exists := vsm.volumes[volumeID]; exists {
			// Update existing volume
			oldState := volumeState.CurrentState
			volumeState.CurrentState = masterVolume
			volumeState.LastMasterUpdate = now

			// Check for unexpected changes
			if oldState != nil && vsm.hasUnexpectedChanges(oldState, masterVolume) {
				vsm.addInconsistency(volumeState, InconsistencyVolumeUnexpected,
					"Volume changed unexpectedly since last sync", SeverityMedium)
			}
		} else {
			// New volume detected
			vsm.volumes[volumeID] = &VolumeState{
				VolumeID:         volumeID,
				CurrentState:     masterVolume,
				InProgressTasks:  []*TaskImpact{},
				PlannedChanges:   []*PlannedOperation{},
				LastMasterUpdate: now,
				Inconsistencies:  []StateInconsistency{},
			}
		}
	}

	// Detect missing volumes (volumes we knew about but master doesn't report)
	for volumeID, volumeState := range vsm.volumes {
		if _, existsInMaster := masterVolumes[volumeID]; !existsInMaster {
			// Check if this is expected (due to deletion task)
			if !vsm.isVolumeDeletionExpected(volumeID) {
				vsm.addInconsistency(volumeState, InconsistencyVolumeMissing,
					"Volume missing from master but not expected to be deleted", SeverityHigh)
			}
		}
	}
}

// updateShardStates updates EC shard states
func (vsm *VolumeStateManager) updateShardStates(masterShards map[uint32]map[int]*ShardInfo) {
	now := time.Now()

	// Update existing shard states
	for volumeID, shardMap := range masterShards {
		if shardState, exists := vsm.ecShards[volumeID]; exists {
			shardState.CurrentShards = shardMap
			shardState.LastUpdate = now
		} else {
			vsm.ecShards[volumeID] = &ECShardState{
				VolumeID:        volumeID,
				CurrentShards:   shardMap,
				InProgressTasks: []*TaskImpact{},
				PlannedShards:   make(map[int]*PlannedShard),
				PredictedShards: make(map[int]*ShardInfo),
				LastUpdate:      now,
			}
		}
	}

	// Check for missing shards that we expected to exist
	for volumeID, shardState := range vsm.ecShards {
		if masterShardMap, exists := masterShards[volumeID]; exists {
			vsm.validateShardConsistency(shardState, masterShardMap)
		}
	}
}

// detectInconsistencies identifies state inconsistencies
func (vsm *VolumeStateManager) detectInconsistencies() {
	for _, volumeState := range vsm.volumes {
		vsm.detectVolumeInconsistencies(volumeState)
	}

	for _, shardState := range vsm.ecShards {
		vsm.detectShardInconsistencies(shardState)
	}

	vsm.detectOrphanedTasks()
	vsm.detectDuplicateTasks()
	vsm.detectCapacityInconsistencies()
}

// updateCapacityInfo updates server capacity information
func (vsm *VolumeStateManager) updateCapacityInfo() {
	for server := range vsm.capacityCache {
		vsm.recalculateServerCapacity(server)
	}
}

// recalculatePredictedStates recalculates predicted states after all operations
func (vsm *VolumeStateManager) recalculatePredictedStates() {
	for _, volumeState := range vsm.volumes {
		vsm.calculatePredictedVolumeState(volumeState)
	}

	for _, shardState := range vsm.ecShards {
		vsm.calculatePredictedShardState(shardState)
	}
}

// Helper methods (simplified implementations)

func (vsm *VolumeStateManager) hasUnexpectedChanges(old, new *VolumeInfo) bool {
	return old.Size != new.Size || old.ReadOnly != new.ReadOnly
}

func (vsm *VolumeStateManager) isVolumeDeletionExpected(volumeID uint32) bool {
	for _, impact := range vsm.inProgressTasks {
		if impact.VolumeID == volumeID && impact.VolumeChanges != nil && impact.VolumeChanges.WillBeDeleted {
			return true
		}
	}
	return false
}

func (vsm *VolumeStateManager) addInconsistency(volumeState *VolumeState, incType InconsistencyType, desc string, severity SeverityLevel) {
	inconsistency := StateInconsistency{
		Type:        incType,
		Description: desc,
		DetectedAt:  time.Now(),
		Severity:    severity,
		VolumeID:    volumeState.VolumeID,
	}
	volumeState.Inconsistencies = append(volumeState.Inconsistencies, inconsistency)

	glog.Warningf("State inconsistency detected for volume %d: %s", volumeState.VolumeID, desc)
}

func (vsm *VolumeStateManager) removeTaskFromVolume(volumeState *VolumeState, taskID string) {
	for i, task := range volumeState.InProgressTasks {
		if task.TaskID == taskID {
			volumeState.InProgressTasks = append(volumeState.InProgressTasks[:i], volumeState.InProgressTasks[i+1:]...)
			break
		}
	}
}

func (vsm *VolumeStateManager) removeTaskFromShards(shardState *ECShardState, taskID string) {
	for i, task := range shardState.InProgressTasks {
		if task.TaskID == taskID {
			shardState.InProgressTasks = append(shardState.InProgressTasks[:i], shardState.InProgressTasks[i+1:]...)
			break
		}
	}
}

func (vsm *VolumeStateManager) copyVolumeState(state *VolumeState) *VolumeState {
	// Return a deep copy (implementation would be more detailed)
	return &VolumeState{
		VolumeID:         state.VolumeID,
		CurrentState:     state.CurrentState,
		LastMasterUpdate: state.LastMasterUpdate,
	}
}

func (vsm *VolumeStateManager) copyECShardState(state *ECShardState) *ECShardState {
	// Return a deep copy (implementation would be more detailed)
	return &ECShardState{
		VolumeID:   state.VolumeID,
		LastUpdate: state.LastUpdate,
	}
}

// Placeholder implementations for consistency checking methods
func (vsm *VolumeStateManager) validateShardConsistency(shardState *ECShardState, masterShards map[int]*ShardInfo) {
}
func (vsm *VolumeStateManager) detectVolumeInconsistencies(volumeState *VolumeState)   {}
func (vsm *VolumeStateManager) detectShardInconsistencies(shardState *ECShardState)    {}
func (vsm *VolumeStateManager) detectOrphanedTasks()                                   {}
func (vsm *VolumeStateManager) detectDuplicateTasks()                                  {}
func (vsm *VolumeStateManager) detectCapacityInconsistencies()                         {}
func (vsm *VolumeStateManager) recalculateServerCapacity(server string)                {}
func (vsm *VolumeStateManager) calculatePredictedVolumeState(volumeState *VolumeState) {}
func (vsm *VolumeStateManager) calculatePredictedShardState(shardState *ECShardState)  {}

// PlannedShard represents a planned shard operation
type PlannedShard struct {
	ShardID      int
	Operation    string // "create", "delete", "move"
	TargetServer string
	ScheduledAt  time.Time
}
