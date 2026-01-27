package topology

import (
	"fmt"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
)

// AssignTask moves a task from pending to assigned and reserves capacity
func (at *ActiveTopology) AssignTask(taskID string) error {
	at.mutex.Lock()
	defer at.mutex.Unlock()

	task, exists := at.pendingTasks[taskID]
	if !exists {
		return fmt.Errorf("pending task %s not found", taskID)
	}

	// Check if all destination disks have sufficient capacity to reserve
	for _, dest := range task.Destinations {
		targetKey := fmt.Sprintf("%s:%d", dest.TargetServer, dest.TargetDisk)
		if targetDisk, exists := at.disks[targetKey]; exists {
			availableCapacity := at.getEffectiveAvailableCapacityUnsafe(targetDisk)

			// Check if we have enough total capacity using the improved unified comparison
			if !availableCapacity.CanAccommodate(dest.StorageChange) {
				return fmt.Errorf("insufficient capacity on target disk %s:%d. Available: %+v, Required: %+v",
					dest.TargetServer, dest.TargetDisk, availableCapacity, dest.StorageChange)
			}
		} else if dest.TargetServer != "" {
			// Fail fast if destination disk is not found in topology
			return fmt.Errorf("destination disk %s not found in topology", targetKey)
		}
	}

	// Move task to assigned and reserve capacity
	delete(at.pendingTasks, taskID)
	task.Status = TaskStatusInProgress
	at.assignedTasks[taskID] = task
	at.reassignTaskStates()

	// Log capacity reservation information for all sources and destinations
	totalSourceImpact := StorageSlotChange{}
	totalDestImpact := StorageSlotChange{}
	for _, source := range task.Sources {
		totalSourceImpact.AddInPlace(source.StorageChange)
	}
	for _, dest := range task.Destinations {
		totalDestImpact.AddInPlace(dest.StorageChange)
	}

	glog.V(2).Infof("Task %s assigned and capacity reserved: %d sources (VolumeSlots:%d, ShardSlots:%d), %d destinations (VolumeSlots:%d, ShardSlots:%d)",
		taskID, len(task.Sources), totalSourceImpact.VolumeSlots, totalSourceImpact.ShardSlots,
		len(task.Destinations), totalDestImpact.VolumeSlots, totalDestImpact.ShardSlots)

	return nil
}

// CompleteTask moves a task from assigned to recent and releases reserved capacity
// NOTE: This only releases the reserved capacity. The actual topology update (VolumeCount changes)
// should be handled by the master when it receives the task completion notification.
func (at *ActiveTopology) CompleteTask(taskID string) error {
	at.mutex.Lock()
	defer at.mutex.Unlock()

	task, exists := at.assignedTasks[taskID]
	if !exists {
		return fmt.Errorf("assigned task %s not found", taskID)
	}

	// Release reserved capacity by moving task to completed state
	delete(at.assignedTasks, taskID)
	task.Status = TaskStatusCompleted
	task.CompletedAt = time.Now()
	at.recentTasks[taskID] = task
	at.reassignTaskStates()

	// Log capacity release information for all sources and destinations
	totalSourceImpact := StorageSlotChange{}
	totalDestImpact := StorageSlotChange{}
	for _, source := range task.Sources {
		totalSourceImpact.AddInPlace(source.StorageChange)
	}
	for _, dest := range task.Destinations {
		totalDestImpact.AddInPlace(dest.StorageChange)
	}

	glog.V(2).Infof("Task %s completed and capacity released: %d sources (VolumeSlots:%d, ShardSlots:%d), %d destinations (VolumeSlots:%d, ShardSlots:%d)",
		taskID, len(task.Sources), totalSourceImpact.VolumeSlots, totalSourceImpact.ShardSlots,
		len(task.Destinations), totalDestImpact.VolumeSlots, totalDestImpact.ShardSlots)

	// Clean up old recent tasks
	at.cleanupRecentTasks()

	return nil
}

// ApplyActualStorageChange updates the topology to reflect actual storage changes after task completion
// This should be called when the master updates the topology with new VolumeCount information
func (at *ActiveTopology) ApplyActualStorageChange(nodeID string, diskID uint32, volumeCountChange int64) {
	at.mutex.Lock()
	defer at.mutex.Unlock()

	diskKey := fmt.Sprintf("%s:%d", nodeID, diskID)
	if disk, exists := at.disks[diskKey]; exists && disk.DiskInfo != nil && disk.DiskInfo.DiskInfo != nil {
		oldCount := disk.DiskInfo.DiskInfo.VolumeCount
		disk.DiskInfo.DiskInfo.VolumeCount += volumeCountChange

		glog.V(2).Infof("Applied actual storage change on disk %s: volume_count %d -> %d (change: %+d)",
			diskKey, oldCount, disk.DiskInfo.DiskInfo.VolumeCount, volumeCountChange)
	}
}

// AddPendingTask is the unified function that handles both simple and complex task creation
func (at *ActiveTopology) AddPendingTask(spec TaskSpec) error {
	// Validation
	if len(spec.Sources) == 0 {
		return fmt.Errorf("at least one source is required")
	}
	if len(spec.Destinations) == 0 {
		return fmt.Errorf("at least one destination is required")
	}

	at.mutex.Lock()
	defer at.mutex.Unlock()

	// Build sources array
	sources := make([]TaskSource, len(spec.Sources))
	for i, sourceSpec := range spec.Sources {
		var storageImpact StorageSlotChange
		var estimatedSize int64

		if sourceSpec.StorageImpact != nil {
			// Use manually specified impact
			storageImpact = *sourceSpec.StorageImpact
		} else {
			// Auto-calculate based on task type and cleanup type
			storageImpact = at.calculateSourceStorageImpact(spec.TaskType, sourceSpec.CleanupType, spec.VolumeSize)
		}

		if sourceSpec.EstimatedSize != nil {
			estimatedSize = *sourceSpec.EstimatedSize
		} else {
			estimatedSize = spec.VolumeSize // Default to volume size
		}

		sources[i] = TaskSource{
			SourceServer:  sourceSpec.ServerID,
			SourceDisk:    sourceSpec.DiskID,
			StorageChange: storageImpact,
			EstimatedSize: estimatedSize,
		}
	}

	// Build destinations array
	destinations := make([]TaskDestination, len(spec.Destinations))
	for i, destSpec := range spec.Destinations {
		var storageImpact StorageSlotChange
		var estimatedSize int64

		if destSpec.StorageImpact != nil {
			// Use manually specified impact
			storageImpact = *destSpec.StorageImpact
		} else {
			// Auto-calculate based on task type
			_, storageImpact = CalculateTaskStorageImpact(spec.TaskType, spec.VolumeSize)
		}

		if destSpec.EstimatedSize != nil {
			estimatedSize = *destSpec.EstimatedSize
		} else {
			estimatedSize = spec.VolumeSize // Default to volume size
		}

		destinations[i] = TaskDestination{
			TargetServer:  destSpec.ServerID,
			TargetDisk:    destSpec.DiskID,
			StorageChange: storageImpact,
			EstimatedSize: estimatedSize,
		}
	}

	// Create the task
	task := &taskState{
		VolumeID:      spec.VolumeID,
		TaskType:      spec.TaskType,
		Status:        TaskStatusPending,
		StartedAt:     time.Now(),
		EstimatedSize: spec.VolumeSize,
		Sources:       sources,
		Destinations:  destinations,
	}

	at.pendingTasks[spec.TaskID] = task
	at.assignTaskToDisk(task)

	return nil
}

// RestoreMaintenanceTask restores a task from persistent storage into the active topology
func (at *ActiveTopology) RestoreMaintenanceTask(taskID string, volumeID uint32, taskType TaskType, status TaskStatus, sources []TaskSource, destinations []TaskDestination, estimatedSize int64) error {
	at.mutex.Lock()
	defer at.mutex.Unlock()

	task := &taskState{
		VolumeID:      volumeID,
		TaskType:      taskType,
		Status:        status,
		StartedAt:     time.Now(), // Fallback if not provided, will be updated by heartbeats
		EstimatedSize: estimatedSize,
		Sources:       sources,
		Destinations:  destinations,
	}

	if status == TaskStatusInProgress {
		at.assignedTasks[taskID] = task
	} else if status == TaskStatusPending {
		at.pendingTasks[taskID] = task
	} else {
		return nil // Ignore other statuses for topology tracking
	}

	// Re-register task with disks for capacity tracking
	at.assignTaskToDisk(task)

	glog.V(1).Infof("Restored %s task %s in topology: volume %d, %d sources, %d destinations",
		taskType, taskID, volumeID, len(sources), len(destinations))

	return nil
}

// HasTask checks if there is any pending or assigned task for the given volume and task type.
// If taskType is TaskTypeNone, it checks for ANY task type.
func (at *ActiveTopology) HasTask(volumeID uint32, taskType TaskType) bool {
	at.mutex.RLock()
	defer at.mutex.RUnlock()

	for _, task := range at.pendingTasks {
		if task.VolumeID == volumeID && (taskType == TaskTypeNone || task.TaskType == taskType) {
			return true
		}
	}

	for _, task := range at.assignedTasks {
		if task.VolumeID == volumeID && (taskType == TaskTypeNone || task.TaskType == taskType) {
			return true
		}
	}

	return false
}

// HasAnyTask checks if there is any pending or assigned task for the given volume across all types.
func (at *ActiveTopology) HasAnyTask(volumeID uint32) bool {
	return at.HasTask(volumeID, TaskTypeNone)
}

// calculateSourceStorageImpact calculates storage impact for sources based on task type and cleanup type
func (at *ActiveTopology) calculateSourceStorageImpact(taskType TaskType, cleanupType SourceCleanupType, volumeSize int64) StorageSlotChange {
	switch taskType {
	case TaskTypeErasureCoding:
		switch cleanupType {
		case CleanupVolumeReplica:
			impact, _ := CalculateTaskStorageImpact(TaskTypeErasureCoding, volumeSize)
			return impact
		case CleanupECShards:
			return CalculateECShardCleanupImpact(volumeSize)
		default:
			impact, _ := CalculateTaskStorageImpact(TaskTypeErasureCoding, volumeSize)
			return impact
		}
	default:
		impact, _ := CalculateTaskStorageImpact(taskType, volumeSize)
		return impact
	}
}

// SourceCleanupType indicates what type of data needs to be cleaned up from a source
type SourceCleanupType int

const (
	CleanupVolumeReplica SourceCleanupType = iota // Clean up volume replica (frees volume slots)
	CleanupECShards                               // Clean up existing EC shards (frees shard slots)
)

// TaskSourceSpec represents a source specification for task creation
type TaskSourceSpec struct {
	ServerID      string
	DiskID        uint32
	DataCenter    string             // Data center of the source server
	Rack          string             // Rack of the source server
	CleanupType   SourceCleanupType  // For EC: volume replica vs existing shards
	StorageImpact *StorageSlotChange // Optional: manual override
	EstimatedSize *int64             // Optional: manual override
}

// TaskDestinationSpec represents a destination specification for task creation
type TaskDestinationSpec struct {
	ServerID      string
	DiskID        uint32
	StorageImpact *StorageSlotChange // Optional: manual override
	EstimatedSize *int64             // Optional: manual override
}

// TaskSpec represents a complete task specification
type TaskSpec struct {
	TaskID       string
	TaskType     TaskType
	VolumeID     uint32
	VolumeSize   int64                 // Used for auto-calculation when manual impacts not provided
	Sources      []TaskSourceSpec      // Can be single or multiple
	Destinations []TaskDestinationSpec // Can be single or multiple
}
