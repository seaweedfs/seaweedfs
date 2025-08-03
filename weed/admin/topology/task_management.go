package topology

import (
	"fmt"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
)

// addPendingTaskWithStorageInfo adds a pending task with detailed storage impact information
func (at *ActiveTopology) addPendingTaskWithStorageInfo(taskID string, taskType TaskType, volumeID uint32,
	sourceServer string, sourceDisk uint32, targetServer string, targetDisk uint32,
	sourceStorageChange, targetStorageChange StorageSlotChange, estimatedSize int64) {
	at.mutex.Lock()
	defer at.mutex.Unlock()

	task := &taskState{
		VolumeID:            volumeID,
		TaskType:            taskType,
		SourceServer:        sourceServer,
		SourceDisk:          sourceDisk,
		TargetServer:        targetServer,
		TargetDisk:          targetDisk,
		Status:              TaskStatusPending,
		StartedAt:           time.Now(),
		SourceStorageChange: sourceStorageChange,
		TargetStorageChange: targetStorageChange,
		EstimatedSize:       estimatedSize,
	}

	at.pendingTasks[taskID] = task
	at.assignTaskToDisk(task)
}

// AssignTask moves a task from pending to assigned and reserves capacity
func (at *ActiveTopology) AssignTask(taskID string) error {
	at.mutex.Lock()
	defer at.mutex.Unlock()

	task, exists := at.pendingTasks[taskID]
	if !exists {
		return fmt.Errorf("pending task %s not found", taskID)
	}

	// Check if target disk has sufficient capacity to reserve
	if task.TargetServer != "" {
		targetKey := fmt.Sprintf("%s:%d", task.TargetServer, task.TargetDisk)
		if targetDisk, exists := at.disks[targetKey]; exists {
			currentCapacity := at.getEffectiveAvailableCapacityUnsafe(targetDisk)
			requiredCapacity := int64(task.TargetStorageChange.VolumeSlots) + int64(task.TargetStorageChange.ShardSlots)/10

			if currentCapacity < requiredCapacity {
				return fmt.Errorf("insufficient capacity on target disk %s:%d: available=%d, required=%d",
					task.TargetServer, task.TargetDisk, currentCapacity, requiredCapacity)
			}
		}
	}

	// Move task to assigned and reserve capacity
	delete(at.pendingTasks, taskID)
	task.Status = TaskStatusInProgress
	at.assignedTasks[taskID] = task
	at.reassignTaskStates()

	glog.V(2).Infof("Task %s assigned and capacity reserved: source_change={VolumeSlots:%d, ShardSlots:%d}, target_change={VolumeSlots:%d, ShardSlots:%d}",
		taskID, task.SourceStorageChange.VolumeSlots, task.SourceStorageChange.ShardSlots,
		task.TargetStorageChange.VolumeSlots, task.TargetStorageChange.ShardSlots)

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

	glog.V(2).Infof("Task %s completed and capacity released: source_change={VolumeSlots:%d, ShardSlots:%d}, target_change={VolumeSlots:%d, ShardSlots:%d}",
		taskID, task.SourceStorageChange.VolumeSlots, task.SourceStorageChange.ShardSlots,
		task.TargetStorageChange.VolumeSlots, task.TargetStorageChange.ShardSlots)

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

// AddPendingTaskForTaskType is a convenience method for simple single-target tasks
// For EC tasks with multiple targets, use AddPendingECShardTask instead.
func (at *ActiveTopology) AddPendingTaskForTaskType(taskID string, taskType TaskType, volumeID uint32,
	sourceServer string, sourceDisk uint32, targetServer string, targetDisk uint32,
	volumeSize int64) {

	sourceChange, targetChange := CalculateTaskStorageImpact(taskType, volumeSize)
	at.addPendingTaskWithStorageInfo(taskID, taskType, volumeID, sourceServer, sourceDisk,
		targetServer, targetDisk, sourceChange, targetChange, volumeSize)
}

// AddPendingECShardTask adds a pending EC shard task with multiple destinations
//
// This function creates a single task that represents the entire EC operation with
// multiple destinations. This is cleaner than creating multiple sub-tasks and allows
// proper task lifecycle management with a single task ID.
func (at *ActiveTopology) AddPendingECShardTask(taskID string, volumeID uint32, sourceServer string, sourceDisk uint32,
	shardDestinations []string, shardDiskIDs []uint32, shardCount int32, expectedShardSize int64, originalVolumeSize int64) error {

	if len(shardDestinations) != len(shardDiskIDs) {
		return fmt.Errorf("shard destinations and disk IDs must have same length")
	}

	glog.V(2).Infof("Creating single EC task %s with %d destinations for volume %d",
		taskID, len(shardDestinations), volumeID)

	// Calculate source storage impact (EC frees the original volume)
	sourceChange, _ := CalculateTaskStorageImpact(TaskTypeErasureCoding, originalVolumeSize)

	// Build destinations array for the EC task
	destinations := make([]TaskDestination, len(shardDestinations))
	for i, destination := range shardDestinations {
		shardsForThisDisk := int32(1) // Typically 1 shard per destination
		shardImpact := CalculateECShardStorageImpact(shardsForThisDisk, expectedShardSize)

		destinations[i] = TaskDestination{
			TargetServer:  destination,
			TargetDisk:    shardDiskIDs[i],
			StorageChange: shardImpact,
			EstimatedSize: expectedShardSize,
		}

		glog.V(3).Infof("EC destination %d: volume %d -> %s (disk %d, impact: %+v)",
			i, volumeID, destination, shardDiskIDs[i], shardImpact)
	}

	// Create a single EC task with multiple destinations
	at.addPendingTaskWithMultipleDestinations(taskID, TaskTypeErasureCoding, volumeID,
		sourceServer, sourceDisk, sourceChange, destinations, originalVolumeSize)

	glog.V(2).Infof("EC task %s created for volume %d: source=%s:%d, %d destinations",
		taskID, volumeID, sourceServer, sourceDisk, len(destinations))

	return nil
}

// addPendingTaskWithMultipleDestinations adds a pending task with multiple destinations (for EC operations)
func (at *ActiveTopology) addPendingTaskWithMultipleDestinations(taskID string, taskType TaskType, volumeID uint32,
	sourceServer string, sourceDisk uint32, sourceChange StorageSlotChange, destinations []TaskDestination, estimatedSize int64) {

	at.mutex.Lock()
	defer at.mutex.Unlock()

	task := &taskState{
		VolumeID:            volumeID,
		TaskType:            taskType,
		SourceServer:        sourceServer,
		SourceDisk:          sourceDisk,
		Status:              TaskStatusPending,
		StartedAt:           time.Now(),
		SourceStorageChange: sourceChange,
		EstimatedSize:       estimatedSize,
		Destinations:        destinations, // Multiple destinations for EC
	}

	at.pendingTasks[taskID] = task

	// Apply capacity impact to source disk
	sourceDiskKey := fmt.Sprintf("%s:%d", sourceServer, sourceDisk)
	if sourceDiskObj, exists := at.disks[sourceDiskKey]; exists {
		sourceDiskObj.pendingTasks = append(sourceDiskObj.pendingTasks, task)
	}

	// Apply capacity impact to all destination disks (avoid duplicates for same disk)
	addedToDisks := make(map[string]bool)
	for _, dest := range destinations {
		destDiskKey := fmt.Sprintf("%s:%d", dest.TargetServer, dest.TargetDisk)
		if !addedToDisks[destDiskKey] {
			if destDiskObj, exists := at.disks[destDiskKey]; exists {
				destDiskObj.pendingTasks = append(destDiskObj.pendingTasks, task)
				addedToDisks[destDiskKey] = true
			}
		}
	}

	glog.V(2).Infof("Added pending %s task %s: volume %d, source %s:%d, %d destinations",
		taskType, taskID, volumeID, sourceServer, sourceDisk, len(destinations))
}
