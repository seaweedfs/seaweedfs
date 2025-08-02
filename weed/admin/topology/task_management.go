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

// AddPendingECShardTask adds a pending EC shard task with specific shard-level impact
//
// IMPORTANT: This function creates multiple internal tasks with derived IDs:
// - One source task: taskID+"_source"
// - Multiple shard tasks: taskID+"_shard_"+i
//
// The caller only provides a single taskID but should be aware that this creates
// multiple internal tracking entries. This design allows for granular capacity
// management but means the caller cannot directly manage the sub-tasks.
//
// TODO: Consider refactoring to either:
// 1. Return the list of generated task IDs to the caller, or
// 2. Redesign taskState to support multiple destinations in a single task
func (at *ActiveTopology) AddPendingECShardTask(taskID string, volumeID uint32, sourceServer string, sourceDisk uint32,
	shardDestinations []string, shardDiskIDs []uint32, shardCount int32, expectedShardSize int64, originalVolumeSize int64) error {

	if len(shardDestinations) != len(shardDiskIDs) {
		return fmt.Errorf("shard destinations and disk IDs must have same length")
	}

	glog.V(2).Infof("Creating EC task %s with 1 source task + %d shard tasks for volume %d",
		taskID, len(shardDestinations), volumeID)

	// Source task: removes original volume
	sourceChange, _ := CalculateTaskStorageImpact(TaskTypeErasureCoding, originalVolumeSize)
	sourceTaskID := taskID + "_source"
	at.addPendingTaskWithStorageInfo(sourceTaskID, TaskTypeErasureCoding, volumeID,
		sourceServer, sourceDisk, "", 0, sourceChange, StorageSlotChange{}, originalVolumeSize)

	// Add shard tasks for each destination
	for i, destination := range shardDestinations {
		shardTaskID := fmt.Sprintf("%s_shard_%d", taskID, i)
		shardsForThisDisk := int32(1) // Typically 1 shard per task, but could be more

		shardImpact := CalculateECShardStorageImpact(shardsForThisDisk, expectedShardSize)
		at.addPendingTaskWithStorageInfo(shardTaskID, TaskTypeErasureCoding, volumeID,
			"", 0, destination, shardDiskIDs[i], StorageSlotChange{}, shardImpact, expectedShardSize)

		glog.V(3).Infof("Created EC shard task %s: volume %d -> %s (disk %d)",
			shardTaskID, volumeID, destination, shardDiskIDs[i])
	}

	return nil
}
