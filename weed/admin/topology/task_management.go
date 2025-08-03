package topology

import (
	"fmt"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/storage/erasure_coding"
)

// addPendingTaskWithStorageInfo adds a pending task with detailed storage impact information
func (at *ActiveTopology) addPendingTaskWithStorageInfo(taskID string, taskType TaskType, volumeID uint32,
	sourceServer string, sourceDisk uint32, targetServer string, targetDisk uint32,
	sourceStorageChange, targetStorageChange StorageSlotChange, estimatedSize int64) {
	at.mutex.Lock()
	defer at.mutex.Unlock()

	// Create task using unified structure (single source, single destination)
	task := &taskState{
		VolumeID:      volumeID,
		TaskType:      taskType,
		Status:        TaskStatusPending,
		StartedAt:     time.Now(),
		EstimatedSize: estimatedSize,
		Sources: []TaskSource{
			{
				SourceServer:  sourceServer,
				SourceDisk:    sourceDisk,
				StorageChange: sourceStorageChange,
				EstimatedSize: estimatedSize,
			},
		},
		Destinations: []TaskDestination{
			{
				TargetServer:  targetServer,
				TargetDisk:    targetDisk,
				StorageChange: targetStorageChange,
				EstimatedSize: estimatedSize,
			},
		},
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

	// Check if all destination disks have sufficient capacity to reserve
	for _, dest := range task.Destinations {
		targetKey := fmt.Sprintf("%s:%d", dest.TargetServer, dest.TargetDisk)
		if targetDisk, exists := at.disks[targetKey]; exists {
			availableCapacity := at.getEffectiveAvailableCapacityUnsafe(targetDisk)

			// Check if we have enough volume slots for volume-based requirements
			if dest.StorageChange.VolumeSlots > 0 && int64(availableCapacity.VolumeSlots) < int64(dest.StorageChange.VolumeSlots) {
				return fmt.Errorf("insufficient volume capacity on target disk %s:%d: available=%d volume slots, required=%d volume slots",
					dest.TargetServer, dest.TargetDisk, availableCapacity.VolumeSlots, dest.StorageChange.VolumeSlots)
			}

			// Check if we have enough total capacity (volume slots) for mixed requirements
			requiredCapacity := int64(dest.StorageChange.VolumeSlots) + int64(dest.StorageChange.ShardSlots)/10
			if requiredCapacity > 0 && int64(availableCapacity.VolumeSlots) < requiredCapacity {
				return fmt.Errorf("insufficient total capacity on target disk %s:%d: available=%d volume slots, required=%d equivalent slots",
					dest.TargetServer, dest.TargetDisk, availableCapacity.VolumeSlots, requiredCapacity)
			}
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

// AddPendingTaskForTaskType is a convenience method for simple single-target tasks
// For EC tasks with multiple targets, use AddPendingECShardTask instead.
func (at *ActiveTopology) AddPendingTaskForTaskType(taskID string, taskType TaskType, volumeID uint32,
	sourceServer string, sourceDisk uint32, targetServer string, targetDisk uint32,
	volumeSize int64) {

	sourceChange, targetChange := CalculateTaskStorageImpact(taskType, volumeSize)
	at.addPendingTaskWithStorageInfo(taskID, taskType, volumeID, sourceServer, sourceDisk,
		targetServer, targetDisk, sourceChange, targetChange, volumeSize)
}

// SourceCleanupType indicates what type of data needs to be cleaned up from a source
type SourceCleanupType int

const (
	CleanupVolumeReplica SourceCleanupType = iota // Clean up volume replica (frees volume slots)
	CleanupECShards                               // Clean up existing EC shards (frees shard slots)
)

// TaskSourceLocation represents a source location for task creation
type TaskSourceLocation struct {
	ServerID    string
	DiskID      uint32
	CleanupType SourceCleanupType // What type of cleanup is needed
}

// AddPendingECShardTask adds a pending EC shard task with multiple sources and destinations
//
// This function creates a single task that represents the entire EC operation with
// multiple sources (for replicated volume cleanup) and multiple destinations (for EC shards).
// This is cleaner than creating multiple sub-tasks and allows proper task lifecycle management with a single task ID.
func (at *ActiveTopology) AddPendingECShardTask(taskID string, volumeID uint32,
	sourceLocations []TaskSourceLocation, shardDestinations []string, shardDiskIDs []uint32,
	shardCount int32, expectedShardSize int64, originalVolumeSize int64) error {

	if len(shardDestinations) != len(shardDiskIDs) {
		return fmt.Errorf("shard destinations and disk IDs must have same length")
	}

	if len(sourceLocations) == 0 {
		return fmt.Errorf("at least one source location is required")
	}

	glog.V(2).Infof("Creating single EC task %s with %d sources and %d destinations for volume %d",
		taskID, len(sourceLocations), len(shardDestinations), volumeID)

	// Build sources array for the EC task (volumes and existing EC shards to be cleaned up)
	sources := make([]TaskSource, len(sourceLocations))
	for i, sourceLocation := range sourceLocations {
		var storageImpact StorageSlotChange
		var estimatedSize int64
		var cleanupDesc string

		switch sourceLocation.CleanupType {
		case CleanupVolumeReplica:
			// Cleaning up volume replica frees volume slots
			storageImpact, _ = CalculateTaskStorageImpact(TaskTypeErasureCoding, originalVolumeSize)
			estimatedSize = originalVolumeSize
			cleanupDesc = "volume replica"
		case CleanupECShards:
			// Cleaning up existing EC shards frees shard slots
			storageImpact = CalculateECShardCleanupImpact(originalVolumeSize)
			estimatedSize = originalVolumeSize / int64(erasure_coding.TotalShardsCount) // Estimate shard size
			cleanupDesc = "existing EC shards"
		default:
			// Default to volume replica cleanup
			storageImpact, _ = CalculateTaskStorageImpact(TaskTypeErasureCoding, originalVolumeSize)
			estimatedSize = originalVolumeSize
			cleanupDesc = "unknown (default to volume)"
		}

		sources[i] = TaskSource{
			SourceServer:  sourceLocation.ServerID,
			SourceDisk:    sourceLocation.DiskID,
			StorageChange: storageImpact,
			EstimatedSize: estimatedSize,
		}

		glog.V(3).Infof("EC source %d: volume %d from %s (disk %d, cleanup: %s, impact: %+v)",
			i, volumeID, sourceLocation.ServerID, sourceLocation.DiskID, cleanupDesc, storageImpact)
	}

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

	// Create a single EC task with multiple sources and destinations
	at.addPendingTaskWithMultipleSourcesAndDestinations(taskID, TaskTypeErasureCoding, volumeID,
		sources, destinations, originalVolumeSize)

	glog.V(2).Infof("EC task %s created for volume %d: %d sources, %d destinations",
		taskID, volumeID, len(sources), len(destinations))

	return nil
}

// addPendingTaskWithMultipleSourcesAndDestinations adds a pending task with multiple sources and destinations (for EC operations on replicated volumes)
func (at *ActiveTopology) addPendingTaskWithMultipleSourcesAndDestinations(taskID string, taskType TaskType, volumeID uint32,
	sources []TaskSource, destinations []TaskDestination, estimatedSize int64) {

	at.mutex.Lock()
	defer at.mutex.Unlock()

	task := &taskState{
		VolumeID:      volumeID,
		TaskType:      taskType,
		Status:        TaskStatusPending,
		StartedAt:     time.Now(),
		EstimatedSize: estimatedSize,
		Sources:       sources,      // Multiple sources for replica cleanup
		Destinations:  destinations, // Multiple destinations for EC shards
	}

	at.pendingTasks[taskID] = task

	// Apply capacity impact to all source disks (avoid duplicates for same disk)
	addedSourceDisks := make(map[string]bool)
	for _, source := range sources {
		sourceDiskKey := fmt.Sprintf("%s:%d", source.SourceServer, source.SourceDisk)
		if !addedSourceDisks[sourceDiskKey] {
			if sourceDiskObj, exists := at.disks[sourceDiskKey]; exists {
				sourceDiskObj.pendingTasks = append(sourceDiskObj.pendingTasks, task)
				addedSourceDisks[sourceDiskKey] = true
			}
		}
	}

	// Apply capacity impact to all destination disks (avoid duplicates for same disk)
	addedDestDisks := make(map[string]bool)
	for _, dest := range destinations {
		destDiskKey := fmt.Sprintf("%s:%d", dest.TargetServer, dest.TargetDisk)
		if !addedDestDisks[destDiskKey] {
			if destDiskObj, exists := at.disks[destDiskKey]; exists {
				destDiskObj.pendingTasks = append(destDiskObj.pendingTasks, task)
				addedDestDisks[destDiskKey] = true
			}
		}
	}

	glog.V(2).Infof("Added pending %s task %s: volume %d, %d sources, %d destinations",
		taskType, taskID, volumeID, len(sources), len(destinations))
}
