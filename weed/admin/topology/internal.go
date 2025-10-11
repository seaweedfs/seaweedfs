package topology

import (
	"fmt"
	"time"
)

// reassignTaskStates assigns tasks to the appropriate disks
func (at *ActiveTopology) reassignTaskStates() {
	// Clear existing task assignments
	for _, disk := range at.disks {
		disk.pendingTasks = nil
		disk.assignedTasks = nil
		disk.recentTasks = nil
	}

	// Reassign pending tasks
	for _, task := range at.pendingTasks {
		at.assignTaskToDisk(task)
	}

	// Reassign assigned tasks
	for _, task := range at.assignedTasks {
		at.assignTaskToDisk(task)
	}

	// Reassign recent tasks
	for _, task := range at.recentTasks {
		at.assignTaskToDisk(task)
	}
}

// assignTaskToDisk assigns a task to the appropriate disk(s)
func (at *ActiveTopology) assignTaskToDisk(task *taskState) {
	addedDisks := make(map[string]bool)

	// Local helper function to assign task to a disk and avoid code duplication
	assign := func(server string, diskID uint32) {
		key := fmt.Sprintf("%s:%d", server, diskID)
		if server == "" || addedDisks[key] {
			return
		}
		if disk, exists := at.disks[key]; exists {
			switch task.Status {
			case TaskStatusPending:
				disk.pendingTasks = append(disk.pendingTasks, task)
			case TaskStatusInProgress:
				disk.assignedTasks = append(disk.assignedTasks, task)
			case TaskStatusCompleted:
				disk.recentTasks = append(disk.recentTasks, task)
			}
			addedDisks[key] = true
		}
	}

	// Assign to all source disks
	for _, source := range task.Sources {
		assign(source.SourceServer, source.SourceDisk)
	}

	// Assign to all destination disks (duplicates automatically avoided by helper)
	for _, dest := range task.Destinations {
		assign(dest.TargetServer, dest.TargetDisk)
	}
}

// isDiskAvailable checks if a disk can accept new tasks (general availability)
func (at *ActiveTopology) isDiskAvailable(disk *activeDisk, taskType TaskType) bool {
	// Check if disk has too many pending and active tasks
	activeLoad := len(disk.pendingTasks) + len(disk.assignedTasks)
	if activeLoad >= MaxConcurrentTasksPerDisk {
		return false
	}

	// For general availability, only check disk capacity
	// Volume-specific conflicts are checked in isDiskAvailableForVolume
	return true
}

// isDiskAvailableForVolume checks if a disk can accept a new task for a specific volume
func (at *ActiveTopology) isDiskAvailableForVolume(disk *activeDisk, taskType TaskType, volumeID uint32) bool {
	// Check basic availability first
	if !at.isDiskAvailable(disk, taskType) {
		return false
	}

	// Check for volume-specific conflicts in ALL task states:
	// 1. Pending tasks (queued but not yet started)
	for _, task := range disk.pendingTasks {
		if at.areTasksConflicting(task, taskType, volumeID) {
			return false
		}
	}

	// 2. Assigned/Active tasks (currently running)
	for _, task := range disk.assignedTasks {
		if at.areTasksConflicting(task, taskType, volumeID) {
			return false
		}
	}

	// 3. Recent tasks (just completed - avoid immediate re-scheduling on same volume)
	for _, task := range disk.recentTasks {
		if at.areTasksConflicting(task, taskType, volumeID) {
			return false
		}
	}

	return true
}

// areTasksConflicting checks if a new task conflicts with an existing task
func (at *ActiveTopology) areTasksConflicting(existingTask *taskState, newTaskType TaskType, newVolumeID uint32) bool {
	// PRIMARY RULE: Tasks on the same volume always conflict (prevents race conditions)
	if existingTask.VolumeID == newVolumeID {
		return true
	}

	// SECONDARY RULE: Some task types may have global conflicts (rare cases)
	return at.areTaskTypesGloballyConflicting(existingTask.TaskType, newTaskType)
}

// areTaskTypesGloballyConflicting checks for rare global task type conflicts
// These should be minimal - most conflicts should be volume-specific
func (at *ActiveTopology) areTaskTypesGloballyConflicting(existing, new TaskType) bool {
	// Define very limited global conflicts (cross-volume conflicts)
	// Most conflicts should be volume-based, not global
	globalConflictMap := map[TaskType][]TaskType{
		// Example: Some hypothetical global resource conflicts could go here
		// Currently empty - volume-based conflicts are sufficient
	}

	if conflicts, exists := globalConflictMap[existing]; exists {
		for _, conflictType := range conflicts {
			if conflictType == new {
				return true
			}
		}
	}

	return false
}

// cleanupRecentTasks removes old recent tasks
func (at *ActiveTopology) cleanupRecentTasks() {
	cutoff := time.Now().Add(-time.Duration(at.recentTaskWindowSeconds) * time.Second)

	for taskID, task := range at.recentTasks {
		if task.CompletedAt.Before(cutoff) {
			delete(at.recentTasks, taskID)
		}
	}
}
