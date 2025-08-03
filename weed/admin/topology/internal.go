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

	// Assign to all source disks
	for _, source := range task.Sources {
		sourceKey := fmt.Sprintf("%s:%d", source.SourceServer, source.SourceDisk)
		if !addedDisks[sourceKey] {
			if sourceDisk, exists := at.disks[sourceKey]; exists {
				switch task.Status {
				case TaskStatusPending:
					sourceDisk.pendingTasks = append(sourceDisk.pendingTasks, task)
				case TaskStatusInProgress:
					sourceDisk.assignedTasks = append(sourceDisk.assignedTasks, task)
				case TaskStatusCompleted:
					sourceDisk.recentTasks = append(sourceDisk.recentTasks, task)
				}
				addedDisks[sourceKey] = true
			}
		}
	}

	// Assign to all destination disks (avoid duplicates with sources)
	for _, dest := range task.Destinations {
		destKey := fmt.Sprintf("%s:%d", dest.TargetServer, dest.TargetDisk)
		if !addedDisks[destKey] {
			if destDisk, exists := at.disks[destKey]; exists {
				switch task.Status {
				case TaskStatusPending:
					destDisk.pendingTasks = append(destDisk.pendingTasks, task)
				case TaskStatusInProgress:
					destDisk.assignedTasks = append(destDisk.assignedTasks, task)
				case TaskStatusCompleted:
					destDisk.recentTasks = append(destDisk.recentTasks, task)
				}
				addedDisks[destKey] = true
			}
		}
	}
}

// isDiskAvailable checks if a disk can accept new tasks
func (at *ActiveTopology) isDiskAvailable(disk *activeDisk, taskType TaskType) bool {
	// Check if disk has too many pending and active tasks
	activeLoad := len(disk.pendingTasks) + len(disk.assignedTasks)
	if activeLoad >= MaxConcurrentTasksPerDisk {
		return false
	}

	// Check for conflicting task types
	for _, task := range disk.assignedTasks {
		if at.areTaskTypesConflicting(task.TaskType, taskType) {
			return false
		}
	}

	return true
}

// areTaskTypesConflicting checks if two task types conflict
func (at *ActiveTopology) areTaskTypesConflicting(existing, new TaskType) bool {
	// Examples of conflicting task types
	conflictMap := map[TaskType][]TaskType{
		TaskTypeVacuum:        {TaskTypeBalance, TaskTypeErasureCoding},
		TaskTypeBalance:       {TaskTypeVacuum, TaskTypeErasureCoding},
		TaskTypeErasureCoding: {TaskTypeVacuum, TaskTypeBalance},
	}

	if conflicts, exists := conflictMap[existing]; exists {
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
