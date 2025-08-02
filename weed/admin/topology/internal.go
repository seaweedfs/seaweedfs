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
	// Assign to source disk
	sourceKey := fmt.Sprintf("%s:%d", task.SourceServer, task.SourceDisk)
	if sourceDisk, exists := at.disks[sourceKey]; exists {
		switch task.Status {
		case TaskStatusPending:
			sourceDisk.pendingTasks = append(sourceDisk.pendingTasks, task)
		case TaskStatusInProgress:
			sourceDisk.assignedTasks = append(sourceDisk.assignedTasks, task)
		case TaskStatusCompleted:
			sourceDisk.recentTasks = append(sourceDisk.recentTasks, task)
		}
	}

	// Assign to target disk if it exists and is different from source
	if task.TargetServer != "" && (task.TargetServer != task.SourceServer || task.TargetDisk != task.SourceDisk) {
		targetKey := fmt.Sprintf("%s:%d", task.TargetServer, task.TargetDisk)
		if targetDisk, exists := at.disks[targetKey]; exists {
			switch task.Status {
			case TaskStatusPending:
				targetDisk.pendingTasks = append(targetDisk.pendingTasks, task)
			case TaskStatusInProgress:
				targetDisk.assignedTasks = append(targetDisk.assignedTasks, task)
			case TaskStatusCompleted:
				targetDisk.recentTasks = append(targetDisk.recentTasks, task)
			}
		}
	}
}

// isDiskAvailable checks if a disk can accept new tasks
func (at *ActiveTopology) isDiskAvailable(disk *activeDisk, taskType TaskType) bool {
	// Check if disk has too many active tasks
	activeLoad := len(disk.pendingTasks) + len(disk.assignedTasks)
	if activeLoad >= 2 { // Max 2 concurrent tasks per disk
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
