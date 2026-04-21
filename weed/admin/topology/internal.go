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

// isDiskAvailable checks if a disk can accept new tasks.
//
// Cross-task-type conflicts are intentionally NOT checked here. Different job
// types (Balance, ErasureCoding, Vacuum) operate on different volumes — the
// per-volume safety guarantee is enforced one layer up by HasAnyTask at task
// detection time. Per-disk load shaping is handled separately by
// MaxConcurrentTasksPerDisk and capacity reservation.
//
// A previous implementation declared Balance/EC/Vacuum mutually exclusive per
// disk, which on small clusters could permanently exclude a disk from EC
// placement whenever any unrelated balance task was in flight (see #9147).
func (at *ActiveTopology) isDiskAvailable(disk *activeDisk, taskType TaskType) bool {
	// Check if disk has too many pending and active tasks
	activeLoad := len(disk.pendingTasks) + len(disk.assignedTasks)
	if MaxConcurrentTasksPerDisk > 0 && activeLoad >= MaxConcurrentTasksPerDisk {
		return false
	}

	return true
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
