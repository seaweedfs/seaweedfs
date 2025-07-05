package cluster_replication

import (
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/worker/types"
)

// SimpleScheduler implements cluster replication task scheduling
type SimpleScheduler struct {
	maxConcurrent int
	enabled       bool
}

// NewSimpleScheduler creates a new cluster replication scheduler
func NewSimpleScheduler() *SimpleScheduler {
	return &SimpleScheduler{
		maxConcurrent: 3,     // Default for cluster replication
		enabled:       false, // Disabled by default as it needs MQ setup
	}
}

// GetTaskType returns the task type
func (s *SimpleScheduler) GetTaskType() types.TaskType {
	return types.TaskTypeClusterReplication
}

// CanScheduleNow determines if a cluster replication task can be scheduled now
func (s *SimpleScheduler) CanScheduleNow(task *types.Task, runningTasks []*types.Task, availableWorkers []*types.Worker) bool {
	if !s.enabled {
		return false
	}

	// Check if we have available workers
	if len(availableWorkers) == 0 {
		return false
	}

	// Count running cluster replication tasks
	runningCount := 0
	for _, runningTask := range runningTasks {
		if runningTask.Type == types.TaskTypeClusterReplication {
			runningCount++
		}
	}

	// Check concurrency limit
	if runningCount >= s.maxConcurrent {
		glog.V(3).Infof("Cluster replication scheduler: at concurrency limit (%d/%d)", runningCount, s.maxConcurrent)
		return false
	}

	// Check if any worker can handle cluster replication tasks
	for _, worker := range availableWorkers {
		for _, capability := range worker.Capabilities {
			if capability == types.TaskTypeClusterReplication {
				glog.V(3).Infof("Cluster replication scheduler: can schedule task")
				return true
			}
		}
	}

	return false
}

// GetMaxConcurrent returns the maximum number of concurrent tasks
func (s *SimpleScheduler) GetMaxConcurrent() int {
	return s.maxConcurrent
}

// GetPriority returns the priority for this task
func (s *SimpleScheduler) GetPriority(task *types.Task) types.TaskPriority {
	// Check replication mode from parameters
	if task.Parameters != nil {
		if mode, ok := task.Parameters["replication_mode"].(string); ok {
			switch mode {
			case "sync":
				return types.TaskPriorityHigh
			case "backup":
				return types.TaskPriorityLow
			}
		}
	}
	return types.TaskPriorityNormal
}

// WasTaskRecentlyCompleted checks if a similar task was recently completed
func (s *SimpleScheduler) WasTaskRecentlyCompleted(task *types.Task, completedTasks []*types.Task, now time.Time) bool {
	// Check if the same file was recently replicated
	interval := 1 * time.Hour // Allow retry sooner for cluster replication
	cutoff := now.Add(-interval)

	if task.Parameters == nil {
		return false
	}

	sourcePath, hasSource := task.Parameters["source_path"].(string)
	targetCluster, hasTarget := task.Parameters["target_cluster"].(string)

	if !hasSource || !hasTarget {
		return false
	}

	for _, completedTask := range completedTasks {
		if completedTask.Type == types.TaskTypeClusterReplication &&
			completedTask.Status == types.TaskStatusCompleted &&
			completedTask.CompletedAt != nil &&
			completedTask.CompletedAt.After(cutoff) &&
			completedTask.Parameters != nil {

			if completedSource, ok := completedTask.Parameters["source_path"].(string); ok {
				if completedTarget, ok := completedTask.Parameters["target_cluster"].(string); ok {
					if completedSource == sourcePath && completedTarget == targetCluster {
						return true
					}
				}
			}
		}
	}
	return false
}

// IsEnabled returns whether this task type is enabled
func (s *SimpleScheduler) IsEnabled() bool {
	return s.enabled
}

// Configuration setters

func (s *SimpleScheduler) SetEnabled(enabled bool) {
	s.enabled = enabled
}

func (s *SimpleScheduler) SetMaxConcurrent(max int) {
	s.maxConcurrent = max
}
