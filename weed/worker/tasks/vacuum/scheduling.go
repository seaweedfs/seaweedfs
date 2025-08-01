package vacuum

import (
	"github.com/seaweedfs/seaweedfs/weed/worker/tasks/base"
	"github.com/seaweedfs/seaweedfs/weed/worker/types"
)

// Scheduling implements the scheduling logic for vacuum tasks
func Scheduling(task *types.TaskInput, runningTasks []*types.TaskInput, availableWorkers []*types.WorkerData, config base.TaskConfig) bool {
	vacuumConfig := config.(*Config)

	// Count running vacuum tasks
	runningVacuumCount := 0
	for _, runningTask := range runningTasks {
		if runningTask.Type == types.TaskTypeVacuum {
			runningVacuumCount++
		}
	}

	// Check concurrency limit
	if runningVacuumCount >= vacuumConfig.MaxConcurrent {
		return false
	}

	// Check for available workers with vacuum capability
	for _, worker := range availableWorkers {
		if worker.CurrentLoad < worker.MaxConcurrent {
			for _, capability := range worker.Capabilities {
				if capability == types.TaskTypeVacuum {
					return true
				}
			}
		}
	}

	return false
}
