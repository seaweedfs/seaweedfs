package erasure_coding

import (
	"github.com/seaweedfs/seaweedfs/weed/worker/tasks/base"
	"github.com/seaweedfs/seaweedfs/weed/worker/types"
)

// Scheduling implements the scheduling logic for erasure coding tasks
func Scheduling(task *types.TaskInput, runningTasks []*types.TaskInput, availableWorkers []*types.WorkerData, config base.TaskConfig) bool {
	ecConfig := config.(*Config)

	// Check if we have available workers
	if len(availableWorkers) == 0 {
		return false
	}

	// Count running EC tasks
	runningCount := 0
	for _, runningTask := range runningTasks {
		if runningTask.Type == types.TaskTypeErasureCoding {
			runningCount++
		}
	}

	// Check concurrency limit
	if runningCount >= ecConfig.MaxConcurrent {
		return false
	}

	// Check if any worker can handle EC tasks
	for _, worker := range availableWorkers {
		for _, capability := range worker.Capabilities {
			if capability == types.TaskTypeErasureCoding {
				return true
			}
		}
	}

	return false
}
