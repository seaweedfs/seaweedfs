package ec_balance

import (
	"github.com/seaweedfs/seaweedfs/weed/worker/tasks/base"
	"github.com/seaweedfs/seaweedfs/weed/worker/types"
)

// Scheduling implements the scheduling logic for EC balance tasks
func Scheduling(task *types.TaskInput, runningTasks []*types.TaskInput, availableWorkers []*types.WorkerData, config base.TaskConfig) bool {
	ecbConfig := config.(*Config)

	// Check if we have available workers
	if len(availableWorkers) == 0 {
		return false
	}

	// Count running EC balance tasks
	runningCount := 0
	for _, runningTask := range runningTasks {
		if runningTask.Type == types.TaskTypeECBalance {
			runningCount++
		}
	}

	// Check concurrency limit
	if runningCount >= ecbConfig.MaxConcurrent {
		return false
	}

	// Check if any worker can handle EC balance tasks
	for _, worker := range availableWorkers {
		for _, capability := range worker.Capabilities {
			if capability == types.TaskTypeECBalance {
				return true
			}
		}
	}

	return false
}
