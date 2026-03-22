package delete_empty

import (
	"github.com/seaweedfs/seaweedfs/weed/worker/tasks/base"
	"github.com/seaweedfs/seaweedfs/weed/worker/types"
)

// Scheduling implements the scheduling logic for compaction tasks
func Scheduling(task *types.TaskInput, runningTasks []*types.TaskInput, availableWorkers []*types.WorkerData, config base.TaskConfig) bool {
	cfg := config.(*Config)

	runningCount := 0
	for _, runningTask := range runningTasks {
		if runningTask.Type == types.TaskTypeCompaction {
			runningCount++
		}
	}

	if runningCount >= cfg.MaxConcurrent {
		return false
	}

	for _, worker := range availableWorkers {
		if worker.CurrentLoad < worker.MaxConcurrent {
			for _, capability := range worker.Capabilities {
				if capability == types.TaskTypeCompaction {
					return true
				}
			}
		}
	}

	return false
}
