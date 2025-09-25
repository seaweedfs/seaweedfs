package ec_vacuum

import (
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/worker/tasks/base"
	"github.com/seaweedfs/seaweedfs/weed/worker/types"
)

// Scheduling determines if an EC vacuum task should be scheduled for execution
func Scheduling(task *types.TaskInput, runningTasks []*types.TaskInput, availableWorkers []*types.WorkerData, config base.TaskConfig) bool {
	ecVacuumConfig, ok := config.(*Config)
	if !ok {
		glog.Errorf("EC vacuum scheduling: invalid config type")
		return false
	}

	// Count running EC vacuum tasks
	runningCount := 0
	for _, runningTask := range runningTasks {
		if runningTask.Type == types.TaskType("ec_vacuum") {
			runningCount++
		}
	}

	// Check concurrency limit
	if runningCount >= ecVacuumConfig.MaxConcurrent {
		glog.V(2).Infof("EC vacuum scheduling: max concurrent limit reached (%d/%d)", runningCount, ecVacuumConfig.MaxConcurrent)
		return false
	}

	// Check if any worker can handle EC vacuum tasks
	hasCapableWorker := false
	var selectedWorker *types.WorkerData

	for _, worker := range availableWorkers {
		if canWorkerHandleEcVacuum(worker, task) {
			hasCapableWorker = true
			selectedWorker = worker
			break
		}
	}

	if !hasCapableWorker {
		glog.V(2).Infof("EC vacuum scheduling: no capable workers available for task %s", task.ID)
		return false
	}

	// Check worker resource availability
	if !hasEnoughResources(selectedWorker, task) {
		glog.V(2).Infof("EC vacuum scheduling: worker %s doesn't have enough resources for task %s",
			selectedWorker.ID, task.ID)
		return false
	}

	// Additional checks for EC vacuum specific requirements
	if !meetsEcVacuumRequirements(task, ecVacuumConfig) {
		glog.V(2).Infof("EC vacuum scheduling: task %s doesn't meet EC vacuum requirements", task.ID)
		return false
	}

	glog.V(1).Infof("EC vacuum scheduling: approved task %s for worker %s", task.ID, selectedWorker.ID)
	return true
}

// canWorkerHandleEcVacuum checks if a worker can handle EC vacuum tasks
func canWorkerHandleEcVacuum(worker *types.WorkerData, task *types.TaskInput) bool {
	// Check if worker has EC vacuum capability
	for _, capability := range worker.Capabilities {
		if capability == types.TaskType("ec_vacuum") {
			return true
		}
		// Also accept workers with general erasure_coding capability
		if capability == types.TaskType("erasure_coding") {
			return true
		}
	}

	glog.V(3).Infof("Worker %s lacks EC vacuum capability", worker.ID)
	return false
}

// hasEnoughResources checks if a worker has sufficient resources for EC vacuum
func hasEnoughResources(worker *types.WorkerData, task *types.TaskInput) bool {
	// Check current load using what's available in WorkerData
	if worker.CurrentLoad >= 2 { // Conservative limit for EC vacuum
		glog.V(3).Infof("Worker %s at capacity: load=%d", worker.ID, worker.CurrentLoad)
		return false
	}

	// EC vacuum tasks require more resources than regular tasks
	// because they involve decode/encode operations
	// We'll assume workers have sufficient resources for now
	// In a production system, these checks would be more sophisticated

	return true
}

// meetsEcVacuumRequirements checks EC vacuum specific requirements
func meetsEcVacuumRequirements(task *types.TaskInput, config *Config) bool {
	// Validate task has required parameters
	if task.VolumeID == 0 {
		glog.V(3).Infof("EC vacuum task %s missing volume ID", task.ID)
		return false
	}

	// Check if this is during allowed time windows (if any restrictions)
	// For now, we allow EC vacuum anytime, but this could be made configurable

	// Validate collection filter if specified
	if config.CollectionFilter != "" && task.Collection != config.CollectionFilter {
		glog.V(3).Infof("EC vacuum task %s collection %s doesn't match filter %s",
			task.ID, task.Collection, config.CollectionFilter)
		return false
	}

	// Additional safety checks could be added here, such as:
	// - Checking if volume is currently being written to
	// - Verifying minimum deletion threshold is still met
	// - Ensuring cluster health is good for such operations

	return true
}

// GetResourceRequirements returns the resource requirements for EC vacuum tasks
func GetResourceRequirements() map[string]interface{} {
	return map[string]interface{}{
		"MinConcurrentSlots":   2,    // Need extra slots for decode/encode
		"MinDiskSpaceGB":       10,   // Minimum 10GB free space
		"MinMemoryMB":          1024, // 1GB memory for operations
		"PreferredNetworkMbps": 100,  // Good network for shard transfers
		"RequiredCapabilities": []string{"ec_vacuum", "erasure_coding"},
		"ConflictingTaskTypes": []string{"erasure_coding"}, // Don't run with regular EC tasks on same volume
	}
}

// CalculateTaskPriority calculates priority for EC vacuum tasks
func CalculateTaskPriority(task *types.TaskInput, metrics *types.VolumeHealthMetrics) types.TaskPriority {
	// Higher priority for larger volumes (more space to reclaim)
	if task.VolumeID > 1000000 { // Rough size indicator
		return types.TaskPriorityMedium
	}

	// Default priority
	return types.TaskPriorityLow
}
