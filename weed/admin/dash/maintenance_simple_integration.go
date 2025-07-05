package dash

import (
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/worker/tasks/vacuum"
	"github.com/seaweedfs/seaweedfs/weed/worker/types"
)

// SimplifiedMaintenanceIntegration bridges the simplified task system with existing maintenance
type SimplifiedMaintenanceIntegration struct {
	taskRegistry *types.SimpleTaskRegistry
	uiRegistry   *types.UIRegistry

	// Bridge to existing system
	maintenanceQueue  *MaintenanceQueue
	maintenancePolicy *MaintenancePolicy
}

// NewSimplifiedMaintenanceIntegration creates the integration bridge
func NewSimplifiedMaintenanceIntegration(queue *MaintenanceQueue, policy *MaintenancePolicy) *SimplifiedMaintenanceIntegration {
	integration := &SimplifiedMaintenanceIntegration{
		taskRegistry:      types.NewSimpleTaskRegistry(),
		uiRegistry:        types.NewUIRegistry(),
		maintenanceQueue:  queue,
		maintenancePolicy: policy,
	}

	// Register all simplified tasks
	integration.registerAllTasks()

	return integration
}

// registerAllTasks registers all available simplified tasks
func (s *SimplifiedMaintenanceIntegration) registerAllTasks() {
	// Register vacuum task with both logic and UI
	vacuum.RegisterSimple(s.taskRegistry)

	// TODO: Register UI providers when vacuum.RegisterUI is implemented
	// detector := s.taskRegistry.GetDetector(types.TaskTypeVacuum).(*vacuum.SimpleDetector)
	// scheduler := s.taskRegistry.GetScheduler(types.TaskTypeVacuum).(*vacuum.SimpleScheduler)
	// vacuum.RegisterUI(s.uiRegistry, detector, scheduler)

	glog.V(1).Infof("Registered simplified tasks")
}

// ScanWithSimplifiedTasks performs a scan using the simplified task system
func (s *SimplifiedMaintenanceIntegration) ScanWithSimplifiedTasks(volumeMetrics []*types.VolumeHealthMetrics) ([]*TaskDetectionResult, error) {
	var allResults []*TaskDetectionResult

	// Create cluster info
	clusterInfo := &types.ClusterInfo{
		TotalVolumes: len(volumeMetrics),
		LastUpdated:  time.Now(),
	}

	// Run detection for each registered task type
	for taskType, detector := range s.taskRegistry.GetAllDetectors() {
		if !detector.IsEnabled() {
			continue
		}

		glog.V(2).Infof("Running simplified detection for task type: %s", taskType)

		results, err := detector.ScanForTasks(volumeMetrics, clusterInfo)
		if err != nil {
			glog.Errorf("Failed to scan for %s tasks: %v", taskType, err)
			continue
		}

		// Convert simplified results to existing system format
		for _, result := range results {
			existingResult := s.convertToExistingFormat(result)
			allResults = append(allResults, existingResult)
		}

		glog.V(2).Infof("Found %d %s tasks", len(results), taskType)
	}

	return allResults, nil
}

// convertToExistingFormat converts simplified task results to existing system format
func (s *SimplifiedMaintenanceIntegration) convertToExistingFormat(result *types.TaskDetectionResult) *TaskDetectionResult {
	// Convert types between simplified and existing system
	var existingType MaintenanceTaskType
	var existingPriority MaintenanceTaskPriority

	switch result.TaskType {
	case types.TaskTypeVacuum:
		existingType = TaskTypeVacuum
	case types.TaskTypeErasureCoding:
		existingType = TaskTypeErasureCoding
	case types.TaskTypeRemoteUpload:
		existingType = TaskTypeRemoteUpload
	case types.TaskTypeFixReplication:
		existingType = TaskTypeFixReplication
	case types.TaskTypeBalance:
		existingType = TaskTypeBalance
	default:
		existingType = TaskTypeVacuum
	}

	switch result.Priority {
	case types.TaskPriorityLow:
		existingPriority = PriorityLow
	case types.TaskPriorityNormal:
		existingPriority = PriorityNormal
	case types.TaskPriorityHigh:
		existingPriority = PriorityHigh
	}

	return &TaskDetectionResult{
		TaskType:   existingType,
		VolumeID:   result.VolumeID,
		Server:     result.Server,
		Collection: result.Collection,
		Priority:   existingPriority,
		Reason:     result.Reason,
		Parameters: result.Parameters,
		ScheduleAt: result.ScheduleAt,
	}
}

// CanScheduleWithSimplifiedLogic determines if a task can be scheduled using simplified logic
func (s *SimplifiedMaintenanceIntegration) CanScheduleWithSimplifiedLogic(task *MaintenanceTask, runningTasks []*MaintenanceTask, availableWorkers []*MaintenanceWorker) bool {
	// Convert existing types to simplified types
	simplifiedTask := s.convertTaskToSimplified(task)
	simplifiedRunningTasks := s.convertTasksToSimplified(runningTasks)
	simplifiedWorkers := s.convertWorkersToSimplified(availableWorkers)

	// Get the appropriate scheduler
	var taskType types.TaskType
	switch task.Type {
	case TaskTypeVacuum:
		taskType = types.TaskTypeVacuum
	case TaskTypeErasureCoding:
		taskType = types.TaskTypeErasureCoding
	default:
		return false // Fallback to existing logic
	}

	scheduler := s.taskRegistry.GetScheduler(taskType)
	if scheduler == nil {
		return false
	}

	return scheduler.CanScheduleNow(simplifiedTask, simplifiedRunningTasks, simplifiedWorkers)
}

// convertTaskToSimplified converts existing task to simplified format
func (s *SimplifiedMaintenanceIntegration) convertTaskToSimplified(task *MaintenanceTask) *types.Task {
	var taskType types.TaskType
	var priority types.TaskPriority

	switch task.Type {
	case TaskTypeVacuum:
		taskType = types.TaskTypeVacuum
	case TaskTypeErasureCoding:
		taskType = types.TaskTypeErasureCoding
	case TaskTypeRemoteUpload:
		taskType = types.TaskTypeRemoteUpload
	case TaskTypeFixReplication:
		taskType = types.TaskTypeFixReplication
	case TaskTypeBalance:
		taskType = types.TaskTypeBalance
	}

	switch task.Priority {
	case PriorityLow:
		priority = types.TaskPriorityLow
	case PriorityNormal:
		priority = types.TaskPriorityNormal
	case PriorityHigh:
		priority = types.TaskPriorityHigh
	}

	return &types.Task{
		ID:         task.ID,
		Type:       taskType,
		Priority:   priority,
		VolumeID:   task.VolumeID,
		Server:     task.Server,
		Collection: task.Collection,
		Parameters: task.Parameters,
		CreatedAt:  task.CreatedAt,
	}
}

// convertTasksToSimplified converts multiple tasks
func (s *SimplifiedMaintenanceIntegration) convertTasksToSimplified(tasks []*MaintenanceTask) []*types.Task {
	var result []*types.Task
	for _, task := range tasks {
		result = append(result, s.convertTaskToSimplified(task))
	}
	return result
}

// convertWorkersToSimplified converts workers to simplified format
func (s *SimplifiedMaintenanceIntegration) convertWorkersToSimplified(workers []*MaintenanceWorker) []*types.Worker {
	var result []*types.Worker
	for _, worker := range workers {
		capabilities := make([]types.TaskType, 0, len(worker.Capabilities))
		for _, cap := range worker.Capabilities {
			switch cap {
			case TaskTypeVacuum:
				capabilities = append(capabilities, types.TaskTypeVacuum)
			case TaskTypeErasureCoding:
				capabilities = append(capabilities, types.TaskTypeErasureCoding)
			case TaskTypeRemoteUpload:
				capabilities = append(capabilities, types.TaskTypeRemoteUpload)
			case TaskTypeFixReplication:
				capabilities = append(capabilities, types.TaskTypeFixReplication)
			case TaskTypeBalance:
				capabilities = append(capabilities, types.TaskTypeBalance)
			}
		}

		result = append(result, &types.Worker{
			ID:            worker.ID,
			Address:       worker.Address,
			Capabilities:  capabilities,
			MaxConcurrent: worker.MaxConcurrent,
			CurrentLoad:   worker.CurrentLoad,
		})
	}
	return result
}

// GetUIProvider returns the UI provider for a task type
func (s *SimplifiedMaintenanceIntegration) GetUIProvider(taskType MaintenanceTaskType) types.TaskUIProvider {
	var simplifiedType types.TaskType
	switch taskType {
	case TaskTypeVacuum:
		simplifiedType = types.TaskTypeVacuum
	case TaskTypeErasureCoding:
		simplifiedType = types.TaskTypeErasureCoding
	default:
		return nil
	}

	return s.uiRegistry.GetProvider(simplifiedType)
}

// GetAllTaskStats returns stats for all registered simplified tasks
func (s *SimplifiedMaintenanceIntegration) GetAllTaskStats() []*types.TaskStats {
	var stats []*types.TaskStats

	for taskType, detector := range s.taskRegistry.GetAllDetectors() {
		uiProvider := s.uiRegistry.GetProvider(taskType)
		if uiProvider == nil {
			continue
		}

		stat := &types.TaskStats{
			TaskType:      taskType,
			DisplayName:   uiProvider.GetDisplayName(),
			Enabled:       detector.IsEnabled(),
			LastScan:      time.Now().Add(-detector.ScanInterval()),
			NextScan:      time.Now().Add(detector.ScanInterval()),
			ScanInterval:  detector.ScanInterval(),
			MaxConcurrent: s.taskRegistry.GetScheduler(taskType).GetMaxConcurrent(),
			// Would need to get these from actual queue/stats
			PendingTasks:   0,
			RunningTasks:   0,
			CompletedToday: 0,
			FailedToday:    0,
		}

		stats = append(stats, stat)
	}

	return stats
}

// Example usage:
// integration := NewSimplifiedMaintenanceIntegration(queue, policy)
// results, err := integration.ScanWithSimplifiedTasks(volumeMetrics)
// canSchedule := integration.CanScheduleWithSimplifiedLogic(task, runningTasks, workers)
