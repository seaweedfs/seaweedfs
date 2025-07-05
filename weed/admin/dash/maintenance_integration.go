package dash

import (
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/worker/tasks/balance"
	"github.com/seaweedfs/seaweedfs/weed/worker/tasks/cluster_replication"
	"github.com/seaweedfs/seaweedfs/weed/worker/tasks/erasure_coding"
	"github.com/seaweedfs/seaweedfs/weed/worker/tasks/remote_upload"
	"github.com/seaweedfs/seaweedfs/weed/worker/tasks/replication"
	"github.com/seaweedfs/seaweedfs/weed/worker/tasks/vacuum"
	"github.com/seaweedfs/seaweedfs/weed/worker/types"
)

// MaintenanceIntegration bridges the task system with existing maintenance
type MaintenanceIntegration struct {
	taskRegistry *types.TaskRegistry
	uiRegistry   *types.UIRegistry

	// Bridge to existing system
	maintenanceQueue  *MaintenanceQueue
	maintenancePolicy *MaintenancePolicy
}

// NewMaintenanceIntegration creates the integration bridge
func NewMaintenanceIntegration(queue *MaintenanceQueue, policy *MaintenancePolicy) *MaintenanceIntegration {
	integration := &MaintenanceIntegration{
		taskRegistry:      types.NewTaskRegistry(),
		uiRegistry:        types.NewUIRegistry(),
		maintenanceQueue:  queue,
		maintenancePolicy: policy,
	}

	// Register all tasks
	integration.registerAllTasks()

	return integration
}

// registerAllTasks registers all available tasks
func (s *MaintenanceIntegration) registerAllTasks() {
	// Register vacuum task with both logic and UI
	vacuum.RegisterSimple(s.taskRegistry)

	// Register erasure coding task
	erasure_coding.RegisterSimple(s.taskRegistry)

	// Register remote upload task
	remote_upload.RegisterSimple(s.taskRegistry)

	// Register replication task
	replication.RegisterSimple(s.taskRegistry)

	// Register balance task
	balance.RegisterSimple(s.taskRegistry)

	// Register cluster replication task
	cluster_replication.RegisterSimple(s.taskRegistry)

	// Configure tasks from policy
	s.configureTasksFromPolicy()

	glog.V(1).Infof("Registered tasks: vacuum, erasure_coding, remote_upload, replication, balance, cluster_replication")
}

// configureTasksFromPolicy dynamically configures all registered tasks based on the maintenance policy
func (s *MaintenanceIntegration) configureTasksFromPolicy() {
	if s.maintenancePolicy == nil {
		return
	}

	// Configure all registered detectors and schedulers dynamically
	configuredCount := 0

	// Get all registered task types from the registry
	for taskType, detector := range s.taskRegistry.GetAllDetectors() {
		// Configure detector based on task type
		s.configureDetectorForTaskType(taskType, detector)
		configuredCount++
	}

	for taskType, scheduler := range s.taskRegistry.GetAllSchedulers() {
		// Configure scheduler based on task type
		s.configureSchedulerForTaskType(taskType, scheduler)
	}

	glog.V(1).Infof("Dynamically configured %d task types from maintenance policy", configuredCount)
}

// configureDetectorForTaskType configures a detector based on its task type and the maintenance policy
func (s *MaintenanceIntegration) configureDetectorForTaskType(taskType types.TaskType, detector types.TaskDetector) {
	switch taskType {
	case types.TaskTypeVacuum:
		if vacuumDetector := vacuum.GetDetector(s.taskRegistry); vacuumDetector != nil {
			vacuumDetector.SetEnabled(s.maintenancePolicy.VacuumEnabled)
			vacuumDetector.SetGarbageThreshold(s.maintenancePolicy.VacuumGarbageRatio)
			vacuumDetector.SetMinVolumeAge(time.Duration(s.maintenancePolicy.VacuumMinInterval) * time.Hour)
		}
	case types.TaskTypeErasureCoding:
		if ecDetector := erasure_coding.GetSimpleDetector(s.taskRegistry); ecDetector != nil {
			ecDetector.SetEnabled(s.maintenancePolicy.ECEnabled)
			ecDetector.SetVolumeAgeHours(s.maintenancePolicy.ECVolumeAgeHours)
			ecDetector.SetFullnessRatio(s.maintenancePolicy.ECFullnessRatio)
		}
	case types.TaskTypeRemoteUpload:
		if remoteDetector := remote_upload.GetSimpleDetector(s.taskRegistry); remoteDetector != nil {
			remoteDetector.SetEnabled(s.maintenancePolicy.RemoteUploadEnabled)
			remoteDetector.SetAgeHours(s.maintenancePolicy.RemoteUploadAgeHours)
			remoteDetector.SetPattern(s.maintenancePolicy.RemoteUploadPattern)
		}
	case types.TaskTypeFixReplication:
		if replDetector := replication.GetSimpleDetector(s.taskRegistry); replDetector != nil {
			replDetector.SetEnabled(s.maintenancePolicy.ReplicationFixEnabled)
		}
	case types.TaskTypeBalance:
		if balanceDetector := balance.GetSimpleDetector(s.taskRegistry); balanceDetector != nil {
			balanceDetector.SetEnabled(s.maintenancePolicy.BalanceEnabled)
			balanceDetector.SetThreshold(s.maintenancePolicy.BalanceThreshold)
		}
	case types.TaskTypeClusterReplication:
		if clusterDetector := cluster_replication.GetSimpleDetector(s.taskRegistry); clusterDetector != nil {
			clusterDetector.SetEnabled(false) // Disabled by default as it needs MQ setup
		}
	default:
		glog.V(2).Infof("No specific configuration for detector task type: %s", taskType)
	}
}

// configureSchedulerForTaskType configures a scheduler based on its task type and the maintenance policy
func (s *MaintenanceIntegration) configureSchedulerForTaskType(taskType types.TaskType, scheduler types.TaskScheduler) {
	switch taskType {
	case types.TaskTypeVacuum:
		if vacuumScheduler := vacuum.GetScheduler(s.taskRegistry); vacuumScheduler != nil {
			vacuumScheduler.SetMaxConcurrent(s.maintenancePolicy.VacuumMaxConcurrent)
			vacuumScheduler.SetMinInterval(time.Duration(s.maintenancePolicy.VacuumMinInterval) * time.Hour)
		}
	case types.TaskTypeErasureCoding:
		if ecScheduler := erasure_coding.GetSimpleScheduler(s.taskRegistry); ecScheduler != nil {
			ecScheduler.SetEnabled(s.maintenancePolicy.ECEnabled)
			ecScheduler.SetMaxConcurrent(s.maintenancePolicy.ECMaxConcurrent)
		}
	case types.TaskTypeRemoteUpload:
		if remoteScheduler := remote_upload.GetSimpleScheduler(s.taskRegistry); remoteScheduler != nil {
			remoteScheduler.SetEnabled(s.maintenancePolicy.RemoteUploadEnabled)
			remoteScheduler.SetMaxConcurrent(s.maintenancePolicy.RemoteUploadMaxConcurrent)
		}
	case types.TaskTypeFixReplication:
		if replScheduler := replication.GetSimpleScheduler(s.taskRegistry); replScheduler != nil {
			replScheduler.SetEnabled(s.maintenancePolicy.ReplicationFixEnabled)
			replScheduler.SetMaxConcurrent(s.maintenancePolicy.ReplicationMaxConcurrent)
		}
	case types.TaskTypeBalance:
		if balanceScheduler := balance.GetSimpleScheduler(s.taskRegistry); balanceScheduler != nil {
			balanceScheduler.SetEnabled(s.maintenancePolicy.BalanceEnabled)
			balanceScheduler.SetMaxConcurrent(s.maintenancePolicy.BalanceMaxConcurrent)
		}
	case types.TaskTypeClusterReplication:
		if clusterScheduler := cluster_replication.GetSimpleScheduler(s.taskRegistry); clusterScheduler != nil {
			clusterScheduler.SetEnabled(false) // Disabled by default as it needs MQ setup
			clusterScheduler.SetMaxConcurrent(3)
		}
	default:
		glog.V(2).Infof("No specific configuration for scheduler task type: %s", taskType)
	}
}

// ScanWithTaskDetectors performs a scan using the task system
func (s *MaintenanceIntegration) ScanWithTaskDetectors(volumeMetrics []*types.VolumeHealthMetrics) ([]*TaskDetectionResult, error) {
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

		glog.V(2).Infof("Running detection for task type: %s", taskType)

		results, err := detector.ScanForTasks(volumeMetrics, clusterInfo)
		if err != nil {
			glog.Errorf("Failed to scan for %s tasks: %v", taskType, err)
			continue
		}

		// Convert results to existing system format
		for _, result := range results {
			existingResult := s.convertToExistingFormat(result)
			allResults = append(allResults, existingResult)
		}

		glog.V(2).Infof("Found %d %s tasks", len(results), taskType)
	}

	return allResults, nil
}

// convertToExistingFormat converts task results to existing system format
func (s *MaintenanceIntegration) convertToExistingFormat(result *types.TaskDetectionResult) *TaskDetectionResult {
	// Convert types between task system and existing system
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
	case types.TaskTypeClusterReplication:
		existingType = TaskTypeClusterReplication
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

// CanScheduleWithTaskSchedulers determines if a task can be scheduled using task schedulers
func (s *MaintenanceIntegration) CanScheduleWithTaskSchedulers(task *MaintenanceTask, runningTasks []*MaintenanceTask, availableWorkers []*MaintenanceWorker) bool {
	// Convert existing types to task types
	taskObject := s.convertTaskToTaskSystem(task)
	runningTaskObjects := s.convertTasksToTaskSystem(runningTasks)
	workerObjects := s.convertWorkersToTaskSystem(availableWorkers)

	// Get the appropriate scheduler for all task types
	var taskType types.TaskType
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
	case TaskTypeClusterReplication:
		taskType = types.TaskTypeClusterReplication
	default:
		return false // Fallback to existing logic for unknown types
	}

	scheduler := s.taskRegistry.GetScheduler(taskType)
	if scheduler == nil {
		return false
	}

	return scheduler.CanScheduleNow(taskObject, runningTaskObjects, workerObjects)
}

// convertTaskToTaskSystem converts existing task to task system format
func (s *MaintenanceIntegration) convertTaskToTaskSystem(task *MaintenanceTask) *types.Task {
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
	case TaskTypeClusterReplication:
		taskType = types.TaskTypeClusterReplication
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

// convertTasksToTaskSystem converts multiple tasks
func (s *MaintenanceIntegration) convertTasksToTaskSystem(tasks []*MaintenanceTask) []*types.Task {
	var result []*types.Task
	for _, task := range tasks {
		result = append(result, s.convertTaskToTaskSystem(task))
	}
	return result
}

// convertWorkersToTaskSystem converts workers to task system format
func (s *MaintenanceIntegration) convertWorkersToTaskSystem(workers []*MaintenanceWorker) []*types.Worker {
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
			case TaskTypeClusterReplication:
				capabilities = append(capabilities, types.TaskTypeClusterReplication)
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
func (s *MaintenanceIntegration) GetUIProvider(taskType MaintenanceTaskType) types.TaskUIProvider {
	var taskSystemType types.TaskType
	switch taskType {
	case TaskTypeVacuum:
		taskSystemType = types.TaskTypeVacuum
	case TaskTypeErasureCoding:
		taskSystemType = types.TaskTypeErasureCoding
	default:
		return nil
	}

	return s.uiRegistry.GetProvider(taskSystemType)
}

// GetAllTaskStats returns stats for all registered tasks
func (s *MaintenanceIntegration) GetAllTaskStats() []*types.TaskStats {
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
