package dash

import (
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/worker/tasks/balance"
	"github.com/seaweedfs/seaweedfs/weed/worker/tasks/erasure_coding"
	"github.com/seaweedfs/seaweedfs/weed/worker/tasks/remote_upload"
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

	// Type conversion maps
	taskTypeMap    map[types.TaskType]MaintenanceTaskType
	revTaskTypeMap map[MaintenanceTaskType]types.TaskType
	priorityMap    map[types.TaskPriority]MaintenanceTaskPriority
	revPriorityMap map[MaintenanceTaskPriority]types.TaskPriority
}

// NewMaintenanceIntegration creates the integration bridge
func NewMaintenanceIntegration(queue *MaintenanceQueue, policy *MaintenancePolicy) *MaintenanceIntegration {
	integration := &MaintenanceIntegration{
		taskRegistry:      types.NewTaskRegistry(),
		uiRegistry:        types.NewUIRegistry(),
		maintenanceQueue:  queue,
		maintenancePolicy: policy,
	}

	// Initialize type conversion maps
	integration.initializeTypeMaps()

	// Register all tasks
	integration.registerAllTasks()

	return integration
}

// initializeTypeMaps creates the type conversion maps for dynamic conversion
func (s *MaintenanceIntegration) initializeTypeMaps() {
	// Task type mappings
	s.taskTypeMap = map[types.TaskType]MaintenanceTaskType{
		types.TaskTypeVacuum:             TaskTypeVacuum,
		types.TaskTypeErasureCoding:      TaskTypeErasureCoding,
		types.TaskTypeRemoteUpload:       TaskTypeRemoteUpload,
		types.TaskTypeFixReplication:     TaskTypeFixReplication,
		types.TaskTypeBalance:            TaskTypeBalance,
		types.TaskTypeClusterReplication: TaskTypeClusterReplication,
	}

	// Reverse task type mappings
	s.revTaskTypeMap = map[MaintenanceTaskType]types.TaskType{
		TaskTypeVacuum:             types.TaskTypeVacuum,
		TaskTypeErasureCoding:      types.TaskTypeErasureCoding,
		TaskTypeRemoteUpload:       types.TaskTypeRemoteUpload,
		TaskTypeFixReplication:     types.TaskTypeFixReplication,
		TaskTypeBalance:            types.TaskTypeBalance,
		TaskTypeClusterReplication: types.TaskTypeClusterReplication,
	}

	// Priority mappings
	s.priorityMap = map[types.TaskPriority]MaintenanceTaskPriority{
		types.TaskPriorityLow:    PriorityLow,
		types.TaskPriorityNormal: PriorityNormal,
		types.TaskPriorityHigh:   PriorityHigh,
	}

	// Reverse priority mappings
	s.revPriorityMap = map[MaintenanceTaskPriority]types.TaskPriority{
		PriorityLow:      types.TaskPriorityLow,
		PriorityNormal:   types.TaskPriorityNormal,
		PriorityHigh:     types.TaskPriorityHigh,
		PriorityCritical: types.TaskPriorityHigh, // Map critical to high
	}
}

// registerAllTasks registers all available tasks
func (s *MaintenanceIntegration) registerAllTasks() {
	// Register vacuum task with both logic and UI
	vacuum.RegisterSimple(s.taskRegistry)

	// Register erasure coding task
	erasure_coding.RegisterSimple(s.taskRegistry)

	// Register remote upload task
	remote_upload.RegisterSimple(s.taskRegistry)

	// Register balance task
	balance.RegisterSimple(s.taskRegistry)

	// Configure tasks from policy
	s.configureTasksFromPolicy()

	glog.V(1).Infof("Registered tasks: vacuum, erasure_coding, remote_upload, replication, balance, cluster_replication")
}

// configureTasksFromPolicy dynamically configures all registered tasks based on the maintenance policy
func (s *MaintenanceIntegration) configureTasksFromPolicy() {
	if s.maintenancePolicy == nil {
		return
	}

	// Configure all registered detectors and schedulers dynamically using policy configuration
	configuredCount := 0

	// Get all registered task types from the registry
	for taskType, detector := range s.taskRegistry.GetAllDetectors() {
		// Configure detector using policy-based configuration
		s.configureDetectorFromPolicy(taskType, detector)
		configuredCount++
	}

	for taskType, scheduler := range s.taskRegistry.GetAllSchedulers() {
		// Configure scheduler using policy-based configuration
		s.configureSchedulerFromPolicy(taskType, scheduler)
	}

	glog.V(1).Infof("Dynamically configured %d task types from maintenance policy", configuredCount)
}

// configureDetectorFromPolicy configures a detector using policy-based configuration
func (s *MaintenanceIntegration) configureDetectorFromPolicy(taskType types.TaskType, detector types.TaskDetector) {
	// Try to configure using PolicyConfigurableDetector interface if supported
	if configurableDetector, ok := detector.(types.PolicyConfigurableDetector); ok {
		configurableDetector.ConfigureFromPolicy(s.maintenancePolicy)
		glog.V(2).Infof("Configured detector %s using policy interface", taskType)
		return
	}

	// Fallback to specific configurations for existing detectors
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
	case types.TaskTypeBalance:
		if balanceDetector := balance.GetSimpleDetector(s.taskRegistry); balanceDetector != nil {
			balanceDetector.SetEnabled(s.maintenancePolicy.BalanceEnabled)
			balanceDetector.SetThreshold(s.maintenancePolicy.BalanceThreshold)
		}
	default:
		glog.V(2).Infof("No specific configuration for detector task type: %s", taskType)
	}
}

// configureSchedulerFromPolicy configures a scheduler using policy-based configuration
func (s *MaintenanceIntegration) configureSchedulerFromPolicy(taskType types.TaskType, scheduler types.TaskScheduler) {
	// Try to configure using PolicyConfigurableScheduler interface if supported
	if configurableScheduler, ok := scheduler.(types.PolicyConfigurableScheduler); ok {
		configurableScheduler.ConfigureFromPolicy(s.maintenancePolicy)
		glog.V(2).Infof("Configured scheduler %s using policy interface", taskType)
		return
	}

	// Fallback to specific configurations for existing schedulers
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
	case types.TaskTypeBalance:
		if balanceScheduler := balance.GetSimpleScheduler(s.taskRegistry); balanceScheduler != nil {
			balanceScheduler.SetEnabled(s.maintenancePolicy.BalanceEnabled)
			balanceScheduler.SetMaxConcurrent(s.maintenancePolicy.BalanceMaxConcurrent)
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

// convertToExistingFormat converts task results to existing system format using dynamic mapping
func (s *MaintenanceIntegration) convertToExistingFormat(result *types.TaskDetectionResult) *TaskDetectionResult {
	// Convert types using mapping tables
	existingType, exists := s.taskTypeMap[result.TaskType]
	if !exists {
		glog.Warningf("Unknown task type %s, defaulting to vacuum", result.TaskType)
		existingType = TaskTypeVacuum
	}

	existingPriority, exists := s.priorityMap[result.Priority]
	if !exists {
		glog.Warningf("Unknown priority %d, defaulting to normal", result.Priority)
		existingPriority = PriorityNormal
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

// CanScheduleWithTaskSchedulers determines if a task can be scheduled using task schedulers with dynamic type conversion
func (s *MaintenanceIntegration) CanScheduleWithTaskSchedulers(task *MaintenanceTask, runningTasks []*MaintenanceTask, availableWorkers []*MaintenanceWorker) bool {
	// Convert existing types to task types using mapping
	taskType, exists := s.revTaskTypeMap[task.Type]
	if !exists {
		glog.V(2).Infof("Unknown task type %s for scheduling, falling back to existing logic", task.Type)
		return false // Fallback to existing logic for unknown types
	}

	// Convert task objects
	taskObject := s.convertTaskToTaskSystem(task)
	runningTaskObjects := s.convertTasksToTaskSystem(runningTasks)
	workerObjects := s.convertWorkersToTaskSystem(availableWorkers)

	// Get the appropriate scheduler
	scheduler := s.taskRegistry.GetScheduler(taskType)
	if scheduler == nil {
		glog.V(2).Infof("No scheduler found for task type %s", taskType)
		return false
	}

	return scheduler.CanScheduleNow(taskObject, runningTaskObjects, workerObjects)
}

// convertTaskToTaskSystem converts existing task to task system format using dynamic mapping
func (s *MaintenanceIntegration) convertTaskToTaskSystem(task *MaintenanceTask) *types.Task {
	// Convert task type using mapping
	taskType, exists := s.revTaskTypeMap[task.Type]
	if !exists {
		glog.Warningf("Unknown task type %s in conversion, defaulting to vacuum", task.Type)
		taskType = types.TaskTypeVacuum
	}

	// Convert priority using mapping
	priority, exists := s.revPriorityMap[task.Priority]
	if !exists {
		glog.Warningf("Unknown priority %d in conversion, defaulting to normal", task.Priority)
		priority = types.TaskPriorityNormal
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

// convertWorkersToTaskSystem converts workers to task system format using dynamic mapping
func (s *MaintenanceIntegration) convertWorkersToTaskSystem(workers []*MaintenanceWorker) []*types.Worker {
	var result []*types.Worker
	for _, worker := range workers {
		capabilities := make([]types.TaskType, 0, len(worker.Capabilities))
		for _, cap := range worker.Capabilities {
			// Convert capability using mapping
			taskType, exists := s.revTaskTypeMap[cap]
			if exists {
				capabilities = append(capabilities, taskType)
			} else {
				glog.V(3).Infof("Unknown capability %s for worker %s, skipping", cap, worker.ID)
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

// GetUIProvider returns the UI provider for a task type using dynamic mapping
func (s *MaintenanceIntegration) GetUIProvider(taskType MaintenanceTaskType) types.TaskUIProvider {
	// Convert task type using mapping
	taskSystemType, exists := s.revTaskTypeMap[taskType]
	if !exists {
		glog.V(3).Infof("Unknown task type %s for UI provider", taskType)
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
