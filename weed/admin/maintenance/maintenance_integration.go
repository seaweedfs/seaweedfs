package maintenance

import (
	"time"

	"github.com/seaweedfs/seaweedfs/weed/admin/topology"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/worker/tasks"
	"github.com/seaweedfs/seaweedfs/weed/worker/types"
)

// MaintenanceIntegration bridges the task system with existing maintenance
type MaintenanceIntegration struct {
	taskRegistry *types.TaskRegistry
	uiRegistry   *types.UIRegistry

	// Bridge to existing system
	maintenanceQueue  *MaintenanceQueue
	maintenancePolicy *MaintenancePolicy

	// Pending operations tracker
	pendingOperations *PendingOperations

	// Active topology for task detection and target selection
	activeTopology *topology.ActiveTopology

	// Type conversion maps
	taskTypeMap    map[types.TaskType]MaintenanceTaskType
	revTaskTypeMap map[MaintenanceTaskType]types.TaskType
	priorityMap    map[types.TaskPriority]MaintenanceTaskPriority
	revPriorityMap map[MaintenanceTaskPriority]types.TaskPriority
}

// NewMaintenanceIntegration creates the integration bridge
func NewMaintenanceIntegration(queue *MaintenanceQueue, policy *MaintenancePolicy) *MaintenanceIntegration {
	integration := &MaintenanceIntegration{
		taskRegistry:      tasks.GetGlobalTypesRegistry(), // Use global types registry with auto-registered tasks
		uiRegistry:        tasks.GetGlobalUIRegistry(),    // Use global UI registry with auto-registered UI providers
		maintenanceQueue:  queue,
		maintenancePolicy: policy,
		pendingOperations: NewPendingOperations(),
	}

	// Initialize active topology with 10 second recent task window
	integration.activeTopology = topology.NewActiveTopology(10)

	// Initialize type conversion maps
	integration.initializeTypeMaps()

	// Register all tasks
	integration.registerAllTasks()

	return integration
}

// initializeTypeMaps creates the type conversion maps for dynamic conversion
func (s *MaintenanceIntegration) initializeTypeMaps() {
	// Initialize empty maps
	s.taskTypeMap = make(map[types.TaskType]MaintenanceTaskType)
	s.revTaskTypeMap = make(map[MaintenanceTaskType]types.TaskType)

	// Build task type mappings dynamically from registered tasks after registration
	// This will be called from registerAllTasks() after all tasks are registered

	// Priority mappings (these are static and don't depend on registered tasks)
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

// buildTaskTypeMappings dynamically builds task type mappings from registered tasks
func (s *MaintenanceIntegration) buildTaskTypeMappings() {
	// Clear existing mappings
	s.taskTypeMap = make(map[types.TaskType]MaintenanceTaskType)
	s.revTaskTypeMap = make(map[MaintenanceTaskType]types.TaskType)

	// Build mappings from registered detectors
	for workerTaskType := range s.taskRegistry.GetAllDetectors() {
		// Convert types.TaskType to MaintenanceTaskType by string conversion
		maintenanceTaskType := MaintenanceTaskType(string(workerTaskType))

		s.taskTypeMap[workerTaskType] = maintenanceTaskType
		s.revTaskTypeMap[maintenanceTaskType] = workerTaskType

		glog.V(3).Infof("Dynamically mapped task type: %s <-> %s", workerTaskType, maintenanceTaskType)
	}

	glog.V(2).Infof("Built %d dynamic task type mappings", len(s.taskTypeMap))
}

// registerAllTasks registers all available tasks
func (s *MaintenanceIntegration) registerAllTasks() {
	// Tasks are already auto-registered via import statements
	// No manual registration needed

	// Build dynamic type mappings from registered tasks
	s.buildTaskTypeMappings()

	// Configure tasks from policy
	s.ConfigureTasksFromPolicy()

	registeredTaskTypes := make([]string, 0, len(s.taskTypeMap))
	for _, maintenanceTaskType := range s.taskTypeMap {
		registeredTaskTypes = append(registeredTaskTypes, string(maintenanceTaskType))
	}
	glog.V(1).Infof("Registered tasks: %v", registeredTaskTypes)
}

// ConfigureTasksFromPolicy dynamically configures all registered tasks based on the maintenance policy
func (s *MaintenanceIntegration) ConfigureTasksFromPolicy() {
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

	// Apply basic configuration that all detectors should support
	if basicDetector, ok := detector.(interface{ SetEnabled(bool) }); ok {
		// Convert task system type to maintenance task type for policy lookup
		maintenanceTaskType, exists := s.taskTypeMap[taskType]
		if exists {
			enabled := IsTaskEnabled(s.maintenancePolicy, maintenanceTaskType)
			basicDetector.SetEnabled(enabled)
			glog.V(3).Infof("Set enabled=%v for detector %s", enabled, taskType)
		}
	}

	// For detectors that don't implement PolicyConfigurableDetector interface,
	// they should be updated to implement it for full policy-based configuration
	glog.V(2).Infof("Detector %s should implement PolicyConfigurableDetector interface for full policy support", taskType)
}

// configureSchedulerFromPolicy configures a scheduler using policy-based configuration
func (s *MaintenanceIntegration) configureSchedulerFromPolicy(taskType types.TaskType, scheduler types.TaskScheduler) {
	// Try to configure using PolicyConfigurableScheduler interface if supported
	if configurableScheduler, ok := scheduler.(types.PolicyConfigurableScheduler); ok {
		configurableScheduler.ConfigureFromPolicy(s.maintenancePolicy)
		glog.V(2).Infof("Configured scheduler %s using policy interface", taskType)
		return
	}

	// Apply basic configuration that all schedulers should support
	maintenanceTaskType, exists := s.taskTypeMap[taskType]
	if !exists {
		glog.V(3).Infof("No maintenance task type mapping for %s, skipping configuration", taskType)
		return
	}

	// Set enabled status if scheduler supports it
	if enableableScheduler, ok := scheduler.(interface{ SetEnabled(bool) }); ok {
		enabled := IsTaskEnabled(s.maintenancePolicy, maintenanceTaskType)
		enableableScheduler.SetEnabled(enabled)
		glog.V(3).Infof("Set enabled=%v for scheduler %s", enabled, taskType)
	}

	// Set max concurrent if scheduler supports it
	if concurrentScheduler, ok := scheduler.(interface{ SetMaxConcurrent(int) }); ok {
		maxConcurrent := GetMaxConcurrent(s.maintenancePolicy, maintenanceTaskType)
		if maxConcurrent > 0 {
			concurrentScheduler.SetMaxConcurrent(maxConcurrent)
			glog.V(3).Infof("Set max concurrent=%d for scheduler %s", maxConcurrent, taskType)
		}
	}

	// For schedulers that don't implement PolicyConfigurableScheduler interface,
	// they should be updated to implement it for full policy-based configuration
	glog.V(2).Infof("Scheduler %s should implement PolicyConfigurableScheduler interface for full policy support", taskType)
}

// ScanWithTaskDetectors performs a scan using the task system
func (s *MaintenanceIntegration) ScanWithTaskDetectors(volumeMetrics []*types.VolumeHealthMetrics) ([]*TaskDetectionResult, error) {
	// Note: ActiveTopology gets updated from topology info instead of volume metrics
	glog.V(2).Infof("Processed %d volume metrics for task detection", len(volumeMetrics))

	// Filter out volumes with pending operations to avoid duplicates
	filteredMetrics := s.pendingOperations.FilterVolumeMetricsExcludingPending(volumeMetrics)

	glog.V(1).Infof("Scanning %d volumes (filtered from %d) excluding pending operations",
		len(filteredMetrics), len(volumeMetrics))

	var allResults []*TaskDetectionResult

	// Create cluster info
	clusterInfo := &types.ClusterInfo{
		TotalVolumes:   len(filteredMetrics),
		LastUpdated:    time.Now(),
		ActiveTopology: s.activeTopology, // Provide ActiveTopology for destination planning
	}

	// Run detection for each registered task type
	for taskType, detector := range s.taskRegistry.GetAllDetectors() {
		if !detector.IsEnabled() {
			continue
		}

		glog.V(2).Infof("Running detection for task type: %s", taskType)

		results, err := detector.ScanForTasks(filteredMetrics, clusterInfo)
		if err != nil {
			glog.Errorf("Failed to scan for %s tasks: %v", taskType, err)
			continue
		}

		// Convert results to existing system format and check for conflicts
		for _, result := range results {
			existingResult := s.convertToExistingFormat(result)
			if existingResult != nil {
				// Double-check for conflicts with pending operations
				opType := s.mapMaintenanceTaskTypeToPendingOperationType(existingResult.TaskType)
				if !s.pendingOperations.WouldConflictWithPending(existingResult.VolumeID, opType) {
					// All task types should now have TypedParams populated during detection phase
					if existingResult.TypedParams == nil {
						glog.Warningf("Task %s for volume %d has no typed parameters - skipping (task parameter creation may have failed)",
							existingResult.TaskType, existingResult.VolumeID)
						continue
					}
					allResults = append(allResults, existingResult)
				} else {
					glog.V(2).Infof("Skipping task %s for volume %d due to conflict with pending operation",
						existingResult.TaskType, existingResult.VolumeID)
				}
			}
		}

		glog.V(2).Infof("Found %d %s tasks", len(results), taskType)
	}

	return allResults, nil
}

// UpdateTopologyInfo updates the volume shard tracker with topology information for empty servers
func (s *MaintenanceIntegration) UpdateTopologyInfo(topologyInfo *master_pb.TopologyInfo) error {
	// Log topology details before update for diagnostics
	if topologyInfo != nil {
		dcCount, nodeCount, diskCount := topology.CountTopologyResources(topologyInfo)
		glog.V(2).Infof("UpdateTopologyInfo: received topology with %d datacenters, %d nodes, %d disks",
			dcCount, nodeCount, diskCount)
	} else {
		glog.Warningf("UpdateTopologyInfo: received nil topologyInfo")
	}

	err := s.activeTopology.UpdateTopology(topologyInfo)

	if err != nil {
		glog.Errorf("UpdateTopologyInfo: topology update failed: %v", err)
	} else {
		// Log success with current disk count
		currentDiskCount := s.activeTopology.GetDiskCount()
		glog.V(1).Infof("UpdateTopologyInfo: topology update successful, active topology now has %d disks", currentDiskCount)
	}

	return err
}

// convertToExistingFormat converts task results to existing system format using dynamic mapping
func (s *MaintenanceIntegration) convertToExistingFormat(result *types.TaskDetectionResult) *TaskDetectionResult {
	// Convert types using mapping tables
	existingType, exists := s.taskTypeMap[result.TaskType]
	if !exists {
		glog.Warningf("Unknown task type %s, skipping conversion", result.TaskType)
		// Return nil to indicate conversion failed - caller should handle this
		return nil
	}

	existingPriority, exists := s.priorityMap[result.Priority]
	if !exists {
		glog.Warningf("Unknown priority %s, defaulting to normal", result.Priority)
		existingPriority = PriorityNormal
	}

	return &TaskDetectionResult{
		TaskType:    existingType,
		VolumeID:    result.VolumeID,
		Server:      result.Server,
		Collection:  result.Collection,
		Priority:    existingPriority,
		Reason:      result.Reason,
		TypedParams: result.TypedParams,
		ScheduleAt:  result.ScheduleAt,
	}
}

// CanScheduleWithTaskSchedulers determines if a task can be scheduled using task schedulers with dynamic type conversion
func (s *MaintenanceIntegration) CanScheduleWithTaskSchedulers(task *MaintenanceTask, runningTasks []*MaintenanceTask, availableWorkers []*MaintenanceWorker) bool {

	// Convert existing types to task types using mapping
	taskType, exists := s.revTaskTypeMap[task.Type]
	if !exists {
		return false // Fallback to existing logic for unknown types
	}

	// Convert task objects
	taskObject := s.convertTaskToTaskSystem(task)
	if taskObject == nil {
		return false
	}

	runningTaskObjects := s.convertTasksToTaskSystem(runningTasks)
	workerObjects := s.convertWorkersToTaskSystem(availableWorkers)

	// Get the appropriate scheduler
	scheduler := s.taskRegistry.GetScheduler(taskType)
	if scheduler == nil {
		return false
	}

	canSchedule := scheduler.CanScheduleNow(taskObject, runningTaskObjects, workerObjects)

	return canSchedule
}

// convertTaskToTaskSystem converts existing task to task system format using dynamic mapping
func (s *MaintenanceIntegration) convertTaskToTaskSystem(task *MaintenanceTask) *types.TaskInput {
	// Convert task type using mapping
	taskType, exists := s.revTaskTypeMap[task.Type]
	if !exists {
		glog.Errorf("Unknown task type %s in conversion, cannot convert task", task.Type)
		// Return nil to indicate conversion failed
		return nil
	}

	// Convert priority using mapping
	priority, exists := s.revPriorityMap[task.Priority]
	if !exists {
		glog.Warningf("Unknown priority %d in conversion, defaulting to normal", task.Priority)
		priority = types.TaskPriorityNormal
	}

	return &types.TaskInput{
		ID:          task.ID,
		Type:        taskType,
		Priority:    priority,
		VolumeID:    task.VolumeID,
		Server:      task.Server,
		Collection:  task.Collection,
		TypedParams: task.TypedParams,
		CreatedAt:   task.CreatedAt,
	}
}

// convertTasksToTaskSystem converts multiple tasks
func (s *MaintenanceIntegration) convertTasksToTaskSystem(tasks []*MaintenanceTask) []*types.TaskInput {
	var result []*types.TaskInput
	for _, task := range tasks {
		converted := s.convertTaskToTaskSystem(task)
		if converted != nil {
			result = append(result, converted)
		}
	}
	return result
}

// convertWorkersToTaskSystem converts workers to task system format using dynamic mapping
func (s *MaintenanceIntegration) convertWorkersToTaskSystem(workers []*MaintenanceWorker) []*types.WorkerData {
	var result []*types.WorkerData
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

		result = append(result, &types.WorkerData{
			ID:            worker.ID,
			Address:       worker.Address,
			Capabilities:  capabilities,
			MaxConcurrent: worker.MaxConcurrent,
			CurrentLoad:   worker.CurrentLoad,
		})
	}
	return result
}

// GetTaskScheduler returns the scheduler for a task type using dynamic mapping
func (s *MaintenanceIntegration) GetTaskScheduler(taskType MaintenanceTaskType) types.TaskScheduler {
	// Convert task type using mapping
	taskSystemType, exists := s.revTaskTypeMap[taskType]
	if !exists {
		glog.V(3).Infof("Unknown task type %s for scheduler", taskType)
		return nil
	}

	return s.taskRegistry.GetScheduler(taskSystemType)
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

// mapMaintenanceTaskTypeToPendingOperationType converts a maintenance task type to a pending operation type
func (s *MaintenanceIntegration) mapMaintenanceTaskTypeToPendingOperationType(taskType MaintenanceTaskType) PendingOperationType {
	switch taskType {
	case MaintenanceTaskType("balance"):
		return OpTypeVolumeBalance
	case MaintenanceTaskType("erasure_coding"):
		return OpTypeErasureCoding
	case MaintenanceTaskType("vacuum"):
		return OpTypeVacuum
	case MaintenanceTaskType("replication"):
		return OpTypeReplication
	default:
		// For other task types, assume they're volume operations
		return OpTypeVolumeMove
	}
}

// GetPendingOperations returns the pending operations tracker
func (s *MaintenanceIntegration) GetPendingOperations() *PendingOperations {
	return s.pendingOperations
}

// GetActiveTopology returns the active topology for task detection
func (s *MaintenanceIntegration) GetActiveTopology() *topology.ActiveTopology {
	return s.activeTopology
}
