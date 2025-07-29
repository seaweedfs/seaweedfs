package maintenance

import (
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/worker_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage/erasure_coding"
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

	// Enhanced volume/shard tracker with destination planning
	volumeShardTracker *VolumeShardTracker

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

	// Initialize volume/shard tracker with pending operations integration
	integration.volumeShardTracker = NewVolumeShardTracker(integration.pendingOperations)

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
	// Update volume/shard tracker with latest master data
	if err := s.volumeShardTracker.UpdateFromMaster(volumeMetrics); err != nil {
		glog.Errorf("Failed to update volume/shard tracker: %v", err)
	}

	// Filter out volumes with pending operations to avoid duplicates
	filteredMetrics := s.pendingOperations.FilterVolumeMetricsExcludingPending(volumeMetrics)

	glog.V(1).Infof("Scanning %d volumes (filtered from %d) excluding pending operations",
		len(filteredMetrics), len(volumeMetrics))

	var allResults []*TaskDetectionResult

	// Create cluster info
	clusterInfo := &types.ClusterInfo{
		TotalVolumes: len(filteredMetrics),
		LastUpdated:  time.Now(),
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
					// Plan destination for operations that need it
					s.planDestinationForTask(existingResult, opType)
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
		glog.Warningf("Unknown priority %d, defaulting to normal", result.Priority)
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
	glog.Infof("DEBUG CanScheduleWithTaskSchedulers: Checking task %s (type: %s)", task.ID, task.Type)

	// Convert existing types to task types using mapping
	taskType, exists := s.revTaskTypeMap[task.Type]
	if !exists {
		glog.Infof("DEBUG CanScheduleWithTaskSchedulers: Unknown task type %s for scheduling, falling back to existing logic", task.Type)
		return false // Fallback to existing logic for unknown types
	}

	glog.Infof("DEBUG CanScheduleWithTaskSchedulers: Mapped task type %s to %s", task.Type, taskType)

	// Convert task objects
	taskObject := s.convertTaskToTaskSystem(task)
	if taskObject == nil {
		glog.Infof("DEBUG CanScheduleWithTaskSchedulers: Failed to convert task %s for scheduling", task.ID)
		return false
	}

	glog.Infof("DEBUG CanScheduleWithTaskSchedulers: Successfully converted task %s", task.ID)

	runningTaskObjects := s.convertTasksToTaskSystem(runningTasks)
	workerObjects := s.convertWorkersToTaskSystem(availableWorkers)

	glog.Infof("DEBUG CanScheduleWithTaskSchedulers: Converted %d running tasks and %d workers", len(runningTaskObjects), len(workerObjects))

	// Get the appropriate scheduler
	scheduler := s.taskRegistry.GetScheduler(taskType)
	if scheduler == nil {
		glog.Infof("DEBUG CanScheduleWithTaskSchedulers: No scheduler found for task type %s", taskType)
		return false
	}

	glog.Infof("DEBUG CanScheduleWithTaskSchedulers: Found scheduler for task type %s", taskType)

	canSchedule := scheduler.CanScheduleNow(taskObject, runningTaskObjects, workerObjects)
	glog.Infof("DEBUG CanScheduleWithTaskSchedulers: Scheduler decision for task %s: %v", task.ID, canSchedule)

	return canSchedule
}

// convertTaskToTaskSystem converts existing task to task system format using dynamic mapping
func (s *MaintenanceIntegration) convertTaskToTaskSystem(task *MaintenanceTask) *types.Task {
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

	return &types.Task{
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
func (s *MaintenanceIntegration) convertTasksToTaskSystem(tasks []*MaintenanceTask) []*types.Task {
	var result []*types.Task
	for _, task := range tasks {
		converted := s.convertTaskToTaskSystem(task)
		if converted != nil {
			result = append(result, converted)
		}
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

// GetVolumeShardTracker returns the volume/shard tracker
func (s *MaintenanceIntegration) GetVolumeShardTracker() *VolumeShardTracker {
	return s.volumeShardTracker
}

// planDestinationForTask plans the destination for a task that requires it and creates typed protobuf parameters
func (s *MaintenanceIntegration) planDestinationForTask(task *TaskDetectionResult, opType PendingOperationType) {
	// Only plan destinations for operations that move volumes/shards
	if opType == OpTypeVacuum {
		// For vacuum tasks, create VacuumTaskParams
		s.createVacuumTaskParams(task)
		return
	}

	glog.V(1).Infof("Planning destination for %s task on volume %d (server: %s)", task.TaskType, task.VolumeID, task.Server)

	// Plan destination using the volume/shard tracker
	destinationPlan, err := s.volumeShardTracker.PlanDestinationForVolume(
		task.VolumeID,
		opType,
		task.Server,
	)

	if err != nil {
		glog.Warningf("Failed to plan primary destination for %s task volume %d: %v",
			task.TaskType, task.VolumeID, err)
		// Don't return here - still try to create task params which might work with multiple destinations
	}

	// Create typed protobuf parameters based on operation type
	switch opType {
	case OpTypeErasureCoding:
		if destinationPlan == nil {
			// Create empty destination plan for EC tasks when primary planning fails
			destinationPlan = &DestinationPlan{
				TargetNode: "",
				Conflicts:  []string{"primary_destination_planning_failed"},
			}
		}
		s.createErasureCodingTaskParams(task, destinationPlan)
	case OpTypeVolumeMove, OpTypeVolumeBalance:
		if destinationPlan == nil {
			glog.Warningf("Cannot create balance task for volume %d: destination planning failed", task.VolumeID)
			return
		}
		s.createBalanceTaskParams(task, destinationPlan)
	case OpTypeReplication:
		if destinationPlan == nil {
			glog.Warningf("Cannot create replication task for volume %d: destination planning failed", task.VolumeID)
			return
		}
		s.createReplicationTaskParams(task, destinationPlan)
	default:
		glog.V(2).Infof("Unknown operation type for task %s: %v", task.TaskType, opType)
	}

	if destinationPlan != nil && destinationPlan.TargetNode != "" {
		glog.V(1).Infof("Completed destination planning for %s task on volume %d: %s -> %s",
			task.TaskType, task.VolumeID, task.Server, destinationPlan.TargetNode)
	} else {
		glog.V(1).Infof("Completed destination planning for %s task on volume %d: no primary destination planned",
			task.TaskType, task.VolumeID)
	}
}

// createVacuumTaskParams creates typed parameters for vacuum tasks
func (s *MaintenanceIntegration) createVacuumTaskParams(task *TaskDetectionResult) {
	// Get configuration from policy instead of using hard-coded values
	vacuumConfig := GetVacuumTaskConfig(s.maintenancePolicy, MaintenanceTaskType("vacuum"))

	// Use configured values or defaults if config is not available
	garbageThreshold := 0.3                    // Default 30%
	verifyChecksum := true                     // Default to verify
	batchSize := int32(1000)                   // Default batch size
	workingDir := "/tmp/seaweedfs_vacuum_work" // Default working directory

	if vacuumConfig != nil {
		garbageThreshold = vacuumConfig.GarbageThreshold
		// Note: VacuumTaskConfig has GarbageThreshold, MinVolumeAgeHours, MinIntervalSeconds
		// Other fields like VerifyChecksum, BatchSize, WorkingDir would need to be added
		// to the protobuf definition if they should be configurable
	}

	// Create typed protobuf parameters
	task.TypedParams = &worker_pb.TaskParams{
		VolumeId:   task.VolumeID,
		Server:     task.Server,
		Collection: task.Collection,
		TaskParams: &worker_pb.TaskParams_VacuumParams{
			VacuumParams: &worker_pb.VacuumTaskParams{
				GarbageThreshold: garbageThreshold,
				ForceVacuum:      false,
				BatchSize:        batchSize,
				WorkingDir:       workingDir,
				VerifyChecksum:   verifyChecksum,
			},
		},
	}
}

// createErasureCodingTaskParams creates typed parameters for EC tasks
func (s *MaintenanceIntegration) createErasureCodingTaskParams(task *TaskDetectionResult, destinationPlan *DestinationPlan) {
	// Get configuration from policy instead of using hard-coded values
	ecConfig := GetErasureCodingTaskConfig(s.maintenancePolicy, MaintenanceTaskType("erasure_coding"))

	// Get multiple destinations for EC operations
	multipleNodes := s.planMultipleECDestinations(task.VolumeID, task.Server)

	// Set primary destination from the plan
	primaryDestNode := destinationPlan.TargetNode

	// Use multiple nodes if available, otherwise use primary destination
	destNodes := multipleNodes
	if len(destNodes) == 0 && primaryDestNode != "" {
		destNodes = []string{primaryDestNode}
		glog.V(1).Infof("Using primary destination node for EC task volume %d: %s", task.VolumeID, primaryDestNode)
	}

	// CRITICAL: If no destinations available, reject the task
	if len(destNodes) == 0 {
		glog.Warningf("No destinations available for EC task volume %d - rejecting task (insufficient capacity or nodes)", task.VolumeID)
		// Don't create TypedParams - this will cause the task to be rejected
		task.TypedParams = nil
		return
	}

	// EC parameters are constants from erasure_coding package
	dataShards := int32(erasure_coding.DataShardsCount)
	parityShards := int32(erasure_coding.ParityShardsCount)

	// Use configured values or defaults if config is not available
	workingDir := "/tmp/seaweedfs_ec_work"
	masterClient := "localhost:9333" // Could be configurable
	cleanupSource := true

	if ecConfig != nil {
		// Note: ErasureCodingTaskConfig has FullnessRatio, QuietForSeconds, MinVolumeSizeMb, CollectionFilter
		// DataShards and ParityShards are fixed constants from erasure_coding package
		// Other fields like WorkingDir, MasterClient could be configurable if added to protobuf
	}

	// Create typed protobuf parameters
	task.TypedParams = &worker_pb.TaskParams{
		VolumeId:   task.VolumeID,
		Server:     task.Server,
		Collection: task.Collection,
		TaskParams: &worker_pb.TaskParams_ErasureCodingParams{
			ErasureCodingParams: &worker_pb.ErasureCodingTaskParams{
				DestNodes:          destNodes,
				PrimaryDestNode:    primaryDestNode,
				EstimatedShardSize: destinationPlan.ExpectedSize,
				DataShards:         dataShards,
				ParityShards:       parityShards,
				WorkingDir:         workingDir,
				MasterClient:       masterClient,
				CleanupSource:      cleanupSource,
				PlacementConflicts: destinationPlan.Conflicts,
			},
		},
	}

	glog.V(1).Infof("Created EC task params with %d destinations for volume %d (primary: %s, multiple: %v)",
		len(destNodes), task.VolumeID, primaryDestNode, multipleNodes)
}

// createBalanceTaskParams creates typed parameters for balance/move tasks
func (s *MaintenanceIntegration) createBalanceTaskParams(task *TaskDetectionResult, destinationPlan *DestinationPlan) {
	// Get configuration from policy instead of using hard-coded values
	balanceConfig := GetBalanceTaskConfig(s.maintenancePolicy, MaintenanceTaskType("balance"))

	// Use configured values or defaults if config is not available
	forceMove := false            // Default to safe mode
	timeoutSeconds := int32(3600) // 1 hour default timeout

	if balanceConfig != nil {
		// Note: BalanceTaskConfig has ImbalanceThreshold, MinServerCount
		// Other fields like ForceMove, TimeoutSeconds would need to be added
		// to the protobuf definition if they should be configurable
		// For now we use the existing fields and keep defaults for others
	}

	// Create typed protobuf parameters
	task.TypedParams = &worker_pb.TaskParams{
		VolumeId:   task.VolumeID,
		Server:     task.Server,
		Collection: task.Collection,
		TaskParams: &worker_pb.TaskParams_BalanceParams{
			BalanceParams: &worker_pb.BalanceTaskParams{
				DestNode:           destinationPlan.TargetNode,
				EstimatedSize:      destinationPlan.ExpectedSize,
				DestRack:           destinationPlan.TargetRack,
				DestDc:             destinationPlan.TargetDC,
				PlacementScore:     destinationPlan.PlacementScore,
				PlacementConflicts: destinationPlan.Conflicts,
				ForceMove:          forceMove,
				TimeoutSeconds:     timeoutSeconds,
			},
		},
	}
}

// createReplicationTaskParams creates typed parameters for replication tasks
func (s *MaintenanceIntegration) createReplicationTaskParams(task *TaskDetectionResult, destinationPlan *DestinationPlan) {
	// Get configuration from policy instead of using hard-coded values
	replicationConfig := GetReplicationTaskConfig(s.maintenancePolicy, MaintenanceTaskType("replication"))

	// Use configured values or defaults if config is not available
	replicaCount := int32(2)  // Default
	verifyConsistency := true // Default to verify consistency

	if replicationConfig != nil {
		replicaCount = replicationConfig.TargetReplicaCount
		// Note: ReplicationTaskConfig has TargetReplicaCount
		// Other fields like VerifyConsistency would need to be added
		// to the protobuf definition if they should be configurable
	}

	// Create typed protobuf parameters
	task.TypedParams = &worker_pb.TaskParams{
		VolumeId:   task.VolumeID,
		Server:     task.Server,
		Collection: task.Collection,
		TaskParams: &worker_pb.TaskParams_ReplicationParams{
			ReplicationParams: &worker_pb.ReplicationTaskParams{
				DestNode:           destinationPlan.TargetNode,
				EstimatedSize:      destinationPlan.ExpectedSize,
				DestRack:           destinationPlan.TargetRack,
				DestDc:             destinationPlan.TargetDC,
				PlacementScore:     destinationPlan.PlacementScore,
				PlacementConflicts: destinationPlan.Conflicts,
				ReplicaCount:       replicaCount,
				VerifyConsistency:  verifyConsistency,
			},
		},
	}
}

// planMultipleECDestinations plans multiple destinations for EC shard distribution
func (s *MaintenanceIntegration) planMultipleECDestinations(volumeID uint32, sourceNode string) []string {
	// Get available nodes from volume/shard tracker
	clusterStats := s.volumeShardTracker.getClusterStats()
	totalShards := erasure_coding.TotalShardsCount
	if clusterStats.TotalNodes < totalShards {
		glog.Warningf("EC destination planning failed for volume %d: insufficient cluster nodes (%d < %d required)",
			volumeID, clusterStats.TotalNodes, totalShards)
		return []string{}
	}

	// Get volume size - skip if unknown
	volumeReplicas, err := s.volumeShardTracker.getVolumeInfo(volumeID)
	if err != nil || len(volumeReplicas) == 0 {
		glog.Warningf("EC destination planning failed for volume %d: volume info not available - %v", volumeID, err)
		return []string{}
	}

	volumeSize := volumeReplicas[0].Size
	if volumeSize == 0 {
		glog.Warningf("EC destination planning failed for volume %d: volume has zero size", volumeID)
		return []string{}
	}

	// Calculate estimated shard size based on EC constants
	estimatedShardSize := volumeSize / uint64(erasure_coding.DataShardsCount)
	glog.V(1).Infof("EC destination planning for volume %d: volume_size=%d bytes, estimated_shard_size=%d bytes",
		volumeID, volumeSize, estimatedShardSize)

	// Get all nodes with sufficient capacity
	var availableNodes []string
	var insufficientNodes []string
	var excludedSource bool

	for nodeID, nodeInfo := range s.volumeShardTracker.nodes {
		if nodeID == sourceNode {
			excludedSource = true
			glog.V(2).Infof("EC destination planning for volume %d: excluding source node %s", volumeID, nodeID)
			continue // Skip source node
		}

		if nodeInfo.FreeCapacity >= estimatedShardSize {
			availableNodes = append(availableNodes, nodeID)
			glog.V(2).Infof("EC destination planning for volume %d: node %s has sufficient capacity (free: %d bytes)",
				volumeID, nodeID, nodeInfo.FreeCapacity)
		} else {
			insufficientNodes = append(insufficientNodes, nodeID)
			glog.V(2).Infof("EC destination planning for volume %d: node %s has insufficient capacity (free: %d bytes, need: %d bytes)",
				volumeID, nodeID, nodeInfo.FreeCapacity, estimatedShardSize)
		}
	}

	if !excludedSource {
		glog.V(1).Infof("EC destination planning for volume %d: source node %s not found in cluster", volumeID, sourceNode)
	}

	if len(availableNodes) == 0 {
		glog.Warningf("EC destination planning failed for volume %d: no nodes have sufficient capacity (%d bytes) for EC shards. "+
			"Cluster status: total_nodes=%d, insufficient_nodes=%v, excluded_source=%s",
			volumeID, estimatedShardSize, clusterStats.TotalNodes, insufficientNodes, sourceNode)
		return []string{}
	}

	// Return up to totalShards nodes for EC distribution
	maxNodes := totalShards
	if len(availableNodes) < maxNodes {
		maxNodes = len(availableNodes)
	}

	selectedNodes := availableNodes[:maxNodes]
	glog.Infof("EC destination planning succeeded for volume %d: selected %d nodes (shard_size: %d bytes, volume_size: %d bytes): %v",
		volumeID, len(selectedNodes), estimatedShardSize, volumeSize, selectedNodes)

	return selectedNodes
}
