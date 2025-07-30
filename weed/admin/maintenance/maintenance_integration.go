package maintenance

import (
	"context"
	"fmt"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/admin/topology"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/operation"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/worker_pb"
	"github.com/seaweedfs/seaweedfs/weed/worker/tasks"
	"github.com/seaweedfs/seaweedfs/weed/worker/types"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
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

// UpdateTopologyInfo updates the volume shard tracker with topology information for empty servers
func (s *MaintenanceIntegration) UpdateTopologyInfo(topologyInfo *master_pb.TopologyInfo) error {
	return s.activeTopology.UpdateTopology(topologyInfo)
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

// GetActiveTopology returns the active topology for task detection
func (s *MaintenanceIntegration) GetActiveTopology() *topology.ActiveTopology {
	return s.activeTopology
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

	// Use ActiveTopology for destination planning
	destinationPlan, err := s.planDestinationWithActiveTopology(task, opType)

	if err != nil {
		glog.Warningf("Failed to plan primary destination for %s task volume %d: %v",
			task.TaskType, task.VolumeID, err)
		// Don't return here - still try to create task params which might work with multiple destinations
	}

	// Create typed protobuf parameters based on operation type
	switch opType {
	case OpTypeErasureCoding:
		if destinationPlan == nil {
			glog.Warningf("Cannot create EC task for volume %d: destination planning failed", task.VolumeID)
			return
		}
		s.createErasureCodingTaskParams(task, destinationPlan)
	case OpTypeVolumeMove, OpTypeVolumeBalance:
		if destinationPlan == nil {
			glog.Warningf("Cannot create balance task for volume %d: destination planning failed", task.VolumeID)
			return
		}
		s.createBalanceTaskParams(task, destinationPlan.(*topology.DestinationPlan))
	case OpTypeReplication:
		if destinationPlan == nil {
			glog.Warningf("Cannot create replication task for volume %d: destination planning failed", task.VolumeID)
			return
		}
		s.createReplicationTaskParams(task, destinationPlan.(*topology.DestinationPlan))
	default:
		glog.V(2).Infof("Unknown operation type for task %s: %v", task.TaskType, opType)
	}

	if destinationPlan != nil {
		switch plan := destinationPlan.(type) {
		case *topology.DestinationPlan:
			glog.V(1).Infof("Completed destination planning for %s task on volume %d: %s -> %s",
				task.TaskType, task.VolumeID, task.Server, plan.TargetNode)
		case *topology.MultiDestinationPlan:
			glog.V(1).Infof("Completed EC destination planning for volume %d: %s -> %d destinations (racks: %d, DCs: %d)",
				task.VolumeID, task.Server, len(plan.Plans), plan.SuccessfulRack, plan.SuccessfulDCs)
		}
	} else {
		glog.V(1).Infof("Completed destination planning for %s task on volume %d: no destination planned",
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

// planDestinationWithActiveTopology uses ActiveTopology to plan destinations
func (s *MaintenanceIntegration) planDestinationWithActiveTopology(task *TaskDetectionResult, opType PendingOperationType) (interface{}, error) {
	// Get source node information from topology
	var sourceRack, sourceDC string

	// Extract rack and DC from topology info
	topologyInfo := s.activeTopology.GetTopologyInfo()
	if topologyInfo != nil {
		for _, dc := range topologyInfo.DataCenterInfos {
			for _, rack := range dc.RackInfos {
				for _, dataNodeInfo := range rack.DataNodeInfos {
					if dataNodeInfo.Id == task.Server {
						sourceDC = dc.Id
						sourceRack = rack.Id
						break
					}
				}
				if sourceRack != "" {
					break
				}
			}
			if sourceDC != "" {
				break
			}
		}
	}

	switch opType {
	case OpTypeVolumeBalance, OpTypeVolumeMove:
		// Plan single destination for balance operation
		return s.activeTopology.PlanBalanceDestination(task.VolumeID, task.Server, sourceRack, sourceDC, 0)

	case OpTypeErasureCoding:
		// Plan multiple destinations for EC operation using adaptive shard counts
		// Start with the default configuration, but fall back to smaller configurations if insufficient disks
		totalShards := s.getOptimalECShardCount()
		multiPlan, err := s.activeTopology.PlanECDestinations(task.VolumeID, task.Server, sourceRack, sourceDC, totalShards)
		if err != nil {
			return nil, err
		}
		if multiPlan != nil && len(multiPlan.Plans) > 0 {
			// Return the multi-destination plan for EC
			return multiPlan, nil
		}
		return nil, fmt.Errorf("no EC destinations found")

	default:
		return nil, fmt.Errorf("unsupported operation type for destination planning: %v", opType)
	}
}

// createErasureCodingTaskParams creates typed parameters for EC tasks
func (s *MaintenanceIntegration) createErasureCodingTaskParams(task *TaskDetectionResult, destinationPlan interface{}) {
	// Determine EC shard counts based on the number of planned destinations
	multiPlan, ok := destinationPlan.(*topology.MultiDestinationPlan)
	if !ok {
		glog.Warningf("EC task for volume %d received unexpected destination plan type", task.VolumeID)
		task.TypedParams = nil
		return
	}

	// Use adaptive shard configuration based on actual planned destinations
	totalShards := len(multiPlan.Plans)
	dataShards, parityShards := s.getECShardCounts(totalShards)

	// Extract disk-aware destinations from the multi-destination plan
	var destinations []*worker_pb.ECDestination
	var allConflicts []string

	for _, plan := range multiPlan.Plans {
		allConflicts = append(allConflicts, plan.Conflicts...)

		// Create disk-aware destination
		destinations = append(destinations, &worker_pb.ECDestination{
			Node:           plan.TargetNode,
			DiskId:         plan.TargetDisk,
			Rack:           plan.TargetRack,
			DataCenter:     plan.TargetDC,
			PlacementScore: plan.PlacementScore,
		})
	}

	glog.V(1).Infof("EC destination planning for volume %d: got %d destinations (%d+%d shards) across %d racks and %d DCs",
		task.VolumeID, len(destinations), dataShards, parityShards, multiPlan.SuccessfulRack, multiPlan.SuccessfulDCs)

	if len(destinations) == 0 {
		glog.Warningf("No destinations available for EC task volume %d - rejecting task", task.VolumeID)
		task.TypedParams = nil
		return
	}

	// Collect existing EC shard locations for cleanup
	existingShardLocations := s.collectExistingEcShardLocations(task.VolumeID)

	// Create EC task parameters
	ecParams := &worker_pb.ErasureCodingTaskParams{
		Destinations:           destinations, // Disk-aware destinations
		DataShards:             dataShards,
		ParityShards:           parityShards,
		WorkingDir:             "/tmp/seaweedfs_ec_work",
		MasterClient:           "localhost:9333",
		CleanupSource:          true,
		ExistingShardLocations: existingShardLocations, // Pass existing shards for cleanup
	}

	// Add placement conflicts if any
	if len(allConflicts) > 0 {
		// Remove duplicates
		conflictMap := make(map[string]bool)
		var uniqueConflicts []string
		for _, conflict := range allConflicts {
			if !conflictMap[conflict] {
				conflictMap[conflict] = true
				uniqueConflicts = append(uniqueConflicts, conflict)
			}
		}
		ecParams.PlacementConflicts = uniqueConflicts
	}

	// Wrap in TaskParams
	task.TypedParams = &worker_pb.TaskParams{
		VolumeId:   task.VolumeID,
		Server:     task.Server,
		Collection: task.Collection,
		TaskParams: &worker_pb.TaskParams_ErasureCodingParams{
			ErasureCodingParams: ecParams,
		},
	}

	glog.V(1).Infof("Created EC task params with %d destinations for volume %d",
		len(destinations), task.VolumeID)
}

// createBalanceTaskParams creates typed parameters for balance/move tasks
func (s *MaintenanceIntegration) createBalanceTaskParams(task *TaskDetectionResult, destinationPlan *topology.DestinationPlan) {
	// balanceConfig could be used for future config options like ImbalanceThreshold, MinServerCount

	// Create balance task parameters
	balanceParams := &worker_pb.BalanceTaskParams{
		DestNode:       destinationPlan.TargetNode,
		EstimatedSize:  destinationPlan.ExpectedSize,
		DestRack:       destinationPlan.TargetRack,
		DestDc:         destinationPlan.TargetDC,
		PlacementScore: destinationPlan.PlacementScore,
		ForceMove:      false, // Default to false
		TimeoutSeconds: 300,   // Default 5 minutes
	}

	// Add placement conflicts if any
	if len(destinationPlan.Conflicts) > 0 {
		balanceParams.PlacementConflicts = destinationPlan.Conflicts
	}

	// Note: balanceConfig would have ImbalanceThreshold, MinServerCount if needed for future enhancements

	// Wrap in TaskParams
	task.TypedParams = &worker_pb.TaskParams{
		VolumeId:   task.VolumeID,
		Server:     task.Server,
		Collection: task.Collection,
		TaskParams: &worker_pb.TaskParams_BalanceParams{
			BalanceParams: balanceParams,
		},
	}

	glog.V(1).Infof("Created balance task params for volume %d: %s -> %s (score: %.2f)",
		task.VolumeID, task.Server, destinationPlan.TargetNode, destinationPlan.PlacementScore)
}

// createReplicationTaskParams creates typed parameters for replication tasks
func (s *MaintenanceIntegration) createReplicationTaskParams(task *TaskDetectionResult, destinationPlan *topology.DestinationPlan) {
	// replicationConfig could be used for future config options like TargetReplicaCount

	// Create replication task parameters
	replicationParams := &worker_pb.ReplicationTaskParams{
		DestNode:       destinationPlan.TargetNode,
		DestRack:       destinationPlan.TargetRack,
		DestDc:         destinationPlan.TargetDC,
		PlacementScore: destinationPlan.PlacementScore,
	}

	// Add placement conflicts if any
	if len(destinationPlan.Conflicts) > 0 {
		replicationParams.PlacementConflicts = destinationPlan.Conflicts
	}

	// Note: replicationConfig would have TargetReplicaCount if needed for future enhancements

	// Wrap in TaskParams
	task.TypedParams = &worker_pb.TaskParams{
		VolumeId:   task.VolumeID,
		Server:     task.Server,
		Collection: task.Collection,
		TaskParams: &worker_pb.TaskParams_ReplicationParams{
			ReplicationParams: replicationParams,
		},
	}

	glog.V(1).Infof("Created replication task params for volume %d: %s -> %s",
		task.VolumeID, task.Server, destinationPlan.TargetNode)
}

// getOptimalECShardCount returns the optimal number of EC shards based on available disks
// Uses a simplified approach to avoid blocking during UI access
func (s *MaintenanceIntegration) getOptimalECShardCount() int {
	// Try to get available disks quickly, but don't block if topology is busy
	availableDisks := s.getAvailableDisksQuickly()

	// EC configurations in order of preference: (data+parity=total)
	// Use smaller configurations for smaller clusters
	if availableDisks >= 14 {
		glog.V(1).Infof("Using default EC configuration: 10+4=14 shards for %d available disks", availableDisks)
		return 14 // Default: 10+4
	} else if availableDisks >= 6 {
		glog.V(1).Infof("Using small cluster EC configuration: 4+2=6 shards for %d available disks", availableDisks)
		return 6 // Small cluster: 4+2
	} else if availableDisks >= 4 {
		glog.V(1).Infof("Using minimal EC configuration: 3+1=4 shards for %d available disks", availableDisks)
		return 4 // Minimal: 3+1
	} else {
		glog.V(1).Infof("Using very small cluster EC configuration: 2+1=3 shards for %d available disks", availableDisks)
		return 3 // Very small: 2+1
	}
}

// getAvailableDisksQuickly returns available disk count with a fast path to avoid UI blocking
func (s *MaintenanceIntegration) getAvailableDisksQuickly() int {
	// Use ActiveTopology's optimized disk counting if available
	// Use empty task type and node filter for general availability check
	allDisks := s.activeTopology.GetAvailableDisks(topology.TaskTypeErasureCoding, "")
	if len(allDisks) > 0 {
		return len(allDisks)
	}

	// Fallback: try to count from topology but don't hold locks for too long
	topologyInfo := s.activeTopology.GetTopologyInfo()
	return s.countAvailableDisks(topologyInfo)
}

// countAvailableDisks counts the total number of available disks in the topology
func (s *MaintenanceIntegration) countAvailableDisks(topologyInfo *master_pb.TopologyInfo) int {
	if topologyInfo == nil {
		return 0
	}

	diskCount := 0
	for _, dc := range topologyInfo.DataCenterInfos {
		for _, rack := range dc.RackInfos {
			for _, node := range rack.DataNodeInfos {
				diskCount += len(node.DiskInfos)
			}
		}
	}

	return diskCount
}

// getECShardCounts determines data and parity shard counts for a given total
func (s *MaintenanceIntegration) getECShardCounts(totalShards int) (int32, int32) {
	// Map total shards to (data, parity) configurations
	switch totalShards {
	case 14:
		return 10, 4 // Default: 10+4
	case 9:
		return 6, 3 // Medium: 6+3
	case 6:
		return 4, 2 // Small: 4+2
	case 4:
		return 3, 1 // Minimal: 3+1
	case 3:
		return 2, 1 // Very small: 2+1
	default:
		// For any other total, try to maintain roughly 3:1 or 4:1 ratio
		if totalShards >= 4 {
			parityShards := totalShards / 4
			if parityShards < 1 {
				parityShards = 1
			}
			dataShards := totalShards - parityShards
			return int32(dataShards), int32(parityShards)
		}
		// Fallback for very small clusters
		return int32(totalShards - 1), 1
	}
}

// collectExistingEcShardLocations queries the master for existing EC shard locations during planning
func (s *MaintenanceIntegration) collectExistingEcShardLocations(volumeId uint32) []*worker_pb.ExistingECShardLocation {
	var existingShardLocations []*worker_pb.ExistingECShardLocation

	// Use insecure connection for simplicity - in production this might be configurable
	grpcDialOption := grpc.WithTransportCredentials(insecure.NewCredentials())

	err := operation.WithMasterServerClient(false, pb.ServerAddress("localhost:9333"), grpcDialOption,
		func(masterClient master_pb.SeaweedClient) error {
			req := &master_pb.LookupEcVolumeRequest{
				VolumeId: volumeId,
			}
			resp, err := masterClient.LookupEcVolume(context.Background(), req)
			if err != nil {
				// If volume doesn't exist as EC volume, that's fine - just no existing shards
				glog.V(1).Infof("LookupEcVolume for volume %d returned: %v (this is normal if no existing EC shards)", volumeId, err)
				return nil
			}

			// Group shard locations by server
			serverShardMap := make(map[string][]uint32)
			for _, shardIdLocation := range resp.ShardIdLocations {
				shardId := uint32(shardIdLocation.ShardId)
				for _, location := range shardIdLocation.Locations {
					serverAddr := pb.NewServerAddressFromLocation(location)
					serverShardMap[string(serverAddr)] = append(serverShardMap[string(serverAddr)], shardId)
				}
			}

			// Convert to protobuf format
			for serverAddr, shardIds := range serverShardMap {
				existingShardLocations = append(existingShardLocations, &worker_pb.ExistingECShardLocation{
					Node:     serverAddr,
					ShardIds: shardIds,
				})
			}

			return nil
		})

	if err != nil {
		glog.Errorf("Failed to lookup existing EC shards from master for volume %d: %v", volumeId, err)
		// Return empty list - cleanup will be skipped but task can continue
		return []*worker_pb.ExistingECShardLocation{}
	}

	if len(existingShardLocations) > 0 {
		glog.V(1).Infof("Found existing EC shards for volume %d on %d servers during planning", volumeId, len(existingShardLocations))
	}

	return existingShardLocations
}
