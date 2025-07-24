package task

import (
	"fmt"
	"math/rand"
	"sync"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/wdclient"
	"github.com/seaweedfs/seaweedfs/weed/worker/types"
)

// AdminServer manages the distributed task system
type AdminServer struct {
	config             *AdminConfig
	masterClient       *wdclient.MasterClient
	taskDiscovery      *TaskDiscoveryEngine
	workerRegistry     *WorkerRegistry
	taskScheduler      *TaskScheduler
	volumeStateManager *VolumeStateManager // Enhanced state management
	failureHandler     *FailureHandler
	inProgressTasks    map[string]*InProgressTask
	taskQueue          *PriorityTaskQueue
	running            bool
	stopChan           chan struct{}
	mutex              sync.RWMutex
}

// AdminConfig holds configuration for the admin server
type AdminConfig struct {
	ScanInterval          time.Duration
	WorkerTimeout         time.Duration
	TaskTimeout           time.Duration
	MaxRetries            int
	ReconcileInterval     time.Duration
	EnableFailureRecovery bool
	MaxConcurrentTasks    int
}

// NewAdminServer creates a new admin server instance
func NewAdminServer(config *AdminConfig, masterClient *wdclient.MasterClient) *AdminServer {
	if config == nil {
		config = DefaultAdminConfig()
	}

	return &AdminServer{
		config:             config,
		masterClient:       masterClient,
		volumeStateManager: NewVolumeStateManager(masterClient), // Initialize comprehensive state manager
		inProgressTasks:    make(map[string]*InProgressTask),
		taskQueue:          NewPriorityTaskQueue(),
		stopChan:           make(chan struct{}),
	}
}

// Start starts the admin server
func (as *AdminServer) Start() error {
	as.mutex.Lock()
	defer as.mutex.Unlock()

	if as.running {
		return fmt.Errorf("admin server is already running")
	}

	// Initialize components
	as.taskDiscovery = NewTaskDiscoveryEngine(as.masterClient, as.config.ScanInterval)
	as.workerRegistry = NewWorkerRegistry()
	as.taskScheduler = NewTaskScheduler(as.workerRegistry, as.taskQueue)
	as.failureHandler = NewFailureHandler(as.config)

	as.running = true

	// Start background goroutines
	go as.discoveryLoop()
	go as.schedulingLoop()
	go as.monitoringLoop()
	go as.reconciliationLoop()

	if as.config.EnableFailureRecovery {
		go as.failureRecoveryLoop()
	}

	glog.Infof("Admin server started")
	return nil
}

// Stop stops the admin server
func (as *AdminServer) Stop() error {
	as.mutex.Lock()
	defer as.mutex.Unlock()

	if !as.running {
		return nil
	}

	as.running = false
	close(as.stopChan)

	// Wait for in-progress tasks to complete or timeout
	timeout := time.NewTimer(30 * time.Second)
	defer timeout.Stop()

	for len(as.inProgressTasks) > 0 {
		select {
		case <-timeout.C:
			glog.Warningf("Admin server stopping with %d tasks still running", len(as.inProgressTasks))
			break
		case <-time.After(time.Second):
			// Check again
		}
	}

	glog.Infof("Admin server stopped")
	return nil
}

// RegisterWorker registers a new worker
func (as *AdminServer) RegisterWorker(worker *types.Worker) error {
	as.mutex.Lock()
	defer as.mutex.Unlock()

	if !as.running {
		return fmt.Errorf("admin server is not running")
	}

	return as.workerRegistry.RegisterWorker(worker)
}

// UnregisterWorker removes a worker
func (as *AdminServer) UnregisterWorker(workerID string) error {
	as.mutex.Lock()
	defer as.mutex.Unlock()

	// Reschedule any tasks assigned to this worker
	for taskID, task := range as.inProgressTasks {
		if task.WorkerID == workerID {
			glog.Warningf("Rescheduling task %s due to worker %s unregistration", taskID, workerID)
			as.rescheduleTask(task.Task)
			delete(as.inProgressTasks, taskID)
		}
	}

	return as.workerRegistry.UnregisterWorker(workerID)
}

// UpdateWorkerHeartbeat updates worker heartbeat
func (as *AdminServer) UpdateWorkerHeartbeat(workerID string, status *types.WorkerStatus) error {
	as.mutex.Lock()
	defer as.mutex.Unlock()

	return as.workerRegistry.UpdateWorkerHeartbeat(workerID, status)
}

// RequestTask handles task requests from workers
func (as *AdminServer) RequestTask(workerID string, capabilities []types.TaskType) (*types.Task, error) {
	as.mutex.RLock()
	defer as.mutex.RUnlock()

	if !as.running {
		return nil, fmt.Errorf("admin server is not running")
	}

	worker, exists := as.workerRegistry.GetWorker(workerID)
	if !exists {
		return nil, fmt.Errorf("worker %s not registered", workerID)
	}

	// Check if worker has capacity
	if worker.CurrentLoad >= worker.MaxConcurrent {
		return nil, nil // No capacity
	}

	// Get next task for this worker
	task := as.taskScheduler.GetNextTask(workerID, capabilities)
	if task == nil {
		return nil, nil // No suitable tasks
	}

	// Check if volume can be assigned (using comprehensive state management)
	if !as.canAssignTask(task, worker) {
		return nil, nil // Cannot assign due to capacity or state constraints
	}

	// Assign task to worker
	inProgressTask := &InProgressTask{
		Task:         task,
		WorkerID:     workerID,
		StartedAt:    time.Now(),
		LastUpdate:   time.Now(),
		Progress:     0.0,
		EstimatedEnd: time.Now().Add(as.estimateTaskDuration(task)),
	}

	as.inProgressTasks[task.ID] = inProgressTask
	worker.CurrentLoad++

	// Register task impact with state manager
	impact := as.createTaskImpact(task, workerID)
	as.volumeStateManager.RegisterTaskImpact(task.ID, impact)
	inProgressTask.VolumeReserved = true

	glog.V(1).Infof("Assigned task %s to worker %s", task.ID, workerID)
	return task, nil
}

// UpdateTaskProgress updates task progress
func (as *AdminServer) UpdateTaskProgress(taskID string, progress float64) error {
	as.mutex.Lock()
	defer as.mutex.Unlock()

	task, exists := as.inProgressTasks[taskID]
	if !exists {
		return fmt.Errorf("task %s not found", taskID)
	}

	task.Progress = progress
	task.LastUpdate = time.Now()

	glog.V(2).Infof("Task %s progress: %.1f%%", taskID, progress)
	return nil
}

// CompleteTask marks a task as completed
func (as *AdminServer) CompleteTask(taskID string, success bool, errorMsg string) error {
	as.mutex.Lock()
	defer as.mutex.Unlock()

	task, exists := as.inProgressTasks[taskID]
	if !exists {
		return fmt.Errorf("task %s not found", taskID)
	}

	// Update worker load
	if worker, exists := as.workerRegistry.GetWorker(task.WorkerID); exists {
		worker.CurrentLoad--
	}

	// Unregister task impact from state manager
	if task.VolumeReserved {
		as.volumeStateManager.UnregisterTaskImpact(taskID)
	}

	// Record completion
	if success {
		glog.Infof("Task %s completed successfully by worker %s", taskID, task.WorkerID)
		// The state manager will handle volume state updates
	} else {
		glog.Errorf("Task %s failed: %s", taskID, errorMsg)

		// Reschedule if retries available
		if task.Task.RetryCount < as.config.MaxRetries {
			task.Task.RetryCount++
			task.Task.Error = errorMsg
			as.rescheduleTask(task.Task)
		}
	}

	delete(as.inProgressTasks, taskID)
	return nil
}

// GetInProgressTask returns in-progress task for a volume
func (as *AdminServer) GetInProgressTask(volumeID uint32) *InProgressTask {
	as.mutex.RLock()
	defer as.mutex.RUnlock()

	for _, task := range as.inProgressTasks {
		if task.Task.VolumeID == volumeID {
			return task
		}
	}
	return nil
}

// GetPendingChange returns pending volume change
func (as *AdminServer) GetPendingChange(volumeID uint32) *VolumeChange {
	return as.volumeStateManager.GetPendingChange(volumeID)
}

// discoveryLoop runs task discovery periodically
func (as *AdminServer) discoveryLoop() {
	ticker := time.NewTicker(as.config.ScanInterval)
	defer ticker.Stop()

	for {
		select {
		case <-as.stopChan:
			return
		case <-ticker.C:
			as.runTaskDiscovery()
		}
	}
}

// runTaskDiscovery discovers new tasks
func (as *AdminServer) runTaskDiscovery() {
	candidates, err := as.taskDiscovery.ScanForTasks()
	if err != nil {
		glog.Errorf("Task discovery failed: %v", err)
		return
	}

	for _, candidate := range candidates {
		// Check for duplicates
		if as.isDuplicateTask(candidate) {
			continue
		}

		// Create task
		task := &types.Task{
			ID:          generateTaskID(),
			Type:        candidate.TaskType,
			Status:      types.TaskStatusPending,
			Priority:    candidate.Priority,
			VolumeID:    candidate.VolumeID,
			Server:      candidate.Server,
			Collection:  candidate.Collection,
			Parameters:  candidate.Parameters,
			CreatedAt:   time.Now(),
			ScheduledAt: candidate.ScheduleAt,
			MaxRetries:  as.config.MaxRetries,
		}

		as.taskQueue.Push(task)
		glog.V(1).Infof("Discovered new task: %s for volume %d", task.Type, task.VolumeID)
	}
}

// schedulingLoop runs task scheduling
func (as *AdminServer) schedulingLoop() {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-as.stopChan:
			return
		case <-ticker.C:
			as.processTaskQueue()
		}
	}
}

// processTaskQueue processes pending tasks
func (as *AdminServer) processTaskQueue() {
	// Get available workers
	workers := as.workerRegistry.GetAvailableWorkers()
	if len(workers) == 0 {
		return
	}

	// Process up to max concurrent tasks
	processed := 0
	for processed < as.config.MaxConcurrentTasks && !as.taskQueue.IsEmpty() {
		task := as.taskQueue.Peek()
		if task == nil {
			break
		}

		// Find suitable worker
		worker := as.taskScheduler.SelectWorker(task, workers)
		if worker == nil {
			break // No suitable workers available
		}

		// Task will be assigned when worker requests it
		as.taskQueue.Pop()
		processed++
	}
}

// monitoringLoop monitors task progress and timeouts
func (as *AdminServer) monitoringLoop() {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-as.stopChan:
			return
		case <-ticker.C:
			as.checkTaskTimeouts()
		}
	}
}

// checkTaskTimeouts checks for stuck or timed-out tasks
func (as *AdminServer) checkTaskTimeouts() {
	as.mutex.Lock()
	defer as.mutex.Unlock()

	now := time.Now()
	for taskID, task := range as.inProgressTasks {
		// Check for stuck tasks (no progress updates)
		if now.Sub(task.LastUpdate) > as.config.TaskTimeout {
			glog.Warningf("Task %s appears stuck, last update %v ago", taskID, now.Sub(task.LastUpdate))
			as.handleStuckTask(task)
			continue
		}

		// Check for tasks exceeding estimated time
		if now.After(task.EstimatedEnd) && task.Progress < 90.0 {
			estimatedRemaining := time.Duration(float64(now.Sub(task.StartedAt)) * (100.0 - task.Progress) / task.Progress)
			if estimatedRemaining > 2*as.config.TaskTimeout {
				glog.Warningf("Task %s significantly over estimated time", taskID)
				as.handleSlowTask(task)
			}
		}
	}
}

// reconciliationLoop reconciles volume state with master
func (as *AdminServer) reconciliationLoop() {
	ticker := time.NewTicker(as.config.ReconcileInterval)
	defer ticker.Stop()

	for {
		select {
		case <-as.stopChan:
			return
		case <-ticker.C:
			// Use comprehensive state manager for reconciliation
			if err := as.volumeStateManager.SyncWithMaster(); err != nil {
				glog.Errorf("Volume state reconciliation failed: %v", err)
			}
		}
	}
}

// failureRecoveryLoop handles worker failures and recovery
func (as *AdminServer) failureRecoveryLoop() {
	ticker := time.NewTicker(as.config.WorkerTimeout / 2)
	defer ticker.Stop()

	for {
		select {
		case <-as.stopChan:
			return
		case <-ticker.C:
			as.handleWorkerFailures()
		}
	}
}

// handleWorkerFailures detects and handles worker failures
func (as *AdminServer) handleWorkerFailures() {
	as.mutex.Lock()
	defer as.mutex.Unlock()

	timedOutWorkers := as.workerRegistry.GetTimedOutWorkers(as.config.WorkerTimeout)
	for _, workerID := range timedOutWorkers {
		glog.Warningf("Worker %s timed out, rescheduling tasks", workerID)

		// Reschedule tasks from timed-out worker
		for taskID, task := range as.inProgressTasks {
			if task.WorkerID == workerID {
				as.rescheduleTask(task.Task)
				delete(as.inProgressTasks, taskID)
			}
		}

		as.workerRegistry.MarkWorkerInactive(workerID)
	}
}

// isDuplicateTask checks if a task is duplicate
func (as *AdminServer) isDuplicateTask(candidate *VolumeCandidate) bool {
	// Check in-progress tasks
	for _, task := range as.inProgressTasks {
		if task.Task.VolumeID == candidate.VolumeID && task.Task.Type == candidate.TaskType {
			return true
		}
	}

	// Check pending tasks
	return as.taskQueue.HasTask(candidate.VolumeID, candidate.TaskType)
}

// rescheduleTask reschedules a failed task
func (as *AdminServer) rescheduleTask(task *types.Task) {
	task.Status = types.TaskStatusPending
	task.ScheduledAt = time.Now().Add(time.Duration(task.RetryCount) * 5 * time.Minute) // Exponential backoff
	as.taskQueue.Push(task)
}

// handleStuckTask handles a stuck task
func (as *AdminServer) handleStuckTask(task *InProgressTask) {
	glog.Warningf("Handling stuck task %s", task.Task.ID)

	// Mark worker as potentially problematic
	as.workerRegistry.RecordWorkerIssue(task.WorkerID, "task_stuck")

	// Reschedule task
	if task.Task.RetryCount < as.config.MaxRetries {
		as.rescheduleTask(task.Task)
	}

	// Release volume reservation
	if task.VolumeReserved {
		as.volumeStateManager.UnregisterTaskImpact(task.Task.ID) // Use state manager to release
	}

	delete(as.inProgressTasks, task.Task.ID)
}

// handleSlowTask handles a slow task
func (as *AdminServer) handleSlowTask(task *InProgressTask) {
	glog.V(1).Infof("Task %s is running slower than expected", task.Task.ID)
	// Could implement priority adjustments or resource allocation here
}

// estimateTaskDuration estimates how long a task will take
func (as *AdminServer) estimateTaskDuration(task *types.Task) time.Duration {
	switch task.Type {
	case types.TaskTypeErasureCoding:
		return 15 * time.Minute // Base estimate
	case types.TaskTypeVacuum:
		return 10 * time.Minute // Base estimate
	default:
		return 5 * time.Minute
	}
}

// DefaultAdminConfig returns default admin server configuration
func DefaultAdminConfig() *AdminConfig {
	return &AdminConfig{
		ScanInterval:          30 * time.Minute,
		WorkerTimeout:         5 * time.Minute,
		TaskTimeout:           10 * time.Minute,
		MaxRetries:            3,
		ReconcileInterval:     5 * time.Minute,
		EnableFailureRecovery: true,
		MaxConcurrentTasks:    10,
	}
}

// canAssignTask checks if a task can be assigned considering current state
func (as *AdminServer) canAssignTask(task *types.Task, worker *types.Worker) bool {
	// Check server capacity using accurate state information
	volumeState := as.volumeStateManager.GetVolumeState(task.VolumeID)
	if volumeState == nil {
		glog.Warningf("No state information for volume %d", task.VolumeID)
		return false
	}

	// For EC tasks, check if volume is suitable and capacity is available
	if task.Type == types.TaskTypeErasureCoding {
		// Estimate space needed for EC shards (roughly 40% more space)
		estimatedShardSize := int64(float64(volumeState.CurrentState.Size) * 1.4)

		if !as.volumeStateManager.CanAssignVolumeToServer(estimatedShardSize, worker.Address) {
			glog.V(2).Infof("Insufficient capacity on server %s for EC task on volume %d",
				worker.Address, task.VolumeID)
			return false
		}
	}

	// For vacuum tasks, check if there are conflicts
	if task.Type == types.TaskTypeVacuum {
		// Check if volume is already being worked on
		for _, inProgressTask := range as.inProgressTasks {
			if inProgressTask.Task.VolumeID == task.VolumeID {
				glog.V(2).Infof("Volume %d already has task in progress", task.VolumeID)
				return false
			}
		}
	}

	return true
}

// createTaskImpact creates a TaskImpact for state tracking
func (as *AdminServer) createTaskImpact(task *types.Task, workerID string) *TaskImpact {
	impact := &TaskImpact{
		TaskID:        task.ID,
		TaskType:      task.Type,
		VolumeID:      task.VolumeID,
		WorkerID:      workerID,
		StartedAt:     time.Now(),
		EstimatedEnd:  time.Now().Add(as.estimateTaskDuration(task)),
		VolumeChanges: &VolumeChanges{},
		ShardChanges:  make(map[int]*ShardChange),
		CapacityDelta: make(map[string]int64),
	}

	// Configure impact based on task type
	switch task.Type {
	case types.TaskTypeErasureCoding:
		impact.VolumeChanges.WillBecomeReadOnly = true
		// EC will create 14 shards, estimate capacity impact
		volumeState := as.volumeStateManager.GetVolumeState(task.VolumeID)
		if volumeState != nil {
			estimatedShardSize := int64(float64(volumeState.CurrentState.Size) * 1.4)
			impact.CapacityDelta[task.Server] = estimatedShardSize
		}

		// Plan shard creation
		for i := 0; i < 14; i++ { // 10 data + 4 parity shards
			impact.ShardChanges[i] = &ShardChange{
				ShardID:       i,
				WillBeCreated: true,
				TargetServer:  task.Server, // Simplified - in real implementation would distribute across servers
			}
		}

	case types.TaskTypeVacuum:
		// Vacuum typically reduces volume size
		volumeState := as.volumeStateManager.GetVolumeState(task.VolumeID)
		if volumeState != nil {
			// Estimate space savings (based on garbage ratio)
			garbageRatio := float64(volumeState.CurrentState.DeletedByteCount) / float64(volumeState.CurrentState.Size)
			spaceSavings := int64(float64(volumeState.CurrentState.Size) * garbageRatio)
			impact.VolumeChanges.SizeChange = -spaceSavings
			impact.CapacityDelta[task.Server] = -spaceSavings
		}
	}

	return impact
}

// GetVolumeState returns current volume state (for debugging/monitoring)
func (as *AdminServer) GetVolumeState(volumeID uint32) *VolumeState {
	return as.volumeStateManager.GetVolumeState(volumeID)
}

// GetSystemStats returns comprehensive system statistics
func (as *AdminServer) GetSystemStats() map[string]interface{} {
	as.mutex.RLock()
	defer as.mutex.RUnlock()

	stats := make(map[string]interface{})

	// Basic stats
	stats["running"] = as.running
	stats["in_progress_tasks"] = len(as.inProgressTasks)
	stats["queued_tasks"] = as.taskQueue.Size()
	stats["last_reconciliation"] = as.volumeStateManager.lastMasterSync

	// Worker stats
	if as.workerRegistry != nil {
		stats["worker_registry"] = as.workerRegistry.GetRegistryStats()
	}

	// Get server capacity information
	serverStats := make(map[string]*CapacityInfo)
	// This would iterate through known servers and get their capacity info
	stats["server_capacity"] = serverStats

	// Task breakdown by type
	tasksByType := make(map[types.TaskType]int)
	for _, task := range as.inProgressTasks {
		tasksByType[task.Task.Type]++
	}
	stats["tasks_by_type"] = tasksByType

	return stats
}

// generateTaskID generates a unique task ID
func generateTaskID() string {
	// Simple task ID generation - in production would use UUID or similar
	return fmt.Sprintf("task_%d_%d", time.Now().UnixNano(), rand.Intn(10000))
}
