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

// TaskHistory represents task execution history
type TaskHistory struct {
	entries []TaskHistoryEntry
	mutex   sync.RWMutex
}

// TaskHistoryEntry represents a single task history entry
type TaskHistoryEntry struct {
	TaskID       string
	TaskType     types.TaskType
	VolumeID     uint32
	WorkerID     string
	Status       types.TaskStatus
	StartedAt    time.Time
	CompletedAt  time.Time
	Duration     time.Duration
	ErrorMessage string
}

// NewTaskHistory creates a new task history
func NewTaskHistory() *TaskHistory {
	return &TaskHistory{
		entries: make([]TaskHistoryEntry, 0),
	}
}

// AddEntry adds a new task history entry
func (th *TaskHistory) AddEntry(entry TaskHistoryEntry) {
	th.mutex.Lock()
	defer th.mutex.Unlock()

	th.entries = append(th.entries, entry)

	// Keep only the last 1000 entries
	if len(th.entries) > 1000 {
		th.entries = th.entries[len(th.entries)-1000:]
	}
}

// GetRecentEntries returns the most recent entries
func (th *TaskHistory) GetRecentEntries(limit int) []*TaskHistoryEntry {
	th.mutex.RLock()
	defer th.mutex.RUnlock()

	start := len(th.entries) - limit
	if start < 0 {
		start = 0
	}

	result := make([]*TaskHistoryEntry, len(th.entries)-start)
	for i, entry := range th.entries[start:] {
		entryCopy := entry
		result[i] = &entryCopy
	}

	return result
}

// AdminServer manages task distribution and worker coordination
type AdminServer struct {
	ID                 string
	Config             *AdminConfig
	masterClient       *wdclient.MasterClient
	volumeStateManager *VolumeStateManager
	workerRegistry     *WorkerRegistry
	taskQueue          *PriorityTaskQueue
	taskScheduler      *TaskScheduler
	taskHistory        *TaskHistory
	failureHandler     *FailureHandler
	masterSync         *MasterSynchronizer
	workerComm         *WorkerCommunicationManager
	running            bool
	stopCh             chan struct{}
	mutex              sync.RWMutex

	// Task tracking
	activeTasks map[string]*InProgressTask
	tasksMutex  sync.RWMutex
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
	adminServer := &AdminServer{
		ID:                 generateAdminServerID(),
		Config:             config,
		masterClient:       masterClient,
		volumeStateManager: NewVolumeStateManager(masterClient),
		workerRegistry:     NewWorkerRegistry(),
		taskQueue:          NewPriorityTaskQueue(),
		taskHistory:        NewTaskHistory(),
		failureHandler:     NewFailureHandler(config),
		activeTasks:        make(map[string]*InProgressTask),
		stopCh:             make(chan struct{}),
	}

	// Initialize components that depend on admin server
	adminServer.taskScheduler = NewTaskScheduler(adminServer.workerRegistry, adminServer.taskQueue)
	adminServer.masterSync = NewMasterSynchronizer(masterClient, adminServer.volumeStateManager, adminServer)
	adminServer.workerComm = NewWorkerCommunicationManager(adminServer)

	glog.Infof("Created admin server %s", adminServer.ID)
	return adminServer
}

// Start starts the admin server
func (as *AdminServer) Start() error {
	as.mutex.Lock()
	defer as.mutex.Unlock()

	if as.running {
		return nil
	}

	glog.Infof("Starting admin server %s", as.ID)

	// Start components
	as.masterSync.Start()
	as.workerComm.Start()

	// Start background loops
	go as.taskAssignmentLoop()
	go as.taskMonitoringLoop()
	go as.reconciliationLoop()
	go as.metricsLoop()

	as.running = true
	glog.Infof("Admin server %s started successfully", as.ID)

	return nil
}

// Stop stops the admin server
func (as *AdminServer) Stop() {
	as.mutex.Lock()
	defer as.mutex.Unlock()

	if !as.running {
		return
	}

	glog.Infof("Stopping admin server %s", as.ID)

	close(as.stopCh)

	// Stop components
	as.masterSync.Stop()
	as.workerComm.Stop()

	as.running = false
	glog.Infof("Admin server %s stopped", as.ID)
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
	for taskID, task := range as.activeTasks {
		if task.WorkerID == workerID {
			glog.Warningf("Rescheduling task %s due to worker %s unregistration", taskID, workerID)
			as.ReassignTask(taskID, "worker unregistration")
			delete(as.activeTasks, taskID)
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
	if !as.canAssignTask(task, workerID) {
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

	as.activeTasks[task.ID] = inProgressTask
	worker.CurrentLoad++

	// Register task impact with state manager
	impact := as.createTaskImpact(task)
	as.volumeStateManager.RegisterTaskImpact(task.ID, impact)
	inProgressTask.VolumeReserved = true

	glog.V(1).Infof("Assigned task %s to worker %s", task.ID, workerID)
	return task, nil
}

// UpdateTaskProgress updates task progress
func (as *AdminServer) UpdateTaskProgress(taskID string, progress float64) error {
	as.tasksMutex.Lock()
	defer as.tasksMutex.Unlock()

	inProgressTask, exists := as.activeTasks[taskID]
	if !exists {
		return fmt.Errorf("task %s not found", taskID)
	}

	inProgressTask.Progress = progress
	inProgressTask.LastUpdate = time.Now()

	glog.V(2).Infof("Task %s progress: %.1f%%", taskID, progress)
	return nil
}

// CompleteTask marks a task as completed
func (as *AdminServer) CompleteTask(taskID string, success bool, errorMsg string) error {
	as.tasksMutex.Lock()
	defer as.tasksMutex.Unlock()

	inProgressTask, exists := as.activeTasks[taskID]
	if !exists {
		return fmt.Errorf("task %s not found", taskID)
	}

	// Remove from active tasks
	delete(as.activeTasks, taskID)

	// Update worker load
	if worker, exists := as.workerRegistry.GetWorker(inProgressTask.WorkerID); exists {
		worker.CurrentLoad--
	}

	// Unregister task impact
	as.volumeStateManager.UnregisterTaskImpact(taskID)

	// Record in task history
	status := types.TaskStatusCompleted
	if !success {
		status = types.TaskStatusFailed
	}

	as.taskHistory.AddEntry(TaskHistoryEntry{
		TaskID:       taskID,
		TaskType:     inProgressTask.Task.Type,
		VolumeID:     inProgressTask.Task.VolumeID,
		WorkerID:     inProgressTask.WorkerID,
		Status:       status,
		StartedAt:    inProgressTask.StartedAt,
		CompletedAt:  time.Now(),
		Duration:     time.Since(inProgressTask.StartedAt),
		ErrorMessage: errorMsg,
	})

	glog.Infof("Task %s completed: success=%v", taskID, success)
	return nil
}

// QueueTask adds a new task to the task queue
func (as *AdminServer) QueueTask(task *types.Task) error {
	as.mutex.Lock()
	defer as.mutex.Unlock()

	if !as.running {
		return fmt.Errorf("admin server is not running")
	}

	// Validate the task
	if task == nil {
		return fmt.Errorf("task cannot be nil")
	}

	if task.ID == "" {
		task.ID = generateTaskID()
	}

	// Set creation timestamp if not set
	if task.CreatedAt.IsZero() {
		task.CreatedAt = time.Now()
	}

	// Check if task for this volume is already queued or in progress
	if as.isVolumeAlreadyQueued(task.VolumeID, task.Type) {
		glog.V(2).Infof("Task for volume %d already queued or in progress, skipping", task.VolumeID)
		return nil
	}

	// Add to task queue
	as.taskQueue.Push(task)

	glog.V(1).Infof("Queued task %s (%s) for volume %d with priority %v",
		task.ID, task.Type, task.VolumeID, task.Priority)

	return nil
}

// Helper methods

// canAssignTask checks if a task can be assigned to a worker
func (as *AdminServer) canAssignTask(task *types.Task, workerID string) bool {
	worker, exists := as.workerRegistry.GetWorker(workerID)
	if !exists {
		return false
	}

	// Check worker capacity
	if worker.CurrentLoad >= worker.MaxConcurrent {
		return false
	}

	// Check if worker has required capability
	hasCapability := false
	for _, cap := range worker.Capabilities {
		if cap == task.Type {
			hasCapability = true
			break
		}
	}
	if !hasCapability {
		return false
	}

	return true
}

// createTaskImpact creates a TaskImpact for the given task
func (as *AdminServer) createTaskImpact(task *types.Task) *TaskImpact {
	impact := &TaskImpact{
		TaskID:        task.ID,
		VolumeID:      task.VolumeID,
		TaskType:      task.Type,
		StartedAt:     time.Now(),
		EstimatedEnd:  time.Now().Add(as.estimateTaskDuration(task)),
		CapacityDelta: make(map[string]int64),
		VolumeChanges: &VolumeChanges{},
		ShardChanges:  make(map[int]*ShardChange),
	}

	// Set task-specific impacts
	switch task.Type {
	case types.TaskTypeErasureCoding:
		impact.VolumeChanges.WillBecomeReadOnly = true
		impact.EstimatedEnd = time.Now().Add(2 * time.Hour) // EC takes longer

		// EC encoding requires temporary space
		if server, ok := task.Parameters["server"]; ok {
			if serverStr, ok := server.(string); ok {
				volumeState := as.volumeStateManager.GetVolumeState(task.VolumeID)
				if volumeState != nil && volumeState.CurrentState != nil {
					// Estimate 2x volume size needed temporarily
					impact.CapacityDelta[serverStr] = int64(volumeState.CurrentState.Size * 2)
				}
			}
		}

	case types.TaskTypeVacuum:
		// Vacuum reduces volume size
		if server, ok := task.Parameters["server"]; ok {
			if serverStr, ok := server.(string); ok {
				// Estimate 30% space reclamation
				volumeState := as.volumeStateManager.GetVolumeState(task.VolumeID)
				if volumeState != nil && volumeState.CurrentState != nil {
					impact.CapacityDelta[serverStr] = -int64(float64(volumeState.CurrentState.Size) * 0.3)
				}
			}
		}
	}

	return impact
}

// estimateTaskDuration estimates how long a task will take
func (as *AdminServer) estimateTaskDuration(task *types.Task) time.Duration {
	switch task.Type {
	case types.TaskTypeErasureCoding:
		return 2 * time.Hour
	case types.TaskTypeVacuum:
		return 30 * time.Minute
	default:
		return 1 * time.Hour
	}
}

// isVolumeAlreadyQueued checks if a task for the volume is already queued or in progress
func (as *AdminServer) isVolumeAlreadyQueued(volumeID uint32, taskType types.TaskType) bool {
	// Check active tasks
	as.tasksMutex.RLock()
	for _, inProgressTask := range as.activeTasks {
		if inProgressTask.Task.VolumeID == volumeID && inProgressTask.Task.Type == taskType {
			as.tasksMutex.RUnlock()
			return true
		}
	}
	as.tasksMutex.RUnlock()

	// Check queued tasks
	return as.taskQueue.HasTask(volumeID, taskType)
}

// Background loops

// taskAssignmentLoop handles automatic task assignment to workers
func (as *AdminServer) taskAssignmentLoop() {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			as.processTaskAssignments()
		case <-as.stopCh:
			return
		}
	}
}

// processTaskAssignments attempts to assign pending tasks to available workers
func (as *AdminServer) processTaskAssignments() {
	// Get available workers
	workers := as.workerRegistry.GetAvailableWorkers()
	if len(workers) == 0 {
		return // No workers available
	}

	// For each worker with available capacity, try to assign a task
	for _, worker := range workers {
		if worker.CurrentLoad < worker.MaxConcurrent {
			task := as.taskScheduler.GetNextTask(worker.ID, worker.Capabilities)
			if task != nil {
				// Try to assign task directly
				_, err := as.RequestTask(worker.ID, worker.Capabilities)
				if err != nil {
					glog.Errorf("Failed to assign task to worker %s: %v", worker.ID, err)
				}
			}
		}
	}
}

// taskMonitoringLoop monitors task progress and handles timeouts
func (as *AdminServer) taskMonitoringLoop() {
	ticker := time.NewTicker(1 * time.Minute)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			as.checkTaskTimeouts()
		case <-as.stopCh:
			return
		}
	}
}

// checkTaskTimeouts checks for tasks that have timed out
func (as *AdminServer) checkTaskTimeouts() {
	as.tasksMutex.Lock()
	defer as.tasksMutex.Unlock()

	now := time.Now()
	timeout := 2 * time.Hour // Default task timeout

	for taskID, inProgressTask := range as.activeTasks {
		if now.Sub(inProgressTask.LastUpdate) > timeout {
			glog.Warningf("Task %s timed out (last update: %v)", taskID, inProgressTask.LastUpdate)
			as.ReassignTask(taskID, "task timeout")
		}
	}
}

// ReassignTask reassigns a task due to worker failure
func (as *AdminServer) ReassignTask(taskID, reason string) {
	as.tasksMutex.Lock()
	defer as.tasksMutex.Unlock()

	inProgressTask, exists := as.activeTasks[taskID]
	if !exists {
		return
	}

	glog.Infof("Reassigning task %s due to: %s", taskID, reason)

	// Reset task status
	inProgressTask.Task.Status = types.TaskStatusPending

	// Unregister current task impact
	as.volumeStateManager.UnregisterTaskImpact(taskID)

	// Remove from active tasks
	delete(as.activeTasks, taskID)

	// Put back in queue with higher priority
	inProgressTask.Task.Priority = types.TaskPriorityHigh
	as.taskQueue.Push(inProgressTask.Task)
}

// reconciliationLoop periodically reconciles state with master
func (as *AdminServer) reconciliationLoop() {
	ticker := time.NewTicker(5 * time.Minute)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			as.performReconciliation()
		case <-as.stopCh:
			return
		}
	}
}

// performReconciliation reconciles admin state with master
func (as *AdminServer) performReconciliation() {
	glog.V(1).Infof("Starting state reconciliation")

	// Sync with master
	err := as.volumeStateManager.SyncWithMaster()
	if err != nil {
		glog.Errorf("Failed to sync with master during reconciliation: %v", err)
		return
	}

	glog.V(1).Infof("State reconciliation completed")
}

// metricsLoop periodically logs metrics and statistics
func (as *AdminServer) metricsLoop() {
	ticker := time.NewTicker(1 * time.Minute)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			as.logMetrics()
		case <-as.stopCh:
			return
		}
	}
}

// logMetrics logs current system metrics
func (as *AdminServer) logMetrics() {
	as.tasksMutex.RLock()
	activeTasks := len(as.activeTasks)
	as.tasksMutex.RUnlock()

	queuedTasks := as.taskQueue.Size()
	activeWorkers := len(as.workerRegistry.GetAvailableWorkers())

	glog.V(1).Infof("Admin server metrics: active_tasks=%d, queued_tasks=%d, active_workers=%d",
		activeTasks, queuedTasks, activeWorkers)
}

// GetAvailableWorkers returns workers capable of handling the specified task type
func (as *AdminServer) GetAvailableWorkers(taskType string) []*types.Worker {
	workers := as.workerRegistry.GetAvailableWorkers()
	var available []*types.Worker

	for _, worker := range workers {
		if worker.CurrentLoad < worker.MaxConcurrent {
			for _, cap := range worker.Capabilities {
				if string(cap) == taskType {
					available = append(available, worker)
					break
				}
			}
		}
	}

	return available
}

// GetSystemStats returns current system statistics
func (as *AdminServer) GetSystemStats() *SystemStats {
	as.tasksMutex.RLock()
	activeTasks := len(as.activeTasks)
	as.tasksMutex.RUnlock()

	queuedTasks := as.taskQueue.Size()
	activeWorkers := len(as.workerRegistry.GetAvailableWorkers())

	return &SystemStats{
		ActiveTasks:   activeTasks,
		QueuedTasks:   queuedTasks,
		ActiveWorkers: activeWorkers,
		TotalWorkers:  len(as.workerRegistry.GetAvailableWorkers()),
		Uptime:        time.Since(time.Now()), // This should be tracked properly
	}
}

// Getter methods for testing
func (as *AdminServer) GetQueuedTaskCount() int {
	return as.taskQueue.Size()
}

func (as *AdminServer) GetActiveTaskCount() int {
	as.tasksMutex.RLock()
	defer as.tasksMutex.RUnlock()
	return len(as.activeTasks)
}

func (as *AdminServer) GetTaskHistory() []*TaskHistoryEntry {
	return as.taskHistory.GetRecentEntries(100)
}

func (as *AdminServer) GetVolumeStateManager() *VolumeStateManager {
	return as.volumeStateManager
}

func (as *AdminServer) GetWorkerRegistry() *WorkerRegistry {
	return as.workerRegistry
}

// generateTaskID generates a unique task ID
func generateTaskID() string {
	return fmt.Sprintf("task_%d_%d", time.Now().UnixNano(), rand.Intn(10000))
}

// generateAdminServerID generates a unique admin server ID
func generateAdminServerID() string {
	return fmt.Sprintf("admin-%d", time.Now().Unix())
}

// SystemStats represents system statistics
type SystemStats struct {
	ActiveTasks    int
	QueuedTasks    int
	ActiveWorkers  int
	TotalWorkers   int
	Uptime         time.Duration
	LastMasterSync time.Time
}
