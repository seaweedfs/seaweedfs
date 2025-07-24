package task

import (
	"fmt"
	"sync"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/wdclient"
	"github.com/seaweedfs/seaweedfs/weed/worker/types"
)

// MinimalAdminConfig contains configuration for the minimal admin server
type MinimalAdminConfig struct {
	ScanInterval          time.Duration
	WorkerTimeout         time.Duration
	TaskTimeout           time.Duration
	MaxRetries            int
	ReconcileInterval     time.Duration
	EnableFailureRecovery bool
	MaxConcurrentTasks    int
}

// MinimalAdminServer manages workers and tasks with a simple implementation
type MinimalAdminServer struct {
	config       *MinimalAdminConfig
	masterClient *wdclient.MasterClient
	running      bool
	mutex        sync.RWMutex

	// Task management
	tasks       map[string]*types.Task
	taskQueue   []*types.Task
	activeTasks map[string]*types.Task

	// Worker management
	workers      map[string]*types.Worker
	workerStatus map[string]*types.WorkerStatus

	// Task history
	taskHistory []MinimalTaskHistoryEntry
}

// MinimalTaskHistoryEntry represents a single task history entry
type MinimalTaskHistoryEntry struct {
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

// MinimalSystemStats represents system statistics
type MinimalSystemStats struct {
	ActiveTasks   int
	QueuedTasks   int
	ActiveWorkers int
	TotalTasks    int
}

// NewMinimalAdminServer creates a new minimal admin server
func NewMinimalAdminServer(config *MinimalAdminConfig, masterClient *wdclient.MasterClient) *MinimalAdminServer {
	return &MinimalAdminServer{
		config:       config,
		masterClient: masterClient,
		tasks:        make(map[string]*types.Task),
		taskQueue:    make([]*types.Task, 0),
		activeTasks:  make(map[string]*types.Task),
		workers:      make(map[string]*types.Worker),
		workerStatus: make(map[string]*types.WorkerStatus),
		taskHistory:  make([]MinimalTaskHistoryEntry, 0),
	}
}

// Start starts the minimal admin server
func (as *MinimalAdminServer) Start() error {
	as.mutex.Lock()
	defer as.mutex.Unlock()

	if as.running {
		return fmt.Errorf("admin server is already running")
	}

	as.running = true
	return nil
}

// Stop stops the minimal admin server
func (as *MinimalAdminServer) Stop() error {
	as.mutex.Lock()
	defer as.mutex.Unlock()

	as.running = false
	return nil
}

// RegisterWorker registers a new worker
func (as *MinimalAdminServer) RegisterWorker(worker *types.Worker) error {
	as.mutex.Lock()
	defer as.mutex.Unlock()

	if !as.running {
		return fmt.Errorf("admin server is not running")
	}

	as.workers[worker.ID] = worker
	as.workerStatus[worker.ID] = &types.WorkerStatus{
		Status:      "active",
		CurrentLoad: 0,
	}

	return nil
}

// QueueTask adds a new task to the task queue
func (as *MinimalAdminServer) QueueTask(task *types.Task) error {
	as.mutex.Lock()
	defer as.mutex.Unlock()

	if !as.running {
		return fmt.Errorf("admin server is not running")
	}

	if task.ID == "" {
		task.ID = fmt.Sprintf("task-%d", time.Now().UnixNano())
	}

	task.Status = types.TaskStatusPending
	task.CreatedAt = time.Now()

	as.tasks[task.ID] = task
	as.taskQueue = append(as.taskQueue, task)

	return nil
}

// RequestTask requests a task for a worker
func (as *MinimalAdminServer) RequestTask(workerID string, capabilities []types.TaskType) (*types.Task, error) {
	as.mutex.Lock()
	defer as.mutex.Unlock()

	if !as.running {
		return nil, fmt.Errorf("admin server is not running")
	}

	// Check if worker exists
	worker, exists := as.workers[workerID]
	if !exists {
		return nil, fmt.Errorf("worker %s not found", workerID)
	}

	// Check if worker has capacity
	status := as.workerStatus[workerID]
	if status.CurrentLoad >= worker.MaxConcurrent {
		return nil, nil // No capacity
	}

	// Find a suitable task
	for i, task := range as.taskQueue {
		if task.Status != types.TaskStatusPending {
			continue
		}

		// Check if worker can handle this task type
		canHandle := false
		for _, capability := range capabilities {
			if task.Type == capability {
				canHandle = true
				break
			}
		}

		if canHandle {
			// Assign task to worker
			task.Status = types.TaskStatusInProgress
			task.WorkerID = workerID
			now := time.Now()
			task.StartedAt = &now

			// Move task from queue to active tasks
			as.taskQueue = append(as.taskQueue[:i], as.taskQueue[i+1:]...)
			as.activeTasks[task.ID] = task

			// Update worker load
			status.CurrentLoad++

			return task, nil
		}
	}

	return nil, nil // No suitable task found
}

// UpdateTaskProgress updates task progress
func (as *MinimalAdminServer) UpdateTaskProgress(taskID string, progress float64) error {
	as.mutex.Lock()
	defer as.mutex.Unlock()

	task, exists := as.tasks[taskID]
	if !exists {
		return fmt.Errorf("task %s not found", taskID)
	}

	task.Progress = progress

	return nil
}

// CompleteTask marks a task as completed
func (as *MinimalAdminServer) CompleteTask(taskID string, success bool, errorMessage string) error {
	as.mutex.Lock()
	defer as.mutex.Unlock()

	task, exists := as.tasks[taskID]
	if !exists {
		return fmt.Errorf("task %s not found", taskID)
	}

	// Update task status
	if success {
		task.Status = types.TaskStatusCompleted
	} else {
		task.Status = types.TaskStatusFailed
		task.Error = errorMessage
	}

	now := time.Now()
	task.CompletedAt = &now

	// Remove from active tasks
	delete(as.activeTasks, taskID)

	// Update worker load
	if task.WorkerID != "" {
		if status, exists := as.workerStatus[task.WorkerID]; exists {
			status.CurrentLoad--
		}
	}

	// Add to history
	var duration time.Duration
	if task.StartedAt != nil {
		duration = now.Sub(*task.StartedAt)
	}

	entry := MinimalTaskHistoryEntry{
		TaskID:       task.ID,
		TaskType:     task.Type,
		VolumeID:     task.VolumeID,
		WorkerID:     task.WorkerID,
		Status:       task.Status,
		StartedAt:    *task.StartedAt,
		CompletedAt:  now,
		Duration:     duration,
		ErrorMessage: errorMessage,
	}
	as.taskHistory = append(as.taskHistory, entry)

	return nil
}

// UpdateWorkerHeartbeat updates worker heartbeat
func (as *MinimalAdminServer) UpdateWorkerHeartbeat(workerID string, status *types.WorkerStatus) error {
	as.mutex.Lock()
	defer as.mutex.Unlock()

	worker, exists := as.workers[workerID]
	if !exists {
		return fmt.Errorf("worker %s not found", workerID)
	}

	worker.LastHeartbeat = time.Now()
	as.workerStatus[workerID] = status

	return nil
}

// GetSystemStats returns system statistics
func (as *MinimalAdminServer) GetSystemStats() *MinimalSystemStats {
	as.mutex.RLock()
	defer as.mutex.RUnlock()

	activeWorkers := 0
	for _, status := range as.workerStatus {
		if status.Status == "active" {
			activeWorkers++
		}
	}

	return &MinimalSystemStats{
		ActiveTasks:   len(as.activeTasks),
		QueuedTasks:   len(as.taskQueue),
		ActiveWorkers: activeWorkers,
		TotalTasks:    len(as.tasks),
	}
}

// GetQueuedTaskCount returns the number of queued tasks
func (as *MinimalAdminServer) GetQueuedTaskCount() int {
	as.mutex.RLock()
	defer as.mutex.RUnlock()
	return len(as.taskQueue)
}

// GetActiveTaskCount returns the number of active tasks
func (as *MinimalAdminServer) GetActiveTaskCount() int {
	as.mutex.RLock()
	defer as.mutex.RUnlock()
	return len(as.activeTasks)
}

// GetTaskHistory returns task history
func (as *MinimalAdminServer) GetTaskHistory() []MinimalTaskHistoryEntry {
	as.mutex.RLock()
	defer as.mutex.RUnlock()

	// Return a copy of the history
	history := make([]MinimalTaskHistoryEntry, len(as.taskHistory))
	copy(history, as.taskHistory)
	return history
}
