package worker

import (
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/worker/tasks"
	"github.com/seaweedfs/seaweedfs/weed/worker/types"

	// Import task packages to trigger their auto-registration
	_ "github.com/seaweedfs/seaweedfs/weed/worker/tasks/balance"
	_ "github.com/seaweedfs/seaweedfs/weed/worker/tasks/erasure_coding"
	_ "github.com/seaweedfs/seaweedfs/weed/worker/tasks/vacuum"
)

// Worker represents a maintenance worker instance
type Worker struct {
	id              string
	config          *types.WorkerConfig
	registry        *tasks.TaskRegistry
	currentTasks    map[string]*types.Task
	adminClient     AdminClient
	running         bool
	stopChan        chan struct{}
	mutex           sync.RWMutex
	startTime       time.Time
	tasksCompleted  int
	tasksFailed     int
	heartbeatTicker *time.Ticker
	requestTicker   *time.Ticker
}

// AdminClient defines the interface for communicating with the admin server
type AdminClient interface {
	Connect() error
	Disconnect() error
	RegisterWorker(worker *types.Worker) error
	SendHeartbeat(workerID string, status *types.WorkerStatus) error
	RequestTask(workerID string, capabilities []types.TaskType) (*types.Task, error)
	CompleteTask(taskID string, success bool, errorMsg string) error
	UpdateTaskProgress(taskID string, progress float64) error
	IsConnected() bool
}

// NewWorker creates a new worker instance
func NewWorker(config *types.WorkerConfig) (*Worker, error) {
	if config == nil {
		config = types.DefaultWorkerConfig()
	}

	// Always auto-generate worker ID
	hostname, _ := os.Hostname()
	workerID := fmt.Sprintf("worker-%s-%d", hostname, time.Now().Unix())

	// Use the global registry that already has all tasks registered
	registry := tasks.GetGlobalRegistry()

	worker := &Worker{
		id:           workerID,
		config:       config,
		registry:     registry,
		currentTasks: make(map[string]*types.Task),
		stopChan:     make(chan struct{}),
		startTime:    time.Now(),
	}

	glog.V(1).Infof("Worker created with %d registered task types", len(registry.GetSupportedTypes()))

	return worker, nil
}

// ID returns the worker ID
func (w *Worker) ID() string {
	return w.id
}

// Start starts the worker
func (w *Worker) Start() error {
	w.mutex.Lock()
	defer w.mutex.Unlock()

	if w.running {
		return fmt.Errorf("worker is already running")
	}

	if w.adminClient == nil {
		return fmt.Errorf("admin client is not set")
	}

	// Connect to admin server
	if err := w.adminClient.Connect(); err != nil {
		return fmt.Errorf("failed to connect to admin server: %v", err)
	}

	w.running = true
	w.startTime = time.Now()

	// Register with admin server
	workerInfo := &types.Worker{
		ID:            w.id,
		Capabilities:  w.config.Capabilities,
		MaxConcurrent: w.config.MaxConcurrent,
		Status:        "active",
		CurrentLoad:   0,
		LastHeartbeat: time.Now(),
	}

	if err := w.adminClient.RegisterWorker(workerInfo); err != nil {
		w.running = false
		w.adminClient.Disconnect()
		return fmt.Errorf("failed to register worker: %v", err)
	}

	// Start worker loops
	go w.heartbeatLoop()
	go w.taskRequestLoop()

	glog.Infof("Worker %s started", w.id)
	return nil
}

// Stop stops the worker
func (w *Worker) Stop() error {
	w.mutex.Lock()
	defer w.mutex.Unlock()

	if !w.running {
		return nil
	}

	w.running = false
	close(w.stopChan)

	// Stop tickers
	if w.heartbeatTicker != nil {
		w.heartbeatTicker.Stop()
	}
	if w.requestTicker != nil {
		w.requestTicker.Stop()
	}

	// Wait for current tasks to complete or timeout
	timeout := time.NewTimer(30 * time.Second)
	defer timeout.Stop()

	for len(w.currentTasks) > 0 {
		select {
		case <-timeout.C:
			glog.Warningf("Worker %s stopping with %d tasks still running", w.id, len(w.currentTasks))
			break
		case <-time.After(time.Second):
			// Check again
		}
	}

	// Disconnect from admin server
	if w.adminClient != nil {
		if err := w.adminClient.Disconnect(); err != nil {
			glog.Errorf("Error disconnecting from admin server: %v", err)
		}
	}

	glog.Infof("Worker %s stopped", w.id)
	return nil
}

// RegisterTask registers a task factory
func (w *Worker) RegisterTask(taskType types.TaskType, factory types.TaskFactory) {
	w.registry.Register(taskType, factory)
}

// GetCapabilities returns the worker capabilities
func (w *Worker) GetCapabilities() []types.TaskType {
	return w.config.Capabilities
}

// GetStatus returns the current worker status
func (w *Worker) GetStatus() types.WorkerStatus {
	w.mutex.RLock()
	defer w.mutex.RUnlock()

	var currentTasks []types.Task
	for _, task := range w.currentTasks {
		currentTasks = append(currentTasks, *task)
	}

	status := "active"
	if len(w.currentTasks) >= w.config.MaxConcurrent {
		status = "busy"
	}

	return types.WorkerStatus{
		WorkerID:       w.id,
		Status:         status,
		Capabilities:   w.config.Capabilities,
		MaxConcurrent:  w.config.MaxConcurrent,
		CurrentLoad:    len(w.currentTasks),
		LastHeartbeat:  time.Now(),
		CurrentTasks:   currentTasks,
		Uptime:         time.Since(w.startTime),
		TasksCompleted: w.tasksCompleted,
		TasksFailed:    w.tasksFailed,
	}
}

// HandleTask handles a task execution
func (w *Worker) HandleTask(task *types.Task) error {
	w.mutex.Lock()
	if len(w.currentTasks) >= w.config.MaxConcurrent {
		w.mutex.Unlock()
		return fmt.Errorf("worker is at capacity")
	}
	w.currentTasks[task.ID] = task
	w.mutex.Unlock()

	// Execute task in goroutine
	go w.executeTask(task)

	return nil
}

// SetCapabilities sets the worker capabilities
func (w *Worker) SetCapabilities(capabilities []types.TaskType) {
	w.config.Capabilities = capabilities
}

// SetMaxConcurrent sets the maximum concurrent tasks
func (w *Worker) SetMaxConcurrent(max int) {
	w.config.MaxConcurrent = max
}

// SetHeartbeatInterval sets the heartbeat interval
func (w *Worker) SetHeartbeatInterval(interval time.Duration) {
	w.config.HeartbeatInterval = interval
}

// SetTaskRequestInterval sets the task request interval
func (w *Worker) SetTaskRequestInterval(interval time.Duration) {
	w.config.TaskRequestInterval = interval
}

// SetAdminClient sets the admin client
func (w *Worker) SetAdminClient(client AdminClient) {
	w.adminClient = client
}

// executeTask executes a task
func (w *Worker) executeTask(task *types.Task) {
	defer func() {
		w.mutex.Lock()
		delete(w.currentTasks, task.ID)
		w.mutex.Unlock()
	}()

	glog.Infof("Worker %s executing task %s: %s", w.id, task.ID, task.Type)

	// Create task instance
	taskParams := types.TaskParams{
		VolumeID:   task.VolumeID,
		Server:     task.Server,
		Collection: task.Collection,
		Parameters: task.Parameters,
	}

	taskInstance, err := w.registry.CreateTask(task.Type, taskParams)
	if err != nil {
		w.completeTask(task.ID, false, fmt.Sprintf("failed to create task: %v", err))
		return
	}

	// Execute task
	err = taskInstance.Execute(taskParams)

	// Report completion
	if err != nil {
		w.completeTask(task.ID, false, err.Error())
		w.tasksFailed++
		glog.Errorf("Worker %s failed to execute task %s: %v", w.id, task.ID, err)
	} else {
		w.completeTask(task.ID, true, "")
		w.tasksCompleted++
		glog.Infof("Worker %s completed task %s successfully", w.id, task.ID)
	}
}

// completeTask reports task completion to admin server
func (w *Worker) completeTask(taskID string, success bool, errorMsg string) {
	if w.adminClient != nil {
		if err := w.adminClient.CompleteTask(taskID, success, errorMsg); err != nil {
			glog.Errorf("Failed to report task completion: %v", err)
		}
	}
}

// heartbeatLoop sends periodic heartbeats to the admin server
func (w *Worker) heartbeatLoop() {
	w.heartbeatTicker = time.NewTicker(w.config.HeartbeatInterval)
	defer w.heartbeatTicker.Stop()

	for {
		select {
		case <-w.stopChan:
			return
		case <-w.heartbeatTicker.C:
			w.sendHeartbeat()
		}
	}
}

// taskRequestLoop periodically requests new tasks from the admin server
func (w *Worker) taskRequestLoop() {
	w.requestTicker = time.NewTicker(w.config.TaskRequestInterval)
	defer w.requestTicker.Stop()

	for {
		select {
		case <-w.stopChan:
			return
		case <-w.requestTicker.C:
			w.requestTasks()
		}
	}
}

// sendHeartbeat sends heartbeat to admin server
func (w *Worker) sendHeartbeat() {
	if w.adminClient != nil {
		if err := w.adminClient.SendHeartbeat(w.id, &types.WorkerStatus{
			WorkerID:      w.id,
			Status:        "active",
			Capabilities:  w.config.Capabilities,
			MaxConcurrent: w.config.MaxConcurrent,
			CurrentLoad:   len(w.currentTasks),
			LastHeartbeat: time.Now(),
		}); err != nil {
			glog.Warningf("Failed to send heartbeat: %v", err)
		}
	}
}

// requestTasks requests new tasks from the admin server
func (w *Worker) requestTasks() {
	w.mutex.RLock()
	currentLoad := len(w.currentTasks)
	w.mutex.RUnlock()

	if currentLoad >= w.config.MaxConcurrent {
		return // Already at capacity
	}

	if w.adminClient != nil {
		task, err := w.adminClient.RequestTask(w.id, w.config.Capabilities)
		if err != nil {
			glog.V(2).Infof("Failed to request task: %v", err)
			return
		}

		if task != nil {
			if err := w.HandleTask(task); err != nil {
				glog.Errorf("Failed to handle task: %v", err)
			}
		}
	}
}

// GetTaskRegistry returns the task registry
func (w *Worker) GetTaskRegistry() *tasks.TaskRegistry {
	return w.registry
}

// GetCurrentTasks returns the current tasks
func (w *Worker) GetCurrentTasks() map[string]*types.Task {
	w.mutex.RLock()
	defer w.mutex.RUnlock()

	tasks := make(map[string]*types.Task)
	for id, task := range w.currentTasks {
		tasks[id] = task
	}
	return tasks
}

// GetConfig returns the worker configuration
func (w *Worker) GetConfig() *types.WorkerConfig {
	return w.config
}

// GetPerformanceMetrics returns performance metrics
func (w *Worker) GetPerformanceMetrics() *types.WorkerPerformance {
	w.mutex.RLock()
	defer w.mutex.RUnlock()

	uptime := time.Since(w.startTime)
	var successRate float64
	totalTasks := w.tasksCompleted + w.tasksFailed
	if totalTasks > 0 {
		successRate = float64(w.tasksCompleted) / float64(totalTasks) * 100
	}

	return &types.WorkerPerformance{
		TasksCompleted:  w.tasksCompleted,
		TasksFailed:     w.tasksFailed,
		AverageTaskTime: 0, // Would need to track this
		Uptime:          uptime,
		SuccessRate:     successRate,
	}
}
