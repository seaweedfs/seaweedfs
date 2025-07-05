package worker

import (
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/worker/tasks"
	"github.com/seaweedfs/seaweedfs/weed/worker/types"
)

// Worker represents a maintenance worker instance
type Worker struct {
	id              string
	address         string
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
	RegisterWorker(worker *types.Worker) error
	RequestTask(workerID string, capabilities []types.TaskType) (*types.Task, error)
	CompleteTask(taskID string, error string) error
	UpdateTaskProgress(taskID string, progress float64) error
	SendHeartbeat(workerID string) error
}

// NewWorker creates a new worker instance
func NewWorker(config *types.WorkerConfig) (*Worker, error) {
	if config == nil {
		config = types.DefaultWorkerConfig()
	}

	// Always auto-generate worker ID
	hostname, _ := os.Hostname()
	workerID := fmt.Sprintf("worker-%s-%d", hostname, time.Now().Unix())

	// Auto-generate address
	address := ":8082"

	registry := tasks.NewTaskRegistry()

	worker := &Worker{
		id:           workerID,
		address:      address,
		config:       config,
		registry:     registry,
		currentTasks: make(map[string]*types.Task),
		stopChan:     make(chan struct{}),
		startTime:    time.Now(),
	}

	// Register default tasks
	worker.registerDefaultTasks()

	return worker, nil
}

// ID returns the worker ID
func (w *Worker) ID() string {
	return w.id
}

// Address returns the worker address
func (w *Worker) Address() string {
	return w.address
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

	w.running = true
	w.startTime = time.Now()

	// Register with admin server
	workerInfo := &types.Worker{
		ID:            w.id,
		Address:       w.address,
		Capabilities:  w.config.Capabilities,
		MaxConcurrent: w.config.MaxConcurrent,
		Status:        "active",
		CurrentLoad:   0,
		LastHeartbeat: time.Now(),
	}

	if err := w.adminClient.RegisterWorker(workerInfo); err != nil {
		w.running = false
		return fmt.Errorf("failed to register worker: %v", err)
	}

	// Start worker loops
	go w.heartbeatLoop()
	go w.taskRequestLoop()

	glog.Infof("Worker %s started at %s", w.id, w.address)
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
			return nil
		case <-time.After(time.Second):
			// Check again
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
		Address:        w.address,
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
		w.completeTask(task.ID, fmt.Sprintf("failed to create task: %v", err))
		return
	}

	// Execute task
	err = taskInstance.Execute(taskParams)

	// Report completion
	if err != nil {
		w.completeTask(task.ID, err.Error())
		w.tasksFailed++
		glog.Errorf("Worker %s failed to execute task %s: %v", w.id, task.ID, err)
	} else {
		w.completeTask(task.ID, "")
		w.tasksCompleted++
		glog.Infof("Worker %s completed task %s successfully", w.id, task.ID)
	}
}

// completeTask reports task completion to admin server
func (w *Worker) completeTask(taskID string, errorMsg string) {
	if w.adminClient != nil {
		if err := w.adminClient.CompleteTask(taskID, errorMsg); err != nil {
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
		if err := w.adminClient.SendHeartbeat(w.id); err != nil {
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

// registerDefaultTasks registers the default task implementations
func (w *Worker) registerDefaultTasks() {
	// Register vacuum task
	RegisterVacuumTask(w.registry)

	// Additional task registrations can be added here
	// RegisterErasureCodingTask(w.registry)
	// RegisterRemoteUploadTask(w.registry)
	// RegisterReplicationTask(w.registry)
	// RegisterBalanceTask(w.registry)
}

// RegisterVacuumTask registers the vacuum task with a registry
func RegisterVacuumTask(registry *tasks.TaskRegistry) {
	factory := NewVacuumTaskFactory()
	registry.Register(types.TaskTypeVacuum, factory)
}

// NewVacuumTaskFactory creates a new vacuum task factory
func NewVacuumTaskFactory() types.TaskFactory {
	return &VacuumTaskFactory{
		capabilities: []string{"vacuum", "storage"},
		description:  "Vacuum operation to reclaim disk space by removing deleted files",
	}
}

// VacuumTaskFactory creates vacuum task instances
type VacuumTaskFactory struct {
	capabilities []string
	description  string
}

// Create creates a new vacuum task instance
func (f *VacuumTaskFactory) Create(params types.TaskParams) (types.TaskInterface, error) {
	// Validate parameters
	if params.VolumeID == 0 {
		return nil, fmt.Errorf("volume_id is required")
	}
	if params.Server == "" {
		return nil, fmt.Errorf("server is required")
	}

	task := &VacuumTaskImpl{
		taskType:  types.TaskTypeVacuum,
		server:    params.Server,
		volumeID:  params.VolumeID,
		progress:  0.0,
		cancelled: false,
	}

	return task, nil
}

// Capabilities returns the capabilities required for this task type
func (f *VacuumTaskFactory) Capabilities() []string {
	return f.capabilities
}

// Description returns the description of this task type
func (f *VacuumTaskFactory) Description() string {
	return f.description
}

// VacuumTaskImpl implements vacuum operation task
type VacuumTaskImpl struct {
	taskType  types.TaskType
	server    string
	volumeID  uint32
	progress  float64
	cancelled bool
	mutex     sync.RWMutex
}

// Type returns the task type
func (t *VacuumTaskImpl) Type() types.TaskType {
	return t.taskType
}

// Execute executes the vacuum task
func (t *VacuumTaskImpl) Execute(params types.TaskParams) error {
	glog.Infof("Starting vacuum task for volume %d on server %s", t.volumeID, t.server)

	// Simulate vacuum operation with progress updates
	steps := []struct {
		name     string
		duration time.Duration
		progress float64
	}{
		{"Analyzing volume", 2 * time.Second, 10},
		{"Identifying garbage", 3 * time.Second, 30},
		{"Compacting data", 8 * time.Second, 70},
		{"Updating metadata", 2 * time.Second, 90},
		{"Finalizing", 1 * time.Second, 100},
	}

	for _, step := range steps {
		if t.IsCancelled() {
			return fmt.Errorf("task cancelled during %s", step.name)
		}

		glog.V(2).Infof("Vacuum task %s: %s", t.Type(), step.name)
		time.Sleep(step.duration)
		t.SetProgress(step.progress)
	}

	glog.Infof("Vacuum task completed for volume %d on server %s", t.volumeID, t.server)
	return nil
}

// Validate validates the task parameters
func (t *VacuumTaskImpl) Validate(params types.TaskParams) error {
	if params.VolumeID == 0 {
		return fmt.Errorf("volume_id is required")
	}
	if params.Server == "" {
		return fmt.Errorf("server is required")
	}
	return nil
}

// EstimateTime estimates the time needed for the task
func (t *VacuumTaskImpl) EstimateTime(params types.TaskParams) time.Duration {
	return 15 * time.Second
}

// GetProgress returns the current progress (0.0 to 100.0)
func (t *VacuumTaskImpl) GetProgress() float64 {
	t.mutex.RLock()
	defer t.mutex.RUnlock()
	return t.progress
}

// SetProgress sets the current progress
func (t *VacuumTaskImpl) SetProgress(progress float64) {
	t.mutex.Lock()
	defer t.mutex.Unlock()
	if progress < 0 {
		progress = 0
	}
	if progress > 100 {
		progress = 100
	}
	t.progress = progress
}

// Cancel cancels the task
func (t *VacuumTaskImpl) Cancel() error {
	t.mutex.Lock()
	defer t.mutex.Unlock()
	t.cancelled = true
	return nil
}

// IsCancelled returns whether the task is cancelled
func (t *VacuumTaskImpl) IsCancelled() bool {
	t.mutex.RLock()
	defer t.mutex.RUnlock()
	return t.cancelled
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
