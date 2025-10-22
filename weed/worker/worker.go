package worker

import (
	"context"
	"crypto/rand"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/worker_pb"
	"github.com/seaweedfs/seaweedfs/weed/worker/tasks"
	"github.com/seaweedfs/seaweedfs/weed/worker/types"

	// Import task packages to trigger their auto-registration
	_ "github.com/seaweedfs/seaweedfs/weed/worker/tasks/balance"
	_ "github.com/seaweedfs/seaweedfs/weed/worker/tasks/erasure_coding"
	_ "github.com/seaweedfs/seaweedfs/weed/worker/tasks/vacuum"
)

// Worker represents a maintenance worker instance
type Worker struct {
	id             string
	config         *types.WorkerConfig
	registry       *tasks.TaskRegistry
	cmds           chan workerCommand
	state          *workerState
	taskLogHandler *tasks.TaskLogHandler
	closeOnce      sync.Once
}
type workerState struct {
	running         bool
	adminClient     AdminClient
	startTime       time.Time
	stopChan        chan struct{}
	heartbeatTicker *time.Ticker
	requestTicker   *time.Ticker
	currentTasks    map[string]*types.TaskInput
	tasksCompleted  int
	tasksFailed     int
}

type workerAction string

const (
	ActionStart             workerAction = "start"
	ActionStop              workerAction = "stop"
	ActionGetStatus         workerAction = "getstatus"
	ActionGetTaskLoad       workerAction = "getload"
	ActionSetTask           workerAction = "settask"
	ActionSetAdmin          workerAction = "setadmin"
	ActionRemoveTask        workerAction = "removetask"
	ActionGetAdmin          workerAction = "getadmin"
	ActionIncTaskFail       workerAction = "inctaskfail"
	ActionIncTaskComplete   workerAction = "inctaskcomplete"
	ActionGetHbTick         workerAction = "gethbtick"
	ActionGetReqTick        workerAction = "getreqtick"
	ActionGetStopChan       workerAction = "getstopchan"
	ActionSetHbTick         workerAction = "sethbtick"
	ActionSetReqTick        workerAction = "setreqtick"
	ActionGetStartTime      workerAction = "getstarttime"
	ActionGetCompletedTasks workerAction = "getcompletedtasks"
	ActionGetFailedTasks    workerAction = "getfailedtasks"
	ActionCancelTask        workerAction = "canceltask"
	// ... other worker actions like Stop, Status, etc.
)

type statusResponse chan types.WorkerStatus
type workerCommand struct {
	action workerAction
	data   any
	resp   chan error // for reporting success/failure
}

// AdminClient defines the interface for communicating with the admin server
type AdminClient interface {
	Connect() error
	Disconnect() error
	RegisterWorker(worker *types.WorkerData) error
	SendHeartbeat(workerID string, status *types.WorkerStatus) error
	RequestTask(workerID string, capabilities []types.TaskType) (*types.TaskInput, error)
	CompleteTask(taskID string, success bool, errorMsg string) error
	CompleteTaskWithMetadata(taskID string, success bool, errorMsg string, metadata map[string]string) error
	UpdateTaskProgress(taskID string, progress float64) error
	IsConnected() bool
}

// GenerateOrLoadWorkerID generates a unique worker ID or loads existing one from working directory
func GenerateOrLoadWorkerID(workingDir string) (string, error) {
	const workerIDFile = "worker.id"

	var idFilePath string
	if workingDir != "" {
		idFilePath = filepath.Join(workingDir, workerIDFile)
	} else {
		// Use current working directory if none specified
		wd, err := os.Getwd()
		if err != nil {
			return "", fmt.Errorf("failed to get working directory: %w", err)
		}
		idFilePath = filepath.Join(wd, workerIDFile)
	}

	// Try to read existing worker ID
	if data, err := os.ReadFile(idFilePath); err == nil {
		workerID := strings.TrimSpace(string(data))
		if workerID != "" {
			glog.Infof("Loaded existing worker ID from %s: %s", idFilePath, workerID)
			return workerID, nil
		}
	}

	// Generate simplified worker ID
	hostname, _ := os.Hostname()
	if hostname == "" {
		hostname = "unknown"
	}

	// Use short hostname - take first 6 chars or last part after dots
	shortHostname := hostname
	if len(hostname) > 6 {
		if parts := strings.Split(hostname, "."); len(parts) > 1 {
			// Use last part before domain (e.g., "worker1" from "worker1.example.com")
			shortHostname = parts[0]
			if len(shortHostname) > 6 {
				shortHostname = shortHostname[:6]
			}
		} else {
			// Use first 6 characters
			shortHostname = hostname[:6]
		}
	}

	// Generate random component for uniqueness (2 bytes = 4 hex chars)
	randomBytes := make([]byte, 2)
	var workerID string
	if _, err := rand.Read(randomBytes); err != nil {
		// Fallback to short timestamp if crypto/rand fails
		timestamp := time.Now().Unix() % 10000 // last 4 digits
		workerID = fmt.Sprintf("w-%s-%04d", shortHostname, timestamp)
		glog.Infof("Generated fallback worker ID: %s", workerID)
	} else {
		// Use random hex for uniqueness
		randomHex := fmt.Sprintf("%x", randomBytes)
		workerID = fmt.Sprintf("w-%s-%s", shortHostname, randomHex)
		glog.Infof("Generated new worker ID: %s", workerID)
	}

	// Save worker ID to file
	if err := os.WriteFile(idFilePath, []byte(workerID), 0644); err != nil {
		glog.Warningf("Failed to save worker ID to %s: %v", idFilePath, err)
	} else {
		glog.Infof("Saved worker ID to %s", idFilePath)
	}

	return workerID, nil
}

// NewWorker creates a new worker instance
func NewWorker(config *types.WorkerConfig) (*Worker, error) {
	if config == nil {
		config = types.DefaultWorkerConfig()
	}

	// Generate or load persistent worker ID
	workerID, err := GenerateOrLoadWorkerID(config.BaseWorkingDir)
	if err != nil {
		return nil, fmt.Errorf("failed to generate or load worker ID: %w", err)
	}

	// Use the global unified registry that already has all tasks registered
	registry := tasks.GetGlobalTaskRegistry()

	// Initialize task log handler
	logDir := filepath.Join(config.BaseWorkingDir, "task_logs")
	// Ensure the base task log directory exists to avoid errors when admin requests logs
	if err := os.MkdirAll(logDir, 0755); err != nil {
		glog.Warningf("Failed to create task log base directory %s: %v", logDir, err)
	}
	taskLogHandler := tasks.NewTaskLogHandler(logDir)

	worker := &Worker{
		id:             workerID,
		config:         config,
		registry:       registry,
		taskLogHandler: taskLogHandler,
		cmds:           make(chan workerCommand),
	}

	glog.V(1).Infof("Worker created with %d registered task types", len(registry.GetAll()))
	go worker.managerLoop()
	return worker, nil
}

func (w *Worker) managerLoop() {
	w.state = &workerState{
		startTime:    time.Now(),
		stopChan:     make(chan struct{}),
		currentTasks: make(map[string]*types.TaskInput),
	}

	for cmd := range w.cmds {
		switch cmd.action {
		case ActionStart:
			w.handleStart(cmd)
		case ActionStop:
			w.handleStop(cmd)
		case ActionGetStatus:
			respCh := cmd.data.(statusResponse)
			var currentTasks []types.TaskInput
			for _, task := range w.state.currentTasks {
				currentTasks = append(currentTasks, *task)
			}

			statusStr := "active"
			if len(w.state.currentTasks) >= w.config.MaxConcurrent {
				statusStr = "busy"
			}

			status := types.WorkerStatus{
				WorkerID:       w.id,
				Status:         statusStr,
				Capabilities:   w.config.Capabilities,
				MaxConcurrent:  w.config.MaxConcurrent,
				CurrentLoad:    len(w.state.currentTasks),
				LastHeartbeat:  time.Now(),
				CurrentTasks:   currentTasks,
				Uptime:         time.Since(w.state.startTime),
				TasksCompleted: w.state.tasksCompleted,
				TasksFailed:    w.state.tasksFailed,
			}
			respCh <- status
		case ActionGetTaskLoad:
			respCh := cmd.data.(chan int)
			respCh <- len(w.state.currentTasks)
		case ActionSetTask:
			currentLoad := len(w.state.currentTasks)
			if currentLoad >= w.config.MaxConcurrent {
				cmd.resp <- fmt.Errorf("worker is at capacity")
			}
			task := cmd.data.(*types.TaskInput)
			w.state.currentTasks[task.ID] = task
			cmd.resp <- nil
		case ActionSetAdmin:
			admin := cmd.data.(AdminClient)
			w.state.adminClient = admin
		case ActionRemoveTask:
			taskID := cmd.data.(string)
			delete(w.state.currentTasks, taskID)
		case ActionGetAdmin:
			respCh := cmd.data.(chan AdminClient)
			respCh <- w.state.adminClient
		case ActionIncTaskFail:
			w.state.tasksFailed++
		case ActionIncTaskComplete:
			w.state.tasksCompleted++
		case ActionGetHbTick:
			respCh := cmd.data.(chan *time.Ticker)
			respCh <- w.state.heartbeatTicker
		case ActionGetReqTick:
			respCh := cmd.data.(chan *time.Ticker)
			respCh <- w.state.requestTicker
		case ActionSetHbTick:
			w.state.heartbeatTicker = cmd.data.(*time.Ticker)
		case ActionSetReqTick:
			w.state.requestTicker = cmd.data.(*time.Ticker)
		case ActionGetStopChan:
			cmd.data.(chan chan struct{}) <- w.state.stopChan
		case ActionGetStartTime:
			cmd.data.(chan time.Time) <- w.state.startTime
		case ActionGetCompletedTasks:
			cmd.data.(chan int) <- w.state.tasksCompleted
		case ActionGetFailedTasks:
			cmd.data.(chan int) <- w.state.tasksFailed
		case ActionCancelTask:
			taskID := cmd.data.(string)
			if task, exists := w.state.currentTasks[taskID]; exists {
				glog.Infof("Cancelling task %s", task.ID)
				// TODO: Implement actual task cancellation logic
			} else {
				glog.Warningf("Cannot cancel task %s: task not found", taskID)
			}

		}
	}
}

func (w *Worker) getTaskLoad() int {
	respCh := make(chan int, 1)
	w.cmds <- workerCommand{
		action: ActionGetTaskLoad,
		data:   respCh,
		resp:   nil,
	}
	return <-respCh
}

func (w *Worker) setTask(task *types.TaskInput) error {
	resp := make(chan error)
	w.cmds <- workerCommand{
		action: ActionSetTask,
		data:   task,
		resp:   resp,
	}
	if err := <-resp; err != nil {
		glog.Errorf("TASK REJECTED: Worker %s at capacity (%d/%d) - rejecting task %s",
			w.id, w.getTaskLoad(), w.config.MaxConcurrent, task.ID)
		return err
	}
	newLoad := w.getTaskLoad()

	glog.Infof("TASK ACCEPTED: Worker %s accepted task %s - current load: %d/%d",
		w.id, task.ID, newLoad, w.config.MaxConcurrent)
	return nil
}

func (w *Worker) removeTask(task *types.TaskInput) int {
	w.cmds <- workerCommand{
		action: ActionRemoveTask,
		data:   task.ID,
	}
	return w.getTaskLoad()
}

func (w *Worker) getAdmin() AdminClient {
	respCh := make(chan AdminClient, 1)
	w.cmds <- workerCommand{
		action: ActionGetAdmin,
		data:   respCh,
	}
	return <-respCh
}

func (w *Worker) getStopChan() chan struct{} {
	respCh := make(chan chan struct{}, 1)
	w.cmds <- workerCommand{
		action: ActionGetStopChan,
		data:   respCh,
	}
	return <-respCh
}

func (w *Worker) getHbTick() *time.Ticker {
	respCh := make(chan *time.Ticker, 1)
	w.cmds <- workerCommand{
		action: ActionGetHbTick,
		data:   respCh,
	}
	return <-respCh
}

func (w *Worker) getReqTick() *time.Ticker {
	respCh := make(chan *time.Ticker, 1)
	w.cmds <- workerCommand{
		action: ActionGetReqTick,
		data:   respCh,
	}
	return <-respCh
}

func (w *Worker) setHbTick(tick *time.Ticker) *time.Ticker {
	w.cmds <- workerCommand{
		action: ActionSetHbTick,
		data:   tick,
	}
	return w.getHbTick()
}

func (w *Worker) setReqTick(tick *time.Ticker) *time.Ticker {
	w.cmds <- workerCommand{
		action: ActionSetReqTick,
		data:   tick,
	}
	return w.getReqTick()
}

func (w *Worker) getStartTime() time.Time {
	respCh := make(chan time.Time, 1)
	w.cmds <- workerCommand{
		action: ActionGetStartTime,
		data:   respCh,
	}
	return <-respCh
}
func (w *Worker) getCompletedTasks() int {
	respCh := make(chan int, 1)
	w.cmds <- workerCommand{
		action: ActionGetCompletedTasks,
		data:   respCh,
	}
	return <-respCh
}
func (w *Worker) getFailedTasks() int {
	respCh := make(chan int, 1)
	w.cmds <- workerCommand{
		action: ActionGetFailedTasks,
		data:   respCh,
	}
	return <-respCh
}

// getTaskLoggerConfig returns the task logger configuration with worker's log directory
func (w *Worker) getTaskLoggerConfig() tasks.TaskLoggerConfig {
	config := tasks.DefaultTaskLoggerConfig()

	// Use worker's configured log directory (BaseWorkingDir is guaranteed to be non-empty)
	logDir := filepath.Join(w.config.BaseWorkingDir, "task_logs")
	config.BaseLogDir = logDir

	return config
}

// ID returns the worker ID
func (w *Worker) ID() string {
	return w.id
}

func (w *Worker) Start() error {
	resp := make(chan error)
	w.cmds <- workerCommand{
		action: ActionStart,
		resp:   resp,
	}
	return <-resp
}

// Start starts the worker
func (w *Worker) handleStart(cmd workerCommand) {
	if w.state.running {
		cmd.resp <- fmt.Errorf("worker is already running")
		return
	}

	if w.state.adminClient == nil {
		cmd.resp <- fmt.Errorf("admin client is not set")
		return
	}

	w.state.running = true
	w.state.startTime = time.Now()

	// Prepare worker info for registration
	workerInfo := &types.WorkerData{
		ID:            w.id,
		Capabilities:  w.config.Capabilities,
		MaxConcurrent: w.config.MaxConcurrent,
		Status:        "active",
		CurrentLoad:   0,
		LastHeartbeat: time.Now(),
	}

	// Register worker info with client first (this stores it for use during connection)
	if err := w.state.adminClient.RegisterWorker(workerInfo); err != nil {
		glog.V(1).Infof("Worker info stored for registration: %v", err)
		// This is expected if not connected yet
	}

	// Start connection attempt (will register immediately if successful)
	glog.Infof("WORKER STARTING: Worker %s starting with capabilities %v, max concurrent: %d",
		w.id, w.config.Capabilities, w.config.MaxConcurrent)

	// Try initial connection, but don't fail if it doesn't work immediately
	if err := w.state.adminClient.Connect(); err != nil {
		glog.Warningf("INITIAL CONNECTION FAILED: Worker %s initial connection to admin server failed, will keep retrying: %v", w.id, err)
		// Don't return error - let the reconnection loop handle it
	} else {
		glog.Infof("INITIAL CONNECTION SUCCESS: Worker %s successfully connected to admin server", w.id)
	}

	// Start worker loops regardless of initial connection status
	// They will handle connection failures gracefully
	glog.V(1).Infof("STARTING LOOPS: Worker %s starting background loops", w.id)
	go w.heartbeatLoop()
	go w.taskRequestLoop()
	go w.connectionMonitorLoop()
	go w.messageProcessingLoop()

	glog.Infof("WORKER STARTED: Worker %s started successfully (connection attempts will continue in background)", w.id)
	cmd.resp <- nil
}

func (w *Worker) Stop() error {
	resp := make(chan error)
	w.cmds <- workerCommand{
		action: ActionStop,
		resp:   resp,
	}
	if err := <-resp; err != nil {
		return err
	}

	// Wait for tasks to finish
	timeout := time.NewTimer(30 * time.Second)
	defer timeout.Stop()
	for w.getTaskLoad() > 0 {
		select {
		case <-timeout.C:
			glog.Warningf("Worker %s stopping with %d tasks still running", w.id, w.getTaskLoad())
			goto end_wait
		case <-time.After(100 * time.Millisecond):
		}
	}
end_wait:

	// Disconnect from admin server
	if adminClient := w.getAdmin(); adminClient != nil {
		if err := adminClient.Disconnect(); err != nil {
			glog.Errorf("Error disconnecting from admin server: %v", err)
		}
	}

	w.closeOnce.Do(func() {
		close(w.cmds)
	})
	glog.Infof("Worker %s stopped", w.id)
	return nil
}

// Stop stops the worker
func (w *Worker) handleStop(cmd workerCommand) {
	if !w.state.running {
		cmd.resp <- nil
		return
	}

	w.state.running = false
	close(w.state.stopChan)

	// Stop tickers
	if w.state.heartbeatTicker != nil {
		w.state.heartbeatTicker.Stop()
	}
	if w.state.requestTicker != nil {
		w.state.requestTicker.Stop()
	}

	cmd.resp <- nil
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
	respCh := make(statusResponse, 1)
	w.cmds <- workerCommand{
		action: ActionGetStatus,
		data:   respCh,
		resp:   nil,
	}
	return <-respCh
}

// HandleTask handles a task execution
func (w *Worker) HandleTask(task *types.TaskInput) error {
	glog.V(1).Infof("Worker %s received task %s (type: %s, volume: %d)",
		w.id, task.ID, task.Type, task.VolumeID)

	if err := w.setTask(task); err != nil {
		return err
	}

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
	w.cmds <- workerCommand{
		action: ActionSetAdmin,
		data:   client,
	}
}

// executeTask executes a task
func (w *Worker) executeTask(task *types.TaskInput) {
	startTime := time.Now()

	defer func() {
		currentLoad := w.removeTask(task)

		duration := time.Since(startTime)
		glog.Infof("TASK EXECUTION FINISHED: Worker %s finished executing task %s after %v - current load: %d/%d",
			w.id, task.ID, duration, currentLoad, w.config.MaxConcurrent)
	}()

	glog.Infof("TASK EXECUTION STARTED: Worker %s starting execution of task %s (type: %s, volume: %d, server: %s, collection: %s) at %v",
		w.id, task.ID, task.Type, task.VolumeID, task.Server, task.Collection, startTime.Format(time.RFC3339))

	// Report task start to admin server
	if err := w.getAdmin().UpdateTaskProgress(task.ID, 0.0); err != nil {
		glog.V(1).Infof("Failed to report task start to admin: %v", err)
	}

	// Determine task-specific working directory (BaseWorkingDir is guaranteed to be non-empty)
	taskWorkingDir := filepath.Join(w.config.BaseWorkingDir, string(task.Type))
	glog.V(2).Infof("WORKING DIRECTORY: Task %s using working directory: %s", task.ID, taskWorkingDir)

	// Check if we have typed protobuf parameters
	if task.TypedParams == nil {
		w.completeTask(task.ID, false, "task has no typed parameters - task was not properly planned")
		glog.Errorf("Worker %s rejecting task %s: no typed parameters", w.id, task.ID)
		return
	}

	// Use new task execution system with unified Task interface
	glog.V(1).Infof("Executing task %s with typed protobuf parameters", task.ID)

	// Initialize a file-based task logger so admin can retrieve logs
	// Build minimal params for logger metadata
	loggerParams := types.TaskParams{
		VolumeID:    task.VolumeID,
		Collection:  task.Collection,
		TypedParams: task.TypedParams,
	}
	loggerConfig := w.getTaskLoggerConfig()
	fileLogger, logErr := tasks.NewTaskLogger(task.ID, task.Type, w.id, loggerParams, loggerConfig)
	if logErr != nil {
		glog.Warningf("Failed to initialize file logger for task %s: %v", task.ID, logErr)
	} else {
		defer func() {
			if err := fileLogger.Close(); err != nil {
				glog.V(1).Infof("Failed to close task logger for %s: %v", task.ID, err)
			}
		}()
		fileLogger.Info("Task %s started (type=%s, server=%s, collection=%s)", task.ID, task.Type, task.Server, task.Collection)
	}

	taskFactory := w.registry.Get(task.Type)
	if taskFactory == nil {
		w.completeTask(task.ID, false, fmt.Sprintf("task factory not available for %s: task type not found", task.Type))
		glog.Errorf("Worker %s failed to get task factory for %s type %v", w.id, task.ID, task.Type)

		// Log supported task types for debugging
		allFactories := w.registry.GetAll()
		glog.Errorf("Available task types: %d", len(allFactories))
		for taskType := range allFactories {
			glog.Errorf("Supported task type: %v", taskType)
		}
		return
	}

	taskInstance, err := taskFactory.Create(task.TypedParams)
	if err != nil {
		w.completeTask(task.ID, false, fmt.Sprintf("failed to create task for %s: %v", task.Type, err))
		glog.Errorf("Worker %s failed to create task %s type %v: %v", w.id, task.ID, task.Type, err)
		return
	}

	// Task execution uses the new unified Task interface
	glog.V(2).Infof("Executing task %s in working directory: %s", task.ID, taskWorkingDir)

	// If we have a file logger, adapt it so task WithFields logs are captured into file
	if fileLogger != nil {
		if withLogger, ok := taskInstance.(interface{ SetLogger(types.Logger) }); ok {
			withLogger.SetLogger(newTaskLoggerAdapter(fileLogger))
		}
	}

	// Set progress callback that reports to admin server
	taskInstance.SetProgressCallback(func(progress float64, stage string) {
		// Report progress updates to admin server
		glog.V(2).Infof("Task %s progress: %.1f%% - %s", task.ID, progress, stage)
		if err := w.getAdmin().UpdateTaskProgress(task.ID, progress); err != nil {
			glog.V(1).Infof("Failed to report task progress to admin: %v", err)
		}
		if fileLogger != nil {
			// Use meaningful stage description or fallback to generic message
			message := stage
			if message == "" {
				message = fmt.Sprintf("Progress: %.1f%%", progress)
			}
			fileLogger.LogProgress(progress, message)
		}
	})

	// Execute task with context
	ctx := context.Background()
	err = taskInstance.Execute(ctx, task.TypedParams)

	// Report completion
	if err != nil {
		w.completeTask(task.ID, false, err.Error())
		w.cmds <- workerCommand{
			action: ActionIncTaskFail,
		}
		glog.Errorf("Worker %s failed to execute task %s: %v", w.id, task.ID, err)
		if fileLogger != nil {
			fileLogger.LogStatus("failed", err.Error())
			fileLogger.Error("Task %s failed: %v", task.ID, err)
		}
	} else {
		w.completeTask(task.ID, true, "")
		w.cmds <- workerCommand{
			action: ActionIncTaskComplete,
		}
		glog.Infof("Worker %s completed task %s successfully", w.id, task.ID)
		if fileLogger != nil {
			fileLogger.Info("Task %s completed successfully", task.ID)
		}
	}
	return
}

// completeTask reports task completion to admin server
func (w *Worker) completeTask(taskID string, success bool, errorMsg string) {
	if w.getAdmin() != nil {
		if err := w.getAdmin().CompleteTask(taskID, success, errorMsg); err != nil {
			glog.Errorf("Failed to report task completion: %v", err)
		}
	}
}

// heartbeatLoop sends periodic heartbeats to the admin server
func (w *Worker) heartbeatLoop() {
	defer w.setHbTick(time.NewTicker(w.config.HeartbeatInterval)).Stop()
	ticker := w.getHbTick()
	stopChan := w.getStopChan()
	for {
		select {
		case <-stopChan:
			return
		case <-ticker.C:
			w.sendHeartbeat()
		}
	}
}

// taskRequestLoop periodically requests new tasks from the admin server
func (w *Worker) taskRequestLoop() {
	defer w.setReqTick(time.NewTicker(w.config.TaskRequestInterval)).Stop()
	ticker := w.getReqTick()
	stopChan := w.getStopChan()
	for {
		select {
		case <-stopChan:
			return
		case <-ticker.C:
			w.requestTasks()
		}
	}
}

// sendHeartbeat sends heartbeat to admin server
func (w *Worker) sendHeartbeat() {
	if w.getAdmin() != nil {
		if err := w.getAdmin().SendHeartbeat(w.id, &types.WorkerStatus{
			WorkerID:      w.id,
			Status:        "active",
			Capabilities:  w.config.Capabilities,
			MaxConcurrent: w.config.MaxConcurrent,
			CurrentLoad:   w.getTaskLoad(),
			LastHeartbeat: time.Now(),
		}); err != nil {
			glog.Warningf("Failed to send heartbeat: %v", err)
		}
	}
}

// requestTasks requests new tasks from the admin server
func (w *Worker) requestTasks() {
	currentLoad := w.getTaskLoad()

	if currentLoad >= w.config.MaxConcurrent {
		glog.V(3).Infof("TASK REQUEST SKIPPED: Worker %s at capacity (%d/%d)",
			w.id, currentLoad, w.config.MaxConcurrent)
		return // Already at capacity
	}

	if w.getAdmin() != nil {
		glog.V(3).Infof("REQUESTING TASK: Worker %s requesting task from admin server (current load: %d/%d, capabilities: %v)",
			w.id, currentLoad, w.config.MaxConcurrent, w.config.Capabilities)

		task, err := w.getAdmin().RequestTask(w.id, w.config.Capabilities)
		if err != nil {
			glog.V(2).Infof("TASK REQUEST FAILED: Worker %s failed to request task: %v", w.id, err)
			return
		}

		if task != nil {
			glog.Infof("TASK RESPONSE RECEIVED: Worker %s received task from admin server - ID: %s, Type: %s",
				w.id, task.ID, task.Type)
			if err := w.HandleTask(task); err != nil {
				glog.Errorf("TASK HANDLING FAILED: Worker %s failed to handle task %s: %v", w.id, task.ID, err)
			}
		} else {
			glog.V(3).Infof("NO TASK AVAILABLE: Worker %s - admin server has no tasks available", w.id)
		}
	}
}

// GetTaskRegistry returns the task registry
func (w *Worker) GetTaskRegistry() *tasks.TaskRegistry {
	return w.registry
}

// registerWorker registers the worker with the admin server
func (w *Worker) registerWorker() {
	workerInfo := &types.WorkerData{
		ID:            w.id,
		Capabilities:  w.config.Capabilities,
		MaxConcurrent: w.config.MaxConcurrent,
		Status:        "active",
		CurrentLoad:   0,
		LastHeartbeat: time.Now(),
	}

	if err := w.getAdmin().RegisterWorker(workerInfo); err != nil {
		glog.Warningf("Failed to register worker (will retry on next heartbeat): %v", err)
	} else {
		glog.Infof("Worker %s registered successfully with admin server", w.id)
	}
}

// connectionMonitorLoop monitors connection status
func (w *Worker) connectionMonitorLoop() {
	ticker := time.NewTicker(30 * time.Second) // Check every 30 seconds
	defer ticker.Stop()

	lastConnectionStatus := false
	stopChan := w.getStopChan()
	for {
		select {
		case <-stopChan:
			glog.V(1).Infof("CONNECTION MONITOR STOPPING: Worker %s connection monitor loop stopping", w.id)
			return
		case <-ticker.C:
			// Monitor connection status and log changes
			currentConnectionStatus := w.getAdmin() != nil && w.getAdmin().IsConnected()

			if currentConnectionStatus != lastConnectionStatus {
				if currentConnectionStatus {
					glog.Infof("CONNECTION RESTORED: Worker %s connection status changed: connected", w.id)
				} else {
					glog.Warningf("CONNECTION LOST: Worker %s connection status changed: disconnected", w.id)
				}
				lastConnectionStatus = currentConnectionStatus
			} else {
				if currentConnectionStatus {
					glog.V(3).Infof("CONNECTION OK: Worker %s connection status: connected", w.id)
				} else {
					glog.V(1).Infof("CONNECTION DOWN: Worker %s connection status: disconnected, reconnection in progress", w.id)
				}
			}
		}
	}
}

// GetConfig returns the worker configuration
func (w *Worker) GetConfig() *types.WorkerConfig {
	return w.config
}

// GetPerformanceMetrics returns performance metrics
func (w *Worker) GetPerformanceMetrics() *types.WorkerPerformance {

	uptime := time.Since(w.getStartTime())
	var successRate float64
	totalTasks := w.getCompletedTasks() + w.getFailedTasks()
	if totalTasks > 0 {
		successRate = float64(w.getCompletedTasks()) / float64(totalTasks) * 100
	}

	return &types.WorkerPerformance{
		TasksCompleted:  w.getCompletedTasks(),
		TasksFailed:     w.getFailedTasks(),
		AverageTaskTime: 0, // Would need to track this
		Uptime:          uptime,
		SuccessRate:     successRate,
	}
}

// messageProcessingLoop processes incoming admin messages
func (w *Worker) messageProcessingLoop() {
	glog.Infof("MESSAGE LOOP STARTED: Worker %s message processing loop started", w.id)

	// Get access to the incoming message channel from gRPC client
	grpcClient, ok := w.getAdmin().(*GrpcAdminClient)
	if !ok {
		glog.Warningf("MESSAGE LOOP UNAVAILABLE: Worker %s admin client is not gRPC client, message processing not available", w.id)
		return
	}

	incomingChan := grpcClient.GetIncomingChannel()
	glog.V(1).Infof("MESSAGE CHANNEL READY: Worker %s connected to incoming message channel", w.id)
	stopChan := w.getStopChan()
	for {
		select {
		case <-stopChan:
			glog.Infof("MESSAGE LOOP STOPPING: Worker %s message processing loop stopping", w.id)
			return
		case message := <-incomingChan:
			if message != nil {
				glog.V(3).Infof("MESSAGE PROCESSING: Worker %s processing incoming message", w.id)
				w.processAdminMessage(message)
			} else {
				glog.V(3).Infof("NULL MESSAGE: Worker %s received nil message", w.id)
			}
		}
	}
}

// processAdminMessage processes different types of admin messages
func (w *Worker) processAdminMessage(message *worker_pb.AdminMessage) {
	glog.V(4).Infof("ADMIN MESSAGE RECEIVED: Worker %s received admin message: %T", w.id, message.Message)

	switch msg := message.Message.(type) {
	case *worker_pb.AdminMessage_RegistrationResponse:
		glog.V(2).Infof("REGISTRATION RESPONSE: Worker %s received registration response", w.id)
		w.handleRegistrationResponse(msg.RegistrationResponse)
	case *worker_pb.AdminMessage_HeartbeatResponse:
		glog.V(3).Infof("HEARTBEAT RESPONSE: Worker %s received heartbeat response", w.id)
		w.handleHeartbeatResponse(msg.HeartbeatResponse)
	case *worker_pb.AdminMessage_TaskLogRequest:
		glog.V(1).Infof("TASK LOG REQUEST: Worker %s received task log request for task %s", w.id, msg.TaskLogRequest.TaskId)
		w.handleTaskLogRequest(msg.TaskLogRequest)
	case *worker_pb.AdminMessage_TaskAssignment:
		taskAssign := msg.TaskAssignment
		glog.V(1).Infof("Worker %s received direct task assignment %s (type: %s, volume: %d)",
			w.id, taskAssign.TaskId, taskAssign.TaskType, taskAssign.Params.VolumeId)

		// Convert to task and handle it
		task := &types.TaskInput{
			ID:          taskAssign.TaskId,
			Type:        types.TaskType(taskAssign.TaskType),
			Status:      types.TaskStatusAssigned,
			VolumeID:    taskAssign.Params.VolumeId,
			Server:      getServerFromParams(taskAssign.Params),
			Collection:  taskAssign.Params.Collection,
			Priority:    types.TaskPriority(taskAssign.Priority),
			CreatedAt:   time.Unix(taskAssign.CreatedTime, 0),
			TypedParams: taskAssign.Params,
		}

		if err := w.HandleTask(task); err != nil {
			glog.Errorf("DIRECT TASK ASSIGNMENT FAILED: Worker %s failed to handle direct task assignment %s: %v", w.id, task.ID, err)
		}
	case *worker_pb.AdminMessage_TaskCancellation:
		glog.Infof("TASK CANCELLATION: Worker %s received task cancellation for task %s", w.id, msg.TaskCancellation.TaskId)
		w.handleTaskCancellation(msg.TaskCancellation)
	case *worker_pb.AdminMessage_AdminShutdown:
		glog.Infof("ADMIN SHUTDOWN: Worker %s received admin shutdown message", w.id)
		w.handleAdminShutdown(msg.AdminShutdown)
	default:
		glog.V(1).Infof("UNKNOWN MESSAGE: Worker %s received unknown admin message type: %T", w.id, message.Message)
	}
}

// handleTaskLogRequest processes task log requests from admin server
func (w *Worker) handleTaskLogRequest(request *worker_pb.TaskLogRequest) {
	glog.V(1).Infof("Worker %s handling task log request for task %s", w.id, request.TaskId)

	// Use the task log handler to process the request
	response := w.taskLogHandler.HandleLogRequest(request)

	// Send response back to admin server
	responseMsg := &worker_pb.WorkerMessage{
		WorkerId:  w.id,
		Timestamp: time.Now().Unix(),
		Message: &worker_pb.WorkerMessage_TaskLogResponse{
			TaskLogResponse: response,
		},
	}

	grpcClient, ok := w.getAdmin().(*GrpcAdminClient)
	if !ok {
		glog.Errorf("Cannot send task log response: admin client is not gRPC client")
		return
	}

	select {
	case grpcClient.outgoing <- responseMsg:
		glog.V(1).Infof("Task log response sent for task %s", request.TaskId)
	case <-time.After(5 * time.Second):
		glog.Errorf("Failed to send task log response for task %s: timeout", request.TaskId)
	}
}

// handleTaskCancellation processes task cancellation requests
func (w *Worker) handleTaskCancellation(cancellation *worker_pb.TaskCancellation) {
	glog.Infof("Worker %s received task cancellation for task %s", w.id, cancellation.TaskId)

	w.cmds <- workerCommand{
		action: ActionCancelTask,
		data:   cancellation.TaskId,
		resp:   nil,
	}
}

// handleAdminShutdown processes admin shutdown notifications
func (w *Worker) handleAdminShutdown(shutdown *worker_pb.AdminShutdown) {
	glog.Infof("Worker %s received admin shutdown notification: %s", w.id, shutdown.Reason)

	gracefulSeconds := shutdown.GracefulShutdownSeconds
	if gracefulSeconds > 0 {
		glog.Infof("Graceful shutdown in %d seconds", gracefulSeconds)
		time.AfterFunc(time.Duration(gracefulSeconds)*time.Second, func() {
			w.Stop()
		})
	} else {
		// Immediate shutdown
		go w.Stop()
	}
}

// handleRegistrationResponse processes registration response from admin server
func (w *Worker) handleRegistrationResponse(response *worker_pb.RegistrationResponse) {
	glog.V(2).Infof("Worker %s processed registration response: success=%v", w.id, response.Success)
	if !response.Success {
		glog.Warningf("Worker %s registration failed: %s", w.id, response.Message)
	}
	// Registration responses are typically handled by the gRPC client during connection setup
	// No additional action needed here
}

// handleHeartbeatResponse processes heartbeat response from admin server
func (w *Worker) handleHeartbeatResponse(response *worker_pb.HeartbeatResponse) {
	glog.V(4).Infof("Worker %s processed heartbeat response", w.id)
	// Heartbeat responses are mainly for keeping the connection alive
	// The admin may include configuration updates or status information in the future
	// For now, just acknowledge receipt
}
