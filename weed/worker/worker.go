package worker

import (
	"crypto/rand"
	"fmt"
	"net"
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
	taskLogHandler  *tasks.TaskLogHandler
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

	// Generate new unique worker ID with host information
	hostname, _ := os.Hostname()
	if hostname == "" {
		hostname = "unknown"
	}

	// Get local IP address for better host identification
	var hostIP string
	if addrs, err := net.InterfaceAddrs(); err == nil {
		for _, addr := range addrs {
			if ipnet, ok := addr.(*net.IPNet); ok && !ipnet.IP.IsLoopback() {
				if ipnet.IP.To4() != nil {
					hostIP = ipnet.IP.String()
					break
				}
			}
		}
	}
	if hostIP == "" {
		hostIP = "noip"
	}

	// Create host identifier combining hostname and IP
	hostID := fmt.Sprintf("%s@%s", hostname, hostIP)

	// Generate random component for uniqueness
	randomBytes := make([]byte, 4)
	var workerID string
	if _, err := rand.Read(randomBytes); err != nil {
		// Fallback to timestamp if crypto/rand fails
		workerID = fmt.Sprintf("worker-%s-%d", hostID, time.Now().Unix())
		glog.Infof("Generated fallback worker ID: %s", workerID)
	} else {
		// Use random bytes + timestamp for uniqueness
		randomHex := fmt.Sprintf("%x", randomBytes)
		timestamp := time.Now().Unix()
		workerID = fmt.Sprintf("worker-%s-%s-%d", hostID, randomHex, timestamp)
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

	// Use the global registry that already has all tasks registered
	registry := tasks.GetGlobalRegistry()

	// Initialize task log handler
	logDir := "/tmp/seaweedfs/task_logs"
	if config.BaseWorkingDir != "" {
		logDir = filepath.Join(config.BaseWorkingDir, "task_logs")
	}
	taskLogHandler := tasks.NewTaskLogHandler(logDir)

	worker := &Worker{
		id:             workerID,
		config:         config,
		registry:       registry,
		currentTasks:   make(map[string]*types.Task),
		stopChan:       make(chan struct{}),
		startTime:      time.Now(),
		taskLogHandler: taskLogHandler,
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

	w.running = true
	w.startTime = time.Now()

	// Prepare worker info for registration
	workerInfo := &types.Worker{
		ID:            w.id,
		Capabilities:  w.config.Capabilities,
		MaxConcurrent: w.config.MaxConcurrent,
		Status:        "active",
		CurrentLoad:   0,
		LastHeartbeat: time.Now(),
	}

	// Register worker info with client first (this stores it for use during connection)
	if err := w.adminClient.RegisterWorker(workerInfo); err != nil {
		glog.V(1).Infof("Worker info stored for registration: %v", err)
		// This is expected if not connected yet
	}

	// Start connection attempt (will register immediately if successful)
	glog.Infof("Worker %s starting, attempting to connect to admin server...", w.id)

	// Try initial connection, but don't fail if it doesn't work immediately
	if err := w.adminClient.Connect(); err != nil {
		glog.Warningf("Initial connection to admin server failed, will keep retrying: %v", err)
		// Don't return error - let the reconnection loop handle it
	}

	// Start worker loops regardless of initial connection status
	// They will handle connection failures gracefully
	go w.heartbeatLoop()
	go w.taskRequestLoop()
	go w.connectionMonitorLoop()
	go w.messageProcessingLoop()

	glog.Infof("Worker %s started (connection attempts will continue in background)", w.id)
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

	// Determine task-specific working directory
	var taskWorkingDir string
	if w.config.BaseWorkingDir != "" {
		taskWorkingDir = fmt.Sprintf("%s/%s", w.config.BaseWorkingDir, string(task.Type))
	}

	// Check if we have typed protobuf parameters
	if task.TypedParams != nil {
		// Use typed task execution
		glog.V(1).Infof("Executing task %s with typed protobuf parameters", task.ID)

		typedRegistry := types.GetGlobalTypedTaskRegistry()
		typedTaskInstance, err := typedRegistry.CreateTypedTask(task.Type)
		if err != nil {
			// Fall back to legacy task execution if typed task not available
			glog.V(1).Infof("Typed task not available for %s, falling back to legacy execution: %v", task.Type, err)
		} else {
			// Initialize logging for typed tasks if they support it
			if loggerProvider, ok := typedTaskInstance.(tasks.LoggerProvider); ok {
				taskParams := types.TaskParams{
					VolumeID:       task.VolumeID,
					Server:         task.Server,
					Collection:     task.Collection,
					WorkingDir:     taskWorkingDir,
					TypedParams:    task.TypedParams,
					GrpcDialOption: w.config.GrpcDialOption,
				}

				if err := loggerProvider.InitializeTaskLogger(task.ID, w.id, taskParams); err != nil {
					glog.Warningf("Failed to initialize task logger for %s: %v", task.ID, err)
				}
			}

			// Set progress callback
			typedTaskInstance.SetProgressCallback(func(progress float64) {
				// Report progress updates
				glog.V(2).Infof("Task %s progress: %.1f%%", task.ID, progress)
			})

			// Execute typed task
			err = typedTaskInstance.ExecuteTyped(task.TypedParams)

			// Report completion
			if err != nil {
				w.completeTask(task.ID, false, err.Error())
				w.tasksFailed++
				glog.Errorf("Worker %s failed to execute typed task %s: %v", w.id, task.ID, err)
			} else {
				w.completeTask(task.ID, true, "")
				w.tasksCompleted++
				glog.Infof("Worker %s completed typed task %s successfully", w.id, task.ID)
			}
			return
		}
	}

	// Legacy task execution path
	glog.V(1).Infof("Executing task %s with legacy parameters", task.ID)
	taskParams := types.TaskParams{
		VolumeID:       task.VolumeID,
		Server:         task.Server,
		Collection:     task.Collection,
		WorkingDir:     taskWorkingDir,
		TypedParams:    task.TypedParams,
		GrpcDialOption: w.config.GrpcDialOption,
	}

	taskInstance, err := w.registry.CreateTask(task.Type, taskParams)
	if err != nil {
		w.completeTask(task.ID, false, fmt.Sprintf("failed to create task: %v", err))
		return
	}

	// Initialize logging for legacy tasks if they support it
	if loggerProvider, ok := taskInstance.(tasks.LoggerProvider); ok {
		if err := loggerProvider.InitializeTaskLogger(task.ID, w.id, taskParams); err != nil {
			glog.Warningf("Failed to initialize task logger for %s: %v", task.ID, err)
		}
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

// registerWorker registers the worker with the admin server
func (w *Worker) registerWorker() {
	workerInfo := &types.Worker{
		ID:            w.id,
		Capabilities:  w.config.Capabilities,
		MaxConcurrent: w.config.MaxConcurrent,
		Status:        "active",
		CurrentLoad:   0,
		LastHeartbeat: time.Now(),
	}

	if err := w.adminClient.RegisterWorker(workerInfo); err != nil {
		glog.Warningf("Failed to register worker (will retry on next heartbeat): %v", err)
	} else {
		glog.Infof("Worker %s registered successfully with admin server", w.id)
	}
}

// connectionMonitorLoop monitors connection status
func (w *Worker) connectionMonitorLoop() {
	ticker := time.NewTicker(30 * time.Second) // Check every 30 seconds
	defer ticker.Stop()

	for {
		select {
		case <-w.stopChan:
			return
		case <-ticker.C:
			// Just monitor connection status - registration is handled automatically
			// by the client's reconnection logic
			if w.adminClient != nil && w.adminClient.IsConnected() {
				glog.V(2).Infof("Worker %s connection status: connected", w.id)
			} else if w.adminClient != nil {
				glog.V(1).Infof("Worker %s connection status: disconnected, reconnection in progress", w.id)
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

// messageProcessingLoop processes incoming admin messages
func (w *Worker) messageProcessingLoop() {
	glog.V(1).Infof("Worker %s message processing loop started", w.id)

	// Get access to the incoming message channel from gRPC client
	grpcClient, ok := w.adminClient.(*GrpcAdminClient)
	if !ok {
		glog.V(1).Infof("Admin client is not gRPC client, message processing not available")
		return
	}

	incomingChan := grpcClient.GetIncomingChannel()

	for {
		select {
		case <-w.stopChan:
			glog.V(1).Infof("Worker %s message processing loop stopping", w.id)
			return
		case message := <-incomingChan:
			w.processAdminMessage(message)
		}
	}
}

// processAdminMessage processes different types of admin messages
func (w *Worker) processAdminMessage(message *worker_pb.AdminMessage) {
	glog.V(2).Infof("Worker %s received admin message: %T", w.id, message.Message)

	switch msg := message.Message.(type) {
	case *worker_pb.AdminMessage_TaskLogRequest:
		w.handleTaskLogRequest(msg.TaskLogRequest)
	case *worker_pb.AdminMessage_TaskAssignment:
		// Task assignments are already handled by the task request loop
		glog.V(2).Infof("Task assignment message received (handled elsewhere)")
	case *worker_pb.AdminMessage_TaskCancellation:
		w.handleTaskCancellation(msg.TaskCancellation)
	case *worker_pb.AdminMessage_AdminShutdown:
		w.handleAdminShutdown(msg.AdminShutdown)
	default:
		glog.V(1).Infof("Worker %s received unknown admin message type: %T", w.id, message.Message)
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

	grpcClient, ok := w.adminClient.(*GrpcAdminClient)
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

	w.mutex.Lock()
	defer w.mutex.Unlock()

	if task, exists := w.currentTasks[cancellation.TaskId]; exists {
		// TODO: Implement task cancellation logic
		glog.Infof("Cancelling task %s", task.ID)
	} else {
		glog.Warningf("Cannot cancel task %s: task not found", cancellation.TaskId)
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
