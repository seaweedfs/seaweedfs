package maintenance

import (
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/admin/dash"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/worker/tasks"
	"github.com/seaweedfs/seaweedfs/weed/worker/tasks/balance"
	"github.com/seaweedfs/seaweedfs/weed/worker/tasks/erasure_coding"
	"github.com/seaweedfs/seaweedfs/weed/worker/tasks/remote_upload"
	"github.com/seaweedfs/seaweedfs/weed/worker/tasks/vacuum"
	"github.com/seaweedfs/seaweedfs/weed/worker/types"
)

// MaintenanceWorkerService manages maintenance task execution
// TaskExecutor defines the function signature for task execution
type TaskExecutor func(*MaintenanceWorkerService, *MaintenanceTask) error

// TaskExecutorFactory creates a task executor for a given worker service
type TaskExecutorFactory func() TaskExecutor

// Global registry for task executor factories
var taskExecutorFactories = make(map[MaintenanceTaskType]TaskExecutorFactory)
var executorRegistryMutex sync.RWMutex

// RegisterTaskExecutorFactory registers a factory function for creating task executors
func RegisterTaskExecutorFactory(taskType MaintenanceTaskType, factory TaskExecutorFactory) {
	executorRegistryMutex.Lock()
	defer executorRegistryMutex.Unlock()
	taskExecutorFactories[taskType] = factory
	glog.V(2).Infof("Registered executor factory for task type: %s", taskType)
}

// GetTaskExecutorFactory returns the factory for a task type
func GetTaskExecutorFactory(taskType MaintenanceTaskType) (TaskExecutorFactory, bool) {
	executorRegistryMutex.RLock()
	defer executorRegistryMutex.RUnlock()
	factory, exists := taskExecutorFactories[taskType]
	return factory, exists
}

// GetSupportedExecutorTaskTypes returns all task types with registered executor factories
func GetSupportedExecutorTaskTypes() []MaintenanceTaskType {
	executorRegistryMutex.RLock()
	defer executorRegistryMutex.RUnlock()

	taskTypes := make([]MaintenanceTaskType, 0, len(taskExecutorFactories))
	for taskType := range taskExecutorFactories {
		taskTypes = append(taskTypes, taskType)
	}
	return taskTypes
}

// createGenericTaskExecutor creates a generic task executor that uses the task registry
func createGenericTaskExecutor() TaskExecutor {
	return func(mws *MaintenanceWorkerService, task *MaintenanceTask) error {
		return mws.executeGenericTask(task)
	}
}

// init registers the generic executor factory for all task types
func init() {
	// Register generic executor for all task types
	taskTypes := []MaintenanceTaskType{
		TaskTypeVacuum,
		TaskTypeErasureCoding,
		TaskTypeRemoteUpload,
		TaskTypeFixReplication,
		TaskTypeBalance,
		TaskTypeClusterReplication,
	}

	for _, taskType := range taskTypes {
		RegisterTaskExecutorFactory(taskType, createGenericTaskExecutor)
	}

	glog.V(1).Infof("Registered generic task executor for %d task types", len(taskTypes))
}

type MaintenanceWorkerService struct {
	workerID      string
	address       string
	adminServer   string
	capabilities  []MaintenanceTaskType
	maxConcurrent int
	currentTasks  map[string]*MaintenanceTask
	queue         *MaintenanceQueue
	adminClient   *dash.AdminServer
	running       bool
	stopChan      chan struct{}

	// Task execution registry
	taskExecutors map[MaintenanceTaskType]TaskExecutor

	// Task registry for creating task instances
	taskRegistry *tasks.TaskRegistry
}

// NewMaintenanceWorkerService creates a new maintenance worker service
func NewMaintenanceWorkerService(workerID, address, adminServer string) *MaintenanceWorkerService {
	worker := &MaintenanceWorkerService{
		workerID:      workerID,
		address:       address,
		adminServer:   adminServer,
		capabilities:  []MaintenanceTaskType{TaskTypeVacuum, TaskTypeErasureCoding, TaskTypeRemoteUpload, TaskTypeFixReplication, TaskTypeBalance, TaskTypeClusterReplication},
		maxConcurrent: 2, // Default concurrent task limit
		currentTasks:  make(map[string]*MaintenanceTask),
		stopChan:      make(chan struct{}),
		taskExecutors: make(map[MaintenanceTaskType]TaskExecutor),
		taskRegistry:  tasks.NewTaskRegistry(), // Initialize task registry
	}

	// Initialize task executor registry
	worker.initializeTaskExecutors()

	// Register task types with the registry
	worker.registerTaskTypes()

	return worker
}

// registerTaskTypes registers all supported task types with the task registry
func (mws *MaintenanceWorkerService) registerTaskTypes() {
	// Register all the task types
	vacuum.Register(mws.taskRegistry)
	erasure_coding.Register(mws.taskRegistry)
	remote_upload.Register(mws.taskRegistry)
	balance.Register(mws.taskRegistry)

	glog.V(2).Infof("Registered task types with worker task registry")
}

// executeGenericTask executes a task using the task registry instead of hardcoded methods
func (mws *MaintenanceWorkerService) executeGenericTask(task *MaintenanceTask) error {
	glog.V(2).Infof("Executing generic task %s: %s for volume %d", task.ID, task.Type, task.VolumeID)

	// Convert MaintenanceTask to types.TaskType
	taskType := types.TaskType(string(task.Type))

	// Create task parameters
	taskParams := types.TaskParams{
		VolumeID:   task.VolumeID,
		Server:     task.Server,
		Collection: task.Collection,
		Parameters: task.Parameters,
	}

	// Create task instance using the registry
	taskInstance, err := mws.taskRegistry.CreateTask(taskType, taskParams)
	if err != nil {
		return fmt.Errorf("failed to create task instance: %v", err)
	}

	// Update progress to show task has started
	mws.updateTaskProgress(task.ID, 5)

	// Execute the task
	err = taskInstance.Execute(taskParams)
	if err != nil {
		return fmt.Errorf("task execution failed: %v", err)
	}

	// Update progress to show completion
	mws.updateTaskProgress(task.ID, 100)

	glog.V(2).Infof("Generic task %s completed successfully", task.ID)
	return nil
}

// initializeTaskExecutors sets up the task execution registry dynamically
func (mws *MaintenanceWorkerService) initializeTaskExecutors() {
	mws.taskExecutors = make(map[MaintenanceTaskType]TaskExecutor)

	// Get all registered executor factories and create executors
	executorRegistryMutex.RLock()
	defer executorRegistryMutex.RUnlock()

	for taskType, factory := range taskExecutorFactories {
		executor := factory()
		mws.taskExecutors[taskType] = executor
		glog.V(3).Infof("Initialized executor for task type: %s", taskType)
	}

	glog.V(2).Infof("Initialized %d task executors", len(mws.taskExecutors))
}

// RegisterTaskExecutor allows dynamic registration of new task executors
func (mws *MaintenanceWorkerService) RegisterTaskExecutor(taskType MaintenanceTaskType, executor TaskExecutor) {
	if mws.taskExecutors == nil {
		mws.taskExecutors = make(map[MaintenanceTaskType]TaskExecutor)
	}
	mws.taskExecutors[taskType] = executor
	glog.V(1).Infof("Registered executor for task type: %s", taskType)
}

// GetSupportedTaskTypes returns all task types that this worker can execute
func (mws *MaintenanceWorkerService) GetSupportedTaskTypes() []MaintenanceTaskType {
	return GetSupportedExecutorTaskTypes()
}

// Start begins the worker service
func (mws *MaintenanceWorkerService) Start() error {
	mws.running = true

	// Register with admin server
	worker := &MaintenanceWorker{
		ID:            mws.workerID,
		Address:       mws.address,
		Capabilities:  mws.capabilities,
		MaxConcurrent: mws.maxConcurrent,
	}

	if mws.queue != nil {
		mws.queue.RegisterWorker(worker)
	}

	// Start worker loop
	go mws.workerLoop()

	glog.Infof("Maintenance worker %s started at %s", mws.workerID, mws.address)
	return nil
}

// Stop terminates the worker service
func (mws *MaintenanceWorkerService) Stop() {
	mws.running = false
	close(mws.stopChan)

	// Wait for current tasks to complete or timeout
	timeout := time.NewTimer(30 * time.Second)
	defer timeout.Stop()

	for len(mws.currentTasks) > 0 {
		select {
		case <-timeout.C:
			glog.Warningf("Worker %s stopping with %d tasks still running", mws.workerID, len(mws.currentTasks))
			return
		case <-time.After(time.Second):
			// Check again
		}
	}

	glog.Infof("Maintenance worker %s stopped", mws.workerID)
}

// workerLoop is the main worker event loop
func (mws *MaintenanceWorkerService) workerLoop() {
	heartbeatTicker := time.NewTicker(30 * time.Second)
	defer heartbeatTicker.Stop()

	taskRequestTicker := time.NewTicker(5 * time.Second)
	defer taskRequestTicker.Stop()

	for mws.running {
		select {
		case <-mws.stopChan:
			return
		case <-heartbeatTicker.C:
			mws.sendHeartbeat()
		case <-taskRequestTicker.C:
			mws.requestTasks()
		}
	}
}

// sendHeartbeat sends heartbeat to admin server
func (mws *MaintenanceWorkerService) sendHeartbeat() {
	if mws.queue != nil {
		mws.queue.UpdateWorkerHeartbeat(mws.workerID)
	}
}

// requestTasks requests new tasks from the admin server
func (mws *MaintenanceWorkerService) requestTasks() {
	if len(mws.currentTasks) >= mws.maxConcurrent {
		return // Already at capacity
	}

	if mws.queue != nil {
		task := mws.queue.GetNextTask(mws.workerID, mws.capabilities)
		if task != nil {
			mws.executeTask(task)
		}
	}
}

// executeTask executes a maintenance task
func (mws *MaintenanceWorkerService) executeTask(task *MaintenanceTask) {
	mws.currentTasks[task.ID] = task

	go func() {
		defer func() {
			delete(mws.currentTasks, task.ID)
		}()

		glog.Infof("Worker %s executing task %s: %s", mws.workerID, task.ID, task.Type)

		// Execute task using dynamic executor registry
		var err error
		if executor, exists := mws.taskExecutors[task.Type]; exists {
			err = executor(mws, task)
		} else {
			err = fmt.Errorf("unsupported task type: %s", task.Type)
			glog.Errorf("No executor registered for task type: %s", task.Type)
		}

		// Report task completion
		if mws.queue != nil {
			errorMsg := ""
			if err != nil {
				errorMsg = err.Error()
			}
			mws.queue.CompleteTask(task.ID, errorMsg)
		}

		if err != nil {
			glog.Errorf("Worker %s failed to execute task %s: %v", mws.workerID, task.ID, err)
		} else {
			glog.Infof("Worker %s completed task %s successfully", mws.workerID, task.ID)
		}
	}()
}

// updateTaskProgress updates the progress of a task
func (mws *MaintenanceWorkerService) updateTaskProgress(taskID string, progress float64) {
	if mws.queue != nil {
		mws.queue.UpdateTaskProgress(taskID, progress)
	}
}

// GetStatus returns the current status of the worker
func (mws *MaintenanceWorkerService) GetStatus() map[string]interface{} {
	return map[string]interface{}{
		"worker_id":      mws.workerID,
		"address":        mws.address,
		"running":        mws.running,
		"capabilities":   mws.capabilities,
		"max_concurrent": mws.maxConcurrent,
		"current_tasks":  len(mws.currentTasks),
		"task_details":   mws.currentTasks,
	}
}

// SetQueue sets the maintenance queue for the worker
func (mws *MaintenanceWorkerService) SetQueue(queue *MaintenanceQueue) {
	mws.queue = queue
}

// SetAdminClient sets the admin client for the worker
func (mws *MaintenanceWorkerService) SetAdminClient(client *dash.AdminServer) {
	mws.adminClient = client
}

// SetCapabilities sets the worker capabilities
func (mws *MaintenanceWorkerService) SetCapabilities(capabilities []MaintenanceTaskType) {
	mws.capabilities = capabilities
}

// SetMaxConcurrent sets the maximum concurrent tasks
func (mws *MaintenanceWorkerService) SetMaxConcurrent(max int) {
	mws.maxConcurrent = max
}

// SetHeartbeatInterval sets the heartbeat interval (placeholder for future use)
func (mws *MaintenanceWorkerService) SetHeartbeatInterval(interval time.Duration) {
	// Future implementation for configurable heartbeat
}

// SetTaskRequestInterval sets the task request interval (placeholder for future use)
func (mws *MaintenanceWorkerService) SetTaskRequestInterval(interval time.Duration) {
	// Future implementation for configurable task requests
}

// MaintenanceWorkerCommand represents a standalone maintenance worker command
type MaintenanceWorkerCommand struct {
	workerService *MaintenanceWorkerService
}

// NewMaintenanceWorkerCommand creates a new worker command
func NewMaintenanceWorkerCommand(workerID, address, adminServer string) *MaintenanceWorkerCommand {
	return &MaintenanceWorkerCommand{
		workerService: NewMaintenanceWorkerService(workerID, address, adminServer),
	}
}

// Run starts the maintenance worker as a standalone service
func (mwc *MaintenanceWorkerCommand) Run() error {
	// Generate worker ID if not provided
	if mwc.workerService.workerID == "" {
		hostname, _ := os.Hostname()
		mwc.workerService.workerID = fmt.Sprintf("worker-%s-%d", hostname, time.Now().Unix())
	}

	// Start the worker service
	err := mwc.workerService.Start()
	if err != nil {
		return fmt.Errorf("failed to start maintenance worker: %v", err)
	}

	// Wait for interrupt signal
	select {}
}
