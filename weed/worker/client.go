package worker

import (
	"fmt"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/worker/types"
)

// HTTPAdminClient implements AdminClient interface using HTTP
type HTTPAdminClient struct {
	adminAddress string
	workerID     string
	timeout      time.Duration
}

// NewHTTPAdminClient creates a new HTTP admin client
func NewHTTPAdminClient(adminAddress string, workerID string) *HTTPAdminClient {
	return &HTTPAdminClient{
		adminAddress: adminAddress,
		workerID:     workerID,
		timeout:      30 * time.Second,
	}
}

// RegisterWorker registers a worker with the admin server
func (c *HTTPAdminClient) RegisterWorker(worker *types.Worker) error {
	glog.V(2).Infof("Registering worker %s with admin server at %s", worker.ID, c.adminAddress)

	// TODO: Implement HTTP request to register worker
	// For now, we'll simulate successful registration
	glog.V(2).Infof("Worker %s registered successfully", worker.ID)

	return nil
}

// RequestTask requests a new task from the admin server
func (c *HTTPAdminClient) RequestTask(workerID string, capabilities []types.TaskType) (*types.Task, error) {
	glog.V(3).Infof("Requesting task for worker %s with capabilities %v", workerID, capabilities)

	// TODO: Implement HTTP request to get next task
	// For now, we'll return nil (no task available)

	return nil, nil
}

// CompleteTask reports task completion to the admin server
func (c *HTTPAdminClient) CompleteTask(taskID string, errorMsg string) error {
	if errorMsg != "" {
		glog.V(2).Infof("Reporting task %s completion with error: %s", taskID, errorMsg)
	} else {
		glog.V(2).Infof("Reporting task %s completion successfully", taskID)
	}

	// TODO: Implement HTTP request to report task completion
	// For now, we'll simulate successful reporting

	return nil
}

// UpdateTaskProgress updates task progress on the admin server
func (c *HTTPAdminClient) UpdateTaskProgress(taskID string, progress float64) error {
	glog.V(3).Infof("Updating task %s progress to %.2f%%", taskID, progress)

	// TODO: Implement HTTP request to update task progress
	// For now, we'll simulate successful update

	return nil
}

// SendHeartbeat sends a heartbeat to the admin server
func (c *HTTPAdminClient) SendHeartbeat(workerID string) error {
	glog.V(3).Infof("Sending heartbeat for worker %s", workerID)

	// TODO: Implement HTTP request to send heartbeat
	// For now, we'll simulate successful heartbeat

	return nil
}

// SetTimeout sets the client timeout
func (c *HTTPAdminClient) SetTimeout(timeout time.Duration) {
	c.timeout = timeout
}

// MockAdminClient is a mock implementation for testing
type MockAdminClient struct {
	registeredWorkers map[string]*types.Worker
	tasks             []*types.Task
	completedTasks    map[string]string
	heartbeats        map[string]time.Time
	taskProgress      map[string]float64
}

// NewMockAdminClient creates a new mock admin client
func NewMockAdminClient() *MockAdminClient {
	return &MockAdminClient{
		registeredWorkers: make(map[string]*types.Worker),
		tasks:             make([]*types.Task, 0),
		completedTasks:    make(map[string]string),
		heartbeats:        make(map[string]time.Time),
		taskProgress:      make(map[string]float64),
	}
}

// RegisterWorker registers a worker (mock implementation)
func (m *MockAdminClient) RegisterWorker(worker *types.Worker) error {
	m.registeredWorkers[worker.ID] = worker
	glog.V(2).Infof("Mock: Registered worker %s", worker.ID)
	return nil
}

// RequestTask requests a new task (mock implementation)
func (m *MockAdminClient) RequestTask(workerID string, capabilities []types.TaskType) (*types.Task, error) {
	// Return a mock task for testing
	if len(m.tasks) > 0 {
		task := m.tasks[0]
		m.tasks = m.tasks[1:]

		// Check if worker can handle this task
		canHandle := false
		for _, capability := range capabilities {
			if capability == task.Type {
				canHandle = true
				break
			}
		}

		if canHandle {
			task.WorkerID = workerID
			task.Status = types.TaskStatusAssigned
			now := time.Now()
			task.StartedAt = &now

			glog.V(2).Infof("Mock: Assigned task %s to worker %s", task.ID, workerID)
			return task, nil
		}
	}

	return nil, nil
}

// CompleteTask reports task completion (mock implementation)
func (m *MockAdminClient) CompleteTask(taskID string, errorMsg string) error {
	m.completedTasks[taskID] = errorMsg
	glog.V(2).Infof("Mock: Task %s completed with error: %s", taskID, errorMsg)
	return nil
}

// UpdateTaskProgress updates task progress (mock implementation)
func (m *MockAdminClient) UpdateTaskProgress(taskID string, progress float64) error {
	m.taskProgress[taskID] = progress
	glog.V(3).Infof("Mock: Task %s progress updated to %.2f%%", taskID, progress)
	return nil
}

// SendHeartbeat sends a heartbeat (mock implementation)
func (m *MockAdminClient) SendHeartbeat(workerID string) error {
	m.heartbeats[workerID] = time.Now()
	glog.V(3).Infof("Mock: Heartbeat received from worker %s", workerID)
	return nil
}

// AddTask adds a task to the mock queue
func (m *MockAdminClient) AddTask(task *types.Task) {
	m.tasks = append(m.tasks, task)
}

// GetRegisteredWorkers returns all registered workers
func (m *MockAdminClient) GetRegisteredWorkers() map[string]*types.Worker {
	return m.registeredWorkers
}

// GetCompletedTasks returns all completed tasks
func (m *MockAdminClient) GetCompletedTasks() map[string]string {
	return m.completedTasks
}

// GetTaskProgress returns task progress information
func (m *MockAdminClient) GetTaskProgress() map[string]float64 {
	return m.taskProgress
}

// GetHeartbeats returns heartbeat information
func (m *MockAdminClient) GetHeartbeats() map[string]time.Time {
	return m.heartbeats
}

// CreateAdminClient creates an admin client based on the configuration
func CreateAdminClient(adminAddress string, workerID string, clientType string) (AdminClient, error) {
	switch clientType {
	case "http", "":
		return NewHTTPAdminClient(adminAddress, workerID), nil
	case "mock":
		return NewMockAdminClient(), nil
	default:
		return nil, fmt.Errorf("unsupported client type: %s", clientType)
	}
}

// AdminClientConfig contains configuration for admin client
type AdminClientConfig struct {
	AdminAddress string        `json:"admin_address"`
	WorkerID     string        `json:"worker_id"`
	ClientType   string        `json:"client_type"`
	Timeout      time.Duration `json:"timeout"`
}

// DefaultAdminClientConfig returns default admin client configuration
func DefaultAdminClientConfig() *AdminClientConfig {
	return &AdminClientConfig{
		AdminAddress: "localhost:9333",
		ClientType:   "http",
		Timeout:      30 * time.Second,
	}
}
