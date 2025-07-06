package worker

import (
	"context"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/worker_pb"
	"github.com/seaweedfs/seaweedfs/weed/worker/types"
	"google.golang.org/grpc"
)

// GrpcAdminClient implements AdminClient using gRPC bidirectional streaming
type GrpcAdminClient struct {
	adminAddress string
	workerID     string
	dialOption   grpc.DialOption

	conn         *grpc.ClientConn
	client       worker_pb.WorkerServiceClient
	stream       worker_pb.WorkerService_WorkerStreamClient
	streamCtx    context.Context
	streamCancel context.CancelFunc

	connected       bool
	reconnecting    bool
	shouldReconnect bool
	mutex           sync.RWMutex

	// Reconnection parameters
	maxReconnectAttempts int
	reconnectBackoff     time.Duration
	maxReconnectBackoff  time.Duration
	reconnectMultiplier  float64

	// Worker registration info for re-registration after reconnection
	lastWorkerInfo *types.Worker

	// Channels for communication
	outgoing       chan *worker_pb.WorkerMessage
	incoming       chan *worker_pb.AdminMessage
	responseChans  map[string]chan *worker_pb.AdminMessage
	responsesMutex sync.RWMutex

	// Shutdown channel
	shutdownChan chan struct{}
}

// NewGrpcAdminClient creates a new gRPC admin client
func NewGrpcAdminClient(adminAddress string, workerID string, dialOption grpc.DialOption) *GrpcAdminClient {
	// Admin uses HTTP port + 10000 as gRPC port
	grpcAddress := pb.ServerToGrpcAddress(adminAddress)

	return &GrpcAdminClient{
		adminAddress:         grpcAddress,
		workerID:             workerID,
		dialOption:           dialOption,
		shouldReconnect:      true,
		maxReconnectAttempts: 0, // 0 means infinite attempts
		reconnectBackoff:     1 * time.Second,
		maxReconnectBackoff:  30 * time.Second,
		reconnectMultiplier:  1.5,
		outgoing:             make(chan *worker_pb.WorkerMessage, 100),
		incoming:             make(chan *worker_pb.AdminMessage, 100),
		responseChans:        make(map[string]chan *worker_pb.AdminMessage),
		shutdownChan:         make(chan struct{}),
	}
}

// Connect establishes gRPC connection to admin server with TLS detection
func (c *GrpcAdminClient) Connect() error {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	if c.connected {
		return fmt.Errorf("already connected")
	}

	// Detect TLS support and create appropriate connection
	conn, err := c.createConnection()
	if err != nil {
		return fmt.Errorf("failed to connect to admin server: %v", err)
	}

	c.conn = conn
	c.client = worker_pb.NewWorkerServiceClient(conn)

	// Create bidirectional stream
	c.streamCtx, c.streamCancel = context.WithCancel(context.Background())
	stream, err := c.client.WorkerStream(c.streamCtx)
	if err != nil {
		c.conn.Close()
		return fmt.Errorf("failed to create worker stream: %v", err)
	}

	c.stream = stream
	c.connected = true

	// Start stream handlers and reconnection loop
	go c.handleOutgoing()
	go c.handleIncoming()
	go c.reconnectionLoop()

	glog.Infof("Connected to admin server at %s", c.adminAddress)
	return nil
}

// createConnection attempts to connect using the provided dial option
func (c *GrpcAdminClient) createConnection() (*grpc.ClientConn, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	conn, err := pb.GrpcDial(ctx, c.adminAddress, false, c.dialOption)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to admin server: %v", err)
	}

	glog.Infof("Connected to admin server at %s", c.adminAddress)
	return conn, nil
}

// Disconnect closes the gRPC connection
func (c *GrpcAdminClient) Disconnect() error {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	if !c.connected {
		return nil
	}

	c.connected = false
	c.shouldReconnect = false

	// Send shutdown signal to stop reconnection loop
	select {
	case c.shutdownChan <- struct{}{}:
	default:
	}

	// Send shutdown message
	shutdownMsg := &worker_pb.WorkerMessage{
		WorkerId:  c.workerID,
		Timestamp: time.Now().Unix(),
		Message: &worker_pb.WorkerMessage_Shutdown{
			Shutdown: &worker_pb.WorkerShutdown{
				WorkerId: c.workerID,
				Reason:   "normal shutdown",
			},
		},
	}

	select {
	case c.outgoing <- shutdownMsg:
	case <-time.After(time.Second):
		glog.Warningf("Failed to send shutdown message")
	}

	// Cancel stream context
	if c.streamCancel != nil {
		c.streamCancel()
	}

	// Close stream
	if c.stream != nil {
		c.stream.CloseSend()
	}

	// Close connection
	if c.conn != nil {
		c.conn.Close()
	}

	// Close channels
	close(c.outgoing)
	close(c.incoming)

	glog.Infof("Disconnected from admin server")
	return nil
}

// reconnectionLoop handles automatic reconnection with exponential backoff
func (c *GrpcAdminClient) reconnectionLoop() {
	backoff := c.reconnectBackoff
	attempts := 0

	for {
		select {
		case <-c.shutdownChan:
			return
		default:
		}

		c.mutex.RLock()
		shouldReconnect := c.shouldReconnect && !c.connected && !c.reconnecting
		c.mutex.RUnlock()

		if !shouldReconnect {
			time.Sleep(time.Second)
			continue
		}

		c.mutex.Lock()
		c.reconnecting = true
		c.mutex.Unlock()

		glog.Infof("Attempting to reconnect to admin server (attempt %d)", attempts+1)

		// Attempt to reconnect
		if err := c.reconnect(); err != nil {
			attempts++
			glog.Errorf("Reconnection attempt %d failed: %v", attempts, err)

			// Reset reconnecting flag
			c.mutex.Lock()
			c.reconnecting = false
			c.mutex.Unlock()

			// Check if we should give up
			if c.maxReconnectAttempts > 0 && attempts >= c.maxReconnectAttempts {
				glog.Errorf("Max reconnection attempts (%d) reached, giving up", c.maxReconnectAttempts)
				c.mutex.Lock()
				c.shouldReconnect = false
				c.mutex.Unlock()
				return
			}

			// Wait with exponential backoff
			glog.Infof("Waiting %v before next reconnection attempt", backoff)

			select {
			case <-c.shutdownChan:
				return
			case <-time.After(backoff):
			}

			// Increase backoff
			backoff = time.Duration(float64(backoff) * c.reconnectMultiplier)
			if backoff > c.maxReconnectBackoff {
				backoff = c.maxReconnectBackoff
			}
		} else {
			// Successful reconnection
			attempts = 0
			backoff = c.reconnectBackoff
			glog.Infof("Successfully reconnected to admin server")

			c.mutex.Lock()
			c.reconnecting = false
			c.mutex.Unlock()
		}
	}
}

// reconnect attempts to re-establish the connection
func (c *GrpcAdminClient) reconnect() error {
	// Clean up existing connection completely
	c.mutex.Lock()
	if c.streamCancel != nil {
		c.streamCancel()
	}
	if c.stream != nil {
		c.stream.CloseSend()
	}
	if c.conn != nil {
		c.conn.Close()
	}
	c.mutex.Unlock()

	// Create new connection
	conn, err := c.createConnection()
	if err != nil {
		return fmt.Errorf("failed to create connection: %v", err)
	}

	client := worker_pb.NewWorkerServiceClient(conn)

	// Create new stream
	streamCtx, streamCancel := context.WithCancel(context.Background())
	stream, err := client.WorkerStream(streamCtx)
	if err != nil {
		conn.Close()
		streamCancel()
		return fmt.Errorf("failed to create stream: %v", err)
	}

	// Update client state
	c.mutex.Lock()
	c.conn = conn
	c.client = client
	c.stream = stream
	c.streamCtx = streamCtx
	c.streamCancel = streamCancel
	c.connected = true
	c.mutex.Unlock()

	// Restart stream handlers
	go c.handleOutgoing()
	go c.handleIncoming()

	// Re-register worker if we have previous registration info
	c.mutex.RLock()
	workerInfo := c.lastWorkerInfo
	c.mutex.RUnlock()

	if workerInfo != nil {
		glog.Infof("Re-registering worker after reconnection...")
		if err := c.sendRegistration(workerInfo); err != nil {
			glog.Errorf("Failed to re-register worker: %v", err)
			// Don't fail the reconnection because of registration failure
			// The registration will be retried on next heartbeat or operation
		}
	}

	return nil
}

// handleOutgoing processes outgoing messages to admin
func (c *GrpcAdminClient) handleOutgoing() {
	for msg := range c.outgoing {
		c.mutex.RLock()
		connected := c.connected
		stream := c.stream
		c.mutex.RUnlock()

		if !connected {
			break
		}

		if err := stream.Send(msg); err != nil {
			glog.Errorf("Failed to send message to admin: %v", err)
			c.mutex.Lock()
			c.connected = false
			c.mutex.Unlock()
			break
		}
	}
}

// handleIncoming processes incoming messages from admin
func (c *GrpcAdminClient) handleIncoming() {
	for {
		c.mutex.RLock()
		connected := c.connected
		stream := c.stream
		c.mutex.RUnlock()

		if !connected {
			break
		}

		msg, err := stream.Recv()
		if err != nil {
			if err == io.EOF {
				glog.Infof("Admin server closed the stream")
			} else {
				glog.Errorf("Failed to receive message from admin: %v", err)
			}
			c.mutex.Lock()
			c.connected = false
			c.mutex.Unlock()
			break
		}

		// Route message to waiting goroutines or general handler
		select {
		case c.incoming <- msg:
		case <-time.After(time.Second):
			glog.Warningf("Incoming message buffer full, dropping message")
		}
	}
}

// RegisterWorker registers the worker with the admin server
func (c *GrpcAdminClient) RegisterWorker(worker *types.Worker) error {
	if !c.connected {
		return fmt.Errorf("not connected to admin server")
	}

	// Store worker info for re-registration after reconnection
	c.mutex.Lock()
	c.lastWorkerInfo = worker
	c.mutex.Unlock()

	return c.sendRegistration(worker)
}

// sendRegistration sends the registration message and waits for response
func (c *GrpcAdminClient) sendRegistration(worker *types.Worker) error {
	capabilities := make([]string, len(worker.Capabilities))
	for i, cap := range worker.Capabilities {
		capabilities[i] = string(cap)
	}

	msg := &worker_pb.WorkerMessage{
		WorkerId:  c.workerID,
		Timestamp: time.Now().Unix(),
		Message: &worker_pb.WorkerMessage_Registration{
			Registration: &worker_pb.WorkerRegistration{
				WorkerId:      c.workerID,
				Address:       worker.Address,
				Capabilities:  capabilities,
				MaxConcurrent: int32(worker.MaxConcurrent),
				Metadata:      make(map[string]string),
			},
		},
	}

	select {
	case c.outgoing <- msg:
	case <-time.After(5 * time.Second):
		return fmt.Errorf("failed to send registration message: timeout")
	}

	// Wait for registration response
	timeout := time.NewTimer(10 * time.Second)
	defer timeout.Stop()

	for {
		select {
		case response := <-c.incoming:
			if regResp := response.GetRegistrationResponse(); regResp != nil {
				if regResp.Success {
					glog.Infof("Worker registered successfully: %s", regResp.Message)
					return nil
				}
				return fmt.Errorf("registration failed: %s", regResp.Message)
			}
		case <-timeout.C:
			return fmt.Errorf("registration timeout")
		}
	}
}

// SendHeartbeat sends heartbeat to admin server
func (c *GrpcAdminClient) SendHeartbeat(workerID string, status *types.WorkerStatus) error {
	if !c.connected {
		// Wait for reconnection for a short time
		if err := c.waitForConnection(10 * time.Second); err != nil {
			return fmt.Errorf("not connected to admin server: %v", err)
		}
	}

	taskIds := make([]string, len(status.CurrentTasks))
	for i, task := range status.CurrentTasks {
		taskIds[i] = task.ID
	}

	msg := &worker_pb.WorkerMessage{
		WorkerId:  c.workerID,
		Timestamp: time.Now().Unix(),
		Message: &worker_pb.WorkerMessage_Heartbeat{
			Heartbeat: &worker_pb.WorkerHeartbeat{
				WorkerId:       c.workerID,
				Status:         status.Status,
				CurrentLoad:    int32(status.CurrentLoad),
				MaxConcurrent:  int32(status.MaxConcurrent),
				CurrentTaskIds: taskIds,
				TasksCompleted: int32(status.TasksCompleted),
				TasksFailed:    int32(status.TasksFailed),
				UptimeSeconds:  int64(status.Uptime.Seconds()),
			},
		},
	}

	select {
	case c.outgoing <- msg:
		return nil
	case <-time.After(time.Second):
		return fmt.Errorf("failed to send heartbeat: timeout")
	}
}

// RequestTask requests a new task from admin server
func (c *GrpcAdminClient) RequestTask(workerID string, capabilities []types.TaskType) (*types.Task, error) {
	if !c.connected {
		// Wait for reconnection for a short time
		if err := c.waitForConnection(5 * time.Second); err != nil {
			return nil, fmt.Errorf("not connected to admin server: %v", err)
		}
	}

	caps := make([]string, len(capabilities))
	for i, cap := range capabilities {
		caps[i] = string(cap)
	}

	msg := &worker_pb.WorkerMessage{
		WorkerId:  c.workerID,
		Timestamp: time.Now().Unix(),
		Message: &worker_pb.WorkerMessage_TaskRequest{
			TaskRequest: &worker_pb.TaskRequest{
				WorkerId:       c.workerID,
				Capabilities:   caps,
				AvailableSlots: 1, // Request one task
			},
		},
	}

	select {
	case c.outgoing <- msg:
	case <-time.After(time.Second):
		return nil, fmt.Errorf("failed to send task request: timeout")
	}

	// Wait for task assignment
	timeout := time.NewTimer(5 * time.Second)
	defer timeout.Stop()

	for {
		select {
		case response := <-c.incoming:
			if taskAssign := response.GetTaskAssignment(); taskAssign != nil {
				// Convert parameters map[string]string to map[string]interface{}
				parameters := make(map[string]interface{})
				for k, v := range taskAssign.Params.Parameters {
					parameters[k] = v
				}

				// Convert to our task type
				task := &types.Task{
					ID:         taskAssign.TaskId,
					Type:       types.TaskType(taskAssign.TaskType),
					Status:     types.TaskStatusAssigned,
					VolumeID:   taskAssign.Params.VolumeId,
					Server:     taskAssign.Params.Server,
					Collection: taskAssign.Params.Collection,
					Priority:   types.TaskPriority(taskAssign.Priority),
					CreatedAt:  time.Unix(taskAssign.CreatedTime, 0),
					Parameters: parameters,
				}
				return task, nil
			}
		case <-timeout.C:
			return nil, nil // No task available
		}
	}
}

// CompleteTask reports task completion to admin server
func (c *GrpcAdminClient) CompleteTask(taskID string, success bool, errorMsg string) error {
	if !c.connected {
		// Wait for reconnection for a short time
		if err := c.waitForConnection(5 * time.Second); err != nil {
			return fmt.Errorf("not connected to admin server: %v", err)
		}
	}

	msg := &worker_pb.WorkerMessage{
		WorkerId:  c.workerID,
		Timestamp: time.Now().Unix(),
		Message: &worker_pb.WorkerMessage_TaskComplete{
			TaskComplete: &worker_pb.TaskComplete{
				TaskId:         taskID,
				WorkerId:       c.workerID,
				Success:        success,
				ErrorMessage:   errorMsg,
				CompletionTime: time.Now().Unix(),
			},
		},
	}

	select {
	case c.outgoing <- msg:
		return nil
	case <-time.After(time.Second):
		return fmt.Errorf("failed to send task completion: timeout")
	}
}

// UpdateTaskProgress updates task progress to admin server
func (c *GrpcAdminClient) UpdateTaskProgress(taskID string, progress float64) error {
	if !c.connected {
		// Wait for reconnection for a short time
		if err := c.waitForConnection(5 * time.Second); err != nil {
			return fmt.Errorf("not connected to admin server: %v", err)
		}
	}

	msg := &worker_pb.WorkerMessage{
		WorkerId:  c.workerID,
		Timestamp: time.Now().Unix(),
		Message: &worker_pb.WorkerMessage_TaskUpdate{
			TaskUpdate: &worker_pb.TaskUpdate{
				TaskId:   taskID,
				WorkerId: c.workerID,
				Status:   "in_progress",
				Progress: float32(progress),
			},
		},
	}

	select {
	case c.outgoing <- msg:
		return nil
	case <-time.After(time.Second):
		return fmt.Errorf("failed to send task progress: timeout")
	}
}

// IsConnected returns whether the client is connected
func (c *GrpcAdminClient) IsConnected() bool {
	c.mutex.RLock()
	defer c.mutex.RUnlock()
	return c.connected
}

// IsReconnecting returns whether the client is currently attempting to reconnect
func (c *GrpcAdminClient) IsReconnecting() bool {
	c.mutex.RLock()
	defer c.mutex.RUnlock()
	return c.reconnecting
}

// SetReconnectionSettings allows configuration of reconnection behavior
func (c *GrpcAdminClient) SetReconnectionSettings(maxAttempts int, initialBackoff, maxBackoff time.Duration, multiplier float64) {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	c.maxReconnectAttempts = maxAttempts
	c.reconnectBackoff = initialBackoff
	c.maxReconnectBackoff = maxBackoff
	c.reconnectMultiplier = multiplier
}

// StopReconnection stops the reconnection loop
func (c *GrpcAdminClient) StopReconnection() {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	c.shouldReconnect = false
}

// StartReconnection starts the reconnection loop
func (c *GrpcAdminClient) StartReconnection() {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	c.shouldReconnect = true
}

// waitForConnection waits for the connection to be established or timeout
func (c *GrpcAdminClient) waitForConnection(timeout time.Duration) error {
	deadline := time.Now().Add(timeout)

	for time.Now().Before(deadline) {
		c.mutex.RLock()
		connected := c.connected
		shouldReconnect := c.shouldReconnect
		c.mutex.RUnlock()

		if connected {
			return nil
		}

		if !shouldReconnect {
			return fmt.Errorf("reconnection is disabled")
		}

		time.Sleep(100 * time.Millisecond)
	}

	return fmt.Errorf("timeout waiting for connection")
}

// MockAdminClient provides a mock implementation for testing
type MockAdminClient struct {
	workerID  string
	connected bool
	tasks     []*types.Task
	mutex     sync.RWMutex
}

// NewMockAdminClient creates a new mock admin client
func NewMockAdminClient() *MockAdminClient {
	return &MockAdminClient{
		connected: true,
		tasks:     make([]*types.Task, 0),
	}
}

// Connect mock implementation
func (m *MockAdminClient) Connect() error {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	m.connected = true
	return nil
}

// Disconnect mock implementation
func (m *MockAdminClient) Disconnect() error {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	m.connected = false
	return nil
}

// RegisterWorker mock implementation
func (m *MockAdminClient) RegisterWorker(worker *types.Worker) error {
	m.workerID = worker.ID
	glog.Infof("Mock: Worker %s registered with capabilities: %v", worker.ID, worker.Capabilities)
	return nil
}

// SendHeartbeat mock implementation
func (m *MockAdminClient) SendHeartbeat(workerID string, status *types.WorkerStatus) error {
	glog.V(2).Infof("Mock: Heartbeat from worker %s, status: %s, load: %d/%d",
		workerID, status.Status, status.CurrentLoad, status.MaxConcurrent)
	return nil
}

// RequestTask mock implementation
func (m *MockAdminClient) RequestTask(workerID string, capabilities []types.TaskType) (*types.Task, error) {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	if len(m.tasks) > 0 {
		task := m.tasks[0]
		m.tasks = m.tasks[1:]
		glog.Infof("Mock: Assigned task %s to worker %s", task.ID, workerID)
		return task, nil
	}

	// No tasks available
	return nil, nil
}

// CompleteTask mock implementation
func (m *MockAdminClient) CompleteTask(taskID string, success bool, errorMsg string) error {
	if success {
		glog.Infof("Mock: Task %s completed successfully", taskID)
	} else {
		glog.Infof("Mock: Task %s failed: %s", taskID, errorMsg)
	}
	return nil
}

// UpdateTaskProgress mock implementation
func (m *MockAdminClient) UpdateTaskProgress(taskID string, progress float64) error {
	glog.V(2).Infof("Mock: Task %s progress: %.1f%%", taskID, progress)
	return nil
}

// IsConnected mock implementation
func (m *MockAdminClient) IsConnected() bool {
	m.mutex.RLock()
	defer m.mutex.RUnlock()
	return m.connected
}

// AddMockTask adds a mock task for testing
func (m *MockAdminClient) AddMockTask(task *types.Task) {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	m.tasks = append(m.tasks, task)
}

// CreateAdminClient creates an admin client with the provided dial option
func CreateAdminClient(adminServer string, workerID string, dialOption grpc.DialOption) (AdminClient, error) {
	return NewGrpcAdminClient(adminServer, workerID, dialOption), nil
}
