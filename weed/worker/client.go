package worker

import (
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/worker_pb"
	"github.com/seaweedfs/seaweedfs/weed/worker/types"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
)

// GrpcAdminClient implements AdminClient using gRPC bidirectional streaming
type GrpcAdminClient struct {
	adminAddress string
	workerID     string

	conn         *grpc.ClientConn
	client       worker_pb.WorkerServiceClient
	stream       worker_pb.WorkerService_WorkerStreamClient
	streamCtx    context.Context
	streamCancel context.CancelFunc

	connected bool
	mutex     sync.RWMutex

	// Channels for communication
	outgoing       chan *worker_pb.WorkerMessage
	incoming       chan *worker_pb.AdminMessage
	responseChans  map[string]chan *worker_pb.AdminMessage
	responsesMutex sync.RWMutex
}

// NewGrpcAdminClient creates a new gRPC admin client
func NewGrpcAdminClient(adminAddress string, workerID string) *GrpcAdminClient {
	// Admin uses HTTP port + 10000 as gRPC port
	grpcAddress := pb.ServerToGrpcAddress(adminAddress)

	return &GrpcAdminClient{
		adminAddress:  grpcAddress,
		workerID:      workerID,
		outgoing:      make(chan *worker_pb.WorkerMessage, 100),
		incoming:      make(chan *worker_pb.AdminMessage, 100),
		responseChans: make(map[string]chan *worker_pb.AdminMessage),
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

	// Start stream handlers
	go c.handleOutgoing()
	go c.handleIncoming()

	glog.Infof("Connected to admin server at %s", c.adminAddress)
	return nil
}

// createConnection attempts to connect with TLS first, falls back to insecure
func (c *GrpcAdminClient) createConnection() (*grpc.ClientConn, error) {
	// First try TLS connection and test it
	conn, err := c.tryTLSConnection()
	if err == nil {
		// Test the TLS connection by trying to create a client and ping
		if c.testConnection(conn) {
			glog.Infof("Connected to admin server using TLS")
			return conn, nil
		}
		// TLS connection created but doesn't work, close it
		conn.Close()
		glog.V(2).Infof("TLS connection test failed, attempting insecure connection")
	} else {
		glog.V(2).Infof("TLS connection failed: %v, attempting insecure connection", err)
	}

	// Fall back to insecure connection
	conn, err = c.tryInsecureConnection()
	if err == nil {
		glog.Infof("Connected to admin server using insecure connection")
		return conn, nil
	}

	return nil, fmt.Errorf("both TLS and insecure connections failed: %v", err)
}

// testConnection verifies that a gRPC connection actually works
func (c *GrpcAdminClient) testConnection(conn *grpc.ClientConn) bool {
	// Create a test client and try a simple operation
	client := worker_pb.NewWorkerServiceClient(conn)

	// Try to create a stream with a short timeout to test the connection
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	stream, err := client.WorkerStream(ctx)
	if err != nil {
		glog.V(3).Infof("Connection test failed: %v", err)
		return false
	}

	// Close the test stream immediately
	stream.CloseSend()

	return true
}

// tryTLSConnection attempts to connect using TLS
func (c *GrpcAdminClient) tryTLSConnection() (*grpc.ClientConn, error) {
	// Create TLS config that skips verification for self-signed certs
	tlsConfig := &tls.Config{
		InsecureSkipVerify: true, // For development/self-signed certs
		MinVersion:         tls.VersionTLS12,
	}
	creds := credentials.NewTLS(tlsConfig)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	return pb.GrpcDial(ctx, c.adminAddress, false, grpc.WithTransportCredentials(creds))
}

// tryInsecureConnection attempts to connect without TLS
func (c *GrpcAdminClient) tryInsecureConnection() (*grpc.ClientConn, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	return pb.GrpcDial(ctx, c.adminAddress, false, grpc.WithTransportCredentials(insecure.NewCredentials()))
}

// Disconnect closes the gRPC connection
func (c *GrpcAdminClient) Disconnect() error {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	if !c.connected {
		return nil
	}

	c.connected = false

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

// handleOutgoing processes outgoing messages to admin
func (c *GrpcAdminClient) handleOutgoing() {
	for msg := range c.outgoing {
		if !c.connected {
			break
		}

		if err := c.stream.Send(msg); err != nil {
			glog.Errorf("Failed to send message to admin: %v", err)
			break
		}
	}
}

// handleIncoming processes incoming messages from admin
func (c *GrpcAdminClient) handleIncoming() {
	for {
		if !c.connected {
			break
		}

		msg, err := c.stream.Recv()
		if err != nil {
			if err == io.EOF {
				glog.Infof("Admin server closed the stream")
			} else {
				glog.Errorf("Failed to receive message from admin: %v", err)
			}
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
		return fmt.Errorf("not connected to admin server")
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
		return nil, fmt.Errorf("not connected to admin server")
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
		return fmt.Errorf("not connected to admin server")
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
		return fmt.Errorf("not connected to admin server")
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

// CreateAdminClient creates an admin client, defaulting to gRPC
func CreateAdminClient(adminServer string, workerID string, clientType string) (AdminClient, error) {
	switch clientType {
	case "mock":
		return NewMockAdminClient(), nil
	default:
		// Default to gRPC for all cases (including "grpc" and unknown types)
		return NewGrpcAdminClient(adminServer, workerID), nil
	}
}
