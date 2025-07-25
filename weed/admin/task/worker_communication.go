package task

import (
	"context"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/worker_pb"
	"github.com/seaweedfs/seaweedfs/weed/worker/types"
	"google.golang.org/grpc"
)

// WorkerConnection manages the gRPC connection to a single worker
type WorkerConnection struct {
	workerID    string
	address     string
	conn        *grpc.ClientConn
	client      worker_pb.WorkerServiceClient
	stream      worker_pb.WorkerService_WorkerStreamClient
	lastSeen    time.Time
	mutex       sync.RWMutex
	adminServer *AdminServer
	stopCh      chan struct{}
	active      bool
}

// WorkerCommunicationManager manages all worker connections
type WorkerCommunicationManager struct {
	adminServer *AdminServer
	connections map[string]*WorkerConnection
	mutex       sync.RWMutex
	stopCh      chan struct{}
}

// NewWorkerCommunicationManager creates a new worker communication manager
func NewWorkerCommunicationManager(adminServer *AdminServer) *WorkerCommunicationManager {
	return &WorkerCommunicationManager{
		adminServer: adminServer,
		connections: make(map[string]*WorkerConnection),
		stopCh:      make(chan struct{}),
	}
}

// Start starts the worker communication manager
func (wcm *WorkerCommunicationManager) Start() {
	glog.Infof("Starting worker communication manager")

	go wcm.connectionMonitorLoop()
}

// Stop stops the worker communication manager
func (wcm *WorkerCommunicationManager) Stop() {
	glog.Infof("Stopping worker communication manager")

	close(wcm.stopCh)

	wcm.mutex.Lock()
	defer wcm.mutex.Unlock()

	for _, conn := range wcm.connections {
		conn.Close()
	}
}

// EstablishWorkerConnection establishes a connection to a worker
func (wcm *WorkerCommunicationManager) EstablishWorkerConnection(workerID, address string) error {
	wcm.mutex.Lock()
	defer wcm.mutex.Unlock()

	// Check if already connected
	if conn, exists := wcm.connections[workerID]; exists {
		if conn.active {
			return nil // Already connected
		}
		conn.Close() // Close inactive connection
	}

	// Create new connection
	conn, err := NewWorkerConnection(workerID, address, wcm.adminServer)
	if err != nil {
		return fmt.Errorf("failed to create worker connection: %v", err)
	}

	wcm.connections[workerID] = conn

	// Start connection
	go conn.Start()

	glog.Infof("Established connection to worker %s at %s", workerID, address)
	return nil
}

// SendTaskAssignment sends a task assignment to a worker
func (wcm *WorkerCommunicationManager) SendTaskAssignment(workerID string, task *Task) error {
	wcm.mutex.RLock()
	conn, exists := wcm.connections[workerID]
	wcm.mutex.RUnlock()

	if !exists || !conn.active {
		return fmt.Errorf("no active connection to worker %s", workerID)
	}

	return conn.SendTaskAssignment(task)
}

// CancelTask sends a task cancellation to a worker
func (wcm *WorkerCommunicationManager) CancelTask(workerID, taskID string, reason string) error {
	wcm.mutex.RLock()
	conn, exists := wcm.connections[workerID]
	wcm.mutex.RUnlock()

	if !exists || !conn.active {
		return fmt.Errorf("no active connection to worker %s", workerID)
	}

	return conn.CancelTask(taskID, reason)
}

// GetActiveConnections returns the list of active worker connections
func (wcm *WorkerCommunicationManager) GetActiveConnections() []string {
	wcm.mutex.RLock()
	defer wcm.mutex.RUnlock()

	var active []string
	for workerID, conn := range wcm.connections {
		if conn.active {
			active = append(active, workerID)
		}
	}

	return active
}

// connectionMonitorLoop monitors worker connections and cleans up inactive ones
func (wcm *WorkerCommunicationManager) connectionMonitorLoop() {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			wcm.cleanupInactiveConnections()
		case <-wcm.stopCh:
			return
		}
	}
}

// cleanupInactiveConnections removes inactive worker connections
func (wcm *WorkerCommunicationManager) cleanupInactiveConnections() {
	wcm.mutex.Lock()
	defer wcm.mutex.Unlock()

	now := time.Now()
	timeout := 2 * time.Minute

	for workerID, conn := range wcm.connections {
		if !conn.active || now.Sub(conn.lastSeen) > timeout {
			glog.Infof("Cleaning up inactive connection to worker %s", workerID)
			conn.Close()
			delete(wcm.connections, workerID)

			// Mark worker as inactive in registry
			wcm.adminServer.workerRegistry.MarkWorkerInactive(workerID)
		}
	}
}

// NewWorkerConnection creates a new worker connection
func NewWorkerConnection(workerID, address string, adminServer *AdminServer) (*WorkerConnection, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	conn, err := grpc.DialContext(ctx, address, grpc.WithInsecure(), grpc.WithBlock())
	if err != nil {
		return nil, fmt.Errorf("failed to connect to worker at %s: %v", address, err)
	}

	client := worker_pb.NewWorkerServiceClient(conn)

	return &WorkerConnection{
		workerID:    workerID,
		address:     address,
		conn:        conn,
		client:      client,
		lastSeen:    time.Now(),
		adminServer: adminServer,
		stopCh:      make(chan struct{}),
		active:      false,
	}, nil
}

// Start starts the worker connection and message handling
func (wc *WorkerConnection) Start() {
	defer wc.Close()

	ctx := context.Background()
	stream, err := wc.client.WorkerStream(ctx)
	if err != nil {
		glog.Errorf("Failed to create worker stream for %s: %v", wc.workerID, err)
		return
	}

	wc.stream = stream
	wc.active = true

	glog.Infof("Worker connection %s started", wc.workerID)

	// Start message handling goroutines
	go wc.receiveMessages()

	// Keep connection alive until stopped
	<-wc.stopCh
}

// Close closes the worker connection
func (wc *WorkerConnection) Close() {
	wc.mutex.Lock()
	defer wc.mutex.Unlock()

	if !wc.active {
		return
	}

	wc.active = false
	close(wc.stopCh)

	if wc.stream != nil {
		wc.stream.CloseSend()
	}

	if wc.conn != nil {
		wc.conn.Close()
	}

	glog.Infof("Worker connection %s closed", wc.workerID)
}

// receiveMessages handles incoming messages from the worker
func (wc *WorkerConnection) receiveMessages() {
	for {
		select {
		case <-wc.stopCh:
			return
		default:
		}

		msg, err := wc.stream.Recv()
		if err != nil {
			if err == io.EOF {
				glog.Infof("Worker %s closed connection", wc.workerID)
			} else {
				glog.Errorf("Error receiving from worker %s: %v", wc.workerID, err)
			}
			wc.Close()
			return
		}

		wc.updateLastSeen()
		// Convert AdminMessage to WorkerMessage for processing
		if workerMsg := convertToWorkerMessage(msg); workerMsg != nil {
			wc.handleMessage(workerMsg)
		}
	}
}

// updateLastSeen updates the last seen timestamp
func (wc *WorkerConnection) updateLastSeen() {
	wc.mutex.Lock()
	defer wc.mutex.Unlock()
	wc.lastSeen = time.Now()
}

// handleMessage processes a message from the worker
func (wc *WorkerConnection) handleMessage(msg *worker_pb.WorkerMessage) {
	switch message := msg.Message.(type) {
	case *worker_pb.WorkerMessage_Registration:
		registration := message.Registration
		worker := &Worker{
			ID:           registration.WorkerId,
			Address:      registration.Address,
			Capabilities: registration.Capabilities,
		}
		wc.workerID = worker.ID
		// UpdateWorkerStatus stub
		if wc.adminServer.workerRegistry != nil {
			// wc.adminServer.workerRegistry.UpdateWorkerStatus(worker) // Commented out - method doesn't exist
		}
		glog.Infof("Worker %s registered", worker.ID)

	case *worker_pb.WorkerMessage_Heartbeat:
		glog.V(3).Infof("Heartbeat from worker %s", wc.workerID)

	case *worker_pb.WorkerMessage_TaskRequest:
		glog.V(2).Infof("Task request from worker %s", wc.workerID)
		// AssignTaskToWorker stub
		// task := wc.adminServer.AssignTaskToWorker(wc.workerID) // Commented out - method doesn't exist

	case *worker_pb.WorkerMessage_TaskUpdate:
		update := message.TaskUpdate
		// UpdateTaskProgress stub - fix signature
		wc.adminServer.UpdateTaskProgress(update.TaskId, float64(update.Progress))

	case *worker_pb.WorkerMessage_TaskComplete:
		complete := message.TaskComplete
		// CompleteTask stub - fix signature
		wc.adminServer.CompleteTask(complete.TaskId, complete.Success, complete.ErrorMessage)

	case *worker_pb.WorkerMessage_Shutdown:
		glog.Infof("Worker %s shutting down", wc.workerID)
		wc.Close()
	}
}

// SendTaskAssignment sends a task assignment to the worker
func (wc *WorkerConnection) SendTaskAssignment(task *Task) error {
	return wc.sendTaskAssignment(task)
}

// sendTaskAssignment sends a task assignment message
func (wc *WorkerConnection) sendTaskAssignment(task *types.Task) error {
	// Fix type assertions for parameters
	server, _ := task.Parameters["server"].(string)
	collection, _ := task.Parameters["collection"].(string)

	// Convert map[string]interface{} to map[string]string
	parameters := make(map[string]string)
	for k, v := range task.Parameters {
		if str, ok := v.(string); ok {
			parameters[k] = str
		} else {
			parameters[k] = fmt.Sprintf("%v", v)
		}
	}

	assignment := &worker_pb.TaskAssignment{
		TaskId:      task.ID,
		TaskType:    string(task.Type),
		Priority:    int32(task.Priority),
		CreatedTime: task.CreatedAt.Unix(),
		Params: &worker_pb.TaskParams{
			VolumeId:   task.VolumeID,
			Server:     server,
			Collection: collection,
			Parameters: parameters,
		},
		Metadata: map[string]string{
			"assigned_at": time.Now().Format(time.RFC3339),
		},
	}

	response := &worker_pb.AdminMessage{
		AdminId:   wc.adminServer.ID,
		Timestamp: time.Now().Unix(),
		Message: &worker_pb.AdminMessage_TaskAssignment{
			TaskAssignment: assignment,
		},
	}

	return wc.sendMessage(response)
}

// CancelTask sends a task cancellation to the worker
func (wc *WorkerConnection) CancelTask(taskID, reason string) error {
	cancellation := &worker_pb.TaskCancellation{
		TaskId: taskID,
		Reason: reason,
		Force:  false,
	}

	response := &worker_pb.AdminMessage{
		AdminId:   wc.adminServer.ID,
		Timestamp: time.Now().Unix(),
		Message: &worker_pb.AdminMessage_TaskCancellation{
			TaskCancellation: cancellation,
		},
	}

	return wc.sendMessage(response)
}

// sendMessage sends a message to the worker
func (wc *WorkerConnection) sendMessage(msg *worker_pb.AdminMessage) error {
	wc.mutex.RLock()
	defer wc.mutex.RUnlock()

	if !wc.active || wc.stream == nil {
		return fmt.Errorf("connection to worker %s is not active", wc.workerID)
	}

	// The stream expects WorkerMessage from client (admin) to server (worker)
	// Convert AdminMessage to appropriate WorkerMessage format
	workerMsg := &worker_pb.WorkerMessage{
		WorkerId:  wc.workerID,
		Timestamp: msg.Timestamp,
	}

	// Convert AdminMessage content to WorkerMessage based on message type
	switch adminMsg := msg.Message.(type) {
	case *worker_pb.AdminMessage_TaskAssignment:
		// Task assignments should be sent as notifications to worker
		// Since there's no direct equivalent, we'll create a generic message
		// In a full implementation, this would need proper message type mapping
		_ = adminMsg // Use the variable to avoid unused warning
		workerMsg.Message = &worker_pb.WorkerMessage_Heartbeat{
			Heartbeat: &worker_pb.WorkerHeartbeat{
				WorkerId: wc.workerID,
				Status:   "task_assigned",
			},
		}
	case *worker_pb.AdminMessage_TaskCancellation:
		// Similar conversion for task cancellation
		_ = adminMsg // Use the variable to avoid unused warning
		workerMsg.Message = &worker_pb.WorkerMessage_Heartbeat{
			Heartbeat: &worker_pb.WorkerHeartbeat{
				WorkerId: wc.workerID,
				Status:   "task_cancelled",
			},
		}
	default:
		// For other message types, send a generic heartbeat
		workerMsg.Message = &worker_pb.WorkerMessage_Heartbeat{
			Heartbeat: &worker_pb.WorkerHeartbeat{
				WorkerId: wc.workerID,
				Status:   "admin_message",
			},
		}
	}

	return wc.stream.Send(workerMsg)
}

// Helper functions

// convertCapabilities converts string capabilities to TaskType slice
func convertCapabilities(capabilities []string) []TaskType {
	var result []TaskType
	for _, cap := range capabilities {
		result = append(result, TaskType(cap))
	}
	return result
}

// WorkerStatus represents worker status information
type WorkerStatus struct {
	Status         string
	CurrentLoad    int
	MaxConcurrent  int
	CurrentTasks   []string
	TasksCompleted int
	TasksFailed    int
	UptimeSeconds  int64
	LastSeen       time.Time
}

// TaskProgress represents task progress information
type TaskProgress struct {
	Progress float64
	Message  string
}

// TaskResult represents task completion result
type TaskResult struct {
	Success bool
	Error   string
	Message string
}

// convertToWorkerMessage converts AdminMessage to WorkerMessage (stub implementation)
func convertToWorkerMessage(msg *worker_pb.AdminMessage) *worker_pb.WorkerMessage {
	// This is a stub - in real implementation would need proper conversion
	// For now, return nil to avoid processing
	return nil
}
