package task

import (
	"context"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/worker_pb"
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
		wc.handleMessage(msg)
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
		wc.handleRegistration(message.Registration)
	case *worker_pb.WorkerMessage_Heartbeat:
		wc.handleHeartbeat(message.Heartbeat)
	case *worker_pb.WorkerMessage_TaskRequest:
		wc.handleTaskRequest(message.TaskRequest)
	case *worker_pb.WorkerMessage_TaskUpdate:
		wc.handleTaskUpdate(message.TaskUpdate)
	case *worker_pb.WorkerMessage_TaskComplete:
		wc.handleTaskComplete(message.TaskComplete)
	case *worker_pb.WorkerMessage_Shutdown:
		wc.handleShutdown(message.Shutdown)
	default:
		glog.Warningf("Unknown message type from worker %s", wc.workerID)
	}
}

// handleRegistration processes worker registration
func (wc *WorkerConnection) handleRegistration(reg *worker_pb.WorkerRegistration) {
	glog.Infof("Worker %s registering with capabilities: %v", reg.WorkerId, reg.Capabilities)

	// Convert to internal worker type
	worker := &Worker{
		ID:            reg.WorkerId,
		Address:       reg.Address,
		Capabilities:  convertCapabilities(reg.Capabilities),
		MaxConcurrent: int(reg.MaxConcurrent),
		Status:        "active",
		LastSeen:      time.Now(),
		CurrentLoad:   0,
		TasksAssigned: []string{},
	}

	// Register with worker registry
	wc.adminServer.workerRegistry.RegisterWorker(worker)

	// Send registration response
	response := &worker_pb.AdminMessage{
		AdminId:   wc.adminServer.ID,
		Timestamp: time.Now().Unix(),
		Message: &worker_pb.AdminMessage_RegistrationResponse{
			RegistrationResponse: &worker_pb.RegistrationResponse{
				Success:          true,
				Message:          "Registration successful",
				AssignedWorkerId: reg.WorkerId,
			},
		},
	}

	wc.sendMessage(response)
}

// handleHeartbeat processes worker heartbeat
func (wc *WorkerConnection) handleHeartbeat(hb *worker_pb.WorkerHeartbeat) {
	glog.V(2).Infof("Heartbeat from worker %s: status=%s, load=%d/%d",
		hb.WorkerId, hb.Status, hb.CurrentLoad, hb.MaxConcurrent)

	// Update worker status in registry
	wc.adminServer.workerRegistry.UpdateWorkerStatus(hb.WorkerId, &WorkerStatus{
		Status:         hb.Status,
		CurrentLoad:    int(hb.CurrentLoad),
		MaxConcurrent:  int(hb.MaxConcurrent),
		CurrentTasks:   hb.CurrentTaskIds,
		TasksCompleted: int(hb.TasksCompleted),
		TasksFailed:    int(hb.TasksFailed),
		UptimeSeconds:  hb.UptimeSeconds,
		LastSeen:       time.Now(),
	})

	// Send heartbeat response
	response := &worker_pb.AdminMessage{
		AdminId:   wc.adminServer.ID,
		Timestamp: time.Now().Unix(),
		Message: &worker_pb.AdminMessage_HeartbeatResponse{
			HeartbeatResponse: &worker_pb.HeartbeatResponse{
				Success: true,
				Message: "Heartbeat acknowledged",
			},
		},
	}

	wc.sendMessage(response)
}

// handleTaskRequest processes worker task request
func (wc *WorkerConnection) handleTaskRequest(req *worker_pb.TaskRequest) {
	glog.V(1).Infof("Task request from worker %s: capabilities=%v, slots=%d",
		req.WorkerId, req.Capabilities, req.AvailableSlots)

	// Get next available task for this worker
	capabilities := convertCapabilities(req.Capabilities)
	task := wc.adminServer.taskScheduler.GetNextTask(req.WorkerId, capabilities)

	if task != nil {
		// Assign task to worker
		err := wc.adminServer.AssignTaskToWorker(task.ID, req.WorkerId)
		if err != nil {
			glog.Errorf("Failed to assign task %s to worker %s: %v", task.ID, req.WorkerId, err)
			return
		}

		// Send task assignment
		wc.sendTaskAssignment(task)
		glog.Infof("Assigned task %s (%s) to worker %s", task.ID, task.Type, req.WorkerId)
	}
	// If no task available, no response needed - worker will request again later
}

// handleTaskUpdate processes task progress update
func (wc *WorkerConnection) handleTaskUpdate(update *worker_pb.TaskUpdate) {
	glog.V(1).Infof("Task update for %s from worker %s: status=%s, progress=%.1f%%",
		update.TaskId, update.WorkerId, update.Status, update.Progress*100)

	// Update task progress in admin server
	wc.adminServer.UpdateTaskProgress(update.TaskId, update.WorkerId, &TaskProgress{
		Status:    TaskStatus(update.Status),
		Progress:  update.Progress,
		Message:   update.Message,
		UpdatedAt: time.Now(),
	})
}

// handleTaskComplete processes task completion
func (wc *WorkerConnection) handleTaskComplete(complete *worker_pb.TaskComplete) {
	glog.Infof("Task %s completed by worker %s: success=%v",
		complete.TaskId, complete.WorkerId, complete.Success)

	// Update task completion in admin server
	var status TaskStatus
	if complete.Success {
		status = TaskStatusCompleted
	} else {
		status = TaskStatusFailed
	}

	result := &TaskResult{
		TaskID:         complete.TaskId,
		WorkerID:       complete.WorkerId,
		Status:         status,
		Success:        complete.Success,
		ErrorMessage:   complete.ErrorMessage,
		CompletedAt:    time.Unix(complete.CompletionTime, 0),
		ResultMetadata: complete.ResultMetadata,
	}

	wc.adminServer.CompleteTask(complete.TaskId, result)
}

// handleShutdown processes worker shutdown notification
func (wc *WorkerConnection) handleShutdown(shutdown *worker_pb.WorkerShutdown) {
	glog.Infof("Worker %s shutting down: %s, pending tasks: %v",
		shutdown.WorkerId, shutdown.Reason, shutdown.PendingTaskIds)

	// Handle pending tasks - reassign them
	for _, taskID := range shutdown.PendingTaskIds {
		wc.adminServer.ReassignTask(taskID, "worker shutdown")
	}

	// Remove worker from registry
	wc.adminServer.workerRegistry.UnregisterWorker(shutdown.WorkerId)

	wc.Close()
}

// SendTaskAssignment sends a task assignment to the worker
func (wc *WorkerConnection) SendTaskAssignment(task *Task) error {
	return wc.sendTaskAssignment(task)
}

// sendTaskAssignment sends a task assignment message
func (wc *WorkerConnection) sendTaskAssignment(task *Task) error {
	assignment := &worker_pb.TaskAssignment{
		TaskId:      task.ID,
		TaskType:    string(task.Type),
		Priority:    int32(task.Priority),
		CreatedTime: task.CreatedAt.Unix(),
		Params: &worker_pb.TaskParams{
			VolumeId:   task.VolumeID,
			Server:     task.Parameters["server"],
			Collection: task.Parameters["collection"],
			Parameters: task.Parameters,
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

	return wc.stream.Send(msg)
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
	Status    TaskStatus
	Progress  float32
	Message   string
	UpdatedAt time.Time
}

// TaskResult represents task completion result
type TaskResult struct {
	TaskID         string
	WorkerID       string
	Status         TaskStatus
	Success        bool
	ErrorMessage   string
	CompletedAt    time.Time
	ResultMetadata map[string]string
}
