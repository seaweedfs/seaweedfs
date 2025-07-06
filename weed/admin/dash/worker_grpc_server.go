package dash

import (
	"context"
	"fmt"
	"io"
	"net"
	"sync"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/worker_pb"
	"github.com/seaweedfs/seaweedfs/weed/security"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"google.golang.org/grpc"
	"google.golang.org/grpc/peer"
)

// WorkerGrpcServer implements the WorkerService gRPC interface
type WorkerGrpcServer struct {
	worker_pb.UnimplementedWorkerServiceServer
	adminServer *AdminServer

	// Worker connection management
	connections map[string]*WorkerConnection
	connMutex   sync.RWMutex

	// gRPC server
	grpcServer *grpc.Server
	listener   net.Listener
	running    bool
	stopChan   chan struct{}
}

// WorkerConnection represents an active worker connection
type WorkerConnection struct {
	workerID      string
	stream        worker_pb.WorkerService_WorkerStreamServer
	lastSeen      time.Time
	capabilities  []MaintenanceTaskType
	address       string
	maxConcurrent int32
	outgoing      chan *worker_pb.AdminMessage
	ctx           context.Context
	cancel        context.CancelFunc
}

// NewWorkerGrpcServer creates a new gRPC server for worker connections
func NewWorkerGrpcServer(adminServer *AdminServer) *WorkerGrpcServer {
	return &WorkerGrpcServer{
		adminServer: adminServer,
		connections: make(map[string]*WorkerConnection),
		stopChan:    make(chan struct{}),
	}
}

// StartWithTLS starts the gRPC server on the specified port with optional TLS
func (s *WorkerGrpcServer) StartWithTLS(port int) error {
	if s.running {
		return fmt.Errorf("worker gRPC server is already running")
	}

	// Create listener
	listener, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
	if err != nil {
		return fmt.Errorf("failed to listen on port %d: %v", port, err)
	}

	// Create gRPC server with optional TLS
	grpcServer := pb.NewGrpcServer(security.LoadServerTLS(util.GetViper(), "grpc.admin"))

	worker_pb.RegisterWorkerServiceServer(grpcServer, s)

	s.grpcServer = grpcServer
	s.listener = listener
	s.running = true

	// Start cleanup routine
	go s.cleanupRoutine()

	// Start serving in a goroutine
	go func() {
		if err := s.grpcServer.Serve(listener); err != nil {
			if s.running {
				glog.Errorf("Worker gRPC server error: %v", err)
			}
		}
	}()

	return nil
}

// Stop stops the gRPC server
func (s *WorkerGrpcServer) Stop() error {
	if !s.running {
		return nil
	}

	s.running = false
	close(s.stopChan)

	// Close all worker connections
	s.connMutex.Lock()
	for _, conn := range s.connections {
		conn.cancel()
		close(conn.outgoing)
	}
	s.connections = make(map[string]*WorkerConnection)
	s.connMutex.Unlock()

	// Stop gRPC server
	if s.grpcServer != nil {
		s.grpcServer.GracefulStop()
	}

	// Close listener
	if s.listener != nil {
		s.listener.Close()
	}

	glog.Infof("Worker gRPC server stopped")
	return nil
}

// WorkerStream handles bidirectional communication with workers
func (s *WorkerGrpcServer) WorkerStream(stream worker_pb.WorkerService_WorkerStreamServer) error {
	ctx := stream.Context()

	// get client address
	address := findClientAddress(ctx)

	// Wait for initial registration message
	msg, err := stream.Recv()
	if err != nil {
		return fmt.Errorf("failed to receive registration message: %v", err)
	}

	registration := msg.GetRegistration()
	if registration == nil {
		return fmt.Errorf("first message must be registration")
	}
	registration.Address = address

	workerID := registration.WorkerId
	if workerID == "" {
		return fmt.Errorf("worker ID cannot be empty")
	}

	glog.Infof("Worker %s connecting from %s", workerID, registration.Address)

	// Create worker connection
	connCtx, connCancel := context.WithCancel(ctx)
	conn := &WorkerConnection{
		workerID:      workerID,
		stream:        stream,
		lastSeen:      time.Now(),
		address:       registration.Address,
		maxConcurrent: registration.MaxConcurrent,
		outgoing:      make(chan *worker_pb.AdminMessage, 100),
		ctx:           connCtx,
		cancel:        connCancel,
	}

	// Convert capabilities
	capabilities := make([]MaintenanceTaskType, len(registration.Capabilities))
	for i, cap := range registration.Capabilities {
		capabilities[i] = MaintenanceTaskType(cap)
	}
	conn.capabilities = capabilities

	// Register connection
	s.connMutex.Lock()
	s.connections[workerID] = conn
	s.connMutex.Unlock()

	// Register worker with maintenance manager
	s.registerWorkerWithManager(conn)

	// Send registration response
	regResponse := &worker_pb.AdminMessage{
		Timestamp: time.Now().Unix(),
		Message: &worker_pb.AdminMessage_RegistrationResponse{
			RegistrationResponse: &worker_pb.RegistrationResponse{
				Success: true,
				Message: "Worker registered successfully",
			},
		},
	}

	select {
	case conn.outgoing <- regResponse:
	case <-time.After(5 * time.Second):
		glog.Errorf("Failed to send registration response to worker %s", workerID)
	}

	// Start outgoing message handler
	go s.handleOutgoingMessages(conn)

	// Handle incoming messages
	for {
		select {
		case <-ctx.Done():
			glog.Infof("Worker %s connection closed: %v", workerID, ctx.Err())
			s.unregisterWorker(workerID)
			return nil
		case <-connCtx.Done():
			glog.Infof("Worker %s connection cancelled", workerID)
			s.unregisterWorker(workerID)
			return nil
		default:
		}

		msg, err := stream.Recv()
		if err != nil {
			if err == io.EOF {
				glog.Infof("Worker %s disconnected", workerID)
			} else {
				glog.Errorf("Error receiving from worker %s: %v", workerID, err)
			}
			s.unregisterWorker(workerID)
			return err
		}

		conn.lastSeen = time.Now()
		s.handleWorkerMessage(conn, msg)
	}
}

// handleOutgoingMessages sends messages to worker
func (s *WorkerGrpcServer) handleOutgoingMessages(conn *WorkerConnection) {
	for {
		select {
		case <-conn.ctx.Done():
			return
		case msg, ok := <-conn.outgoing:
			if !ok {
				return
			}

			if err := conn.stream.Send(msg); err != nil {
				glog.Errorf("Failed to send message to worker %s: %v", conn.workerID, err)
				conn.cancel()
				return
			}
		}
	}
}

// handleWorkerMessage processes incoming messages from workers
func (s *WorkerGrpcServer) handleWorkerMessage(conn *WorkerConnection, msg *worker_pb.WorkerMessage) {
	workerID := conn.workerID

	switch m := msg.Message.(type) {
	case *worker_pb.WorkerMessage_Heartbeat:
		s.handleHeartbeat(conn, m.Heartbeat)

	case *worker_pb.WorkerMessage_TaskRequest:
		s.handleTaskRequest(conn, m.TaskRequest)

	case *worker_pb.WorkerMessage_TaskUpdate:
		s.handleTaskUpdate(conn, m.TaskUpdate)

	case *worker_pb.WorkerMessage_TaskComplete:
		s.handleTaskCompletion(conn, m.TaskComplete)

	case *worker_pb.WorkerMessage_Shutdown:
		glog.Infof("Worker %s shutting down: %s", workerID, m.Shutdown.Reason)
		s.unregisterWorker(workerID)

	default:
		glog.Warningf("Unknown message type from worker %s", workerID)
	}
}

// registerWorkerWithManager registers the worker with the maintenance manager
func (s *WorkerGrpcServer) registerWorkerWithManager(conn *WorkerConnection) {
	if s.adminServer.maintenanceManager == nil {
		return
	}

	worker := &MaintenanceWorker{
		ID:            conn.workerID,
		Address:       conn.address,
		LastHeartbeat: time.Now(),
		Status:        "active",
		Capabilities:  conn.capabilities,
		MaxConcurrent: int(conn.maxConcurrent),
		CurrentLoad:   0,
	}

	s.adminServer.maintenanceManager.RegisterWorker(worker)
	glog.V(1).Infof("Registered worker %s with maintenance manager", conn.workerID)
}

// handleHeartbeat processes heartbeat messages
func (s *WorkerGrpcServer) handleHeartbeat(conn *WorkerConnection, heartbeat *worker_pb.WorkerHeartbeat) {
	if s.adminServer.maintenanceManager != nil {
		s.adminServer.maintenanceManager.UpdateWorkerHeartbeat(conn.workerID)
	}

	// Send heartbeat response
	response := &worker_pb.AdminMessage{
		Timestamp: time.Now().Unix(),
		Message: &worker_pb.AdminMessage_HeartbeatResponse{
			HeartbeatResponse: &worker_pb.HeartbeatResponse{
				Success: true,
				Message: "Heartbeat acknowledged",
			},
		},
	}

	select {
	case conn.outgoing <- response:
	case <-time.After(time.Second):
		glog.Warningf("Failed to send heartbeat response to worker %s", conn.workerID)
	}
}

// handleTaskRequest processes task requests from workers
func (s *WorkerGrpcServer) handleTaskRequest(conn *WorkerConnection, request *worker_pb.TaskRequest) {
	if s.adminServer.maintenanceManager == nil {
		return
	}

	// Get next task from maintenance manager
	task := s.adminServer.maintenanceManager.GetNextTask(conn.workerID, conn.capabilities)

	if task != nil {
		// Send task assignment
		assignment := &worker_pb.AdminMessage{
			Timestamp: time.Now().Unix(),
			Message: &worker_pb.AdminMessage_TaskAssignment{
				TaskAssignment: &worker_pb.TaskAssignment{
					TaskId:   task.ID,
					TaskType: string(task.Type),
					Params: &worker_pb.TaskParams{
						VolumeId:   task.VolumeID,
						Server:     task.Server,
						Collection: task.Collection,
						Parameters: convertTaskParameters(task.Parameters),
					},
					Priority:    int32(task.Priority),
					CreatedTime: time.Now().Unix(),
				},
			},
		}

		select {
		case conn.outgoing <- assignment:
			glog.V(2).Infof("Assigned task %s to worker %s", task.ID, conn.workerID)
		case <-time.After(time.Second):
			glog.Warningf("Failed to send task assignment to worker %s", conn.workerID)
		}
	}
}

// handleTaskUpdate processes task progress updates
func (s *WorkerGrpcServer) handleTaskUpdate(conn *WorkerConnection, update *worker_pb.TaskUpdate) {
	if s.adminServer.maintenanceManager != nil {
		s.adminServer.maintenanceManager.UpdateTaskProgress(update.TaskId, float64(update.Progress))
		glog.V(3).Infof("Updated task %s progress: %.1f%%", update.TaskId, update.Progress)
	}
}

// handleTaskCompletion processes task completion notifications
func (s *WorkerGrpcServer) handleTaskCompletion(conn *WorkerConnection, completion *worker_pb.TaskComplete) {
	if s.adminServer.maintenanceManager != nil {
		errorMsg := ""
		if !completion.Success {
			errorMsg = completion.ErrorMessage
		}
		s.adminServer.maintenanceManager.CompleteTask(completion.TaskId, errorMsg)

		if completion.Success {
			glog.V(1).Infof("Worker %s completed task %s successfully", conn.workerID, completion.TaskId)
		} else {
			glog.Errorf("Worker %s failed task %s: %s", conn.workerID, completion.TaskId, completion.ErrorMessage)
		}
	}
}

// unregisterWorker removes a worker connection
func (s *WorkerGrpcServer) unregisterWorker(workerID string) {
	s.connMutex.Lock()
	if conn, exists := s.connections[workerID]; exists {
		conn.cancel()
		close(conn.outgoing)
		delete(s.connections, workerID)
	}
	s.connMutex.Unlock()

	glog.V(1).Infof("Unregistered worker %s", workerID)
}

// cleanupRoutine periodically cleans up stale connections
func (s *WorkerGrpcServer) cleanupRoutine() {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-s.stopChan:
			return
		case <-ticker.C:
			s.cleanupStaleConnections()
		}
	}
}

// cleanupStaleConnections removes connections that haven't been seen recently
func (s *WorkerGrpcServer) cleanupStaleConnections() {
	cutoff := time.Now().Add(-2 * time.Minute)

	s.connMutex.Lock()
	defer s.connMutex.Unlock()

	for workerID, conn := range s.connections {
		if conn.lastSeen.Before(cutoff) {
			glog.Warningf("Cleaning up stale worker connection: %s", workerID)
			conn.cancel()
			close(conn.outgoing)
			delete(s.connections, workerID)
		}
	}
}

// GetConnectedWorkers returns a list of currently connected workers
func (s *WorkerGrpcServer) GetConnectedWorkers() []string {
	s.connMutex.RLock()
	defer s.connMutex.RUnlock()

	workers := make([]string, 0, len(s.connections))
	for workerID := range s.connections {
		workers = append(workers, workerID)
	}
	return workers
}

// convertTaskParameters converts task parameters to protobuf format
func convertTaskParameters(params map[string]interface{}) map[string]string {
	result := make(map[string]string)
	for key, value := range params {
		result[key] = fmt.Sprintf("%v", value)
	}
	return result
}

func findClientAddress(ctx context.Context) string {
	// fmt.Printf("FromContext %+v\n", ctx)
	pr, ok := peer.FromContext(ctx)
	if !ok {
		glog.Error("failed to get peer from ctx")
		return ""
	}
	if pr.Addr == net.Addr(nil) {
		glog.Error("failed to get peer address")
		return ""
	}
	return pr.Addr.String()
}
