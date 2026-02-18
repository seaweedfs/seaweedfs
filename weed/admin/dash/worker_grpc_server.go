package dash

import (
	"context"
	"fmt"
	"io"
	"net"
	"strconv"
	"sync"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/admin/maintenance"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/plugin_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/worker_pb"
	"github.com/seaweedfs/seaweedfs/weed/security"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"google.golang.org/grpc"
	"google.golang.org/grpc/peer"
)

const (
	maxLogFetchLimit   = 1000
	maxLogMessageSize  = 2000
	maxLogFieldsCount  = 20
	logRequestTimeout  = 10 * time.Second
	logResponseTimeout = 30 * time.Second
	logSendTimeout     = 10 * time.Second
)

// WorkerGrpcServer implements the WorkerService gRPC interface
type WorkerGrpcServer struct {
	worker_pb.UnimplementedWorkerServiceServer
	adminServer *AdminServer

	// Worker connection management
	connections map[string]*WorkerConnection
	connMutex   sync.RWMutex

	// Log request correlation
	pendingLogRequests map[string]*LogRequestContext
	logRequestsMutex   sync.RWMutex

	// gRPC server
	grpcServer *grpc.Server
	listener   net.Listener
	running    bool
	stopChan   chan struct{}
}

// LogRequestContext tracks pending log requests
type LogRequestContext struct {
	TaskID     string
	WorkerID   string
	ResponseCh chan *worker_pb.TaskLogResponse
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
		adminServer:        adminServer,
		connections:        make(map[string]*WorkerConnection),
		pendingLogRequests: make(map[string]*LogRequestContext),
		stopChan:           make(chan struct{}),
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
	if plugin := s.adminServer.GetPlugin(); plugin != nil {
		plugin_pb.RegisterPluginControlServiceServer(grpcServer, plugin)
		glog.V(0).Infof("Plugin gRPC service registered on worker gRPC server")
	}

	s.grpcServer = grpcServer
	s.listener = listener
	s.running = true

	// Start background routines
	go s.cleanupRoutine()
	go s.activeLogFetchLoop()

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

// ListenPort returns the currently bound worker gRPC listen port.
func (s *WorkerGrpcServer) ListenPort() int {
	if s == nil || s.listener == nil {
		return 0
	}
	if tcpAddr, ok := s.listener.Addr().(*net.TCPAddr); ok {
		return tcpAddr.Port
	}
	_, portStr, err := net.SplitHostPort(s.listener.Addr().String())
	if err != nil {
		return 0
	}
	port, err := strconv.Atoi(portStr)
	if err != nil {
		return 0
	}
	return port
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
		s.safeCloseOutgoingChannel(conn, "Stop")
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
		return fmt.Errorf("failed to receive registration message: %w", err)
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

	// Register connection - clean up old connection if worker is reconnecting
	s.connMutex.Lock()
	if oldConn, exists := s.connections[workerID]; exists {
		glog.Infof("Worker %s reconnected, cleaning up old connection", workerID)
		// Cancel old connection to stop its goroutines
		oldConn.cancel()
		// Don't close oldConn.outgoing here as it may cause panic in handleOutgoingMessages
		// Let the goroutine exit naturally when it detects context cancellation
	}
	s.connections[workerID] = conn
	s.connMutex.Unlock()

	// Register worker with maintenance manager
	s.registerWorkerWithManager(conn)

	// IMPORTANT: Start outgoing message handler BEFORE sending registration response
	// This ensures the handler is ready to process messages and prevents race conditions
	// where the worker might send requests before we're ready to respond
	go s.handleOutgoingMessages(conn)

	// Send registration response (after handler is started)
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
		glog.V(1).Infof("Registration response sent to worker %s", workerID)
	case <-time.After(5 * time.Second):
		glog.Errorf("Failed to send registration response to worker %s", workerID)
	}

	// Handle incoming messages
	for {
		select {
		case <-ctx.Done():
			glog.Infof("Worker %s connection closed: %v", workerID, ctx.Err())
			s.unregisterWorker(conn)
			return nil
		case <-connCtx.Done():
			glog.Infof("Worker %s connection cancelled", workerID)
			s.unregisterWorker(conn)
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
			s.unregisterWorker(conn)
			return err
		}

		s.connMutex.Lock()
		conn.lastSeen = time.Now()
		s.connMutex.Unlock()
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

	case *worker_pb.WorkerMessage_TaskLogResponse:
		s.handleTaskLogResponse(conn, m.TaskLogResponse)

	case *worker_pb.WorkerMessage_Shutdown:
		glog.Infof("Worker %s shutting down: %s", workerID, m.Shutdown.Reason)
		s.unregisterWorker(conn)

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

		// Use typed params directly - master client should already be configured in the params
		var taskParams *worker_pb.TaskParams
		if task.TypedParams != nil {
			taskParams = task.TypedParams
		} else {
			// Create basic params if none exist
			taskParams = &worker_pb.TaskParams{
				VolumeId:   task.VolumeID,
				Collection: task.Collection,
				Sources: []*worker_pb.TaskSource{
					{
						Node:     task.Server,
						VolumeId: task.VolumeID,
					},
				},
			}
		}

		// Send task assignment
		assignment := &worker_pb.AdminMessage{
			Timestamp: time.Now().Unix(),
			Message: &worker_pb.AdminMessage_TaskAssignment{
				TaskAssignment: &worker_pb.TaskAssignment{
					TaskId:      task.ID,
					TaskType:    string(task.Type),
					Params:      taskParams,
					Priority:    int32(task.Priority),
					CreatedTime: time.Now().Unix(),
				},
			},
		}

		select {
		case conn.outgoing <- assignment:
		case <-time.After(time.Second):
			glog.Warningf("Failed to send task assignment to worker %s", conn.workerID)
		}
	} else {
		// Send explicit "No Task" response to prevent worker timeout
		// Workers expect a TaskAssignment message but will sleep if TaskId is empty
		noTaskAssignment := &worker_pb.AdminMessage{
			Timestamp: time.Now().Unix(),
			Message: &worker_pb.AdminMessage_TaskAssignment{
				TaskAssignment: &worker_pb.TaskAssignment{
					TaskId: "", // Empty TaskId indicates no task available
				},
			},
		}

		select {
		case conn.outgoing <- noTaskAssignment:
			glog.V(4).Infof("Sent 'No Task' response to worker %s", conn.workerID)
		case <-time.After(time.Second):
			// If we can't send, the worker will eventually time out and reconnect, which is fine
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

		// Fetch and persist logs
		go s.FetchAndSaveLogs(conn.workerID, completion.TaskId)
	}
}

// FetchAndSaveLogs retrieves logs from a worker and saves them to disk
func (s *WorkerGrpcServer) FetchAndSaveLogs(workerID, taskID string) error {
	// Add a small initial delay to allow worker to finalize and sync logs
	// especially when this is called immediately after TaskComplete
	time.Sleep(300 * time.Millisecond)

	var workerLogs []*worker_pb.TaskLogEntry
	var err error

	// Retry a few times if fetch fails, as logs might be in the middle of a terminal sync
	for attempt := 1; attempt <= 3; attempt++ {
		workerLogs, err = s.RequestTaskLogs(workerID, taskID, maxLogFetchLimit, "")
		if err == nil {
			break
		}
		if attempt < 3 {
			glog.V(1).Infof("Fetch logs attempt %d failed for task %s: %v. Retrying in 1s...", attempt, taskID, err)
			time.Sleep(1 * time.Second)
		}
	}

	if err != nil {
		glog.Warningf("Failed to fetch logs for task %s after 3 attempts: %v", taskID, err)
		return err
	}

	// Convert logs
	var maintenanceLogs []*maintenance.TaskExecutionLog
	for _, workerLog := range workerLogs {
		maintenanceLog := &maintenance.TaskExecutionLog{
			Timestamp: time.Unix(workerLog.Timestamp, 0),
			Level:     workerLog.Level,
			Message:   workerLog.Message,
			Source:    "worker",
			TaskID:    taskID,
			WorkerID:  workerID,
		}

		// Truncate very long messages to prevent rendering issues and disk bloat
		if len(maintenanceLog.Message) > maxLogMessageSize {
			maintenanceLog.Message = maintenanceLog.Message[:maxLogMessageSize] + "... (truncated)"
		}

		// carry structured fields if present
		if len(workerLog.Fields) > 0 {
			maintenanceLog.Fields = make(map[string]string)
			fieldCount := 0
			for k, v := range workerLog.Fields {
				if fieldCount >= maxLogFieldsCount {
					maintenanceLog.Fields["..."] = fmt.Sprintf("(%d more fields truncated)", len(workerLog.Fields)-maxLogFieldsCount)
					break
				}
				maintenanceLog.Fields[k] = v
				fieldCount++
			}
		}

		// carry optional progress/status
		if workerLog.Progress != 0 {
			p := float64(workerLog.Progress)
			maintenanceLog.Progress = &p
		}
		if workerLog.Status != "" {
			maintenanceLog.Status = workerLog.Status
		}
		maintenanceLogs = append(maintenanceLogs, maintenanceLog)
	}

	// Persist logs
	if s.adminServer.configPersistence != nil {
		if err := s.adminServer.configPersistence.SaveTaskExecutionLogs(taskID, maintenanceLogs); err != nil {
			glog.Errorf("Failed to persist logs for task %s: %v", taskID, err)
			return err
		}
	}
	return nil
}

// handleTaskLogResponse processes task log responses from workers
func (s *WorkerGrpcServer) handleTaskLogResponse(conn *WorkerConnection, response *worker_pb.TaskLogResponse) {
	requestKey := fmt.Sprintf("%s:%s", response.WorkerId, response.TaskId)

	s.logRequestsMutex.RLock()
	requestContext, exists := s.pendingLogRequests[requestKey]
	s.logRequestsMutex.RUnlock()

	if !exists {
		glog.Warningf("Received unexpected log response for task %s from worker %s", response.TaskId, response.WorkerId)
		return
	}

	glog.V(1).Infof("Received log response for task %s from worker %s: %d entries", response.TaskId, response.WorkerId, len(response.LogEntries))

	// Send response to waiting channel
	select {
	case requestContext.ResponseCh <- response:
		// Response delivered successfully
	case <-time.After(time.Second):
		glog.Warningf("Failed to deliver log response for task %s from worker %s: timeout", response.TaskId, response.WorkerId)
	}

	// Clean up the pending request
	s.logRequestsMutex.Lock()
	delete(s.pendingLogRequests, requestKey)
	s.logRequestsMutex.Unlock()
}

// safeCloseOutgoingChannel safely closes the outgoing channel for a worker connection.
func (s *WorkerGrpcServer) safeCloseOutgoingChannel(conn *WorkerConnection, source string) {
	defer func() {
		if r := recover(); r != nil {
			glog.V(1).Infof("%s: recovered from panic closing outgoing channel for worker %s: %v", source, conn.workerID, r)
		}
	}()
	close(conn.outgoing)
}

// unregisterWorker removes a worker connection
func (s *WorkerGrpcServer) unregisterWorker(conn *WorkerConnection) {
	s.connMutex.Lock()
	existingConn, exists := s.connections[conn.workerID]
	if !exists {
		s.connMutex.Unlock()
		glog.V(2).Infof("unregisterWorker: worker %s not found in connections map (already unregistered)", conn.workerID)
		return
	}

	// Only remove if it matches the specific connection instance
	if existingConn != conn {
		s.connMutex.Unlock()
		glog.V(1).Infof("unregisterWorker: worker %s connection replaced, skipping unregister for old connection", conn.workerID)
		return
	}

	// Remove from map first to prevent duplicate cleanup attempts
	delete(s.connections, conn.workerID)
	s.connMutex.Unlock()

	// Cancel context to signal goroutines to stop
	conn.cancel()

	// Safely close the outgoing channel with recover to handle potential double-close
	s.safeCloseOutgoingChannel(conn, "unregisterWorker")

	glog.V(1).Infof("Unregistered worker %s", conn.workerID)
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
	// collect connections to remove first to avoid deadlock if unregisterWorker locks
	var toRemove []*WorkerConnection
	for _, conn := range s.connections {
		if conn.lastSeen.Before(cutoff) {
			toRemove = append(toRemove, conn)
		}
	}
	s.connMutex.Unlock()

	for _, conn := range toRemove {
		glog.Warningf("Cleaning up stale worker connection: %s", conn.workerID)
		s.unregisterWorker(conn)
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

// RequestTaskLogs requests execution logs from a worker for a specific task
func (s *WorkerGrpcServer) RequestTaskLogs(workerID, taskID string, maxEntries int32, logLevel string) ([]*worker_pb.TaskLogEntry, error) {
	s.connMutex.RLock()
	conn, exists := s.connections[workerID]
	s.connMutex.RUnlock()

	if !exists {
		return nil, fmt.Errorf("worker %s is not connected", workerID)
	}

	// Create response channel for this request
	responseCh := make(chan *worker_pb.TaskLogResponse, 1)
	requestKey := fmt.Sprintf("%s:%s", workerID, taskID)

	// Register pending request
	requestContext := &LogRequestContext{
		TaskID:     taskID,
		WorkerID:   workerID,
		ResponseCh: responseCh,
	}

	s.logRequestsMutex.Lock()
	if _, exists := s.pendingLogRequests[requestKey]; exists {
		s.logRequestsMutex.Unlock()
		return nil, fmt.Errorf("a log request for task %s is already in progress", taskID)
	}
	s.pendingLogRequests[requestKey] = requestContext
	s.logRequestsMutex.Unlock()

	// Create log request message
	logRequest := &worker_pb.AdminMessage{
		AdminId:   "admin-server",
		Timestamp: time.Now().Unix(),
		Message: &worker_pb.AdminMessage_TaskLogRequest{
			TaskLogRequest: &worker_pb.TaskLogRequest{
				TaskId:          taskID,
				WorkerId:        workerID,
				IncludeMetadata: true,
				MaxEntries:      maxEntries,
				LogLevel:        logLevel,
			},
		},
	}

	// Send the request through the worker's outgoing channel
	select {
	case conn.outgoing <- logRequest:
		glog.V(1).Infof("Log request sent to worker %s for task %s", workerID, taskID)
	case <-time.After(logSendTimeout):
		// Clean up pending request on timeout
		s.logRequestsMutex.Lock()
		if s.pendingLogRequests[requestKey] == requestContext {
			delete(s.pendingLogRequests, requestKey)
		}
		s.logRequestsMutex.Unlock()
		return nil, fmt.Errorf("timeout sending log request to worker %s", workerID)
	}

	// Wait for response
	select {
	case response := <-responseCh:
		if !response.Success {
			return nil, fmt.Errorf("worker log request failed: %s", response.ErrorMessage)
		}
		glog.V(1).Infof("Received %d log entries for task %s from worker %s", len(response.LogEntries), taskID, workerID)
		return response.LogEntries, nil
	case <-time.After(logResponseTimeout):
		// Clean up pending request on timeout
		s.logRequestsMutex.Lock()
		if s.pendingLogRequests[requestKey] == requestContext {
			delete(s.pendingLogRequests, requestKey)
		}
		s.logRequestsMutex.Unlock()
		return nil, fmt.Errorf("timeout waiting for log response from worker %s", workerID)
	}
}

// RequestTaskLogsFromAllWorkers requests logs for a task from all connected workers
func (s *WorkerGrpcServer) RequestTaskLogsFromAllWorkers(taskID string, maxEntries int32, logLevel string) (map[string][]*worker_pb.TaskLogEntry, error) {
	s.connMutex.RLock()
	workerIDs := make([]string, 0, len(s.connections))
	for workerID := range s.connections {
		workerIDs = append(workerIDs, workerID)
	}
	s.connMutex.RUnlock()

	results := make(map[string][]*worker_pb.TaskLogEntry)

	for _, workerID := range workerIDs {
		logs, err := s.RequestTaskLogs(workerID, taskID, maxEntries, logLevel)
		if err != nil {
			glog.V(1).Infof("Failed to get logs from worker %s for task %s: %v", workerID, taskID, err)
			// Store empty result with error information for debugging
			results[workerID+"_error"] = []*worker_pb.TaskLogEntry{
				{
					Timestamp: time.Now().Unix(),
					Level:     "ERROR",
					Message:   fmt.Sprintf("Failed to retrieve logs from worker %s: %v", workerID, err),
					Fields:    map[string]string{"source": "admin"},
				},
			}
			continue
		}
		if len(logs) > 0 {
			results[workerID] = logs
		} else {
			glog.V(2).Infof("No logs found for task %s on worker %s", taskID, workerID)
		}
	}

	return results, nil
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

// activeLogFetchLoop periodically fetches logs for all in-progress tasks
func (s *WorkerGrpcServer) activeLogFetchLoop() {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-s.stopChan:
			return
		case <-ticker.C:
			if !s.running || s.adminServer == nil || s.adminServer.maintenanceManager == nil {
				continue
			}

			// Get all in-progress tasks
			tasks := s.adminServer.maintenanceManager.GetTasks(maintenance.TaskStatusInProgress, "", 0)
			if len(tasks) == 0 {
				continue
			}

			glog.V(2).Infof("Background log fetcher: found %d in-progress tasks", len(tasks))
			for _, task := range tasks {
				if task.WorkerID != "" {
					// Use a goroutine to avoid blocking the loop
					go func(wID, tID string) {
						if err := s.FetchAndSaveLogs(wID, tID); err != nil {
							glog.V(2).Infof("Background log fetch failed for task %s on worker %s: %v", tID, wID, err)
						}
					}(task.WorkerID, task.ID)
				}
			}
		}
	}
}
