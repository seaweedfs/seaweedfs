package worker

import (
	"context"
	"fmt"
	"net"
	"strconv"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/volume_server_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/worker_pb"
)

// ECWorker implements maintenance worker with actual EC functionality
type ECWorker struct {
	workerID      string
	adminAddress  string
	grpcAddress   string
	capabilities  []string
	maxConcurrent int

	// gRPC server and client
	server      *grpc.Server
	adminConn   *grpc.ClientConn
	adminClient worker_pb.WorkerServiceClient
	adminStream worker_pb.WorkerService_WorkerStreamClient

	// Task management
	currentTasks map[string]*ActiveTask
	taskMutex    sync.RWMutex

	// Control
	running bool
	stopCh  chan struct{}
	mutex   sync.RWMutex
}

// ActiveTask represents a task currently being executed
type ActiveTask struct {
	ID         string
	Type       string
	VolumeID   uint32
	Server     string
	Parameters map[string]string
	StartedAt  time.Time
	Progress   float32
	Status     string
	Context    context.Context
	Cancel     context.CancelFunc
}

// NewECWorker creates a new EC worker
func NewECWorker(workerID, adminAddress, grpcAddress string) *ECWorker {
	return &ECWorker{
		workerID:      workerID,
		adminAddress:  adminAddress,
		grpcAddress:   grpcAddress,
		capabilities:  []string{"ec_encode", "ec_rebuild", "vacuum"},
		maxConcurrent: 2, // Can handle 2 concurrent tasks
		currentTasks:  make(map[string]*ActiveTask),
		stopCh:        make(chan struct{}),
	}
}

// Start starts the worker
func (w *ECWorker) Start() error {
	w.mutex.Lock()
	defer w.mutex.Unlock()

	if w.running {
		return fmt.Errorf("worker already running")
	}

	glog.Infof("Starting EC worker %s", w.workerID)

	// Start gRPC server
	err := w.startGRPCServer()
	if err != nil {
		return fmt.Errorf("failed to start gRPC server: %v", err)
	}

	// Connect to admin server
	err = w.connectToAdmin()
	if err != nil {
		return fmt.Errorf("failed to connect to admin: %v", err)
	}

	w.running = true

	// Start background goroutines
	go w.adminCommunicationLoop()
	go w.heartbeatLoop()
	go w.taskRequestLoop()

	glog.Infof("EC worker %s started successfully", w.workerID)
	return nil
}

// Stop stops the worker
func (w *ECWorker) Stop() {
	w.mutex.Lock()
	defer w.mutex.Unlock()

	if !w.running {
		return
	}

	glog.Infof("Stopping EC worker %s", w.workerID)

	close(w.stopCh)

	// Cancel all active tasks
	w.taskMutex.Lock()
	for _, task := range w.currentTasks {
		task.Cancel()
	}
	w.taskMutex.Unlock()

	// Close connections
	if w.adminConn != nil {
		w.adminConn.Close()
	}

	if w.server != nil {
		w.server.Stop()
	}

	w.running = false
	glog.Infof("EC worker %s stopped", w.workerID)
}

// startGRPCServer starts the worker's gRPC server
func (w *ECWorker) startGRPCServer() error {
	listener, err := net.Listen("tcp", w.grpcAddress)
	if err != nil {
		return fmt.Errorf("failed to listen on %s: %v", w.grpcAddress, err)
	}

	w.server = grpc.NewServer()
	// Register any worker-specific services here

	go func() {
		err := w.server.Serve(listener)
		if err != nil {
			glog.Errorf("gRPC server error: %v", err)
		}
	}()

	glog.Infof("Worker gRPC server listening on %s", w.grpcAddress)
	return nil
}

// connectToAdmin establishes connection to admin server
func (w *ECWorker) connectToAdmin() error {
	// Convert to gRPC address (HTTP port + 10000)
	grpcAddress := pb.ServerToGrpcAddress(w.adminAddress)
	conn, err := grpc.NewClient(grpcAddress, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return fmt.Errorf("failed to connect to admin at %s: %v", w.adminAddress, err)
	}

	w.adminConn = conn
	w.adminClient = worker_pb.NewWorkerServiceClient(conn)

	// Create bidirectional stream
	stream, err := w.adminClient.WorkerStream(context.Background())
	if err != nil {
		return fmt.Errorf("failed to create admin stream: %v", err)
	}

	w.adminStream = stream

	// Send registration message
	err = w.sendRegistration()
	if err != nil {
		return fmt.Errorf("failed to register with admin: %v", err)
	}

	glog.Infof("Connected to admin server at %s", w.adminAddress)
	return nil
}

// sendRegistration sends worker registration to admin
func (w *ECWorker) sendRegistration() error {
	registration := &worker_pb.WorkerMessage{
		WorkerId:  w.workerID,
		Timestamp: time.Now().Unix(),
		Message: &worker_pb.WorkerMessage_Registration{
			Registration: &worker_pb.WorkerRegistration{
				WorkerId:      w.workerID,
				Address:       w.grpcAddress,
				Capabilities:  w.capabilities,
				MaxConcurrent: int32(w.maxConcurrent),
				Metadata: map[string]string{
					"version": "1.0",
					"type":    "ec_worker",
				},
			},
		},
	}

	return w.adminStream.Send(registration)
}

// adminCommunicationLoop handles messages from admin server
func (w *ECWorker) adminCommunicationLoop() {
	for {
		select {
		case <-w.stopCh:
			return
		default:
		}

		msg, err := w.adminStream.Recv()
		if err != nil {
			glog.Errorf("Error receiving from admin: %v", err)
			time.Sleep(5 * time.Second) // Retry connection
			continue
		}

		w.handleAdminMessage(msg)
	}
}

// handleAdminMessage processes messages from admin server
func (w *ECWorker) handleAdminMessage(msg *worker_pb.AdminMessage) {
	switch message := msg.Message.(type) {
	case *worker_pb.AdminMessage_RegistrationResponse:
		w.handleRegistrationResponse(message.RegistrationResponse)
	case *worker_pb.AdminMessage_TaskAssignment:
		w.handleTaskAssignment(message.TaskAssignment)
	case *worker_pb.AdminMessage_TaskCancellation:
		w.handleTaskCancellation(message.TaskCancellation)
	case *worker_pb.AdminMessage_AdminShutdown:
		w.handleAdminShutdown(message.AdminShutdown)
	default:
		glog.Warningf("Unknown message type from admin")
	}
}

// handleRegistrationResponse processes registration response
func (w *ECWorker) handleRegistrationResponse(resp *worker_pb.RegistrationResponse) {
	if resp.Success {
		glog.Infof("Worker %s registered successfully with admin", w.workerID)
	} else {
		glog.Errorf("Worker registration failed: %s", resp.Message)
	}
}

// handleTaskAssignment processes task assignment from admin
func (w *ECWorker) handleTaskAssignment(assignment *worker_pb.TaskAssignment) {
	glog.Infof("Received task assignment: %s (%s) for volume %d",
		assignment.TaskId, assignment.TaskType, assignment.Params.VolumeId)

	// Check if we can accept the task
	w.taskMutex.RLock()
	currentLoad := len(w.currentTasks)
	w.taskMutex.RUnlock()

	if currentLoad >= w.maxConcurrent {
		glog.Warningf("Worker at capacity, cannot accept task %s", assignment.TaskId)
		return
	}

	// Create active task
	ctx, cancel := context.WithCancel(context.Background())
	task := &ActiveTask{
		ID:         assignment.TaskId,
		Type:       assignment.TaskType,
		VolumeID:   assignment.Params.VolumeId,
		Server:     assignment.Params.Server,
		Parameters: assignment.Params.Parameters,
		StartedAt:  time.Now(),
		Progress:   0.0,
		Status:     "started",
		Context:    ctx,
		Cancel:     cancel,
	}

	w.taskMutex.Lock()
	w.currentTasks[assignment.TaskId] = task
	w.taskMutex.Unlock()

	// Start task execution
	go w.executeTask(task)
}

// handleTaskCancellation processes task cancellation
func (w *ECWorker) handleTaskCancellation(cancellation *worker_pb.TaskCancellation) {
	glog.Infof("Received task cancellation: %s", cancellation.TaskId)

	w.taskMutex.Lock()
	defer w.taskMutex.Unlock()

	if task, exists := w.currentTasks[cancellation.TaskId]; exists {
		task.Cancel()
		delete(w.currentTasks, cancellation.TaskId)
		glog.Infof("Cancelled task %s", cancellation.TaskId)
	}
}

// handleAdminShutdown processes admin shutdown notification
func (w *ECWorker) handleAdminShutdown(shutdown *worker_pb.AdminShutdown) {
	glog.Infof("Admin server shutting down: %s", shutdown.Reason)
	w.Stop()
}

// heartbeatLoop sends periodic heartbeats to admin
func (w *ECWorker) heartbeatLoop() {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			w.sendHeartbeat()
		case <-w.stopCh:
			return
		}
	}
}

// sendHeartbeat sends heartbeat to admin server
func (w *ECWorker) sendHeartbeat() {
	w.taskMutex.RLock()
	currentLoad := len(w.currentTasks)
	taskIDs := make([]string, 0, len(w.currentTasks))
	for taskID := range w.currentTasks {
		taskIDs = append(taskIDs, taskID)
	}
	w.taskMutex.RUnlock()

	heartbeat := &worker_pb.WorkerMessage{
		WorkerId:  w.workerID,
		Timestamp: time.Now().Unix(),
		Message: &worker_pb.WorkerMessage_Heartbeat{
			Heartbeat: &worker_pb.WorkerHeartbeat{
				WorkerId:       w.workerID,
				Status:         "active",
				CurrentLoad:    int32(currentLoad),
				MaxConcurrent:  int32(w.maxConcurrent),
				CurrentTaskIds: taskIDs,
				TasksCompleted: 0,                                       // TODO: Track completed tasks
				TasksFailed:    0,                                       // TODO: Track failed tasks
				UptimeSeconds:  int64(time.Since(time.Now()).Seconds()), // TODO: Track actual uptime
			},
		},
	}

	if err := w.adminStream.Send(heartbeat); err != nil {
		glog.Errorf("Failed to send heartbeat: %v", err)
	}
}

// taskRequestLoop periodically requests new tasks from admin
func (w *ECWorker) taskRequestLoop() {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			w.requestTasks()
		case <-w.stopCh:
			return
		}
	}
}

// requestTasks requests new tasks from admin if we have capacity
func (w *ECWorker) requestTasks() {
	w.taskMutex.RLock()
	currentLoad := len(w.currentTasks)
	w.taskMutex.RUnlock()

	availableSlots := w.maxConcurrent - currentLoad
	if availableSlots <= 0 {
		return // No capacity
	}

	request := &worker_pb.WorkerMessage{
		WorkerId:  w.workerID,
		Timestamp: time.Now().Unix(),
		Message: &worker_pb.WorkerMessage_TaskRequest{
			TaskRequest: &worker_pb.TaskRequest{
				WorkerId:       w.workerID,
				Capabilities:   w.capabilities,
				AvailableSlots: int32(availableSlots),
			},
		},
	}

	if err := w.adminStream.Send(request); err != nil {
		glog.Errorf("Failed to request tasks: %v", err)
	}
}

// executeTask executes a task based on its type
func (w *ECWorker) executeTask(task *ActiveTask) {
	defer func() {
		w.taskMutex.Lock()
		delete(w.currentTasks, task.ID)
		w.taskMutex.Unlock()
	}()

	glog.Infof("Starting execution of task %s (%s) for volume %d",
		task.ID, task.Type, task.VolumeID)

	var err error
	var success bool

	switch task.Type {
	case "ec_encode":
		success, err = w.executeECEncode(task)
	case "ec_rebuild":
		success, err = w.executeECRebuild(task)
	case "vacuum":
		success, err = w.executeVacuum(task)
	default:
		err = fmt.Errorf("unknown task type: %s", task.Type)
		success = false
	}

	// Send completion message
	w.sendTaskCompletion(task, success, err)

	if success {
		glog.Infof("Task %s completed successfully", task.ID)
	} else {
		glog.Errorf("Task %s failed: %v", task.ID, err)
	}
}

// executeECEncode performs actual EC encoding on a volume
func (w *ECWorker) executeECEncode(task *ActiveTask) (bool, error) {
	glog.Infof("Performing EC encoding on volume %d", task.VolumeID)

	// Update progress
	w.sendTaskUpdate(task, 0.1, "Initializing EC encoding")

	// Connect to volume server
	volumeServerAddress := task.Server
	if volumeServerAddress == "" {
		return false, fmt.Errorf("no volume server address provided")
	}

	// Convert to gRPC address (HTTP port + 10000)
	grpcAddress := pb.ServerToGrpcAddress(volumeServerAddress)
	conn, err := grpc.NewClient(grpcAddress, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return false, fmt.Errorf("failed to connect to volume server %s: %v", volumeServerAddress, err)
	}
	defer conn.Close()

	client := volume_server_pb.NewVolumeServerClient(conn)

	// Step 1: Generate EC shards
	w.sendTaskUpdate(task, 0.2, "Generating EC shards")

	generateReq := &volume_server_pb.VolumeEcShardsGenerateRequest{
		VolumeId:   task.VolumeID,
		Collection: task.Parameters["collection"],
	}

	_, err = client.VolumeEcShardsGenerate(task.Context, generateReq)
	if err != nil {
		return false, fmt.Errorf("EC shard generation failed: %v", err)
	}

	w.sendTaskUpdate(task, 0.6, "EC shards generated successfully")

	// Step 2: Mount EC volume
	w.sendTaskUpdate(task, 0.8, "Mounting EC volume")

	mountReq := &volume_server_pb.VolumeEcShardsMountRequest{
		VolumeId:   task.VolumeID,
		Collection: task.Parameters["collection"],
		ShardIds:   []uint32{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13}, // All EC shards
	}

	_, err = client.VolumeEcShardsMount(task.Context, mountReq)
	if err != nil {
		return false, fmt.Errorf("EC shard mount failed: %v", err)
	}

	// Step 3: Mark original volume as read-only
	w.sendTaskUpdate(task, 0.9, "Marking volume read-only")

	readOnlyReq := &volume_server_pb.VolumeMarkReadonlyRequest{
		VolumeId: task.VolumeID,
	}

	_, err = client.VolumeMarkReadonly(task.Context, readOnlyReq)
	if err != nil {
		glog.Warningf("Failed to mark volume %d read-only: %v", task.VolumeID, err)
		// This is not a critical failure for EC encoding
	}

	w.sendTaskUpdate(task, 1.0, "EC encoding completed")

	return true, nil
}

// executeECRebuild performs EC shard rebuilding
func (w *ECWorker) executeECRebuild(task *ActiveTask) (bool, error) {
	glog.Infof("Performing EC rebuild on volume %d", task.VolumeID)

	w.sendTaskUpdate(task, 0.1, "Initializing EC rebuild")

	// Connect to volume server
	grpcAddress := pb.ServerToGrpcAddress(task.Server)
	conn, err := grpc.NewClient(grpcAddress, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return false, fmt.Errorf("failed to connect to volume server: %v", err)
	}
	defer conn.Close()

	client := volume_server_pb.NewVolumeServerClient(conn)

	// Rebuild missing/corrupted shards
	w.sendTaskUpdate(task, 0.5, "Rebuilding EC shards")

	rebuildReq := &volume_server_pb.VolumeEcShardsRebuildRequest{
		VolumeId:   task.VolumeID,
		Collection: task.Parameters["collection"],
	}

	_, err = client.VolumeEcShardsRebuild(task.Context, rebuildReq)
	if err != nil {
		return false, fmt.Errorf("EC rebuild failed: %v", err)
	}

	w.sendTaskUpdate(task, 1.0, "EC rebuild completed")

	return true, nil
}

// executeVacuum performs volume vacuum operation
func (w *ECWorker) executeVacuum(task *ActiveTask) (bool, error) {
	glog.Infof("Performing vacuum on volume %d", task.VolumeID)

	w.sendTaskUpdate(task, 0.1, "Initializing vacuum")

	// Parse garbage threshold
	thresholdStr := task.Parameters["garbage_threshold"]
	if thresholdStr == "" {
		thresholdStr = "0.3" // Default 30%
	}

	threshold, err := strconv.ParseFloat(thresholdStr, 32)
	if err != nil {
		return false, fmt.Errorf("invalid garbage threshold: %v", err)
	}

	// Connect to volume server
	grpcAddress := pb.ServerToGrpcAddress(task.Server)
	conn, err := grpc.NewClient(grpcAddress, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return false, fmt.Errorf("failed to connect to volume server: %v", err)
	}
	defer conn.Close()

	client := volume_server_pb.NewVolumeServerClient(conn)

	// Step 1: Check vacuum eligibility
	w.sendTaskUpdate(task, 0.2, "Checking vacuum eligibility")

	checkReq := &volume_server_pb.VacuumVolumeCheckRequest{
		VolumeId: task.VolumeID,
	}

	checkResp, err := client.VacuumVolumeCheck(task.Context, checkReq)
	if err != nil {
		return false, fmt.Errorf("vacuum check failed: %v", err)
	}

	if checkResp.GarbageRatio < float64(threshold) {
		return true, fmt.Errorf("volume %d garbage ratio %.2f%% below threshold %.2f%%",
			task.VolumeID, checkResp.GarbageRatio*100, threshold*100)
	}

	// Step 2: Compact volume
	w.sendTaskUpdate(task, 0.4, "Compacting volume")

	compactReq := &volume_server_pb.VacuumVolumeCompactRequest{
		VolumeId: task.VolumeID,
	}

	compactStream, err := client.VacuumVolumeCompact(task.Context, compactReq)
	if err != nil {
		return false, fmt.Errorf("vacuum compact failed: %v", err)
	}

	// Process compact stream
	for {
		resp, err := compactStream.Recv()
		if err != nil {
			if err.Error() == "EOF" {
				break
			}
			return false, fmt.Errorf("vacuum compact stream error: %v", err)
		}

		progress := 0.4 + 0.4*(float64(resp.ProcessedBytes)/float64(resp.LoadAvg_1M)) // Rough progress estimate
		w.sendTaskUpdate(task, float32(progress), "Compacting volume")
	}

	// Step 3: Commit vacuum
	w.sendTaskUpdate(task, 0.9, "Committing vacuum")

	commitReq := &volume_server_pb.VacuumVolumeCommitRequest{
		VolumeId: task.VolumeID,
	}

	_, err = client.VacuumVolumeCommit(task.Context, commitReq)
	if err != nil {
		return false, fmt.Errorf("vacuum commit failed: %v", err)
	}

	// Step 4: Cleanup
	w.sendTaskUpdate(task, 0.95, "Cleaning up")

	cleanupReq := &volume_server_pb.VacuumVolumeCleanupRequest{
		VolumeId: task.VolumeID,
	}

	_, err = client.VacuumVolumeCleanup(task.Context, cleanupReq)
	if err != nil {
		glog.Warningf("Vacuum cleanup warning: %v", err)
		// Non-critical error
	}

	w.sendTaskUpdate(task, 1.0, "Vacuum completed successfully")

	return true, nil
}

// sendTaskUpdate sends task progress update to admin
func (w *ECWorker) sendTaskUpdate(task *ActiveTask, progress float32, message string) {
	task.Progress = progress
	task.Status = message

	update := &worker_pb.WorkerMessage{
		WorkerId:  w.workerID,
		Timestamp: time.Now().Unix(),
		Message: &worker_pb.WorkerMessage_TaskUpdate{
			TaskUpdate: &worker_pb.TaskUpdate{
				TaskId:   task.ID,
				WorkerId: w.workerID,
				Status:   task.Status,
				Progress: progress,
				Message:  message,
				Metadata: map[string]string{
					"updated_at": time.Now().Format(time.RFC3339),
				},
			},
		},
	}

	if err := w.adminStream.Send(update); err != nil {
		glog.Errorf("Failed to send task update: %v", err)
	}
}

// sendTaskCompletion sends task completion to admin
func (w *ECWorker) sendTaskCompletion(task *ActiveTask, success bool, taskErr error) {
	var errorMessage string
	if taskErr != nil {
		errorMessage = taskErr.Error()
	}

	completion := &worker_pb.WorkerMessage{
		WorkerId:  w.workerID,
		Timestamp: time.Now().Unix(),
		Message: &worker_pb.WorkerMessage_TaskComplete{
			TaskComplete: &worker_pb.TaskComplete{
				TaskId:         task.ID,
				WorkerId:       w.workerID,
				Success:        success,
				ErrorMessage:   errorMessage,
				CompletionTime: time.Now().Unix(),
				ResultMetadata: map[string]string{
					"duration": time.Since(task.StartedAt).String(),
				},
			},
		},
	}

	if err := w.adminStream.Send(completion); err != nil {
		glog.Errorf("Failed to send task completion: %v", err)
	}
}
