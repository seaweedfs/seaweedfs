package worker

import (
	"context"
	"fmt"
	"io"
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
	comms clientChannels

	// Reconnection parameters
	maxReconnectAttempts int
	reconnectBackoff     time.Duration
	maxReconnectBackoff  time.Duration
	reconnectMultiplier  float64

	// Channels for communication
	outgoing chan *worker_pb.WorkerMessage
	incoming chan *worker_pb.AdminMessage

	state grpcState
}
type connectionEvent struct {
	connected bool
	err       error
}

type clientChannels struct {
	stop             chan struct{}
	connectionEvents chan connectionEvent
	streamErrors     chan error
}

type grpcState struct {
	started        bool
	lastWorkerInfo *types.WorkerData
}

// NewGrpcAdminClient creates a new gRPC admin client
func NewGrpcAdminClient(adminAddress string, workerID string, dialOption grpc.DialOption) *GrpcAdminClient {
	// Admin uses HTTP port + 10000 as gRPC port
	grpcAddress := pb.ServerToGrpcAddress(adminAddress)

	c := &GrpcAdminClient{
		adminAddress:         grpcAddress,
		workerID:             workerID,
		dialOption:           dialOption,
		maxReconnectAttempts: 0, // 0 means infinite attempts
		reconnectBackoff:     1 * time.Second,
		maxReconnectBackoff:  30 * time.Second,
		reconnectMultiplier:  1.5,
		outgoing:             make(chan *worker_pb.WorkerMessage, 100),
		incoming:             make(chan *worker_pb.AdminMessage, 100),
		state:                grpcState{started: false},
	}
	return c
}

// Connect establishes gRPC connection to admin server with TLS detection
func (c *GrpcAdminClient) Connect(workerInfo *types.WorkerData) error {
	if c.state.started {
		return fmt.Errorf("already started")

	}
	// Register worker info with client first (this stores it for use during connection)
	if err := c.RegisterWorker(workerInfo); err != nil {
		glog.V(1).Infof("Worker info stored for registration: %v", err)
		// This is expected if not connected yet
	}

	c.state.started = true
	c.comms.stop = make(chan struct{})
	c.comms.connectionEvents = make(chan connectionEvent)
	c.comms.streamErrors = make(chan error, 2)
	go c.connectionProcess()

	// Attempt the initial connection
	event := <-c.comms.connectionEvents
	if event.err != nil {
		glog.V(1).Infof("Initial connection failed, will retry: %v", event.err)
		return event.err
	} else {
		return nil
	}
}

func (c *GrpcAdminClient) GetEvents() chan connectionEvent {
	return c.comms.connectionEvents
}

func (c *GrpcAdminClient) connectionProcess() {
	var (
		conn *grpc.ClientConn
	)

	// Initial connection attempt
	conn, err := c.tryConnect()
	if err != nil {
		glog.Warningf("Initial connection failed: %v", err)
		c.comms.connectionEvents <- connectionEvent{connected: false, err: err}
		c.comms.streamErrors <- err
		c.comms.streamErrors <- err
	} else {
		c.comms.connectionEvents <- connectionEvent{connected: true}
	}

	for {
		select {
		case <-c.comms.stop:
			c.comms.connectionEvents <- connectionEvent{connected: false}
			if conn != nil {
				<-c.comms.streamErrors
				<-c.comms.streamErrors
				conn.Close()
			}
			return
		case err := <-c.comms.streamErrors:
			<-c.comms.streamErrors // now both incomingProcess and outgoingProcess
			// have been cleaned up
			glog.Warningf("Stream error: %v, reconnecting...", err)
			if conn != nil {
				conn.Close()
				conn = nil
			}
			c.comms.connectionEvents <- connectionEvent{connected: false, err: err}
			conn, err = c.tryConnectWithBackoff()
			if err != nil {
				glog.Errorf("Reconnection failed: %v", err)
			} else {
				c.comms.connectionEvents <- connectionEvent{connected: true}
			}

		}

	}

}

// createConnection attempts to connect using the provided dial option
func (c *GrpcAdminClient) createConnection() (*grpc.ClientConn, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	conn, err := pb.GrpcDial(ctx, c.adminAddress, false, c.dialOption)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to admin server: %w", err)
	}

	return conn, nil
}

func (c *GrpcAdminClient) tryConnect() (*grpc.ClientConn, error) {
	glog.Infof("Connecting to admin server at %s", c.adminAddress)

	conn, err := c.createConnection()
	if err != nil {
		return nil, fmt.Errorf("connection failed: %w", err)
	}

	stream, err := worker_pb.NewWorkerServiceClient(conn).WorkerStream(context.Background())
	if err != nil {
		conn.Close()
		return nil, fmt.Errorf("stream creation failed: %w", err)
	}

	if c.state.lastWorkerInfo != nil {
		if err := c.sendRegistrationSync(c.state.lastWorkerInfo, stream); err != nil {
			conn.Close()
			return nil, fmt.Errorf("registration failed: %w", err)
		}
		glog.Infof("Worker registered successfully")
	}

	// Start stream handlers
	go c.outgoingProcess(stream)
	go c.incomingProcess(stream)

	glog.Infof("Connected to admin server at %s", c.adminAddress)
	return conn, nil
}

func (c *GrpcAdminClient) tryConnectWithBackoff() (*grpc.ClientConn, error) {
	backoff := time.Second
	attempts := 0
	for {
		if conn, err := c.tryConnect(); err == nil {
			return conn, nil
		}

		attempts++
		if c.maxReconnectAttempts > 0 && attempts >= c.maxReconnectAttempts {
			return nil, fmt.Errorf("max reconnection attempts reached")
		}

		// Exponential backoff
		backoff = time.Duration(float64(backoff) * c.reconnectMultiplier)
		if backoff > c.maxReconnectBackoff {
			backoff = c.maxReconnectBackoff
		}

		glog.Infof("Reconnection failed, retrying in %v", backoff)
		select {
		case <-c.comms.stop:
			return nil, fmt.Errorf("cancelled")
		case <-time.After(backoff):
		}
	}
}

// handleOutgoing processes outgoing messages to admin
func (c *GrpcAdminClient) outgoingProcess(stream worker_pb.WorkerService_WorkerStreamClient) {

	for {
		select {
		case <-c.comms.stop:
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
			stream.Send(shutdownMsg)
			close(c.outgoing)
			c.comms.streamErrors <- nil
			return
		case msg := <-c.outgoing:
			if err := stream.Send(msg); err != nil {
				glog.Errorf("Failed to send message: %v", err)
				c.comms.streamErrors <- err
				return
			}
		}
	}
}

// handleIncoming processes incoming messages from admin
func (c *GrpcAdminClient) incomingProcess(stream worker_pb.WorkerService_WorkerStreamClient) {
	workerID := c.state.lastWorkerInfo.ID
	glog.V(1).Infof("INCOMING HANDLER STARTED: Worker %s incoming message handler started", workerID)

	for {
		glog.V(4).Infof("LISTENING: Worker %s waiting for message from admin server", workerID)

		select {
		case <-c.comms.stop:
			close(c.incoming)
			glog.V(1).Infof("INCOMING HANDLER STOPPED: Worker %s stopping incoming handler - received exit signal", workerID)
			c.comms.streamErrors <- nil
			return
		default:
			msg, err := stream.Recv()
			if err != nil {
				if err == io.EOF {
					glog.Infof("STREAM CLOSED: Worker %s admin server closed the stream", workerID)
				} else {
					glog.Errorf("RECEIVE ERROR: Worker %s failed to receive message from admin: %v", workerID, err)
				}
				c.comms.streamErrors <- err
				return // Exit the receiver goroutine on error/EOF
			}
			glog.V(4).Infof("MESSAGE RECEIVED: Worker %s received message from admin server: %T", workerID, msg.Message)
			select {
			case c.incoming <- msg:
				glog.V(3).Infof("MESSAGE ROUTED: Worker %s successfully routed message to handler", workerID)
			case <-time.After(time.Second):
				glog.Warningf("MESSAGE DROPPED: Worker %s incoming message buffer full, dropping message: %T", workerID, msg.Message)
			}

		}
	}
}

// Connect establishes gRPC connection to admin server with TLS detection
func (c *GrpcAdminClient) Disconnect() error {
	if !c.state.started {
		glog.Errorf("already disconnected")
		return nil
	}
	c.state.started = false

	// Send shutdown signal to stop connection Process
	close(c.comms.stop)

	glog.Infof("Disconnected from admin server")
	return nil
}

// RegisterWorker registers the worker with the admin server
func (c *GrpcAdminClient) RegisterWorker(worker *types.WorkerData) error {
	c.state.lastWorkerInfo = worker
	if !c.state.started {
		glog.V(1).Infof("Not started yet, worker info stored for registration upon connection")
		// Respond immediately with success (registration will happen later)
		return nil
	}
	err := c.sendRegistration(worker)
	return err
}

// sendRegistration sends the registration message and waits for response
func (c *GrpcAdminClient) sendRegistration(worker *types.WorkerData) error {
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

// sendRegistrationSync sends the registration message synchronously
func (c *GrpcAdminClient) sendRegistrationSync(worker *types.WorkerData, stream worker_pb.WorkerService_WorkerStreamClient) error {
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

	// Send directly to stream to ensure it's the first message
	if err := stream.Send(msg); err != nil {
		return fmt.Errorf("failed to send registration message: %w", err)
	}

	// Create a channel to receive the response
	responseChan := make(chan *worker_pb.AdminMessage, 1)
	errChan := make(chan error, 1)

	// Start a goroutine to listen for the response
	go func() {
		for {
			response, err := stream.Recv()
			if err != nil {
				errChan <- fmt.Errorf("failed to receive registration response: %w", err)
				return
			}

			if regResp := response.GetRegistrationResponse(); regResp != nil {
				responseChan <- response
				return
			}
			// Continue waiting if it's not a registration response
			// If stream is stuck, reconnect() will kill it, cleaning up this
			// goroutine
		}
	}()

	// Wait for registration response with timeout
	timeout := time.NewTimer(10 * time.Second)
	defer timeout.Stop()

	select {
	case response := <-responseChan:
		if regResp := response.GetRegistrationResponse(); regResp != nil {
			if regResp.Success {
				glog.V(1).Infof("Worker registered successfully: %s", regResp.Message)
				return nil
			}
			return fmt.Errorf("registration failed: %s", regResp.Message)
		}
		return fmt.Errorf("unexpected response type")
	case err := <-errChan:
		return err
	case <-timeout.C:
		return fmt.Errorf("registration timeout")
	}
}

// SendHeartbeat sends heartbeat to admin server
func (c *GrpcAdminClient) SendHeartbeat(workerID string, status *types.WorkerStatus) error {

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
func (c *GrpcAdminClient) RequestTask(workerID string, capabilities []types.TaskType) (*types.TaskInput, error) {

	caps := make([]string, len(capabilities))
	for i, cap := range capabilities {
		caps[i] = string(cap)
	}

	glog.V(3).Infof("📤 SENDING TASK REQUEST: Worker %s sending task request to admin server with capabilities: %v",
		workerID, capabilities)

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
		glog.V(3).Infof("TASK REQUEST SENT: Worker %s successfully sent task request to admin server", workerID)
	case <-time.After(time.Second):
		glog.Errorf("TASK REQUEST TIMEOUT: Worker %s failed to send task request: timeout", workerID)
		return nil, fmt.Errorf("failed to send task request: timeout")
	}

	// Wait for task assignment
	glog.V(3).Infof("WAITING FOR RESPONSE: Worker %s waiting for task assignment response (5s timeout)", workerID)
	timeout := time.NewTimer(5 * time.Second)
	defer timeout.Stop()

	for {
		select {
		case response := <-c.incoming:
			glog.V(3).Infof("RESPONSE RECEIVED: Worker %s received response from admin server: %T", workerID, response.Message)
			if taskAssign := response.GetTaskAssignment(); taskAssign != nil {
				glog.V(1).Infof("Worker %s received task assignment in response: %s (type: %s, volume: %d)",
					workerID, taskAssign.TaskId, taskAssign.TaskType, taskAssign.Params.VolumeId)

				// Convert to our task type
				task := &types.TaskInput{
					ID:         taskAssign.TaskId,
					Type:       types.TaskType(taskAssign.TaskType),
					Status:     types.TaskStatusAssigned,
					VolumeID:   taskAssign.Params.VolumeId,
					Server:     getServerFromParams(taskAssign.Params),
					Collection: taskAssign.Params.Collection,
					Priority:   types.TaskPriority(taskAssign.Priority),
					CreatedAt:  time.Unix(taskAssign.CreatedTime, 0),
					// Use typed protobuf parameters directly
					TypedParams: taskAssign.Params,
				}
				return task, nil
			} else {
				glog.V(3).Infof("NON-TASK RESPONSE: Worker %s received non-task response: %T", workerID, response.Message)
			}
		case <-timeout.C:
			glog.V(3).Infof("TASK REQUEST TIMEOUT: Worker %s - no task assignment received within 5 seconds", workerID)
			return nil, nil // No task available
		}
	}
}

// CompleteTask reports task completion to admin server
func (c *GrpcAdminClient) CompleteTask(taskID string, success bool, errorMsg string) error {
	return c.CompleteTaskWithMetadata(taskID, success, errorMsg, nil)
}

// CompleteTaskWithMetadata reports task completion with additional metadata
func (c *GrpcAdminClient) CompleteTaskWithMetadata(taskID string, success bool, errorMsg string, metadata map[string]string) error {
	taskComplete := &worker_pb.TaskComplete{
		TaskId:         taskID,
		WorkerId:       c.workerID,
		Success:        success,
		ErrorMessage:   errorMsg,
		CompletionTime: time.Now().Unix(),
	}

	// Add metadata if provided
	if metadata != nil {
		taskComplete.ResultMetadata = metadata
	}

	msg := &worker_pb.WorkerMessage{
		WorkerId:  c.workerID,
		Timestamp: time.Now().Unix(),
		Message: &worker_pb.WorkerMessage_TaskComplete{
			TaskComplete: taskComplete,
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

// GetIncomingChannel returns the incoming message channel for message processing
// This allows the worker to process admin messages directly
func (c *GrpcAdminClient) GetIncomingChannel() <-chan *worker_pb.AdminMessage {
	return c.incoming
}

// NewAdminClient creates an admin client with the provided dial option
func NewAdminClient(adminServer string, workerID string, dialOption grpc.DialOption) AdminClient {
	return NewGrpcAdminClient(adminServer, workerID, dialOption)
}

// getServerFromParams extracts server address from unified sources
func getServerFromParams(params *worker_pb.TaskParams) string {
	if len(params.Sources) > 0 {
		return params.Sources[0].Node
	}
	return ""
}
