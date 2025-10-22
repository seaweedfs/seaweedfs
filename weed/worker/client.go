package worker

import (
	"context"
	"errors"
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

var (
	ErrAlreadyConnected = errors.New("already connected")
)

// GrpcAdminClient implements AdminClient using gRPC bidirectional streaming
type GrpcAdminClient struct {
	adminAddress string
	workerID     string
	dialOption   grpc.DialOption

	cmds      chan grpcCommand
	closeOnce sync.Once

	// Reconnection parameters
	maxReconnectAttempts int
	reconnectBackoff     time.Duration
	maxReconnectBackoff  time.Duration
	reconnectMultiplier  float64

	// Channels for communication
	outgoing      chan *worker_pb.WorkerMessage
	incoming      chan *worker_pb.AdminMessage
	responseChans map[string]chan *worker_pb.AdminMessage
}

type grpcAction string

const (
	ActionConnect              grpcAction = "connect"
	ActionDisconnect           grpcAction = "disconnect"
	ActionReconnect            grpcAction = "reconnect"
	ActionStreamError          grpcAction = "stream_error"
	ActionRegisterWorker       grpcAction = "register_worker"
	ActionQueryReconnecting    grpcAction = "query_reconnecting"
	ActionQueryConnected       grpcAction = "query_connected"
	ActionQueryShouldReconnect grpcAction = "query_shouldreconnect"
)

type registrationRequest struct {
	Worker *types.WorkerData
	Resp   chan error // Used to send the registration result back
}

type grpcCommand struct {
	action grpcAction
	data   any
	resp   chan error // for reporting success/failure
}

type grpcState struct {
	connected       bool
	reconnecting    bool
	shouldReconnect bool
	conn            *grpc.ClientConn
	client          worker_pb.WorkerServiceClient
	stream          worker_pb.WorkerService_WorkerStreamClient
	streamCtx       context.Context
	streamCancel    context.CancelFunc
	lastWorkerInfo  *types.WorkerData
	reconnectStop   chan struct{}
	streamExit      chan struct{}
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
		responseChans:        make(map[string]chan *worker_pb.AdminMessage),
		cmds:                 make(chan grpcCommand),
	}
	go c.managerLoop()
	return c
}

func (c *GrpcAdminClient) managerLoop() {
	state := &grpcState{shouldReconnect: true}

	for cmd := range c.cmds {
		switch cmd.action {
		case ActionConnect:
			c.handleConnect(cmd, state)
		case ActionDisconnect:
			c.handleDisconnect(cmd, state)
		case ActionReconnect:
			if state.connected || state.reconnecting || !state.shouldReconnect {
				cmd.resp <- ErrAlreadyConnected
				continue
			}
			state.reconnecting = true // Manager acknowledges the attempt
			err := c.reconnect(state)
			state.reconnecting = false
			cmd.resp <- err
		case ActionStreamError:
			state.connected = false
		case ActionRegisterWorker:
			req := cmd.data.(registrationRequest)
			state.lastWorkerInfo = req.Worker
			if !state.connected {
				glog.V(1).Infof("Not connected yet, worker info stored for registration upon connection")
				// Respond immediately with success (registration will happen later)
				req.Resp <- nil
				continue
			}
			err := c.sendRegistration(req.Worker)
			req.Resp <- err
		case ActionQueryConnected:
			respCh := cmd.data.(chan bool)
			respCh <- state.connected
		case ActionQueryReconnecting:
			respCh := cmd.data.(chan bool)
			respCh <- state.reconnecting
		case ActionQueryShouldReconnect:
			respCh := cmd.data.(chan bool)
			respCh <- state.shouldReconnect
		}
	}
}

// Connect establishes gRPC connection to admin server with TLS detection
func (c *GrpcAdminClient) Connect() error {
	resp := make(chan error)
	c.cmds <- grpcCommand{
		action: ActionConnect,
		resp:   resp,
	}
	return <-resp
}

func (c *GrpcAdminClient) handleConnect(cmd grpcCommand, s *grpcState) {
	if s.connected {
		cmd.resp <- fmt.Errorf("already connected")
		return
	}

	// Start reconnection loop immediately (async)
	stop := make(chan struct{})
	s.reconnectStop = stop
	go c.reconnectionLoop(stop)

	// Attempt the initial connection
	err := c.attemptConnection(s)
	if err != nil {
		glog.V(1).Infof("Initial connection failed, reconnection loop will retry: %v", err)
		cmd.resp <- err
		return
	}
	cmd.resp <- nil
}

// createConnection attempts to connect using the provided dial option
func (c *GrpcAdminClient) createConnection() (*grpc.ClientConn, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	conn, err := pb.GrpcDial(ctx, c.adminAddress, false, c.dialOption)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to admin server: %w", err)
	}

	glog.Infof("Connected to admin server at %s", c.adminAddress)
	return conn, nil
}

// attemptConnection tries to establish the connection without managing the reconnection loop
func (c *GrpcAdminClient) attemptConnection(s *grpcState) error {
	// Detect TLS support and create appropriate connection
	conn, err := c.createConnection()
	if err != nil {
		return fmt.Errorf("failed to connect to admin server: %w", err)
	}

	s.conn = conn
	s.client = worker_pb.NewWorkerServiceClient(conn)

	// Create bidirectional stream
	s.streamCtx, s.streamCancel = context.WithCancel(context.Background())
	stream, err := s.client.WorkerStream(s.streamCtx)
	glog.Infof("Worker stream created")
	if err != nil {
		s.conn.Close()
		return fmt.Errorf("failed to create worker stream: %w", err)
	}
	s.connected = true
	s.stream = stream

	// Always check for worker info and send registration immediately as the very first message
	if s.lastWorkerInfo != nil {
		// Send registration synchronously as the very first message
		if err := c.sendRegistrationSync(s.lastWorkerInfo, s.stream); err != nil {
			s.conn.Close()
			s.connected = false
			return fmt.Errorf("failed to register worker: %w", err)
		}
		glog.Infof("Worker registered successfully with admin server")
	} else {
		// No worker info yet - stream will wait for registration
		glog.V(1).Infof("Connected to admin server, waiting for worker registration info")
	}

	// Start stream handlers
	s.streamExit = make(chan struct{})
	go handleOutgoing(s.stream, s.streamExit, c.outgoing, c.cmds)
	go handleIncoming(c.workerID, s.stream, s.streamExit, c.incoming, c.cmds)

	glog.Infof("Connected to admin server at %s", c.adminAddress)
	return nil
}

// reconnect attempts to re-establish the connection
func (c *GrpcAdminClient) reconnect(s *grpcState) error {
	// Clean up existing connection completely
	if s.streamCancel != nil {
		s.streamCancel()
	}
	if s.stream != nil {
		s.stream.CloseSend()
	}
	if s.conn != nil {
		s.conn.Close()
	}
	s.connected = false

	// Attempt to re-establish connection using the same logic as initial connection
	if err := c.attemptConnection(s); err != nil {
		return fmt.Errorf("failed to reconnect: %w", err)
	}

	// Registration is now handled in attemptConnection if worker info is available
	return nil
}

// reconnectionLoop handles automatic reconnection with exponential backoff
func (c *GrpcAdminClient) reconnectionLoop(reconnectStop chan struct{}) {
	backoff := c.reconnectBackoff
	attempts := 0

	for {
		waitDuration := backoff
		if attempts == 0 {
			waitDuration = time.Second
		}
		select {
		case <-reconnectStop:
			return
		case <-time.After(waitDuration):
		}
		resp := make(chan error, 1)
		c.cmds <- grpcCommand{
			action: ActionReconnect,
			resp:   resp,
		}
		err := <-resp
		if err == nil {
			// Successful reconnection
			attempts = 0
			backoff = c.reconnectBackoff
			glog.Infof("Successfully reconnected to admin server")
		} else if errors.Is(err, ErrAlreadyConnected) {
			attempts = 0
			backoff = c.reconnectBackoff
		} else {
			attempts++
			glog.Errorf("Reconnection attempt %d failed: %v", attempts, err)

			// Check if we should give up
			if c.maxReconnectAttempts > 0 && attempts >= c.maxReconnectAttempts {
				glog.Errorf("Max reconnection attempts (%d) reached, giving up", c.maxReconnectAttempts)
				return
			}

			// Increase backoff
			backoff = time.Duration(float64(backoff) * c.reconnectMultiplier)
			if backoff > c.maxReconnectBackoff {
				backoff = c.maxReconnectBackoff
			}
			glog.Infof("Waiting %v before next reconnection attempt", backoff)
		}
	}
}

// handleOutgoing processes outgoing messages to admin
func handleOutgoing(
	stream worker_pb.WorkerService_WorkerStreamClient,
	streamExit <-chan struct{},
	outgoing <-chan *worker_pb.WorkerMessage,
	cmds chan<- grpcCommand) {

	msgCh := make(chan *worker_pb.WorkerMessage)
	errCh := make(chan error, 1) // Buffered to prevent blocking if the manager is busy
	// Goroutine to handle blocking stream.Recv() and simultaneously handle exit
	// signals
	go func() {
		for msg := range msgCh {
			if err := stream.Send(msg); err != nil {
				errCh <- err
				return // Exit the receiver goroutine on error/EOF
			}
		}
		close(errCh)
	}()

	for msg := range outgoing {
		select {
		case msgCh <- msg:
		case err := <-errCh:
			glog.Errorf("Failed to send message to admin: %v", err)
			cmds <- grpcCommand{action: ActionStreamError, data: err}
			return
		case <-streamExit:
			close(msgCh)
			<-errCh
			return
		}
	}
}

// handleIncoming processes incoming messages from admin
func handleIncoming(
	workerID string,
	stream worker_pb.WorkerService_WorkerStreamClient,
	streamExit <-chan struct{},
	incoming chan<- *worker_pb.AdminMessage,
	cmds chan<- grpcCommand) {
	glog.V(1).Infof("INCOMING HANDLER STARTED: Worker %s incoming message handler started", workerID)
	msgCh := make(chan *worker_pb.AdminMessage)
	errCh := make(chan error, 1) // Buffered to prevent blocking if the manager is busy
	// Goroutine to handle blocking stream.Recv() and simultaneously handle exit
	// signals
	go func() {
		for {
			msg, err := stream.Recv()
			if err != nil {
				errCh <- err
				return // Exit the receiver goroutine on error/EOF
			}
			msgCh <- msg
		}
	}()

	for {
		glog.V(4).Infof("LISTENING: Worker %s waiting for message from admin server", workerID)

		select {
		case msg := <-msgCh:
			// Message successfully received from the stream
			glog.V(4).Infof("MESSAGE RECEIVED: Worker %s received message from admin server: %T", workerID, msg.Message)

			// Route message to waiting goroutines or general handler (original select logic)
			select {
			case incoming <- msg:
				glog.V(3).Infof("MESSAGE ROUTED: Worker %s successfully routed message to handler", workerID)
			case <-time.After(time.Second):
				glog.Warningf("MESSAGE DROPPED: Worker %s incoming message buffer full, dropping message: %T", workerID, msg.Message)
			}

		case err := <-errCh:
			// Stream Receiver goroutine reported an error (EOF or network error)
			if err == io.EOF {
				glog.Infof("STREAM CLOSED: Worker %s admin server closed the stream", workerID)
			} else {
				glog.Errorf("RECEIVE ERROR: Worker %s failed to receive message from admin: %v", workerID, err)
			}

			// Report the failure as a command to the managerLoop (blocking)
			cmds <- grpcCommand{action: ActionStreamError, data: err}

			// Exit the main handler loop
			glog.V(1).Infof("INCOMING HANDLER STOPPED: Worker %s stopping incoming handler due to stream error", workerID)
			return

		case <-streamExit:
			// Manager closed this channel, signaling a controlled disconnection.
			glog.V(1).Infof("INCOMING HANDLER STOPPED: Worker %s stopping incoming handler - received exit signal", workerID)
			return
		}
	}
}

// Connect establishes gRPC connection to admin server with TLS detection
func (c *GrpcAdminClient) Disconnect() error {
	resp := make(chan error)
	c.cmds <- grpcCommand{
		action: ActionDisconnect,
		resp:   resp,
	}
	err := <-resp
	c.closeOnce.Do(func() {
		close(c.cmds)
	})
	return err
}

func (c *GrpcAdminClient) handleDisconnect(cmd grpcCommand, s *grpcState) {
	if !s.connected {
		cmd.resp <- fmt.Errorf("already disconnected")
		return
	}

	// Send shutdown signal to stop reconnection loop
	close(s.reconnectStop)

	// Send shutdown signal to stop handlers loop
	close(s.streamExit)

	s.connected = false
	s.shouldReconnect = false

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

	// Close outgoing/incoming
	select {
	case c.outgoing <- shutdownMsg:
	case <-time.After(time.Second):
		glog.Warningf("Failed to send shutdown message")
	}

	// Cancel stream context
	if s.streamCancel != nil {
		s.streamCancel()
	}

	// Close stream
	if s.stream != nil {
		s.stream.CloseSend()
	}

	// Close connection
	if s.conn != nil {
		s.conn.Close()
	}

	// Close channels
	close(c.outgoing)
	close(c.incoming)

	glog.Infof("Disconnected from admin server")
	cmd.resp <- nil
}

// RegisterWorker registers the worker with the admin server
func (c *GrpcAdminClient) RegisterWorker(worker *types.WorkerData) error {
	respCh := make(chan error, 1)
	request := registrationRequest{
		Worker: worker,
		Resp:   respCh,
	}
	c.cmds <- grpcCommand{
		action: ActionRegisterWorker,
		data:   request,
	}
	return <-respCh
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

func (c *GrpcAdminClient) IsConnected() bool {
	respCh := make(chan bool, 1)

	c.cmds <- grpcCommand{
		action: ActionQueryConnected,
		data:   respCh,
	}

	return <-respCh
}

func (c *GrpcAdminClient) IsReconnecting() bool {
	respCh := make(chan bool, 1)

	c.cmds <- grpcCommand{
		action: ActionQueryReconnecting,
		data:   respCh,
	}

	return <-respCh
}

func (c *GrpcAdminClient) ShouldReconnect() bool {
	respCh := make(chan bool, 1)

	c.cmds <- grpcCommand{
		action: ActionQueryShouldReconnect,
		data:   respCh,
	}

	return <-respCh
}

// SendHeartbeat sends heartbeat to admin server
func (c *GrpcAdminClient) SendHeartbeat(workerID string, status *types.WorkerStatus) error {
	if !c.IsConnected() {
		// If we're currently reconnecting, don't wait - just skip the heartbeat
		reconnecting := c.IsReconnecting()

		if reconnecting {
			// Don't treat as an error - reconnection is in progress
			glog.V(2).Infof("Skipping heartbeat during reconnection")
			return nil
		}

		// Wait for reconnection for a short time
		if err := c.waitForConnection(10 * time.Second); err != nil {
			return fmt.Errorf("not connected to admin server: %w", err)
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
func (c *GrpcAdminClient) RequestTask(workerID string, capabilities []types.TaskType) (*types.TaskInput, error) {
	if !c.IsConnected() {
		// If we're currently reconnecting, don't wait - just return no task
		reconnecting := c.IsReconnecting()

		if reconnecting {
			// Don't treat as an error - reconnection is in progress
			glog.V(2).Infof("RECONNECTING: Worker %s skipping task request during reconnection", workerID)
			return nil, nil
		}

		// Wait for reconnection for a short time
		if err := c.waitForConnection(5 * time.Second); err != nil {
			return nil, fmt.Errorf("not connected to admin server: %w", err)
		}
	}

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
	if !c.IsConnected() {
		// If we're currently reconnecting, don't wait - just skip the completion report
		reconnecting := c.IsReconnecting()

		if reconnecting {
			// Don't treat as an error - reconnection is in progress
			glog.V(2).Infof("Skipping task completion report during reconnection for task %s", taskID)
			return nil
		}

		// Wait for reconnection for a short time
		if err := c.waitForConnection(5 * time.Second); err != nil {
			return fmt.Errorf("not connected to admin server: %w", err)
		}
	}

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
	if !c.IsConnected() {
		// If we're currently reconnecting, don't wait - just skip the progress update
		reconnecting := c.IsReconnecting()

		if reconnecting {
			// Don't treat as an error - reconnection is in progress
			glog.V(2).Infof("Skipping task progress update during reconnection for task %s", taskID)
			return nil
		}

		// Wait for reconnection for a short time
		if err := c.waitForConnection(5 * time.Second); err != nil {
			return fmt.Errorf("not connected to admin server: %w", err)
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

// waitForConnection waits for the connection to be established or timeout
func (c *GrpcAdminClient) waitForConnection(timeout time.Duration) error {
	deadline := time.Now().Add(timeout)

	for time.Now().Before(deadline) {
		connected := c.IsConnected()
		shouldReconnect := c.ShouldReconnect()

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

// GetIncomingChannel returns the incoming message channel for message processing
// This allows the worker to process admin messages directly
func (c *GrpcAdminClient) GetIncomingChannel() <-chan *worker_pb.AdminMessage {
	return c.incoming
}

// CreateAdminClient creates an admin client with the provided dial option
func CreateAdminClient(adminServer string, workerID string, dialOption grpc.DialOption) (AdminClient, error) {
	return NewGrpcAdminClient(adminServer, workerID, dialOption), nil
}

// getServerFromParams extracts server address from unified sources
func getServerFromParams(params *worker_pb.TaskParams) string {
	if len(params.Sources) > 0 {
		return params.Sources[0].Node
	}
	return ""
}
