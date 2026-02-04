package worker

import (
	"context"
	"errors"
	"fmt"
	"io"
	"time"

	"crypto/rand"

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

	cmds chan grpcCommand

	// Session identification for logging
	sessionID string

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
	ActionResetReconnectStop   grpcAction = "reset_reconnect_stop"
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
	streamFailed    chan struct{} // Signals when stream has failed
	regWait         chan *worker_pb.RegistrationResponse
}

// NewGrpcAdminClient creates a new gRPC admin client
func NewGrpcAdminClient(adminAddress string, workerID string, dialOption grpc.DialOption) *GrpcAdminClient {
	// Admin uses HTTP port + 10000 as gRPC port
	grpcAddress := pb.ServerToGrpcAddress(adminAddress)

	// Generate a unique session ID for logging
	sessionBytes := make([]byte, 2)
	rand.Read(sessionBytes)
	sessionID := fmt.Sprintf("%x", sessionBytes)

	c := &GrpcAdminClient{
		adminAddress:         grpcAddress,
		workerID:             workerID,
		sessionID:            sessionID,
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

// drainAndCloseRegWaitChannel drains any pending messages from the regWait channel
// and then safely closes it. This prevents losing RegistrationResponse messages
// that were sent before the channel is closed.
func drainAndCloseRegWaitChannel(ch *chan *worker_pb.RegistrationResponse) {
	if ch == nil || *ch == nil {
		return
	}
	for {
		select {
		case <-*ch:
			// continue draining until channel is empty
		default:
			close(*ch)
			*ch = nil
			return
		}
	}
}

// safeCloseChannel safely closes a channel and sets it to nil to prevent double-close panics.
// NOTE: This function is NOT thread-safe. It is safe to use in this codebase because all calls
// are serialized within the managerLoop goroutine. If this function is used in concurrent contexts
// in the future, synchronization (e.g., sync.Mutex) should be added.
func (c *GrpcAdminClient) safeCloseChannel(chPtr *chan struct{}) {
	if *chPtr != nil {
		close(*chPtr)
		*chPtr = nil
	}
}

func (c *GrpcAdminClient) managerLoop() {
	state := &grpcState{shouldReconnect: true}

	glog.V(1).Infof("[session %s] Manager loop started for worker %s", c.sessionID, c.workerID)

out:
	for cmd := range c.cmds {
		glog.V(4).Infof("[session %s] Manager received command: %s", c.sessionID, cmd.action)
		switch cmd.action {
		case ActionConnect:
			c.handleConnect(cmd, state)
		case ActionDisconnect:
			c.handleDisconnect(cmd, state)
			break out
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
			// Restart reconnection loop if needed
			if state.shouldReconnect && state.reconnectStop == nil {
				glog.V(1).Infof("[session %s] Stream error, starting reconnection loop", c.sessionID)
				stop := make(chan struct{})
				state.reconnectStop = stop
				go c.reconnectionLoop(stop, func() {
					c.cmds <- grpcCommand{action: ActionResetReconnectStop, data: state}
				})
			}
		case ActionRegisterWorker:
			req := cmd.data.(registrationRequest)
			state.lastWorkerInfo = req.Worker
			if !state.connected {
				glog.V(1).Infof("[session %s] Not connected yet, worker info stored for registration upon connection", c.sessionID)
				// Respond immediately with success (registration will happen later)
				req.Resp <- nil
				continue
			}
			// Capture channel pointers to avoid race condition with reconnect
			streamFailedCh := state.streamFailed
			regWaitCh := state.regWait
			if streamFailedCh == nil || regWaitCh == nil {
				req.Resp <- fmt.Errorf("stream not ready for registration")
				continue
			}
			err := c.sendRegistration(req.Worker, streamFailedCh, regWaitCh)
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
		case ActionResetReconnectStop:
			// This is an internal action to reset the stop channel when reconnectionLoop exits
			s := cmd.data.(*grpcState)
			s.reconnectStop = nil
		}
	}
}

// Connect establishes gRPC connection to admin server with TLS detection
func (c *GrpcAdminClient) Connect() error {
	resp := make(chan error, 1)
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

	// Start reconnection loop immediately if not already running (async)
	if s.reconnectStop == nil {
		glog.V(1).Infof("[session %s] Starting reconnection loop", c.sessionID)
		stop := make(chan struct{})
		s.reconnectStop = stop
		go c.reconnectionLoop(stop, func() {
			// This callback is executed when the reconnectionLoop exits.
			// It ensures that s.reconnectStop is reset to nil, allowing a new loop to be started later.
			c.cmds <- grpcCommand{action: ActionResetReconnectStop, data: s}
		})
	} else {
		glog.V(1).Infof("[session %s] Reconnection loop already running", c.sessionID)
	}

	// Attempt the initial connection
	err := c.attemptConnection(s)
	if err != nil {
		glog.Warningf("[session %s] Initial connection failed, reconnection loop will retry: %v", c.sessionID, err)
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
		return nil, fmt.Errorf("failed to connect to admin server %s: %w", c.adminAddress, err)
	}

	glog.Infof("[session %s] Connected to admin server at %s", c.sessionID, c.adminAddress)
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
	glog.Infof("[session %s] Worker stream created", c.sessionID)
	if err != nil {
		s.conn.Close()
		return fmt.Errorf("failed to create worker stream: %w", err)
	}
	s.connected = true
	s.stream = stream

	// Start stream handlers BEFORE sending registration
	// This ensures handleIncoming is ready to receive the registration response
	s.streamExit = make(chan struct{})
	s.streamFailed = make(chan struct{})
	s.regWait = make(chan *worker_pb.RegistrationResponse, 1)
	go handleOutgoing(c.sessionID, s.stream, s.streamExit, c.outgoing, c.cmds)
	go handleIncoming(c.sessionID, c.workerID, s.stream, s.streamExit, c.incoming, c.cmds, s.streamFailed, s.regWait)

	// Always check for worker info and send registration immediately as the very first message
	if s.lastWorkerInfo != nil {
		// Send registration via the normal outgoing channel and wait for response via incoming
		if err := c.sendRegistration(s.lastWorkerInfo, s.streamFailed, s.regWait); err != nil {
			c.safeCloseChannel(&s.streamExit)
			c.safeCloseChannel(&s.streamFailed)
			drainAndCloseRegWaitChannel(&s.regWait)
			s.streamCancel()
			s.conn.Close()
			s.connected = false
			return fmt.Errorf("failed to register worker: %w", err)
		}
		glog.Infof("[session %s] Worker %s registered successfully with admin server", c.sessionID, c.workerID)
	} else {
		// No worker info yet - stream will wait for registration
		glog.V(1).Infof("[session %s] Connected to admin server, waiting for worker registration info", c.sessionID)
	}

	glog.V(1).Infof("[session %s] attemptConnection successful", c.sessionID)
	return nil
}

// reconnect attempts to re-establish the connection
func (c *GrpcAdminClient) reconnect(s *grpcState) error {
	glog.V(1).Infof("[session %s] Reconnection attempt starting", c.sessionID)
	// Clean up existing connection completely
	c.safeCloseChannel(&s.streamExit)
	c.safeCloseChannel(&s.streamFailed)
	drainAndCloseRegWaitChannel(&s.regWait)
	if s.streamCancel != nil {
		s.streamCancel()
	}
	if s.conn != nil {
		s.conn.Close()
	}
	s.connected = false

	// Attempt to re-establish connection using the same logic as initial connection
	if err := c.attemptConnection(s); err != nil {
		return fmt.Errorf("failed to reconnect: %w", err)
	}

	glog.Infof("[session %s] Successfully reconnected to admin server", c.sessionID)
	// Registration is now handled in attemptConnection if worker info is available
	return nil
}

// reconnectionLoop handles automatic reconnection with exponential backoff
func (c *GrpcAdminClient) reconnectionLoop(reconnectStop chan struct{}, onExit func()) {
	defer onExit() // Ensure the cleanup callback is called when the loop exits
	backoff := c.reconnectBackoff
	attempts := 0

	for {
		attempts++ // Count this attempt (starts at 1)
		waitDuration := backoff
		if attempts == 1 {
			waitDuration = 100 * time.Millisecond // Quick retry for the very first failure
		}
		glog.V(2).Infof("[session %s] Reconnection loop sleeping for %v before attempt %d", c.sessionID, waitDuration, attempts)
		select {
		case <-reconnectStop:
			glog.V(1).Infof("[session %s] Reconnection loop stopping (received signal)", c.sessionID)
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
			glog.Infof("[session %s] Successfully reconnected to admin server, stopping reconnection loop", c.sessionID)
			return // EXIT ON SUCCESS
		} else if errors.Is(err, ErrAlreadyConnected) {
			glog.V(1).Infof("[session %s] Already connected, stopping reconnection loop", c.sessionID)
			return // EXIT ON SUCCESS (already connected)
		} else {
			glog.Warningf("[session %s] Reconnection attempt %d failed: %v", c.sessionID, attempts, err)

			// Check if we should give up
			if c.maxReconnectAttempts > 0 && attempts >= c.maxReconnectAttempts {
				glog.Errorf("[session %s] Max reconnection attempts (%d) reached, giving up", c.sessionID, c.maxReconnectAttempts)
				return
			}

			// Increase backoff
			backoff = time.Duration(float64(backoff) * c.reconnectMultiplier)
			if backoff > c.maxReconnectBackoff {
				backoff = c.maxReconnectBackoff
			}
			glog.V(1).Infof("[session %s] Waiting %v before next reconnection attempt", c.sessionID, backoff)
		}
	}
}

// handleOutgoing processes outgoing messages to admin
func handleOutgoing(
	sessionID string,
	stream worker_pb.WorkerService_WorkerStreamClient,
	streamExit <-chan struct{},
	outgoing <-chan *worker_pb.WorkerMessage,
	cmds chan<- grpcCommand) {

	glog.V(1).Infof("[session %s] Outgoing message handler started", sessionID)
	defer glog.V(1).Infof("[session %s] Outgoing message handler stopping", sessionID)
	msgCh := make(chan *worker_pb.WorkerMessage, 1)
	errCh := make(chan error, 1) // Buffered to prevent blocking if the manager is busy

	// Goroutine that reads from msgCh and performs the blocking stream.Send() calls.
	go func() {
		for msg := range msgCh {
			if err := stream.Send(msg); err != nil {
				errCh <- err
				return
			}
		}
		close(errCh)
	}()

	// Helper function to handle stream errors and cleanup
	handleStreamError := func(err error) {
		if err != nil {
			glog.Warningf("[session %s] Stream send error: %v", sessionID, err)
			select {
			case cmds <- grpcCommand{action: ActionStreamError, data: err}:
				// Successfully queued
			default:
				// Manager busy, queue asynchronously to avoid blocking
				glog.V(2).Infof("[session %s] Manager busy, queuing stream error asynchronously from outgoing handler: %v", sessionID, err)
				go func(e error) {
					select {
					case cmds <- grpcCommand{action: ActionStreamError, data: e}:
					case <-time.After(2 * time.Second):
						glog.Warningf("[session %s] Failed to send stream error to manager from outgoing handler, channel blocked: %v", sessionID, e)
					}
				}(err)
			}
		}
	}

	// Helper function to cleanup resources
	cleanup := func() {
		close(msgCh)
		<-errCh
	}

	for {
		select {
		case <-streamExit:
			cleanup()
			return
		case err := <-errCh:
			handleStreamError(err)
			return
		case msg, ok := <-outgoing:
			if !ok {
				cleanup()
				return
			}
			select {
			case msgCh <- msg:
				// Message queued successfully
			case <-streamExit:
				cleanup()
				return
			case err := <-errCh:
				handleStreamError(err)
				return
			}
		}
	}
}

// handleIncoming processes incoming messages from admin
func handleIncoming(
	sessionID string,
	workerID string,
	stream worker_pb.WorkerService_WorkerStreamClient,
	streamExit <-chan struct{},
	incoming chan<- *worker_pb.AdminMessage,
	cmds chan<- grpcCommand,
	streamFailed chan struct{},
	regWait chan *worker_pb.RegistrationResponse) {

	glog.V(1).Infof("[session %s] Incoming message handler started", sessionID)
	defer glog.V(1).Infof("[session %s] Incoming message handler stopping", sessionID)
	msgCh := make(chan *worker_pb.AdminMessage)
	errCh := make(chan error, 1) // Buffered to prevent blocking if the manager is busy
	// regWait is buffered with size 1 so that the registration response can be sent
	// even if the receiver goroutine has not yet started waiting on the channel.
	// This non-blocking send pattern avoids a race between sendRegistration and handleIncoming.
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
		select {
		case msg := <-msgCh:
			// Message successfully received from the stream
			glog.V(4).Infof("[session %s] Received message from admin server: %T", sessionID, msg.Message)

			// If this is a registration response, also publish to the registration waiter.
			// regWait is buffered (size 1) so that the response can be sent even if sendRegistration
			// hasn't started waiting yet, preventing a race condition between the two goroutines.
			if rr := msg.GetRegistrationResponse(); rr != nil {
				select {
				case regWait <- rr:
					glog.V(3).Infof("[session %s] Registration response routed to waiter", sessionID)
				default:
					glog.V(2).Infof("[session %s] Registration response dropped (no waiter)", sessionID)
				}
			}

			// Route message to general handler.
			select {
			case incoming <- msg:
				glog.V(3).Infof("[session %s] Message routed to incoming channel", sessionID)
			case <-time.After(time.Second):
				glog.Warningf("[session %s] Incoming message buffer full, dropping message: %T", sessionID, msg.Message)
			}

		case err := <-errCh:
			// Stream Receiver goroutine reported an error (EOF or network error)
			if err == io.EOF {
				glog.Infof("[session %s] Admin server closed the stream", sessionID)
			} else {
				glog.Warningf("[session %s] Stream receive error: %v", sessionID, err)
			}

			// Signal that stream has failed (non-blocking)
			select {
			case streamFailed <- struct{}{}:
			default:
			}

			// Report the failure as a command to the managerLoop.
			// Try non-blocking first; if the manager is busy and the channel is full,
			// fall back to an asynchronous blocking send so the error is not lost.
			select {
			case cmds <- grpcCommand{action: ActionStreamError, data: err}:
			default:
				glog.V(2).Infof("[session %s] Manager busy, queuing stream error asynchronously from incoming handler: %v", sessionID, err)
				go func(e error) {
					select {
					case cmds <- grpcCommand{action: ActionStreamError, data: e}:
					case <-time.After(2 * time.Second):
						glog.Warningf("[session %s] Failed to send stream error to manager from incoming handler, channel blocked: %v", sessionID, e)
					}
				}(err)
			}

			// Exit the main handler loop
			glog.V(1).Infof("[session %s] Incoming message handler stopping due to stream error", sessionID)
			return

		case <-streamExit:
			// Manager closed this channel, signaling a controlled disconnection.
			glog.V(1).Infof("[session %s] Incoming message handler stopping - received exit signal", sessionID)
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
	return err
}

func (c *GrpcAdminClient) handleDisconnect(cmd grpcCommand, s *grpcState) {
	if !s.connected {
		cmd.resp <- fmt.Errorf("already disconnected")
		return
	}

	// Send shutdown signal to stop reconnection loop
	c.safeCloseChannel(&s.reconnectStop)

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

	// Send shutdown signal to stop handlers loop
	c.safeCloseChannel(&s.streamExit)
	c.safeCloseChannel(&s.streamFailed)
	drainAndCloseRegWaitChannel(&s.regWait)

	// Cancel stream context
	if s.streamCancel != nil {
		s.streamCancel()
	}

	// Close connection
	if s.conn != nil {
		s.conn.Close()
	}

	// Close channels to signal all goroutines to stop
	// This will cause any pending sends/receives to fail gracefully
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
func (c *GrpcAdminClient) sendRegistration(worker *types.WorkerData, streamFailed <-chan struct{}, regWait <-chan *worker_pb.RegistrationResponse) error {
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
	case <-streamFailed:
		return fmt.Errorf("stream failed before registration message could be sent")
	}

	// Wait for registration response
	// Use longer timeout for reconnections since admin server might be busy
	timeout := time.NewTimer(30 * time.Second)
	defer timeout.Stop()

	for {
		select {
		case regResp, ok := <-regWait:
			if !ok || regResp == nil {
				return fmt.Errorf("registration failed: channel closed unexpectedly")
			}
			if regResp.Success {
				glog.Infof("Worker registered successfully: %s", regResp.Message)
				return nil
			}
			return fmt.Errorf("registration failed: %s", regResp.Message)
		case <-streamFailed:
			return fmt.Errorf("registration failed: stream closed by server")
		case <-timeout.C:
			return fmt.Errorf("registration failed: timeout waiting for response")
		}
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

	glog.V(3).Infof("SENDING TASK REQUEST: Worker %s sending task request to admin server with capabilities: %v",
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
	glog.V(3).Infof("WAITING FOR RESPONSE: Worker %s waiting for task assignment response (30s timeout)", workerID)
	timeout := time.NewTimer(30 * time.Second)
	defer timeout.Stop()

	for {
		select {
		case response := <-c.incoming:
			glog.V(3).Infof("RESPONSE RECEIVED: Worker %s received response from admin server: %T", workerID, response.Message)
			if taskAssign := response.GetTaskAssignment(); taskAssign != nil {
				// Validate TaskId is not empty before processing
				if taskAssign.TaskId == "" {
					glog.Warningf("Worker %s received task assignment with empty TaskId, ignoring", workerID)
					continue
				}

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
			glog.V(3).Infof("TASK REQUEST TIMEOUT: Worker %s - no task assignment received within 30 seconds", workerID)
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
