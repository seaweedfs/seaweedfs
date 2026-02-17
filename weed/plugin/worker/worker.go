package pluginworker

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/plugin_pb"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	defaultHeartbeatInterval = 15 * time.Second
	defaultReconnectDelay    = 5 * time.Second
	defaultSendBufferSize    = 256
)

// DetectionSender sends detection responses for one request.
type DetectionSender interface {
	SendProposals(*plugin_pb.DetectionProposals) error
	SendComplete(*plugin_pb.DetectionComplete) error
}

// ExecutionSender sends execution progress/completion responses for one request.
type ExecutionSender interface {
	SendProgress(*plugin_pb.JobProgressUpdate) error
	SendCompleted(*plugin_pb.JobCompleted) error
}

// JobHandler implements one plugin job type on the worker side.
type JobHandler interface {
	Capability() *plugin_pb.JobTypeCapability
	Descriptor() *plugin_pb.JobTypeDescriptor
	Detect(context.Context, *plugin_pb.RunDetectionRequest, DetectionSender) error
	Execute(context.Context, *plugin_pb.ExecuteJobRequest, ExecutionSender) error
}

// WorkerOptions configures one plugin worker process.
type WorkerOptions struct {
	AdminServer             string
	WorkerID                string
	WorkerVersion           string
	WorkerAddress           string
	HeartbeatInterval       time.Duration
	ReconnectDelay          time.Duration
	MaxDetectionConcurrency int
	MaxExecutionConcurrency int
	GrpcDialOption          grpc.DialOption
	Handler                 JobHandler
}

// Worker runs one plugin job handler over plugin.proto stream.
type Worker struct {
	opts WorkerOptions

	detectSlots chan struct{}
	execSlots   chan struct{}

	runningMu   sync.RWMutex
	runningWork map[string]*plugin_pb.RunningWork

	workCancelMu sync.Mutex
	workCancel   map[string]context.CancelFunc

	workerID string
}

// NewWorker creates a plugin worker instance.
func NewWorker(options WorkerOptions) (*Worker, error) {
	if strings.TrimSpace(options.AdminServer) == "" {
		return nil, fmt.Errorf("admin server is required")
	}
	if options.Handler == nil {
		return nil, fmt.Errorf("job handler is required")
	}
	if options.GrpcDialOption == nil {
		return nil, fmt.Errorf("grpc dial option is required")
	}
	if options.HeartbeatInterval <= 0 {
		options.HeartbeatInterval = defaultHeartbeatInterval
	}
	if options.ReconnectDelay <= 0 {
		options.ReconnectDelay = defaultReconnectDelay
	}
	if options.MaxDetectionConcurrency <= 0 {
		options.MaxDetectionConcurrency = 1
	}
	if options.MaxExecutionConcurrency <= 0 {
		options.MaxExecutionConcurrency = 1
	}
	if strings.TrimSpace(options.WorkerVersion) == "" {
		options.WorkerVersion = "dev"
	}

	workerID := strings.TrimSpace(options.WorkerID)
	if workerID == "" {
		workerID = generateWorkerID()
	}

	workerAddress := strings.TrimSpace(options.WorkerAddress)
	if workerAddress == "" {
		hostname, _ := os.Hostname()
		workerAddress = hostname
	}
	opts := options
	opts.WorkerAddress = workerAddress

	w := &Worker{
		opts:        opts,
		detectSlots: make(chan struct{}, opts.MaxDetectionConcurrency),
		execSlots:   make(chan struct{}, opts.MaxExecutionConcurrency),
		runningWork: make(map[string]*plugin_pb.RunningWork),
		workCancel:  make(map[string]context.CancelFunc),
		workerID:    workerID,
	}
	return w, nil
}

// Run keeps the plugin worker connected and reconnects on stream failures.
func (w *Worker) Run(ctx context.Context) error {
	adminAddress := pb.ServerToGrpcAddress(w.opts.AdminServer)

	for {
		select {
		case <-ctx.Done():
			return nil
		default:
		}

		if err := w.runOnce(ctx, adminAddress); err != nil {
			if ctx.Err() != nil {
				return nil
			}
			glog.Warningf("Plugin worker %s stream ended: %v", w.workerID, err)
		}

		select {
		case <-ctx.Done():
			return nil
		case <-time.After(w.opts.ReconnectDelay):
		}
	}
}

func (w *Worker) runOnce(ctx context.Context, adminAddress string) error {
	dialCtx, cancelDial := context.WithTimeout(ctx, 5*time.Second)
	defer cancelDial()

	conn, err := pb.GrpcDial(dialCtx, adminAddress, false, w.opts.GrpcDialOption)
	if err != nil {
		return fmt.Errorf("dial admin %s: %w", adminAddress, err)
	}
	defer conn.Close()

	client := plugin_pb.NewPluginControlServiceClient(conn)
	connCtx, cancelConn := context.WithCancel(ctx)
	defer cancelConn()

	stream, err := client.WorkerStream(connCtx)
	if err != nil {
		return fmt.Errorf("open worker stream: %w", err)
	}

	sendCh := make(chan *plugin_pb.WorkerToAdminMessage, defaultSendBufferSize)
	sendErrCh := make(chan error, 1)

	send := func(msg *plugin_pb.WorkerToAdminMessage) bool {
		if msg == nil {
			return false
		}
		msg.WorkerId = w.workerID
		if msg.SentAt == nil {
			msg.SentAt = timestamppb.Now()
		}
		select {
		case <-connCtx.Done():
			return false
		case sendCh <- msg:
			return true
		}
	}

	go func() {
		for {
			select {
			case <-connCtx.Done():
				return
			case msg := <-sendCh:
				if msg == nil {
					continue
				}
				if err := stream.Send(msg); err != nil {
					select {
					case sendErrCh <- err:
					default:
					}
					cancelConn()
					return
				}
			}
		}
	}()

	if !send(&plugin_pb.WorkerToAdminMessage{
		Body: &plugin_pb.WorkerToAdminMessage_Hello{Hello: w.buildHello()},
	}) {
		return fmt.Errorf("send worker hello: stream closed")
	}

	heartbeatTicker := time.NewTicker(w.opts.HeartbeatInterval)
	defer heartbeatTicker.Stop()

	go func() {
		for {
			select {
			case <-connCtx.Done():
				return
			case <-heartbeatTicker.C:
				send(&plugin_pb.WorkerToAdminMessage{
					Body: &plugin_pb.WorkerToAdminMessage_Heartbeat{Heartbeat: w.buildHeartbeat()},
				})
			}
		}
	}()

	for {
		select {
		case <-connCtx.Done():
			return connCtx.Err()
		case err := <-sendErrCh:
			return fmt.Errorf("send to admin stream: %w", err)
		default:
		}

		message, err := stream.Recv()
		if err != nil {
			return fmt.Errorf("recv admin message: %w", err)
		}

		w.handleAdminMessage(connCtx, message, send)
	}
}

func (w *Worker) handleAdminMessage(
	ctx context.Context,
	message *plugin_pb.AdminToWorkerMessage,
	send func(*plugin_pb.WorkerToAdminMessage) bool,
) {
	if message == nil {
		return
	}

	switch body := message.Body.(type) {
	case *plugin_pb.AdminToWorkerMessage_Hello:
		_ = body
	case *plugin_pb.AdminToWorkerMessage_RequestConfigSchema:
		w.handleSchemaRequest(message.GetRequestId(), body.RequestConfigSchema, send)
	case *plugin_pb.AdminToWorkerMessage_RunDetectionRequest:
		w.handleDetectionRequest(ctx, message.GetRequestId(), body.RunDetectionRequest, send)
	case *plugin_pb.AdminToWorkerMessage_ExecuteJobRequest:
		w.handleExecuteRequest(ctx, message.GetRequestId(), body.ExecuteJobRequest, send)
	case *plugin_pb.AdminToWorkerMessage_CancelRequest:
		cancel := body.CancelRequest
		targetID := ""
		if cancel != nil {
			targetID = strings.TrimSpace(cancel.TargetId)
		}
		accepted := false
		ackMessage := "cancel target is required"
		if targetID != "" {
			if w.cancelWork(targetID) {
				accepted = true
				ackMessage = "cancel request accepted"
			} else {
				ackMessage = "cancel target not found"
			}
		}
		send(&plugin_pb.WorkerToAdminMessage{
			Body: &plugin_pb.WorkerToAdminMessage_Acknowledge{Acknowledge: &plugin_pb.WorkerAcknowledge{
				RequestId: message.GetRequestId(),
				Accepted:  accepted,
				Message:   ackMessage,
			}},
		})
	case *plugin_pb.AdminToWorkerMessage_Shutdown:
		send(&plugin_pb.WorkerToAdminMessage{
			Body: &plugin_pb.WorkerToAdminMessage_Acknowledge{Acknowledge: &plugin_pb.WorkerAcknowledge{
				RequestId: message.GetRequestId(),
				Accepted:  true,
				Message:   "shutdown acknowledged",
			}},
		})
	default:
		send(&plugin_pb.WorkerToAdminMessage{
			Body: &plugin_pb.WorkerToAdminMessage_Acknowledge{Acknowledge: &plugin_pb.WorkerAcknowledge{
				RequestId: message.GetRequestId(),
				Accepted:  false,
				Message:   "unsupported request body",
			}},
		})
	}
}

func (w *Worker) handleSchemaRequest(requestID string, request *plugin_pb.RequestConfigSchema, send func(*plugin_pb.WorkerToAdminMessage) bool) {
	jobType := ""
	if request != nil {
		jobType = request.JobType
	}
	descriptor := w.opts.Handler.Descriptor()
	if descriptor == nil || descriptor.JobType == "" {
		send(&plugin_pb.WorkerToAdminMessage{
			Body: &plugin_pb.WorkerToAdminMessage_ConfigSchemaResponse{ConfigSchemaResponse: &plugin_pb.ConfigSchemaResponse{
				RequestId:    requestID,
				JobType:      jobType,
				Success:      false,
				ErrorMessage: "handler descriptor is not configured",
			}},
		})
		return
	}
	if jobType != "" && descriptor.JobType != jobType {
		send(&plugin_pb.WorkerToAdminMessage{
			Body: &plugin_pb.WorkerToAdminMessage_ConfigSchemaResponse{ConfigSchemaResponse: &plugin_pb.ConfigSchemaResponse{
				RequestId:    requestID,
				JobType:      jobType,
				Success:      false,
				ErrorMessage: fmt.Sprintf("job type %q is not handled by this worker", jobType),
			}},
		})
		return
	}

	send(&plugin_pb.WorkerToAdminMessage{
		Body: &plugin_pb.WorkerToAdminMessage_ConfigSchemaResponse{ConfigSchemaResponse: &plugin_pb.ConfigSchemaResponse{
			RequestId:         requestID,
			JobType:           descriptor.JobType,
			Success:           true,
			JobTypeDescriptor: descriptor,
		}},
	})
}

func (w *Worker) handleDetectionRequest(
	ctx context.Context,
	requestID string,
	request *plugin_pb.RunDetectionRequest,
	send func(*plugin_pb.WorkerToAdminMessage) bool,
) {
	if request == nil {
		send(&plugin_pb.WorkerToAdminMessage{
			Body: &plugin_pb.WorkerToAdminMessage_DetectionComplete{DetectionComplete: &plugin_pb.DetectionComplete{
				RequestId:    requestID,
				Success:      false,
				ErrorMessage: "run detection request is nil",
			}},
		})
		return
	}

	select {
	case w.detectSlots <- struct{}{}:
	default:
		send(&plugin_pb.WorkerToAdminMessage{
			Body: &plugin_pb.WorkerToAdminMessage_DetectionComplete{DetectionComplete: &plugin_pb.DetectionComplete{
				RequestId:    requestID,
				JobType:      request.JobType,
				Success:      false,
				ErrorMessage: "detector is at capacity",
			}},
		})
		return
	}

	workKey := "detect:" + requestID
	w.setRunningWork(workKey, &plugin_pb.RunningWork{
		WorkId:          requestID,
		Kind:            plugin_pb.WorkKind_WORK_KIND_DETECTION,
		JobType:         request.JobType,
		State:           plugin_pb.JobState_JOB_STATE_RUNNING,
		ProgressPercent: 0,
		Stage:           "detecting",
	})

	send(&plugin_pb.WorkerToAdminMessage{
		Body: &plugin_pb.WorkerToAdminMessage_Acknowledge{Acknowledge: &plugin_pb.WorkerAcknowledge{
			RequestId: requestID,
			Accepted:  true,
			Message:   "detection request accepted",
		}},
	})

	go func() {
		requestCtx, cancelRequest := context.WithCancel(ctx)
		w.setWorkCancel(cancelRequest, requestID)
		defer func() {
			w.clearWorkCancel(requestID)
			cancelRequest()
			<-w.detectSlots
			w.clearRunningWork(workKey)
		}()

		detectionSender := &detectionSender{
			requestID: requestID,
			jobType:   request.JobType,
			send:      send,
		}
		if err := w.opts.Handler.Detect(requestCtx, request, detectionSender); err != nil {
			detectionSender.SendComplete(&plugin_pb.DetectionComplete{
				Success:      false,
				ErrorMessage: err.Error(),
			})
		}
	}()
}

func (w *Worker) handleExecuteRequest(
	ctx context.Context,
	requestID string,
	request *plugin_pb.ExecuteJobRequest,
	send func(*plugin_pb.WorkerToAdminMessage) bool,
) {
	if request == nil || request.Job == nil {
		send(&plugin_pb.WorkerToAdminMessage{
			Body: &plugin_pb.WorkerToAdminMessage_JobCompleted{JobCompleted: &plugin_pb.JobCompleted{
				RequestId:    requestID,
				Success:      false,
				ErrorMessage: "execute request/job is nil",
			}},
		})
		return
	}

	select {
	case w.execSlots <- struct{}{}:
	default:
		send(&plugin_pb.WorkerToAdminMessage{
			Body: &plugin_pb.WorkerToAdminMessage_JobCompleted{JobCompleted: &plugin_pb.JobCompleted{
				RequestId:    requestID,
				JobId:        request.Job.JobId,
				JobType:      request.Job.JobType,
				Success:      false,
				ErrorMessage: "executor is at capacity",
			}},
		})
		return
	}

	workKey := "exec:" + requestID
	w.setRunningWork(workKey, &plugin_pb.RunningWork{
		WorkId:          request.Job.JobId,
		Kind:            plugin_pb.WorkKind_WORK_KIND_EXECUTION,
		JobType:         request.Job.JobType,
		State:           plugin_pb.JobState_JOB_STATE_RUNNING,
		ProgressPercent: 0,
		Stage:           "starting",
	})

	send(&plugin_pb.WorkerToAdminMessage{
		Body: &plugin_pb.WorkerToAdminMessage_Acknowledge{Acknowledge: &plugin_pb.WorkerAcknowledge{
			RequestId: requestID,
			Accepted:  true,
			Message:   "execute request accepted",
		}},
	})

	go func() {
		requestCtx, cancelRequest := context.WithCancel(ctx)
		w.setWorkCancel(cancelRequest, requestID, request.Job.JobId)
		defer func() {
			w.clearWorkCancel(requestID, request.Job.JobId)
			cancelRequest()
			<-w.execSlots
			w.clearRunningWork(workKey)
		}()

		executionSender := &executionSender{
			requestID: requestID,
			jobID:     request.Job.JobId,
			jobType:   request.Job.JobType,
			send:      send,
			onProgress: func(progress float64, stage string) {
				w.updateRunningExecution(workKey, progress, stage)
			},
		}
		if err := w.opts.Handler.Execute(requestCtx, request, executionSender); err != nil {
			executionSender.SendCompleted(&plugin_pb.JobCompleted{
				Success:      false,
				ErrorMessage: err.Error(),
			})
		}
	}()
}

func (w *Worker) buildHello() *plugin_pb.WorkerHello {
	capability := w.opts.Handler.Capability()
	if capability == nil {
		capability = &plugin_pb.JobTypeCapability{}
	} else {
		capability = proto.Clone(capability).(*plugin_pb.JobTypeCapability)
	}
	capability.MaxDetectionConcurrency = int32(cap(w.detectSlots))
	capability.MaxExecutionConcurrency = int32(cap(w.execSlots))

	instanceID := generateWorkerID()
	return &plugin_pb.WorkerHello{
		WorkerId:         w.workerID,
		WorkerInstanceId: "inst-" + instanceID,
		Address:          w.opts.WorkerAddress,
		WorkerVersion:    w.opts.WorkerVersion,
		ProtocolVersion:  "plugin.v1",
		Capabilities:     []*plugin_pb.JobTypeCapability{capability},
		Metadata: map[string]string{
			"runtime": "plugin",
		},
	}
}

func (w *Worker) buildHeartbeat() *plugin_pb.WorkerHeartbeat {
	w.runningMu.RLock()
	running := make([]*plugin_pb.RunningWork, 0, len(w.runningWork))
	for _, work := range w.runningWork {
		if work == nil {
			continue
		}
		cloned := *work
		running = append(running, &cloned)
	}
	w.runningMu.RUnlock()

	detectUsed := len(w.detectSlots)
	execUsed := len(w.execSlots)
	return &plugin_pb.WorkerHeartbeat{
		WorkerId:            w.workerID,
		RunningWork:         running,
		DetectionSlotsUsed:  int32(detectUsed),
		DetectionSlotsTotal: int32(cap(w.detectSlots)),
		ExecutionSlotsUsed:  int32(execUsed),
		ExecutionSlotsTotal: int32(cap(w.execSlots)),
		QueuedJobsByType:    map[string]int32{},
		Metadata: map[string]string{
			"runtime": "plugin",
		},
	}
}

func (w *Worker) setRunningWork(key string, work *plugin_pb.RunningWork) {
	if strings.TrimSpace(key) == "" || work == nil {
		return
	}
	w.runningMu.Lock()
	w.runningWork[key] = work
	w.runningMu.Unlock()
}

func (w *Worker) clearRunningWork(key string) {
	w.runningMu.Lock()
	delete(w.runningWork, key)
	w.runningMu.Unlock()
}

func (w *Worker) updateRunningExecution(key string, progress float64, stage string) {
	w.runningMu.Lock()
	if running := w.runningWork[key]; running != nil {
		running.ProgressPercent = progress
		if strings.TrimSpace(stage) != "" {
			running.Stage = stage
		}
		running.State = plugin_pb.JobState_JOB_STATE_RUNNING
	}
	w.runningMu.Unlock()
}

type detectionSender struct {
	requestID string
	jobType   string
	send      func(*plugin_pb.WorkerToAdminMessage) bool
}

func (s *detectionSender) SendProposals(proposals *plugin_pb.DetectionProposals) error {
	if proposals == nil {
		return fmt.Errorf("detection proposals are nil")
	}
	if proposals.RequestId == "" {
		proposals.RequestId = s.requestID
	}
	if proposals.JobType == "" {
		proposals.JobType = s.jobType
	}
	if !s.send(&plugin_pb.WorkerToAdminMessage{
		Body: &plugin_pb.WorkerToAdminMessage_DetectionProposals{DetectionProposals: proposals},
	}) {
		return fmt.Errorf("stream closed")
	}
	return nil
}

func (s *detectionSender) SendComplete(complete *plugin_pb.DetectionComplete) error {
	if complete == nil {
		return fmt.Errorf("detection complete is nil")
	}
	if complete.RequestId == "" {
		complete.RequestId = s.requestID
	}
	if complete.JobType == "" {
		complete.JobType = s.jobType
	}
	if !s.send(&plugin_pb.WorkerToAdminMessage{
		Body: &plugin_pb.WorkerToAdminMessage_DetectionComplete{DetectionComplete: complete},
	}) {
		return fmt.Errorf("stream closed")
	}
	return nil
}

type executionSender struct {
	requestID  string
	jobID      string
	jobType    string
	send       func(*plugin_pb.WorkerToAdminMessage) bool
	onProgress func(progress float64, stage string)
}

func (s *executionSender) SendProgress(progress *plugin_pb.JobProgressUpdate) error {
	if progress == nil {
		return fmt.Errorf("job progress is nil")
	}
	if progress.RequestId == "" {
		progress.RequestId = s.requestID
	}
	if progress.JobId == "" {
		progress.JobId = s.jobID
	}
	if progress.JobType == "" {
		progress.JobType = s.jobType
	}
	if progress.UpdatedAt == nil {
		progress.UpdatedAt = timestamppb.Now()
	}
	if s.onProgress != nil {
		s.onProgress(progress.ProgressPercent, progress.Stage)
	}
	if !s.send(&plugin_pb.WorkerToAdminMessage{
		Body: &plugin_pb.WorkerToAdminMessage_JobProgressUpdate{JobProgressUpdate: progress},
	}) {
		return fmt.Errorf("stream closed")
	}
	return nil
}

func (s *executionSender) SendCompleted(completed *plugin_pb.JobCompleted) error {
	if completed == nil {
		return fmt.Errorf("job completed is nil")
	}
	if completed.RequestId == "" {
		completed.RequestId = s.requestID
	}
	if completed.JobId == "" {
		completed.JobId = s.jobID
	}
	if completed.JobType == "" {
		completed.JobType = s.jobType
	}
	if completed.CompletedAt == nil {
		completed.CompletedAt = timestamppb.Now()
	}
	if !s.send(&plugin_pb.WorkerToAdminMessage{
		Body: &plugin_pb.WorkerToAdminMessage_JobCompleted{JobCompleted: completed},
	}) {
		return fmt.Errorf("stream closed")
	}
	return nil
}

func generateWorkerID() string {
	random := make([]byte, 3)
	if _, err := rand.Read(random); err != nil {
		return fmt.Sprintf("plugin-%d", time.Now().UnixNano())
	}
	return "plugin-" + hex.EncodeToString(random)
}

func (w *Worker) setWorkCancel(cancel context.CancelFunc, keys ...string) {
	if cancel == nil {
		return
	}
	w.workCancelMu.Lock()
	defer w.workCancelMu.Unlock()
	for _, key := range keys {
		key = strings.TrimSpace(key)
		if key == "" {
			continue
		}
		w.workCancel[key] = cancel
	}
}

func (w *Worker) clearWorkCancel(keys ...string) {
	w.workCancelMu.Lock()
	defer w.workCancelMu.Unlock()
	for _, key := range keys {
		key = strings.TrimSpace(key)
		if key == "" {
			continue
		}
		delete(w.workCancel, key)
	}
}

func (w *Worker) cancelWork(targetID string) bool {
	targetID = strings.TrimSpace(targetID)
	if targetID == "" {
		return false
	}

	w.workCancelMu.Lock()
	cancel := w.workCancel[targetID]
	w.workCancelMu.Unlock()
	if cancel == nil {
		return false
	}
	cancel()
	return true
}
