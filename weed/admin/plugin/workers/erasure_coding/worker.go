package erasure_coding

import (
	"context"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/plugin_pb"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// Worker is the main erasure coding plugin worker
type Worker struct {
	id         string
	name       string
	version    string
	masterAddr string
	httpPort   int

	// gRPC connection
	conn   *grpc.ClientConn
	client plugin_pb.PluginServiceClient

	// Detector and executor
	detector *Detector

	// Context and coordination
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	// Running jobs
	jobsMu      sync.RWMutex
	runningJobs map[string]context.CancelFunc
}

// NewWorker creates a new erasure coding worker
func NewWorker(id, masterAddr string, httpPort int) *Worker {
	return &Worker{
		id:          id,
		name:        "erasure_coding_worker",
		version:     "v1",
		masterAddr:  masterAddr,
		httpPort:    httpPort,
		runningJobs: make(map[string]context.CancelFunc),
		detector:    NewDetector(masterAddr),
	}
}

// Start starts the worker and connects to the admin server
func (w *Worker) Start(ctx context.Context) error {
	glog.Infof("erasure_coding worker: starting (id=%s, master=%s, httpPort=%d)", w.id, w.masterAddr, w.httpPort)

	w.ctx, w.cancel = context.WithCancel(ctx)

	// Connect to admin server via gRPC
	// Admin server runs on httpPort + 10000
	adminPort := w.httpPort + 10000
	adminAddr := fmt.Sprintf("localhost:%d", adminPort)

	glog.Infof("erasure_coding worker: connecting to admin at %s", adminAddr)

	conn, err := grpc.Dial(adminAddr, grpc.WithInsecure())
	if err != nil {
		return fmt.Errorf("failed to connect to admin server: %w", err)
	}

	w.conn = conn
	w.client = plugin_pb.NewPluginServiceClient(conn)

	// Start connection handler in goroutine
	w.wg.Add(1)
	go w.handleConnection()

	return nil
}

// Stop stops the worker and closes all connections
func (w *Worker) Stop() error {
	glog.Infof("erasure_coding worker: stopping (id=%s)", w.id)

	// Cancel context to signal all goroutines
	if w.cancel != nil {
		w.cancel()
	}

	// Wait for goroutines with timeout
	done := make(chan struct{})
	go func() {
		w.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(10 * time.Second):
		glog.Warningf("erasure_coding worker: graceful shutdown timeout")
	}

	// Close gRPC connection
	if w.conn != nil {
		if err := w.conn.Close(); err != nil {
			glog.Errorf("erasure_coding worker: error closing gRPC connection: %v", err)
		}
	}

	glog.Infof("erasure_coding worker: stopped")
	return nil
}

// handleConnection handles the bidirectional gRPC connection with the admin server
func (w *Worker) handleConnection() {
	defer w.wg.Done()

	glog.Infof("erasure_coding worker: establishing bidirectional stream")

	// Create bidirectional stream
	stream, err := w.client.Connect(w.ctx)
	if err != nil {
		glog.Errorf("erasure_coding worker: failed to create stream: %v", err)
		return
	}

	// Send registration message
	if err := w.sendRegistration(stream); err != nil {
		glog.Errorf("erasure_coding worker: failed to send registration: %v", err)
		return
	}

	// Start heartbeat goroutine
	w.wg.Add(1)
	go w.sendHeartbeats(stream)

	// Start job executor goroutine
	w.wg.Add(1)
	go w.executeJobs(stream)

	// Listen for messages from admin
	w.listenForMessages(stream)

	glog.Infof("erasure_coding worker: connection closed")
}

// sendRegistration sends the initial registration message
func (w *Worker) sendRegistration(stream plugin_pb.PluginService_ConnectClient) error {
	capabilities := []*plugin_pb.JobTypeCapability{
		{
			JobType:    "erasure_coding",
			CanDetect:  true,
			CanExecute: true,
			Version:    "v1",
		},
	}

	register := &plugin_pb.PluginRegister{
		PluginId:        w.id,
		Name:            w.name,
		Version:         w.version,
		ProtocolVersion: "v1",
		Capabilities:    capabilities,
	}

	msg := &plugin_pb.PluginMessage{
		Content: &plugin_pb.PluginMessage_Register{
			Register: register,
		},
	}

	return stream.Send(msg)
}

// sendHeartbeats sends periodic heartbeat messages
func (w *Worker) sendHeartbeats(stream plugin_pb.PluginService_ConnectClient) {
	defer w.wg.Done()

	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-w.ctx.Done():
			return
		case <-ticker.C:
			w.jobsMu.RLock()
			pendingJobs := int32(len(w.runningJobs))
			w.jobsMu.RUnlock()

			heartbeat := &plugin_pb.PluginHeartbeat{
				PluginId:        w.id,
				Timestamp:       timestamppb.Now(),
				UptimeSeconds:   int64(time.Since(time.Now()).Seconds()),
				PendingJobs:     pendingJobs,
				CpuUsagePercent: 0, // TODO: Get actual CPU usage
				MemoryUsageMb:   0, // TODO: Get actual memory usage
			}

			msg := &plugin_pb.PluginMessage{
				Content: &plugin_pb.PluginMessage_Heartbeat{
					Heartbeat: heartbeat,
				},
			}

			if err := stream.Send(msg); err != nil {
				glog.Errorf("erasure_coding worker: failed to send heartbeat: %v", err)
				return
			}
		}
	}
}

// executeJobs periodically detects and executes jobs
func (w *Worker) executeJobs(stream plugin_pb.PluginService_ConnectClient) {
	defer w.wg.Done()

	ticker := time.NewTicker(5 * time.Minute)
	defer ticker.Stop()

	for {
		select {
		case <-w.ctx.Done():
			return
		case <-ticker.C:
			// TODO: Implement job detection and execution
			// Query admin for jobs or use detector to find jobs
		}
	}
}

// listenForMessages listens for messages from the admin server
func (w *Worker) listenForMessages(stream plugin_pb.PluginService_ConnectClient) {
	for {
		msg, err := stream.Recv()
		if err == io.EOF {
			glog.Infof("erasure_coding worker: admin closed connection")
			return
		}
		if err != nil {
			glog.Errorf("erasure_coding worker: failed to receive message: %v", err)
			return
		}

		if msg.Content == nil {
			continue
		}

		switch content := msg.Content.(type) {
		case *plugin_pb.AdminMessage_JobRequest:
			w.handleJobRequest(content.JobRequest)
		case *plugin_pb.AdminMessage_ConfigUpdate:
			glog.Infof("erasure_coding worker: received config update: %s", content.ConfigUpdate.JobType)
		case *plugin_pb.AdminMessage_AdminCommand:
			w.handleAdminCommand(content.AdminCommand)
		}
	}
}

// handleJobRequest processes a job request from the admin
func (w *Worker) handleJobRequest(jobReq *plugin_pb.JobRequest) {
	glog.Infof("erasure_coding worker: received job request (id=%s, type=%s)", jobReq.JobId, jobReq.JobType)

	// Create job context
	jobCtx, cancel := context.WithCancel(w.ctx)

	w.jobsMu.Lock()
	w.runningJobs[jobReq.JobId] = cancel
	w.jobsMu.Unlock()

	// Execute job in goroutine
	w.wg.Add(1)
	go func() {
		defer w.wg.Done()
		defer func() {
			w.jobsMu.Lock()
			delete(w.runningJobs, jobReq.JobId)
			w.jobsMu.Unlock()
		}()

		w.executeJobRequest(jobCtx, jobReq)
	}()
}

// executeJobRequest executes a single job request
func (w *Worker) executeJobRequest(ctx context.Context, jobReq *plugin_pb.JobRequest) {
	// Build job config from request
	config := &plugin_pb.JobTypeConfig{
		JobType:      jobReq.JobType,
		AdminConfig:  nil,
		WorkerConfig: jobReq.Config,
	}

	// Create executor
	executor := NewExecutor(jobReq.JobId, config)

	// Progress channel
	progressChan := make(chan *plugin_pb.JobProgress, 10)

	// Execute job
	go func() {
		if err := executor.Execute(ctx, jobReq.Metadata, progressChan); err != nil {
			glog.Errorf("erasure_coding worker: job execution failed (id=%s): %v", jobReq.JobId, err)
		}
		close(progressChan)
	}()

	// TODO: Send progress updates back to admin via ExecuteJob RPC
	for progress := range progressChan {
		_ = progress
		glog.Infof("erasure_coding worker: job %s progress: %d%%", jobReq.JobId, progress.ProgressPercent)
	}
}

// handleAdminCommand processes admin commands
func (w *Worker) handleAdminCommand(cmd *plugin_pb.AdminCommand) {
	glog.Infof("erasure_coding worker: received admin command: %v", cmd.CommandType)

	switch cmd.CommandType {
	case plugin_pb.AdminCommand_RELOAD_CONFIG:
		glog.Infof("erasure_coding worker: reloading configuration")
	case plugin_pb.AdminCommand_ENABLE_JOB_TYPE:
		glog.Infof("erasure_coding worker: enabling erasure_coding job type")
	case plugin_pb.AdminCommand_DISABLE_JOB_TYPE:
		glog.Infof("erasure_coding worker: disabling erasure_coding job type")
	case plugin_pb.AdminCommand_SHUTDOWN:
		glog.Infof("erasure_coding worker: received shutdown command")
		w.Stop()
	}
}

// GetCapabilities returns the worker's capabilities
func (w *Worker) GetCapabilities() []*plugin_pb.JobTypeCapability {
	return []*plugin_pb.JobTypeCapability{
		{
			JobType:    "erasure_coding",
			CanDetect:  true,
			CanExecute: true,
			Version:    "v1",
		},
	}
}

// GetConfigurationSchema returns the configuration schema
func (w *Worker) GetConfigurationSchema() *plugin_pb.JobTypeConfigSchema {
	return GetConfigurationSchema()
}

// Run is a convenience method that starts the worker and waits for context cancellation
func (w *Worker) Run(ctx context.Context) error {
	if err := w.Start(ctx); err != nil {
		return err
	}
	defer w.Stop()

	<-ctx.Done()
	return nil
}

// StartWorkerServer starts the erasure coding worker with the given configuration
func StartWorkerServer(masterAddr string, httpPort int) error {
	pluginID := fmt.Sprintf("ec_worker_%d", time.Now().Unix())
	worker := NewWorker(pluginID, masterAddr, httpPort)

	ctx := context.Background()
	return worker.Run(ctx)
}
