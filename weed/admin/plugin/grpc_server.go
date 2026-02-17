package plugin

import (
	"context"
	"fmt"
	"io"
	"sync"

	"github.com/seaweedfs/seaweedfs/weed/pb/plugin_pb"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/grpc"
)

// GrpcServer implements the plugin gRPC service
type GrpcServer struct {
	plugin_pb.UnimplementedPluginServiceServer
	plugin_pb.UnimplementedAdminQueryServiceServer
	plugin_pb.UnimplementedAdminCommandServiceServer

	registry   *Registry
	dispatcher *Dispatcher
	configMgr  *ConfigManager

	// Active connections
	connections map[string]*grpc.ClientStream
	connMu      sync.RWMutex
}

// NewGrpcServer creates a new gRPC server
func NewGrpcServer(registry *Registry, dispatcher *Dispatcher, configMgr *ConfigManager) *GrpcServer {
	return &GrpcServer{
		registry:    registry,
		dispatcher:  dispatcher,
		configMgr:   configMgr,
		connections: make(map[string]*grpc.ClientStream),
	}
}

// ============================================================================
// PluginService Implementation
// ============================================================================

// Connect handles bidirectional streaming for plugin registration and lifecycle
func (s *GrpcServer) Connect(stream grpc.BidiStreamingServer[plugin_pb.PluginMessage, plugin_pb.AdminMessage]) error {
	var pluginID string

	for {
		msg, err := stream.Recv()
		if err == io.EOF {
			// Stream closed normally
			if pluginID != "" {
				s.registry.UnregisterPlugin(pluginID)
			}
			return nil
		}
		if err != nil {
			return err
		}

		// Handle different message types
		switch content := msg.Content.(type) {
		case *plugin_pb.PluginMessage_Register:
			register := content.Register
			pluginID = register.PluginId
			_, _ = s.registry.RegisterPlugin(pluginID, register)
			if err != nil {
				return err
			}

		case *plugin_pb.PluginMessage_Heartbeat:
			heartbeat := content.Heartbeat
			if err := s.registry.UpdateHeartbeat(heartbeat.PluginId, heartbeat); err != nil {
				return err
			}

		case *plugin_pb.PluginMessage_StatusUpdate:
			// Handle job status updates
			statusUpdate := content.StatusUpdate
			if err := s.handleStatusUpdate(statusUpdate); err != nil {
				return err
			}
		}

		// Send acknowledgment
		adminMsg := &plugin_pb.AdminMessage{
			Content: &plugin_pb.AdminMessage_AdminCommand{
				AdminCommand: &plugin_pb.AdminCommand{
					CommandType: plugin_pb.AdminCommand_RELOAD_CONFIG,
				},
			},
		}

		if err := stream.Send(adminMsg); err != nil {
			if pluginID != "" {
				s.registry.UnregisterPlugin(pluginID)
			}
			return err
		}
	}
}

// ExecuteJob handles bidirectional streaming for job execution
func (s *GrpcServer) ExecuteJob(stream grpc.BidiStreamingServer[plugin_pb.JobExecutionMessage, plugin_pb.JobProgressMessage]) error {
	var jobID string

	for {
		execMsg, err := stream.Recv()
		if err == io.EOF {
			// Stream closed normally
			return nil
		}
		if err != nil {
			return err
		}

		jobID = execMsg.JobId

		// Handle different execution message types
		switch content := execMsg.Content.(type) {
		case *plugin_pb.JobExecutionMessage_JobStarted:
			started := content.JobStarted
			if err := s.handleJobStarted(jobID, started); err != nil {
				return err
			}

		case *plugin_pb.JobExecutionMessage_Progress:
			progress := content.Progress
			if err := s.dispatcher.UpdateJobProgress(jobID, progress.ProgressPercent); err != nil {
				return err
			}

		case *plugin_pb.JobExecutionMessage_Checkpoint:
			checkpoint := content.Checkpoint
			// Store checkpoint for resumption
			if err := s.storeCheckpoint(jobID, checkpoint); err != nil {
				return err
			}

		case *plugin_pb.JobExecutionMessage_JobCompleted:
			completed := content.JobCompleted
			if err := s.dispatcher.CompleteJob(jobID, completed); err != nil {
				return err
			}

		case *plugin_pb.JobExecutionMessage_JobFailed:
			failed := content.JobFailed
			if err := s.dispatcher.FailJob(jobID, failed); err != nil {
				return err
			}

		case *plugin_pb.JobExecutionMessage_LogEntry:
			logEntry := content.LogEntry
			if err := s.storeLogEntry(jobID, logEntry); err != nil {
				return err
			}
		}

		// Send progress update
		progressMsg := &plugin_pb.JobProgressMessage{
			JobId: jobID,
			Content: &plugin_pb.JobProgressMessage_Update{
				Update: &plugin_pb.JobProgressUpdate{
					JobId:             jobID,
					ProgressPercent:   0,
					CurrentStep:       "processing",
					StatusMessage:     "Processing job",
					Timestamp:         nil,
				},
			},
		}

		if err := stream.Send(progressMsg); err != nil {
			return err
		}
	}
}

// ============================================================================
// AdminQueryService Implementation
// ============================================================================

// GetPluginStats returns current plugin system statistics
func (s *GrpcServer) GetPluginStats(ctx context.Context, req *emptypb.Empty) (*plugin_pb.PluginStats, error) {
	plugins := s.registry.ListPlugins()
	jobs := s.dispatcher.ListAllJobs()

	var totalJobs, pendingJobs, runningJobs, completedJobs, failedJobs int32

	for _, job := range jobs {
		totalJobs++
		switch job.GetState() {
		case JobStatePending:
			pendingJobs++
		case JobStateRunning:
			runningJobs++
		case JobStateCompleted:
			completedJobs++
		case JobStateFailed:
			failedJobs++
		}
	}

	stats := &plugin_pb.PluginStats{
		TotalPlugins:  int32(len(plugins)),
		ActivePlugins: int32(len(plugins)),
		TotalJobs:     totalJobs,
		PendingJobs:   pendingJobs,
		RunningJobs:   runningJobs,
		CompletedJobs: completedJobs,
		FailedJobs:    failedJobs,
	}

	return stats, nil
}

// ListPlugins returns all connected plugins
func (s *GrpcServer) ListPlugins(ctx context.Context, req *emptypb.Empty) (*plugin_pb.PluginList, error) {
	plugins := s.registry.ListPlugins()
	pluginInfos := make([]*plugin_pb.PluginInfo, 0, len(plugins))

	for _, p := range plugins {
		info := &plugin_pb.PluginInfo{
			PluginId:        p.ID,
			Name:            p.Name,
			Version:         p.Version,
			ProtocolVersion: p.ProtocolVersion,
			Healthy:         p.IsHealthy(),
			Status:          p.GetState().String(),
		}

		// Add capabilities
		for _, cap := range p.Capabilities {
			info.Capabilities = append(info.Capabilities, cap)
		}

		pluginInfos = append(pluginInfos, info)
	}

	return &plugin_pb.PluginList{Plugins: pluginInfos}, nil
}

// ListJobs returns jobs filtered by type and status
func (s *GrpcServer) ListJobs(ctx context.Context, req *plugin_pb.ListJobsRequest) (*plugin_pb.JobList, error) {
	var state JobState
	switch req.Status {
	case plugin_pb.ListJobsRequest_PENDING:
		state = JobStatePending
	case plugin_pb.ListJobsRequest_RUNNING:
		state = JobStateRunning
	case plugin_pb.ListJobsRequest_COMPLETED:
		state = JobStateCompleted
	case plugin_pb.ListJobsRequest_FAILED:
		state = JobStateFailed
	default:
		state = JobStatePending
	}

	jobs := s.dispatcher.ListJobs(req.JobType, state)

	jobPbs := make([]*plugin_pb.Job, 0, len(jobs))
	for _, job := range jobs {
		jobPb := &plugin_pb.Job{
			JobId:              job.ID,
			JobType:            job.Type,
			Description:        job.Description,
			Priority:           job.Priority,
			CreatedAt:          nil,
			UpdatedAt:          nil,
			ExecutorPluginId:   job.ExecutorID,
			ProgressPercent:    job.GetProgress(),
			Config:             job.Config,
		}
		jobPbs = append(jobPbs, jobPb)
	}

	return &plugin_pb.JobList{
		Jobs:            jobPbs,
		TotalCount:      int32(len(jobPbs)),
		ReturnedCount:   int32(len(jobPbs)),
	}, nil
}

// GetJob returns detailed job information
func (s *GrpcServer) GetJob(ctx context.Context, req *plugin_pb.GetJobRequest) (*plugin_pb.JobDetail, error) {
	job, err := s.dispatcher.GetJob(req.JobId)
	if err != nil {
		return nil, fmt.Errorf("job not found: %w", err)
	}

	jobPb := &plugin_pb.Job{
		JobId:              job.ID,
		JobType:            job.Type,
		Description:        job.Description,
		Priority:           job.Priority,
		ExecutorPluginId:   job.ExecutorID,
		ProgressPercent:    job.GetProgress(),
		Config:             job.Config,
	}

	detail := &plugin_pb.JobDetail{
		Job:              jobPb,
		LogEntries:       job.LogEntries,
		CompletionInfo:   job.CompletedInfo,
		FailureInfo:      job.FailedInfo,
		CheckpointIds:    job.CheckpointIDs,
	}

	return detail, nil
}

// GetJobExecutionLog returns job execution logs
func (s *GrpcServer) GetJobExecutionLog(req *plugin_pb.GetJobExecutionLogRequest, stream grpc.ServerStreamingServer[plugin_pb.ExecutionLogEntry]) error {
	job, err := s.dispatcher.GetJob(req.JobId)
	if err != nil {
		return fmt.Errorf("job not found: %w", err)
	}

	// Send log entries
	for _, entry := range job.LogEntries {
		if err := stream.Send(entry); err != nil {
			return err
		}
	}

	return nil
}

// ============================================================================
// AdminCommandService Implementation
// ============================================================================

// UpdateJobTypeConfig updates configuration for a job type
func (s *GrpcServer) UpdateJobTypeConfig(ctx context.Context, req *plugin_pb.UpdateJobTypeConfigRequest) (*plugin_pb.JobTypeConfig, error) {
	config := req.Config

	_, err := s.configMgr.UpdateConfig(
		config.JobType,
		config.Enabled,
		config.AdminConfig,
		config.WorkerConfig,
		"admin",
	)
	if err != nil {
		return nil, fmt.Errorf("failed to update config: %w", err)
	}

	return config, nil
}

// CreateJob creates a new job
func (s *GrpcServer) CreateJob(ctx context.Context, req *plugin_pb.CreateJobRequest) (*plugin_pb.Job, error) {
	jobReq := &plugin_pb.JobRequest{
		JobType:       req.JobType,
		Description:   req.Description,
		Priority:      req.Priority,
		Config:        req.Config,
		Metadata:      req.Metadata,
		RequestSource: "admin",
	}

	job, err := s.dispatcher.QueueJob(req.JobType, jobReq, "")
	if err != nil {
		return nil, fmt.Errorf("failed to create job: %w", err)
	}

	return &plugin_pb.Job{
		JobId:           job.ID,
		JobType:         job.Type,
		Description:     job.Description,
		Priority:        job.Priority,
		ProgressPercent: job.GetProgress(),
		Config:          job.Config,
	}, nil
}

// CancelJob cancels a running job
func (s *GrpcServer) CancelJob(ctx context.Context, req *plugin_pb.CancelJobRequest) (*emptypb.Empty, error) {
	if err := s.dispatcher.CancelJob(req.JobId); err != nil {
		return nil, fmt.Errorf("failed to cancel job: %w", err)
	}

	return &emptypb.Empty{}, nil
}

// RetryJob retries a failed job
func (s *GrpcServer) RetryJob(ctx context.Context, req *plugin_pb.RetryJobRequest) (*plugin_pb.Job, error) {
	if err := s.dispatcher.RetryJob(req.JobId); err != nil {
		return nil, fmt.Errorf("failed to retry job: %w", err)
	}

	job, err := s.dispatcher.GetJob(req.JobId)
	if err != nil {
		return nil, fmt.Errorf("job not found: %w", err)
	}

	return &plugin_pb.Job{
		JobId:           job.ID,
		JobType:         job.Type,
		Description:     job.Description,
		Priority:        job.Priority,
		ProgressPercent: job.GetProgress(),
		Config:          job.Config,
	}, nil
}

// ============================================================================
// Helper Methods
// ============================================================================

// handleStatusUpdate handles execution status updates
func (s *GrpcServer) handleStatusUpdate(update *plugin_pb.ExecutionStatusUpdate) error {
	if err := s.dispatcher.UpdateJobProgress(update.JobId, update.ProgressPercent); err != nil {
		return err
	}
	return nil
}

// handleJobStarted handles job start messages
func (s *GrpcServer) handleJobStarted(jobID string, started *plugin_pb.JobStarted) error {
	job, err := s.dispatcher.GetJob(jobID)
	if err != nil {
		return err
	}

	job.SetState(JobStateRunning)
	return nil
}

// storeCheckpoint stores a job checkpoint
func (s *GrpcServer) storeCheckpoint(jobID string, checkpoint *plugin_pb.JobCheckpoint) error {
	job, err := s.dispatcher.GetJob(jobID)
	if err != nil {
		return err
	}

	job.mu.Lock()
	defer job.mu.Unlock()

	job.CheckpointIDs = append(job.CheckpointIDs, checkpoint.CheckpointId)
	return nil
}

// storeLogEntry stores a log entry for a job
func (s *GrpcServer) storeLogEntry(jobID string, logEntry *plugin_pb.ExecutionLog) error {
	job, err := s.dispatcher.GetJob(jobID)
	if err != nil {
		return err
	}

	job.mu.Lock()
	defer job.mu.Unlock()

	job.LogEntries = append(job.LogEntries, &plugin_pb.ExecutionLogEntry{
		Timestamp: logEntry.Timestamp,
		Level:     logEntry.Level,
		Message:   logEntry.Message,
		Context:   logEntry.Context,
	})

	return nil
}
