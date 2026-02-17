package testing

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb/plugin_pb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// TestHarness provides utilities for testing plugin functionality
type TestHarness struct {
	ctx             context.Context
	cancel          context.CancelFunc
	adminServer     *MockAdminServer
	pluginClient    plugin_pb.PluginService_ConnectClient
	executionClient plugin_pb.PluginService_ExecuteJobClient
	mu              sync.RWMutex
	createdJobs     map[string]*plugin_pb.JobRequest
	executedJobs    map[string]*ExecutionTracker
	progressUpdates map[string][]*plugin_pb.JobProgress
	testDataDir     string
}

// ExecutionTracker tracks the execution of a job
type ExecutionTracker struct {
	JobID           string
	Status          string
	ProgressPercent int32
	Messages        []*plugin_pb.JobExecutionMessage
	StartTime       time.Time
	LastUpdate      time.Time
	mu              sync.RWMutex
}

// NewTestHarness creates a new test harness
func NewTestHarness(ctx context.Context) *TestHarness {
	if ctx == nil {
		ctx = context.Background()
	}
	ctx, cancel := context.WithCancel(ctx)

	return &TestHarness{
		ctx:             ctx,
		cancel:          cancel,
		adminServer:     NewMockAdminServer(),
		createdJobs:     make(map[string]*plugin_pb.JobRequest),
		executedJobs:    make(map[string]*ExecutionTracker),
		progressUpdates: make(map[string][]*plugin_pb.JobProgress),
	}
}

// Setup initializes the test harness and starts mock connections
func (h *TestHarness) Setup() error {
	h.mu.Lock()
	defer h.mu.Unlock()

	if h.adminServer == nil {
		h.adminServer = NewMockAdminServer()
	}

	return nil
}

// Teardown cleans up test resources
func (h *TestHarness) Teardown() error {
	h.mu.Lock()
	defer h.mu.Unlock()

	if h.cancel != nil {
		h.cancel()
	}

	if h.adminServer != nil {
		h.adminServer.Close()
	}

	return nil
}

// GetContext returns the test context
func (h *TestHarness) GetContext() context.Context {
	return h.ctx
}

// CreateJob creates a test job request
func (h *TestHarness) CreateJob(jobType, description string, priority int64, config []*plugin_pb.ConfigFieldValue) *plugin_pb.JobRequest {
	h.mu.Lock()
	defer h.mu.Unlock()

	jobID := fmt.Sprintf("test-job-%d", time.Now().UnixNano())
	job := &plugin_pb.JobRequest{
		JobId:       jobID,
		JobType:     jobType,
		Description: description,
		Priority:    priority,
		CreatedAt:   timestamppb.Now(),
		Config:      config,
		Metadata:    make(map[string]string),
	}

	h.createdJobs[jobID] = job
	return job
}

// ExecuteJob simulates job execution and returns a tracker
func (h *TestHarness) ExecuteJob(job *plugin_pb.JobRequest) *ExecutionTracker {
	h.mu.Lock()
	defer h.mu.Unlock()

	tracker := &ExecutionTracker{
		JobID:     job.JobId,
		Status:    "running",
		Messages:  make([]*plugin_pb.JobExecutionMessage, 0),
		StartTime: time.Now(),
	}

	h.executedJobs[job.JobId] = tracker
	return tracker
}

// UpdateJobProgress updates the progress of a job
func (h *TestHarness) UpdateJobProgress(jobID string, progressPercent int32, currentStep, statusMessage string) {
	h.mu.Lock()
	defer h.mu.Unlock()

	progress := &plugin_pb.JobProgress{
		ProgressPercent: progressPercent,
		CurrentStep:     currentStep,
		StatusMessage:   statusMessage,
		UpdatedAt:       timestamppb.Now(),
	}

	h.progressUpdates[jobID] = append(h.progressUpdates[jobID], progress)

	if tracker, ok := h.executedJobs[jobID]; ok {
		tracker.mu.Lock()
		tracker.ProgressPercent = progressPercent
		tracker.LastUpdate = time.Now()
		tracker.mu.Unlock()
	}
}

// VerifyProgress checks if job progress meets expectations
func (h *TestHarness) VerifyProgress(jobID string, expectedPercent int32) error {
	h.mu.RLock()
	defer h.mu.RUnlock()

	tracker, ok := h.executedJobs[jobID]
	if !ok {
		return fmt.Errorf("job not found: %s", jobID)
	}

	tracker.mu.RLock()
	actual := tracker.ProgressPercent
	tracker.mu.RUnlock()

	if actual != expectedPercent {
		return fmt.Errorf("progress mismatch for job %s: expected %d, got %d", jobID, expectedPercent, actual)
	}

	return nil
}

// CompleteJob marks a job as completed
func (h *TestHarness) CompleteJob(jobID string, summary string, output map[string]string) error {
	h.mu.Lock()
	defer h.mu.Unlock()

	tracker, ok := h.executedJobs[jobID]
	if !ok {
		return fmt.Errorf("job not found: %s", jobID)
	}

	tracker.mu.Lock()
	tracker.Status = "completed"
	tracker.ProgressPercent = 100
	tracker.LastUpdate = time.Now()
	tracker.mu.Unlock()

	completed := &plugin_pb.JobExecutionMessage{
		JobId: jobID,
		Content: &plugin_pb.JobExecutionMessage_JobCompleted{
			JobCompleted: &plugin_pb.JobCompleted{
				CompletedAt: timestamppb.Now(),
				Summary:     summary,
				Output:      output,
			},
		},
	}

	tracker.mu.Lock()
	tracker.Messages = append(tracker.Messages, completed)
	tracker.mu.Unlock()

	return nil
}

// FailJob marks a job as failed
func (h *TestHarness) FailJob(jobID, errorCode, errorMessage string, retryable bool) error {
	h.mu.Lock()
	defer h.mu.Unlock()

	tracker, ok := h.executedJobs[jobID]
	if !ok {
		return fmt.Errorf("job not found: %s", jobID)
	}

	tracker.mu.Lock()
	tracker.Status = "failed"
	tracker.LastUpdate = time.Now()
	tracker.mu.Unlock()

	failed := &plugin_pb.JobExecutionMessage{
		JobId: jobID,
		Content: &plugin_pb.JobExecutionMessage_JobFailed{
			JobFailed: &plugin_pb.JobFailed{
				ErrorCode:    errorCode,
				ErrorMessage: errorMessage,
				Retryable:    retryable,
				FailedAt:     timestamppb.Now(),
				RetryCount:   0,
			},
		},
	}

	tracker.mu.Lock()
	tracker.Messages = append(tracker.Messages, failed)
	tracker.mu.Unlock()

	return nil
}

// GetJobMessages returns all execution messages for a job
func (h *TestHarness) GetJobMessages(jobID string) []*plugin_pb.JobExecutionMessage {
	h.mu.RLock()
	defer h.mu.RUnlock()

	tracker, ok := h.executedJobs[jobID]
	if !ok {
		return nil
	}

	tracker.mu.RLock()
	defer tracker.mu.RUnlock()
	return tracker.Messages
}

// GetJobStatus returns the current status of a job
func (h *TestHarness) GetJobStatus(jobID string) (string, error) {
	h.mu.RLock()
	defer h.mu.RUnlock()

	tracker, ok := h.executedJobs[jobID]
	if !ok {
		return "", fmt.Errorf("job not found: %s", jobID)
	}

	tracker.mu.RLock()
	defer tracker.mu.RUnlock()
	return tracker.Status, nil
}

// GetAdminServer returns the mock admin server
func (h *TestHarness) GetAdminServer() *MockAdminServer {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return h.adminServer
}

// WaitForJobCompletion waits for a job to complete or timeout
func (h *TestHarness) WaitForJobCompletion(jobID string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)

	for {
		if time.Now().After(deadline) {
			return fmt.Errorf("job completion timeout: %s", jobID)
		}

		status, err := h.GetJobStatus(jobID)
		if err != nil {
			return err
		}

		if status == "completed" || status == "failed" {
			return nil
		}

		time.Sleep(100 * time.Millisecond)
	}
}

// AssertJobExists verifies that a job was created
func (h *TestHarness) AssertJobExists(jobID string) error {
	h.mu.RLock()
	defer h.mu.RUnlock()

	if _, ok := h.createdJobs[jobID]; !ok {
		return fmt.Errorf("job does not exist: %s", jobID)
	}

	return nil
}

// AssertJobNotExists verifies that a job was not created
func (h *TestHarness) AssertJobNotExists(jobID string) error {
	h.mu.RLock()
	defer h.mu.RUnlock()

	if _, ok := h.createdJobs[jobID]; ok {
		return fmt.Errorf("job should not exist: %s", jobID)
	}

	return nil
}

// AssertJobStatus verifies that a job has the expected status
func (h *TestHarness) AssertJobStatus(jobID, expectedStatus string) error {
	status, err := h.GetJobStatus(jobID)
	if err != nil {
		return err
	}

	if status != expectedStatus {
		return fmt.Errorf("job status mismatch for %s: expected %s, got %s", jobID, expectedStatus, status)
	}

	return nil
}

// CreatePluginRegister creates a test plugin registration message
func CreatePluginRegister(pluginID, name, version string, capabilities []*plugin_pb.JobTypeCapability) *plugin_pb.PluginMessage {
	return &plugin_pb.PluginMessage{
		Content: &plugin_pb.PluginMessage_Register{
			Register: &plugin_pb.PluginRegister{
				PluginId:        pluginID,
				Name:            name,
				Version:         version,
				ProtocolVersion: "v1",
				Capabilities:    capabilities,
			},
		},
	}
}

// CreatePluginHeartbeat creates a test plugin heartbeat message
func CreatePluginHeartbeat(pluginID string, pendingJobs, runningJobs int32, cpuUsage, memoryUsage float32) *plugin_pb.PluginMessage {
	return &plugin_pb.PluginMessage{
		Content: &plugin_pb.PluginMessage_Heartbeat{
			Heartbeat: &plugin_pb.PluginHeartbeat{
				PluginId:        pluginID,
				Timestamp:       timestamppb.Now(),
				UptimeSeconds:   3600,
				PendingJobs:     pendingJobs,
				CpuUsagePercent: cpuUsage,
				MemoryUsageMb:   memoryUsage,
			},
		},
	}
}

// CreateJobStartedMessage creates a job started message
func CreateJobStartedMessage(jobID, executorID string) *plugin_pb.JobExecutionMessage {
	return &plugin_pb.JobExecutionMessage{
		JobId: jobID,
		Content: &plugin_pb.JobExecutionMessage_JobStarted{
			JobStarted: &plugin_pb.JobStarted{
				StartedAt:  timestamppb.Now(),
				ExecutorId: executorID,
			},
		},
	}
}

// CreateJobProgressMessage creates a job progress message
func CreateJobProgressMessage(jobID string, progressPercent int32, currentStep, statusMessage string) *plugin_pb.JobExecutionMessage {
	return &plugin_pb.JobExecutionMessage{
		JobId: jobID,
		Content: &plugin_pb.JobExecutionMessage_Progress{
			Progress: &plugin_pb.JobProgress{
				ProgressPercent: progressPercent,
				CurrentStep:     currentStep,
				StatusMessage:   statusMessage,
				UpdatedAt:       timestamppb.Now(),
			},
		},
	}
}

// CreateJobCompletedMessage creates a job completed message
func CreateJobCompletedMessage(jobID, summary string, output map[string]string) *plugin_pb.JobExecutionMessage {
	return &plugin_pb.JobExecutionMessage{
		JobId: jobID,
		Content: &plugin_pb.JobExecutionMessage_JobCompleted{
			JobCompleted: &plugin_pb.JobCompleted{
				CompletedAt: timestamppb.Now(),
				Summary:     summary,
				Output:      output,
			},
		},
	}
}

// CreateJobFailedMessage creates a job failed message
func CreateJobFailedMessage(jobID, errorCode, errorMessage string, retryable bool) *plugin_pb.JobExecutionMessage {
	return &plugin_pb.JobExecutionMessage{
		JobId: jobID,
		Content: &plugin_pb.JobExecutionMessage_JobFailed{
			JobFailed: &plugin_pb.JobFailed{
				ErrorCode:    errorCode,
				ErrorMessage: errorMessage,
				Retryable:    retryable,
				FailedAt:     timestamppb.Now(),
				RetryCount:   0,
			},
		},
	}
}
