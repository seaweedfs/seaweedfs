package testing

import (
	"context"
	"sync"
	"time"

	"google.golang.org/protobuf/types/known/durationpb"

	"github.com/seaweedfs/seaweedfs/weed/pb/plugin_pb"
)

// MockPluginService simulates the admin-side PluginService for testing
type MockPluginService struct {
	mu                   sync.RWMutex
	plugins              map[string]*MockPluginInstance
	jobs                 map[string]*MockJob
	jobCounter           int
	heartbeats           map[string]*plugin_pb.HealthReport
	lastHeartbeatTime    map[string]time.Time
	jobDispatchCalls     int
	registrationCalls    int
	receivedHealthReports []plugin_pb.HealthReport
}

// MockPluginInstance tracks a registered plugin
type MockPluginInstance struct {
	ID                    string
	Name                  string
	Version               string
	Status                string
	Capabilities         []string
	MaxConcurrentJobs    int
	ConnectedAt          time.Time
	LastHeartbeat        time.Time
	ActiveJobCount       int
	CompletedJobCount    int
	FailedJobCount       int
	CapabilitiesDetail   *plugin_pb.PluginCapabilities
	Metadata             map[string]string
}

// MockJob represents a job dispatched to a plugin
type MockJob struct {
	ID            string
	Type          string
	PluginID      string
	Payload       *plugin_pb.JobPayload
	Timeout       time.Duration
	RetryCount    int
	Context       map[string]string
	Status        plugin_pb.ExecutionStatus
	DispatchedAt  time.Time
	ExecutedAt    *time.Time
	Result        *plugin_pb.JobResult
	ResultMessage string
	StreamCalls   int
}

// NewMockPluginService creates a new mock admin service
func NewMockPluginService() *MockPluginService {
	return &MockPluginService{
		plugins:               make(map[string]*MockPluginInstance),
		jobs:                  make(map[string]*MockJob),
		heartbeats:            make(map[string]*plugin_pb.HealthReport),
		lastHeartbeatTime:     make(map[string]time.Time),
		receivedHealthReports: make([]plugin_pb.HealthReport, 0),
	}
}

// Connect handles plugin registration
func (m *MockPluginService) Connect(ctx context.Context, req *plugin_pb.PluginConnectRequest) (*plugin_pb.PluginConnectResponse, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.registrationCalls++

	// Register the plugin
	plugin := &MockPluginInstance{
		ID:                  req.PluginId,
		Name:                req.PluginName,
		Version:             req.Version,
		Status:              "connected",
		Capabilities:        req.Capabilities,
		MaxConcurrentJobs:   int(req.MaxConcurrentJobs),
		ConnectedAt:         time.Now(),
		LastHeartbeat:       time.Now(),
		CapabilitiesDetail:  req.CapabilitiesDetail,
		Metadata:            req.Metadata,
	}
	m.plugins[req.PluginId] = plugin
	m.lastHeartbeatTime[req.PluginId] = time.Now()

	// Build response with assigned types
	assignedTypes := req.Capabilities

	config := &plugin_pb.PluginConfig{
		PluginId:   req.PluginId,
		Properties: make(map[string]string),
		JobTypes:   make([]*plugin_pb.JobTypeConfig, 0),
	}

	return &plugin_pb.PluginConnectResponse{
		Success:      true,
		Message:      "Plugin registered successfully",
		MasterId:     "mock-master-001",
		AssignedTypes: assignedTypes,
		Config:       config,
	}, nil
}

// SimulateJobExecution simulates job execution
func (m *MockPluginService) SimulateJobExecution(req *plugin_pb.ExecuteJobRequest) error {
	m.mu.Lock()

	m.jobDispatchCalls++

	// Create job entry
	job := &MockJob{
		ID:           req.JobId,
		Type:         req.JobType,
		Payload:      req.Payload,
		Timeout:      durationFromProto(req.Timeout),
		RetryCount:   int(req.RetryCount),
		Context:      req.Context,
		DispatchedAt: time.Now(),
		Status:       plugin_pb.ExecutionStatus_EXECUTION_STATUS_ACCEPTED,
		StreamCalls:  0,
	}
	m.jobs[req.JobId] = job
	m.mu.Unlock()

	// Simulate job execution
	time.Sleep(50 * time.Millisecond)

	// Update job status
	m.mu.Lock()
	job.StreamCalls++
	job.Status = plugin_pb.ExecutionStatus_EXECUTION_STATUS_RUNNING
	m.mu.Unlock()

	// Simulate processing
	time.Sleep(50 * time.Millisecond)

	m.mu.Lock()
	job.StreamCalls++
	job.Status = plugin_pb.ExecutionStatus_EXECUTION_STATUS_COMPLETED
	job.ResultMessage = "Job completed successfully"
	now := time.Now()
	job.ExecutedAt = &now
	m.mu.Unlock()

	return nil
}

// ExecuteJob simulates job dispatch
func (m *MockPluginService) ExecuteJob(ctx context.Context, req *plugin_pb.ExecuteJobRequest) (*plugin_pb.ExecuteJobResponse, error) {
	m.mu.Lock()
	m.jobDispatchCalls++
	m.mu.Unlock()

	return &plugin_pb.ExecuteJobResponse{
		JobId:   req.JobId,
		Status:  plugin_pb.ExecutionStatus_EXECUTION_STATUS_ACCEPTED,
		Message: "Job accepted",
	}, nil
}

// ReportHealth handles plugin health reports
func (m *MockPluginService) ReportHealth(ctx context.Context, report *plugin_pb.HealthReport) (*plugin_pb.HealthReportResponse, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.heartbeats[report.PluginId] = report
	m.lastHeartbeatTime[report.PluginId] = time.Now()
	m.receivedHealthReports = append(m.receivedHealthReports, *report)

	// Update plugin status
	if plugin, ok := m.plugins[report.PluginId]; ok {
		plugin.LastHeartbeat = time.Now()
		plugin.ActiveJobCount = int(report.ActiveJobs)
	}

	return &plugin_pb.HealthReportResponse{
		Acknowledged: true,
		Feedback:     "Health report received",
	}, nil
}

// GetConfig handles config retrieval
func (m *MockPluginService) GetConfig(ctx context.Context, req *plugin_pb.GetConfigRequest) (*plugin_pb.GetConfigResponse, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	config := &plugin_pb.PluginConfig{
		PluginId:   req.PluginId,
		Properties: make(map[string]string),
		JobTypes:   make([]*plugin_pb.JobTypeConfig, 0),
	}

	return &plugin_pb.GetConfigResponse{
		Config:  config,
		Version: 1,
	}, nil
}

// SubmitResult handles job result submission
func (m *MockPluginService) SubmitResult(ctx context.Context, req *plugin_pb.JobResultRequest) (*plugin_pb.JobResultResponse, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if job, ok := m.jobs[req.JobId]; ok {
		job.Status = req.Status
		job.Result = req.Result
		job.ResultMessage = req.Message
	}

	return &plugin_pb.JobResultResponse{
		Acknowledged:   true,
		ActionsToTake: []string{},
	}, nil
}

// GetRegistrationCount returns how many times Connect was called
func (m *MockPluginService) GetRegistrationCount() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.registrationCalls
}

// GetJobDispatchCount returns how many times ExecuteJob was called
func (m *MockPluginService) GetJobDispatchCount() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.jobDispatchCalls
}

// GetPluginCount returns the number of registered plugins
func (m *MockPluginService) GetPluginCount() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.plugins)
}

// GetPlugin returns a registered plugin by ID
func (m *MockPluginService) GetPlugin(pluginID string) *MockPluginInstance {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.plugins[pluginID]
}

// GetJob returns a dispatched job by ID
func (m *MockPluginService) GetJob(jobID string) *MockJob {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.jobs[jobID]
}

// GetJobCount returns the total number of dispatched jobs
func (m *MockPluginService) GetJobCount() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.jobs)
}

// GetLastHeartbeat returns the last heartbeat time for a plugin
func (m *MockPluginService) GetLastHeartbeat(pluginID string) *time.Time {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if t, ok := m.lastHeartbeatTime[pluginID]; ok {
		return &t
	}
	return nil
}

// GetHeartbeatCount returns how many heartbeats have been received
func (m *MockPluginService) GetHeartbeatCount() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.receivedHealthReports)
}

// ResetCounters resets all counters for a fresh test
func (m *MockPluginService) ResetCounters() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.registrationCalls = 0
	m.jobDispatchCalls = 0
	m.plugins = make(map[string]*MockPluginInstance)
	m.jobs = make(map[string]*MockJob)
	m.heartbeats = make(map[string]*plugin_pb.HealthReport)
	m.lastHeartbeatTime = make(map[string]time.Time)
	m.receivedHealthReports = make([]plugin_pb.HealthReport, 0)
}

// VerifyJobCompleted checks if a job was completed successfully
func (m *MockPluginService) VerifyJobCompleted(jobID string) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	job, ok := m.jobs[jobID]
	if !ok {
		return false
	}
	return job.Status == plugin_pb.ExecutionStatus_EXECUTION_STATUS_COMPLETED
}

// VerifyJobFailed checks if a job failed
func (m *MockPluginService) VerifyJobFailed(jobID string) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	job, ok := m.jobs[jobID]
	if !ok {
		return false
	}
	return job.Status == plugin_pb.ExecutionStatus_EXECUTION_STATUS_FAILED
}

// GetJobStatus returns the current status of a job
func (m *MockPluginService) GetJobStatus(jobID string) plugin_pb.ExecutionStatus {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if job, ok := m.jobs[jobID]; ok {
		return job.Status
	}
	return plugin_pb.ExecutionStatus_EXECUTION_STATUS_UNKNOWN
}

// VerifyPluginRegistered checks if a plugin is registered
func (m *MockPluginService) VerifyPluginRegistered(pluginID string) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	_, ok := m.plugins[pluginID]
	return ok
}

// durationFromProto converts proto Duration to time.Duration
func durationFromProto(d *durationpb.Duration) time.Duration {
	if d == nil {
		return 0
	}
	return time.Duration(d.Seconds)*time.Second + time.Duration(d.Nanos)
}
