package testing

import (
	"context"
	"io"
	"sync"
	"time"

	"google.golang.org/grpc"

	"github.com/seaweedfs/seaweedfs/weed/pb/plugin_pb"
)

// MockPlugin simulates a plugin worker instance for testing
type MockPlugin struct {
	mu                      sync.RWMutex
	ID                      string
	Name                    string
	Version                 string
	Status                  string
	Capabilities            []string
	CapabilitiesDetail      *plugin_pb.PluginCapabilities
	MaxConcurrentJobs       int
	Config                  *plugin_pb.PluginConfig
	ActiveJobs              map[string]*MockJobExecution
	CompletedJobs           int
	FailedJobs              int
	ConnectStreamCalls      int
	ExecuteJobStreamCalls   int
	ReportHealthCalls       int
	GetConfigCalls          int
	SubmitResultCalls       int
	ReceivedJobs            []*plugin_pb.ExecuteJobRequest
	ReceivedHealthReports   []*plugin_pb.HealthReport
	LastError               string
	SimulateError           bool
	SimulateErrorType       string
	SchemaData              []byte
	DetectionResults        []*DetectionResult
	ExecutionResults        []*ExecutionResult
}

// MockJobExecution tracks job execution state
type MockJobExecution struct {
	JobID        string
	Type         string
	Status       plugin_pb.ExecutionStatus
	Progress     float32
	CurrentStep  string
	StartTime    time.Time
	EndTime      *time.Time
	Result       *plugin_pb.JobResult
	ErrorMessage string
}

// DetectionResult represents detection results
type DetectionResult struct {
	ResourceID   string
	DetectionType string
	Severity     string
	Description  string
	Data         []byte
}

// ExecutionResult represents execution results
type ExecutionResult struct {
	ResourceID   string
	Success      bool
	ErrorMessage string
	Data         []byte
}

// NewMockPlugin creates a new mock plugin
func NewMockPlugin(id, name, version string) *MockPlugin {
	return &MockPlugin{
		ID:                    id,
		Name:                  name,
		Version:               version,
		Status:                "ready",
		Capabilities:          make([]string, 0),
		CapabilitiesDetail:    &plugin_pb.PluginCapabilities{},
		MaxConcurrentJobs:     5,
		Config:                &plugin_pb.PluginConfig{},
		ActiveJobs:            make(map[string]*MockJobExecution),
		ReceivedJobs:          make([]*plugin_pb.ExecuteJobRequest, 0),
		ReceivedHealthReports: make([]*plugin_pb.HealthReport, 0),
		DetectionResults:      make([]*DetectionResult, 0),
		ExecutionResults:      make([]*ExecutionResult, 0),
	}
}

// AddCapability adds a capability to the plugin
func (m *MockPlugin) AddCapability(cap string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.Capabilities = append(m.Capabilities, cap)
}

// AddDetectionCapability adds a detection capability
func (m *MockPlugin) AddDetectionCapability(typ, desc string, minInterval int32, requiresFullScan bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.CapabilitiesDetail == nil {
		m.CapabilitiesDetail = &plugin_pb.PluginCapabilities{}
	}
	m.CapabilitiesDetail.Detection = append(m.CapabilitiesDetail.Detection, &plugin_pb.DetectionCapability{
		Type:               typ,
		Description:        desc,
		MinIntervalSeconds: minInterval,
		RequiresFullScan:   requiresFullScan,
	})
	m.Capabilities = append(m.Capabilities, typ)
}

// AddMaintenanceCapability adds a maintenance capability
func (m *MockPlugin) AddMaintenanceCapability(typ, desc string, requiredDetections []string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.CapabilitiesDetail == nil {
		m.CapabilitiesDetail = &plugin_pb.PluginCapabilities{}
	}
	m.CapabilitiesDetail.Maintenance = append(m.CapabilitiesDetail.Maintenance, &plugin_pb.MaintenanceCapability{
		Type:                   typ,
		Description:            desc,
		RequiredDetectionTypes: requiredDetections,
	})
}

// SetSchema sets the schema data
func (m *MockPlugin) SetSchema(data []byte) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.SchemaData = data
}

// AddDetectionResult adds a detection result
func (m *MockPlugin) AddDetectionResult(resourceID, detectionType, severity, description string, data []byte) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.DetectionResults = append(m.DetectionResults, &DetectionResult{
		ResourceID:    resourceID,
		DetectionType: detectionType,
		Severity:      severity,
		Description:   description,
		Data:          data,
	})
}

// AddExecutionResult adds an execution result
func (m *MockPlugin) AddExecutionResult(resourceID string, success bool, errorMsg string, data []byte) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.ExecutionResults = append(m.ExecutionResults, &ExecutionResult{
		ResourceID:   resourceID,
		Success:      success,
		ErrorMessage: errorMsg,
		Data:         data,
	})
}

// GetConfigurationSchema implements schema retrieval
func (m *MockPlugin) GetConfigurationSchema(ctx context.Context) ([]byte, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if m.SimulateError && m.SimulateErrorType == "schema" {
		return nil, ErrSimulatedError
	}

	return m.SchemaData, nil
}

// DetectJobs implements detection logic
func (m *MockPlugin) DetectJobs(ctx context.Context) ([]*DetectionResult, error) {
	m.mu.Lock()
	m.ReportHealthCalls++
	results := make([]*DetectionResult, len(m.DetectionResults))
	copy(results, m.DetectionResults)
	m.mu.Unlock()

	if m.SimulateError && m.SimulateErrorType == "detect" {
		return nil, ErrSimulatedError
	}

	return results, nil
}

// ExecuteJob implements job execution
func (m *MockPlugin) ExecuteJob(ctx context.Context, jobID string, jobType string, payload *plugin_pb.JobPayload) (*ExecutionResult, error) {
	m.mu.Lock()
	m.ExecuteJobStreamCalls++

	execution := &MockJobExecution{
		JobID:      jobID,
		Type:       jobType,
		Status:     plugin_pb.ExecutionStatus_EXECUTION_STATUS_RUNNING,
		StartTime:  time.Now(),
		Progress:   0,
		CurrentStep: "initialized",
	}
	m.ActiveJobs[jobID] = execution
	m.mu.Unlock()

	// Simulate execution steps
	steps := []string{"initialized", "validating", "processing", "finalizing"}

	for i, step := range steps {
		select {
		case <-ctx.Done():
			m.mu.Lock()
			execution.Status = plugin_pb.ExecutionStatus_EXECUTION_STATUS_CANCELLED
			execution.ErrorMessage = "context cancelled"
			delete(m.ActiveJobs, jobID)
			m.mu.Unlock()
			return nil, ctx.Err()
		default:
		}

		m.mu.Lock()
		execution.CurrentStep = step
		execution.Progress = float32((i + 1) * 25)
		m.mu.Unlock()

		time.Sleep(10 * time.Millisecond)
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	if m.SimulateError && m.SimulateErrorType == "execute" {
		execution.Status = plugin_pb.ExecutionStatus_EXECUTION_STATUS_FAILED
		execution.ErrorMessage = "simulated execution error"
		m.FailedJobs++
		delete(m.ActiveJobs, jobID)
		return nil, ErrSimulatedError
	}

	// Get results
	result := &ExecutionResult{
		ResourceID:   jobID,
		Success:      true,
		ErrorMessage: "",
	}

	if len(m.ExecutionResults) > 0 {
		result = m.ExecutionResults[0]
		m.ExecutionResults = m.ExecutionResults[1:]
	}

	execution.Status = plugin_pb.ExecutionStatus_EXECUTION_STATUS_COMPLETED
	execution.Progress = 100
	execution.CurrentStep = "completed"
	now := time.Now()
	execution.EndTime = &now

	m.CompletedJobs++
	delete(m.ActiveJobs, jobID)

	return result, nil
}

// ConnectStream simulates the Connect RPC stream
func (m *MockPlugin) ConnectStream(ctx context.Context, conn grpc.ClientConnInterface) error {
	m.mu.Lock()
	m.ConnectStreamCalls++
	m.mu.Unlock()

	if m.SimulateError && m.SimulateErrorType == "connect" {
		return ErrSimulatedError
	}

	return nil
}

// ExecuteJobStream simulates the ExecuteJob RPC stream
func (m *MockPlugin) ExecuteJobStream(ctx context.Context, conn grpc.ClientConnInterface, jobID string) error {
	m.mu.Lock()
	m.ExecuteJobStreamCalls++
	m.mu.Unlock()

	if m.SimulateError && m.SimulateErrorType == "executestream" {
		return ErrSimulatedError
	}

	return nil
}

// ReportHealth sends a health report
func (m *MockPlugin) ReportHealth(ctx context.Context, conn grpc.ClientConnInterface) error {
	m.mu.Lock()
	m.ReportHealthCalls++

	activeCount := len(m.ActiveJobs)
	m.mu.Unlock()

	if m.SimulateError && m.SimulateErrorType == "health" {
		return ErrSimulatedError
	}

	report := &plugin_pb.HealthReport{
		PluginId:   m.ID,
		TimestampMs: time.Now().UnixMilli(),
		Status:     plugin_pb.HealthStatus_HEALTH_STATUS_HEALTHY,
		ActiveJobs: int32(activeCount),
	}

	m.mu.Lock()
	m.ReceivedHealthReports = append(m.ReceivedHealthReports, report)
	m.mu.Unlock()

	return nil
}

// GetConfig retrieves configuration
func (m *MockPlugin) GetConfig(ctx context.Context, conn grpc.ClientConnInterface) (*plugin_pb.PluginConfig, error) {
	m.mu.Lock()
	m.GetConfigCalls++
	defer m.mu.Unlock()

	if m.SimulateError && m.SimulateErrorType == "getconfig" {
		return nil, ErrSimulatedError
	}

	return m.Config, nil
}

// SubmitResult submits job results
func (m *MockPlugin) SubmitResult(ctx context.Context, conn grpc.ClientConnInterface, jobID string, result *plugin_pb.JobResult) error {
	m.mu.Lock()
	m.SubmitResultCalls++
	defer m.mu.Unlock()

	if m.SimulateError && m.SimulateErrorType == "submitresult" {
		return ErrSimulatedError
	}

	return nil
}

// GetActiveJobCount returns the number of active jobs
func (m *MockPlugin) GetActiveJobCount() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.ActiveJobs)
}

// GetCompletedJobCount returns the number of completed jobs
func (m *MockPlugin) GetCompletedJobCount() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.CompletedJobs
}

// GetFailedJobCount returns the number of failed jobs
func (m *MockPlugin) GetFailedJobCount() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.FailedJobs
}

// GetStreamCallCount returns the count of stream calls
func (m *MockPlugin) GetStreamCallCount() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.ExecuteJobStreamCalls
}

// GetHealthReportCount returns the count of health reports sent
func (m *MockPlugin) GetHealthReportCount() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.ReportHealthCalls
}

// EnableErrorSimulation enables error simulation
func (m *MockPlugin) EnableErrorSimulation(errorType string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.SimulateError = true
	m.SimulateErrorType = errorType
}

// DisableErrorSimulation disables error simulation
func (m *MockPlugin) DisableErrorSimulation() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.SimulateError = false
	m.SimulateErrorType = ""
}

// Reset clears all counters and state
func (m *MockPlugin) Reset() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.ActiveJobs = make(map[string]*MockJobExecution)
	m.CompletedJobs = 0
	m.FailedJobs = 0
	m.ConnectStreamCalls = 0
	m.ExecuteJobStreamCalls = 0
	m.ReportHealthCalls = 0
	m.GetConfigCalls = 0
	m.SubmitResultCalls = 0
	m.ReceivedJobs = make([]*plugin_pb.ExecuteJobRequest, 0)
	m.ReceivedHealthReports = make([]*plugin_pb.HealthReport, 0)
	m.LastError = ""
	m.SimulateError = false
	m.SimulateErrorType = ""
}

// GetJobExecution returns execution details for a job
func (m *MockPlugin) GetJobExecution(jobID string) *MockJobExecution {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.ActiveJobs[jobID]
}

// TrackJob records a received job
func (m *MockPlugin) TrackJob(req *plugin_pb.ExecuteJobRequest) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.ReceivedJobs = append(m.ReceivedJobs, req)
}

// GetReceivedJobCount returns the count of received jobs
func (m *MockPlugin) GetReceivedJobCount() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.ReceivedJobs)
}

// SimulateStreamError simulates an error during streaming
func (m *MockPlugin) SimulateStreamError(reason error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.LastError = reason.Error()
}

// SetStatus sets the plugin status
func (m *MockPlugin) SetStatus(status string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.Status = status
}

// GetStatus returns the plugin status
func (m *MockPlugin) GetStatus() string {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.Status
}
