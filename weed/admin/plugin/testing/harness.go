package testing

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb/plugin_pb"
)

// TestHarness provides a complete testing environment for plugins
type TestHarness struct {
	mu                sync.RWMutex
	adminService      *MockPluginService
	plugins           map[string]*MockPlugin
	registrations     map[string]*RegistrationRecord
	jobs              map[string]*JobTracker
	detections        map[string][]*DetectionRecord
	executions        map[string]*ExecutionRecord
	startTime         time.Time
	timeout           time.Duration
	testName          string
	registrationWait  time.Duration
	executionWait     time.Duration
	expectedPlugins   int
	registeredPlugins int
	failureReasons    []string
}

// RegistrationRecord tracks plugin registration details
type RegistrationRecord struct {
	PluginID           string
	RegisteredAt       time.Time
	Version            string
	Capabilities      []string
	MaxConcurrentJobs int
	Status             string
}

// JobTracker tracks job lifecycle
type JobTracker struct {
	JobID        string
	Type         string
	PluginID     string
	Status       plugin_pb.ExecutionStatus
	CreatedAt    time.Time
	StartedAt    *time.Time
	CompletedAt  *time.Time
	Result       *plugin_pb.JobResult
	ErrorMessage string
	Detections   []*DetectionRecord
}

// DetectionRecord represents a detection result
type DetectionRecord struct {
	ResourceID    string
	DetectionType string
	Severity      string
	Description   string
	Data          []byte
}

// ExecutionRecord tracks execution details
type ExecutionRecord struct {
	ResourceID   string
	Type         string
	ExecutedAt   time.Time
	CompletedAt  *time.Time
	Success      bool
	ErrorMessage string
	Data         []byte
}

// NewTestHarness creates a new test harness
func NewTestHarness(testName string) *TestHarness {
	return &TestHarness{
		testName:         testName,
		adminService:     NewMockPluginService(),
		plugins:          make(map[string]*MockPlugin),
		registrations:    make(map[string]*RegistrationRecord),
		jobs:             make(map[string]*JobTracker),
		detections:       make(map[string][]*DetectionRecord),
		executions:       make(map[string]*ExecutionRecord),
		startTime:        time.Now(),
		timeout:          10 * time.Second,
		registrationWait: 100 * time.Millisecond,
		executionWait:    100 * time.Millisecond,
		failureReasons:   make([]string, 0),
	}
}

// SetTimeout sets the overall test timeout
func (h *TestHarness) SetTimeout(timeout time.Duration) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.timeout = timeout
}

// SetRegistrationWait sets the wait time for plugin registration
func (h *TestHarness) SetRegistrationWait(duration time.Duration) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.registrationWait = duration
}

// SetExecutionWait sets the wait time for job execution
func (h *TestHarness) SetExecutionWait(duration time.Duration) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.executionWait = duration
}

// RegisterPlugin simulates plugin registration
func (h *TestHarness) RegisterPlugin(plugin *MockPlugin) error {
	h.mu.Lock()

	if plugin == nil {
		h.failureReasons = append(h.failureReasons, "plugin is nil")
		h.mu.Unlock()
		return fmt.Errorf("plugin is nil")
	}

	h.plugins[plugin.ID] = plugin
	h.mu.Unlock()

	// Simulate registration with admin service
	req := &plugin_pb.PluginConnectRequest{
		PluginId:           plugin.ID,
		PluginName:         plugin.Name,
		Version:            plugin.Version,
		Capabilities:       plugin.Capabilities,
		CapabilitiesDetail: plugin.CapabilitiesDetail,
		MaxConcurrentJobs:  int32(plugin.MaxConcurrentJobs),
	}

	ctx, cancel := context.WithTimeout(context.Background(), h.timeout)
	defer cancel()

	resp, err := h.adminService.Connect(ctx, req)
	if err != nil {
		h.mu.Lock()
		h.failureReasons = append(h.failureReasons, fmt.Sprintf("registration failed: %v", err))
		h.mu.Unlock()
		return err
	}

	if !resp.Success {
		h.mu.Lock()
		h.failureReasons = append(h.failureReasons, "registration response was not successful")
		h.mu.Unlock()
		return fmt.Errorf("registration failed: %s", resp.Message)
	}

	h.mu.Lock()
	h.registrations[plugin.ID] = &RegistrationRecord{
		PluginID:           plugin.ID,
		RegisteredAt:       time.Now(),
		Version:            plugin.Version,
		Capabilities:       plugin.Capabilities,
		MaxConcurrentJobs:  plugin.MaxConcurrentJobs,
		Status:             "registered",
	}
	h.registeredPlugins++
	h.mu.Unlock()

	return nil
}

// RegisterMultiplePlugins registers multiple plugins
func (h *TestHarness) RegisterMultiplePlugins(plugins ...*MockPlugin) error {
	for _, plugin := range plugins {
		if err := h.RegisterPlugin(plugin); err != nil {
			return err
		}
	}
	return nil
}

// ExpectPlugins sets the expected number of plugins
func (h *TestHarness) ExpectPlugins(count int) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.expectedPlugins = count
}

// DispatchJob sends a job to a plugin
func (h *TestHarness) DispatchJob(pluginID string, jobType string, payload *plugin_pb.JobPayload) (string, error) {
	h.mu.RLock()
	plugin, ok := h.plugins[pluginID]
	h.mu.RUnlock()

	if !ok {
		return "", fmt.Errorf("plugin not found: %s", pluginID)
	}

	jobID := fmt.Sprintf("job-%d-%d", len(h.jobs), time.Now().UnixNano())

	req := &plugin_pb.ExecuteJobRequest{
		JobId:   jobID,
		JobType: jobType,
		Payload: payload,
	}

	ctx, cancel := context.WithTimeout(context.Background(), h.timeout)
	defer cancel()

	// Simulate job dispatch
	err := h.adminService.SimulateJobExecution(req)
	if err != nil {
		h.mu.Lock()
		h.failureReasons = append(h.failureReasons, fmt.Sprintf("job dispatch failed: %v", err))
		h.mu.Unlock()
		return "", err
	}

	// Wait for job to complete
	time.Sleep(h.executionWait)

	// Verify job execution
	plugin.TrackJob(req)

	_, executionErr := plugin.ExecuteJob(ctx, jobID, jobType, payload)

	h.mu.Lock()
	h.jobs[jobID] = &JobTracker{
		JobID:      jobID,
		Type:       jobType,
		PluginID:   pluginID,
		Status:     plugin_pb.ExecutionStatus_EXECUTION_STATUS_RUNNING,
		CreatedAt:  time.Now(),
	}
	h.mu.Unlock()

	// Simulate completion after a small delay
	time.Sleep(h.executionWait)

	h.mu.Lock()
	h.jobs[jobID].Status = plugin_pb.ExecutionStatus_EXECUTION_STATUS_COMPLETED
	now := time.Now()
	h.jobs[jobID].CompletedAt = &now
	h.mu.Unlock()

	if executionErr != nil {
		h.mu.Lock()
		h.jobs[jobID].Status = plugin_pb.ExecutionStatus_EXECUTION_STATUS_FAILED
		h.jobs[jobID].ErrorMessage = executionErr.Error()
		h.mu.Unlock()
	}

	return jobID, nil
}

// VerifyRegistration checks if a plugin was registered
func (h *TestHarness) VerifyRegistration(pluginID string) bool {
	h.mu.RLock()
	defer h.mu.RUnlock()
	_, ok := h.registrations[pluginID]
	return ok
}

// VerifyJobCompleted checks if a job completed successfully
func (h *TestHarness) VerifyJobCompleted(jobID string) bool {
	h.mu.RLock()
	defer h.mu.RUnlock()

	job, ok := h.jobs[jobID]
	if !ok {
		return false
	}

	return job.Status == plugin_pb.ExecutionStatus_EXECUTION_STATUS_COMPLETED
}

// VerifyJobFailed checks if a job failed
func (h *TestHarness) VerifyJobFailed(jobID string) bool {
	h.mu.RLock()
	defer h.mu.RUnlock()

	job, ok := h.jobs[jobID]
	if !ok {
		return false
	}

	return job.Status == plugin_pb.ExecutionStatus_EXECUTION_STATUS_FAILED
}

// VerifyPluginCapability checks if a plugin has a capability
func (h *TestHarness) VerifyPluginCapability(pluginID string, capability string) bool {
	h.mu.RLock()
	defer h.mu.RUnlock()

	reg, ok := h.registrations[pluginID]
	if !ok {
		return false
	}

	for _, cap := range reg.Capabilities {
		if cap == capability {
			return true
		}
	}

	return false
}

// GetJobStatus returns the status of a job
func (h *TestHarness) GetJobStatus(jobID string) plugin_pb.ExecutionStatus {
	h.mu.RLock()
	defer h.mu.RUnlock()

	job, ok := h.jobs[jobID]
	if !ok {
		return plugin_pb.ExecutionStatus_EXECUTION_STATUS_UNKNOWN
	}

	return job.Status
}

// GetPlugin returns a registered plugin
func (h *TestHarness) GetPlugin(pluginID string) *MockPlugin {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return h.plugins[pluginID]
}

// GetRegistrationCount returns the number of registered plugins
func (h *TestHarness) GetRegistrationCount() int {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return h.registeredPlugins
}

// GetJobCount returns the total number of jobs dispatched
func (h *TestHarness) GetJobCount() int {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return len(h.jobs)
}

// SimulateDetection simulates detection results
func (h *TestHarness) SimulateDetection(pluginID string, result *DetectionRecord) error {
	h.mu.RLock()
	plugin, ok := h.plugins[pluginID]
	h.mu.RUnlock()

	if !ok {
		return fmt.Errorf("plugin not found: %s", pluginID)
	}

	plugin.AddDetectionResult(result.ResourceID, result.DetectionType, result.Severity, result.Description, result.Data)

	h.mu.Lock()
	if _, exists := h.detections[pluginID]; !exists {
		h.detections[pluginID] = make([]*DetectionRecord, 0)
	}
	h.detections[pluginID] = append(h.detections[pluginID], result)
	h.mu.Unlock()

	return nil
}

// GetAdminService returns the underlying admin service
func (h *TestHarness) GetAdminService() *MockPluginService {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return h.adminService
}

// GetTestDuration returns the elapsed test time
func (h *TestHarness) GetTestDuration() time.Duration {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return time.Since(h.startTime)
}

// ReportFailure records a test failure reason
func (h *TestHarness) ReportFailure(reason string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.failureReasons = append(h.failureReasons, reason)
}

// HasFailures checks if any failures were recorded
func (h *TestHarness) HasFailures() bool {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return len(h.failureReasons) > 0
}

// GetFailures returns all recorded failures
func (h *TestHarness) GetFailures() []string {
	h.mu.RLock()
	defer h.mu.RUnlock()
	failures := make([]string, len(h.failureReasons))
	copy(failures, h.failureReasons)
	return failures
}

// WaitForRegistration waits for a specific number of plugins to register
func (h *TestHarness) WaitForRegistration(count int, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for {
		h.mu.RLock()
		current := h.registeredPlugins
		h.mu.RUnlock()

		if current >= count {
			return true
		}

		if time.Now().After(deadline) {
			return false
		}

		time.Sleep(10 * time.Millisecond)
	}
}

// VerifyAdminServiceStats checks admin service statistics
func (h *TestHarness) VerifyAdminServiceStats(regCount, jobCount int) bool {
	return h.adminService.GetRegistrationCount() == regCount &&
		h.adminService.GetJobDispatchCount() == jobCount
}

// GetCompletedJobCount returns the number of completed jobs
func (h *TestHarness) GetCompletedJobCount() int {
	h.mu.RLock()
	defer h.mu.RUnlock()

	count := 0
	for _, job := range h.jobs {
		if job.Status == plugin_pb.ExecutionStatus_EXECUTION_STATUS_COMPLETED {
			count++
		}
	}
	return count
}

// GetFailedJobCount returns the number of failed jobs
func (h *TestHarness) GetFailedJobCount() int {
	h.mu.RLock()
	defer h.mu.RUnlock()

	count := 0
	for _, job := range h.jobs {
		if job.Status == plugin_pb.ExecutionStatus_EXECUTION_STATUS_FAILED {
			count++
		}
	}
	return count
}

// Cleanup performs cleanup after a test
func (h *TestHarness) Cleanup() {
	h.mu.Lock()
	defer h.mu.Unlock()

	// Reset all plugins
	for _, plugin := range h.plugins {
		plugin.Reset()
	}

	// Reset admin service
	h.adminService.ResetCounters()

	// Clear tracking
	h.registrations = make(map[string]*RegistrationRecord)
	h.jobs = make(map[string]*JobTracker)
	h.detections = make(map[string][]*DetectionRecord)
	h.executions = make(map[string]*ExecutionRecord)
	h.failureReasons = make([]string, 0)
	h.registeredPlugins = 0
	h.startTime = time.Now()
}

// MockExecuteJobStream is a mock implementation of the ExecuteJob stream
type MockExecuteJobStream struct {
	responses []*plugin_pb.ExecuteJobResponse
	mu        sync.Mutex
}

// Send sends a response on the stream
func (m *MockExecuteJobStream) Send(resp *plugin_pb.ExecuteJobResponse) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.responses = append(m.responses, resp)
	return nil
}

// Recv receives a response from the stream
func (m *MockExecuteJobStream) Recv() (*plugin_pb.ExecuteJobResponse, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if len(m.responses) == 0 {
		return nil, fmt.Errorf("no responses")
	}
	resp := m.responses[0]
	m.responses = m.responses[1:]
	return resp, nil
}

// SetHeader sets the metadata header
func (m *MockExecuteJobStream) SetHeader(map[string][]string) error {
	return nil
}

// SendHeader sends the metadata header
func (m *MockExecuteJobStream) SendHeader(map[string][]string) error {
	return nil
}

// SetTrailer sets the metadata trailer
func (m *MockExecuteJobStream) SetTrailer(map[string][]string) {
}

// Context returns the context
func (m *MockExecuteJobStream) Context() context.Context {
	return context.Background()
}

// SendMsg sends a message on the stream
func (m *MockExecuteJobStream) SendMsg(interface{}) error {
	return nil
}

// RecvMsg receives a message from the stream
func (m *MockExecuteJobStream) RecvMsg(interface{}) error {
	return nil
}
