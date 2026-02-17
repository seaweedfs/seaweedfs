package testing

import (
	"fmt"
	"sync"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb/plugin_pb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// MockPlugin simulates a plugin worker for testing
type MockPlugin struct {
	ID              string
	Name            string
	Version         string
	ProtocolVersion string
	Capabilities    []*plugin_pb.JobTypeCapability

	// Configuration
	DetectionEnabled bool
	ExecutionEnabled bool
	FailureMode      string // "" = success, "detection_error", "execution_error"
	DetectionDelay   time.Duration
	ExecutionDelay   time.Duration

	// Tracking
	mu               sync.RWMutex
	detectedJobs     map[string]*plugin_pb.DetectedJob
	executedJobs     map[string]*JobExecution
	registrationTime time.Time
	callCount        map[string]int
	errors           []string
}

// JobExecution tracks execution of a job
type JobExecution struct {
	JobID           string
	Config          []*plugin_pb.ConfigFieldValue
	Status          string
	ProgressPercent int32
	Messages        []*plugin_pb.JobExecutionMessage
	StartTime       time.Time
	EndTime         time.Time
	ErrorInfo       *plugin_pb.JobFailed
	mu              sync.RWMutex
}

// NewMockPlugin creates a new mock plugin
func NewMockPlugin(id, name, version string) *MockPlugin {
	return &MockPlugin{
		ID:               id,
		Name:             name,
		Version:          version,
		ProtocolVersion:  "v1",
		Capabilities:     make([]*plugin_pb.JobTypeCapability, 0),
		DetectionEnabled: true,
		ExecutionEnabled: true,
		DetectionDelay:   100 * time.Millisecond,
		ExecutionDelay:   100 * time.Millisecond,
		detectedJobs:     make(map[string]*plugin_pb.DetectedJob),
		executedJobs:     make(map[string]*JobExecution),
		registrationTime: time.Now(),
		callCount:        make(map[string]int),
		errors:           make([]string, 0),
	}
}

// AddCapability adds a job type capability to the plugin
func (mp *MockPlugin) AddCapability(jobType string, canDetect, canExecute bool) {
	mp.mu.Lock()
	defer mp.mu.Unlock()

	capability := &plugin_pb.JobTypeCapability{
		JobType:    jobType,
		CanDetect:  canDetect,
		CanExecute: canExecute,
		Version:    "v1",
	}
	mp.Capabilities = append(mp.Capabilities, capability)
}

// GetRegistrationMessage returns the plugin registration message
func (mp *MockPlugin) GetRegistrationMessage() *plugin_pb.PluginMessage {
	mp.mu.RLock()
	defer mp.mu.RUnlock()

	return &plugin_pb.PluginMessage{
		Content: &plugin_pb.PluginMessage_Register{
			Register: &plugin_pb.PluginRegister{
				PluginId:        mp.ID,
				Name:            mp.Name,
				Version:         mp.Version,
				ProtocolVersion: mp.ProtocolVersion,
				Capabilities:    mp.Capabilities,
			},
		},
	}
}

// GetHeartbeatMessage returns a heartbeat message
func (mp *MockPlugin) GetHeartbeatMessage(pendingJobs, runningJobs int32, cpuUsage, memoryUsage float32) *plugin_pb.PluginMessage {
	mp.mu.RLock()
	defer mp.mu.RUnlock()

	uptime := int64(time.Since(mp.registrationTime).Seconds())

	return &plugin_pb.PluginMessage{
		Content: &plugin_pb.PluginMessage_Heartbeat{
			Heartbeat: &plugin_pb.PluginHeartbeat{
				PluginId:        mp.ID,
				Timestamp:       timestamppb.Now(),
				UptimeSeconds:   uptime,
				PendingJobs:     pendingJobs,
				CpuUsagePercent: cpuUsage,
				MemoryUsageMb:   memoryUsage,
			},
		},
	}
}

// SimulateDetection simulates job detection
func (mp *MockPlugin) SimulateDetection(jobType string, detectedCount int) ([]*plugin_pb.DetectedJob, error) {
	mp.mu.Lock()
	defer mp.mu.Unlock()

	mp.callCount["detection"]++

	if !mp.DetectionEnabled {
		err := fmt.Errorf("detection disabled for plugin %s", mp.ID)
		mp.errors = append(mp.errors, err.Error())
		return nil, err
	}

	if mp.FailureMode == "detection_error" {
		err := fmt.Errorf("simulated detection error")
		mp.errors = append(mp.errors, err.Error())
		return nil, err
	}

	time.Sleep(mp.DetectionDelay)

	var jobs []*plugin_pb.DetectedJob
	now := time.Now()

	for i := 0; i < detectedCount; i++ {
		jobKey := fmt.Sprintf("detected-job-%s-%d-%d", jobType, now.Unix(), i)
		job := &plugin_pb.DetectedJob{
			JobKey:      jobKey,
			JobType:     jobType,
			Description: fmt.Sprintf("Detected %s job %d", jobType, i),
			Priority:    int64(10 - i),
			Metadata:    make(map[string]string),
		}

		jobKey2 := fmt.Sprintf("%s-%d", jobType, i)
		mp.detectedJobs[jobKey2] = job
		jobs = append(jobs, job)
	}

	return jobs, nil
}

// SimulateExecution simulates job execution
func (mp *MockPlugin) SimulateExecution(jobID, jobType string, config []*plugin_pb.ConfigFieldValue) (*JobExecution, error) {
	mp.mu.Lock()

	mp.callCount["execution"]++

	if !mp.ExecutionEnabled {
		err := fmt.Errorf("execution disabled for plugin %s", mp.ID)
		mp.errors = append(mp.errors, err.Error())
		mp.mu.Unlock()
		return nil, err
	}

	if mp.FailureMode == "execution_error" {
		err := fmt.Errorf("simulated execution error")
		mp.errors = append(mp.errors, err.Error())
		mp.mu.Unlock()
		return nil, err
	}

	execution := &JobExecution{
		JobID:     jobID,
		Config:    config,
		Status:    "running",
		Messages:  make([]*plugin_pb.JobExecutionMessage, 0),
		StartTime: time.Now(),
	}

	mp.executedJobs[jobID] = execution
	mp.mu.Unlock()

	// Simulate progress updates
	for progress := 0; progress <= 100; progress += 25 {
		time.Sleep(mp.ExecutionDelay)

		mp.mu.Lock()
		if exec, ok := mp.executedJobs[jobID]; ok {
			exec.mu.Lock()
			exec.ProgressPercent = int32(progress)

			msg := &plugin_pb.JobExecutionMessage{
				JobId: jobID,
				Content: &plugin_pb.JobExecutionMessage_Progress{
					Progress: &plugin_pb.JobProgress{
						ProgressPercent: int32(progress),
						CurrentStep:     fmt.Sprintf("Step %d", progress/25),
						StatusMessage:   fmt.Sprintf("Executing step at %d%%", progress),
						UpdatedAt:       timestamppb.Now(),
					},
				},
			}
			exec.Messages = append(exec.Messages, msg)
			exec.mu.Unlock()
		}
		mp.mu.Unlock()
	}

	mp.mu.Lock()
	if exec, ok := mp.executedJobs[jobID]; ok {
		exec.mu.Lock()
		exec.Status = "completed"
		exec.ProgressPercent = 100
		exec.EndTime = time.Now()

		completed := &plugin_pb.JobExecutionMessage{
			JobId: jobID,
			Content: &plugin_pb.JobExecutionMessage_JobCompleted{
				JobCompleted: &plugin_pb.JobCompleted{
					CompletedAt: timestamppb.Now(),
					Summary:     fmt.Sprintf("Job %s completed successfully", jobID),
					Output: map[string]string{
						"result": "success",
						"job_id": jobID,
					},
				},
			},
		}
		exec.Messages = append(exec.Messages, completed)
		exec.mu.Unlock()
	}
	mp.mu.Unlock()

	return execution, nil
}

// SimulateExecutionFailure simulates a job execution failure
func (mp *MockPlugin) SimulateExecutionFailure(jobID string, errorCode, errorMessage string, retryable bool) (*JobExecution, error) {
	mp.mu.Lock()
	defer mp.mu.Unlock()

	execution := &JobExecution{
		JobID:     jobID,
		Status:    "failed",
		Messages:  make([]*plugin_pb.JobExecutionMessage, 0),
		StartTime: time.Now(),
		EndTime:   time.Now(),
	}

	failedMsg := &plugin_pb.JobFailed{
		ErrorCode:    errorCode,
		ErrorMessage: errorMessage,
		Retryable:    retryable,
		FailedAt:     timestamppb.Now(),
		RetryCount:   0,
	}

	msg := &plugin_pb.JobExecutionMessage{
		JobId: jobID,
		Content: &plugin_pb.JobExecutionMessage_JobFailed{
			JobFailed: failedMsg,
		},
	}

	execution.Messages = append(execution.Messages, msg)
	execution.ErrorInfo = failedMsg
	mp.executedJobs[jobID] = execution

	return execution, nil
}

// GetExecutionMessages returns execution messages for a job
func (mp *MockPlugin) GetExecutionMessages(jobID string) []*plugin_pb.JobExecutionMessage {
	mp.mu.RLock()
	defer mp.mu.RUnlock()

	if exec, ok := mp.executedJobs[jobID]; ok {
		exec.mu.RLock()
		defer exec.mu.RUnlock()

		result := make([]*plugin_pb.JobExecutionMessage, len(exec.Messages))
		copy(result, exec.Messages)
		return result
	}

	return nil
}

// GetCallCount returns the number of times a method was called
func (mp *MockPlugin) GetCallCount(method string) int {
	mp.mu.RLock()
	defer mp.mu.RUnlock()

	return mp.callCount[method]
}

// GetErrors returns all recorded errors
func (mp *MockPlugin) GetErrors() []string {
	mp.mu.RLock()
	defer mp.mu.RUnlock()

	result := make([]string, len(mp.errors))
	copy(result, mp.errors)
	return result
}

// SetFailureMode sets the failure simulation mode
func (mp *MockPlugin) SetFailureMode(mode string) {
	mp.mu.Lock()
	defer mp.mu.Unlock()

	mp.FailureMode = mode
}

// SetDetectionDelay sets the detection simulation delay
func (mp *MockPlugin) SetDetectionDelay(delay time.Duration) {
	mp.mu.Lock()
	defer mp.mu.Unlock()

	mp.DetectionDelay = delay
}

// SetExecutionDelay sets the execution simulation delay
func (mp *MockPlugin) SetExecutionDelay(delay time.Duration) {
	mp.mu.Lock()
	defer mp.mu.Unlock()

	mp.ExecutionDelay = delay
}

// Reset clears all state
func (mp *MockPlugin) Reset() {
	mp.mu.Lock()
	defer mp.mu.Unlock()

	mp.detectedJobs = make(map[string]*plugin_pb.DetectedJob)
	mp.executedJobs = make(map[string]*JobExecution)
	mp.callCount = make(map[string]int)
	mp.errors = make([]string, 0)
	mp.FailureMode = ""
}
