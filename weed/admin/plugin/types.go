package plugin

import (
	"sync"
	"time"
)

// JobState represents the current state of a job in the state machine
type JobState int

const (
	JobStatePending JobState = iota
	JobStateScheduled
	JobStateRunning
	JobStateCompleted
	JobStateFailed
	JobStateCancelled
)

func (s JobState) String() string {
	switch s {
	case JobStatePending:
		return "PENDING"
	case JobStateScheduled:
		return "SCHEDULED"
	case JobStateRunning:
		return "RUNNING"
	case JobStateCompleted:
		return "COMPLETED"
	case JobStateFailed:
		return "FAILED"
	case JobStateCancelled:
		return "CANCELLED"
	default:
		return "UNKNOWN"
	}
}

// Job represents a detection or maintenance task
type Job struct {
	mu               sync.RWMutex
	ID               string
	Type             string
	PluginID         string
	State            JobState
	Payload          interface{}
	CreatedAt        time.Time
	StartedAt        *time.Time
	CompletedAt      *time.Time
	ExecutionTime    time.Duration
	RetryCount       int
	MaxRetries       int
	LastError        string
	Result           *JobResult
	DetectionRecords []DetectionRecord
}

// GetState safely retrieves the job state
func (j *Job) GetState() JobState {
	j.mu.RLock()
	defer j.mu.RUnlock()
	return j.State
}

// SetState safely updates the job state
func (j *Job) SetState(state JobState) {
	j.mu.Lock()
	defer j.mu.Unlock()
	j.State = state
	if state == JobStateRunning {
		now := time.Now()
		j.StartedAt = &now
	} else if state == JobStateCompleted || state == JobStateFailed || state == JobStateCancelled {
		now := time.Now()
		j.CompletedAt = &now
		if j.StartedAt != nil {
			j.ExecutionTime = j.CompletedAt.Sub(*j.StartedAt)
		}
	}
}

// JobResult contains the output from job execution
type JobResult struct {
	Success   bool
	Data      []byte
	Warnings  []string
	Errors    []string
	Metadata  map[string]string
	Message   string
}

// DetectionRecord represents a single detection result
type DetectionRecord struct {
	DetectionType    string
	Timestamp        time.Time
	Severity         string
	Description      string
	AffectedResource string
	RawData          []byte
}

// ExecutionRecord persists job execution history
type ExecutionRecord struct {
	JobID       string
	JobType     string
	PluginID    string
	State       JobState
	CreatedAt   time.Time
	StartedAt   *time.Time
	CompletedAt *time.Time
	Payload     interface{}
	Result      *JobResult
	RetryCount  int
	LastError   string
}

// ConnectedPlugin represents a connected plugin instance
type ConnectedPlugin struct {
	mu                    sync.RWMutex
	ID                    string
	Name                  string
	Version               string
	Status                string
	Capabilities         []string
	MaxConcurrentJobs    int
	ActiveJobs           int
	CompletedJobs        int
	FailedJobs           int
	TotalDetections      int64
	AvgExecutionTimeMs   float64
	CPUUsagePercent      float64
	MemoryUsageBytes     int64
	ConnectedAt          time.Time
	LastHeartbeat        time.Time
	Metadata             map[string]string
	HealthCheckInterval  time.Duration
	JobTimeout           time.Duration
}

// IsHealthy checks if the plugin is considered healthy based on heartbeat
func (cp *ConnectedPlugin) IsHealthy(timeout time.Duration) bool {
	cp.mu.RLock()
	defer cp.mu.RUnlock()
	return time.Since(cp.LastHeartbeat) < timeout
}

// UpdateHeartbeat updates the last heartbeat timestamp
func (cp *ConnectedPlugin) UpdateHeartbeat() {
	cp.mu.Lock()
	defer cp.mu.Unlock()
	cp.LastHeartbeat = time.Now()
}

// IncActiveJobs increments active job counter
func (cp *ConnectedPlugin) IncActiveJobs() {
	cp.mu.Lock()
	defer cp.mu.Unlock()
	cp.ActiveJobs++
}

// DecActiveJobs decrements active job counter
func (cp *ConnectedPlugin) DecActiveJobs() {
	cp.mu.Lock()
	defer cp.mu.Unlock()
	if cp.ActiveJobs > 0 {
		cp.ActiveJobs--
	}
}

// JobTypeConfig defines configuration for a specific job type
type JobTypeConfig struct {
	Type              string
	Enabled           bool
	Priority          int
	Interval          time.Duration
	MaxConcurrent     int
	Parameters        map[string]string
	RequiredDetections []string
	DetectionHistory   []DetectionRecord
	ExecutionHistory   []ExecutionRecord
}

// PluginConfig holds all configuration for a plugin
type PluginConfig struct {
	mu                  sync.RWMutex
	PluginID            string
	Properties          map[string]string
	JobTypes            map[string]*JobTypeConfig
	MaxRetries          int
	HealthCheckInterval time.Duration
	JobTimeout          time.Duration
	Environment         map[string]string
}

// GetProperty safely retrieves a configuration property
func (pc *PluginConfig) GetProperty(key string) (string, bool) {
	pc.mu.RLock()
	defer pc.mu.RUnlock()
	val, ok := pc.Properties[key]
	return val, ok
}

// SetProperty safely sets a configuration property
func (pc *PluginConfig) SetProperty(key, value string) {
	pc.mu.Lock()
	defer pc.mu.Unlock()
	if pc.Properties == nil {
		pc.Properties = make(map[string]string)
	}
	pc.Properties[key] = value
}

// GetJobTypeConfig safely retrieves job type configuration
func (pc *PluginConfig) GetJobTypeConfig(jobType string) (*JobTypeConfig, bool) {
	pc.mu.RLock()
	defer pc.mu.RUnlock()
	cfg, ok := pc.JobTypes[jobType]
	return cfg, ok
}

// SetJobTypeConfig safely sets job type configuration
func (pc *PluginConfig) SetJobTypeConfig(jobType string, cfg *JobTypeConfig) {
	pc.mu.Lock()
	defer pc.mu.Unlock()
	if pc.JobTypes == nil {
		pc.JobTypes = make(map[string]*JobTypeConfig)
	}
	pc.JobTypes[jobType] = cfg
}

// PluginHealth represents the health status of a plugin
type PluginHealth struct {
	mu              sync.RWMutex
	PluginID        string
	Status          string
	ActiveJobs      int
	CPUPercent      int64
	MemoryBytes     int64
	Timestamp       time.Time
	JobProgressList []JobProgress
}

// JobProgress tracks progress of an executing job
type JobProgress struct {
	JobID           string
	ProgressPercent float32
	CurrentStep     string
}

// DetectionCapability describes what a plugin can detect
type DetectionCapability struct {
	Type              string
	Description       string
	MinIntervalSeconds int
	RequiresFullScan  bool
	OutputMetrics     []string
}

// MaintenanceCapability describes maintenance operations a plugin can perform
type MaintenanceCapability struct {
	Type                    string
	Description             string
	RequiredDetectionTypes  []string
	EstimatedDurationSeconds int
}
