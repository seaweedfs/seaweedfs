package plugin

import (
	"sync"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb/plugin_pb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// JobState represents the current state of a job
type JobState int32

const (
	JobStatePending   JobState = 0
	JobStateRunning   JobState = 1
	JobStateCompleted JobState = 2
	JobStateFailed    JobState = 3
	JobStateCancelled JobState = 4
	JobStatePaused    JobState = 5
)

// String returns string representation of JobState
func (s JobState) String() string {
	switch s {
	case JobStatePending:
		return "pending"
	case JobStateRunning:
		return "running"
	case JobStateCompleted:
		return "completed"
	case JobStateFailed:
		return "failed"
	case JobStateCancelled:
		return "cancelled"
	case JobStatePaused:
		return "paused"
	default:
		return "unknown"
	}
}

// PluginState represents the current state of a plugin connection
type PluginState int32

const (
	PluginStateConnecting PluginState = 0
	PluginStateConnected  PluginState = 1
	PluginStateHealthy    PluginState = 2
	PluginStateUnhealthy  PluginState = 3
	PluginStateDisconnected PluginState = 4
)

// String returns string representation of PluginState
func (s PluginState) String() string {
	switch s {
	case PluginStateConnecting:
		return "connecting"
	case PluginStateConnected:
		return "connected"
	case PluginStateHealthy:
		return "healthy"
	case PluginStateUnhealthy:
		return "unhealthy"
	case PluginStateDisconnected:
		return "disconnected"
	default:
		return "unknown"
	}
}

// Job represents a work unit in the plugin system
type Job struct {
	ID              string
	Type            string
	Description     string
	Priority        int64
	State           JobState
	CreatedAt       time.Time
	UpdatedAt       time.Time
	ExecutorID      string
	ProgressPercent int32
	Config          []*plugin_pb.ConfigFieldValue
	Metadata        map[string]string

	// Execution tracking
	Retries        int32
	LastRetryTime  time.Time
	CompletedInfo  *plugin_pb.JobCompleted
	FailedInfo     *plugin_pb.JobFailed
	CheckpointIDs  []string
	LogEntries     []*plugin_pb.ExecutionLogEntry

	// Lock for thread-safe access
	mu sync.RWMutex
}

// ConnectedPlugin represents a connected plugin worker
type ConnectedPlugin struct {
	ID              string
	Name            string
	Version         string
	ProtocolVersion string
	ConnectedAt     time.Time
	LastHeartbeat   time.Time
	State           PluginState
	Healthy         bool

	// Capabilities indexed by job type
	Capabilities map[string]*plugin_pb.JobTypeCapability

	// Current workload
	PendingJobs int32
	RunningJobs int32

	// Resource usage
	CPUUsagePercent  float32
	MemoryUsageMB    float32

	// Communication stream
	StreamConnected bool

	// Lock for thread-safe access
	mu sync.RWMutex
}

// JobTypeConfig holds configuration for a specific job type
type JobTypeConfig struct {
	JobType        string
	Enabled        bool
	AdminConfig    []*plugin_pb.ConfigFieldValue
	WorkerConfig   []*plugin_pb.ConfigFieldValue
	CreatedAt      time.Time
	UpdatedAt      time.Time
	CreatedBy      string

	mu sync.RWMutex
}

// GetState safely gets the job state
func (j *Job) GetState() JobState {
	j.mu.RLock()
	defer j.mu.RUnlock()
	return j.State
}

// SetState safely sets the job state
func (j *Job) SetState(state JobState) {
	j.mu.Lock()
	defer j.mu.Unlock()
	j.State = state
	j.UpdatedAt = time.Now()
}

// GetProgress safely gets progress
func (j *Job) GetProgress() int32 {
	j.mu.RLock()
	defer j.mu.RUnlock()
	return j.ProgressPercent
}

// SetProgress safely sets progress
func (j *Job) SetProgress(percent int32) {
	j.mu.Lock()
	defer j.mu.Unlock()
	j.ProgressPercent = percent
	j.UpdatedAt = time.Now()
}

// GetState safely gets plugin state
func (p *ConnectedPlugin) GetState() PluginState {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.State
}

// SetState safely sets plugin state
func (p *ConnectedPlugin) SetState(state PluginState) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.State = state
	p.LastHeartbeat = time.Now()
}

// IsHealthy safely checks if plugin is healthy
func (p *ConnectedPlugin) IsHealthy() bool {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.Healthy
}

// UpdateHeartbeat safely updates heartbeat
func (p *ConnectedPlugin) UpdateHeartbeat(cpu, memory float32, pending, running int32) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.LastHeartbeat = time.Now()
	p.CPUUsagePercent = cpu
	p.MemoryUsageMB = memory
	p.PendingJobs = pending
	p.RunningJobs = running
}

// GetConfig safely gets configuration
func (c *JobTypeConfig) GetConfig() (*plugin_pb.JobTypeConfig, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	return &plugin_pb.JobTypeConfig{
		JobType:      c.JobType,
		Enabled:      c.Enabled,
		AdminConfig:  c.AdminConfig,
		WorkerConfig: c.WorkerConfig,
		CreatedAt:    timestamppb.New(c.CreatedAt),
		UpdatedAt:    timestamppb.New(c.UpdatedAt),
		CreatedBy:    c.CreatedBy,
	}, nil
}

// SetConfig safely sets configuration
func (c *JobTypeConfig) SetConfig(enabled bool, adminConfig, workerConfig []*plugin_pb.ConfigFieldValue) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.Enabled = enabled
	c.AdminConfig = adminConfig
	c.WorkerConfig = workerConfig
	c.UpdatedAt = time.Now()
}
