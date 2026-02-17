package plugin

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb/plugin_pb"
	"google.golang.org/grpc"
)

// Dispatcher handles job detection and dispatch to executors
type Dispatcher struct {
	registry  *Registry
	queues    map[string]*JobQueue
	configMgr *ConfigManager

	mu sync.RWMutex

	// Detection scheduling
	detectionIntervals map[string]time.Duration
	detectionTickers   map[string]*time.Ticker
	stopChan           chan struct{}

	// Job counter for ID generation
	jobCounter   uint64
	jobCounterMu sync.Mutex

	// Active streams for execution
	executionStreams map[string]*ExecutionStream
	streamsMu        sync.RWMutex
}

// ExecutionStream tracks an active job execution stream
type ExecutionStream struct {
	JobID      string
	PluginID   string
	Connection *grpc.ClientConn
	Context    context.Context
	Cancel     context.CancelFunc
	StartTime  time.Time
	LastUpdate time.Time
}

// NewDispatcher creates a new job dispatcher
func NewDispatcher(registry *Registry, configMgr *ConfigManager) *Dispatcher {
	return &Dispatcher{
		registry:           registry,
		queues:             make(map[string]*JobQueue),
		configMgr:          configMgr,
		detectionIntervals: make(map[string]time.Duration),
		detectionTickers:   make(map[string]*time.Ticker),
		stopChan:           make(chan struct{}),
		executionStreams:   make(map[string]*ExecutionStream),
	}
}

// RegisterJobType registers a job type for detection and dispatch
func (d *Dispatcher) RegisterJobType(jobType string, detectionInterval time.Duration) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	if _, exists := d.queues[jobType]; exists {
		return fmt.Errorf("job type %s already registered", jobType)
	}

	d.queues[jobType] = NewJobQueue(jobType)
	d.detectionIntervals[jobType] = detectionInterval

	return nil
}

// Start starts the dispatcher with detection scheduling
func (d *Dispatcher) Start() {
	d.mu.Lock()
	defer d.mu.Unlock()

	for jobType, interval := range d.detectionIntervals {
		ticker := time.NewTicker(interval)
		d.detectionTickers[jobType] = ticker
		go d.detectionLoop(jobType, ticker)
	}
}

// Stop stops the dispatcher
func (d *Dispatcher) Stop() {
	close(d.stopChan)

	d.mu.Lock()
	defer d.mu.Unlock()

	for _, ticker := range d.detectionTickers {
		ticker.Stop()
	}
}

// detectionLoop periodically runs detection for a job type
func (d *Dispatcher) detectionLoop(jobType string, ticker *time.Ticker) {
	for {
		select {
		case <-ticker.C:
			d.runDetection(jobType)
		case <-d.stopChan:
			return
		}
	}
}

// runDetection runs detection for a job type and queues detected jobs
func (d *Dispatcher) runDetection(jobType string) {
	// Get detector plugin
	_, err := d.registry.GetDetectorForJobType(jobType)
	if err != nil {
		// No detector available, skip
		return
	}

	// Get job type config
	config, err := d.configMgr.GetConfig(jobType)
	if err != nil || !config.Enabled {
		// Config not found or disabled, skip
		return
	}

	// TODO: Call detector plugin's DetectJobs method
	// For now, this is a placeholder for the gRPC call
}

// QueueJob adds a job to the queue
func (d *Dispatcher) QueueJob(jobType string, req *plugin_pb.JobRequest, dedupKey string) (*Job, error) {
	d.mu.RLock()
	queue, exists := d.queues[jobType]
	d.mu.RUnlock()

	if !exists {
		return nil, fmt.Errorf("job type %s not registered", jobType)
	}

	// Generate job ID
	jobID := d.generateJobID()

	return queue.AddJob(jobID, req, dedupKey)
}

// DispatchJob assigns a job to an available executor
func (d *Dispatcher) DispatchJob(jobType string) (*Job, *ConnectedPlugin, error) {
	d.mu.RLock()
	queue, exists := d.queues[jobType]
	d.mu.RUnlock()

	if !exists {
		return nil, nil, fmt.Errorf("job type %s not registered", jobType)
	}

	// Get next pending job
	job, err := queue.GetPendingJob()
	if err != nil {
		return nil, nil, err
	}

	// Get available executor
	executor, err := d.registry.GetExecutorForJobType(jobType)
	if err != nil {
		// Put job back in queue
		queue.mu.Lock()
		queue.pendingQueue = append(queue.pendingQueue, job)
		queue.mu.Unlock()
		return nil, nil, err
	}

	job.ExecutorID = executor.ID
	return job, executor, nil
}

// GetJob retrieves a job by ID across all queues
func (d *Dispatcher) GetJob(jobID string) (*Job, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()

	for _, queue := range d.queues {
		job, err := queue.GetJob(jobID)
		if err == nil {
			return job, nil
		}
	}

	return nil, fmt.Errorf("job %s not found", jobID)
}

// ListJobs returns jobs of a specific type and status
func (d *Dispatcher) ListJobs(jobType string, status JobState) []*Job {
	d.mu.RLock()
	queue, exists := d.queues[jobType]
	d.mu.RUnlock()

	if !exists {
		return nil
	}

	return queue.ListJobs(status, 0)
}

// ListAllJobs returns all jobs across all queues
func (d *Dispatcher) ListAllJobs() []*Job {
	d.mu.RLock()
	defer d.mu.RUnlock()

	var jobs []*Job
	for _, queue := range d.queues {
		jobs = append(jobs, queue.ListAllJobs()...)
	}
	return jobs
}

// UpdateJobProgress updates job progress
func (d *Dispatcher) UpdateJobProgress(jobID string, percent int32) error {
	job, err := d.GetJob(jobID)
	if err != nil {
		return err
	}

	job.SetProgress(percent)
	return nil
}

// CompleteJob marks a job as complete
func (d *Dispatcher) CompleteJob(jobID string, completed *plugin_pb.JobCompleted) error {
	job, err := d.GetJob(jobID)
	if err != nil {
		return err
	}

	d.mu.RLock()
	queue, exists := d.queues[job.Type]
	d.mu.RUnlock()

	if !exists {
		return fmt.Errorf("job type %s not found", job.Type)
	}

	d.removeExecutionStream(jobID)
	return queue.CompleteJob(jobID, completed)
}

// FailJob marks a job as failed
func (d *Dispatcher) FailJob(jobID string, failed *plugin_pb.JobFailed) error {
	job, err := d.GetJob(jobID)
	if err != nil {
		return err
	}

	d.mu.RLock()
	queue, exists := d.queues[job.Type]
	d.mu.RUnlock()

	if !exists {
		return fmt.Errorf("job type %s not found", job.Type)
	}

	d.removeExecutionStream(jobID)
	_, err = queue.FailJob(jobID, failed)
	return err
}

// CancelJob cancels a job
func (d *Dispatcher) CancelJob(jobID string) error {
	job, err := d.GetJob(jobID)
	if err != nil {
		return err
	}

	d.mu.RLock()
	queue, exists := d.queues[job.Type]
	d.mu.RUnlock()

	if !exists {
		return fmt.Errorf("job type %s not found", job.Type)
	}

	d.removeExecutionStream(jobID)
	return queue.CancelJob(jobID)
}

// RetryJob retries a failed job
func (d *Dispatcher) RetryJob(jobID string) error {
	job, err := d.GetJob(jobID)
	if err != nil {
		return err
	}

	state := job.GetState()
	if state != JobStateFailed {
		return fmt.Errorf("can only retry failed jobs, job is in %s state", state.String())
	}

	d.mu.RLock()
	queue, exists := d.queues[job.Type]
	d.mu.RUnlock()

	if !exists {
		return fmt.Errorf("job type %s not found", job.Type)
	}

	job.SetState(JobStatePending)
	job.Retries++
	queue.mu.Lock()
	queue.pendingQueue = append(queue.pendingQueue, job)
	queue.sortPendingQueue()
	queue.mu.Unlock()

	return nil
}

// generateJobID generates a unique job ID
func (d *Dispatcher) generateJobID() string {
	d.jobCounterMu.Lock()
	defer d.jobCounterMu.Unlock()
	d.jobCounter++
	return fmt.Sprintf("job-%d-%d", time.Now().Unix(), d.jobCounter)
}

// RegisterExecutionStream registers a new execution stream
func (d *Dispatcher) RegisterExecutionStream(jobID, pluginID string, ctx context.Context) *ExecutionStream {
	d.streamsMu.Lock()
	defer d.streamsMu.Unlock()

	cancelCtx, cancel := context.WithCancel(ctx)
	stream := &ExecutionStream{
		JobID:      jobID,
		PluginID:   pluginID,
		Context:    cancelCtx,
		Cancel:     cancel,
		StartTime:  time.Now(),
		LastUpdate: time.Now(),
	}

	d.executionStreams[jobID] = stream
	return stream
}

// removeExecutionStream removes an execution stream
func (d *Dispatcher) removeExecutionStream(jobID string) {
	d.streamsMu.Lock()
	defer d.streamsMu.Unlock()

	stream, exists := d.executionStreams[jobID]
	if exists {
		stream.Cancel()
		delete(d.executionStreams, jobID)
	}
}

// GetStats returns dispatcher statistics
func (d *Dispatcher) GetStats() map[string]interface{} {
	d.mu.RLock()
	defer d.mu.RUnlock()

	queueStats := make(map[string]interface{})
	for jobType, queue := range d.queues {
		queueStats[jobType] = queue.GetStats()
	}

	d.streamsMu.RLock()
	activeStreams := len(d.executionStreams)
	d.streamsMu.RUnlock()

	return map[string]interface{}{
		"queue_stats":      queueStats,
		"active_streams":   activeStreams,
		"registered_types": len(d.queues),
	}
}
