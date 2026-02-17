package plugin

import (
	"fmt"
	"sync"
	"time"
)

// Dispatcher orchestrates job detection scheduling and dispatch
type Dispatcher struct {
	mu                        sync.RWMutex
	registry                  *Registry
	queue                     *JobQueue
	detectionSchedules        map[string]*DetectionSchedule
	jobTypeStateManagement    map[string]*JobTypeState
	lastDetectionTime         map[string]time.Time
	detectionConcurrencyLimit map[string]int
}

// DetectionSchedule holds scheduling information for a detection type
type DetectionSchedule struct {
	DetectionType      string
	Interval           time.Duration
	LastExecuted       time.Time
	NextExecutionTime  time.Time
	ExecutionCount     int64
	FailureCount       int64
	AverageExecutionMs float64
}

// JobTypeState manages state for a specific job type
type JobTypeState struct {
	JobType             string
	mu                  sync.RWMutex
	ActiveCount         int
	MaxConcurrent       int
	PendingCount        int
	CompletedCount      int
	FailedCount         int
	LastError           string
	LastExecutionTime   time.Time
	AverageExecutionMs  float64
	ExecutionHistory    []time.Duration
	MaxHistorySize      int
}

// NewDispatcher creates a new job dispatcher
func NewDispatcher(registry *Registry, queue *JobQueue) *Dispatcher {
	return &Dispatcher{
		registry:                  registry,
		queue:                     queue,
		detectionSchedules:        make(map[string]*DetectionSchedule),
		jobTypeStateManagement:    make(map[string]*JobTypeState),
		lastDetectionTime:         make(map[string]time.Time),
		detectionConcurrencyLimit: make(map[string]int),
	}
}

// RegisterDetectionType registers a detection type with scheduling info
func (d *Dispatcher) RegisterDetectionType(detectionType string, interval time.Duration, maxConcurrent int) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	if _, exists := d.detectionSchedules[detectionType]; exists {
		return fmt.Errorf("detection type %s already registered", detectionType)
	}

	d.detectionSchedules[detectionType] = &DetectionSchedule{
		DetectionType:     detectionType,
		Interval:          interval,
		NextExecutionTime: time.Now(),
	}

	d.detectionConcurrencyLimit[detectionType] = maxConcurrent
	d.jobTypeStateManagement[detectionType] = &JobTypeState{
		JobType:        detectionType,
		MaxConcurrent:  maxConcurrent,
		MaxHistorySize: 100,
		ExecutionHistory: make([]time.Duration, 0, 100),
	}

	return nil
}

// UnregisterDetectionType removes a detection type
func (d *Dispatcher) UnregisterDetectionType(detectionType string) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	if _, exists := d.detectionSchedules[detectionType]; !exists {
		return fmt.Errorf("detection type %s not found", detectionType)
	}

	delete(d.detectionSchedules, detectionType)
	delete(d.detectionConcurrencyLimit, detectionType)
	delete(d.jobTypeStateManagement, detectionType)
	delete(d.lastDetectionTime, detectionType)

	return nil
}

// ScheduleDetections checks and schedules detection jobs that are due
func (d *Dispatcher) ScheduleDetections() []string {
	d.mu.Lock()
	defer d.mu.Unlock()

	var scheduledJobs []string
	now := time.Now()

	for detectionType, schedule := range d.detectionSchedules {
		if now.After(schedule.NextExecutionTime) {
			// Check if we haven't exceeded concurrency limit
			state := d.jobTypeStateManagement[detectionType]
			state.mu.RLock()
			activeCount := state.ActiveCount
			maxConcurrent := state.MaxConcurrent
			state.mu.RUnlock()

			if activeCount >= maxConcurrent {
				continue // Skip this detection type for now
			}

			// Create and enqueue job
			jobID := fmt.Sprintf("det-%s-%d", detectionType, now.UnixNano())
			job := &Job{
				ID:        jobID,
				Type:      detectionType,
				State:     JobStatePending,
				CreatedAt: now,
			}

			if err := d.queue.Enqueue(job); err != nil {
				continue
			}

			// Update schedule
			schedule.NextExecutionTime = now.Add(schedule.Interval)
			schedule.ExecutionCount++
			d.lastDetectionTime[detectionType] = now

			scheduledJobs = append(scheduledJobs, jobID)

			// Update state
			state.mu.Lock()
			state.PendingCount++
			state.mu.Unlock()
		}
	}

	return scheduledJobs
}

// DispatchJob assigns a job to an available plugin
func (d *Dispatcher) DispatchJob(job *Job) (string, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()

	// Find plugins capable of handling this job type
	plugins := d.registry.GetPluginsByCapability(job.Type)
	if len(plugins) == 0 {
		return "", fmt.Errorf("no plugins available for job type %s", job.Type)
	}

	// Find least loaded available plugin
	var selectedPlugin *ConnectedPlugin
	minLoad := int(^uint32(0) >> 1)

	for _, plugin := range plugins {
		if plugin.IsHealthy(30 * time.Second) {
			plugin.mu.RLock()
			if plugin.ActiveJobs < plugin.MaxConcurrentJobs && plugin.ActiveJobs < minLoad {
				selectedPlugin = plugin
				minLoad = plugin.ActiveJobs
			}
			plugin.mu.RUnlock()
		}
	}

	if selectedPlugin == nil {
		return "", fmt.Errorf("no healthy plugins available for job type %s", job.Type)
	}

	// Assign job to plugin
	job.PluginID = selectedPlugin.ID
	job.SetState(JobStateScheduled)
	selectedPlugin.IncActiveJobs()

	// Update job type state
	state := d.jobTypeStateManagement[job.Type]
	state.mu.Lock()
	state.ActiveCount++
	state.PendingCount--
	state.mu.Unlock()

	return selectedPlugin.ID, nil
}

// CompleteJob marks a job as completed
func (d *Dispatcher) CompleteJob(job *Job, result *JobResult) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	job.Result = result
	job.SetState(JobStateCompleted)

	// Update plugin
	if plugin, err := d.registry.GetPlugin(job.PluginID); err == nil {
		plugin.DecActiveJobs()
	}

	// Update state
	if state, exists := d.jobTypeStateManagement[job.Type]; exists {
		state.mu.Lock()
		state.ActiveCount--
		state.CompletedCount++
		if job.ExecutionTime > 0 {
			state.ExecutionHistory = append(state.ExecutionHistory, job.ExecutionTime)
			if len(state.ExecutionHistory) > state.MaxHistorySize {
				state.ExecutionHistory = state.ExecutionHistory[1:]
			}
			d.updateAverageExecutionTime(state)
		}
		state.LastExecutionTime = time.Now()
		state.mu.Unlock()
	}

	// Update detection schedule if applicable
	if schedule, exists := d.detectionSchedules[job.Type]; exists {
		schedule.LastExecuted = time.Now()
	}

	// Record execution
	record := &ExecutionRecord{
		JobID:       job.ID,
		JobType:     job.Type,
		PluginID:    job.PluginID,
		State:       job.State,
		CreatedAt:   job.CreatedAt,
		StartedAt:   job.StartedAt,
		CompletedAt: job.CompletedAt,
		Result:      result,
	}
	d.queue.RecordExecution(record)

	return nil
}

// FailJob marks a job as failed
func (d *Dispatcher) FailJob(job *Job, errorMsg string) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	job.LastError = errorMsg
	job.SetState(JobStateFailed)

	// Update plugin
	if plugin, err := d.registry.GetPlugin(job.PluginID); err == nil {
		plugin.DecActiveJobs()
	}

	// Update state
	if state, exists := d.jobTypeStateManagement[job.Type]; exists {
		state.mu.Lock()
		state.ActiveCount--
		state.FailedCount++
		state.LastError = errorMsg
		state.LastExecutionTime = time.Now()
		state.mu.Unlock()
	}

	// Update detection schedule
	if schedule, exists := d.detectionSchedules[job.Type]; exists {
		schedule.FailureCount++
		schedule.LastExecuted = time.Now()
	}

	// Record execution
	record := &ExecutionRecord{
		JobID:       job.ID,
		JobType:     job.Type,
		PluginID:    job.PluginID,
		State:       job.State,
		CreatedAt:   job.CreatedAt,
		StartedAt:   job.StartedAt,
		CompletedAt: job.CompletedAt,
		LastError:   errorMsg,
	}
	d.queue.RecordExecution(record)

	return nil
}

// updateAverageExecutionTime recalculates average execution time from history
func (d *Dispatcher) updateAverageExecutionTime(state *JobTypeState) {
	if len(state.ExecutionHistory) == 0 {
		state.AverageExecutionMs = 0
		return
	}

	var total int64
	for _, duration := range state.ExecutionHistory {
		total += duration.Milliseconds()
	}
	state.AverageExecutionMs = float64(total) / float64(len(state.ExecutionHistory))
}

// GetJobTypeState returns the state for a specific job type
func (d *Dispatcher) GetJobTypeState(jobType string) *JobTypeState {
	d.mu.RLock()
	defer d.mu.RUnlock()

	if state, exists := d.jobTypeStateManagement[jobType]; exists {
		return state
	}
	return nil
}

// GetAllJobTypeStates returns all job type states
func (d *Dispatcher) GetAllJobTypeStates() map[string]*JobTypeState {
	d.mu.RLock()
	defer d.mu.RUnlock()

	result := make(map[string]*JobTypeState)
	for jobType, state := range d.jobTypeStateManagement {
		result[jobType] = state
	}
	return result
}

// GetDetectionSchedule returns the schedule for a detection type
func (d *Dispatcher) GetDetectionSchedule(detectionType string) *DetectionSchedule {
	d.mu.RLock()
	defer d.mu.RUnlock()

	if schedule, exists := d.detectionSchedules[detectionType]; exists {
		return schedule
	}
	return nil
}

// GetDueDetections returns all detection types that are due for execution
func (d *Dispatcher) GetDueDetections() []string {
	d.mu.RLock()
	defer d.mu.RUnlock()

	var due []string
	now := time.Now()

	for detectionType, schedule := range d.detectionSchedules {
		if now.After(schedule.NextExecutionTime) {
			due = append(due, detectionType)
		}
	}

	return due
}

// GetDispatcherStats returns overall dispatcher statistics
func (d *Dispatcher) GetDispatcherStats() map[string]interface{} {
	d.mu.RLock()
	defer d.mu.RUnlock()

	totalActive := 0
	totalCompleted := 0
	totalFailed := 0

	for _, state := range d.jobTypeStateManagement {
		state.mu.RLock()
		totalActive += state.ActiveCount
		totalCompleted += state.CompletedCount
		totalFailed += state.FailedCount
		state.mu.RUnlock()
	}

	return map[string]interface{}{
		"detection_types_registered": len(d.detectionSchedules),
		"total_active_jobs":          totalActive,
		"total_completed_jobs":       totalCompleted,
		"total_failed_jobs":          totalFailed,
		"job_type_states":            len(d.jobTypeStateManagement),
	}
}
