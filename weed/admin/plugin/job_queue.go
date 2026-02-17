package plugin

import (
	"fmt"
	"sync"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb/plugin_pb"
)

// JobQueue manages jobs for a specific job type with priority and deduplication
type JobQueue struct {
	jobType string

	// Active jobs by ID
	jobs map[string]*Job

	// Priority queue
	pendingQueue []*Job

	// Completed jobs history (keeps last 1000)
	completedHistory []*Job
	maxHistorySize   int

	// Deduplication tracking
	seenKeys map[string]bool

	mu sync.RWMutex

	// Retry configuration
	maxRetries      int32
	retryBackoff    time.Duration
	retryBackoffMax time.Duration
}

// NewJobQueue creates a new job queue for a job type
func NewJobQueue(jobType string) *JobQueue {
	return &JobQueue{
		jobType:          jobType,
		jobs:             make(map[string]*Job),
		pendingQueue:     make([]*Job, 0),
		completedHistory: make([]*Job, 0),
		maxHistorySize:   1000,
		seenKeys:         make(map[string]bool),
		maxRetries:       3,
		retryBackoff:     time.Second,
		retryBackoffMax:  5 * time.Minute,
	}
}

// AddJob adds a new job to the queue with deduplication
func (q *JobQueue) AddJob(jobID string, req *plugin_pb.JobRequest, dedupKey string) (*Job, error) {
	q.mu.Lock()
	defer q.mu.Unlock()

	// Check deduplication
	if dedupKey != "" && q.seenKeys[dedupKey] {
		return nil, fmt.Errorf("job with key %s already queued", dedupKey)
	}

	if _, exists := q.jobs[jobID]; exists {
		return nil, fmt.Errorf("job %s already exists", jobID)
	}

	job := &Job{
		ID:          jobID,
		Type:        req.JobType,
		Description: req.Description,
		Priority:    req.Priority,
		State:       JobStatePending,
		CreatedAt:   time.Now(),
		UpdatedAt:   time.Now(),
		Config:      req.Config,
		Metadata:    req.Metadata,
		Retries:     0,
	}

	q.jobs[jobID] = job
	q.pendingQueue = append(q.pendingQueue, job)

	if dedupKey != "" {
		q.seenKeys[dedupKey] = true
	}

	// Sort by priority (higher priority first)
	q.sortPendingQueue()

	return job, nil
}

// GetPendingJob returns the next job to execute
func (q *JobQueue) GetPendingJob() (*Job, error) {
	q.mu.Lock()
	defer q.mu.Unlock()

	if len(q.pendingQueue) == 0 {
		return nil, fmt.Errorf("no pending jobs")
	}

	job := q.pendingQueue[0]
	q.pendingQueue = q.pendingQueue[1:]

	job.SetState(JobStateRunning)

	return job, nil
}

// GetJob retrieves a job by ID
func (q *JobQueue) GetJob(jobID string) (*Job, error) {
	q.mu.RLock()
	defer q.mu.RUnlock()

	job, exists := q.jobs[jobID]
	if !exists {
		return nil, fmt.Errorf("job %s not found", jobID)
	}

	return job, nil
}

// CompleteJob marks a job as completed
func (q *JobQueue) CompleteJob(jobID string, completed *plugin_pb.JobCompleted) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	job, exists := q.jobs[jobID]
	if !exists {
		return fmt.Errorf("job %s not found", jobID)
	}

	job.SetState(JobStateCompleted)
	job.CompletedInfo = completed

	q.addToHistory(job)

	return nil
}

// FailJob marks a job as failed and determines if it should be retried
func (q *JobQueue) FailJob(jobID string, failed *plugin_pb.JobFailed) (*Job, error) {
	q.mu.Lock()
	defer q.mu.Unlock()

	job, exists := q.jobs[jobID]
	if !exists {
		return nil, fmt.Errorf("job %s not found", jobID)
	}

	job.FailedInfo = failed

	// Check if we should retry
	if failed.Retryable && job.Retries < q.maxRetries {
		job.Retries++
		job.LastRetryTime = time.Now()
		job.SetState(JobStatePending)

		// Add back to queue with exponential backoff for retry
		q.pendingQueue = append(q.pendingQueue, job)
		q.sortPendingQueue()

		return job, nil
	}

	// No retry, mark as failed
	job.SetState(JobStateFailed)
	q.addToHistory(job)

	return job, nil
}

// PauseJob pauses a running job
func (q *JobQueue) PauseJob(jobID string) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	job, exists := q.jobs[jobID]
	if !exists {
		return fmt.Errorf("job %s not found", jobID)
	}

	if job.GetState() != JobStateRunning {
		return fmt.Errorf("job %s is not running", jobID)
	}

	job.SetState(JobStatePaused)
	return nil
}

// ResumeJob resumes a paused job
func (q *JobQueue) ResumeJob(jobID string) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	job, exists := q.jobs[jobID]
	if !exists {
		return fmt.Errorf("job %s not found", jobID)
	}

	if job.GetState() != JobStatePaused {
		return fmt.Errorf("job %s is not paused", jobID)
	}

	job.SetState(JobStateRunning)
	return nil
}

// CancelJob cancels a job
func (q *JobQueue) CancelJob(jobID string) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	job, exists := q.jobs[jobID]
	if !exists {
		return fmt.Errorf("job %s not found", jobID)
	}

	state := job.GetState()
	if state != JobStatePending && state != JobStateRunning && state != JobStatePaused {
		return fmt.Errorf("cannot cancel job in %s state", state.String())
	}

	job.SetState(JobStateCancelled)

	// Remove from pending queue if present
	for i, j := range q.pendingQueue {
		if j.ID == jobID {
			q.pendingQueue = append(q.pendingQueue[:i], q.pendingQueue[i+1:]...)
			break
		}
	}

	q.addToHistory(job)

	return nil
}

// ListJobs returns jobs filtered by status
func (q *JobQueue) ListJobs(status JobState, limit int) []*Job {
	q.mu.RLock()
	defer q.mu.RUnlock()

	var jobs []*Job

	// Include pending jobs from queue
	for _, job := range q.pendingQueue {
		if job.GetState() == status {
			jobs = append(jobs, job)
		}
	}

	// Include all active jobs
	for _, job := range q.jobs {
		if job.GetState() == status {
			jobs = append(jobs, job)
		}
	}

	if limit > 0 && len(jobs) > limit {
		jobs = jobs[:limit]
	}

	return jobs
}

// ListAllJobs returns all jobs
func (q *JobQueue) ListAllJobs() []*Job {
	q.mu.RLock()
	defer q.mu.RUnlock()

	jobs := make([]*Job, 0, len(q.jobs))
	for _, job := range q.jobs {
		jobs = append(jobs, job)
	}

	// Add completed history
	for _, job := range q.completedHistory {
		jobs = append(jobs, job)
	}

	return jobs
}

// sortPendingQueue sorts the pending queue by priority (higher first)
func (q *JobQueue) sortPendingQueue() {
	for i := 0; i < len(q.pendingQueue)-1; i++ {
		for j := i + 1; j < len(q.pendingQueue); j++ {
			if q.pendingQueue[j].Priority > q.pendingQueue[i].Priority {
				q.pendingQueue[i], q.pendingQueue[j] = q.pendingQueue[j], q.pendingQueue[i]
			}
		}
	}
}

// addToHistory adds a completed job to history
func (q *JobQueue) addToHistory(job *Job) {
	q.completedHistory = append(q.completedHistory, job)

	// Keep only last N entries
	if len(q.completedHistory) > q.maxHistorySize {
		q.completedHistory = q.completedHistory[len(q.completedHistory)-q.maxHistorySize:]
	}

	// Remove from active jobs
	delete(q.jobs, job.ID)
}

// GetPendingCount returns number of pending jobs
func (q *JobQueue) GetPendingCount() int {
	q.mu.RLock()
	defer q.mu.RUnlock()
	return len(q.pendingQueue)
}

// GetStats returns queue statistics
func (q *JobQueue) GetStats() map[string]interface{} {
	q.mu.RLock()
	defer q.mu.RUnlock()

	var running, completed, failed, cancelled int

	for _, job := range q.jobs {
		switch job.GetState() {
		case JobStateRunning:
			running++
		case JobStateCompleted:
			completed++
		case JobStateFailed:
			failed++
		case JobStateCancelled:
			cancelled++
		}
	}

	completed += len(q.completedHistory)

	return map[string]interface{}{
		"job_type":     q.jobType,
		"pending":      len(q.pendingQueue),
		"running":      running,
		"completed":    completed,
		"failed":       failed,
		"cancelled":    cancelled,
		"total":        len(q.jobs) + len(q.completedHistory),
		"history_size": len(q.completedHistory),
	}
}
