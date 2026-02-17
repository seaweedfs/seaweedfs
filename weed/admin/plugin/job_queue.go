package plugin

import (
	"container/heap"
	"fmt"
	"sync"
	"time"
)

// JobQueue manages job queueing with priority, deduplication, retry and history
type JobQueue struct {
	mu              sync.RWMutex
	priorityQueue   *PriorityQueue
	seenJobs        map[string]bool // For deduplication
	jobHistory      []*ExecutionRecord
	maxHistorySize  int
	deduplicationTTL time.Duration
	lastSeenJob     map[string]time.Time
}

// PriorityQueue implements heap.Interface for job ordering
type PriorityQueue []*Job

func (pq PriorityQueue) Len() int { return len(pq) }

func (pq PriorityQueue) Less(i, j int) bool {
	// Higher priority jobs come first
	if pq[i].Type != pq[j].Type {
		return pq[i].Type < pq[j].Type
	}
	// If same type, earlier creation time comes first
	return pq[i].CreatedAt.Before(pq[j].CreatedAt)
}

func (pq PriorityQueue) Swap(i, j int) {
	pq[i], pq[j] = pq[j], pq[i]
}

func (pq *PriorityQueue) Push(x interface{}) {
	*pq = append(*pq, x.(*Job))
}

func (pq *PriorityQueue) Pop() interface{} {
	old := *pq
	n := len(old)
	item := old[n-1]
	*pq = old[0 : n-1]
	return item
}

// NewJobQueue creates a new job queue
func NewJobQueue(maxHistorySize int, deduplicationTTL time.Duration) *JobQueue {
	jq := &JobQueue{
		priorityQueue:    &PriorityQueue{},
		seenJobs:         make(map[string]bool),
		jobHistory:       make([]*ExecutionRecord, 0, maxHistorySize),
		maxHistorySize:   maxHistorySize,
		deduplicationTTL: deduplicationTTL,
		lastSeenJob:      make(map[string]time.Time),
	}
	heap.Init(jq.priorityQueue)
	return jq
}

// Enqueue adds a job to the queue with deduplication
func (jq *JobQueue) Enqueue(job *Job) error {
	jq.mu.Lock()
	defer jq.mu.Unlock()

	// Check for duplicates within TTL window
	if lastSeen, exists := jq.lastSeenJob[job.ID]; exists {
		if time.Since(lastSeen) < jq.deduplicationTTL {
			return fmt.Errorf("job %s already enqueued recently", job.ID)
		}
	}

	job.SetState(JobStatePending)
	heap.Push(jq.priorityQueue, job)
	jq.seenJobs[job.ID] = true
	jq.lastSeenJob[job.ID] = time.Now()

	return nil
}

// Dequeue retrieves the next job from the queue
func (jq *JobQueue) Dequeue() *Job {
	jq.mu.Lock()
	defer jq.mu.Unlock()

	if jq.priorityQueue.Len() == 0 {
		return nil
	}

	job := heap.Pop(jq.priorityQueue).(*Job)
	return job
}

// Peek returns the next job without removing it
func (jq *JobQueue) Peek() *Job {
	jq.mu.RLock()
	defer jq.mu.RUnlock()

	if jq.priorityQueue.Len() == 0 {
		return nil
	}

	return (*jq.priorityQueue)[0]
}

// Size returns the current queue size
func (jq *JobQueue) Size() int {
	jq.mu.RLock()
	defer jq.mu.RUnlock()
	return jq.priorityQueue.Len()
}

// RecordExecution adds an execution record to history
func (jq *JobQueue) RecordExecution(record *ExecutionRecord) {
	jq.mu.Lock()
	defer jq.mu.Unlock()

	jq.jobHistory = append(jq.jobHistory, record)

	// Keep history size bounded
	if len(jq.jobHistory) > jq.maxHistorySize {
		// Remove oldest entries
		removeCount := len(jq.jobHistory) - jq.maxHistorySize
		jq.jobHistory = jq.jobHistory[removeCount:]
	}
}

// GetHistory returns job execution history
func (jq *JobQueue) GetHistory(limit int) []*ExecutionRecord {
	jq.mu.RLock()
	defer jq.mu.RUnlock()

	if limit <= 0 || limit > len(jq.jobHistory) {
		limit = len(jq.jobHistory)
	}

	// Return the most recent entries
	startIdx := len(jq.jobHistory) - limit
	if startIdx < 0 {
		startIdx = 0
	}

	result := make([]*ExecutionRecord, limit)
	copy(result, jq.jobHistory[startIdx:])
	return result
}

// GetHistoryForPlugin returns history for a specific plugin
func (jq *JobQueue) GetHistoryForPlugin(pluginID string, limit int) []*ExecutionRecord {
	jq.mu.RLock()
	defer jq.mu.RUnlock()

	var result []*ExecutionRecord
	for i := len(jq.jobHistory) - 1; i >= 0 && len(result) < limit; i-- {
		if jq.jobHistory[i].PluginID == pluginID {
			result = append(result, jq.jobHistory[i])
		}
	}
	return result
}

// GetHistoryForJobType returns history for a specific job type
func (jq *JobQueue) GetHistoryForJobType(jobType string, limit int) []*ExecutionRecord {
	jq.mu.RLock()
	defer jq.mu.RUnlock()

	var result []*ExecutionRecord
	for i := len(jq.jobHistory) - 1; i >= 0 && len(result) < limit; i-- {
		if jq.jobHistory[i].JobType == jobType {
			result = append(result, jq.jobHistory[i])
		}
	}
	return result
}

// ClearHistory removes all execution history
func (jq *JobQueue) ClearHistory() {
	jq.mu.Lock()
	defer jq.mu.Unlock()
	jq.jobHistory = make([]*ExecutionRecord, 0, jq.maxHistorySize)
}

// PurgeOldHistory removes history entries older than the specified time
func (jq *JobQueue) PurgeOldHistory(beforeTime time.Time) int {
	jq.mu.Lock()
	defer jq.mu.Unlock()

	removed := 0
	newHistory := make([]*ExecutionRecord, 0)

	for _, record := range jq.jobHistory {
		if record.CreatedAt.After(beforeTime) {
			newHistory = append(newHistory, record)
		} else {
			removed++
		}
	}

	jq.jobHistory = newHistory
	return removed
}

// HistorySize returns the number of records in history
func (jq *JobQueue) HistorySize() int {
	jq.mu.RLock()
	defer jq.mu.RUnlock()
	return len(jq.jobHistory)
}

// RetryJob re-enqueues a failed job up to maxRetries times
func (jq *JobQueue) RetryJob(job *Job, maxRetries int) error {
	jq.mu.Lock()
	defer jq.mu.Unlock()

	if job.RetryCount >= maxRetries {
		return fmt.Errorf("job %s exceeded max retries (%d)", job.ID, maxRetries)
	}

	job.RetryCount++
	job.SetState(JobStatePending)
	heap.Push(jq.priorityQueue, job)

	return nil
}

// GetExecutionStats returns statistics about job executions
func (jq *JobQueue) GetExecutionStats() map[string]interface{} {
	jq.mu.RLock()
	defer jq.mu.RUnlock()

	completed := 0
	failed := 0
	totalExecutionTime := int64(0)

	for _, record := range jq.jobHistory {
		switch record.State {
		case JobStateCompleted:
			completed++
		case JobStateFailed:
			failed++
		}
		if record.CompletedAt != nil && record.StartedAt != nil {
			totalExecutionTime += record.CompletedAt.Sub(*record.StartedAt).Milliseconds()
		}
	}

	avgExecutionTime := int64(0)
	if completed+failed > 0 {
		avgExecutionTime = totalExecutionTime / int64(completed+failed)
	}

	return map[string]interface{}{
		"total_history":          len(jq.jobHistory),
		"completed_jobs":         completed,
		"failed_jobs":            failed,
		"avg_execution_time_ms":  avgExecutionTime,
		"current_queue_size":     jq.priorityQueue.Len(),
	}
}

// GetQueuedJobs returns all jobs currently in the queue
func (jq *JobQueue) GetQueuedJobs() []*Job {
	jq.mu.RLock()
	defer jq.mu.RUnlock()

	result := make([]*Job, len(*jq.priorityQueue))
	copy(result, *jq.priorityQueue)
	return result
}

// RemoveJob removes a specific job from the queue
func (jq *JobQueue) RemoveJob(jobID string) bool {
	jq.mu.Lock()
	defer jq.mu.Unlock()

	for i, job := range *jq.priorityQueue {
		if job.ID == jobID {
			heap.Remove(jq.priorityQueue, i)
			return true
		}
	}
	return false
}

// PurgeQueuedJobs clears all pending jobs from the queue
func (jq *JobQueue) PurgeQueuedJobs() int {
	jq.mu.Lock()
	defer jq.mu.Unlock()

	count := jq.priorityQueue.Len()
	*jq.priorityQueue = PriorityQueue{}
	heap.Init(jq.priorityQueue)
	return count
}
