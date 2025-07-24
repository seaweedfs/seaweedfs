package task

import (
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
)

// FailureHandler handles various failure scenarios in the task system
type FailureHandler struct {
	config *AdminConfig
}

// NewFailureHandler creates a new failure handler
func NewFailureHandler(config *AdminConfig) *FailureHandler {
	return &FailureHandler{
		config: config,
	}
}

// HandleWorkerTimeout handles worker timeout scenarios
func (fh *FailureHandler) HandleWorkerTimeout(workerID string, affectedTasks []*InProgressTask) {
	glog.Warningf("Handling worker timeout for worker %s with %d affected tasks", workerID, len(affectedTasks))

	for _, task := range affectedTasks {
		fh.handleTaskFailure(task, "worker_timeout", "Worker became unresponsive")
	}
}

// HandleTaskStuck handles stuck task scenarios
func (fh *FailureHandler) HandleTaskStuck(task *InProgressTask) {
	glog.Warningf("Handling stuck task %s (no progress for %v)", task.Task.ID, time.Since(task.LastUpdate))

	fh.handleTaskFailure(task, "task_stuck", "Task made no progress within timeout period")
}

// HandleTaskFailure handles general task failure scenarios
func (fh *FailureHandler) HandleTaskFailure(task *InProgressTask, reason string, details string) {
	glog.Errorf("Handling task failure for task %s: %s - %s", task.Task.ID, reason, details)

	fh.handleTaskFailure(task, reason, details)
}

// handleTaskFailure is the internal handler for task failures
func (fh *FailureHandler) handleTaskFailure(task *InProgressTask, reason string, details string) {
	// Record failure reason
	task.Task.Error = details

	// Determine if task should be retried
	if task.Task.RetryCount < fh.config.MaxRetries {
		fh.scheduleRetry(task, reason)
	} else {
		fh.markTaskFailed(task, reason)
	}
}

// scheduleRetry schedules a task for retry
func (fh *FailureHandler) scheduleRetry(task *InProgressTask, reason string) {
	task.Task.RetryCount++

	// Calculate retry delay with exponential backoff
	retryDelay := time.Duration(task.Task.RetryCount) * 5 * time.Minute
	task.Task.ScheduledAt = time.Now().Add(retryDelay)

	glog.Infof("Scheduling retry %d/%d for task %s (reason: %s, delay: %v)",
		task.Task.RetryCount, fh.config.MaxRetries, task.Task.ID, reason, retryDelay)
}

// markTaskFailed permanently marks a task as failed
func (fh *FailureHandler) markTaskFailed(task *InProgressTask, reason string) {
	glog.Errorf("Task %s permanently failed after %d retries (reason: %s)",
		task.Task.ID, task.Task.RetryCount, reason)

	// Could trigger alerts or notifications here
	fh.sendFailureAlert(task, reason)
}

// sendFailureAlert sends alerts for permanently failed tasks
func (fh *FailureHandler) sendFailureAlert(task *InProgressTask, reason string) {
	// In a real implementation, this would:
	// 1. Send notifications to administrators
	// 2. Update monitoring dashboards
	// 3. Log to audit trails
	// 4. Possibly trigger automatic remediation

	glog.Errorf("ALERT: Task permanently failed - ID: %s, Type: %s, Volume: %d, Reason: %s",
		task.Task.ID, task.Task.Type, task.Task.VolumeID, reason)
}

// HandleDuplicateTask handles duplicate task detection
func (fh *FailureHandler) HandleDuplicateTask(existingTaskID string, duplicateTaskID string, volumeID uint32) {
	glog.Warningf("Detected duplicate task for volume %d: existing=%s, duplicate=%s",
		volumeID, existingTaskID, duplicateTaskID)

	// Cancel the duplicate task
	// In a real implementation, this would send a cancellation signal
}

// HandleResourceExhaustion handles resource exhaustion scenarios
func (fh *FailureHandler) HandleResourceExhaustion(workerID string, taskType string) {
	glog.Warningf("Worker %s reported resource exhaustion for task type %s", workerID, taskType)

	// Could implement:
	// 1. Temporary worker blacklisting
	// 2. Task redistribution
	// 3. Resource monitoring alerts
}

// GetFailureStats returns failure statistics
func (fh *FailureHandler) GetFailureStats() map[string]interface{} {
	// In a real implementation, this would track:
	// - Failure rates by type
	// - Worker reliability scores
	// - Task retry statistics
	// - System health metrics

	return map[string]interface{}{
		"enabled":        true,
		"max_retries":    fh.config.MaxRetries,
		"task_timeout":   fh.config.TaskTimeout.String(),
		"worker_timeout": fh.config.WorkerTimeout.String(),
	}
}
