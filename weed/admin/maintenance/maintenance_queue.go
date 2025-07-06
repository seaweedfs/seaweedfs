package maintenance

import (
	"sort"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
)

// NewMaintenanceQueue creates a new maintenance queue
func NewMaintenanceQueue(policy *MaintenancePolicy) *MaintenanceQueue {
	queue := &MaintenanceQueue{
		tasks:        make(map[string]*MaintenanceTask),
		workers:      make(map[string]*MaintenanceWorker),
		pendingTasks: make([]*MaintenanceTask, 0),
		policy:       policy,
	}
	return queue
}

// SetIntegration sets the integration reference
func (mq *MaintenanceQueue) SetIntegration(integration *MaintenanceIntegration) {
	mq.integration = integration
	glog.V(1).Infof("Maintenance queue configured with integration")
}

// AddTask adds a new maintenance task to the queue
func (mq *MaintenanceQueue) AddTask(task *MaintenanceTask) {
	mq.mutex.Lock()
	defer mq.mutex.Unlock()

	task.ID = generateTaskID()
	task.Status = TaskStatusPending
	task.CreatedAt = time.Now()
	task.MaxRetries = 3 // Default retry count

	mq.tasks[task.ID] = task
	mq.pendingTasks = append(mq.pendingTasks, task)

	// Sort pending tasks by priority and schedule time
	sort.Slice(mq.pendingTasks, func(i, j int) bool {
		if mq.pendingTasks[i].Priority != mq.pendingTasks[j].Priority {
			return mq.pendingTasks[i].Priority > mq.pendingTasks[j].Priority
		}
		return mq.pendingTasks[i].ScheduledAt.Before(mq.pendingTasks[j].ScheduledAt)
	})

	glog.V(2).Infof("Added maintenance task %s: %s for volume %d", task.ID, task.Type, task.VolumeID)
}

// AddTasksFromResults converts detection results to tasks and adds them to the queue
func (mq *MaintenanceQueue) AddTasksFromResults(results []*TaskDetectionResult) {
	for _, result := range results {
		task := &MaintenanceTask{
			Type:        result.TaskType,
			Priority:    result.Priority,
			VolumeID:    result.VolumeID,
			Server:      result.Server,
			Collection:  result.Collection,
			Parameters:  result.Parameters,
			Reason:      result.Reason,
			ScheduledAt: result.ScheduleAt,
		}
		mq.AddTask(task)
	}
}

// GetNextTask returns the next available task for a worker
func (mq *MaintenanceQueue) GetNextTask(workerID string, capabilities []MaintenanceTaskType) *MaintenanceTask {
	mq.mutex.Lock()
	defer mq.mutex.Unlock()

	worker, exists := mq.workers[workerID]
	if !exists {
		return nil
	}

	// Check if worker has capacity
	if worker.CurrentLoad >= worker.MaxConcurrent {
		return nil
	}

	now := time.Now()

	// Find the next suitable task
	for i, task := range mq.pendingTasks {
		// Check if it's time to execute the task
		if task.ScheduledAt.After(now) {
			continue
		}

		// Check if worker can handle this task type
		if !mq.workerCanHandle(task.Type, capabilities) {
			continue
		}

		// Check scheduling logic - use simplified system if available, otherwise fallback
		if !mq.canScheduleTaskNow(task) {
			continue
		}

		// Assign task to worker
		task.Status = TaskStatusAssigned
		task.WorkerID = workerID
		startTime := now
		task.StartedAt = &startTime

		// Remove from pending tasks
		mq.pendingTasks = append(mq.pendingTasks[:i], mq.pendingTasks[i+1:]...)

		// Update worker
		worker.CurrentTask = task
		worker.CurrentLoad++
		worker.Status = "busy"

		glog.V(2).Infof("Assigned task %s to worker %s", task.ID, workerID)
		return task
	}

	return nil
}

// CompleteTask marks a task as completed
func (mq *MaintenanceQueue) CompleteTask(taskID string, error string) {
	mq.mutex.Lock()
	defer mq.mutex.Unlock()

	task, exists := mq.tasks[taskID]
	if !exists {
		return
	}

	completedTime := time.Now()
	task.CompletedAt = &completedTime

	if error != "" {
		task.Status = TaskStatusFailed
		task.Error = error

		// Check if task should be retried
		if task.RetryCount < task.MaxRetries {
			task.RetryCount++
			task.Status = TaskStatusPending
			task.WorkerID = ""
			task.StartedAt = nil
			task.CompletedAt = nil
			task.Error = ""
			task.ScheduledAt = time.Now().Add(15 * time.Minute) // Retry delay

			mq.pendingTasks = append(mq.pendingTasks, task)
			glog.V(2).Infof("Retrying task %s (attempt %d/%d)", taskID, task.RetryCount, task.MaxRetries)
		} else {
			glog.Errorf("Task %s failed permanently after %d retries: %s", taskID, task.MaxRetries, error)
		}
	} else {
		task.Status = TaskStatusCompleted
		task.Progress = 100
		glog.V(2).Infof("Task %s completed successfully", taskID)
	}

	// Update worker
	if task.WorkerID != "" {
		if worker, exists := mq.workers[task.WorkerID]; exists {
			worker.CurrentTask = nil
			worker.CurrentLoad--
			if worker.CurrentLoad == 0 {
				worker.Status = "active"
			}
		}
	}
}

// UpdateTaskProgress updates the progress of a running task
func (mq *MaintenanceQueue) UpdateTaskProgress(taskID string, progress float64) {
	mq.mutex.RLock()
	defer mq.mutex.RUnlock()

	if task, exists := mq.tasks[taskID]; exists {
		task.Progress = progress
		task.Status = TaskStatusInProgress
	}
}

// RegisterWorker registers a new worker
func (mq *MaintenanceQueue) RegisterWorker(worker *MaintenanceWorker) {
	mq.mutex.Lock()
	defer mq.mutex.Unlock()

	worker.LastHeartbeat = time.Now()
	worker.Status = "active"
	worker.CurrentLoad = 0
	mq.workers[worker.ID] = worker

	glog.V(1).Infof("Registered maintenance worker %s at %s", worker.ID, worker.Address)
}

// UpdateWorkerHeartbeat updates worker heartbeat
func (mq *MaintenanceQueue) UpdateWorkerHeartbeat(workerID string) {
	mq.mutex.Lock()
	defer mq.mutex.Unlock()

	if worker, exists := mq.workers[workerID]; exists {
		worker.LastHeartbeat = time.Now()
	}
}

// GetRunningTaskCount returns the number of running tasks of a specific type
func (mq *MaintenanceQueue) GetRunningTaskCount(taskType MaintenanceTaskType) int {
	mq.mutex.RLock()
	defer mq.mutex.RUnlock()

	count := 0
	for _, task := range mq.tasks {
		if task.Type == taskType && (task.Status == TaskStatusAssigned || task.Status == TaskStatusInProgress) {
			count++
		}
	}
	return count
}

// WasTaskRecentlyCompleted checks if a similar task was recently completed
func (mq *MaintenanceQueue) WasTaskRecentlyCompleted(taskType MaintenanceTaskType, volumeID uint32, server string, now time.Time) bool {
	mq.mutex.RLock()
	defer mq.mutex.RUnlock()

	// Get the repeat prevention interval for this task type
	interval := mq.getRepeatPreventionInterval(taskType)
	cutoff := now.Add(-interval)

	for _, task := range mq.tasks {
		if task.Type == taskType &&
			task.VolumeID == volumeID &&
			task.Server == server &&
			task.Status == TaskStatusCompleted &&
			task.CompletedAt != nil &&
			task.CompletedAt.After(cutoff) {
			return true
		}
	}
	return false
}

// getRepeatPreventionInterval returns the interval for preventing task repetition
func (mq *MaintenanceQueue) getRepeatPreventionInterval(taskType MaintenanceTaskType) time.Duration {
	// First try to get default from task scheduler
	if mq.integration != nil {
		if scheduler := mq.integration.GetTaskScheduler(taskType); scheduler != nil {
			defaultInterval := scheduler.GetDefaultRepeatInterval()
			if defaultInterval > 0 {
				glog.V(3).Infof("Using task scheduler default repeat interval for %s: %v", taskType, defaultInterval)
				return defaultInterval
			}
		}
	}

	// Fallback to policy configuration if no scheduler available or scheduler doesn't provide default
	if mq.policy != nil {
		repeatIntervalHours := mq.policy.GetRepeatInterval(taskType)
		if repeatIntervalHours > 0 {
			interval := time.Duration(repeatIntervalHours) * time.Hour
			glog.V(3).Infof("Using policy configuration repeat interval for %s: %v", taskType, interval)
			return interval
		}
	}

	// Ultimate fallback - but avoid hardcoded values where possible
	glog.V(2).Infof("No scheduler or policy configuration found for task type %s, using minimal default: 1h", taskType)
	return time.Hour // Minimal safe default
}

// GetTasks returns tasks with optional filtering
func (mq *MaintenanceQueue) GetTasks(status MaintenanceTaskStatus, taskType MaintenanceTaskType, limit int) []*MaintenanceTask {
	mq.mutex.RLock()
	defer mq.mutex.RUnlock()

	var tasks []*MaintenanceTask
	for _, task := range mq.tasks {
		if status != "" && task.Status != status {
			continue
		}
		if taskType != "" && task.Type != taskType {
			continue
		}
		tasks = append(tasks, task)
		if limit > 0 && len(tasks) >= limit {
			break
		}
	}

	// Sort by creation time (newest first)
	sort.Slice(tasks, func(i, j int) bool {
		return tasks[i].CreatedAt.After(tasks[j].CreatedAt)
	})

	return tasks
}

// GetWorkers returns all registered workers
func (mq *MaintenanceQueue) GetWorkers() []*MaintenanceWorker {
	mq.mutex.RLock()
	defer mq.mutex.RUnlock()

	var workers []*MaintenanceWorker
	for _, worker := range mq.workers {
		workers = append(workers, worker)
	}
	return workers
}

// generateTaskID generates a unique ID for tasks
func generateTaskID() string {
	const charset = "abcdefghijklmnopqrstuvwxyz0123456789"
	b := make([]byte, 8)
	for i := range b {
		b[i] = charset[i%len(charset)]
	}
	return string(b)
}

// CleanupOldTasks removes old completed and failed tasks
func (mq *MaintenanceQueue) CleanupOldTasks(retention time.Duration) int {
	mq.mutex.Lock()
	defer mq.mutex.Unlock()

	cutoff := time.Now().Add(-retention)
	removed := 0

	for id, task := range mq.tasks {
		if (task.Status == TaskStatusCompleted || task.Status == TaskStatusFailed) &&
			task.CompletedAt != nil &&
			task.CompletedAt.Before(cutoff) {
			delete(mq.tasks, id)
			removed++
		}
	}

	glog.V(2).Infof("Cleaned up %d old maintenance tasks", removed)
	return removed
}

// RemoveStaleWorkers removes workers that haven't sent heartbeat recently
func (mq *MaintenanceQueue) RemoveStaleWorkers(timeout time.Duration) int {
	mq.mutex.Lock()
	defer mq.mutex.Unlock()

	cutoff := time.Now().Add(-timeout)
	removed := 0

	for id, worker := range mq.workers {
		if worker.LastHeartbeat.Before(cutoff) {
			// Mark any assigned tasks as failed
			for _, task := range mq.tasks {
				if task.WorkerID == id && (task.Status == TaskStatusAssigned || task.Status == TaskStatusInProgress) {
					task.Status = TaskStatusFailed
					task.Error = "Worker became unavailable"
					completedTime := time.Now()
					task.CompletedAt = &completedTime
				}
			}

			delete(mq.workers, id)
			removed++
			glog.Warningf("Removed stale maintenance worker %s", id)
		}
	}

	return removed
}

// GetStats returns maintenance statistics
func (mq *MaintenanceQueue) GetStats() *MaintenanceStats {
	mq.mutex.RLock()
	defer mq.mutex.RUnlock()

	stats := &MaintenanceStats{
		TotalTasks:    len(mq.tasks),
		TasksByStatus: make(map[MaintenanceTaskStatus]int),
		TasksByType:   make(map[MaintenanceTaskType]int),
		ActiveWorkers: 0,
	}

	today := time.Now().Truncate(24 * time.Hour)
	var totalDuration time.Duration
	var completedTasks int

	for _, task := range mq.tasks {
		stats.TasksByStatus[task.Status]++
		stats.TasksByType[task.Type]++

		if task.CompletedAt != nil && task.CompletedAt.After(today) {
			if task.Status == TaskStatusCompleted {
				stats.CompletedToday++
			} else if task.Status == TaskStatusFailed {
				stats.FailedToday++
			}

			if task.StartedAt != nil {
				duration := task.CompletedAt.Sub(*task.StartedAt)
				totalDuration += duration
				completedTasks++
			}
		}
	}

	for _, worker := range mq.workers {
		if worker.Status == "active" || worker.Status == "busy" {
			stats.ActiveWorkers++
		}
	}

	if completedTasks > 0 {
		stats.AverageTaskTime = totalDuration / time.Duration(completedTasks)
	}

	return stats
}

// workerCanHandle checks if a worker can handle a specific task type
func (mq *MaintenanceQueue) workerCanHandle(taskType MaintenanceTaskType, capabilities []MaintenanceTaskType) bool {
	for _, capability := range capabilities {
		if capability == taskType {
			return true
		}
	}
	return false
}

// canScheduleTaskNow determines if a task can be scheduled using task schedulers or fallback logic
func (mq *MaintenanceQueue) canScheduleTaskNow(task *MaintenanceTask) bool {
	// Try task scheduling logic first
	if mq.integration != nil {
		// Get all running tasks and available workers
		runningTasks := mq.getRunningTasks()
		availableWorkers := mq.getAvailableWorkers()

		canSchedule := mq.integration.CanScheduleWithTaskSchedulers(task, runningTasks, availableWorkers)
		glog.V(3).Infof("Task scheduler decision for task %s (%s): %v", task.ID, task.Type, canSchedule)
		return canSchedule
	}

	// Fallback to hardcoded logic
	return mq.canExecuteTaskType(task.Type)
}

// canExecuteTaskType checks if we can execute more tasks of this type (concurrency limits) - fallback logic
func (mq *MaintenanceQueue) canExecuteTaskType(taskType MaintenanceTaskType) bool {
	runningCount := mq.GetRunningTaskCount(taskType)
	maxConcurrent := mq.getMaxConcurrentForTaskType(taskType)

	return runningCount < maxConcurrent
}

// getMaxConcurrentForTaskType returns the maximum concurrent tasks allowed for a task type
func (mq *MaintenanceQueue) getMaxConcurrentForTaskType(taskType MaintenanceTaskType) int {
	// First try to get default from task scheduler
	if mq.integration != nil {
		if scheduler := mq.integration.GetTaskScheduler(taskType); scheduler != nil {
			maxConcurrent := scheduler.GetMaxConcurrent()
			if maxConcurrent > 0 {
				glog.V(3).Infof("Using task scheduler max concurrent for %s: %d", taskType, maxConcurrent)
				return maxConcurrent
			}
		}
	}

	// Fallback to policy configuration if no scheduler available or scheduler doesn't provide default
	if mq.policy != nil {
		maxConcurrent := mq.policy.GetMaxConcurrent(taskType)
		if maxConcurrent > 0 {
			glog.V(3).Infof("Using policy configuration max concurrent for %s: %d", taskType, maxConcurrent)
			return maxConcurrent
		}
	}

	// Ultimate fallback - minimal safe default
	glog.V(2).Infof("No scheduler or policy configuration found for task type %s, using minimal default: 1", taskType)
	return 1
}

// getRunningTasks returns all currently running tasks
func (mq *MaintenanceQueue) getRunningTasks() []*MaintenanceTask {
	var runningTasks []*MaintenanceTask
	for _, task := range mq.tasks {
		if task.Status == TaskStatusAssigned || task.Status == TaskStatusInProgress {
			runningTasks = append(runningTasks, task)
		}
	}
	return runningTasks
}

// getAvailableWorkers returns all workers that can take more work
func (mq *MaintenanceQueue) getAvailableWorkers() []*MaintenanceWorker {
	var availableWorkers []*MaintenanceWorker
	for _, worker := range mq.workers {
		if worker.Status == "active" && worker.CurrentLoad < worker.MaxConcurrent {
			availableWorkers = append(availableWorkers, worker)
		}
	}
	return availableWorkers
}
