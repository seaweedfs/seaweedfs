package maintenance

import (
	"crypto/rand"
	"fmt"
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

// SetPersistence sets the task persistence interface
func (mq *MaintenanceQueue) SetPersistence(persistence TaskPersistence) {
	mq.persistence = persistence
	glog.V(1).Infof("Maintenance queue configured with task persistence")
}

// LoadTasksFromPersistence loads tasks from persistent storage on startup
func (mq *MaintenanceQueue) LoadTasksFromPersistence() error {
	if mq.persistence == nil {
		glog.V(1).Infof("No task persistence configured, skipping task loading")
		return nil
	}

	mq.mutex.Lock()
	defer mq.mutex.Unlock()

	glog.Infof("Loading tasks from persistence...")

	tasks, err := mq.persistence.LoadAllTaskStates()
	if err != nil {
		return fmt.Errorf("failed to load task states: %w", err)
	}

	glog.Infof("DEBUG LoadTasksFromPersistence: Found %d tasks in persistence", len(tasks))

	// Reset task maps
	mq.tasks = make(map[string]*MaintenanceTask)
	mq.pendingTasks = make([]*MaintenanceTask, 0)

	// Load tasks by status
	for _, task := range tasks {
		glog.Infof("DEBUG LoadTasksFromPersistence: Loading task %s (type: %s, status: %s, scheduled: %v)", task.ID, task.Type, task.Status, task.ScheduledAt)
		mq.tasks[task.ID] = task

		switch task.Status {
		case TaskStatusPending:
			glog.Infof("DEBUG LoadTasksFromPersistence: Adding task %s to pending queue", task.ID)
			mq.pendingTasks = append(mq.pendingTasks, task)
		case TaskStatusAssigned, TaskStatusInProgress:
			// For assigned/in-progress tasks, we need to check if the worker is still available
			// If not, we should fail them and make them eligible for retry
			if task.WorkerID != "" {
				if _, exists := mq.workers[task.WorkerID]; !exists {
					glog.Warningf("Task %s was assigned to unavailable worker %s, marking as failed", task.ID, task.WorkerID)
					task.Status = TaskStatusFailed
					task.Error = "Worker unavailable after restart"
					completedTime := time.Now()
					task.CompletedAt = &completedTime

					// Check if it should be retried
					if task.RetryCount < task.MaxRetries {
						task.RetryCount++
						task.Status = TaskStatusPending
						task.WorkerID = ""
						task.StartedAt = nil
						task.CompletedAt = nil
						task.Error = ""
						task.ScheduledAt = time.Now().Add(1 * time.Minute) // Retry after restart delay
						glog.Infof("DEBUG LoadTasksFromPersistence: Retrying task %s, adding to pending queue", task.ID)
						mq.pendingTasks = append(mq.pendingTasks, task)
					}
				}
			}
		}
	}

	// Sort pending tasks by priority and schedule time
	sort.Slice(mq.pendingTasks, func(i, j int) bool {
		if mq.pendingTasks[i].Priority != mq.pendingTasks[j].Priority {
			return mq.pendingTasks[i].Priority > mq.pendingTasks[j].Priority
		}
		return mq.pendingTasks[i].ScheduledAt.Before(mq.pendingTasks[j].ScheduledAt)
	})

	glog.Infof("Loaded %d tasks from persistence (%d pending)", len(tasks), len(mq.pendingTasks))
	return nil
}

// saveTaskState saves a task to persistent storage
func (mq *MaintenanceQueue) saveTaskState(task *MaintenanceTask) {
	if mq.persistence != nil {
		if err := mq.persistence.SaveTaskState(task); err != nil {
			glog.Errorf("Failed to save task state for %s: %v", task.ID, err)
		}
	}
}

// cleanupCompletedTasks removes old completed tasks beyond the retention limit
func (mq *MaintenanceQueue) cleanupCompletedTasks() {
	if mq.persistence != nil {
		if err := mq.persistence.CleanupCompletedTasks(); err != nil {
			glog.Errorf("Failed to cleanup completed tasks: %v", err)
		}
	}
}

// AddTask adds a new maintenance task to the queue with deduplication
func (mq *MaintenanceQueue) AddTask(task *MaintenanceTask) {
	mq.mutex.Lock()
	defer mq.mutex.Unlock()

	// Check for duplicate tasks (same type + volume + not completed)
	if mq.hasDuplicateTask(task) {
		glog.V(1).Infof("Task skipped (duplicate): %s for volume %d on %s (already queued or running)",
			task.Type, task.VolumeID, task.Server)
		return
	}

	if task.ID == "" {
		task.ID = generateTaskID()
	}
	task.Status = TaskStatusPending
	task.CreatedAt = time.Now()
	task.MaxRetries = 3 // Default retry count

	// Initialize assignment history and set creation context
	task.AssignmentHistory = make([]*TaskAssignmentRecord, 0)
	if task.CreatedBy == "" {
		task.CreatedBy = "maintenance-system"
	}
	if task.CreationContext == "" {
		task.CreationContext = "Automatic task creation based on system monitoring"
	}
	if task.Tags == nil {
		task.Tags = make(map[string]string)
	}

	mq.tasks[task.ID] = task
	mq.pendingTasks = append(mq.pendingTasks, task)

	// Sort pending tasks by priority and schedule time
	sort.Slice(mq.pendingTasks, func(i, j int) bool {
		if mq.pendingTasks[i].Priority != mq.pendingTasks[j].Priority {
			return mq.pendingTasks[i].Priority > mq.pendingTasks[j].Priority
		}
		return mq.pendingTasks[i].ScheduledAt.Before(mq.pendingTasks[j].ScheduledAt)
	})

	// Save task state to persistence
	mq.saveTaskState(task)

	scheduleInfo := ""
	if !task.ScheduledAt.IsZero() && time.Until(task.ScheduledAt) > time.Minute {
		scheduleInfo = fmt.Sprintf(", scheduled for %v", task.ScheduledAt.Format("15:04:05"))
	}

	glog.Infof("Task queued: %s (%s) volume %d on %s, priority %d%s, reason: %s",
		task.ID, task.Type, task.VolumeID, task.Server, task.Priority, scheduleInfo, task.Reason)
}

// hasDuplicateTask checks if a similar task already exists (same type, volume, and not completed)
func (mq *MaintenanceQueue) hasDuplicateTask(newTask *MaintenanceTask) bool {
	for _, existingTask := range mq.tasks {
		if existingTask.Type == newTask.Type &&
			existingTask.VolumeID == newTask.VolumeID &&
			existingTask.Server == newTask.Server &&
			(existingTask.Status == TaskStatusPending ||
				existingTask.Status == TaskStatusAssigned ||
				existingTask.Status == TaskStatusInProgress) {
			return true
		}
	}
	return false
}

// AddTasksFromResults converts detection results to tasks and adds them to the queue
func (mq *MaintenanceQueue) AddTasksFromResults(results []*TaskDetectionResult) {
	for _, result := range results {
		// Validate that task has proper typed parameters
		if result.TypedParams == nil {
			glog.Warningf("Rejecting invalid task: %s for volume %d on %s - no typed parameters (insufficient destinations or planning failed)",
				result.TaskType, result.VolumeID, result.Server)
			continue
		}

		task := &MaintenanceTask{
			ID:         result.TaskID,
			Type:       result.TaskType,
			Priority:   result.Priority,
			VolumeID:   result.VolumeID,
			Server:     result.Server,
			Collection: result.Collection,
			// Copy typed protobuf parameters
			TypedParams: result.TypedParams,
			Reason:      result.Reason,
			ScheduledAt: result.ScheduleAt,
		}
		mq.AddTask(task)
	}
}

// GetNextTask returns the next available task for a worker
func (mq *MaintenanceQueue) GetNextTask(workerID string, capabilities []MaintenanceTaskType) *MaintenanceTask {
	// Use read lock for initial checks and search
	mq.mutex.RLock()

	worker, exists := mq.workers[workerID]
	if !exists {
		mq.mutex.RUnlock()
		glog.V(2).Infof("Task assignment failed for worker %s: worker not registered", workerID)
		return nil
	}

	// Check if worker has capacity
	if worker.CurrentLoad >= worker.MaxConcurrent {
		mq.mutex.RUnlock()
		glog.V(2).Infof("Task assignment failed for worker %s: at capacity (%d/%d)", workerID, worker.CurrentLoad, worker.MaxConcurrent)
		return nil
	}

	now := time.Now()
	var selectedTask *MaintenanceTask
	var selectedIndex int = -1

	// Find the next suitable task (using read lock)
	for i, task := range mq.pendingTasks {
		// Check if it's time to execute the task
		if task.ScheduledAt.After(now) {
			glog.V(3).Infof("Task %s skipped for worker %s: scheduled for future (%v)", task.ID, workerID, task.ScheduledAt)
			continue
		}

		// Check if worker can handle this task type
		if !mq.workerCanHandle(task.Type, capabilities) {
			glog.V(3).Infof("Task %s (%s) skipped for worker %s: capability mismatch (worker has: %v)", task.ID, task.Type, workerID, capabilities)
			continue
		}

		// Check if this task type needs a cooldown period
		if !mq.canScheduleTaskNow(task) {
			// Add detailed diagnostic information
			runningCount := mq.GetRunningTaskCount(task.Type)
			maxConcurrent := mq.getMaxConcurrentForTaskType(task.Type)
			glog.V(2).Infof("Task %s (%s) skipped for worker %s: scheduling constraints not met (running: %d, max: %d)",
				task.ID, task.Type, workerID, runningCount, maxConcurrent)
			continue
		}

		// Found a suitable task
		selectedTask = task
		selectedIndex = i
		break
	}

	// Release read lock
	mq.mutex.RUnlock()

	// If no task found, return nil
	if selectedTask == nil {
		glog.V(4).Infof("No suitable tasks available for worker %s (checked %d pending tasks)", workerID, len(mq.pendingTasks))
		return nil
	}

	// Now acquire write lock to actually assign the task
	mq.mutex.Lock()
	defer mq.mutex.Unlock()

	// Re-check that the task is still available (it might have been assigned to another worker)
	if selectedIndex >= len(mq.pendingTasks) || mq.pendingTasks[selectedIndex].ID != selectedTask.ID {
		glog.V(2).Infof("Task %s no longer available for worker %s: assigned to another worker", selectedTask.ID, workerID)
		return nil
	}

	// Record assignment history
	workerAddress := ""
	if worker, exists := mq.workers[workerID]; exists {
		workerAddress = worker.Address
	}

	// Create assignment record
	assignmentRecord := &TaskAssignmentRecord{
		WorkerID:      workerID,
		WorkerAddress: workerAddress,
		AssignedAt:    now,
		Reason:        "Task assigned to available worker",
	}

	// Initialize assignment history if nil
	if selectedTask.AssignmentHistory == nil {
		selectedTask.AssignmentHistory = make([]*TaskAssignmentRecord, 0)
	}
	selectedTask.AssignmentHistory = append(selectedTask.AssignmentHistory, assignmentRecord)

	// Assign the task
	selectedTask.Status = TaskStatusAssigned
	selectedTask.WorkerID = workerID
	selectedTask.StartedAt = &now

	// Notify ActiveTopology to reserve capacity (move from pending to assigned)
	if mq.integration != nil {
		if at := mq.integration.GetActiveTopology(); at != nil {
			if err := at.AssignTask(selectedTask.ID); err != nil {
				glog.Warningf("Failed to update ActiveTopology for task assignment %s: %v", selectedTask.ID, err)
			}
		}
	}

	// Remove from pending tasks
	mq.pendingTasks = append(mq.pendingTasks[:selectedIndex], mq.pendingTasks[selectedIndex+1:]...)

	// Update worker load
	if worker, exists := mq.workers[workerID]; exists {
		worker.CurrentLoad++
	}

	// Track pending operation
	mq.trackPendingOperation(selectedTask)

	// Save task state after assignment
	mq.saveTaskState(selectedTask)

	glog.Infof("Task assigned: %s (%s) → worker %s (volume %d, server %s)",
		selectedTask.ID, selectedTask.Type, workerID, selectedTask.VolumeID, selectedTask.Server)

	return selectedTask
}

// CompleteTask marks a task as completed
func (mq *MaintenanceQueue) CompleteTask(taskID string, error string) {
	mq.mutex.Lock()
	defer mq.mutex.Unlock()

	task, exists := mq.tasks[taskID]
	if !exists {
		glog.Warningf("Attempted to complete non-existent task: %s", taskID)
		return
	}

	// Notify ActiveTopology to release capacity (move from assigned to recent)
	// We do this for both success and failure cases to release the capacity
	if mq.integration != nil && mq.integration.GetActiveTopology() != nil {
		if task.Status == TaskStatusAssigned || task.Status == TaskStatusInProgress {
			// Ignore error as task might not be in ActiveTopology (e.g. after restart)
			_ = mq.integration.GetActiveTopology().CompleteTask(taskID)
		}
	}

	completedTime := time.Now()
	task.CompletedAt = &completedTime

	// Calculate task duration
	var duration time.Duration
	if task.StartedAt != nil {
		duration = completedTime.Sub(*task.StartedAt)
	}

	if error != "" {
		task.Status = TaskStatusFailed
		task.Error = error

		// Check if task should be retried
		if task.RetryCount < task.MaxRetries {
			// Record unassignment due to failure/retry
			if task.WorkerID != "" && len(task.AssignmentHistory) > 0 {
				lastAssignment := task.AssignmentHistory[len(task.AssignmentHistory)-1]
				if lastAssignment.UnassignedAt == nil {
					unassignedTime := completedTime
					lastAssignment.UnassignedAt = &unassignedTime
					lastAssignment.Reason = fmt.Sprintf("Task failed, scheduling retry (attempt %d/%d): %s",
						task.RetryCount+1, task.MaxRetries, error)
				}
			}

			task.RetryCount++
			task.Status = TaskStatusPending
			task.WorkerID = ""
			task.StartedAt = nil
			task.CompletedAt = nil
			task.Error = ""
			task.ScheduledAt = time.Now().Add(15 * time.Minute) // Retry delay

			mq.pendingTasks = append(mq.pendingTasks, task)

			// Resync with ActiveTopology (re-add as pending)
			if mq.integration != nil {
				mq.integration.SyncTask(task)
			}

			// Save task state after retry setup
			mq.saveTaskState(task)
			glog.Warningf("Task failed, scheduling retry: %s (%s) attempt %d/%d, worker %s, duration %v, error: %s",
				taskID, task.Type, task.RetryCount, task.MaxRetries, task.WorkerID, duration, error)
		} else {
			// Record unassignment due to permanent failure
			if task.WorkerID != "" && len(task.AssignmentHistory) > 0 {
				lastAssignment := task.AssignmentHistory[len(task.AssignmentHistory)-1]
				if lastAssignment.UnassignedAt == nil {
					unassignedTime := completedTime
					lastAssignment.UnassignedAt = &unassignedTime
					lastAssignment.Reason = fmt.Sprintf("Task failed permanently after %d retries: %s", task.MaxRetries, error)
				}
			}

			// Save task state after permanent failure
			mq.saveTaskState(task)
			glog.Errorf("Task failed permanently: %s (%s) worker %s, duration %v, after %d retries: %s",
				taskID, task.Type, task.WorkerID, duration, task.MaxRetries, error)
		}
	} else {
		task.Status = TaskStatusCompleted
		task.Progress = 100
		// Save task state after successful completion
		mq.saveTaskState(task)
		glog.Infof("Task completed: %s (%s) worker %s, duration %v, volume %d",
			taskID, task.Type, task.WorkerID, duration, task.VolumeID)
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

	// Remove pending operation (unless it's being retried)
	if task.Status != TaskStatusPending {
		mq.removePendingOperation(taskID)
	}

	// Periodically cleanup old completed tasks (every 10th completion)
	if task.Status == TaskStatusCompleted {
		// Simple counter-based trigger for cleanup
		if len(mq.tasks)%10 == 0 {
			go mq.cleanupCompletedTasks()
		}
	}
}

// UpdateTaskProgress updates the progress of a running task
func (mq *MaintenanceQueue) UpdateTaskProgress(taskID string, progress float64) {
	mq.mutex.RLock()
	defer mq.mutex.RUnlock()

	if task, exists := mq.tasks[taskID]; exists {
		oldProgress := task.Progress
		task.Progress = progress
		task.Status = TaskStatusInProgress

		// Update pending operation status
		mq.updatePendingOperationStatus(taskID, "in_progress")

		// Log progress at significant milestones or changes
		if progress == 0 {
			glog.V(1).Infof("Task started: %s (%s) worker %s, volume %d",
				taskID, task.Type, task.WorkerID, task.VolumeID)
		} else if progress >= 100 {
			glog.V(1).Infof("Task progress: %s (%s) worker %s, %.1f%% complete",
				taskID, task.Type, task.WorkerID, progress)
		} else if progress-oldProgress >= 25 { // Log every 25% increment
			glog.V(1).Infof("Task progress: %s (%s) worker %s, %.1f%% complete",
				taskID, task.Type, task.WorkerID, progress)
		}

		// Save task state after progress update
		if progress == 0 || progress >= 100 || progress-oldProgress >= 10 {
			mq.saveTaskState(task)
		}
	} else {
		glog.V(2).Infof("Progress update for unknown task: %s (%.1f%%)", taskID, progress)
	}
}

// RegisterWorker registers a new worker
func (mq *MaintenanceQueue) RegisterWorker(worker *MaintenanceWorker) {
	mq.mutex.Lock()
	defer mq.mutex.Unlock()

	isNewWorker := true
	if existingWorker, exists := mq.workers[worker.ID]; exists {
		isNewWorker = false
		glog.Infof("Worker reconnected: %s at %s (capabilities: %v, max concurrent: %d)",
			worker.ID, worker.Address, worker.Capabilities, worker.MaxConcurrent)

		// Preserve current load when reconnecting
		worker.CurrentLoad = existingWorker.CurrentLoad
	} else {
		glog.Infof("Worker registered: %s at %s (capabilities: %v, max concurrent: %d)",
			worker.ID, worker.Address, worker.Capabilities, worker.MaxConcurrent)
	}

	worker.LastHeartbeat = time.Now()
	worker.Status = "active"
	if isNewWorker {
		worker.CurrentLoad = 0
	}
	mq.workers[worker.ID] = worker
}

// UpdateWorkerHeartbeat updates worker heartbeat
func (mq *MaintenanceQueue) UpdateWorkerHeartbeat(workerID string) {
	mq.mutex.Lock()
	defer mq.mutex.Unlock()

	if worker, exists := mq.workers[workerID]; exists {
		lastSeen := worker.LastHeartbeat
		worker.LastHeartbeat = time.Now()

		// Log if worker was offline for a while
		if time.Since(lastSeen) > 2*time.Minute {
			glog.Infof("Worker %s heartbeat resumed after %v", workerID, time.Since(lastSeen))
		}
	} else {
		glog.V(2).Infof("Heartbeat from unknown worker: %s", workerID)
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
		repeatIntervalHours := GetRepeatInterval(mq.policy, taskType)
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
	}

	// Sort based on status
	if status == TaskStatusCompleted || status == TaskStatusFailed || status == TaskStatusCancelled {
		sort.Slice(tasks, func(i, j int) bool {
			t1 := tasks[i].CompletedAt
			t2 := tasks[j].CompletedAt
			if t1 == nil && t2 == nil {
				return tasks[i].CreatedAt.After(tasks[j].CreatedAt)
			}
			if t1 == nil {
				return false
			}
			if t2 == nil {
				return true
			}
			return t1.After(*t2)
		})
	} else {
		// Default to creation time (newest first)
		sort.Slice(tasks, func(i, j int) bool {
			return tasks[i].CreatedAt.After(tasks[j].CreatedAt)
		})
	}

	// Apply limit after sorting
	if limit > 0 && len(tasks) > limit {
		tasks = tasks[:limit]
	}

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
	randBytes := make([]byte, 8)

	// Generate random bytes
	if _, err := rand.Read(randBytes); err != nil {
		// Fallback to timestamp-based ID if crypto/rand fails
		timestamp := time.Now().UnixNano()
		return fmt.Sprintf("task-%d", timestamp)
	}

	// Convert random bytes to charset
	for i := range b {
		b[i] = charset[int(randBytes[i])%len(charset)]
	}

	// Add timestamp suffix to ensure uniqueness
	timestamp := time.Now().Unix() % 10000 // last 4 digits of timestamp
	return fmt.Sprintf("%s-%04d", string(b), timestamp)
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
			// Mark any assigned tasks as failed and record unassignment
			for _, task := range mq.tasks {
				if task.WorkerID == id && (task.Status == TaskStatusAssigned || task.Status == TaskStatusInProgress) {
					// Record unassignment due to worker becoming unavailable
					if len(task.AssignmentHistory) > 0 {
						lastAssignment := task.AssignmentHistory[len(task.AssignmentHistory)-1]
						if lastAssignment.UnassignedAt == nil {
							unassignedTime := time.Now()
							lastAssignment.UnassignedAt = &unassignedTime
							lastAssignment.Reason = "Worker became unavailable (stale heartbeat)"
						}
					}

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
	glog.V(2).Infof("Checking if task %s (type: %s) can be scheduled", task.ID, task.Type)

	// TEMPORARY FIX: Skip integration task scheduler which is being overly restrictive
	// Use fallback logic directly for now
	glog.V(2).Infof("Using fallback logic for task scheduling")
	canExecute := mq.canExecuteTaskType(task.Type)
	glog.V(2).Infof("Fallback decision for task %s: %v", task.ID, canExecute)
	return canExecute

	// NOTE: Original integration code disabled temporarily
	// Try task scheduling logic first
	/*
		if mq.integration != nil {
			glog.Infof("DEBUG canScheduleTaskNow: Using integration task scheduler")
			// Get all running tasks and available workers
			runningTasks := mq.getRunningTasks()
			availableWorkers := mq.getAvailableWorkers()

			glog.Infof("DEBUG canScheduleTaskNow: Running tasks: %d, Available workers: %d", len(runningTasks), len(availableWorkers))

			canSchedule := mq.integration.CanScheduleWithTaskSchedulers(task, runningTasks, availableWorkers)
			glog.Infof("DEBUG canScheduleTaskNow: Task scheduler decision for task %s (%s): %v", task.ID, task.Type, canSchedule)
			return canSchedule
		}
	*/
}

// canExecuteTaskType checks if we can execute more tasks of this type (concurrency limits) - fallback logic
func (mq *MaintenanceQueue) canExecuteTaskType(taskType MaintenanceTaskType) bool {
	runningCount := mq.GetRunningTaskCount(taskType)
	maxConcurrent := mq.getMaxConcurrentForTaskType(taskType)

	canExecute := runningCount < maxConcurrent
	glog.V(3).Infof("canExecuteTaskType for %s: running=%d, max=%d, canExecute=%v", taskType, runningCount, maxConcurrent, canExecute)

	return canExecute
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
		maxConcurrent := GetMaxConcurrent(mq.policy, taskType)
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

// trackPendingOperation adds a task to the pending operations tracker
func (mq *MaintenanceQueue) trackPendingOperation(task *MaintenanceTask) {
	if mq.integration == nil {
		return
	}

	pendingOps := mq.integration.GetPendingOperations()
	if pendingOps == nil {
		return
	}

	// Skip tracking for tasks without proper typed parameters
	if task.TypedParams == nil {
		glog.V(2).Infof("Skipping pending operation tracking for task %s - no typed parameters", task.ID)
		return
	}

	// Map maintenance task type to pending operation type
	var opType PendingOperationType
	switch task.Type {
	case MaintenanceTaskType("balance"):
		opType = OpTypeVolumeBalance
	case MaintenanceTaskType("erasure_coding"):
		opType = OpTypeErasureCoding
	case MaintenanceTaskType("vacuum"):
		opType = OpTypeVacuum
	case MaintenanceTaskType("replication"):
		opType = OpTypeReplication
	default:
		opType = OpTypeVolumeMove
	}

	// Determine destination node and estimated size from unified targets
	destNode := ""
	estimatedSize := uint64(1024 * 1024 * 1024) // Default 1GB estimate

	// Use unified targets array - the only source of truth
	if len(task.TypedParams.Targets) > 0 {
		destNode = task.TypedParams.Targets[0].Node
		if task.TypedParams.Targets[0].EstimatedSize > 0 {
			estimatedSize = task.TypedParams.Targets[0].EstimatedSize
		}
	}

	// Determine source node from unified sources
	sourceNode := ""
	if len(task.TypedParams.Sources) > 0 {
		sourceNode = task.TypedParams.Sources[0].Node
	}

	operation := &PendingOperation{
		VolumeID:      task.VolumeID,
		OperationType: opType,
		SourceNode:    sourceNode,
		DestNode:      destNode,
		TaskID:        task.ID,
		StartTime:     time.Now(),
		EstimatedSize: estimatedSize,
		Collection:    task.Collection,
		Status:        "assigned",
	}

	pendingOps.AddOperation(operation)
}

// removePendingOperation removes a task from the pending operations tracker
func (mq *MaintenanceQueue) removePendingOperation(taskID string) {
	if mq.integration == nil {
		return
	}

	pendingOps := mq.integration.GetPendingOperations()
	if pendingOps == nil {
		return
	}

	pendingOps.RemoveOperation(taskID)
}

// updatePendingOperationStatus updates the status of a pending operation
func (mq *MaintenanceQueue) updatePendingOperationStatus(taskID string, status string) {
	if mq.integration == nil {
		return
	}

	pendingOps := mq.integration.GetPendingOperations()
	if pendingOps == nil {
		return
	}

	pendingOps.UpdateOperationStatus(taskID, status)
}
