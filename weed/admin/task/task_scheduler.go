package task

import (
	"sync"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/worker/types"
)

// TaskScheduler handles task assignment to workers
type TaskScheduler struct {
	workerRegistry *WorkerRegistry
	taskQueue      *PriorityTaskQueue
	mutex          sync.RWMutex
}

// NewTaskScheduler creates a new task scheduler
func NewTaskScheduler(registry *WorkerRegistry, queue *PriorityTaskQueue) *TaskScheduler {
	return &TaskScheduler{
		workerRegistry: registry,
		taskQueue:      queue,
	}
}

// GetNextTask gets the next suitable task for a worker
func (ts *TaskScheduler) GetNextTask(workerID string, capabilities []types.TaskType) *types.Task {
	ts.mutex.RLock()
	defer ts.mutex.RUnlock()

	// Get worker info
	_, exists := ts.workerRegistry.GetWorker(workerID)
	if !exists {
		return nil
	}

	// Check worker capabilities
	capabilityMap := make(map[types.TaskType]bool)
	for _, cap := range capabilities {
		capabilityMap[cap] = true
	}

	// Find next suitable task
	tasks := ts.taskQueue.GetTasks()
	for _, task := range tasks {
		// Check if worker can handle this task type
		if !capabilityMap[task.Type] {
			continue
		}

		// Check if task is ready to be scheduled
		if !task.ScheduledAt.IsZero() && task.ScheduledAt.After(time.Now()) {
			continue
		}

		// Additional checks can be added here
		// (e.g., server affinity, resource requirements)

		return task
	}

	return nil
}

// SelectWorker selects the best worker for a task
func (ts *TaskScheduler) SelectWorker(task *types.Task, availableWorkers []*types.Worker) *types.Worker {
	ts.mutex.RLock()
	defer ts.mutex.RUnlock()

	var bestWorker *types.Worker
	bestScore := -1.0

	for _, worker := range availableWorkers {
		// Check if worker supports this task type
		if !ts.workerSupportsTask(worker, task.Type) {
			continue
		}

		// Calculate selection score
		score := ts.calculateSelectionScore(worker, task)
		if bestWorker == nil || score > bestScore {
			bestWorker = worker
			bestScore = score
		}
	}

	if bestWorker != nil {
		glog.V(2).Infof("Selected worker %s for task %s (score: %.2f)", bestWorker.ID, task.Type, bestScore)
	}

	return bestWorker
}

// workerSupportsTask checks if a worker supports a task type
func (ts *TaskScheduler) workerSupportsTask(worker *types.Worker, taskType types.TaskType) bool {
	for _, capability := range worker.Capabilities {
		if capability == taskType {
			return true
		}
	}
	return false
}

// calculateSelectionScore calculates a score for worker selection
func (ts *TaskScheduler) calculateSelectionScore(worker *types.Worker, task *types.Task) float64 {
	// Base score from worker registry
	baseScore := ts.workerRegistry.calculateWorkerScore(worker)

	// Task-specific adjustments
	taskScore := baseScore

	// Priority adjustment
	switch task.Priority {
	case types.TaskPriorityHigh:
		taskScore *= 1.2 // Prefer high-performing workers for high-priority tasks
	case types.TaskPriorityLow:
		taskScore *= 0.9 // Low-priority tasks can use any available worker
	}

	// Server affinity bonus (if worker and volume are on same server)
	if task.Server != "" && worker.Address == task.Server {
		taskScore += 0.1
	}

	// Retry penalty (prefer different workers for retried tasks)
	if task.RetryCount > 0 {
		taskScore *= 0.8
	}

	return taskScore
}

// PriorityTaskQueue implements a priority queue for tasks
type PriorityTaskQueue struct {
	tasks []*types.Task
	mutex sync.RWMutex
}

// NewPriorityTaskQueue creates a new priority task queue
func NewPriorityTaskQueue() *PriorityTaskQueue {
	return &PriorityTaskQueue{
		tasks: make([]*types.Task, 0),
	}
}

// Push adds a task to the queue
func (ptq *PriorityTaskQueue) Push(task *types.Task) {
	ptq.mutex.Lock()
	defer ptq.mutex.Unlock()

	// Insert task in priority order (highest priority first)
	inserted := false
	for i, existingTask := range ptq.tasks {
		if task.Priority > existingTask.Priority {
			// Insert at position i
			ptq.tasks = append(ptq.tasks[:i], append([]*types.Task{task}, ptq.tasks[i:]...)...)
			inserted = true
			break
		}
	}

	if !inserted {
		// Add to end
		ptq.tasks = append(ptq.tasks, task)
	}

	glog.V(3).Infof("Added task %s to queue (priority: %d, queue size: %d)", task.ID, task.Priority, len(ptq.tasks))
}

// Pop removes and returns the highest priority task
func (ptq *PriorityTaskQueue) Pop() *types.Task {
	ptq.mutex.Lock()
	defer ptq.mutex.Unlock()

	if len(ptq.tasks) == 0 {
		return nil
	}

	task := ptq.tasks[0]
	ptq.tasks = ptq.tasks[1:]
	return task
}

// Peek returns the highest priority task without removing it
func (ptq *PriorityTaskQueue) Peek() *types.Task {
	ptq.mutex.RLock()
	defer ptq.mutex.RUnlock()

	if len(ptq.tasks) == 0 {
		return nil
	}

	return ptq.tasks[0]
}

// IsEmpty returns true if the queue is empty
func (ptq *PriorityTaskQueue) IsEmpty() bool {
	ptq.mutex.RLock()
	defer ptq.mutex.RUnlock()

	return len(ptq.tasks) == 0
}

// Size returns the number of tasks in the queue
func (ptq *PriorityTaskQueue) Size() int {
	ptq.mutex.RLock()
	defer ptq.mutex.RUnlock()

	return len(ptq.tasks)
}

// HasTask checks if a task exists for a volume and task type
func (ptq *PriorityTaskQueue) HasTask(volumeID uint32, taskType types.TaskType) bool {
	ptq.mutex.RLock()
	defer ptq.mutex.RUnlock()

	for _, task := range ptq.tasks {
		if task.VolumeID == volumeID && task.Type == taskType {
			return true
		}
	}
	return false
}

// GetTasks returns a copy of all tasks in the queue
func (ptq *PriorityTaskQueue) GetTasks() []*types.Task {
	ptq.mutex.RLock()
	defer ptq.mutex.RUnlock()

	tasksCopy := make([]*types.Task, len(ptq.tasks))
	copy(tasksCopy, ptq.tasks)
	return tasksCopy
}

// RemoveTask removes a specific task from the queue
func (ptq *PriorityTaskQueue) RemoveTask(taskID string) bool {
	ptq.mutex.Lock()
	defer ptq.mutex.Unlock()

	for i, task := range ptq.tasks {
		if task.ID == taskID {
			ptq.tasks = append(ptq.tasks[:i], ptq.tasks[i+1:]...)
			glog.V(3).Infof("Removed task %s from queue", taskID)
			return true
		}
	}
	return false
}

// Clear removes all tasks from the queue
func (ptq *PriorityTaskQueue) Clear() {
	ptq.mutex.Lock()
	defer ptq.mutex.Unlock()

	ptq.tasks = ptq.tasks[:0]
	glog.V(3).Infof("Cleared task queue")
}
