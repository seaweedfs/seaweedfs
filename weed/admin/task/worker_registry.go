package task

import (
	"fmt"
	"sync"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/worker/types"
)

// WorkerRegistry manages worker registration and tracking
type WorkerRegistry struct {
	workers      map[string]*types.Worker
	capabilities map[types.TaskType][]*types.Worker
	metrics      map[string]*WorkerMetrics
	issues       map[string][]WorkerIssue
	mutex        sync.RWMutex
}

// WorkerIssue represents an issue with a worker
type WorkerIssue struct {
	Type      string
	Timestamp time.Time
	Details   string
}

// NewWorkerRegistry creates a new worker registry
func NewWorkerRegistry() *WorkerRegistry {
	return &WorkerRegistry{
		workers:      make(map[string]*types.Worker),
		capabilities: make(map[types.TaskType][]*types.Worker),
		metrics:      make(map[string]*WorkerMetrics),
		issues:       make(map[string][]WorkerIssue),
	}
}

// RegisterWorker registers a new worker
func (wr *WorkerRegistry) RegisterWorker(worker *types.Worker) error {
	wr.mutex.Lock()
	defer wr.mutex.Unlock()

	if _, exists := wr.workers[worker.ID]; exists {
		return fmt.Errorf("worker %s already registered", worker.ID)
	}

	// Register worker
	wr.workers[worker.ID] = worker

	// Initialize metrics
	wr.metrics[worker.ID] = &WorkerMetrics{
		TasksCompleted:  0,
		TasksFailed:     0,
		AverageTaskTime: 0,
		LastTaskTime:    time.Time{},
		SuccessRate:     1.0,
	}

	// Update capabilities mapping
	wr.updateCapabilitiesMapping()

	glog.Infof("Registered worker %s with capabilities: %v", worker.ID, worker.Capabilities)
	return nil
}

// UnregisterWorker removes a worker
func (wr *WorkerRegistry) UnregisterWorker(workerID string) error {
	wr.mutex.Lock()
	defer wr.mutex.Unlock()

	if _, exists := wr.workers[workerID]; !exists {
		return fmt.Errorf("worker %s not found", workerID)
	}

	delete(wr.workers, workerID)
	delete(wr.metrics, workerID)
	delete(wr.issues, workerID)

	// Update capabilities mapping
	wr.updateCapabilitiesMapping()

	glog.Infof("Unregistered worker %s", workerID)
	return nil
}

// GetWorker returns a worker by ID
func (wr *WorkerRegistry) GetWorker(workerID string) (*types.Worker, bool) {
	wr.mutex.RLock()
	defer wr.mutex.RUnlock()

	worker, exists := wr.workers[workerID]
	return worker, exists
}

// GetAvailableWorkers returns workers that are available for new tasks
func (wr *WorkerRegistry) GetAvailableWorkers() []*types.Worker {
	wr.mutex.RLock()
	defer wr.mutex.RUnlock()

	var available []*types.Worker
	for _, worker := range wr.workers {
		if worker.Status == "active" && worker.CurrentLoad < worker.MaxConcurrent {
			available = append(available, worker)
		}
	}
	return available
}

// GetWorkersByCapability returns workers that support a specific capability
func (wr *WorkerRegistry) GetWorkersByCapability(taskType types.TaskType) []*types.Worker {
	wr.mutex.RLock()
	defer wr.mutex.RUnlock()

	return wr.capabilities[taskType]
}

// UpdateWorkerHeartbeat updates worker heartbeat and status
func (wr *WorkerRegistry) UpdateWorkerHeartbeat(workerID string, status *types.WorkerStatus) error {
	wr.mutex.Lock()
	defer wr.mutex.Unlock()

	worker, exists := wr.workers[workerID]
	if !exists {
		return fmt.Errorf("worker %s not found", workerID)
	}

	// Update worker status
	worker.LastHeartbeat = time.Now()
	worker.Status = status.Status
	worker.CurrentLoad = status.CurrentLoad

	glog.V(3).Infof("Updated heartbeat for worker %s, status: %s, load: %d/%d",
		workerID, status.Status, status.CurrentLoad, worker.MaxConcurrent)
	return nil
}

// GetTimedOutWorkers returns workers that haven't sent heartbeat within timeout
func (wr *WorkerRegistry) GetTimedOutWorkers(timeout time.Duration) []string {
	wr.mutex.RLock()
	defer wr.mutex.RUnlock()

	var timedOut []string
	cutoff := time.Now().Add(-timeout)

	for workerID, worker := range wr.workers {
		if worker.LastHeartbeat.Before(cutoff) {
			timedOut = append(timedOut, workerID)
		}
	}

	return timedOut
}

// MarkWorkerInactive marks a worker as inactive
func (wr *WorkerRegistry) MarkWorkerInactive(workerID string) {
	wr.mutex.Lock()
	defer wr.mutex.Unlock()

	if worker, exists := wr.workers[workerID]; exists {
		worker.Status = "inactive"
		worker.CurrentLoad = 0
	}
}

// RecordWorkerIssue records an issue with a worker
func (wr *WorkerRegistry) RecordWorkerIssue(workerID string, issueType string) {
	wr.mutex.Lock()
	defer wr.mutex.Unlock()

	issue := WorkerIssue{
		Type:      issueType,
		Timestamp: time.Now(),
		Details:   fmt.Sprintf("Worker issue: %s", issueType),
	}

	wr.issues[workerID] = append(wr.issues[workerID], issue)

	// Limit issue history to last 10 issues
	if len(wr.issues[workerID]) > 10 {
		wr.issues[workerID] = wr.issues[workerID][1:]
	}

	glog.Warningf("Recorded issue for worker %s: %s", workerID, issueType)
}

// GetWorkerMetrics returns metrics for a worker
func (wr *WorkerRegistry) GetWorkerMetrics(workerID string) *WorkerMetrics {
	wr.mutex.RLock()
	defer wr.mutex.RUnlock()

	return wr.metrics[workerID]
}

// UpdateWorkerMetrics updates performance metrics for a worker
func (wr *WorkerRegistry) UpdateWorkerMetrics(workerID string, taskDuration time.Duration, success bool) {
	wr.mutex.Lock()
	defer wr.mutex.Unlock()

	metrics, exists := wr.metrics[workerID]
	if !exists {
		return
	}

	if success {
		metrics.TasksCompleted++
	} else {
		metrics.TasksFailed++
	}

	metrics.LastTaskTime = time.Now()

	// Update average task time
	totalTasks := metrics.TasksCompleted + metrics.TasksFailed
	if totalTasks > 0 {
		oldAvg := metrics.AverageTaskTime
		metrics.AverageTaskTime = time.Duration(
			(float64(oldAvg)*float64(totalTasks-1) + float64(taskDuration)) / float64(totalTasks),
		)
	}

	// Update success rate
	if totalTasks > 0 {
		metrics.SuccessRate = float64(metrics.TasksCompleted) / float64(totalTasks)
	}
}

// GetBestWorkerForTask returns the best worker for a specific task type
func (wr *WorkerRegistry) GetBestWorkerForTask(taskType types.TaskType) *types.Worker {
	wr.mutex.RLock()
	defer wr.mutex.RUnlock()

	candidates := wr.capabilities[taskType]
	if len(candidates) == 0 {
		return nil
	}

	var bestWorker *types.Worker
	bestScore := -1.0

	for _, worker := range candidates {
		// Skip if not available
		if worker.Status != "active" || worker.CurrentLoad >= worker.MaxConcurrent {
			continue
		}

		// Calculate score based on multiple factors
		score := wr.calculateWorkerScore(worker)
		if bestWorker == nil || score > bestScore {
			bestWorker = worker
			bestScore = score
		}
	}

	return bestWorker
}

// calculateWorkerScore calculates a score for worker selection
func (wr *WorkerRegistry) calculateWorkerScore(worker *types.Worker) float64 {
	metrics := wr.metrics[worker.ID]
	if metrics == nil {
		return 0.5 // Default score for new workers
	}

	// Factors for scoring:
	// 1. Available capacity (0.0 to 1.0)
	capacityScore := float64(worker.MaxConcurrent-worker.CurrentLoad) / float64(worker.MaxConcurrent)

	// 2. Success rate (0.0 to 1.0)
	successScore := metrics.SuccessRate

	// 3. Recent activity bonus (workers that completed tasks recently get slight bonus)
	activityScore := 0.0
	if !metrics.LastTaskTime.IsZero() && time.Since(metrics.LastTaskTime) < time.Hour {
		activityScore = 0.1
	}

	// 4. Issue penalty (workers with recent issues get penalty)
	issuePenalty := 0.0
	if issues, exists := wr.issues[worker.ID]; exists {
		recentIssues := 0
		cutoff := time.Now().Add(-time.Hour)
		for _, issue := range issues {
			if issue.Timestamp.After(cutoff) {
				recentIssues++
			}
		}
		issuePenalty = float64(recentIssues) * 0.1
	}

	// Weighted average
	score := (capacityScore*0.4 + successScore*0.4 + activityScore) - issuePenalty

	if score < 0 {
		score = 0
	}
	if score > 1 {
		score = 1
	}

	return score
}

// updateCapabilitiesMapping rebuilds the capabilities mapping
func (wr *WorkerRegistry) updateCapabilitiesMapping() {
	// Clear existing mapping
	for taskType := range wr.capabilities {
		wr.capabilities[taskType] = nil
	}

	// Rebuild mapping
	for _, worker := range wr.workers {
		for _, capability := range worker.Capabilities {
			wr.capabilities[capability] = append(wr.capabilities[capability], worker)
		}
	}
}

// GetRegistryStats returns statistics about the registry
func (wr *WorkerRegistry) GetRegistryStats() map[string]interface{} {
	wr.mutex.RLock()
	defer wr.mutex.RUnlock()

	stats := make(map[string]interface{})
	stats["total_workers"] = len(wr.workers)

	statusCounts := make(map[string]int)
	capabilityCounts := make(map[types.TaskType]int)
	totalLoad := 0
	maxCapacity := 0

	for _, worker := range wr.workers {
		statusCounts[worker.Status]++
		totalLoad += worker.CurrentLoad
		maxCapacity += worker.MaxConcurrent

		for _, capability := range worker.Capabilities {
			capabilityCounts[capability]++
		}
	}

	stats["by_status"] = statusCounts
	stats["by_capability"] = capabilityCounts
	stats["total_load"] = totalLoad
	stats["max_capacity"] = maxCapacity
	stats["utilization"] = float64(totalLoad) / float64(maxCapacity) * 100.0

	return stats
}
