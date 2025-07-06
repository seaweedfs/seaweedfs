package worker

import (
	"fmt"
	"sync"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/worker/types"
)

// Registry manages workers and their statistics
type Registry struct {
	workers map[string]*types.Worker
	stats   *types.RegistryStats
	mutex   sync.RWMutex
}

// NewRegistry creates a new worker registry
func NewRegistry() *Registry {
	return &Registry{
		workers: make(map[string]*types.Worker),
		stats: &types.RegistryStats{
			TotalWorkers:   0,
			ActiveWorkers:  0,
			BusyWorkers:    0,
			IdleWorkers:    0,
			TotalTasks:     0,
			CompletedTasks: 0,
			FailedTasks:    0,
			StartTime:      time.Now(),
		},
	}
}

// RegisterWorker registers a new worker
func (r *Registry) RegisterWorker(worker *types.Worker) error {
	r.mutex.Lock()
	defer r.mutex.Unlock()

	if _, exists := r.workers[worker.ID]; exists {
		return fmt.Errorf("worker %s already registered", worker.ID)
	}

	r.workers[worker.ID] = worker
	r.updateStats()
	return nil
}

// UnregisterWorker removes a worker from the registry
func (r *Registry) UnregisterWorker(workerID string) error {
	r.mutex.Lock()
	defer r.mutex.Unlock()

	if _, exists := r.workers[workerID]; !exists {
		return fmt.Errorf("worker %s not found", workerID)
	}

	delete(r.workers, workerID)
	r.updateStats()
	return nil
}

// GetWorker returns a worker by ID
func (r *Registry) GetWorker(workerID string) (*types.Worker, bool) {
	r.mutex.RLock()
	defer r.mutex.RUnlock()

	worker, exists := r.workers[workerID]
	return worker, exists
}

// ListWorkers returns all registered workers
func (r *Registry) ListWorkers() []*types.Worker {
	r.mutex.RLock()
	defer r.mutex.RUnlock()

	workers := make([]*types.Worker, 0, len(r.workers))
	for _, worker := range r.workers {
		workers = append(workers, worker)
	}
	return workers
}

// GetWorkersByCapability returns workers that support a specific capability
func (r *Registry) GetWorkersByCapability(capability types.TaskType) []*types.Worker {
	r.mutex.RLock()
	defer r.mutex.RUnlock()

	var workers []*types.Worker
	for _, worker := range r.workers {
		for _, cap := range worker.Capabilities {
			if cap == capability {
				workers = append(workers, worker)
				break
			}
		}
	}
	return workers
}

// GetAvailableWorkers returns workers that are available for new tasks
func (r *Registry) GetAvailableWorkers() []*types.Worker {
	r.mutex.RLock()
	defer r.mutex.RUnlock()

	var workers []*types.Worker
	for _, worker := range r.workers {
		if worker.Status == "active" && worker.CurrentLoad < worker.MaxConcurrent {
			workers = append(workers, worker)
		}
	}
	return workers
}

// GetBestWorkerForTask returns the best worker for a specific task
func (r *Registry) GetBestWorkerForTask(taskType types.TaskType) *types.Worker {
	r.mutex.RLock()
	defer r.mutex.RUnlock()

	var bestWorker *types.Worker
	var bestScore float64

	for _, worker := range r.workers {
		// Check if worker supports this task type
		supportsTask := false
		for _, cap := range worker.Capabilities {
			if cap == taskType {
				supportsTask = true
				break
			}
		}

		if !supportsTask {
			continue
		}

		// Check if worker is available
		if worker.Status != "active" || worker.CurrentLoad >= worker.MaxConcurrent {
			continue
		}

		// Calculate score based on current load and capacity
		score := float64(worker.MaxConcurrent-worker.CurrentLoad) / float64(worker.MaxConcurrent)
		if bestWorker == nil || score > bestScore {
			bestWorker = worker
			bestScore = score
		}
	}

	return bestWorker
}

// UpdateWorkerHeartbeat updates the last heartbeat time for a worker
func (r *Registry) UpdateWorkerHeartbeat(workerID string) error {
	r.mutex.Lock()
	defer r.mutex.Unlock()

	worker, exists := r.workers[workerID]
	if !exists {
		return fmt.Errorf("worker %s not found", workerID)
	}

	worker.LastHeartbeat = time.Now()
	return nil
}

// UpdateWorkerLoad updates the current load for a worker
func (r *Registry) UpdateWorkerLoad(workerID string, load int) error {
	r.mutex.Lock()
	defer r.mutex.Unlock()

	worker, exists := r.workers[workerID]
	if !exists {
		return fmt.Errorf("worker %s not found", workerID)
	}

	worker.CurrentLoad = load
	if load >= worker.MaxConcurrent {
		worker.Status = "busy"
	} else {
		worker.Status = "active"
	}

	r.updateStats()
	return nil
}

// UpdateWorkerStatus updates the status of a worker
func (r *Registry) UpdateWorkerStatus(workerID string, status string) error {
	r.mutex.Lock()
	defer r.mutex.Unlock()

	worker, exists := r.workers[workerID]
	if !exists {
		return fmt.Errorf("worker %s not found", workerID)
	}

	worker.Status = status
	r.updateStats()
	return nil
}

// CleanupStaleWorkers removes workers that haven't sent heartbeats recently
func (r *Registry) CleanupStaleWorkers(timeout time.Duration) int {
	r.mutex.Lock()
	defer r.mutex.Unlock()

	var removedCount int
	cutoff := time.Now().Add(-timeout)

	for workerID, worker := range r.workers {
		if worker.LastHeartbeat.Before(cutoff) {
			delete(r.workers, workerID)
			removedCount++
		}
	}

	if removedCount > 0 {
		r.updateStats()
	}

	return removedCount
}

// GetStats returns current registry statistics
func (r *Registry) GetStats() *types.RegistryStats {
	r.mutex.RLock()
	defer r.mutex.RUnlock()

	// Create a copy of the stats to avoid race conditions
	stats := *r.stats
	return &stats
}

// updateStats updates the registry statistics (must be called with lock held)
func (r *Registry) updateStats() {
	r.stats.TotalWorkers = len(r.workers)
	r.stats.ActiveWorkers = 0
	r.stats.BusyWorkers = 0
	r.stats.IdleWorkers = 0

	for _, worker := range r.workers {
		switch worker.Status {
		case "active":
			if worker.CurrentLoad > 0 {
				r.stats.ActiveWorkers++
			} else {
				r.stats.IdleWorkers++
			}
		case "busy":
			r.stats.BusyWorkers++
		}
	}

	r.stats.Uptime = time.Since(r.stats.StartTime)
	r.stats.LastUpdated = time.Now()
}

// GetTaskCapabilities returns all task capabilities available in the registry
func (r *Registry) GetTaskCapabilities() []types.TaskType {
	r.mutex.RLock()
	defer r.mutex.RUnlock()

	capabilitySet := make(map[types.TaskType]bool)
	for _, worker := range r.workers {
		for _, cap := range worker.Capabilities {
			capabilitySet[cap] = true
		}
	}

	var capabilities []types.TaskType
	for cap := range capabilitySet {
		capabilities = append(capabilities, cap)
	}

	return capabilities
}

// GetWorkersByStatus returns workers filtered by status
func (r *Registry) GetWorkersByStatus(status string) []*types.Worker {
	r.mutex.RLock()
	defer r.mutex.RUnlock()

	var workers []*types.Worker
	for _, worker := range r.workers {
		if worker.Status == status {
			workers = append(workers, worker)
		}
	}
	return workers
}

// GetWorkerCount returns the total number of registered workers
func (r *Registry) GetWorkerCount() int {
	r.mutex.RLock()
	defer r.mutex.RUnlock()
	return len(r.workers)
}

// GetWorkerIDs returns all worker IDs
func (r *Registry) GetWorkerIDs() []string {
	r.mutex.RLock()
	defer r.mutex.RUnlock()

	ids := make([]string, 0, len(r.workers))
	for id := range r.workers {
		ids = append(ids, id)
	}
	return ids
}

// GetWorkerSummary returns a summary of all workers
func (r *Registry) GetWorkerSummary() *types.WorkerSummary {
	r.mutex.RLock()
	defer r.mutex.RUnlock()

	summary := &types.WorkerSummary{
		TotalWorkers: len(r.workers),
		ByStatus:     make(map[string]int),
		ByCapability: make(map[types.TaskType]int),
		TotalLoad:    0,
		MaxCapacity:  0,
	}

	for _, worker := range r.workers {
		summary.ByStatus[worker.Status]++
		summary.TotalLoad += worker.CurrentLoad
		summary.MaxCapacity += worker.MaxConcurrent

		for _, cap := range worker.Capabilities {
			summary.ByCapability[cap]++
		}
	}

	return summary
}

// Default global registry instance
var defaultRegistry *Registry
var registryOnce sync.Once

// GetDefaultRegistry returns the default global registry
func GetDefaultRegistry() *Registry {
	registryOnce.Do(func() {
		defaultRegistry = NewRegistry()
	})
	return defaultRegistry
}
