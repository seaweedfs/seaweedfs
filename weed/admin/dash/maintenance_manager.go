package dash

import (
	"fmt"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
)

// MaintenanceManager coordinates the maintenance system
type MaintenanceManager struct {
	config      *MaintenanceConfig
	scanner     *MaintenanceScanner
	queue       *MaintenanceQueue
	adminServer *AdminServer
	running     bool
	stopChan    chan struct{}
}

// NewMaintenanceManager creates a new maintenance manager
func NewMaintenanceManager(adminServer *AdminServer, config *MaintenanceConfig) *MaintenanceManager {
	if config == nil {
		config = DefaultMaintenanceConfig()
	}

	queue := NewMaintenanceQueue(config.Policy)
	scanner := NewMaintenanceScanner(adminServer, config.Policy, queue)

	return &MaintenanceManager{
		config:      config,
		scanner:     scanner,
		queue:       queue,
		adminServer: adminServer,
		stopChan:    make(chan struct{}),
	}
}

// Start begins the maintenance manager
func (mm *MaintenanceManager) Start() error {
	if !mm.config.Enabled {
		glog.V(1).Infof("Maintenance system is disabled")
		return nil
	}

	mm.running = true

	// Start background processes
	go mm.scanLoop()
	go mm.cleanupLoop()

	glog.Infof("Maintenance manager started with scan interval %v", mm.config.ScanInterval)
	return nil
}

// Stop terminates the maintenance manager
func (mm *MaintenanceManager) Stop() {
	mm.running = false
	close(mm.stopChan)
	glog.Infof("Maintenance manager stopped")
}

// scanLoop periodically scans for maintenance tasks
func (mm *MaintenanceManager) scanLoop() {
	ticker := time.NewTicker(mm.config.ScanInterval)
	defer ticker.Stop()

	for mm.running {
		select {
		case <-mm.stopChan:
			return
		case <-ticker.C:
			mm.performScan()
		}
	}
}

// cleanupLoop periodically cleans up old tasks and stale workers
func (mm *MaintenanceManager) cleanupLoop() {
	ticker := time.NewTicker(mm.config.CleanupInterval)
	defer ticker.Stop()

	for mm.running {
		select {
		case <-mm.stopChan:
			return
		case <-ticker.C:
			mm.performCleanup()
		}
	}
}

// performScan executes a maintenance scan
func (mm *MaintenanceManager) performScan() {
	glog.V(2).Infof("Starting maintenance scan")

	results, err := mm.scanner.ScanForMaintenanceTasks()
	if err != nil {
		glog.Errorf("Maintenance scan failed: %v", err)
		return
	}

	if len(results) > 0 {
		mm.queue.AddTasksFromResults(results)
		glog.V(1).Infof("Maintenance scan completed: added %d tasks", len(results))
	} else {
		glog.V(2).Infof("Maintenance scan completed: no tasks needed")
	}
}

// performCleanup cleans up old tasks and stale workers
func (mm *MaintenanceManager) performCleanup() {
	glog.V(2).Infof("Starting maintenance cleanup")

	removedTasks := mm.queue.CleanupOldTasks(mm.config.TaskRetention)
	removedWorkers := mm.queue.RemoveStaleWorkers(mm.config.WorkerTimeout)

	if removedTasks > 0 || removedWorkers > 0 {
		glog.V(1).Infof("Cleanup completed: removed %d old tasks and %d stale workers", removedTasks, removedWorkers)
	}
}

// GetQueue returns the maintenance queue
func (mm *MaintenanceManager) GetQueue() *MaintenanceQueue {
	return mm.queue
}

// GetConfig returns the maintenance configuration
func (mm *MaintenanceManager) GetConfig() *MaintenanceConfig {
	return mm.config
}

// GetStats returns maintenance statistics
func (mm *MaintenanceManager) GetStats() *MaintenanceStats {
	stats := mm.queue.GetStats()
	stats.LastScanTime = time.Now() // Would need to track this properly
	stats.NextScanTime = time.Now().Add(mm.config.ScanInterval)
	return stats
}

// GetTasks returns tasks with filtering
func (mm *MaintenanceManager) GetTasks(status MaintenanceTaskStatus, taskType MaintenanceTaskType, limit int) []*MaintenanceTask {
	return mm.queue.GetTasks(status, taskType, limit)
}

// GetWorkers returns all registered workers
func (mm *MaintenanceManager) GetWorkers() []*MaintenanceWorker {
	return mm.queue.GetWorkers()
}

// TriggerScan manually triggers a maintenance scan
func (mm *MaintenanceManager) TriggerScan() error {
	if !mm.running {
		return fmt.Errorf("maintenance manager is not running")
	}

	go mm.performScan()
	return nil
}

// UpdateConfig updates the maintenance configuration
func (mm *MaintenanceManager) UpdateConfig(config *MaintenanceConfig) error {
	if config == nil {
		return fmt.Errorf("config cannot be nil")
	}

	mm.config = config
	mm.queue.policy = config.Policy
	mm.scanner.policy = config.Policy

	glog.V(1).Infof("Maintenance configuration updated")
	return nil
}

// CancelTask cancels a pending task
func (mm *MaintenanceManager) CancelTask(taskID string) error {
	mm.queue.mutex.Lock()
	defer mm.queue.mutex.Unlock()

	task, exists := mm.queue.tasks[taskID]
	if !exists {
		return fmt.Errorf("task %s not found", taskID)
	}

	if task.Status == TaskStatusPending {
		task.Status = TaskStatusCancelled
		task.CompletedAt = &[]time.Time{time.Now()}[0]

		// Remove from pending tasks
		for i, pendingTask := range mm.queue.pendingTasks {
			if pendingTask.ID == taskID {
				mm.queue.pendingTasks = append(mm.queue.pendingTasks[:i], mm.queue.pendingTasks[i+1:]...)
				break
			}
		}

		glog.V(2).Infof("Cancelled task %s", taskID)
		return nil
	}

	return fmt.Errorf("task %s cannot be cancelled (status: %s)", taskID, task.Status)
}

// RegisterWorker registers a new worker
func (mm *MaintenanceManager) RegisterWorker(worker *MaintenanceWorker) {
	mm.queue.RegisterWorker(worker)
}

// GetNextTask returns the next task for a worker
func (mm *MaintenanceManager) GetNextTask(workerID string, capabilities []MaintenanceTaskType) *MaintenanceTask {
	return mm.queue.GetNextTask(workerID, capabilities)
}

// CompleteTask marks a task as completed
func (mm *MaintenanceManager) CompleteTask(taskID string, error string) {
	mm.queue.CompleteTask(taskID, error)
}

// UpdateTaskProgress updates task progress
func (mm *MaintenanceManager) UpdateTaskProgress(taskID string, progress float64) {
	mm.queue.UpdateTaskProgress(taskID, progress)
}

// UpdateWorkerHeartbeat updates worker heartbeat
func (mm *MaintenanceManager) UpdateWorkerHeartbeat(workerID string) {
	mm.queue.UpdateWorkerHeartbeat(workerID)
}

// Add maintenance manager to AdminServer
func (s *AdminServer) InitMaintenanceManager(config *MaintenanceConfig) {
	s.maintenanceManager = NewMaintenanceManager(s, config)
}

// GetMaintenanceManager returns the maintenance manager
func (s *AdminServer) GetMaintenanceManager() *MaintenanceManager {
	return s.maintenanceManager
}

// StartMaintenanceManager starts the maintenance manager
func (s *AdminServer) StartMaintenanceManager() error {
	if s.maintenanceManager == nil {
		s.InitMaintenanceManager(nil)
	}
	return s.maintenanceManager.Start()
}

// StopMaintenanceManager stops the maintenance manager
func (s *AdminServer) StopMaintenanceManager() {
	if s.maintenanceManager != nil {
		s.maintenanceManager.Stop()
	}
}
