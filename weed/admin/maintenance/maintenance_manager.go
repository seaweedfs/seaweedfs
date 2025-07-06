package maintenance

import (
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
)

// MaintenanceManager coordinates the maintenance system
type MaintenanceManager struct {
	config      *MaintenanceConfig
	scanner     *MaintenanceScanner
	queue       *MaintenanceQueue
	adminClient AdminClient
	running     bool
	stopChan    chan struct{}
	// Error handling and backoff
	errorCount    int
	lastError     error
	lastErrorTime time.Time
	backoffDelay  time.Duration
	mutex         sync.RWMutex
}

// NewMaintenanceManager creates a new maintenance manager
func NewMaintenanceManager(adminClient AdminClient, config *MaintenanceConfig) *MaintenanceManager {
	if config == nil {
		config = DefaultMaintenanceConfig()
	}

	queue := NewMaintenanceQueue(config.Policy)
	scanner := NewMaintenanceScanner(adminClient, config.Policy, queue)

	return &MaintenanceManager{
		config:       config,
		scanner:      scanner,
		queue:        queue,
		adminClient:  adminClient,
		stopChan:     make(chan struct{}),
		backoffDelay: time.Second, // Start with 1 second backoff
	}
}

// Start begins the maintenance manager
func (mm *MaintenanceManager) Start() error {
	if !mm.config.Enabled {
		glog.V(1).Infof("Maintenance system is disabled")
		return nil
	}

	// Validate configuration durations to prevent ticker panics
	if err := mm.validateConfig(); err != nil {
		return fmt.Errorf("invalid maintenance configuration: %v", err)
	}

	mm.running = true

	// Start background processes
	go mm.scanLoop()
	go mm.cleanupLoop()

	glog.Infof("Maintenance manager started with scan interval %ds", mm.config.ScanIntervalSeconds)
	return nil
}

// validateConfig validates the maintenance configuration durations
func (mm *MaintenanceManager) validateConfig() error {
	if mm.config.ScanIntervalSeconds <= 0 {
		glog.Warningf("Invalid scan interval %ds, using default 30m", mm.config.ScanIntervalSeconds)
		mm.config.ScanIntervalSeconds = 30 * 60 // 30 minutes in seconds
	}

	if mm.config.CleanupIntervalSeconds <= 0 {
		glog.Warningf("Invalid cleanup interval %ds, using default 24h", mm.config.CleanupIntervalSeconds)
		mm.config.CleanupIntervalSeconds = 24 * 60 * 60 // 24 hours in seconds
	}

	if mm.config.WorkerTimeoutSeconds <= 0 {
		glog.Warningf("Invalid worker timeout %ds, using default 5m", mm.config.WorkerTimeoutSeconds)
		mm.config.WorkerTimeoutSeconds = 5 * 60 // 5 minutes in seconds
	}

	if mm.config.TaskTimeoutSeconds <= 0 {
		glog.Warningf("Invalid task timeout %ds, using default 2h", mm.config.TaskTimeoutSeconds)
		mm.config.TaskTimeoutSeconds = 2 * 60 * 60 // 2 hours in seconds
	}

	if mm.config.RetryDelaySeconds <= 0 {
		glog.Warningf("Invalid retry delay %ds, using default 15m", mm.config.RetryDelaySeconds)
		mm.config.RetryDelaySeconds = 15 * 60 // 15 minutes in seconds
	}

	if mm.config.TaskRetentionSeconds <= 0 {
		glog.Warningf("Invalid task retention %ds, using default 168h", mm.config.TaskRetentionSeconds)
		mm.config.TaskRetentionSeconds = 7 * 24 * 60 * 60 // 7 days in seconds
	}

	return nil
}

// IsRunning returns whether the maintenance manager is currently running
func (mm *MaintenanceManager) IsRunning() bool {
	return mm.running
}

// Stop terminates the maintenance manager
func (mm *MaintenanceManager) Stop() {
	mm.running = false
	close(mm.stopChan)
	glog.Infof("Maintenance manager stopped")
}

// scanLoop periodically scans for maintenance tasks with adaptive timing
func (mm *MaintenanceManager) scanLoop() {
	scanInterval := time.Duration(mm.config.ScanIntervalSeconds) * time.Second
	ticker := time.NewTicker(scanInterval)
	defer ticker.Stop()

	for mm.running {
		select {
		case <-mm.stopChan:
			return
		case <-ticker.C:
			glog.V(1).Infof("Performing maintenance scan every %v", scanInterval)
			mm.performScan()

			// Adjust ticker interval based on error state
			mm.mutex.RLock()
			currentInterval := scanInterval
			if mm.errorCount > 0 {
				// Use backoff delay when there are errors
				currentInterval = mm.backoffDelay
				if currentInterval > scanInterval {
					// Don't make it longer than the configured interval * 10
					maxInterval := scanInterval * 10
					if currentInterval > maxInterval {
						currentInterval = maxInterval
					}
				}
			}
			mm.mutex.RUnlock()

			// Reset ticker with new interval if needed
			if currentInterval != scanInterval {
				ticker.Stop()
				ticker = time.NewTicker(currentInterval)
			}
		}
	}
}

// cleanupLoop periodically cleans up old tasks and stale workers
func (mm *MaintenanceManager) cleanupLoop() {
	cleanupInterval := time.Duration(mm.config.CleanupIntervalSeconds) * time.Second
	ticker := time.NewTicker(cleanupInterval)
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

// performScan executes a maintenance scan with error handling and backoff
func (mm *MaintenanceManager) performScan() {
	mm.mutex.Lock()
	defer mm.mutex.Unlock()

	glog.V(2).Infof("Starting maintenance scan")

	results, err := mm.scanner.ScanForMaintenanceTasks()
	if err != nil {
		mm.handleScanError(err)
		return
	}

	// Scan succeeded, reset error tracking
	mm.resetErrorTracking()

	if len(results) > 0 {
		mm.queue.AddTasksFromResults(results)
		glog.V(1).Infof("Maintenance scan completed: added %d tasks", len(results))
	} else {
		glog.V(2).Infof("Maintenance scan completed: no tasks needed")
	}
}

// handleScanError handles scan errors with exponential backoff and reduced logging
func (mm *MaintenanceManager) handleScanError(err error) {
	now := time.Now()
	mm.errorCount++
	mm.lastError = err
	mm.lastErrorTime = now

	// Use exponential backoff with jitter
	if mm.errorCount > 1 {
		mm.backoffDelay = mm.backoffDelay * 2
		if mm.backoffDelay > 5*time.Minute {
			mm.backoffDelay = 5 * time.Minute // Cap at 5 minutes
		}
	}

	// Reduce log frequency based on error count and time
	shouldLog := false
	if mm.errorCount <= 3 {
		// Log first 3 errors immediately
		shouldLog = true
	} else if mm.errorCount <= 10 && mm.errorCount%3 == 0 {
		// Log every 3rd error for errors 4-10
		shouldLog = true
	} else if mm.errorCount%10 == 0 {
		// Log every 10th error after that
		shouldLog = true
	}

	if shouldLog {
		// Check if it's a connection error to provide better messaging
		if isConnectionError(err) {
			if mm.errorCount == 1 {
				glog.Errorf("Maintenance scan failed: %v (will retry with backoff)", err)
			} else {
				glog.Errorf("Maintenance scan still failing after %d attempts: %v (backoff: %v)",
					mm.errorCount, err, mm.backoffDelay)
			}
		} else {
			glog.Errorf("Maintenance scan failed: %v", err)
		}
	} else {
		// Use debug level for suppressed errors
		glog.V(3).Infof("Maintenance scan failed (error #%d, suppressed): %v", mm.errorCount, err)
	}
}

// resetErrorTracking resets error tracking when scan succeeds
func (mm *MaintenanceManager) resetErrorTracking() {
	if mm.errorCount > 0 {
		glog.V(1).Infof("Maintenance scan recovered after %d failed attempts", mm.errorCount)
		mm.errorCount = 0
		mm.lastError = nil
		mm.backoffDelay = time.Second // Reset to initial delay
	}
}

// isConnectionError checks if the error is a connection-related error
func isConnectionError(err error) bool {
	if err == nil {
		return false
	}
	errStr := err.Error()
	return strings.Contains(errStr, "connection refused") ||
		strings.Contains(errStr, "connection error") ||
		strings.Contains(errStr, "dial tcp") ||
		strings.Contains(errStr, "connection timeout") ||
		strings.Contains(errStr, "no route to host") ||
		strings.Contains(errStr, "network unreachable")
}

// performCleanup cleans up old tasks and stale workers
func (mm *MaintenanceManager) performCleanup() {
	glog.V(2).Infof("Starting maintenance cleanup")

	taskRetention := time.Duration(mm.config.TaskRetentionSeconds) * time.Second
	workerTimeout := time.Duration(mm.config.WorkerTimeoutSeconds) * time.Second

	removedTasks := mm.queue.CleanupOldTasks(taskRetention)
	removedWorkers := mm.queue.RemoveStaleWorkers(workerTimeout)

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

	mm.mutex.RLock()
	defer mm.mutex.RUnlock()

	stats.LastScanTime = time.Now() // Would need to track this properly

	// Calculate next scan time based on current error state
	scanInterval := time.Duration(mm.config.ScanIntervalSeconds) * time.Second
	nextScanInterval := scanInterval
	if mm.errorCount > 0 {
		nextScanInterval = mm.backoffDelay
		maxInterval := scanInterval * 10
		if nextScanInterval > maxInterval {
			nextScanInterval = maxInterval
		}
	}
	stats.NextScanTime = time.Now().Add(nextScanInterval)

	return stats
}

// GetErrorState returns the current error state for monitoring
func (mm *MaintenanceManager) GetErrorState() (errorCount int, lastError error, backoffDelay time.Duration) {
	mm.mutex.RLock()
	defer mm.mutex.RUnlock()
	return mm.errorCount, mm.lastError, mm.backoffDelay
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
