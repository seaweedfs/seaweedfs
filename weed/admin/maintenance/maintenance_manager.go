package maintenance

import (
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/worker_pb"
	"github.com/seaweedfs/seaweedfs/weed/worker/tasks/balance"
	"github.com/seaweedfs/seaweedfs/weed/worker/tasks/erasure_coding"
	"github.com/seaweedfs/seaweedfs/weed/worker/tasks/vacuum"
)

// buildPolicyFromTaskConfigs loads task configurations from separate files and builds a MaintenancePolicy
func buildPolicyFromTaskConfigs() *worker_pb.MaintenancePolicy {
	policy := &worker_pb.MaintenancePolicy{
		GlobalMaxConcurrent:          4,
		DefaultRepeatIntervalSeconds: 6 * 3600,  // 6 hours in seconds
		DefaultCheckIntervalSeconds:  12 * 3600, // 12 hours in seconds
		TaskPolicies:                 make(map[string]*worker_pb.TaskPolicy),
	}

	// Load vacuum task configuration
	if vacuumConfig := vacuum.LoadConfigFromPersistence(nil); vacuumConfig != nil {
		policy.TaskPolicies["vacuum"] = &worker_pb.TaskPolicy{
			Enabled:               vacuumConfig.Enabled,
			MaxConcurrent:         int32(vacuumConfig.MaxConcurrent),
			RepeatIntervalSeconds: int32(vacuumConfig.ScanIntervalSeconds),
			CheckIntervalSeconds:  int32(vacuumConfig.ScanIntervalSeconds),
			TaskConfig: &worker_pb.TaskPolicy_VacuumConfig{
				VacuumConfig: &worker_pb.VacuumTaskConfig{
					GarbageThreshold:   float64(vacuumConfig.GarbageThreshold),
					MinVolumeAgeHours:  int32(vacuumConfig.MinVolumeAgeSeconds / 3600), // Convert seconds to hours
					MinIntervalSeconds: int32(vacuumConfig.MinIntervalSeconds),
				},
			},
		}
	}

	// Load erasure coding task configuration
	if ecConfig := erasure_coding.LoadConfigFromPersistence(nil); ecConfig != nil {
		policy.TaskPolicies["erasure_coding"] = &worker_pb.TaskPolicy{
			Enabled:               ecConfig.Enabled,
			MaxConcurrent:         int32(ecConfig.MaxConcurrent),
			RepeatIntervalSeconds: int32(ecConfig.ScanIntervalSeconds),
			CheckIntervalSeconds:  int32(ecConfig.ScanIntervalSeconds),
			TaskConfig: &worker_pb.TaskPolicy_ErasureCodingConfig{
				ErasureCodingConfig: &worker_pb.ErasureCodingTaskConfig{
					FullnessRatio:    float64(ecConfig.FullnessRatio),
					QuietForSeconds:  int32(ecConfig.QuietForSeconds),
					MinVolumeSizeMb:  int32(ecConfig.MinSizeMB),
					CollectionFilter: ecConfig.CollectionFilter,
				},
			},
		}
	}

	// Load balance task configuration
	if balanceConfig := balance.LoadConfigFromPersistence(nil); balanceConfig != nil {
		policy.TaskPolicies["balance"] = &worker_pb.TaskPolicy{
			Enabled:               balanceConfig.Enabled,
			MaxConcurrent:         int32(balanceConfig.MaxConcurrent),
			RepeatIntervalSeconds: int32(balanceConfig.ScanIntervalSeconds),
			CheckIntervalSeconds:  int32(balanceConfig.ScanIntervalSeconds),
			TaskConfig: &worker_pb.TaskPolicy_BalanceConfig{
				BalanceConfig: &worker_pb.BalanceTaskConfig{
					ImbalanceThreshold: float64(balanceConfig.ImbalanceThreshold),
					MinServerCount:     int32(balanceConfig.MinServerCount),
				},
			},
		}
	}

	glog.V(1).Infof("Built maintenance policy from separate task configs - %d task policies loaded", len(policy.TaskPolicies))
	return policy
}

// MaintenanceManager coordinates the maintenance system
type MaintenanceManager struct {
	config      *MaintenanceConfig
	scanner     *MaintenanceScanner
	queue       *MaintenanceQueue
	adminClient AdminClient
	running     bool
	stopChan    chan struct{}
	// Error handling and backoff
	errorCount     int
	lastError      error
	lastErrorTime  time.Time
	backoffDelay   time.Duration
	mutex          sync.RWMutex
	scanInProgress bool
}

// NewMaintenanceManager creates a new maintenance manager
func NewMaintenanceManager(adminClient AdminClient, config *MaintenanceConfig) *MaintenanceManager {
	if config == nil {
		config = DefaultMaintenanceConfig()
	}

	// Use the policy from the config (which is populated from separate task files in LoadMaintenanceConfig)
	policy := config.Policy
	if policy == nil {
		// Fallback: build policy from separate task configuration files if not already populated
		policy = buildPolicyFromTaskConfigs()
	}

	queue := NewMaintenanceQueue(policy)
	scanner := NewMaintenanceScanner(adminClient, policy, queue)

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
		return fmt.Errorf("invalid maintenance configuration: %w", err)
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

			// Use the same synchronization as TriggerScan to prevent concurrent scans
			if err := mm.triggerScanInternal(false); err != nil {
				glog.V(1).Infof("Scheduled scan skipped: %v", err)
			}

			// Adjust ticker interval based on error state (read error state safely)
			currentInterval := mm.getScanInterval(scanInterval)

			// Reset ticker with new interval if needed
			if currentInterval != scanInterval {
				ticker.Stop()
				ticker = time.NewTicker(currentInterval)
			}
		}
	}
}

// getScanInterval safely reads the current scan interval with error backoff
func (mm *MaintenanceManager) getScanInterval(baseInterval time.Duration) time.Duration {
	mm.mutex.RLock()
	defer mm.mutex.RUnlock()

	if mm.errorCount > 0 {
		// Use backoff delay when there are errors
		currentInterval := mm.backoffDelay
		if currentInterval > baseInterval {
			// Don't make it longer than the configured interval * 10
			maxInterval := baseInterval * 10
			if currentInterval > maxInterval {
				currentInterval = maxInterval
			}
		}
		return currentInterval
	}
	return baseInterval
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
	defer func() {
		// Always reset scan in progress flag when done
		mm.mutex.Lock()
		mm.scanInProgress = false
		mm.mutex.Unlock()
	}()

	glog.Infof("Starting maintenance scan...")

	results, err := mm.scanner.ScanForMaintenanceTasks()
	if err != nil {
		// Handle scan error
		mm.mutex.Lock()
		mm.handleScanError(err)
		mm.mutex.Unlock()
		glog.Warningf("Maintenance scan failed: %v", err)
		return
	}

	// Scan succeeded - update state and process results
	mm.handleScanSuccess(results)
}

// handleScanSuccess processes successful scan results with proper lock management
func (mm *MaintenanceManager) handleScanSuccess(results []*TaskDetectionResult) {
	// Update manager state first
	mm.mutex.Lock()
	mm.resetErrorTracking()
	taskCount := len(results)
	mm.mutex.Unlock()

	if taskCount > 0 {
		// Count tasks by type for logging (outside of lock)
		taskCounts := make(map[MaintenanceTaskType]int)
		for _, result := range results {
			taskCounts[result.TaskType]++
		}

		// Add tasks to queue (no manager lock held)
		mm.queue.AddTasksFromResults(results)

		// Log detailed scan results
		glog.Infof("Maintenance scan completed: found %d tasks", taskCount)
		for taskType, count := range taskCounts {
			glog.Infof("  - %s: %d tasks", taskType, count)
		}
	} else {
		glog.Infof("Maintenance scan completed: no maintenance tasks needed")
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

	// Clean up stale pending operations (operations running for more than 4 hours)
	staleOperationTimeout := 4 * time.Hour
	removedOperations := 0
	if mm.scanner != nil && mm.scanner.integration != nil {
		pendingOps := mm.scanner.integration.GetPendingOperations()
		if pendingOps != nil {
			removedOperations = pendingOps.CleanupStaleOperations(staleOperationTimeout)
		}
	}

	if removedTasks > 0 || removedWorkers > 0 || removedOperations > 0 {
		glog.V(1).Infof("Cleanup completed: removed %d old tasks, %d stale workers, and %d stale operations",
			removedTasks, removedWorkers, removedOperations)
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

// ReloadTaskConfigurations reloads task configurations from the current policy
func (mm *MaintenanceManager) ReloadTaskConfigurations() error {
	mm.mutex.Lock()
	defer mm.mutex.Unlock()

	// Trigger configuration reload in the integration layer
	if mm.scanner != nil && mm.scanner.integration != nil {
		mm.scanner.integration.ConfigureTasksFromPolicy()
		glog.V(1).Infof("Task configurations reloaded from policy")
		return nil
	}

	return fmt.Errorf("integration not available for configuration reload")
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
	return mm.triggerScanInternal(true)
}

// triggerScanInternal handles both manual and automatic scan triggers
func (mm *MaintenanceManager) triggerScanInternal(isManual bool) error {
	if !mm.running {
		return fmt.Errorf("maintenance manager is not running")
	}

	// Prevent multiple concurrent scans
	mm.mutex.Lock()
	if mm.scanInProgress {
		mm.mutex.Unlock()
		if isManual {
			glog.V(1).Infof("Manual scan already in progress, ignoring trigger request")
		} else {
			glog.V(2).Infof("Automatic scan already in progress, ignoring scheduled scan")
		}
		return fmt.Errorf("scan already in progress")
	}
	mm.scanInProgress = true
	mm.mutex.Unlock()

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
