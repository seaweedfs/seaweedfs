package balance

import (
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/worker/types"
)

// SimpleScheduler implements TaskScheduler for balance tasks
type SimpleScheduler struct {
	enabled       bool
	maxConcurrent int
	minInterval   time.Duration
	lastScheduled map[string]time.Time // track when we last scheduled a balance for each task type
}

// NewSimpleScheduler creates a new balance scheduler
func NewSimpleScheduler() *SimpleScheduler {
	return &SimpleScheduler{
		enabled:       true,
		maxConcurrent: 1, // Only run one balance at a time
		minInterval:   6 * time.Hour,
		lastScheduled: make(map[string]time.Time),
	}
}

// GetTaskType returns the task type
func (s *SimpleScheduler) GetTaskType() types.TaskType {
	return types.TaskTypeBalance
}

// CanScheduleNow determines if a balance task can be scheduled
func (s *SimpleScheduler) CanScheduleNow(task *types.Task, runningTasks []*types.Task, availableWorkers []*types.Worker) bool {
	if !s.enabled {
		return false
	}

	// Count running balance tasks
	runningBalanceCount := 0
	for _, runningTask := range runningTasks {
		if runningTask.Type == types.TaskTypeBalance {
			runningBalanceCount++
		}
	}

	// Check concurrency limit
	if runningBalanceCount >= s.maxConcurrent {
		glog.V(3).Infof("⏸️ Balance task blocked: too many running (%d >= %d)", runningBalanceCount, s.maxConcurrent)
		return false
	}

	// Check minimum interval between balance operations
	if lastTime, exists := s.lastScheduled["balance"]; exists {
		if time.Since(lastTime) < s.minInterval {
			timeLeft := s.minInterval - time.Since(lastTime)
			glog.V(3).Infof("⏸️ Balance task blocked: too soon (wait %v)", timeLeft)
			return false
		}
	}

	// Check if we have available workers
	availableWorkerCount := 0
	for _, worker := range availableWorkers {
		for _, capability := range worker.Capabilities {
			if capability == types.TaskTypeBalance {
				availableWorkerCount++
				break
			}
		}
	}

	if availableWorkerCount == 0 {
		glog.V(3).Infof("⏸️ Balance task blocked: no available workers")
		return false
	}

	// All checks passed - can schedule
	s.lastScheduled["balance"] = time.Now()
	glog.V(2).Infof("✅ Balance task can be scheduled (running: %d/%d, workers: %d)",
		runningBalanceCount, s.maxConcurrent, availableWorkerCount)
	return true
}

// GetPriority returns the priority for balance tasks
func (s *SimpleScheduler) GetPriority(task *types.Task) types.TaskPriority {
	// Balance is typically normal priority - not urgent but important for optimization
	return types.TaskPriorityNormal
}

// GetMaxConcurrent returns the maximum concurrent balance tasks
func (s *SimpleScheduler) GetMaxConcurrent() int {
	return s.maxConcurrent
}

// GetDefaultRepeatInterval returns the default interval to wait before repeating balance tasks
func (s *SimpleScheduler) GetDefaultRepeatInterval() time.Duration {
	return s.minInterval
}

// IsEnabled returns whether the scheduler is enabled
func (s *SimpleScheduler) IsEnabled() bool {
	return s.enabled
}

// SetEnabled sets whether the scheduler is enabled
func (s *SimpleScheduler) SetEnabled(enabled bool) {
	s.enabled = enabled
	glog.V(1).Infof("🔄 Balance scheduler enabled: %v", enabled)
}

// SetMaxConcurrent sets the maximum concurrent balance tasks
func (s *SimpleScheduler) SetMaxConcurrent(max int) {
	s.maxConcurrent = max
	glog.V(1).Infof("🔄 Balance max concurrent set to: %d", max)
}

// SetMinInterval sets the minimum interval between balance operations
func (s *SimpleScheduler) SetMinInterval(interval time.Duration) {
	s.minInterval = interval
	glog.V(1).Infof("🔄 Balance minimum interval set to: %v", interval)
}

// GetLastScheduled returns when we last scheduled this task type
func (s *SimpleScheduler) GetLastScheduled(taskKey string) time.Time {
	if lastTime, exists := s.lastScheduled[taskKey]; exists {
		return lastTime
	}
	return time.Time{}
}

// SetLastScheduled updates when we last scheduled this task type
func (s *SimpleScheduler) SetLastScheduled(taskKey string, when time.Time) {
	s.lastScheduled[taskKey] = when
}
