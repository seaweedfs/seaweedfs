package balance

import (
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/worker/types"
)

// BalanceScheduler implements TaskScheduler for balance tasks
type BalanceScheduler struct {
	enabled            bool
	maxConcurrent      int
	minInterval        time.Duration
	lastScheduled      map[string]time.Time // track when we last scheduled a balance for each task type
	minServerCount     int
	moveDuringOffHours bool
	offHoursStart      string
	offHoursEnd        string
}

// Compile-time interface assertions
var (
	_ types.TaskScheduler = (*BalanceScheduler)(nil)
)

// NewBalanceScheduler creates a new balance scheduler
func NewBalanceScheduler() *BalanceScheduler {
	return &BalanceScheduler{
		enabled:            true,
		maxConcurrent:      1, // Only run one balance at a time
		minInterval:        6 * time.Hour,
		lastScheduled:      make(map[string]time.Time),
		minServerCount:     3,
		moveDuringOffHours: true,
		offHoursStart:      "23:00",
		offHoursEnd:        "06:00",
	}
}

// GetTaskType returns the task type
func (s *BalanceScheduler) GetTaskType() types.TaskType {
	return types.TaskTypeBalance
}

// CanScheduleNow determines if a balance task can be scheduled
func (s *BalanceScheduler) CanScheduleNow(task *types.Task, runningTasks []*types.Task, availableWorkers []*types.Worker) bool {
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
func (s *BalanceScheduler) GetPriority(task *types.Task) types.TaskPriority {
	// Balance is typically normal priority - not urgent but important for optimization
	return types.TaskPriorityNormal
}

// GetMaxConcurrent returns the maximum concurrent balance tasks
func (s *BalanceScheduler) GetMaxConcurrent() int {
	return s.maxConcurrent
}

// GetDefaultRepeatInterval returns the default interval to wait before repeating balance tasks
func (s *BalanceScheduler) GetDefaultRepeatInterval() time.Duration {
	return s.minInterval
}

// IsEnabled returns whether the scheduler is enabled
func (s *BalanceScheduler) IsEnabled() bool {
	return s.enabled
}

// SetEnabled sets whether the scheduler is enabled
func (s *BalanceScheduler) SetEnabled(enabled bool) {
	s.enabled = enabled
	glog.V(1).Infof("🔄 Balance scheduler enabled: %v", enabled)
}

// SetMaxConcurrent sets the maximum concurrent balance tasks
func (s *BalanceScheduler) SetMaxConcurrent(max int) {
	s.maxConcurrent = max
	glog.V(1).Infof("🔄 Balance max concurrent set to: %d", max)
}

// SetMinInterval sets the minimum interval between balance operations
func (s *BalanceScheduler) SetMinInterval(interval time.Duration) {
	s.minInterval = interval
	glog.V(1).Infof("🔄 Balance minimum interval set to: %v", interval)
}

// GetLastScheduled returns when we last scheduled this task type
func (s *BalanceScheduler) GetLastScheduled(taskKey string) time.Time {
	if lastTime, exists := s.lastScheduled[taskKey]; exists {
		return lastTime
	}
	return time.Time{}
}

// SetLastScheduled updates when we last scheduled this task type
func (s *BalanceScheduler) SetLastScheduled(taskKey string, when time.Time) {
	s.lastScheduled[taskKey] = when
}

// GetMinServerCount returns the minimum server count
func (s *BalanceScheduler) GetMinServerCount() int {
	return s.minServerCount
}

// SetMinServerCount sets the minimum server count
func (s *BalanceScheduler) SetMinServerCount(count int) {
	s.minServerCount = count
	glog.V(1).Infof("🔄 Balance minimum server count set to: %d", count)
}

// GetMoveDuringOffHours returns whether to move only during off-hours
func (s *BalanceScheduler) GetMoveDuringOffHours() bool {
	return s.moveDuringOffHours
}

// SetMoveDuringOffHours sets whether to move only during off-hours
func (s *BalanceScheduler) SetMoveDuringOffHours(enabled bool) {
	s.moveDuringOffHours = enabled
	glog.V(1).Infof("🔄 Balance move during off-hours: %v", enabled)
}

// GetOffHoursStart returns the off-hours start time
func (s *BalanceScheduler) GetOffHoursStart() string {
	return s.offHoursStart
}

// SetOffHoursStart sets the off-hours start time
func (s *BalanceScheduler) SetOffHoursStart(start string) {
	s.offHoursStart = start
	glog.V(1).Infof("🔄 Balance off-hours start time set to: %s", start)
}

// GetOffHoursEnd returns the off-hours end time
func (s *BalanceScheduler) GetOffHoursEnd() string {
	return s.offHoursEnd
}

// SetOffHoursEnd sets the off-hours end time
func (s *BalanceScheduler) SetOffHoursEnd(end string) {
	s.offHoursEnd = end
	glog.V(1).Infof("🔄 Balance off-hours end time set to: %s", end)
}

// GetMinInterval returns the minimum interval
func (s *BalanceScheduler) GetMinInterval() time.Duration {
	return s.minInterval
}
