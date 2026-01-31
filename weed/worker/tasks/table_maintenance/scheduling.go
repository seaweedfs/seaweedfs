package table_maintenance

import (
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb/worker_pb"
	"github.com/seaweedfs/seaweedfs/weed/worker/tasks/base"
	"github.com/seaweedfs/seaweedfs/weed/worker/types"
)

// TableMaintenanceScheduler implements types.TaskScheduler for table maintenance
type TableMaintenanceScheduler struct {
	config *Config
}

// NewTableMaintenanceScheduler creates a new scheduler
func NewTableMaintenanceScheduler(config *Config) *TableMaintenanceScheduler {
	return &TableMaintenanceScheduler{
		config: config,
	}
}

// ShouldSchedule determines if a task should be scheduled based on the detection result
func (s *TableMaintenanceScheduler) ShouldSchedule(result *types.TaskDetectionResult, lastExecution time.Time) bool {
	// Don't schedule if disabled
	if s.config != nil && !s.config.Enabled {
		return false
	}

	// Schedule if never executed
	if lastExecution.IsZero() {
		return true
	}

	// Schedule based on repeat interval
	repeatInterval := s.GetDefaultRepeatInterval()
	return time.Since(lastExecution) >= repeatInterval
}

// GetMaxConcurrent returns the maximum concurrent tasks
func (s *TableMaintenanceScheduler) GetMaxConcurrent() int {
	if s.config != nil && s.config.MaxConcurrent > 0 {
		return s.config.MaxConcurrent
	}
	return 2 // Default
}

// GetDefaultRepeatInterval returns the default repeat interval between task executions
func (s *TableMaintenanceScheduler) GetDefaultRepeatInterval() time.Duration {
	// Table maintenance can run more frequently than volume maintenance
	return 24 * time.Hour // Once per day per table
}

// Scheduling is the function signature required by the task registration system
func Scheduling(task *types.TaskInput, runningTasks []*types.TaskInput, availableWorkers []*types.WorkerData, config base.TaskConfig) bool {
	tableConfig, ok := config.(*Config)
	if !ok {
		tableConfig = NewDefaultConfig()
	}

	// Count running table maintenance tasks
	runningCount := 0
	for _, runningTask := range runningTasks {
		if runningTask.Type == types.TaskTypeTableMaintenance {
			runningCount++
		}
	}

	// Check concurrency limit
	if runningCount >= tableConfig.MaxConcurrent {
		return false
	}

	// Check for available workers with table maintenance capability
	for _, worker := range availableWorkers {
		if worker.CurrentLoad < worker.MaxConcurrent {
			for _, capability := range worker.Capabilities {
				if capability == types.TaskTypeTableMaintenance {
					return true
				}
			}
		}
	}

	return false
}

// CreateTaskFromDetectionResult creates typed task parameters from a detection result
func CreateTaskFromDetectionResult(result *types.TaskDetectionResult) *worker_pb.TaskParams {
	// For table maintenance, the source is the table path
	return &worker_pb.TaskParams{
		Sources: []*worker_pb.TaskSource{
			{
				Node: result.Server, // Table path
			},
		},
		Collection: result.Collection, // Table bucket name
	}
}
