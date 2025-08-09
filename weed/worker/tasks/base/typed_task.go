package base

import (
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb/worker_pb"
	"github.com/seaweedfs/seaweedfs/weed/worker/tasks"
	"github.com/seaweedfs/seaweedfs/weed/worker/types"
)

// BaseTypedTask provides a base implementation for typed tasks with logger support
type BaseTypedTask struct {
	taskType         types.TaskType
	taskID           string
	progress         float64
	progressCallback func(float64, string)
	currentStage     string
	cancelled        bool
	mutex            sync.RWMutex

	// Logger functionality
	logger       tasks.TaskLogger
	loggerConfig types.TaskLoggerConfig
}

// NewBaseTypedTask creates a new base typed task
func NewBaseTypedTask(taskType types.TaskType) *BaseTypedTask {
	return &BaseTypedTask{
		taskType: taskType,
		progress: 0.0,
		loggerConfig: types.TaskLoggerConfig{
			BaseLogDir:    "/data/task_logs",
			MaxTasks:      100,
			MaxLogSizeMB:  10,
			EnableConsole: true,
		},
	}
}

// GetType returns the task type
func (bt *BaseTypedTask) GetType() types.TaskType {
	return bt.taskType
}

// IsCancellable returns whether the task can be cancelled
func (bt *BaseTypedTask) IsCancellable() bool {
	return true // Most tasks can be cancelled
}

// Cancel cancels the task
func (bt *BaseTypedTask) Cancel() error {
	bt.mutex.Lock()
	defer bt.mutex.Unlock()
	bt.cancelled = true
	return nil
}

// IsCancelled returns whether the task has been cancelled
func (bt *BaseTypedTask) IsCancelled() bool {
	bt.mutex.RLock()
	defer bt.mutex.RUnlock()
	return bt.cancelled
}

// GetProgress returns the current progress (0-100)
func (bt *BaseTypedTask) GetProgress() float64 {
	bt.mutex.RLock()
	defer bt.mutex.RUnlock()
	return bt.progress
}

// SetProgress sets the current progress and calls the callback if set
func (bt *BaseTypedTask) SetProgress(progress float64) {
	bt.mutex.Lock()
	callback := bt.progressCallback
	stage := bt.currentStage
	bt.progress = progress
	bt.mutex.Unlock()

	if callback != nil {
		callback(progress, stage)
	}
}

// SetProgressCallback sets the progress callback function
func (bt *BaseTypedTask) SetProgressCallback(callback func(float64, string)) {
	bt.mutex.Lock()
	defer bt.mutex.Unlock()
	bt.progressCallback = callback
}

// SetProgressWithStage sets the current progress with a stage description
func (bt *BaseTypedTask) SetProgressWithStage(progress float64, stage string) {
	bt.mutex.Lock()
	callback := bt.progressCallback
	bt.progress = progress
	bt.currentStage = stage
	bt.mutex.Unlock()

	if callback != nil {
		callback(progress, stage)
	}
}

// SetCurrentStage sets the current stage description
func (bt *BaseTypedTask) SetCurrentStage(stage string) {
	bt.mutex.Lock()
	defer bt.mutex.Unlock()
	bt.currentStage = stage
}

// GetCurrentStage returns the current stage description
func (bt *BaseTypedTask) GetCurrentStage() string {
	bt.mutex.RLock()
	defer bt.mutex.RUnlock()
	return bt.currentStage
}

// SetLoggerConfig sets the logger configuration for this task
func (bt *BaseTypedTask) SetLoggerConfig(config types.TaskLoggerConfig) {
	bt.mutex.Lock()
	defer bt.mutex.Unlock()
	bt.loggerConfig = config
}

// convertToTasksLoggerConfig converts types.TaskLoggerConfig to tasks.TaskLoggerConfig
func convertToTasksLoggerConfig(config types.TaskLoggerConfig) tasks.TaskLoggerConfig {
	return tasks.TaskLoggerConfig{
		BaseLogDir:    config.BaseLogDir,
		MaxTasks:      config.MaxTasks,
		MaxLogSizeMB:  config.MaxLogSizeMB,
		EnableConsole: config.EnableConsole,
	}
}

// InitializeTaskLogger initializes the task logger with task details (LoggerProvider interface)
func (bt *BaseTypedTask) InitializeTaskLogger(taskID string, workerID string, params types.TaskParams) error {
	bt.mutex.Lock()
	defer bt.mutex.Unlock()

	bt.taskID = taskID

	// Convert the logger config to the tasks package type
	tasksLoggerConfig := convertToTasksLoggerConfig(bt.loggerConfig)

	logger, err := tasks.NewTaskLogger(taskID, bt.taskType, workerID, params, tasksLoggerConfig)
	if err != nil {
		return fmt.Errorf("failed to initialize task logger: %w", err)
	}

	bt.logger = logger
	if bt.logger != nil {
		bt.logger.Info("BaseTypedTask initialized for task %s (type: %s)", taskID, bt.taskType)
	}

	return nil
}

// GetTaskLogger returns the task logger (LoggerProvider interface)
func (bt *BaseTypedTask) GetTaskLogger() types.TaskLogger {
	bt.mutex.RLock()
	defer bt.mutex.RUnlock()
	return bt.logger
}

// LogInfo logs an info message
func (bt *BaseTypedTask) LogInfo(message string, args ...interface{}) {
	bt.mutex.RLock()
	logger := bt.logger
	bt.mutex.RUnlock()

	if logger != nil {
		logger.Info(message, args...)
	}
}

// LogWarning logs a warning message
func (bt *BaseTypedTask) LogWarning(message string, args ...interface{}) {
	bt.mutex.RLock()
	logger := bt.logger
	bt.mutex.RUnlock()

	if logger != nil {
		logger.Warning(message, args...)
	}
}

// LogError logs an error message
func (bt *BaseTypedTask) LogError(message string, args ...interface{}) {
	bt.mutex.RLock()
	logger := bt.logger
	bt.mutex.RUnlock()

	if logger != nil {
		logger.Error(message, args...)
	}
}

// LogDebug logs a debug message
func (bt *BaseTypedTask) LogDebug(message string, args ...interface{}) {
	bt.mutex.RLock()
	logger := bt.logger
	bt.mutex.RUnlock()

	if logger != nil {
		logger.Debug(message, args...)
	}
}

// LogWithFields logs a message with structured fields
func (bt *BaseTypedTask) LogWithFields(level string, message string, fields map[string]interface{}) {
	bt.mutex.RLock()
	logger := bt.logger
	bt.mutex.RUnlock()

	if logger != nil {
		logger.LogWithFields(level, message, fields)
	}
}

// ValidateTyped provides basic validation for typed parameters
func (bt *BaseTypedTask) ValidateTyped(params *worker_pb.TaskParams) error {
	if params == nil {
		return errors.New("task parameters cannot be nil")
	}
	if params.VolumeId == 0 {
		return errors.New("volume_id is required")
	}
	if len(params.Sources) == 0 {
		return errors.New("at least one source is required")
	}
	return nil
}

// EstimateTimeTyped provides a default time estimation
func (bt *BaseTypedTask) EstimateTimeTyped(params *worker_pb.TaskParams) time.Duration {
	// Default estimation - concrete tasks should override this
	return 5 * time.Minute
}

// ExecuteTyped is a placeholder that concrete tasks must implement
func (bt *BaseTypedTask) ExecuteTyped(params *worker_pb.TaskParams) error {
	panic("ExecuteTyped must be implemented by concrete task types")
}
