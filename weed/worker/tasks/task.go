package tasks

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/worker_pb"
	"github.com/seaweedfs/seaweedfs/weed/worker/types"
)

// BaseTask provides common functionality for all tasks
type BaseTask struct {
	taskType          types.TaskType
	taskID            string
	progress          float64
	cancelled         bool
	mutex             sync.RWMutex
	startTime         time.Time
	estimatedDuration time.Duration
	logger            TaskLogger
	loggerConfig      TaskLoggerConfig
	progressCallback  func(float64, string) // Callback function for progress updates
	currentStage      string                // Current stage description
}

// NewBaseTask creates a new base task
func NewBaseTask(taskType types.TaskType) *BaseTask {
	return &BaseTask{
		taskType:     taskType,
		progress:     0.0,
		cancelled:    false,
		loggerConfig: DefaultTaskLoggerConfig(),
	}
}

// NewBaseTaskWithLogger creates a new base task with custom logger configuration
func NewBaseTaskWithLogger(taskType types.TaskType, loggerConfig TaskLoggerConfig) *BaseTask {
	return &BaseTask{
		taskType:     taskType,
		progress:     0.0,
		cancelled:    false,
		loggerConfig: loggerConfig,
	}
}

// InitializeLogger initializes the task logger with task details
func (t *BaseTask) InitializeLogger(taskID string, workerID string, params types.TaskParams) error {
	return t.InitializeTaskLogger(taskID, workerID, params)
}

// InitializeTaskLogger initializes the task logger with task details (LoggerProvider interface)
func (t *BaseTask) InitializeTaskLogger(taskID string, workerID string, params types.TaskParams) error {
	t.mutex.Lock()
	defer t.mutex.Unlock()

	t.taskID = taskID

	logger, err := NewTaskLogger(taskID, t.taskType, workerID, params, t.loggerConfig)
	if err != nil {
		return fmt.Errorf("failed to initialize task logger: %w", err)
	}

	t.logger = logger
	t.logger.Info("BaseTask initialized for task %s (type: %s)", taskID, t.taskType)

	return nil
}

// Type returns the task type
func (t *BaseTask) Type() types.TaskType {
	return t.taskType
}

// GetProgress returns the current progress (0.0 to 100.0)
func (t *BaseTask) GetProgress() float64 {
	t.mutex.RLock()
	defer t.mutex.RUnlock()
	return t.progress
}

// SetProgress sets the current progress and logs it
func (t *BaseTask) SetProgress(progress float64) {
	t.mutex.Lock()
	if progress < 0 {
		progress = 0
	}
	if progress > 100 {
		progress = 100
	}
	oldProgress := t.progress
	callback := t.progressCallback
	stage := t.currentStage
	t.progress = progress
	t.mutex.Unlock()

	// Log progress change
	if t.logger != nil && progress != oldProgress {
		message := stage
		if message == "" {
			message = fmt.Sprintf("Progress updated from %.1f%% to %.1f%%", oldProgress, progress)
		}
		t.logger.LogProgress(progress, message)
	}

	// Call progress callback if set
	if callback != nil && progress != oldProgress {
		callback(progress, stage)
	}
}

// SetProgressWithStage sets the current progress with a stage description
func (t *BaseTask) SetProgressWithStage(progress float64, stage string) {
	t.mutex.Lock()
	if progress < 0 {
		progress = 0
	}
	if progress > 100 {
		progress = 100
	}
	callback := t.progressCallback
	t.progress = progress
	t.currentStage = stage
	t.mutex.Unlock()

	// Log progress change
	if t.logger != nil {
		t.logger.LogProgress(progress, stage)
	}

	// Call progress callback if set
	if callback != nil {
		callback(progress, stage)
	}
}

// SetCurrentStage sets the current stage description
func (t *BaseTask) SetCurrentStage(stage string) {
	t.mutex.Lock()
	defer t.mutex.Unlock()
	t.currentStage = stage
}

// GetCurrentStage returns the current stage description
func (t *BaseTask) GetCurrentStage() string {
	t.mutex.RLock()
	defer t.mutex.RUnlock()
	return t.currentStage
}

// Cancel cancels the task
func (t *BaseTask) Cancel() error {
	t.mutex.Lock()
	defer t.mutex.Unlock()

	if t.cancelled {
		return nil
	}

	t.cancelled = true

	if t.logger != nil {
		t.logger.LogStatus("cancelled", "Task cancelled by request")
		t.logger.Warning("Task %s was cancelled", t.taskID)
	}

	return nil
}

// IsCancelled returns whether the task is cancelled
func (t *BaseTask) IsCancelled() bool {
	t.mutex.RLock()
	defer t.mutex.RUnlock()
	return t.cancelled
}

// SetStartTime sets the task start time
func (t *BaseTask) SetStartTime(startTime time.Time) {
	t.mutex.Lock()
	defer t.mutex.Unlock()
	t.startTime = startTime

	if t.logger != nil {
		t.logger.LogStatus("running", fmt.Sprintf("Task started at %s", startTime.Format(time.RFC3339)))
	}
}

// GetStartTime returns the task start time
func (t *BaseTask) GetStartTime() time.Time {
	t.mutex.RLock()
	defer t.mutex.RUnlock()
	return t.startTime
}

// SetEstimatedDuration sets the estimated duration
func (t *BaseTask) SetEstimatedDuration(duration time.Duration) {
	t.mutex.Lock()
	defer t.mutex.Unlock()
	t.estimatedDuration = duration

	if t.logger != nil {
		t.logger.LogWithFields("INFO", "Estimated duration set", map[string]interface{}{
			"estimated_duration": duration.String(),
			"estimated_seconds":  duration.Seconds(),
		})
	}
}

// GetEstimatedDuration returns the estimated duration
func (t *BaseTask) GetEstimatedDuration() time.Duration {
	t.mutex.RLock()
	defer t.mutex.RUnlock()
	return t.estimatedDuration
}

// SetProgressCallback sets the progress callback function
func (t *BaseTask) SetProgressCallback(callback func(float64, string)) {
	t.mutex.Lock()
	defer t.mutex.Unlock()
	t.progressCallback = callback
}

// SetLoggerConfig sets the logger configuration for this task
func (t *BaseTask) SetLoggerConfig(config TaskLoggerConfig) {
	t.mutex.Lock()
	defer t.mutex.Unlock()
	t.loggerConfig = config
}

// GetLogger returns the task logger
func (t *BaseTask) GetLogger() TaskLogger {
	t.mutex.RLock()
	defer t.mutex.RUnlock()
	return t.logger
}

// GetTaskLogger returns the task logger (LoggerProvider interface)
func (t *BaseTask) GetTaskLogger() TaskLogger {
	t.mutex.RLock()
	defer t.mutex.RUnlock()
	return t.logger
}

// LogInfo logs an info message
func (t *BaseTask) LogInfo(message string, args ...interface{}) {
	if t.logger != nil {
		t.logger.Info(message, args...)
	}
}

// LogWarning logs a warning message
func (t *BaseTask) LogWarning(message string, args ...interface{}) {
	if t.logger != nil {
		t.logger.Warning(message, args...)
	}
}

// LogError logs an error message
func (t *BaseTask) LogError(message string, args ...interface{}) {
	if t.logger != nil {
		t.logger.Error(message, args...)
	}
}

// LogDebug logs a debug message
func (t *BaseTask) LogDebug(message string, args ...interface{}) {
	if t.logger != nil {
		t.logger.Debug(message, args...)
	}
}

// LogWithFields logs a message with structured fields
func (t *BaseTask) LogWithFields(level string, message string, fields map[string]interface{}) {
	if t.logger != nil {
		t.logger.LogWithFields(level, message, fields)
	}
}

// FinishTask finalizes the task and closes the logger
func (t *BaseTask) FinishTask(success bool, errorMsg string) error {
	if t.logger != nil {
		if success {
			t.logger.LogStatus("completed", "Task completed successfully")
			t.logger.Info("Task %s finished successfully", t.taskID)
		} else {
			t.logger.LogStatus("failed", fmt.Sprintf("Task failed: %s", errorMsg))
			t.logger.Error("Task %s failed: %s", t.taskID, errorMsg)
		}

		// Close logger
		if err := t.logger.Close(); err != nil {
			glog.Errorf("Failed to close task logger: %v", err)
		}
	}

	return nil
}

// ExecuteTask is a wrapper that handles common task execution logic with logging
func (t *BaseTask) ExecuteTask(ctx context.Context, params types.TaskParams, executor func(context.Context, types.TaskParams) error) error {
	// Initialize logger if not already done
	if t.logger == nil {
		// Generate a temporary task ID if none provided
		if t.taskID == "" {
			t.taskID = fmt.Sprintf("task_%d", time.Now().UnixNano())
		}

		workerID := "unknown"
		if err := t.InitializeLogger(t.taskID, workerID, params); err != nil {
			glog.Warningf("Failed to initialize task logger: %v", err)
		}
	}

	t.SetStartTime(time.Now())
	t.SetProgress(0)

	if t.logger != nil {
		t.logger.LogWithFields("INFO", "Task execution started", map[string]interface{}{
			"volume_id":  params.VolumeID,
			"server":     getServerFromSources(params.TypedParams.Sources),
			"collection": params.Collection,
		})
	}

	// Create a context that can be cancelled
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// Monitor for cancellation
	go func() {
		for !t.IsCancelled() {
			select {
			case <-ctx.Done():
				return
			case <-time.After(time.Second):
				// Check cancellation every second
			}
		}
		t.LogWarning("Task cancellation detected, cancelling context")
		cancel()
	}()

	// Execute the actual task
	t.LogInfo("Starting task executor")
	err := executor(ctx, params)

	if err != nil {
		t.LogError("Task executor failed: %v", err)
		t.FinishTask(false, err.Error())
		return err
	}

	if t.IsCancelled() {
		t.LogWarning("Task was cancelled during execution")
		t.FinishTask(false, "cancelled")
		return context.Canceled
	}

	t.SetProgress(100)
	t.LogInfo("Task executor completed successfully")
	t.FinishTask(true, "")
	return nil
}

// UnsupportedTaskTypeError represents an error for unsupported task types
type UnsupportedTaskTypeError struct {
	TaskType types.TaskType
}

func (e *UnsupportedTaskTypeError) Error() string {
	return "unsupported task type: " + string(e.TaskType)
}

// BaseTaskFactory provides common functionality for task factories
type BaseTaskFactory struct {
	taskType     types.TaskType
	capabilities []string
	description  string
}

// NewBaseTaskFactory creates a new base task factory
func NewBaseTaskFactory(taskType types.TaskType, capabilities []string, description string) *BaseTaskFactory {
	return &BaseTaskFactory{
		taskType:     taskType,
		capabilities: capabilities,
		description:  description,
	}
}

// Capabilities returns the capabilities required for this task type
func (f *BaseTaskFactory) Capabilities() []string {
	return f.capabilities
}

// Description returns the description of this task type
func (f *BaseTaskFactory) Description() string {
	return f.description
}

// ValidateParams validates task parameters
func ValidateParams(params types.TaskParams, requiredFields ...string) error {
	for _, field := range requiredFields {
		switch field {
		case "volume_id":
			if params.VolumeID == 0 {
				return &ValidationError{Field: field, Message: "volume_id is required"}
			}
		case "server":
			if len(params.TypedParams.Sources) == 0 {
				return &ValidationError{Field: field, Message: "server is required"}
			}
		case "collection":
			if params.Collection == "" {
				return &ValidationError{Field: field, Message: "collection is required"}
			}
		}
	}
	return nil
}

// ValidationError represents a parameter validation error
type ValidationError struct {
	Field   string
	Message string
}

func (e *ValidationError) Error() string {
	return e.Field + ": " + e.Message
}

// getServerFromSources extracts the server address from unified sources
func getServerFromSources(sources []*worker_pb.TaskSource) string {
	if len(sources) > 0 {
		return sources[0].Node
	}
	return ""
}
