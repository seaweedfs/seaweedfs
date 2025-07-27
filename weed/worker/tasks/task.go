package tasks

import (
	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/worker/types"
)

// BaseTask provides common functionality for all tasks
type BaseTask struct {
	taskType          types.TaskType
	progress          float64
	cancelled         bool
	mutex             sync.RWMutex
	startTime         time.Time
	estimatedDuration time.Duration

	// Logging functionality
	logFile     *os.File
	logger      *log.Logger
	logFilePath string
}

// NewBaseTask creates a new base task
func NewBaseTask(taskType types.TaskType) *BaseTask {
	return &BaseTask{
		taskType:  taskType,
		progress:  0.0,
		cancelled: false,
	}
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

// SetProgress sets the current progress
func (t *BaseTask) SetProgress(progress float64) {
	t.mutex.Lock()
	defer t.mutex.Unlock()
	if progress < 0 {
		progress = 0
	}
	if progress > 100 {
		progress = 100
	}
	t.progress = progress
}

// Cancel cancels the task
func (t *BaseTask) Cancel() error {
	t.mutex.Lock()
	defer t.mutex.Unlock()
	t.cancelled = true
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
}

// GetEstimatedDuration returns the estimated duration
func (t *BaseTask) GetEstimatedDuration() time.Duration {
	t.mutex.RLock()
	defer t.mutex.RUnlock()
	return t.estimatedDuration
}

// InitializeTaskLogging sets up task-specific logging - can be called by worker or tasks
func (t *BaseTask) InitializeTaskLogging(workingDir, taskID string) error {
	return t.initializeLogging(workingDir, taskID)
}

// CloseTaskLogging properly closes task logging - can be called by worker or tasks
func (t *BaseTask) CloseTaskLogging() {
	t.closeLogging()
}

// ExecuteTask is a wrapper that handles common task execution logic
func (t *BaseTask) ExecuteTask(ctx context.Context, params types.TaskParams, executor func(context.Context, types.TaskParams) error) error {
	t.SetStartTime(time.Now())
	t.SetProgress(0)

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
		cancel()
	}()

	// Execute the actual task
	err := executor(ctx, params)

	if err != nil {
		return err
	}

	if t.IsCancelled() {
		return context.Canceled
	}

	t.SetProgress(100)
	return nil
}

// initializeLogging sets up task-specific logging to a file in the working directory
func (t *BaseTask) initializeLogging(workingDir, taskID string) error {
	if workingDir == "" {
		// If no working directory specified, skip file logging
		return nil
	}

	// Ensure working directory exists
	if err := os.MkdirAll(workingDir, 0755); err != nil {
		return fmt.Errorf("failed to create working directory %s: %v", workingDir, err)
	}

	// Create task-specific log file
	timestamp := time.Now().Format("20060102_150405")
	logFileName := fmt.Sprintf("%s_%s_%s.log", t.taskType, taskID, timestamp)
	t.logFilePath = filepath.Join(workingDir, logFileName)

	logFile, err := os.OpenFile(t.logFilePath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return fmt.Errorf("failed to create log file %s: %v", t.logFilePath, err)
	}

	t.logFile = logFile
	t.logger = log.New(logFile, "", log.LstdFlags|log.Lmicroseconds)

	// Log task initialization
	t.LogInfo("Task %s initialized for %s", taskID, t.taskType)

	return nil
}

// closeLogging properly closes the log file
func (t *BaseTask) closeLogging() {
	if t.logFile != nil {
		t.LogInfo("Task completed, closing log file")
		t.logFile.Close()
		t.logFile = nil
		t.logger = nil
	}
}

// LogInfo writes an info-level log message to both glog and task log file
func (t *BaseTask) LogInfo(format string, args ...interface{}) {
	message := fmt.Sprintf(format, args...)

	// Always log to task file if available
	if t.logger != nil {
		t.logger.Printf("[INFO] %s", message)
	}
}

// LogError writes an error-level log message to both glog and task log file
func (t *BaseTask) LogError(format string, args ...interface{}) {
	message := fmt.Sprintf(format, args...)

	// Always log to task file if available
	if t.logger != nil {
		t.logger.Printf("[ERROR] %s", message)
	}
}

// LogDebug writes a debug-level log message to task log file
func (t *BaseTask) LogDebug(format string, args ...interface{}) {
	message := fmt.Sprintf(format, args...)

	// Always log to task file if available
	if t.logger != nil {
		t.logger.Printf("[DEBUG] %s", message)
	}
}

// LogWarning writes a warning-level log message to both glog and task log file
func (t *BaseTask) LogWarning(format string, args ...interface{}) {
	message := fmt.Sprintf(format, args...)

	// Always log to task file if available
	if t.logger != nil {
		t.logger.Printf("[WARNING] %s", message)
	}
}

// GetLogFilePath returns the path to the task's log file
func (t *BaseTask) GetLogFilePath() string {
	return t.logFilePath
}

// TaskRegistry manages task factories
type TaskRegistry struct {
	factories map[types.TaskType]types.TaskFactory
	mutex     sync.RWMutex
}

// NewTaskRegistry creates a new task registry
func NewTaskRegistry() *TaskRegistry {
	return &TaskRegistry{
		factories: make(map[types.TaskType]types.TaskFactory),
	}
}

// Register registers a task factory
func (r *TaskRegistry) Register(taskType types.TaskType, factory types.TaskFactory) {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	r.factories[taskType] = factory
}

// CreateTask creates a task instance
func (r *TaskRegistry) CreateTask(taskType types.TaskType, params types.TaskParams) (types.TaskInterface, error) {
	r.mutex.RLock()
	factory, exists := r.factories[taskType]
	r.mutex.RUnlock()

	if !exists {
		return nil, &UnsupportedTaskTypeError{TaskType: taskType}
	}

	return factory.Create(params)
}

// GetSupportedTypes returns all supported task types
func (r *TaskRegistry) GetSupportedTypes() []types.TaskType {
	r.mutex.RLock()
	defer r.mutex.RUnlock()

	types := make([]types.TaskType, 0, len(r.factories))
	for taskType := range r.factories {
		types = append(types, taskType)
	}
	return types
}

// GetFactory returns the factory for a task type
func (r *TaskRegistry) GetFactory(taskType types.TaskType) (types.TaskFactory, bool) {
	r.mutex.RLock()
	defer r.mutex.RUnlock()
	factory, exists := r.factories[taskType]
	return factory, exists
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
			if params.Server == "" {
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
