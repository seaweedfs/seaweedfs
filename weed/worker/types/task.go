package types

// This file contains the new unified task interfaces that will replace
// the existing TaskInterface and TypedTaskInterface.

import (
	"context"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb/worker_pb"
)

// UnifiedTask defines the core task interface that all tasks must implement
// This will replace the existing TaskInterface and TypedTaskInterface
type UnifiedTask interface {
	// Core identity
	ID() string
	Type() TaskType

	// Execution
	Execute(ctx context.Context, params *worker_pb.TaskParams) error
	Validate(params *worker_pb.TaskParams) error
	EstimateTime(params *worker_pb.TaskParams) time.Duration

	// Control
	Cancel() error
	IsCancellable() bool

	// Progress
	GetProgress() float64
	SetProgressCallback(func(float64))
}

// TaskWithLogging extends Task with logging capabilities
type UnifiedTaskWithLogging interface {
	UnifiedTask
	Logger
}

// Logger defines standard logging interface
type Logger interface {
	Info(msg string, args ...interface{})
	Warning(msg string, args ...interface{})
	Error(msg string, args ...interface{})
	Debug(msg string, args ...interface{})
	WithFields(fields map[string]interface{}) Logger
}

// LogLevel represents logging severity levels
type LogLevel int

const (
	LogLevelDebug LogLevel = iota
	LogLevelInfo
	LogLevelWarning
	LogLevelError
)

// LoggerConfig defines logger configuration
type LoggerConfig struct {
	MinLevel    LogLevel
	MaxSize     int64
	MaxFiles    int
	Directory   string
	ServiceName string
	EnableJSON  bool
}

// LoggerFactory creates configured loggers
type LoggerFactory interface {
	CreateLogger(ctx context.Context, config LoggerConfig) (Logger, error)
}

// BaseTask provides common task functionality
type UnifiedBaseTask struct {
	id               string
	taskType         TaskType
	progressCallback func(float64)
	logger           Logger
	cancelled        bool
}

// NewBaseTask creates a new base task
func NewUnifiedBaseTask(id string, taskType TaskType) *UnifiedBaseTask {
	return &UnifiedBaseTask{
		id:       id,
		taskType: taskType,
	}
}

// ID returns the task ID
func (t *UnifiedBaseTask) ID() string {
	return t.id
}

// Type returns the task type
func (t *UnifiedBaseTask) Type() TaskType {
	return t.taskType
}

// SetProgressCallback sets the progress callback
func (t *UnifiedBaseTask) SetProgressCallback(callback func(float64)) {
	t.progressCallback = callback
}

// ReportProgress reports current progress through the callback
func (t *UnifiedBaseTask) ReportProgress(progress float64) {
	if t.progressCallback != nil {
		t.progressCallback(progress)
	}
}

// Cancel marks the task as cancelled
func (t *UnifiedBaseTask) Cancel() error {
	t.cancelled = true
	return nil
}

// IsCancellable returns true if the task can be cancelled
func (t *UnifiedBaseTask) IsCancellable() bool {
	return true
}

// IsCancelled returns true if the task has been cancelled
func (t *UnifiedBaseTask) IsCancelled() bool {
	return t.cancelled
}

// SetLogger sets the task logger
func (t *UnifiedBaseTask) SetLogger(logger Logger) {
	t.logger = logger
}

// GetLogger returns the task logger
func (t *UnifiedBaseTask) GetLogger() Logger {
	return t.logger
}
