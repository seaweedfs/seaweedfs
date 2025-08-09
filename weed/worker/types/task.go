package types

// This file contains the new unified task interfaces that will replace
// the existing TaskInterface and TypedTaskInterface.

import (
	"context"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/worker_pb"
)

// Task defines the core task interface that all tasks must implement
type Task interface {
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
	SetProgressCallback(func(float64, string))
}

// TaskWithLogging extends Task with logging capabilities
type TaskWithLogging interface {
	Task
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

// NoOpLogger is a logger that does nothing (silent)
type NoOpLogger struct{}

func (l *NoOpLogger) Info(msg string, args ...interface{})    {}
func (l *NoOpLogger) Warning(msg string, args ...interface{}) {}
func (l *NoOpLogger) Error(msg string, args ...interface{})   {}
func (l *NoOpLogger) Debug(msg string, args ...interface{})   {}
func (l *NoOpLogger) WithFields(fields map[string]interface{}) Logger {
	return l // Return self since we're doing nothing anyway
}

// GlogFallbackLogger is a logger that falls back to glog
type GlogFallbackLogger struct{}

func (l *GlogFallbackLogger) Info(msg string, args ...interface{}) {
	if len(args) > 0 {
		glog.Infof(msg, args...)
	} else {
		glog.Info(msg)
	}
}

func (l *GlogFallbackLogger) Warning(msg string, args ...interface{}) {
	if len(args) > 0 {
		glog.Warningf(msg, args...)
	} else {
		glog.Warning(msg)
	}
}

func (l *GlogFallbackLogger) Error(msg string, args ...interface{}) {
	if len(args) > 0 {
		glog.Errorf(msg, args...)
	} else {
		glog.Error(msg)
	}
}

func (l *GlogFallbackLogger) Debug(msg string, args ...interface{}) {
	if len(args) > 0 {
		glog.V(1).Infof(msg, args...)
	} else {
		glog.V(1).Info(msg)
	}
}

func (l *GlogFallbackLogger) WithFields(fields map[string]interface{}) Logger {
	// For glog fallback, we'll just return self and ignore fields for simplicity
	// A more sophisticated implementation could format the fields into the message
	return l
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
	progressCallback func(float64, string)
	logger           Logger
	cancelled        bool
	currentStage     string
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
func (t *UnifiedBaseTask) SetProgressCallback(callback func(float64, string)) {
	t.progressCallback = callback
}

// ReportProgress reports current progress through the callback
func (t *UnifiedBaseTask) ReportProgress(progress float64) {
	if t.progressCallback != nil {
		t.progressCallback(progress, t.currentStage)
	}
}

// ReportProgressWithStage reports current progress with a specific stage description
func (t *UnifiedBaseTask) ReportProgressWithStage(progress float64, stage string) {
	t.currentStage = stage
	if t.progressCallback != nil {
		t.progressCallback(progress, stage)
	}
}

// SetCurrentStage sets the current stage description
func (t *UnifiedBaseTask) SetCurrentStage(stage string) {
	t.currentStage = stage
}

// GetCurrentStage returns the current stage description
func (t *UnifiedBaseTask) GetCurrentStage() string {
	return t.currentStage
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
