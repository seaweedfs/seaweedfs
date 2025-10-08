package base

import (
	"context"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb/worker_pb"
	"github.com/seaweedfs/seaweedfs/weed/worker/types"
)

// BaseTask provides common task functionality
type BaseTask struct {
	id               string
	taskType         types.TaskType
	progressCallback func(float64, string) // Modified to include stage description
	logger           types.Logger
	cancelled        bool
	currentStage     string
}

// NewBaseTask creates a new base task
func NewBaseTask(id string, taskType types.TaskType) *BaseTask {
	return &BaseTask{
		id:       id,
		taskType: taskType,
		logger:   &types.GlogFallbackLogger{}, // Default fallback logger
	}
}

// ID returns the task ID
func (t *BaseTask) ID() string {
	return t.id
}

// Type returns the task type
func (t *BaseTask) Type() types.TaskType {
	return t.taskType
}

// SetProgressCallback sets the progress callback
func (t *BaseTask) SetProgressCallback(callback func(float64, string)) {
	t.progressCallback = callback
}

// ReportProgress reports current progress through the callback
func (t *BaseTask) ReportProgress(progress float64) {
	if t.progressCallback != nil {
		t.progressCallback(progress, t.currentStage)
	}
}

// ReportProgressWithStage reports current progress with a specific stage description
func (t *BaseTask) ReportProgressWithStage(progress float64, stage string) {
	t.currentStage = stage
	if t.progressCallback != nil {
		t.progressCallback(progress, stage)
	}
}

// SetCurrentStage sets the current stage description
func (t *BaseTask) SetCurrentStage(stage string) {
	t.currentStage = stage
}

// GetCurrentStage returns the current stage description
func (t *BaseTask) GetCurrentStage() string {
	return t.currentStage
}

// GetProgress returns current progress
func (t *BaseTask) GetProgress() float64 {
	// Subclasses should override this
	return 0
}

// Cancel marks the task as cancelled
func (t *BaseTask) Cancel() error {
	t.cancelled = true
	return nil
}

// IsCancellable returns true if the task can be cancelled
func (t *BaseTask) IsCancellable() bool {
	return true
}

// IsCancelled returns true if the task has been cancelled
func (t *BaseTask) IsCancelled() bool {
	return t.cancelled
}

// SetLogger sets the task logger
func (t *BaseTask) SetLogger(logger types.Logger) {
	t.logger = logger
}

// GetLogger returns the task logger
func (t *BaseTask) GetLogger() types.Logger {
	return t.logger
}

// Execute implements the Task interface
func (t *BaseTask) Execute(ctx context.Context, params *worker_pb.TaskParams) error {
	// Subclasses must implement this
	return nil
}

// Validate implements the UnifiedTask interface
func (t *BaseTask) Validate(params *worker_pb.TaskParams) error {
	// Subclasses must implement this
	return nil
}

// EstimateTime implements the UnifiedTask interface
func (t *BaseTask) EstimateTime(params *worker_pb.TaskParams) time.Duration {
	// Subclasses must implement this
	return 0
}
