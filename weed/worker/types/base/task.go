package base

import (
	"context"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb/worker_pb"
	"github.com/seaweedfs/seaweedfs/weed/worker/types"
)

// UnifiedBaseTask provides common task functionality
type UnifiedBaseTask struct {
	id               string
	taskType         types.TaskType
	progressCallback func(float64)
	logger           types.Logger
	cancelled        bool
}

// NewUnifiedBaseTask creates a new base task
func NewUnifiedBaseTask(id string, taskType types.TaskType) *UnifiedBaseTask {
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
func (t *UnifiedBaseTask) Type() types.TaskType {
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

// GetProgress returns current progress
func (t *UnifiedBaseTask) GetProgress() float64 {
	// Subclasses should override this
	return 0
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
func (t *UnifiedBaseTask) SetLogger(logger types.Logger) {
	t.logger = logger
}

// GetLogger returns the task logger
func (t *UnifiedBaseTask) GetLogger() types.Logger {
	return t.logger
}

// Execute implements the UnifiedTask interface
func (t *UnifiedBaseTask) Execute(ctx context.Context, params *worker_pb.TaskParams) error {
	// Subclasses must implement this
	return nil
}

// Validate implements the UnifiedTask interface
func (t *UnifiedBaseTask) Validate(params *worker_pb.TaskParams) error {
	// Subclasses must implement this
	return nil
}

// EstimateTime implements the UnifiedTask interface
func (t *UnifiedBaseTask) EstimateTime(params *worker_pb.TaskParams) time.Duration {
	// Subclasses must implement this
	return 0
}
