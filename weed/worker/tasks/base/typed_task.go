package base

import (
	"errors"
	"sync"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb/worker_pb"
	"github.com/seaweedfs/seaweedfs/weed/worker/types"
)

// BaseTypedTask provides a base implementation for typed tasks
type BaseTypedTask struct {
	taskType         types.TaskType
	progress         float64
	progressCallback func(float64)
	cancelled        bool
	mutex            sync.RWMutex
}

// NewBaseTypedTask creates a new base typed task
func NewBaseTypedTask(taskType types.TaskType) *BaseTypedTask {
	return &BaseTypedTask{
		taskType: taskType,
		progress: 0.0,
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
	bt.progress = progress
	bt.mutex.Unlock()

	if callback != nil {
		callback(progress)
	}
}

// SetProgressCallback sets the progress callback function
func (bt *BaseTypedTask) SetProgressCallback(callback func(float64)) {
	bt.mutex.Lock()
	defer bt.mutex.Unlock()
	bt.progressCallback = callback
}

// ValidateTyped provides basic validation for typed parameters
func (bt *BaseTypedTask) ValidateTyped(params *worker_pb.TaskParams) error {
	if params == nil {
		return errors.New("task parameters cannot be nil")
	}
	if params.VolumeId == 0 {
		return errors.New("volume_id is required")
	}
	if params.Server == "" {
		return errors.New("server is required")
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
