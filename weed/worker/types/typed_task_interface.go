package types

import (
	"errors"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb/worker_pb"
)

var (
	// ErrTaskTypeNotFound is returned when a task type is not registered
	ErrTaskTypeNotFound = errors.New("task type not found")
)

// TypedTaskInterface defines the interface for tasks using typed protobuf parameters
type TypedTaskInterface interface {
	// Execute the task with typed protobuf parameters
	ExecuteTyped(params *worker_pb.TaskParams) error

	// Validate typed task parameters
	ValidateTyped(params *worker_pb.TaskParams) error

	// Estimate execution time based on typed parameters
	EstimateTimeTyped(params *worker_pb.TaskParams) time.Duration

	// Get task type
	GetType() TaskType

	// Check if task can be cancelled
	IsCancellable() bool

	// Cancel the task if running
	Cancel() error

	// Get current progress (0-100)
	GetProgress() float64

	// Set progress callback for progress updates
	SetProgressCallback(callback func(float64))
}

// TypedTaskCreator is a function that creates a new typed task instance
type TypedTaskCreator func() TypedTaskInterface

// TypedTaskRegistry manages typed task creation
type TypedTaskRegistry struct {
	creators map[TaskType]TypedTaskCreator
}

// NewTypedTaskRegistry creates a new typed task registry
func NewTypedTaskRegistry() *TypedTaskRegistry {
	return &TypedTaskRegistry{
		creators: make(map[TaskType]TypedTaskCreator),
	}
}

// RegisterTypedTask registers a typed task creator
func (r *TypedTaskRegistry) RegisterTypedTask(taskType TaskType, creator TypedTaskCreator) {
	r.creators[taskType] = creator
}

// CreateTypedTask creates a new typed task instance
func (r *TypedTaskRegistry) CreateTypedTask(taskType TaskType) (TypedTaskInterface, error) {
	creator, exists := r.creators[taskType]
	if !exists {
		return nil, ErrTaskTypeNotFound
	}
	return creator(), nil
}

// GetSupportedTypes returns all registered typed task types
func (r *TypedTaskRegistry) GetSupportedTypes() []TaskType {
	types := make([]TaskType, 0, len(r.creators))
	for taskType := range r.creators {
		types = append(types, taskType)
	}
	return types
}

// Global typed task registry
var globalTypedTaskRegistry = NewTypedTaskRegistry()

// RegisterGlobalTypedTask registers a typed task globally
func RegisterGlobalTypedTask(taskType TaskType, creator TypedTaskCreator) {
	globalTypedTaskRegistry.RegisterTypedTask(taskType, creator)
}

// GetGlobalTypedTaskRegistry returns the global typed task registry
func GetGlobalTypedTaskRegistry() *TypedTaskRegistry {
	return globalTypedTaskRegistry
}
