package types

// This file contains the new unified worker interfaces that will replace
// the existing WorkerInterface.

import (
	"context"
)

// Worker defines core worker functionality
type Worker interface {
	// Core operations
	Start(ctx context.Context) error
	Stop(ctx context.Context) error
	HandleTask(ctx context.Context, task Task) error

	// Status
	GetStatus() WorkerStatus
	GetCapabilities() []TaskType

	// Configuration
	Configure(config WorkerCreationConfig) error
}

// BaseWorker provides common worker functionality
type BaseWorker struct {
	id            string
	capabilities  []TaskType
	maxConcurrent int
	currentTasks  map[string]Task
	logger        Logger
}

// NewBaseWorker creates a new base worker
func NewBaseWorker(id string) *BaseWorker {
	return &BaseWorker{
		id:           id,
		currentTasks: make(map[string]Task),
	}
}

// Configure applies worker configuration
func (w *BaseWorker) Configure(config WorkerCreationConfig) error {
	w.id = config.ID
	w.capabilities = config.Capabilities
	w.maxConcurrent = config.MaxConcurrent

	if config.LoggerFactory != nil {
		logger, err := config.LoggerFactory.CreateLogger(context.Background(), LoggerConfig{
			ServiceName: "worker-" + w.id,
			MinLevel:    LogLevelInfo,
		})
		if err != nil {
			return err
		}
		w.logger = logger
	}

	return nil
}

// GetCapabilities returns worker capabilities
func (w *BaseWorker) GetCapabilities() []TaskType {
	return w.capabilities
}

// GetStatus returns current worker status
func (w *BaseWorker) GetStatus() WorkerStatus {
	return WorkerStatus{
		WorkerID:      w.id,
		Status:        "active",
		Capabilities:  w.capabilities,
		MaxConcurrent: w.maxConcurrent,
		CurrentLoad:   len(w.currentTasks),
	}
}
