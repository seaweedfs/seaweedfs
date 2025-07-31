package types

// This file contains the new unified factory interfaces that will replace
// the existing factory interfaces.

import (
	"context"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb/worker_pb"
)

// Factory defines a generic factory interface
type Factory[T any, C any] interface {
	// Create new instance with context and config
	Create(ctx context.Context, config C) (T, error)

	// Metadata
	Type() string
	Description() string
	Capabilities() []string
}

// UnifiedTaskFactoryV2 creates new task instances using the new unified interface
type UnifiedTaskFactoryV2 interface {
	Create(params *worker_pb.TaskParams) (UnifiedTask, error)
	Type() string
	Description() string
	Capabilities() []string
}

// UnifiedTaskConfig defines task creation configuration
type UnifiedTaskConfig struct {
	ID         string
	Type       TaskType
	Server     string
	Collection string
	VolumeID   uint32
	Logger     Logger
}

// UnifiedTaskFactory creates new task instances
type UnifiedTaskFactory = Factory[UnifiedTask, UnifiedTaskConfig]

// UnifiedWorkerConfig encapsulates all worker configuration
type UnifiedWorkerConfig struct {
	ID                  string
	Capabilities        []TaskType
	MaxConcurrent       int
	HeartbeatInterval   time.Duration
	TaskRequestInterval time.Duration
	LoggerFactory       LoggerFactory
}

// WorkerFactory creates new worker instances
type UnifiedWorkerFactory = Factory[UnifiedWorker, UnifiedWorkerConfig]
