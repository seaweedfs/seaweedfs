package types

// This file contains the unified factory interfaces.

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

// TaskFactory creates new task instances
type TaskFactory interface {
	Create(params *worker_pb.TaskParams) (Task, error)
	Type() string
	Description() string
	Capabilities() []string
}

// TaskCreationConfig defines task creation configuration
type TaskCreationConfig struct {
	ID         string
	Type       TaskType
	Server     string
	Collection string
	VolumeID   uint32
	Logger     Logger
}

// WorkerCreationConfig encapsulates all worker configuration
type WorkerCreationConfig struct {
	ID                  string
	Capabilities        []TaskType
	MaxConcurrent       int
	HeartbeatInterval   time.Duration
	TaskRequestInterval time.Duration
	LoggerFactory       LoggerFactory
}

// WorkerFactory creates new worker instances
type WorkerFactory = Factory[Worker, WorkerConfig]
