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
