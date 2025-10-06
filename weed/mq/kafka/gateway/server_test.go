package gateway

import (
	"context"

	"github.com/seaweedfs/seaweedfs/weed/mq/kafka/consumer"
	"github.com/seaweedfs/seaweedfs/weed/mq/kafka/protocol"
)

// Note: These test helper functions are currently not used
// Integration tests should use real SeaweedMQ broker handlers instead

// NewTestServerWithHandler creates a test server with a custom handler
// This allows tests to inject specific handlers for different scenarios
func NewTestServerWithHandler(opts Options, handler *protocol.Handler) *Server {
	ctx, cancel := context.WithCancel(context.Background())

	return &Server{
		opts:    opts,
		ctx:     ctx,
		cancel:  cancel,
		handler: handler,
	}
}

// NewTestServerForUnitTests creates a test server with a minimal mock handler for unit tests
// This allows basic gateway functionality testing without requiring SeaweedMQ masters
func NewTestServerForUnitTests(opts Options) *Server {
	ctx, cancel := context.WithCancel(context.Background())

	// Create a minimal handler with in-memory group coordinator
	handler := &protocol.Handler{}
	handler.SetGroupCoordinator(consumer.NewGroupCoordinator())

	return &Server{
		opts:    opts,
		ctx:     ctx,
		cancel:  cancel,
		handler: handler,
	}
}
