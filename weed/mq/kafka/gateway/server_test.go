package gateway

import (
	"context"

	"github.com/seaweedfs/seaweedfs/weed/mq/kafka/protocol"
)

// NewTestServer creates a server for testing with in-memory handlers
// This should ONLY be used for testing
func NewTestServer(opts Options) *Server {
	ctx, cancel := context.WithCancel(context.Background())

	// Use test handler with storage capability
	handler := protocol.NewTestHandler()

	return &Server{
		opts:    opts,
		ctx:     ctx,
		cancel:  cancel,
		handler: handler,
	}
}

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
