package gateway

import (
	"context"

	"github.com/seaweedfs/seaweedfs/weed/mq/kafka/protocol"
)

// NewTestServerForUnitTests creates a server for unit testing without requiring SeaweedMQ masters
// This should ONLY be used for unit tests that don't need real SeaweedMQ functionality
func NewTestServerForUnitTests(opts Options) *Server {
	ctx, cancel := context.WithCancel(context.Background())

	// Create handler for unit tests
	handler := protocol.NewHandlerForUnitTests()

	return &Server{
		opts:    opts,
		ctx:     ctx,
		cancel:  cancel,
		handler: handler,
	}
}
