package testutil

import (
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/mq/kafka/gateway"
)

// GatewayTestServer wraps the gateway server with common test utilities
type GatewayTestServer struct {
	*gateway.Server
	t *testing.T
}

// GatewayOptions contains configuration for test gateway
type GatewayOptions struct {
	Listen        string
	Masters       string
	UseProduction bool
	// Add more options as needed
}

// NewGatewayTestServer creates a new test gateway server with common setup
func NewGatewayTestServer(t *testing.T, opts GatewayOptions) *GatewayTestServer {
	if opts.Listen == "" {
		opts.Listen = "127.0.0.1:0" // Use random port by default
	}

	// Allow switching to production gateway if requested (requires masters)
	var srv *gateway.Server
	if opts.UseProduction {
		if opts.Masters == "" {
			// Fallback to env variable for convenience in CI
			if v := os.Getenv("SEAWEEDFS_MASTERS"); v != "" {
				opts.Masters = v
			} else {
				opts.Masters = "localhost:9333"
			}
		}
		srv = gateway.NewServer(gateway.Options{
			Listen:  opts.Listen,
			Masters: opts.Masters,
		})
	} else {
		srv = gateway.NewTestServer(gateway.Options{
			Listen: opts.Listen,
		})
	}

	return &GatewayTestServer{
		Server: srv,
		t:      t,
	}
}

// StartAndWait starts the gateway and waits for it to be ready
func (g *GatewayTestServer) StartAndWait() string {
	g.t.Helper()

	// Start server in goroutine
	go func() {
		if err := g.Start(); err != nil {
			g.t.Errorf("Failed to start gateway: %v", err)
		}
	}()

	// Wait for server to be ready
	time.Sleep(100 * time.Millisecond)

	host, port := g.GetListenerAddr()
	addr := fmt.Sprintf("%s:%d", host, port)
	g.t.Logf("Gateway running on %s", addr)

	return addr
}

// AddTestTopic adds a topic for testing with default configuration
func (g *GatewayTestServer) AddTestTopic(name string) {
	g.t.Helper()
	g.GetHandler().AddTopicForTesting(name, 1)
	g.t.Logf("Added test topic: %s", name)
}

// AddTestTopics adds multiple topics for testing
func (g *GatewayTestServer) AddTestTopics(names ...string) {
	g.t.Helper()
	for _, name := range names {
		g.AddTestTopic(name)
	}
}

// CleanupAndClose properly closes the gateway server
func (g *GatewayTestServer) CleanupAndClose() {
	g.t.Helper()
	if err := g.Close(); err != nil {
		g.t.Errorf("Failed to close gateway: %v", err)
	}
}
