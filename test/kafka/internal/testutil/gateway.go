package testutil

import (
	"context"
	"fmt"
	"net"
	"os"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/mq/kafka/gateway"
	"github.com/seaweedfs/seaweedfs/weed/mq/kafka/schema"
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
		// For unit testing without real SeaweedMQ masters
		srv = gateway.NewTestServerForUnitTests(gateway.Options{
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
		// Enable schema mode automatically when SCHEMA_REGISTRY_URL is set
		if url := os.Getenv("SCHEMA_REGISTRY_URL"); url != "" {
			h := g.GetHandler()
			if h != nil {
				_ = h.EnableSchemaManagement(schema.ManagerConfig{RegistryURL: url})
			}
		}
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

// SMQAvailabilityMode indicates whether SeaweedMQ is available for testing
type SMQAvailabilityMode int

const (
	SMQUnavailable SMQAvailabilityMode = iota // Use mock handler only
	SMQAvailable                              // SMQ is available, can use production mode
	SMQRequired                               // SMQ is required, skip test if unavailable
)

// CheckSMQAvailability checks if SeaweedFS masters are available for testing
func CheckSMQAvailability() (bool, string) {
	masters := os.Getenv("SEAWEEDFS_MASTERS")
	if masters == "" {
		return false, ""
	}

	// Test if at least one master is reachable
	if masters != "" {
		// Try to connect to the first master to verify availability
		conn, err := net.DialTimeout("tcp", masters, 2*time.Second)
		if err != nil {
			return false, masters // Masters specified but unreachable
		}
		conn.Close()
		return true, masters
	}

	return false, ""
}

// NewGatewayTestServerWithSMQ creates a gateway server that automatically uses SMQ if available
func NewGatewayTestServerWithSMQ(t *testing.T, mode SMQAvailabilityMode) *GatewayTestServer {
	smqAvailable, masters := CheckSMQAvailability()

	switch mode {
	case SMQRequired:
		if !smqAvailable {
			if masters != "" {
				t.Skipf("Skipping test: SEAWEEDFS_MASTERS=%s specified but unreachable", masters)
			} else {
				t.Skip("Skipping test: SEAWEEDFS_MASTERS required but not set")
			}
		}
		t.Logf("Using SMQ-backed gateway with masters: %s", masters)
		return newGatewayTestServerWithTimeout(t, GatewayOptions{
			UseProduction: true,
			Masters:       masters,
		}, 120*time.Second)

	case SMQAvailable:
		if smqAvailable {
			t.Logf("SMQ available, using production gateway with masters: %s", masters)
			return newGatewayTestServerWithTimeout(t, GatewayOptions{
				UseProduction: true,
				Masters:       masters,
			}, 120*time.Second)
		} else {
			t.Logf("SMQ not available, using mock gateway")
			return NewGatewayTestServer(t, GatewayOptions{})
		}

	default: // SMQUnavailable
		t.Logf("Using mock gateway (SMQ integration disabled)")
		return NewGatewayTestServer(t, GatewayOptions{})
	}
}

// newGatewayTestServerWithTimeout creates a gateway server with a timeout to prevent hanging
func newGatewayTestServerWithTimeout(t *testing.T, opts GatewayOptions, timeout time.Duration) *GatewayTestServer {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	done := make(chan *GatewayTestServer, 1)
	errChan := make(chan error, 1)

	go func() {
		defer func() {
			if r := recover(); r != nil {
				errChan <- fmt.Errorf("panic creating gateway: %v", r)
			}
		}()

		// Create the gateway in a goroutine so we can timeout if it hangs
		t.Logf("Creating gateway with masters: %s (with %v timeout)", opts.Masters, timeout)
		gateway := NewGatewayTestServer(t, opts)
		t.Logf("Gateway created successfully")
		done <- gateway
	}()

	select {
	case gateway := <-done:
		return gateway
	case err := <-errChan:
		t.Fatalf("Error creating gateway: %v", err)
	case <-ctx.Done():
		t.Fatalf("Timeout creating gateway after %v - likely SMQ broker discovery failed. Check if MQ brokers are running and accessible.", timeout)
	}

	return nil // This should never be reached
}

// IsSMQMode returns true if the gateway is using real SMQ backend
// This is determined by checking if we have the SEAWEEDFS_MASTERS environment variable
func (g *GatewayTestServer) IsSMQMode() bool {
	available, _ := CheckSMQAvailability()
	return available
}
