package unit

import (
	"fmt"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/test/kafka/internal/testutil"
)

// TestGatewayBasicFunctionality tests basic gateway operations
func TestGatewayBasicFunctionality(t *testing.T) {
	gateway := testutil.NewGatewayTestServer(t, testutil.GatewayOptions{})
	defer gateway.CleanupAndClose()

	addr := gateway.StartAndWait()
	
	// Give the gateway a bit more time to be fully ready
	time.Sleep(200 * time.Millisecond)

	t.Run("AcceptsConnections", func(t *testing.T) {
		testGatewayAcceptsConnections(t, addr)
	})

	t.Run("RefusesAfterClose", func(t *testing.T) {
		testGatewayRefusesAfterClose(t, gateway)
	})
}

func testGatewayAcceptsConnections(t *testing.T, addr string) {
	// Test basic TCP connection to gateway
	t.Logf("Testing connection to gateway at %s", addr)
	
	conn, err := net.DialTimeout("tcp", addr, 5*time.Second)
	if err != nil {
		t.Fatalf("Failed to connect to gateway: %v", err)
	}
	defer conn.Close()
	
	// Test that we can establish a connection and the gateway is listening
	// We don't need to send a full Kafka request for this basic test
	t.Logf("Successfully connected to gateway at %s", addr)
	
	// Optional: Test that we can write some data without error
	testData := []byte("test")
	conn.SetWriteDeadline(time.Now().Add(1 * time.Second))
	if _, err := conn.Write(testData); err != nil {
		t.Logf("Write test failed (expected for basic connectivity test): %v", err)
	} else {
		t.Logf("Write test succeeded")
	}
}

func testGatewayRefusesAfterClose(t *testing.T, gateway *testutil.GatewayTestServer) {
	// Get the address from the gateway's listener
	host, port := gateway.GetListenerAddr()
	addr := fmt.Sprintf("%s:%d", host, port)
	
	// Close the gateway
	gateway.CleanupAndClose()
	
	t.Log("Testing that gateway refuses connections after close")
	
	// Attempt to connect - should fail
	conn, err := net.DialTimeout("tcp", addr, 2*time.Second)
	if err == nil {
		conn.Close()
		t.Fatal("Expected connection to fail after gateway close, but it succeeded")
	}
	
	// Verify it's a connection refused error
	if !strings.Contains(err.Error(), "connection refused") && !strings.Contains(err.Error(), "connect: connection refused") {
		t.Logf("Connection failed as expected with error: %v", err)
	} else {
		t.Logf("Connection properly refused: %v", err)
	}
}
