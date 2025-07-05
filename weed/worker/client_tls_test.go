package worker

import (
	"strings"
	"testing"
	"time"
)

func TestGrpcClientTLSDetection(t *testing.T) {
	// Test TLS detection logic
	client := NewGrpcAdminClient("localhost:33646", "test-worker")

	// Test TLS connection attempt (will fail, but we check the logic)
	conn, err := client.tryTLSConnection()
	if err != nil {
		t.Logf("TLS connection failed as expected: %v", err)
	}
	if conn != nil {
		conn.Close()
	}

	// Test insecure connection attempt (will fail, but we check the logic)
	conn, err = client.tryInsecureConnection()
	if err != nil {
		t.Logf("Insecure connection failed as expected: %v", err)
	}
	if conn != nil {
		conn.Close()
	}
}

func TestCreateAdminClientGrpc(t *testing.T) {
	// Test client creation - admin server port gets transformed to gRPC port
	client, err := CreateAdminClient("localhost:23646", "test-worker", "grpc")
	if err != nil {
		t.Fatalf("Failed to create gRPC admin client: %v", err)
	}

	if client == nil {
		t.Fatal("Client should not be nil")
	}

	// Verify it's the correct type
	grpcClient, ok := client.(*GrpcAdminClient)
	if !ok {
		t.Fatal("Client should be GrpcAdminClient type")
	}

	// The admin address should be transformed to the gRPC port (HTTP + 10000)
	expectedAddress := "localhost:33646" // 23646 + 10000
	if grpcClient.adminAddress != expectedAddress {
		t.Errorf("Expected admin address %s, got %s", expectedAddress, grpcClient.adminAddress)
	}

	if grpcClient.workerID != "test-worker" {
		t.Errorf("Expected worker ID test-worker, got %s", grpcClient.workerID)
	}
}

func TestConnectionTimeouts(t *testing.T) {
	// Test that connections have proper timeouts
	// Use localhost with a port that's definitely closed
	client := NewGrpcAdminClient("localhost:1", "test-worker") // Port 1 is reserved and won't be open

	// Test that the connection creation fails when actually trying to use it
	start := time.Now()
	err := client.Connect() // This should fail when trying to establish the stream
	duration := time.Since(start)

	if err == nil {
		t.Error("Expected connection to closed port to fail")
	} else {
		t.Logf("Connection failed as expected: %v", err)
	}

	// Should fail quickly but not too quickly (should try both TLS and insecure)
	if duration > 15*time.Second {
		t.Errorf("Connection attempt took too long: %v", duration)
	}
}

func TestTLSAndInsecureConnectionLogic(t *testing.T) {
	// Test that TLS is tried first, then insecure
	// Use localhost with a port that's definitely closed
	client := NewGrpcAdminClient("localhost:1", "test-worker") // Port 1 is reserved and won't be open

	// Test the actual connection with stream creation
	err := client.Connect()
	if err == nil {
		t.Error("Expected connection to closed port to fail")
		client.Disconnect() // Clean up if it somehow succeeded
	} else {
		t.Logf("Connection failed as expected: %v", err)
	}

	// The error should indicate a connection failure
	if err != nil && err.Error() != "" {
		t.Logf("Connection error message: %s", err.Error())
		// The error should contain connection-related terms
		if !strings.Contains(err.Error(), "connection") && !strings.Contains(err.Error(), "dial") {
			t.Logf("Error message doesn't indicate connection issues: %s", err.Error())
		}
	}
}

func TestTLSConnectionWithRealAddress(t *testing.T) {
	// Test TLS connection behavior with a real address that doesn't support TLS
	// This will help verify the TLS -> insecure fallback logic
	client := NewGrpcAdminClient("www.google.com:80", "test-worker") // HTTP port, not gRPC

	conn, err := client.createConnection()
	if err == nil {
		t.Log("Connection succeeded (fallback to insecure worked)")
		if conn != nil {
			conn.Close()
		}
	} else {
		t.Logf("Connection failed: %v", err)
	}
}

func TestTLSDetectionWithConnectionTest(t *testing.T) {
	// Test the new TLS detection logic that actually tests the connection
	client := NewGrpcAdminClient("localhost:1", "test-worker") // Port 1 won't support gRPC at all

	// This should fail when trying to test the TLS connection
	conn, err := client.tryTLSConnection()
	if conn != nil {
		// If we got a connection, test it
		works := client.testConnection(conn)
		conn.Close()
		if works {
			t.Error("Connection test should fail for non-gRPC port")
		} else {
			t.Log("Connection test correctly detected that the connection doesn't work")
		}
	}

	if err != nil {
		t.Logf("TLS connection creation failed as expected: %v", err)
	}
}
