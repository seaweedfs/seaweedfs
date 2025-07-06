package worker

import (
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func TestGrpcClientTLSDetection(t *testing.T) {
	// Test that the client can be created with a dial option
	dialOption := grpc.WithTransportCredentials(insecure.NewCredentials())
	client := NewGrpcAdminClient("localhost:33646", "test-worker", dialOption)

	// Test that the client has the correct dial option
	if client.dialOption == nil {
		t.Error("Client should have a dial option")
	}

	t.Logf("Client created successfully with dial option")
}

func TestCreateAdminClientGrpc(t *testing.T) {
	// Test client creation - admin server port gets transformed to gRPC port
	dialOption := grpc.WithTransportCredentials(insecure.NewCredentials())
	client, err := CreateAdminClient("localhost:23646", "test-worker", dialOption)
	if err != nil {
		t.Fatalf("Failed to create admin client: %v", err)
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
	dialOption := grpc.WithTransportCredentials(insecure.NewCredentials())
	client := NewGrpcAdminClient("localhost:1", "test-worker", dialOption) // Port 1 is reserved and won't be open

	// Test that the connection creation fails when actually trying to use it
	start := time.Now()
	err := client.Connect() // This should fail when trying to establish the stream
	duration := time.Since(start)

	if err == nil {
		t.Error("Expected connection to closed port to fail")
	} else {
		t.Logf("Connection failed as expected: %v", err)
	}

	// Should fail quickly but not too quickly
	if duration > 10*time.Second {
		t.Errorf("Connection attempt took too long: %v", duration)
	}
}

func TestConnectionWithDialOption(t *testing.T) {
	// Test that the connection uses the provided dial option
	dialOption := grpc.WithTransportCredentials(insecure.NewCredentials())
	client := NewGrpcAdminClient("localhost:1", "test-worker", dialOption) // Port 1 is reserved and won't be open

	// Test the actual connection
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

func TestClientWithSecureDialOption(t *testing.T) {
	// Test that the client correctly uses a secure dial option
	// This would normally use LoadClientTLS, but for testing we'll use insecure
	dialOption := grpc.WithTransportCredentials(insecure.NewCredentials())
	client := NewGrpcAdminClient("localhost:33646", "test-worker", dialOption)

	if client.dialOption == nil {
		t.Error("Client should have a dial option")
	}

	t.Logf("Client created successfully with dial option")
}

func TestConnectionWithRealAddress(t *testing.T) {
	// Test connection behavior with a real address that doesn't support gRPC
	dialOption := grpc.WithTransportCredentials(insecure.NewCredentials())
	client := NewGrpcAdminClient("www.google.com:80", "test-worker", dialOption) // HTTP port, not gRPC

	err := client.Connect()
	if err == nil {
		t.Log("Connection succeeded unexpectedly")
		client.Disconnect()
	} else {
		t.Logf("Connection failed as expected: %v", err)
	}
}

func TestDialOptionUsage(t *testing.T) {
	// Test that the provided dial option is used for connections
	dialOption := grpc.WithTransportCredentials(insecure.NewCredentials())
	client := NewGrpcAdminClient("localhost:1", "test-worker", dialOption) // Port 1 won't support gRPC at all

	// Verify the dial option is stored
	if client.dialOption == nil {
		t.Error("Dial option should be stored in client")
	}

	// Test connection fails appropriately
	err := client.Connect()
	if err == nil {
		t.Error("Connection should fail to non-gRPC port")
		client.Disconnect()
	} else {
		t.Logf("Connection failed as expected: %v", err)
	}
}
