package worker

import (
	"context"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func TestGrpcConnection(t *testing.T) {
	// Test that we can create a gRPC connection with insecure credentials
	// This tests the connection setup without requiring a running server
	adminAddress := "localhost:33646" // gRPC port for admin server on port 23646

	// This should not fail with transport security errors
	conn, err := pb.GrpcDial(context.Background(), adminAddress, false, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		// Connection failure is expected when no server is running
		// But it should NOT be a transport security error
		if err.Error() == "grpc: no transport security set" {
			t.Fatalf("Transport security error should not occur with insecure credentials: %v", err)
		}
		t.Logf("Connection failed as expected (no server running): %v", err)
	} else {
		// If connection succeeds, clean up
		conn.Close()
		t.Log("Connection succeeded")
	}
}

func TestGrpcAdminClient_Connect(t *testing.T) {
	// Test that the GrpcAdminClient can be created and attempt connection
	dialOption := grpc.WithTransportCredentials(insecure.NewCredentials())
	client := NewGrpcAdminClient("localhost:23646", "test-worker", dialOption)

	// This should not fail with transport security errors
	err := client.Connect()
	if err != nil {
		// Connection failure is expected when no server is running
		// But it should NOT be a transport security error
		if err.Error() == "grpc: no transport security set" {
			t.Fatalf("Transport security error should not occur with insecure credentials: %v", err)
		}
		t.Logf("Connection failed as expected (no server running): %v", err)
	} else {
		// If connection succeeds, clean up
		client.Disconnect()
		t.Log("Connection succeeded")
	}
}

func TestAdminAddressToGrpcAddress(t *testing.T) {
	tests := []struct {
		adminAddress string
		expected     string
	}{
		{"localhost:9333", "localhost:19333"},
		{"localhost:23646", "localhost:33646"},
		{"admin.example.com:9333", "admin.example.com:19333"},
		{"127.0.0.1:8080", "127.0.0.1:18080"},
	}

	for _, test := range tests {
		dialOption := grpc.WithTransportCredentials(insecure.NewCredentials())
		client := NewGrpcAdminClient(test.adminAddress, "test-worker", dialOption)
		result := client.adminAddress
		if result != test.expected {
			t.Errorf("For admin address %s, expected gRPC address %s, got %s",
				test.adminAddress, test.expected, result)
		}
	}
}

func TestMockAdminClient(t *testing.T) {
	// Test that the mock client works correctly
	client := NewMockAdminClient()

	// Should be able to connect/disconnect without errors
	err := client.Connect()
	if err != nil {
		t.Fatalf("Mock client connect failed: %v", err)
	}

	if !client.IsConnected() {
		t.Error("Mock client should be connected")
	}

	err = client.Disconnect()
	if err != nil {
		t.Fatalf("Mock client disconnect failed: %v", err)
	}

	if client.IsConnected() {
		t.Error("Mock client should be disconnected")
	}
}

func TestCreateAdminClient(t *testing.T) {
	// Test client creation
	dialOption := grpc.WithTransportCredentials(insecure.NewCredentials())
	client, err := CreateAdminClient("localhost:9333", "test-worker", dialOption)
	if err != nil {
		t.Fatalf("Failed to create admin client: %v", err)
	}

	if client == nil {
		t.Fatal("Client should not be nil")
	}
}
