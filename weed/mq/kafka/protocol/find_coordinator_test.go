package protocol

import (
	"testing"
)

func TestFindCoordinatorForGroup(t *testing.T) {
	tests := []struct {
		name        string
		groupID     string
		brokerHost  string
		brokerPort  int
		expectError bool
	}{
		{
			name:        "valid group with broker info",
			groupID:     "test-group-1",
			brokerHost:  "localhost",
			brokerPort:  9092,
			expectError: false,
		},
		{
			name:        "different group same broker",
			groupID:     "test-group-2",
			brokerHost:  "localhost",
			brokerPort:  9092,
			expectError: false,
		},
		{
			name:        "empty broker host",
			groupID:     "test-group-3",
			brokerHost:  "",
			brokerPort:  9092,
			expectError: false, // Should still work, returns empty host
		},
		{
			name:        "zero broker port",
			groupID:     "test-group-4",
			brokerHost:  "localhost",
			brokerPort:  0,
			expectError: false, // Should still work, returns zero port
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			handler := &Handler{
				brokerHost: tt.brokerHost,
				brokerPort: tt.brokerPort,
			}

			host, port, nodeID, err := handler.findCoordinatorForGroup(tt.groupID)

			if tt.expectError && err == nil {
				t.Error("Expected error but got none")
				return
			}

			if !tt.expectError && err != nil {
				t.Errorf("Unexpected error: %v", err)
				return
			}

			// For now, the function should always return the current gateway
			if host != tt.brokerHost {
				t.Errorf("Expected host %s, got %s", tt.brokerHost, host)
			}

			if port != tt.brokerPort {
				t.Errorf("Expected port %d, got %d", tt.brokerPort, port)
			}

			if nodeID != 1 {
				t.Errorf("Expected nodeID 1, got %d", nodeID)
			}

			t.Logf("Group %s -> Coordinator %s:%d (node %d)", tt.groupID, host, port, nodeID)
		})
	}
}

func TestFindCoordinatorConsistency(t *testing.T) {
	// Test that the same group ID always returns the same coordinator
	handler := &Handler{
		brokerHost: "localhost",
		brokerPort: 9092,
	}

	groupID := "consistent-test-group"

	// Call multiple times and ensure consistency
	var firstHost string
	var firstPort int
	var firstNodeID int32

	for i := 0; i < 10; i++ {
		host, port, nodeID, err := handler.findCoordinatorForGroup(groupID)
		if err != nil {
			t.Fatalf("Unexpected error on iteration %d: %v", i, err)
		}

		if i == 0 {
			firstHost = host
			firstPort = port
			firstNodeID = nodeID
		} else {
			if host != firstHost || port != firstPort || nodeID != firstNodeID {
				t.Errorf("Inconsistent coordinator on iteration %d: got %s:%d (node %d), expected %s:%d (node %d)",
					i, host, port, nodeID, firstHost, firstPort, firstNodeID)
			}
		}
	}

	t.Logf("Coordinator for group %s is consistently %s:%d (node %d)", groupID, firstHost, firstPort, firstNodeID)
}

func TestFindCoordinatorV0Request(t *testing.T) {
	handler := &Handler{
		brokerHost: "test-broker",
		brokerPort: 9093,
	}

	// Create a simple FindCoordinator v0 request
	// Format: coordinator_key_size (2 bytes) + coordinator_key
	groupID := "test-group"
	requestBody := make([]byte, 2+len(groupID))
	requestBody[0] = 0 // High byte of length
	requestBody[1] = byte(len(groupID)) // Low byte of length
	copy(requestBody[2:], []byte(groupID))

	response, err := handler.handleFindCoordinatorV0(12345, requestBody)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	if len(response) == 0 {
		t.Fatal("Empty response")
	}

	// Basic validation - response should contain correlation ID and coordinator info
	if len(response) < 4 {
		t.Errorf("Response too short: %d bytes", len(response))
	}

	t.Logf("FindCoordinator v0 response: %d bytes", len(response))
}

func TestFindCoordinatorV2Request(t *testing.T) {
	handler := &Handler{
		brokerHost: "test-broker",
		brokerPort: 9093,
	}

	// Create a simple FindCoordinator v2 request
	// Format: coordinator_key_size (2 bytes) + coordinator_key + coordinator_type (1 byte)
	groupID := "test-group"
	requestBody := make([]byte, 2+len(groupID)+1)
	requestBody[0] = 0 // High byte of length
	requestBody[1] = byte(len(groupID)) // Low byte of length
	copy(requestBody[2:2+len(groupID)], []byte(groupID))
	requestBody[2+len(groupID)] = 0 // Coordinator type (0 = consumer group)

	response, err := handler.handleFindCoordinatorV2(12345, requestBody)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	if len(response) == 0 {
		t.Fatal("Empty response")
	}

	// Basic validation - response should contain correlation ID and coordinator info
	if len(response) < 4 {
		t.Errorf("Response too short: %d bytes", len(response))
	}

	t.Logf("FindCoordinator v2 response: %d bytes", len(response))
}
