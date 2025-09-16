package protocol

import (
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/mq/kafka/consumer"
)

func TestHandler_handleDescribeGroups(t *testing.T) {
	handler := &Handler{}

	// Test with no group coordinator
	t.Run("NoCoordinator", func(t *testing.T) {
		request := []byte{
			0, 0, 0, 1, // 1 group
			0, 10, 't', 'e', 's', 't', '-', 'g', 'r', 'o', 'u', 'p', // "test-group"
		}

		response, err := handler.handleDescribeGroups(123, 0, request)
		if err != nil {
			t.Fatalf("Expected no error, got: %v", err)
		}

		if len(response) == 0 {
			t.Fatal("Expected non-empty response")
		}

		// Should contain error code for GROUP_COORDINATOR_NOT_AVAILABLE
		t.Logf("Response length: %d bytes", len(response))
	})

	// Test with group coordinator but no groups
	t.Run("WithCoordinatorNoGroups", func(t *testing.T) {
		coordinator := consumer.NewGroupCoordinator()
		handler.groupCoordinator = coordinator

		request := []byte{
			0, 0, 0, 1, // 1 group
			0, 10, 't', 'e', 's', 't', '-', 'g', 'r', 'o', 'u', 'p', // "test-group"
		}

		response, err := handler.handleDescribeGroups(123, 0, request)
		if err != nil {
			t.Fatalf("Expected no error, got: %v", err)
		}

		if len(response) == 0 {
			t.Fatal("Expected non-empty response")
		}

		// Should contain error code for UNKNOWN_GROUP_ID
		t.Logf("Response length: %d bytes", len(response))
	})

	// Test parsing edge cases
	t.Run("InvalidRequest", func(t *testing.T) {
		request := []byte{0, 0} // Too short

		_, err := handler.handleDescribeGroups(123, 0, request)
		if err == nil {
			t.Fatal("Expected error for invalid request")
		}
	})
}

func TestHandler_handleListGroups(t *testing.T) {
	handler := &Handler{}

	// Test with no group coordinator
	t.Run("NoCoordinator", func(t *testing.T) {
		request := []byte{} // Empty request

		response, err := handler.handleListGroups(123, 0, request)
		if err != nil {
			t.Fatalf("Expected no error, got: %v", err)
		}

		if len(response) == 0 {
			t.Fatal("Expected non-empty response")
		}

		t.Logf("Response length: %d bytes", len(response))
	})

	// Test with group coordinator
	t.Run("WithCoordinator", func(t *testing.T) {
		coordinator := consumer.NewGroupCoordinator()
		handler.groupCoordinator = coordinator

		request := []byte{} // Empty request

		response, err := handler.handleListGroups(123, 0, request)
		if err != nil {
			t.Fatalf("Expected no error, got: %v", err)
		}

		if len(response) == 0 {
			t.Fatal("Expected non-empty response")
		}

		t.Logf("Response length: %d bytes", len(response))
	})

	// Test with states filter (v4+)
	t.Run("WithStatesFilter", func(t *testing.T) {
		coordinator := consumer.NewGroupCoordinator()
		handler.groupCoordinator = coordinator

		// Request with states filter: ["Stable"]
		request := []byte{
			0, 0, 0, 1, // 1 state filter
			0, 6, 'S', 't', 'a', 'b', 'l', 'e', // "Stable"
		}

		response, err := handler.handleListGroups(123, 4, request)
		if err != nil {
			t.Fatalf("Expected no error, got: %v", err)
		}

		if len(response) == 0 {
			t.Fatal("Expected non-empty response")
		}

		t.Logf("Response length: %d bytes", len(response))
	})
}

func TestHandler_parseDescribeGroupsRequest(t *testing.T) {
	handler := &Handler{}

	tests := []struct {
		name        string
		data        []byte
		apiVersion  uint16
		expectError bool
		expectedLen int
	}{
		{
			name: "single group",
			data: []byte{
				0, 0, 0, 1, // 1 group
				0, 10, 't', 'e', 's', 't', '-', 'g', 'r', 'o', 'u', 'p', // "test-group"
			},
			apiVersion:  0,
			expectError: false,
			expectedLen: 1,
		},
		{
			name: "multiple groups",
			data: []byte{
				0, 0, 0, 2, // 2 groups
				0, 6, 'g', 'r', 'o', 'u', 'p', '1', // "group1"
				0, 6, 'g', 'r', 'o', 'u', 'p', '2', // "group2"
			},
			apiVersion:  0,
			expectError: false,
			expectedLen: 2,
		},
		{
			name:        "empty request",
			data:        []byte{},
			apiVersion:  0,
			expectError: true,
		},
		{
			name: "truncated request",
			data: []byte{
				0, 0, 0, 1, // 1 group
				0, 10, 't', 'e', 's', 't', // Incomplete group name
			},
			apiVersion:  0,
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := handler.parseDescribeGroupsRequest(tt.data, tt.apiVersion)

			if tt.expectError {
				if err == nil {
					t.Error("Expected error but got none")
				}
				return
			}

			if err != nil {
				t.Errorf("Unexpected error: %v", err)
				return
			}

			if len(result.GroupIDs) != tt.expectedLen {
				t.Errorf("Expected %d groups, got %d", tt.expectedLen, len(result.GroupIDs))
			}
		})
	}
}

func TestHandler_parseListGroupsRequest(t *testing.T) {
	handler := &Handler{}

	tests := []struct {
		name        string
		data        []byte
		apiVersion  uint16
		expectError bool
		expectedLen int
	}{
		{
			name:        "empty request v0",
			data:        []byte{},
			apiVersion:  0,
			expectError: false,
			expectedLen: 0,
		},
		{
			name: "with states filter v4",
			data: []byte{
				0, 0, 0, 2, // 2 states
				0, 6, 'S', 't', 'a', 'b', 'l', 'e', // "Stable"
				0, 5, 'E', 'm', 'p', 't', 'y', // "Empty"
			},
			apiVersion:  4,
			expectError: false,
			expectedLen: 2,
		},
		{
			name: "no states filter v4",
			data: []byte{
				0, 0, 0, 0, // 0 states
			},
			apiVersion:  4,
			expectError: false,
			expectedLen: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := handler.parseListGroupsRequest(tt.data, tt.apiVersion)

			if tt.expectError {
				if err == nil {
					t.Error("Expected error but got none")
				}
				return
			}

			if err != nil {
				t.Errorf("Unexpected error: %v", err)
				return
			}

			if len(result.StatesFilter) != tt.expectedLen {
				t.Errorf("Expected %d states, got %d", tt.expectedLen, len(result.StatesFilter))
			}
		})
	}
}

func TestHandler_buildDescribeGroupsResponse(t *testing.T) {
	handler := &Handler{}

	response := DescribeGroupsResponse{
		ThrottleTimeMs: 0,
		Groups: []DescribeGroupsGroup{
			{
				ErrorCode:    0,
				GroupID:      "test-group",
				State:        "Stable",
				ProtocolType: "consumer",
				Protocol:     "range",
				Members: []DescribeGroupsMember{
					{
						MemberID:         "member-1",
						GroupInstanceID:  nil,
						ClientID:         "client-1",
						ClientHost:       "localhost",
						MemberMetadata:   []byte("metadata"),
						MemberAssignment: []byte("assignment"),
					},
				},
				AuthorizedOps: []int32{},
			},
		},
	}

	result := handler.buildDescribeGroupsResponse(response, 123, 0)
	if len(result) == 0 {
		t.Fatal("Expected non-empty response")
	}

	t.Logf("Response length: %d bytes", len(result))

	// Test with different API versions
	resultV3 := handler.buildDescribeGroupsResponse(response, 123, 3)
	if len(resultV3) <= len(result) {
		t.Error("Expected v3 response to be larger (includes authorized ops)")
	}
}

func TestHandler_buildListGroupsResponse(t *testing.T) {
	handler := &Handler{}

	response := ListGroupsResponse{
		ThrottleTimeMs: 0,
		ErrorCode:      0,
		Groups: []ListGroupsGroup{
			{
				GroupID:      "group-1",
				ProtocolType: "consumer",
				GroupState:   "Stable",
			},
			{
				GroupID:      "group-2",
				ProtocolType: "consumer",
				GroupState:   "Empty",
			},
		},
	}

	result := handler.buildListGroupsResponse(response, 123, 0)
	if len(result) == 0 {
		t.Fatal("Expected non-empty response")
	}

	t.Logf("Response length: %d bytes", len(result))

	// Test with different API versions
	resultV4 := handler.buildListGroupsResponse(response, 123, 4)
	if len(resultV4) <= len(result) {
		t.Error("Expected v4 response to be larger (includes group state)")
	}
}
