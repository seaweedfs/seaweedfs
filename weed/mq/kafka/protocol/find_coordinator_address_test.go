package protocol

import (
	"testing"
)

func TestHandler_parseAddress(t *testing.T) {
	handler := &Handler{}
	
	tests := []struct {
		name        string
		address     string
		nodeID      int32
		expectHost  string
		expectPort  int
		expectError bool
	}{
		{
			name:        "Valid address with port",
			address:     "localhost:9092",
			nodeID:      1,
			expectHost:  "localhost",
			expectPort:  9092,
			expectError: false,
		},
		{
			name:        "Valid address with different port",
			address:     "gateway.example.com:8080",
			nodeID:      2,
			expectHost:  "gateway.example.com",
			expectPort:  8080,
			expectError: false,
		},
		{
			name:        "IPv4 address with port",
			address:     "192.168.1.100:9093",
			nodeID:      3,
			expectHost:  "192.168.1.100",
			expectPort:  9093,
			expectError: false,
		},
		{
			name:        "IPv6 address with port",
			address:     "[::1]:9094",
			nodeID:      4,
			expectHost:  "::1",
			expectPort:  9094,
			expectError: false,
		},
		{
			name:        "Invalid address without port",
			address:     "localhost",
			nodeID:      5,
			expectHost:  "",
			expectPort:  0,
			expectError: true,
		},
		{
			name:        "Invalid address with non-numeric port",
			address:     "localhost:abc",
			nodeID:      6,
			expectHost:  "",
			expectPort:  0,
			expectError: true,
		},
		{
			name:        "Empty address",
			address:     "",
			nodeID:      7,
			expectHost:  "",
			expectPort:  0,
			expectError: true,
		},
	}
	
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			host, port, nid, err := handler.parseAddress(tt.address, tt.nodeID)
			
			if tt.expectError {
				if err == nil {
					t.Errorf("Expected error for address %s, but got none", tt.address)
				}
				return
			}
			
			if err != nil {
				t.Errorf("Unexpected error for address %s: %v", tt.address, err)
				return
			}
			
			if host != tt.expectHost {
				t.Errorf("Expected host %s, got %s", tt.expectHost, host)
			}
			
			if port != tt.expectPort {
				t.Errorf("Expected port %d, got %d", tt.expectPort, port)
			}
			
			if nid != tt.nodeID {
				t.Errorf("Expected node ID %d, got %d", tt.nodeID, nid)
			}
		})
	}
}

func TestHandler_parseGatewayAddress(t *testing.T) {
	handler := &Handler{}
	
	tests := []struct {
		name        string
		address     string
		expectHost  string
		expectPort  int
		expectError bool
	}{
		{
			name:        "Valid address with port",
			address:     "localhost:9092",
			expectHost:  "localhost",
			expectPort:  9092,
			expectError: false,
		},
		{
			name:        "Valid address with different port",
			address:     "gateway.example.com:8080",
			expectHost:  "gateway.example.com",
			expectPort:  8080,
			expectError: false,
		},
		{
			name:        "IPv4 address with port",
			address:     "192.168.1.100:9093",
			expectHost:  "192.168.1.100",
			expectPort:  9093,
			expectError: false,
		},
		{
			name:        "IPv6 address with port",
			address:     "[2001:db8::1]:9094",
			expectHost:  "2001:db8::1",
			expectPort:  9094,
			expectError: false,
		},
		{
			name:        "Invalid address without port",
			address:     "localhost",
			expectHost:  "",
			expectPort:  0,
			expectError: true,
		},
		{
			name:        "Invalid address with non-numeric port",
			address:     "localhost:abc",
			expectHost:  "",
			expectPort:  0,
			expectError: true,
		},
		{
			name:        "Empty address",
			address:     "",
			expectHost:  "",
			expectPort:  0,
			expectError: true,
		},
	}
	
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			host, port, err := handler.parseGatewayAddress(tt.address)
			
			if tt.expectError {
				if err == nil {
					t.Errorf("Expected error for address %s, but got none", tt.address)
				}
				return
			}
			
			if err != nil {
				t.Errorf("Unexpected error for address %s: %v", tt.address, err)
				return
			}
			
			if host != tt.expectHost {
				t.Errorf("Expected host %s, got %s", tt.expectHost, host)
			}
			
			if port != tt.expectPort {
				t.Errorf("Expected port %d, got %d", tt.expectPort, port)
			}
		})
	}
}
