package security

import (
	"net/http"
	"testing"
)

func TestGetActualRemoteHost(t *testing.T) {
	tests := []struct {
		name        string
		remoteAddr  string
		headers     map[string]string
		expected    string
		expectError bool
	}{
		{
			name:       "Valid host:port format",
			remoteAddr: "192.168.1.1:12345",
			expected:   "192.168.1.1",
		},
		{
			name:       "IPv6 with port",
			remoteAddr: "[::1]:12345",
			expected:   "::1",
		},
		{
			name:       "Localhost with port",
			remoteAddr: "127.0.0.1:8080",
			expected:   "127.0.0.1",
		},
		{
			name:       "IP without port (should use RemoteAddr directly)",
			remoteAddr: "192.168.1.1",
			expected:   "192.168.1.1",
		},
		{
			name:        "Invalid format like @ symbol",
			remoteAddr:  "@",
			expected:    "",
			expectError: true,
		},
		{
			name:        "Empty RemoteAddr",
			remoteAddr:  "",
			expected:    "",
			expectError: true,
		},
		{
			name:       "X-Forwarded-For header takes precedence",
			remoteAddr: "192.168.1.1:12345",
			headers: map[string]string{
				"X-Forwarded-For": "10.0.0.1",
			},
			expected: "10.0.0.1",
		},
		{
			name:       "X-Forwarded-For with multiple IPs",
			remoteAddr: "192.168.1.1:12345",
			headers: map[string]string{
				"X-Forwarded-For": "10.0.0.1,10.0.0.2,10.0.0.3",
			},
			expected: "10.0.0.1",
		},
		{
			name:       "HTTP_X_FORWARDED_FOR header takes precedence",
			remoteAddr: "192.168.1.1:12345",
			headers: map[string]string{
				"HTTP_X_FORWARDED_FOR": "10.0.0.1",
			},
			expected: "10.0.0.1",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := &http.Request{
				RemoteAddr: tt.remoteAddr,
				Header:     make(http.Header),
			}

			// Set headers if provided
			for key, value := range tt.headers {
				req.Header.Set(key, value)
			}

			host, err := GetActualRemoteHost(req)

			if tt.expectError {
				if err == nil {
					t.Errorf("Expected error but got none. Host: %s", host)
				}
			} else {
				if err != nil {
					t.Errorf("Unexpected error: %v", err)
				}
				if host != tt.expected {
					t.Errorf("Expected host %s, got %s", tt.expected, host)
				}
			}
		})
	}
}

func TestGuardWhiteListCheck(t *testing.T) {
	whiteList := []string{"127.0.0.1", "192.168.1.0/24", "10.0.0.1"}
	guard := NewGuard(whiteList, "", 0, "", 0)

	tests := []struct {
		name        string
		remoteAddr  string
		shouldAllow bool
	}{
		{
			name:        "Whitelisted exact IP",
			remoteAddr:  "127.0.0.1:12345",
			shouldAllow: true,
		},
		{
			name:        "Whitelisted IP in CIDR range",
			remoteAddr:  "192.168.1.100:8080",
			shouldAllow: true,
		},
		{
			name:        "Another whitelisted exact IP",
			remoteAddr:  "10.0.0.1:9000",
			shouldAllow: true,
		},
		{
			name:        "Non-whitelisted IP",
			remoteAddr:  "1.2.3.4:5678",
			shouldAllow: false,
		},
		{
			name:        "IP outside CIDR range",
			remoteAddr:  "192.168.2.1:8080",
			shouldAllow: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := &http.Request{
				RemoteAddr: tt.remoteAddr,
				Header:     make(http.Header),
			}

			err := guard.checkWhiteList(nil, req)

			if tt.shouldAllow {
				if err != nil {
					t.Errorf("Expected to allow %s but got error: %v", tt.remoteAddr, err)
				}
			} else {
				if err == nil {
					t.Errorf("Expected to block %s but was allowed", tt.remoteAddr)
				}
			}
		})
	}
}
