package util

import "testing"

func TestParseHostPort(t *testing.T) {
	tests := []struct {
		name       string
		hostPort   string
		wantHost   string
		wantPort   int64
		wantErr    bool
	}{
		{
			name:     "hostname",
			hostPort: "localhost:8888",
			wantHost: "localhost",
			wantPort: 8888,
		},
		{
			name:     "ipv4",
			hostPort: "127.0.0.1:8888",
			wantHost: "127.0.0.1",
			wantPort: 8888,
		},
		{
			name:     "bracketed ipv6",
			hostPort: "[::1]:8888",
			wantHost: "::1",
			wantPort: 8888,
		},
		{
			name:     "missing port",
			hostPort: "localhost",
			wantErr:  true,
		},
		{
			name:     "invalid port",
			hostPort: "localhost:bad",
			wantErr:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotHost, gotPort, err := ParseHostPort(tt.hostPort)
			if gotErr := err != nil; gotErr != tt.wantErr {
				t.Fatalf("ParseHostPort(%q) error = %v, wantErr %v", tt.hostPort, err, tt.wantErr)
			}
			if tt.wantErr {
				return
			}
			if gotHost != tt.wantHost || gotPort != tt.wantPort {
				t.Fatalf("ParseHostPort(%q) = (%q, %d), want (%q, %d)", tt.hostPort, gotHost, gotPort, tt.wantHost, tt.wantPort)
			}
		})
	}
}
