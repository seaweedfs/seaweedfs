package main

import (
	"strings"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/server/postgres"
)

func TestParseAuthMethod(t *testing.T) {
	tests := []struct {
		input    string
		want     postgres.AuthMethod
		wantErr  bool
		errMatch string
	}{
		{"trust", postgres.AuthTrust, false, ""},
		{"password", postgres.AuthPassword, false, ""},
		{"md5", postgres.AuthMD5, false, ""},
		{"TRUST", postgres.AuthTrust, false, ""},   // case insensitive
		{"Password", postgres.AuthPassword, false, ""}, // case insensitive
		{"", postgres.AuthTrust, true, "unsupported auth method"},
		{"invalid", postgres.AuthTrust, true, "unsupported auth method"},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			got, err := parseAuthMethod(tt.input)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("expected error containing %q, got nil", tt.errMatch)
				}
				if !strings.Contains(err.Error(), tt.errMatch) {
					t.Fatalf("expected error containing %q, got %q", tt.errMatch, err.Error())
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != tt.want {
				t.Fatalf("expected auth method %d, got %d", tt.want, got)
			}
		})
	}
}

func TestValidatePortNumber(t *testing.T) {
	tests := []struct {
		port    int
		wantErr bool
	}{
		{-1, true},
		{0, true},
		{1, false},
		{1023, false},  // privileged, allowed but warns
		{1024, false},
		{5432, false},
		{65535, false},
		{65536, true},
	}

	for _, tt := range tests {
		t.Run("", func(t *testing.T) {
			err := validatePortNumber(tt.port)
			if tt.wantErr && err == nil {
				t.Fatalf("validatePortNumber(%d) expected error, got nil", tt.port)
			}
			if !tt.wantErr && err != nil {
				t.Fatalf("validatePortNumber(%d) unexpected error: %v", tt.port, err)
			}
		})
	}
}

func TestParseUsers(t *testing.T) {
	tests := []struct {
		name       string
		usersStr   string
		authMethod postgres.AuthMethod
		wantUsers  int
		wantErr    bool
		errMatch   string
	}{
		{
			name:       "empty_with_trust",
			usersStr:   "",
			authMethod: postgres.AuthTrust,
			wantUsers:  0,
			wantErr:    false,
		},
		{
			name:       "whitespace_only_with_trust",
			usersStr:   "   \t\n",
			authMethod: postgres.AuthTrust,
			wantUsers:  0,
			wantErr:    false,
		},
		{
			name:       "empty_with_md5",
			usersStr:   "",
			authMethod: postgres.AuthMD5,
			wantErr:    true,
			errMatch:   "users must be specified",
		},
		{
			name:       "whitespace_only_with_md5",
			usersStr:   "   ",
			authMethod: postgres.AuthMD5,
			wantErr:    true,
			errMatch:   "users must be specified",
		},
		{
			name:       "valid_json",
			usersStr:   `{"admin":"secret"}`,
			authMethod: postgres.AuthMD5,
			wantUsers:  1,
			wantErr:    false,
		},
		{
			name:       "invalid_json",
			usersStr:   `{"admin":}`,
			authMethod: postgres.AuthTrust,
			wantErr:    true,
			errMatch:   "invalid JSON format",
		},
		{
			name:       "empty_username",
			usersStr:   `{"":"secret"}`,
			authMethod: postgres.AuthMD5,
			wantErr:    true,
			errMatch:   "empty username",
		},
		{
			name:       "empty_password_with_md5",
			usersStr:   `{"admin":""}`,
			authMethod: postgres.AuthMD5,
			wantErr:    true,
			errMatch:   "empty password",
		},
		{
			name:       "empty_password_with_trust",
			usersStr:   `{"admin":""}`,
			authMethod: postgres.AuthTrust,
			wantUsers:  1,
			wantErr:    false,
		},
		{
			name:       "invalid_format",
			usersStr:   "not-json",
			authMethod: postgres.AuthTrust,
			wantErr:    true,
			errMatch:   "invalid user credentials format",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parseUsers(tt.usersStr, tt.authMethod)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("expected error containing %q, got nil", tt.errMatch)
				}
				if !strings.Contains(err.Error(), tt.errMatch) {
					t.Fatalf("expected error containing %q, got %q", tt.errMatch, err.Error())
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if len(got) != tt.wantUsers {
				t.Fatalf("expected %d users, got %d", tt.wantUsers, len(got))
			}
		})
	}
}
