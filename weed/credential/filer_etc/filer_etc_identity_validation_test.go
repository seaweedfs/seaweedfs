package filer_etc

import (
	"strings"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb/iam_pb"
)

func TestValidateIdentity(t *testing.T) {
	tests := []struct {
		name        string
		identity    *iam_pb.Identity
		expectError bool
		errorMsg    string
	}{
		{
			name:        "nil identity",
			identity:    nil,
			expectError: true,
			errorMsg:    "identity cannot be nil",
		},
		{
			name: "empty username",
			identity: &iam_pb.Identity{
				Name: "",
				Credentials: []*iam_pb.Credential{
					{AccessKey: "key", SecretKey: "secret"},
				},
			},
			expectError: true,
			errorMsg:    "username cannot be empty",
		},
		{
			name: "path traversal with slash",
			identity: &iam_pb.Identity{
				Name: "../admin",
				Credentials: []*iam_pb.Credential{
					{AccessKey: "key", SecretKey: "secret"},
				},
			},
			expectError: true,
			errorMsg:    "username cannot contain path separators",
		},
		{
			name: "path traversal with backslash",
			identity: &iam_pb.Identity{
				Name: "..\\admin",
				Credentials: []*iam_pb.Credential{
					{AccessKey: "key", SecretKey: "secret"},
				},
			},
			expectError: true,
			errorMsg:    "username cannot contain path separators",
		},
		{
			name: "path traversal pattern",
			identity: &iam_pb.Identity{
				Name: "user..admin",
				Credentials: []*iam_pb.Credential{
					{AccessKey: "key", SecretKey: "secret"},
				},
			},
			expectError: true,
			errorMsg:    "username cannot contain path traversal patterns",
		},
		{
			name: "no credentials",
			identity: &iam_pb.Identity{
				Name:        "validuser",
				Credentials: []*iam_pb.Credential{},
			},
			expectError: true,
			errorMsg:    "identity must have at least one credential",
		},
		{
			name: "empty access key",
			identity: &iam_pb.Identity{
				Name: "validuser",
				Credentials: []*iam_pb.Credential{
					{AccessKey: "", SecretKey: "secret"},
				},
			},
			expectError: true,
			errorMsg:    "credential at index 0 has empty access key",
		},
		{
			name: "empty secret key",
			identity: &iam_pb.Identity{
				Name: "validuser",
				Credentials: []*iam_pb.Credential{
					{AccessKey: "key", SecretKey: ""},
				},
			},
			expectError: true,
			errorMsg:    "credential at index 0 has empty secret key",
		},
		{
			name: "duplicate access keys",
			identity: &iam_pb.Identity{
				Name: "validuser",
				Credentials: []*iam_pb.Credential{
					{AccessKey: "key1", SecretKey: "secret1"},
					{AccessKey: "key1", SecretKey: "secret2"},
				},
			},
			expectError: true,
			errorMsg:    "duplicate access key within same user",
		},
		{
			name: "valid identity",
			identity: &iam_pb.Identity{
				Name: "validuser",
				Credentials: []*iam_pb.Credential{
					{AccessKey: "key1", SecretKey: "secret1"},
					{AccessKey: "key2", SecretKey: "secret2"},
				},
			},
			expectError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateIdentity(tt.identity)
			if tt.expectError {
				if err == nil {
					t.Errorf("expected error containing '%s', got nil", tt.errorMsg)
				} else if tt.errorMsg != "" && !strings.Contains(err.Error(), tt.errorMsg) {
					t.Errorf("expected error containing '%s', got '%s'", tt.errorMsg, err.Error())
				}
			} else {
				if err != nil {
					t.Errorf("expected no error, got: %v", err)
				}
			}
		})
	}
}
