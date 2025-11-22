package s3api

import (
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
)

// TestBuildResourceARN verifies that resource ARNs use the AWS-compatible format
func TestBuildResourceARN(t *testing.T) {
	tests := []struct {
		name     string
		bucket   string
		object   string
		expected string
	}{
		{
			name:     "bucket only",
			bucket:   "my-bucket",
			object:   "",
			expected: "arn:aws:s3:::my-bucket",
		},
		{
			name:     "bucket with slash",
			bucket:   "my-bucket",
			object:   "/",
			expected: "arn:aws:s3:::my-bucket",
		},
		{
			name:     "bucket and object",
			bucket:   "my-bucket",
			object:   "path/to/object.txt",
			expected: "arn:aws:s3:::my-bucket/path/to/object.txt",
		},
		{
			name:     "bucket and object with leading slash",
			bucket:   "my-bucket",
			object:   "/path/to/object.txt",
			expected: "arn:aws:s3:::my-bucket/path/to/object.txt",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := buildResourceARN(tt.bucket, tt.object)
			if result != tt.expected {
				t.Errorf("buildResourceARN(%q, %q) = %q, want %q", tt.bucket, tt.object, result, tt.expected)
			}
		})
	}
}

// TestBuildPrincipalARN verifies that principal ARNs use the AWS-compatible format
func TestBuildPrincipalARN(t *testing.T) {
	tests := []struct {
		name     string
		identity *Identity
		expected string
	}{
		{
			name:     "nil identity (anonymous)",
			identity: nil,
			expected: "*",
		},
		{
			name: "anonymous user by name",
			identity: &Identity{
				Name: s3_constants.AccountAnonymousId,
				Account: &Account{
					Id: "123456789012",
				},
			},
			expected: "*",
		},
		{
			name: "anonymous user by account ID",
			identity: &Identity{
				Name: "test-user",
				Account: &Account{
					Id: s3_constants.AccountAnonymousId,
				},
			},
			expected: "*",
		},
		{
			name: "identity with account and name",
			identity: &Identity{
				Name: "test-user",
				Account: &Account{
					Id: "123456789012",
				},
			},
			expected: "arn:aws:iam::123456789012:user/test-user",
		},
		{
			name: "identity without account ID",
			identity: &Identity{
				Name: "test-user",
				Account: &Account{
					Id: "",
				},
			},
			expected: "arn:aws:iam::000000000000:user/test-user",
		},
		{
			name: "identity without name",
			identity: &Identity{
				Name: "",
				Account: &Account{
					Id: "123456789012",
				},
			},
			expected: "arn:aws:iam::123456789012:user/unknown",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := buildPrincipalARN(tt.identity)
			if result != tt.expected {
				t.Errorf("buildPrincipalARN() = %q, want %q", result, tt.expected)
			}
		})
	}
}
