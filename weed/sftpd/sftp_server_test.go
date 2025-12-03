package sftpd

import (
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/sftpd/user"
	"github.com/stretchr/testify/assert"
)

func TestToAbsolutePath(t *testing.T) {
	fs := &SftpServer{
		user: &user.User{
			HomeDir: "/sftp/testuser",
		},
	}

	tests := []struct {
		name        string
		userPath    string
		expected    string
		expectError bool
	}{
		{
			name:     "normal path",
			userPath: "/foo.txt",
			expected: "/sftp/testuser/foo.txt",
		},
		{
			name:     "root path",
			userPath: "/",
			expected: "/sftp/testuser",
		},
		{
			name:     "path with dot",
			userPath: "/./foo.txt",
			expected: "/sftp/testuser/foo.txt",
		},
		{
			name:        "path traversal attempts",
			userPath:    "/../foo.txt",
			expectError: true,
		},
		{
			name:        "path traversal attempts 2",
			userPath:    "../../foo.txt",
			expectError: true,
		},
		{
			name:        "path traversal attempts 3",
			userPath:    "/subdir/../../foo.txt",
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := fs.toAbsolutePath(tt.userPath)
			if tt.expectError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.expected, got)
			}
		})
	}
}

