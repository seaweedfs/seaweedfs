package sftpd

import (
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/sftpd/user"
	"github.com/stretchr/testify/assert"
)

func stringPtr(s string) *string {
	return &s
}

func TestToAbsolutePath(t *testing.T) {
	tests := []struct {
		name        string
		homeDir     *string // Use pointer to distinguish between unset and empty
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
		{
			name:     "empty path",
			userPath: "",
			expected: "/sftp/testuser",
		},
		{
			name:     "multiple slashes",
			userPath: "//foo.txt",
			expected: "/sftp/testuser/foo.txt",
		},
		{
			name:     "trailing slash",
			userPath: "/foo/",
			expected: "/sftp/testuser/foo",
		},
		{
			name:     "empty HomeDir passthrough",
			homeDir:  stringPtr(""),
			userPath: "/foo.txt",
			expected: "/foo.txt",
		},
		{
			name:     "root HomeDir passthrough",
			homeDir:  stringPtr("/"),
			userPath: "/foo.txt",
			expected: "/foo.txt",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			homeDir := "/sftp/testuser" // default
			if tt.homeDir != nil {
				homeDir = *tt.homeDir
			}

			fs := &SftpServer{
				user: &user.User{
					HomeDir: homeDir,
				},
			}

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
