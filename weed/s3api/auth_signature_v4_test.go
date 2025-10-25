package s3api

import (
	"testing"
)

func TestBuildPathWithForwardedPrefix(t *testing.T) {
	tests := []struct {
		name            string
		forwardedPrefix string
		urlPath         string
		expected        string
	}{
		{
			name:            "empty prefix returns urlPath",
			forwardedPrefix: "",
			urlPath:         "/bucket/obj",
			expected:        "/bucket/obj",
		},
		{
			name:            "prefix without trailing slash",
			forwardedPrefix: "/storage",
			urlPath:         "/bucket/obj",
			expected:        "/storage/bucket/obj",
		},
		{
			name:            "prefix with trailing slash",
			forwardedPrefix: "/storage/",
			urlPath:         "/bucket/obj",
			expected:        "/storage/bucket/obj",
		},
		{
			name:            "prefix without leading slash",
			forwardedPrefix: "storage",
			urlPath:         "/bucket/obj",
			expected:        "/storage/bucket/obj",
		},
		{
			name:            "prefix without leading slash and with trailing slash",
			forwardedPrefix: "storage/",
			urlPath:         "/bucket/obj",
			expected:        "/storage/bucket/obj",
		},
		{
			name:            "preserve double slashes in key",
			forwardedPrefix: "/storage",
			urlPath:         "/bucket//obj",
			expected:        "/storage/bucket//obj",
		},
		{
			name:            "preserve trailing slash in urlPath",
			forwardedPrefix: "/storage",
			urlPath:         "/bucket/folder/",
			expected:        "/storage/bucket/folder/",
		},
		{
			name:            "preserve trailing slash with prefix having trailing slash",
			forwardedPrefix: "/storage/",
			urlPath:         "/bucket/folder/",
			expected:        "/storage/bucket/folder/",
		},
		{
			name:            "root path",
			forwardedPrefix: "/storage",
			urlPath:         "/",
			expected:        "/storage/",
		},
		{
			name:            "complex key with multiple slashes",
			forwardedPrefix: "/api/v1",
			urlPath:         "/bucket/path//with///slashes",
			expected:        "/api/v1/bucket/path//with///slashes",
		},
		{
			name:            "urlPath without leading slash",
			forwardedPrefix: "/storage",
			urlPath:         "bucket/obj",
			expected:        "/storage/bucket/obj",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := buildPathWithForwardedPrefix(tt.forwardedPrefix, tt.urlPath)
			if result != tt.expected {
				t.Errorf("buildPathWithForwardedPrefix(%q, %q) = %q, want %q",
					tt.forwardedPrefix, tt.urlPath, result, tt.expected)
			}
		})
	}
}
