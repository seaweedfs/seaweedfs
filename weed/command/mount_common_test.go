//go:build linux || darwin || freebsd || windows

package command

import "testing"

func Test_volumeName(t *testing.T) {
	tests := []struct {
		name               string
		filer              string
		filerMountRootPath string
		expected           string
	}{
		{
			name:               "whole tree falls back to the filer",
			filer:              "127.0.0.1:8888",
			filerMountRootPath: "/",
			expected:           "127.0.0.1:8888",
		},
		{
			name:               "empty path falls back to the filer",
			filer:              "127.0.0.1:8888",
			filerMountRootPath: "",
			expected:           "127.0.0.1:8888",
		},
		{
			name:               "several filers stay parseable as one option",
			filer:              "127.0.0.1:8888,127.0.0.1:8889",
			filerMountRootPath: "/",
			expected:           "127.0.0.1:8888+127.0.0.1:8889",
		},
		{
			name:               "mounted directory names the disk",
			filer:              "127.0.0.1:8888",
			filerMountRootPath: "/buckets/images",
			expected:           "images",
		},
		{
			name:               "trailing slash is not a name",
			filer:              "127.0.0.1:8888",
			filerMountRootPath: "/buckets/videos/",
			expected:           "videos",
		},
		{
			name:               "spaces are kept, commas are not",
			filer:              "127.0.0.1:8888",
			filerMountRootPath: "/Image, Disk",
			expected:           "Image+ Disk",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := volumeName(tt.filer, tt.filerMountRootPath); got != tt.expected {
				t.Errorf("volumeName(%q, %q) = %q, want %q", tt.filer, tt.filerMountRootPath, got, tt.expected)
			}
		})
	}
}
