package s3_constants

import (
	"testing"
)

func TestNormalizeObjectKey(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "simple key",
			input:    "file.txt",
			expected: "/file.txt",
		},
		{
			name:     "key with leading slash",
			input:    "/file.txt",
			expected: "/file.txt",
		},
		{
			name:     "key with directory",
			input:    "folder/file.txt",
			expected: "/folder/file.txt",
		},
		{
			name:     "key with leading slash and directory",
			input:    "/folder/file.txt",
			expected: "/folder/file.txt",
		},
		{
			name:     "key with duplicate slashes",
			input:    "folder//subfolder///file.txt",
			expected: "/folder/subfolder/file.txt",
		},
		{
			name:     "Windows backslash - simple",
			input:    "folder\\file.txt",
			expected: "/folder/file.txt",
		},
		{
			name:     "Windows backslash - nested",
			input:    "folder\\subfolder\\file.txt",
			expected: "/folder/subfolder/file.txt",
		},
		{
			name:     "Windows backslash - with leading slash",
			input:    "/folder\\subfolder\\file.txt",
			expected: "/folder/subfolder/file.txt",
		},
		{
			name:     "mixed slashes",
			input:    "folder\\subfolder/another\\file.txt",
			expected: "/folder/subfolder/another/file.txt",
		},
		{
			name:     "Windows full path style (edge case)",
			input:    "C:\\Users\\test\\file.txt",
			expected: "/C:/Users/test/file.txt",
		},
		{
			name:     "empty string",
			input:    "",
			expected: "/",
		},
		{
			name:     "just a slash",
			input:    "/",
			expected: "/",
		},
		{
			name:     "just a backslash",
			input:    "\\",
			expected: "/",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := NormalizeObjectKey(tt.input)
			if result != tt.expected {
				t.Errorf("NormalizeObjectKey(%q) = %q, want %q", tt.input, result, tt.expected)
			}
		})
	}
}

func TestRemoveDuplicateSlashes(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "no duplicates",
			input:    "/folder/file.txt",
			expected: "/folder/file.txt",
		},
		{
			name:     "double slash",
			input:    "/folder//file.txt",
			expected: "/folder/file.txt",
		},
		{
			name:     "triple slash",
			input:    "/folder///file.txt",
			expected: "/folder/file.txt",
		},
		{
			name:     "multiple duplicate locations",
			input:    "//folder//subfolder///file.txt",
			expected: "/folder/subfolder/file.txt",
		},
		{
			name:     "empty string",
			input:    "",
			expected: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := removeDuplicateSlashes(tt.input)
			if result != tt.expected {
				t.Errorf("removeDuplicateSlashes(%q) = %q, want %q", tt.input, result, tt.expected)
			}
		})
	}
}

