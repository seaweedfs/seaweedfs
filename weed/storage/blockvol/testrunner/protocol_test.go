package testrunner

import (
	"testing"
)

func TestIsPathSafe(t *testing.T) {
	tests := []struct {
		path string
		want bool
	}{
		{"/tmp/sw-test-runner/binary", true},
		{"/tmp/sw-test-runner/subdir/file", true},
		{"/tmp/sw-test-runner/nested/deep/file", true},
		{"/tmp/other/file", false},
		{"/etc/passwd", false},
		{"/tmp/sw-test-runner/../etc/passwd", false},
		{"../etc/passwd", false},
		{"/tmp/sw-test-runner/../../root", false},
		{"", false},
		{"/tmp/sw-test-runner-evil/file", false},
	}

	for _, tt := range tests {
		got := isPathSafe(tt.path)
		if got != tt.want {
			t.Errorf("isPathSafe(%q) = %v, want %v", tt.path, got, tt.want)
		}
	}
}

func TestAuthTokenHeaderConstant(t *testing.T) {
	if AuthTokenHeader != "X-Auth-Token" {
		t.Errorf("AuthTokenHeader = %q", AuthTokenHeader)
	}
}
