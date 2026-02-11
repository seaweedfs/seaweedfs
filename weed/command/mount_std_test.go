//go:build linux || darwin || freebsd
// +build linux darwin freebsd

package command

import "testing"

func Test_bucketPathForMountRoot(t *testing.T) {
	tests := []struct {
		name      string
		mountRoot string
		expected  string
		ok        bool
	}{
		{
			name:      "bucket root mount",
			mountRoot: "/buckets/test",
			expected:  "/buckets/test",
			ok:        true,
		},
		{
			name:      "bucket root with trailing slash",
			mountRoot: "/buckets/test/",
			expected:  "/buckets/test",
			ok:        true,
		},
		{
			name:      "subdirectory mount",
			mountRoot: "/buckets/test/data",
			expected:  "",
			ok:        false,
		},
		{
			name:      "non-bucket mount",
			mountRoot: "/data/test",
			expected:  "",
			ok:        false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotPath, gotOK := bucketPathForMountRoot(tt.mountRoot, "/buckets")
			if gotOK != tt.ok {
				t.Fatalf("expected ok=%v, got %v", tt.ok, gotOK)
			}
			if gotPath != tt.expected {
				t.Fatalf("expected path %q, got %q", tt.expected, gotPath)
			}
		})
	}
}
