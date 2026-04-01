package util

import "testing"

func TestExtractBucketPath(t *testing.T) {
	for _, tt := range []struct {
		name         string
		base         string
		target       string
		requireChild bool
		expected     string
		ok           bool
	}{
		{
			name:         "child paths return bucket",
			base:         "/buckets",
			target:       "/buckets/test/folder/file",
			requireChild: true,
			expected:     "/buckets/test",
			ok:           true,
		},
		{
			name:         "bucket root without child fails when required",
			base:         "/buckets",
			target:       "/buckets/test",
			requireChild: true,
			ok:           false,
		},
		{
			name:         "bucket root allowed when not required",
			base:         "/buckets",
			target:       "/buckets/test",
			requireChild: false,
			expected:     "/buckets/test",
			ok:           true,
		},
		{
			name:         "path outside buckets fails",
			base:         "/buckets",
			target:       "/data/test/folder",
			requireChild: true,
			ok:           false,
		},
		{
			name:         "trailing slash on base is normalized",
			base:         "/buckets/",
			target:       "/buckets/test/sub",
			requireChild: true,
			expected:     "/buckets/test",
			ok:           true,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			got, ok := ExtractBucketPath(tt.base, tt.target, tt.requireChild)
			if ok != tt.ok {
				t.Fatalf("expected ok=%v, got %v", tt.ok, ok)
			}
			if got != tt.expected {
				t.Fatalf("expected path %q, got %q", tt.expected, got)
			}
		})
	}
}
