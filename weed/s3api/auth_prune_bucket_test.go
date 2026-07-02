package s3api

import "testing"

func TestActionScopedToBucket(t *testing.T) {
	cases := []struct {
		action string
		bucket string
		want   bool
	}{
		{"Read:bucket", "bucket", true},
		{"Write:bucket/prefix", "bucket", true},
		{"Write:bucket/prefix/*", "bucket", true},
		{"Write:bucket/*", "bucket", true},
		{"Read:other/prefix/*", "bucket", false},
		{"Write:*", "bucket", false},
		{"Write:buck*/x", "bucket", false},
		{"Write:bucketother", "bucket", false},
		{"Admin", "bucket", false},
	}
	for _, c := range cases {
		if got := actionScopedToBucket(c.action, c.bucket); got != c.want {
			t.Errorf("actionScopedToBucket(%q, %q) = %v, want %v", c.action, c.bucket, got, c.want)
		}
	}
}
