package weed_server

import "testing"

func TestBucketUnderBucketsPath(t *testing.T) {
	tests := []struct {
		name        string
		fullPath    string
		bucketsPath string
		want        string
	}{
		{"object in bucket", "/buckets/mybucket/a/b.txt", "/buckets", "mybucket"},
		{"bucket root", "/buckets/mybucket", "/buckets", "mybucket"},
		{"not under buckets path", "/other/path/x", "/buckets", ""},
		{"buckets path itself", "/buckets", "/buckets", ""},
		{"custom buckets path", "/data/buckets/mybucket/x", "/data/buckets", "mybucket"},
		{"nested object", "/buckets/b/deep/nested/key", "/buckets", "b"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := bucketUnderBucketsPath(tt.fullPath, tt.bucketsPath); got != tt.want {
				t.Errorf("bucketUnderBucketsPath(%q, %q) = %q, want %q", tt.fullPath, tt.bucketsPath, got, tt.want)
			}
		})
	}
}
