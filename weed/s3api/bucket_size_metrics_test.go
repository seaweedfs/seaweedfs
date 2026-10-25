package s3api

import (
	"testing"
)

// TestCollectionInfoLogicalSize verifies logical size excludes un-vacuumed
// garbage and never goes negative. Quota enforcement runs on this value so a
// bucket full of tombstones is not flipped read-only while its live data is
// under quota.
func TestCollectionInfoLogicalSize(t *testing.T) {
	cases := []struct {
		name     string
		size     float64
		deleted  float64
		expected float64
	}{
		{"no garbage", 1000, 0, 1000},
		{"some garbage", 1000, 300, 700},
		{"all garbage", 1000, 1000, 0},
		{"deleted exceeds size clamps to zero", 300, 1000, 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			info := &CollectionInfo{Size: tc.size, DeletedByteCount: tc.deleted}
			if got := info.LogicalSize(); got != tc.expected {
				t.Errorf("LogicalSize(): got %.0f, want %.0f", got, tc.expected)
			}
		})
	}
}
