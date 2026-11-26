package s3api

import (
	"context"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/seaweedfs/seaweedfs/weed/stats"
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

// TestBucketSizeMetricsTickWithoutLockDropsGauges checks that an instance
// without the metrics lock drops the bucket gauges it set while it held the
// lock, and that a tick with the lock leaves them to the collection.
func TestBucketSizeMetricsTickWithoutLockDropsGauges(t *testing.T) {
	gauges := map[string]*prometheus.GaugeVec{
		"bucket_size_bytes":          stats.S3BucketSizeBytesGauge,
		"bucket_physical_size_bytes": stats.S3BucketPhysicalSizeBytesGauge,
		"bucket_object_count":        stats.S3BucketObjectCountGauge,
		"bucket_quota_bytes":         stats.S3BucketQuotaBytesGauge,
		"bucket_read_only":           stats.S3BucketReadOnlyGauge,
	}
	t.Cleanup(stats.ResetBucketSizeMetrics)
	for _, bucket := range []string{"tick-a", "tick-b"} {
		stats.UpdateBucketSizeMetrics(bucket, 100, 200, 3)
		stats.UpdateBucketQuotaMetrics(bucket, 1000, true)
	}
	// No masters configured: the collection gives up before it touches a gauge.
	s3a := &S3ApiServer{option: &S3ApiServerOption{}}

	s3a.bucketSizeMetricsTick(context.Background(), true)
	for name, g := range gauges {
		if n := testutil.CollectAndCount(g); n != 2 {
			t.Fatalf("after a tick with the lock, %s has %d series, want 2", name, n)
		}
	}

	s3a.bucketSizeMetricsTick(context.Background(), false)
	for name, g := range gauges {
		if n := testutil.CollectAndCount(g); n != 0 {
			t.Errorf("after a tick without the lock, %s has %d series, want 0", name, n)
		}
	}
}
