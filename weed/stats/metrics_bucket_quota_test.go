package stats

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"
)

func TestUpdateBucketQuotaMetrics(t *testing.T) {
	bucket := "quota-test-bucket"

	UpdateBucketQuotaMetrics(bucket, 1024, true)
	if got := testutil.ToFloat64(S3BucketQuotaBytesGauge.WithLabelValues(bucket)); got != 1024 {
		t.Errorf("quota gauge: got %.0f, want 1024", got)
	}
	if got := testutil.ToFloat64(S3BucketReadOnlyGauge.WithLabelValues(bucket)); got != 1 {
		t.Errorf("read-only gauge: got %.0f, want 1", got)
	}

	UpdateBucketQuotaMetrics(bucket, 1024, false)
	if got := testutil.ToFloat64(S3BucketReadOnlyGauge.WithLabelValues(bucket)); got != 0 {
		t.Errorf("read-only gauge after clearing: got %.0f, want 0", got)
	}

	// disabled quota (negative) removes the quota series but keeps reporting read-only state
	UpdateBucketQuotaMetrics(bucket, -1024, true)
	if n := testutil.CollectAndCount(S3BucketQuotaBytesGauge); n != 0 {
		t.Errorf("quota series after disabling: got %d, want 0", n)
	}
	if got := testutil.ToFloat64(S3BucketReadOnlyGauge.WithLabelValues(bucket)); got != 1 {
		t.Errorf("read-only gauge with disabled quota: got %.0f, want 1", got)
	}

	UpdateBucketQuotaMetrics(bucket, 2048, true)
	DeleteBucketMetrics(bucket)
	if n := testutil.CollectAndCount(S3BucketQuotaBytesGauge); n != 0 {
		t.Errorf("quota series after bucket deletion: got %d, want 0", n)
	}
	if n := testutil.CollectAndCount(S3BucketReadOnlyGauge); n != 0 {
		t.Errorf("read-only series after bucket deletion: got %d, want 0", n)
	}
}
