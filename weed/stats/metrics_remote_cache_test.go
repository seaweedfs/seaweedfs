package stats

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"
)

func TestRecordRemoteCacheRead(t *testing.T) {
	RemoteCacheReadCounter.Reset()
	t.Cleanup(RemoteCacheReadCounter.Reset)

	RecordRemoteCacheRead(RemoteCacheSourceS3, "bkt", true)
	RecordRemoteCacheRead(RemoteCacheSourceS3, "bkt", true)
	RecordRemoteCacheRead(RemoteCacheSourceS3, "bkt", false)
	RecordRemoteCacheRead(RemoteCacheSourceFiler, "", true) // empty bucket -> "_other"

	if got := testutil.ToFloat64(RemoteCacheReadCounter.WithLabelValues(RemoteCacheSourceS3, "bkt", RemoteCacheResultHit)); got != 2 {
		t.Errorf("s3/bkt hit = %v, want 2", got)
	}
	if got := testutil.ToFloat64(RemoteCacheReadCounter.WithLabelValues(RemoteCacheSourceS3, "bkt", RemoteCacheResultMiss)); got != 1 {
		t.Errorf("s3/bkt miss = %v, want 1", got)
	}
	if got := testutil.ToFloat64(RemoteCacheReadCounter.WithLabelValues(RemoteCacheSourceFiler, "_other", RemoteCacheResultHit)); got != 1 {
		t.Errorf("filer/_other hit = %v, want 1", got)
	}
}

func TestDeleteBucketMetricsRemovesRemoteCacheSeries(t *testing.T) {
	RemoteCacheReadCounter.Reset()
	t.Cleanup(RemoteCacheReadCounter.Reset)

	RecordRemoteCacheRead(RemoteCacheSourceS3, "delbkt", true)
	RecordRemoteCacheRead(RemoteCacheSourceS3, "delbkt", false)
	RecordRemoteCacheRead(RemoteCacheSourceFiler, "delbkt", true)
	RecordRemoteCacheRead(RemoteCacheSourceS3, "keepbkt", true)

	if got := testutil.CollectAndCount(RemoteCacheReadCounter); got != 4 {
		t.Fatalf("before delete: %d series, want 4", got)
	}

	DeleteBucketMetrics("delbkt")

	if got := testutil.CollectAndCount(RemoteCacheReadCounter); got != 1 {
		t.Errorf("after delete: %d series, want 1 (only keepbkt)", got)
	}
	if got := testutil.ToFloat64(RemoteCacheReadCounter.WithLabelValues(RemoteCacheSourceS3, "keepbkt", RemoteCacheResultHit)); got != 1 {
		t.Errorf("keepbkt hit should survive deletion, got %v", got)
	}
}
