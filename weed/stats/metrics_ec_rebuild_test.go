package stats

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"
)

func TestECRebuildMetricsRegistered(t *testing.T) {
	VolumeServerECRebuildCounter.Reset()
	t.Cleanup(func() {
		VolumeServerECRebuildCounter.Reset()
	})

	// Seed the CounterVec with a value so collection returns it
	VolumeServerECRebuildCounter.WithLabelValues("success").Inc()

	count := testutil.CollectAndCount(VolumeServerECRebuildCounter)
	if count < 1 {
		t.Errorf("VolumeServerECRebuildCounter: expected at least 1 collection, got %d", count)
	}

	VolumeServerECRebuildHistogram.Observe(0.1)
	count = testutil.CollectAndCount(VolumeServerECRebuildHistogram)
	if count < 1 {
		t.Errorf("VolumeServerECRebuildHistogram: expected at least 1 collection, got %d", count)
	}
}

func TestECRebuildCounterIncrement(t *testing.T) {
	VolumeServerECRebuildCounter.Reset()
	t.Cleanup(func() {
		VolumeServerECRebuildCounter.Reset()
	})

	VolumeServerECRebuildCounter.WithLabelValues("success").Inc()
	VolumeServerECRebuildCounter.WithLabelValues("failure").Inc()

	got := testutil.ToFloat64(VolumeServerECRebuildCounter.WithLabelValues("success"))
	if got != 1 {
		t.Errorf("expected 1.0, got %f", got)
	}
	got = testutil.ToFloat64(VolumeServerECRebuildCounter.WithLabelValues("failure"))
	if got != 1 {
		t.Errorf("expected 1.0, got %f", got)
	}
}
