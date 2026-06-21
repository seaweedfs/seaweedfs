package stats

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"
)

func TestVolumeCreationMetricsRegistered(t *testing.T) {
	MasterVolumeCreationCounter.Reset()

	t.Cleanup(func() {
		MasterVolumeCreationCounter.Reset()
	})

	// Seed the CounterVec so collection returns the labeled series.
	MasterVolumeCreationCounter.WithLabelValues("success").Inc()
	MasterVolumeCreationCounter.WithLabelValues("failure").Inc()

	// Verify the metric is registered in the shared Gather registry under its fully-qualified name.
	count, err := testutil.GatherAndCount(Gather, "SeaweedFS_master_volume_creation_total")
	if err != nil {
		t.Fatalf("GatherAndCount failed: %v", err)
	}
	if count != 2 {
		t.Errorf("expected 2 series (success, failure), got %d", count)
	}
}

func TestVolumeCreationCounterIncrement(t *testing.T) {
	MasterVolumeCreationCounter.Reset()

	t.Cleanup(func() {
		MasterVolumeCreationCounter.Reset()
	})

	MasterVolumeCreationCounter.WithLabelValues("success").Inc()
	MasterVolumeCreationCounter.WithLabelValues("failure").Inc()
	MasterVolumeCreationCounter.WithLabelValues("success").Inc()

	got := testutil.ToFloat64(MasterVolumeCreationCounter.WithLabelValues("success"))
	if got != 2 {
		t.Errorf("expected 2.0, got %f", got)
	}
	got = testutil.ToFloat64(MasterVolumeCreationCounter.WithLabelValues("failure"))
	if got != 1 {
		t.Errorf("expected 1.0, got %f", got)
	}
}
