package stats

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
)

func TestVolumeCreationMetricsRegistered(t *testing.T) {

	VolumeServerVolumeCreationCounter.Reset()

	t.Cleanup(func() {
		VolumeServerVolumeCreationCounter.Reset()
	})

	// Seed CounterVec with a value so collection returns it
	VolumeServerVolumeCreationCounter.WithLabelValues("success").Inc()
	VolumeServerVolumeCreationCounter.WithLabelValues("failure").Inc()

	metrics := []struct {
		name string
		c    prometheus.Collector
	}{
		{"VolumeServerVolumeCreationCounter", VolumeServerVolumeCreationCounter},
	}
	for _, m := range metrics {
		count := testutil.CollectAndCount(m.c)
		if count != 2 {
			t.Errorf("%s: expected 2 collections (success, failure), got %d", m.name, count)
		}
	}
}

func TestVolumeCreationCounterIncrement(t *testing.T) {
	VolumeServerVolumeCreationCounter.Reset()

	t.Cleanup(func() {
		VolumeServerVolumeCreationCounter.Reset()
	})

	VolumeServerVolumeCreationCounter.WithLabelValues("success").Inc()
	VolumeServerVolumeCreationCounter.WithLabelValues("failure").Inc()
	VolumeServerVolumeCreationCounter.WithLabelValues("success").Inc()

	got := testutil.ToFloat64(VolumeServerVolumeCreationCounter.WithLabelValues("success"))
	if got != 2 {
		t.Errorf("expected 2.0, got %f", got)
	}
	got = testutil.ToFloat64(VolumeServerVolumeCreationCounter.WithLabelValues("failure"))
	if got != 1 {
		t.Errorf("expected 1.0, got %f", got)
	}
}
