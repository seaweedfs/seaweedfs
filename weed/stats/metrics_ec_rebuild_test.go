package stats

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"
)

func TestECRebuildMetricsRegistered(t *testing.T) {
	VolumeServerECRebuildCounter.WithLabelValues("success").Inc()
	metrics, err := Gather.Gather()
	if err != nil {
		t.Fatalf("failed to gather metrics: %v", err)
	}

	counterFound := false
	histogramFound := false

	for _, mf := range metrics {
		if mf.GetName() == "SeaweedFS_volumeServer_ec_rebuild_total" {
			counterFound = true
		}
		if mf.GetName() == "SeaweedFS_volumeServer_ec_rebuild_seconds" {
			histogramFound = true
		}
	}

	if !counterFound {
		t.Errorf("VolumeServerECRebuildCounter: metric not registered with Gather")
	}
	if !histogramFound {
		t.Errorf("VolumeServerECRebuildHistogram: metric not registered with Gather")
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
