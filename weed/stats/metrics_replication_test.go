package stats

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
)

func TestReplicationMetricsRegistered(t *testing.T) {

	VolumeServerReplicationCounter.Reset()
	VolumeServerReplicationHistogram.Reset()
	VolumeServerReplicationFailures.Reset()
	MasterUnderReplicatedVolumes.Reset()

	t.Cleanup(func() {
		VolumeServerReplicationCounter.Reset()
		VolumeServerReplicationHistogram.Reset()
		VolumeServerReplicationFailures.Reset()
		MasterUnderReplicatedVolumes.Reset()
	})

	// Seed CounterVec and HistogramVec with a value so collection returns them
	VolumeServerReplicationCounter.WithLabelValues(ReplicationOpWrite, ReplicationSuccess).Inc()
	VolumeServerReplicationHistogram.WithLabelValues(ReplicationOpWrite).Observe(0.1)
	VolumeServerReplicationFailures.WithLabelValues(ReplicationOpWrite, FailureTimeout).Inc()
	VolumeServerReplicationTargets.Observe(1)
	MasterUnderReplicatedVolumes.WithLabelValues("default", "ssd", "1", "").Set(1)

	metrics := []struct {
		name string
		c    prometheus.Collector
	}{
		{"VolumeServerReplicationCounter", VolumeServerReplicationCounter},
		{"VolumeServerReplicationHistogram", VolumeServerReplicationHistogram},
		{"VolumeServerReplicationTargets", VolumeServerReplicationTargets},
		{"VolumeServerReplicationFailures", VolumeServerReplicationFailures},
		{"MasterUnderReplicatedVolumes", MasterUnderReplicatedVolumes},
	}
	for _, m := range metrics {
		count := testutil.CollectAndCount(m.c)
		if count < 1 {
			t.Errorf("%s: expected at least 1 collection, got %d", m.name, count)
		}
	}
}

func TestReplicationCounterIncrement(t *testing.T) {
	VolumeServerReplicationCounter.Reset()

	VolumeServerReplicationCounter.WithLabelValues(ReplicationOpWrite, ReplicationSuccess).Inc()
	VolumeServerReplicationCounter.WithLabelValues(ReplicationOpWrite, ReplicationFailure).Inc()
	VolumeServerReplicationCounter.WithLabelValues(ReplicationOpDelete, ReplicationSuccess).Inc()

	got := testutil.ToFloat64(VolumeServerReplicationCounter.WithLabelValues(ReplicationOpWrite, ReplicationSuccess))
	if got != 1 {
		t.Errorf("expected 1.0, got %f", got)
	}
	got = testutil.ToFloat64(VolumeServerReplicationCounter.WithLabelValues(ReplicationOpDelete, ReplicationSuccess))
	if got != 1 {
		t.Errorf("expected 1.0, got %f", got)
	}
	got = testutil.ToFloat64(VolumeServerReplicationCounter.WithLabelValues(ReplicationOpWrite, ReplicationFailure))
	if got != 1 {
		t.Errorf("expected 1.0, got %f", got)
	}
}

func TestReplicationTargetsHistogram(t *testing.T) {
	// VolumeServerReplicationTargets is a plain prometheus.Histogram
	// (not a HistogramVec) and does not expose a Reset() method, so the
	// histogram state is cumulative across tests that share the Gather
	// registry.  We check >= to tolerate prior observations.
	VolumeServerReplicationTargets.Observe(3)

	metrics, err := Gather.Gather()
	if err != nil {
		t.Fatalf("Failed to gather metrics: %v", err)
	}

	found := false
	for _, mf := range metrics {
		if mf.GetName() == "SeaweedFS_volumeServer_replication_targets" {
			found = true
			h := mf.GetMetric()[0].GetHistogram()
			if h.GetSampleCount() < 1 {
				t.Errorf("expected histogram_count>=1, got %d", h.GetSampleCount())
			}
			if h.GetSampleSum() < 3 {
				t.Errorf("expected histogram_sum>=3, got %f", h.GetSampleSum())
			}
		}
	}
	if !found {
		t.Error("SeaweedFS_volumeServer_replication_targets not found in gathered metrics")
	}
}

func TestUnderReplicatedVolumesGauge(t *testing.T) {
	MasterUnderReplicatedVolumes.WithLabelValues("default", "ssd", "1", "").Set(5)
	got := testutil.ToFloat64(MasterUnderReplicatedVolumes.WithLabelValues("default", "ssd", "1", ""))
	if got != 5 {
		t.Errorf("expected 5.0, got %f", got)
	}
}

func TestReplicationFailuresCounter(t *testing.T) {
	VolumeServerReplicationFailures.Reset()

	VolumeServerReplicationFailures.WithLabelValues(ReplicationOpWrite, FailureTimeout).Inc()
	VolumeServerReplicationFailures.WithLabelValues(ReplicationOpDelete, FailureConnectionRefused).Inc()

	got := testutil.ToFloat64(VolumeServerReplicationFailures.WithLabelValues(ReplicationOpWrite, FailureTimeout))
	if got != 1 {
		t.Errorf("expected 1.0, got %f", got)
	}

	got = testutil.ToFloat64(VolumeServerReplicationFailures.WithLabelValues(ReplicationOpDelete, FailureConnectionRefused))
	if got != 1 {
		t.Errorf("expected 1.0, got %f", got)
	}
}
