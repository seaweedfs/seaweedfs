package storage

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/seaweedfs/seaweedfs/telemetry/proto"
)

func counterValue(t *testing.T, c prometheus.Counter) float64 {
	t.Helper()
	var m dto.Metric
	if err := c.Write(&m); err != nil {
		t.Fatal(err)
	}
	return m.GetCounter().GetValue()
}

func TestClustersUnderTheFloorAreNotKept(t *testing.T) {
	s := newPrometheusStorage(prometheus.NewRegistry())

	// report() sits exactly on the floor, which is enough.
	if err := s.StoreTelemetry(report("aaaaaaaa-0000-0000-0000-000000000001", "4.40")); err != nil {
		t.Fatal(err)
	}
	small := report("bbbbbbbb-0000-0000-0000-000000000002", "4.40")
	small.TotalDiskBytes = proto.MinDiskBytes - 1
	if err := s.StoreTelemetry(small); err != nil {
		t.Fatal(err)
	}

	if _, ok := s.instances[small.TopologyId]; ok {
		t.Fatal("cluster under the floor was kept")
	}
	if _, ok := s.GetHistory(small.TopologyId, 90); ok {
		t.Fatal("history kept for a cluster under the floor")
	}
	if stats := statsOf(t, s); stats["active_instances"] != 1 {
		t.Errorf("active = %v, want the one cluster on the floor", stats["active_instances"])
	}
	if got := counterValue(t, s.telemetryReceived); got != 2 {
		t.Errorf("received = %v, want 2", got)
	}
	if got := counterValue(t, s.reportsSkipped); got != 1 {
		t.Errorf("skipped = %v, want 1", got)
	}
}

// State written before the floor existed carries clusters under it; loading
// drops them and marks the state dirty so the next save sheds them from disk.
func TestLoadStateDropsClustersUnderTheFloor(t *testing.T) {
	path := filepath.Join(t.TempDir(), "telemetry-state.json")

	s := newPrometheusStorage(prometheus.NewRegistry())
	kept := report("aaaaaaaa-0000-0000-0000-000000000001", "4.40")
	if err := s.StoreTelemetry(kept); err != nil {
		t.Fatal(err)
	}
	small := report("bbbbbbbb-0000-0000-0000-000000000002", "4.40")
	small.TotalDiskBytes = proto.MinDiskBytes - 1
	s.instances[small.TopologyId] = &telemetryData{TelemetryData: small, ReceivedAt: time.Now()}
	s.histories[small.TopologyId] = []HistorySample{{Ts: time.Now().Unix(), TotalDiskBytes: small.TotalDiskBytes}}
	if err := s.SaveStateIfDirty(path); err != nil {
		t.Fatal(err)
	}

	s = newPrometheusStorage(prometheus.NewRegistry())
	n, err := s.LoadState(path)
	if err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Errorf("loaded %d instances, want 1", n)
	}
	if _, ok := s.instances[small.TopologyId]; ok {
		t.Error("cluster under the floor survived the load")
	}
	if _, ok := s.histories[small.TopologyId]; ok {
		t.Error("history of a cluster under the floor survived the load")
	}
	if _, ok := s.instances[kept.TopologyId]; !ok {
		t.Error("cluster on the floor was dropped")
	}
	if !s.dirty {
		t.Error("dropping a cluster left the state clean, so it would stay on disk")
	}
}
