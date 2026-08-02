package storage

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/seaweedfs/seaweedfs/telemetry/proto"
)

func TestClusterHistory(t *testing.T) {
	s := newPrometheusStorage(prometheus.NewRegistry())

	report := &proto.TelemetryData{
		TopologyId:        "hist-cluster",
		Version:           "4.40",
		Os:                "linux/amd64",
		VolumeServerCount: 3,
		TotalDiskBytes:    1000,
		TotalVolumeCount:  10,
	}
	if err := s.StoreTelemetry(report); err != nil {
		t.Fatalf("store: %v", err)
	}

	// A second report on the same UTC day replaces the day's sample.
	report.TotalDiskBytes = 2000
	if err := s.StoreTelemetry(report); err != nil {
		t.Fatalf("store: %v", err)
	}
	samples, ok := s.GetHistory("hist-cluster", 90)
	if !ok {
		t.Fatal("cluster missing from history")
	}
	if len(samples) != 1 {
		t.Fatalf("got %d samples, want 1 (same-day replace)", len(samples))
	}
	if samples[0].TotalDiskBytes != 2000 {
		t.Errorf("same-day sample not replaced: got %d", samples[0].TotalDiskBytes)
	}

	// An older sample from a previous day is appended and survives the
	// state-file round trip.
	s.mu.Lock()
	s.histories["hist-cluster"] = append([]HistorySample{{
		Ts:             time.Now().AddDate(0, 0, -5).Unix(),
		TotalDiskBytes: 500,
	}}, s.histories["hist-cluster"]...)
	s.mu.Unlock()

	path := filepath.Join(t.TempDir(), "state.json")
	if err := s.SaveStateIfDirty(path); err != nil {
		t.Fatalf("save: %v", err)
	}
	s.instances = make(map[string]*telemetryData)
	s.histories = make(map[string][]HistorySample)
	if _, err := s.LoadState(path); err != nil {
		t.Fatalf("load: %v", err)
	}
	samples, ok = s.GetHistory("hist-cluster", 90)
	if !ok || len(samples) != 2 {
		t.Fatalf("after round trip: ok=%v samples=%d, want 2", ok, len(samples))
	}
	if samples[0].TotalDiskBytes != 500 || samples[1].TotalDiskBytes != 2000 {
		t.Errorf("samples corrupted after round trip: %+v", samples)
	}

	// GetHistory filters by the requested window.
	samples, _ = s.GetHistory("hist-cluster", 3)
	if len(samples) != 1 {
		t.Errorf("window filter: got %d samples, want 1", len(samples))
	}

	// Unknown cluster reports !ok.
	if _, ok := s.GetHistory("nope", 90); ok {
		t.Error("unknown cluster reported ok")
	}

	// Cleanup drops the history together with the instance.
	s.CleanupOldInstances(0)
	if _, ok := s.GetHistory("hist-cluster", 90); ok {
		t.Error("history survived instance cleanup")
	}
}
