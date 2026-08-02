package storage

import (
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/seaweedfs/seaweedfs/telemetry/proto"
)

func report(id, version string) *proto.TelemetryData {
	return &proto.TelemetryData{
		TopologyId:        id,
		Version:           version,
		Os:                "linux/amd64",
		VolumeServerCount: 1,
		TotalDiskBytes:    100,
		TotalVolumeCount:  1,
	}
}

func statsOf(t *testing.T, s *PrometheusStorage) map[string]interface{} {
	t.Helper()
	stats, err := s.GetStats()
	if err != nil {
		t.Fatalf("stats: %v", err)
	}
	return stats
}

func TestConfirmedClusters(t *testing.T) {
	s := newPrometheusStorage(prometheus.NewRegistry())

	// A single-day cluster is active but not confirmed; with no confirmed
	// clusters yet, distributions fall back to all active clusters.
	if err := s.StoreTelemetry(report("aaaaaaaa-0000-0000-0000-000000000001", "4.40")); err != nil {
		t.Fatal(err)
	}
	stats := statsOf(t, s)
	if stats["active_instances"] != 1 || stats["confirmed_instances"] != 0 {
		t.Fatalf("day one: active=%v confirmed=%v, want 1/0", stats["active_instances"], stats["confirmed_instances"])
	}
	if v := stats["versions"].(map[string]int); v["4.40"] != 1 {
		t.Fatalf("fallback distribution missing active cluster: %v", v)
	}

	// Give cluster A a sample from yesterday: now seen on 2 distinct days.
	s.mu.Lock()
	id := "aaaaaaaa-0000-0000-0000-000000000001"
	s.histories[id] = append([]HistorySample{{
		Ts:             time.Now().AddDate(0, 0, -1).Unix(),
		TotalDiskBytes: 50,
	}}, s.histories[id]...)
	s.mu.Unlock()

	// A one-shot cluster B arrives (like an injected report): it counts as
	// active, but the distributions now only reflect confirmed clusters.
	if err := s.StoreTelemetry(report("bbbbbbbb-0000-0000-0000-000000000002", "9.99")); err != nil {
		t.Fatal(err)
	}
	stats = statsOf(t, s)
	if stats["active_instances"] != 2 || stats["confirmed_instances"] != 1 {
		t.Fatalf("day two: active=%v confirmed=%v, want 2/1", stats["active_instances"], stats["confirmed_instances"])
	}
	v := stats["versions"].(map[string]int)
	if v["4.40"] != 1 {
		t.Errorf("confirmed cluster missing from distribution: %v", v)
	}
	if _, ok := v["9.99"]; ok {
		t.Errorf("one-shot cluster polluted the distribution: %v", v)
	}
}
