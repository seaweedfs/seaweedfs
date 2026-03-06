package testrunner

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestSaveAndLoadBaseline(t *testing.T) {
	dir := t.TempDir()

	b := &Baseline{
		Version:    1,
		GitSHA:     "8b2b5f6f6abc123def",
		Scenario:   "cp84-soak-4h.yaml",
		ScenarioSHA: "abc123",
		Topology:   BaselineTopology{Nodes: 2, Servers: []string{"m01", "M02"}},
		Config:     BaselineConfig{ReplicaFactor: 2, Durability: "best_effort", WALSize: "64MB"},
		Timestamp:  time.Date(2026, 3, 6, 12, 0, 0, 0, time.UTC),
		Metrics: map[string]float64{
			"p99_write_latency_seconds": 0.005,
			"write_iops":               50000,
		},
		DurationSec:     14400,
		HardFailsPassed: true,
	}

	path, err := SaveBaseline(dir, b)
	if err != nil {
		t.Fatalf("SaveBaseline: %v", err)
	}
	if filepath.Base(path) != "20260306-8b2b5f6f6a.json" {
		t.Errorf("unexpected filename: %s", filepath.Base(path))
	}

	// Verify file exists.
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("file not created: %v", err)
	}

	// Load it back.
	loaded, err := LoadLatestBaseline(dir)
	if err != nil {
		t.Fatalf("LoadLatestBaseline: %v", err)
	}
	if loaded.GitSHA != b.GitSHA {
		t.Errorf("GitSHA = %q, want %q", loaded.GitSHA, b.GitSHA)
	}
	if loaded.Metrics["write_iops"] != 50000 {
		t.Errorf("write_iops = %f, want 50000", loaded.Metrics["write_iops"])
	}
}

func TestSaveBaselineIdempotent(t *testing.T) {
	dir := t.TempDir()
	b := &Baseline{
		Version:   1,
		GitSHA:    "abc123",
		Timestamp: time.Date(2026, 3, 6, 12, 0, 0, 0, time.UTC),
		Metrics:   map[string]float64{},
	}

	p1, err := SaveBaseline(dir, b)
	if err != nil {
		t.Fatalf("first save: %v", err)
	}
	p2, err := SaveBaseline(dir, b)
	if err != nil {
		t.Fatalf("second save: %v", err)
	}
	if p1 != p2 {
		t.Errorf("paths differ: %s vs %s", p1, p2)
	}
}

func TestLoadLatestBaseline_Empty(t *testing.T) {
	dir := t.TempDir()
	_, err := LoadLatestBaseline(dir)
	if err == nil {
		t.Error("expected error for empty dir")
	}
}

func TestLoadLatestBaseline_PicksLatest(t *testing.T) {
	dir := t.TempDir()

	old := &Baseline{
		Version: 1, GitSHA: "old1234567",
		Timestamp: time.Date(2026, 3, 1, 0, 0, 0, 0, time.UTC),
		Metrics:   map[string]float64{"write_iops": 40000},
	}
	newer := &Baseline{
		Version: 1, GitSHA: "new1234567",
		Timestamp: time.Date(2026, 3, 6, 0, 0, 0, 0, time.UTC),
		Metrics:   map[string]float64{"write_iops": 50000},
	}

	SaveBaseline(dir, old)
	SaveBaseline(dir, newer)

	loaded, err := LoadLatestBaseline(dir)
	if err != nil {
		t.Fatalf("LoadLatestBaseline: %v", err)
	}
	if loaded.Metrics["write_iops"] != 50000 {
		t.Errorf("expected latest baseline (50000), got %f", loaded.Metrics["write_iops"])
	}
}
