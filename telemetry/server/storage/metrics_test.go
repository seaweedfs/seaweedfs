package storage

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
)

// Every cluster counts towards every day it reported on, not just towards the
// one day it last reported on.
func TestGetMetricsSumsEachDay(t *testing.T) {
	s := newPrometheusStorage(prometheus.NewRegistry())

	// Reported every day of the window.
	seedSamples(s, "daily", HistorySample{TotalDiskBytes: 300, VolumeServerCount: 3},
		-9, -8, -7, -6, -5, -4, -3, -2, -1, 0)
	// Stopped reporting past the active window: counts on its own days only.
	seedSamples(s, "gone", HistorySample{TotalDiskBytes: 900, VolumeServerCount: 9}, -9, -8)

	metrics, err := s.GetMetrics(10)
	if err != nil {
		t.Fatal(err)
	}

	if got := metrics["dates"].([]string); len(got) != 10 {
		t.Fatalf("dates = %v, want 10 days", got)
	}
	if got := metrics["disk_usage"].([]uint64); !equal(got,
		[]uint64{1200, 1200, 300, 300, 300, 300, 300, 300, 300, 300}) {
		t.Errorf("disk_usage = %v, want the fleet total per day", got)
	}
	if got := metrics["server_counts"].([]int64); !equalInt64(got,
		[]int64{12, 12, 3, 3, 3, 3, 3, 3, 3, 3}) {
		t.Errorf("server_counts = %v, want the fleet total per day", got)
	}
}

// A cluster that skipped a day keeps its size on that day rather than dipping
// the fleet total to zero and back.
func TestGetMetricsCarriesSkippedDaysForward(t *testing.T) {
	s := newPrometheusStorage(prometheus.NewRegistry())

	seedSamples(s, "gappy", HistorySample{TotalDiskBytes: 500, VolumeServerCount: 5}, -3, -1)

	metrics, err := s.GetMetrics(4)
	if err != nil {
		t.Fatal(err)
	}
	if got := metrics["disk_usage"].([]uint64); !equal(got, []uint64{500, 500, 500, 500}) {
		t.Errorf("disk_usage = %v, want the skipped days carried forward", got)
	}
}

func equalInt64(a, b []int64) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
