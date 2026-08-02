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

// The window starts at the oldest sample the server actually has, so a server
// with less history than the caller asked for does not report a fleet that grew
// out of nothing on its first day of data.
func TestGetMetricsWindowStartsAtOldestSample(t *testing.T) {
	s := newPrometheusStorage(prometheus.NewRegistry())
	seedSamples(s, "recent", HistorySample{TotalDiskBytes: 100, VolumeServerCount: 1}, -2, -1, 0)

	metrics, err := s.GetMetrics(30)
	if err != nil {
		t.Fatal(err)
	}
	if got := metrics["dates"].([]string); len(got) != 3 {
		t.Errorf("dates = %v, want the 3 days with history, not 30", got)
	}
	if got := metrics["disk_usage"].([]uint64); !equal(got, []uint64{100, 100, 100}) {
		t.Errorf("disk_usage = %v, want no leading zero days", got)
	}

	// History reaching past the requested window still clips to the window.
	seedSamples(s, "old", HistorySample{TotalDiskBytes: 50, VolumeServerCount: 1}, -40, -39)
	metrics, err = s.GetMetrics(10)
	if err != nil {
		t.Fatal(err)
	}
	if got := metrics["dates"].([]string); len(got) != 10 {
		t.Errorf("dates = %v, want 10 days", got)
	}
}

// "Total Disk Usage Over Time" and the stacked "Cluster Sizes Over Time" sit on
// the same dashboard, so their last day has to add up to the same number.
func TestGetMetricsAgreesWithClusterSizes(t *testing.T) {
	s := newPrometheusStorage(prometheus.NewRegistry())
	seedSamples(s, "daily", HistorySample{TotalDiskBytes: 300, VolumeServerCount: 3},
		-9, -8, -7, -6, -5, -4, -3, -2, -1, 0)
	seedSamples(s, "lagging", HistorySample{TotalDiskBytes: 200, VolumeServerCount: 2}, -3, -2)
	seedSamples(s, "gone", HistorySample{TotalDiskBytes: 900, VolumeServerCount: 9}, -9, -8)

	metrics, err := s.GetMetrics(10)
	if err != nil {
		t.Fatal(err)
	}
	disk := metrics["disk_usage"].([]uint64)
	sizes := s.GetClusterSizeSeries(10, 1) // limit forces the Other fold-in too

	if got, want := disk[len(disk)-1], sizes.TotalDisk; got != want {
		t.Errorf("metrics last day = %d, cluster sizes total = %d", got, want)
	}
	if len(disk) != len(sizes.Dates) {
		t.Errorf("metrics has %d days, cluster sizes has %d", len(disk), len(sizes.Dates))
	}
}

// Short-lived clusters -- a CI run, a docker-compose demo -- report once under a
// fresh raft topology id and never again. Held forward for the whole active
// window they would stack up into a fleet that grows every day, so they stay out
// of the totals until they are confirmed.
func TestGetMetricsExcludesUnconfirmedClusters(t *testing.T) {
	s := newPrometheusStorage(prometheus.NewRegistry())

	seedSamples(s, "real", HistorySample{TotalDiskBytes: 300, VolumeServerCount: 3}, -3, -2, -1, 0)
	for _, id := range []string{"ci-1", "ci-2", "ci-3"} {
		seedSamples(s, id, HistorySample{TotalDiskBytes: 5, VolumeServerCount: 14}, -1)
	}

	metrics, err := s.GetMetrics(4)
	if err != nil {
		t.Fatal(err)
	}
	if got := metrics["server_counts"].([]int64); !equalInt64(got, []int64{3, 3, 3, 3}) {
		t.Errorf("server_counts = %v, want the confirmed cluster alone", got)
	}
	if got := metrics["disk_usage"].([]uint64); !equal(got, []uint64{300, 300, 300, 300}) {
		t.Errorf("disk_usage = %v, want the confirmed cluster alone", got)
	}
}

// Until any cluster has two days of history the charts fall back to every
// cluster, so a fresh server doesn't serve empty series.
func TestGetMetricsFallsBackWhenNoneConfirmed(t *testing.T) {
	s := newPrometheusStorage(prometheus.NewRegistry())
	seedSamples(s, "new", HistorySample{TotalDiskBytes: 100, VolumeServerCount: 2}, 0)

	metrics, err := s.GetMetrics(7)
	if err != nil {
		t.Fatal(err)
	}
	if got := metrics["server_counts"].([]int64); !equalInt64(got, []int64{2}) {
		t.Errorf("server_counts = %v, want today's only cluster", got)
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
