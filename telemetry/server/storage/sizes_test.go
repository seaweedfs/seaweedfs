package storage

import (
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/seaweedfs/seaweedfs/telemetry/proto"
)

// seedHistory gives a cluster one sample per listed day offset (0 is today).
func seedHistory(s *PrometheusStorage, id string, disk uint64, dayOffsets ...int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, offset := range dayOffsets {
		s.histories[id] = append(s.histories[id], HistorySample{
			Ts:             time.Now().AddDate(0, 0, offset).Unix(),
			TotalDiskBytes: disk,
		})
	}
}

func TestClusterSizeSeries(t *testing.T) {
	s := newPrometheusStorage(prometheus.NewRegistry())

	// Reported every day of the window.
	seedHistory(s, "daily", 300, -9, -8, -7, -6, -5, -4, -3, -2, -1, 0)
	// Reported two days ago and not since: still active, so its size is held
	// to the right edge instead of dropping out of the stack.
	seedHistory(s, "lagging", 200, -2)
	// Stopped reporting past the active window: its own days only.
	seedHistory(s, "gone", 900, -9, -8)

	series := s.GetClusterSizeSeries(10, 0)
	if len(series.Dates) != 10 {
		t.Fatalf("dates = %v, want 10 days", series.Dates)
	}
	if series.ClusterCount != 3 {
		t.Fatalf("cluster_count = %d, want 3", series.ClusterCount)
	}

	byId := map[string][]uint64{}
	for _, c := range series.Clusters {
		byId[c.ClusterId] = c.Disk
	}
	if got := byId["daily"]; !equal(got, []uint64{300, 300, 300, 300, 300, 300, 300, 300, 300, 300}) {
		t.Errorf("daily = %v, want 300 every day", got)
	}
	if got := byId["lagging"]; !equal(got, []uint64{0, 0, 0, 0, 0, 0, 0, 200, 200, 200}) {
		t.Errorf("lagging = %v, want its size carried to the right edge", got)
	}
	if got := byId["gone"]; !equal(got, []uint64{900, 900, 0, 0, 0, 0, 0, 0, 0, 0}) {
		t.Errorf("gone = %v, want no capacity after its last report", got)
	}

	// The total is the last day's stack height: daily + lagging, not gone.
	if series.TotalDisk != 500 {
		t.Errorf("total_disk = %d, want 500", series.TotalDisk)
	}
	// Largest on the last day comes first so the stack reads top-down.
	if series.Clusters[0].ClusterId != "daily" {
		t.Errorf("order = %s first, want daily", series.Clusters[0].ClusterId)
	}

	// Clusters past the limit are summed into "other", per day.
	series = s.GetClusterSizeSeries(10, 1)
	if len(series.Clusters) != 1 || series.Clusters[0].ClusterId != "daily" {
		t.Fatalf("limited clusters = %+v, want just daily", series.Clusters)
	}
	if series.Other == nil || series.Other.Count != 2 {
		t.Fatalf("other = %+v, want 2 clusters", series.Other)
	}
	if !equal(series.Other.Disk, []uint64{900, 900, 0, 0, 0, 0, 0, 200, 200, 200}) {
		t.Errorf("other = %v, want lagging+gone summed per day", series.Other.Disk)
	}
	if series.ClusterCount != 3 || series.TotalDisk != 500 {
		t.Errorf("limit changed totals: count=%d disk=%d, want 3/500", series.ClusterCount, series.TotalDisk)
	}
}

// A report that lands after an earlier one on the same day replaces it, so the
// series shows one value per cluster per day.
func TestClusterSizeSeriesUsesLatestDailySample(t *testing.T) {
	s := newPrometheusStorage(prometheus.NewRegistry())

	data := &proto.TelemetryData{
		TopologyId:     "aaaaaaaa-0000-0000-0000-000000000001",
		Version:        "4.40",
		Os:             "linux/amd64",
		TotalDiskBytes: 100,
	}
	if err := s.StoreTelemetry(data); err != nil {
		t.Fatal(err)
	}
	data.TotalDiskBytes = 700
	if err := s.StoreTelemetry(data); err != nil {
		t.Fatal(err)
	}

	series := s.GetClusterSizeSeries(3, 0)
	if len(series.Clusters) != 1 {
		t.Fatalf("clusters = %+v, want 1", series.Clusters)
	}
	if got := series.Clusters[0].Disk; !equal(got, []uint64{0, 0, 700}) {
		t.Errorf("disk = %v, want today's latest sample only", got)
	}
	if series.TotalDisk != 700 {
		t.Errorf("total_disk = %d, want 700", series.TotalDisk)
	}
}

func equal(a, b []uint64) bool {
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
