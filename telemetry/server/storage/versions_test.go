package storage

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
)

func TestVersionSeries(t *testing.T) {
	s := newPrometheusStorage(prometheus.NewRegistry())

	// Upgraded mid-window: its band leaves the old version for the new one.
	seedSamples(s, "upgraded", HistorySample{Version: "4.39"}, -6, -5, -4, -3)
	seedSamples(s, "upgraded", HistorySample{Version: "4.40"}, -2, -1, 0)
	// Reported every day on the same version.
	seedSamples(s, "steady", HistorySample{Version: "4.40"}, -6, -5, -4, -3, -2, -1, 0)
	// One day of history only: unconfirmed, so it stays out of the stack.
	seedSamples(s, "oneshot", HistorySample{Version: "4.40"}, -1)
	// Confirmed, but its samples predate versions being recorded.
	seedSamples(s, "versionless", HistorySample{}, -13, -12, -11, -10, -9, -8, -7)

	series := s.GetVersionSeries(10, 0)

	// The versionless days are dropped before the axis is built, so the chart
	// spans the days a version is known for instead of climbing out of blanks.
	if len(series.Dates) != 7 {
		t.Fatalf("dates = %v, want the 7 days with versions", series.Dates)
	}
	if len(series.Versions) != 2 ||
		series.Versions[0].Version != "4.39" || series.Versions[1].Version != "4.40" {
		t.Fatalf("versions = %+v, want 4.39 then 4.40", series.Versions)
	}
	if got := series.Versions[0].Clusters; !equal(got, []uint64{1, 1, 1, 1, 0, 0, 0}) {
		t.Errorf("4.39 = %v, want the upgraded cluster's first four days", got)
	}
	if got := series.Versions[1].Clusters; !equal(got, []uint64{1, 1, 1, 1, 2, 2, 2}) {
		t.Errorf("4.40 = %v, want steady plus upgraded from day 5", got)
	}
	if series.TotalClusters != 2 {
		t.Errorf("total_clusters = %d, want 2", series.TotalClusters)
	}
	if series.Other != nil {
		t.Errorf("other = %+v, want none without a limit", series.Other)
	}
}

func TestVersionSeriesHoldsForwardAndLimits(t *testing.T) {
	s := newPrometheusStorage(prometheus.NewRegistry())

	// Stopped reporting past the active window: its own days only.
	seedSamples(s, "gone", HistorySample{Version: "3.97"}, -14, -13, -12, -11, -10, -9, -8)
	// Reported every day of the window.
	seedSamples(s, "daily", HistorySample{Version: "4.40"}, -9, -8, -7, -6, -5, -4, -3, -2, -1, 0)
	// Stopped reporting two days ago: still active, so it holds its version
	// to the right edge instead of dropping out of the stack.
	seedSamples(s, "lagging", HistorySample{Version: "4.30"}, -8, -7, -6, -5, -4, -3, -2)

	series := s.GetVersionSeries(10, 0)
	if len(series.Dates) != 10 {
		t.Fatalf("dates = %v, want 10 days", series.Dates)
	}
	byVersion := map[string][]uint64{}
	for _, v := range series.Versions {
		byVersion[v.Version] = v.Clusters
	}
	if got := byVersion["3.97"]; !equal(got, []uint64{1, 1, 0, 0, 0, 0, 0, 0, 0, 0}) {
		t.Errorf("3.97 = %v, want nothing after its last report", got)
	}
	if got := byVersion["4.30"]; !equal(got, []uint64{0, 1, 1, 1, 1, 1, 1, 1, 1, 1}) {
		t.Errorf("4.30 = %v, want carried to the right edge", got)
	}
	if got := byVersion["4.40"]; !equal(got, []uint64{1, 1, 1, 1, 1, 1, 1, 1, 1, 1}) {
		t.Errorf("4.40 = %v, want 1 every day", got)
	}
	if series.TotalClusters != 2 {
		t.Errorf("total_clusters = %d, want the 2 clusters still reporting", series.TotalClusters)
	}

	// Past the limit, the versions the fewest clusters are on are summed into
	// one band; ties on the last day keep the newer version.
	series = s.GetVersionSeries(10, 1)
	if len(series.Versions) != 1 || series.Versions[0].Version != "4.40" {
		t.Fatalf("limited versions = %+v, want just 4.40", series.Versions)
	}
	if series.Other == nil || series.Other.Count != 2 {
		t.Fatalf("other = %+v, want 2 versions", series.Other)
	}
	if !equal(series.Other.Clusters, []uint64{1, 2, 1, 1, 1, 1, 1, 1, 1, 1}) {
		t.Errorf("other = %v, want 3.97+4.30 summed per day", series.Other.Clusters)
	}
	if series.TotalClusters != 2 {
		t.Errorf("limit changed total_clusters: %d, want 2", series.TotalClusters)
	}
}

func TestVersionLess(t *testing.T) {
	// Ordered oldest to newest; every pair must compare in this order.
	ordered := []string{"unknown", "3.97", "4.02", "4.30", "4.30-enterprise", "9.99", "10.02"}
	for i := 0; i < len(ordered); i++ {
		for j := i + 1; j < len(ordered); j++ {
			if !versionLess(ordered[i], ordered[j]) {
				t.Errorf("versionLess(%q, %q) = false, want true", ordered[i], ordered[j])
			}
			if versionLess(ordered[j], ordered[i]) {
				t.Errorf("versionLess(%q, %q) = true, want false", ordered[j], ordered[i])
			}
		}
	}
}
