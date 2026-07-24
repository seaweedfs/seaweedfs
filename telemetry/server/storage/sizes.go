package storage

import (
	"sort"
	"time"
)

// ClusterSeries is one cluster's daily disk usage, aligned to the shared date
// axis of the enclosing ClusterSizeSeries.
type ClusterSeries struct {
	ClusterId string   `json:"cluster_id"`
	Disk      []uint64 `json:"disk"`
}

// OtherSeries is the clusters beyond the caller's limit, summed per day so a
// stacked chart still adds up to the fleet total.
type OtherSeries struct {
	Count int      `json:"count"`
	Disk  []uint64 `json:"disk"`
}

// ClusterSizeSeries is per-cluster disk usage over time: one value per cluster
// per day, largest cluster first, ranked by their most recent day.
type ClusterSizeSeries struct {
	Dates        []string        `json:"dates"`
	Clusters     []ClusterSeries `json:"clusters"`
	Other        *OtherSeries    `json:"other,omitempty"`
	ClusterCount int             `json:"cluster_count"`
	TotalDisk    uint64          `json:"total_disk"` // across all clusters on the last day
}

// GetClusterSizeSeries returns the last `days` days of per-cluster disk usage.
// Clusters beyond `limit` are folded into Other. Clusters report roughly once
// a day at no fixed hour, so a day without a report carries the previous value
// forward rather than dropping to zero; a cluster that stopped reporting
// altogether ends at its last sample instead of holding capacity forever.
func (s *PrometheusStorage) GetClusterSizeSeries(days, limit int) ClusterSizeSeries {
	s.mu.RLock()
	defer s.mu.RUnlock()

	now := time.Now().UTC()
	today := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, time.UTC)
	dayOf := make(map[string]int, days)
	series := ClusterSizeSeries{Dates: make([]string, days)}
	for i := range series.Dates {
		series.Dates[i] = today.AddDate(0, 0, i-days+1).Format("2006-01-02")
		dayOf[series.Dates[i]] = i
	}
	activeSince := now.AddDate(0, 0, -activeDays).Unix()

	for id, history := range s.histories {
		disk := make([]uint64, days)
		reported := make([]bool, days)
		first, last := -1, -1
		for _, sample := range history {
			i, ok := dayOf[time.Unix(sample.Ts, 0).UTC().Format("2006-01-02")]
			if !ok {
				continue
			}
			disk[i], reported[i] = sample.TotalDiskBytes, true
			if first < 0 {
				first = i
			}
			last = i
			if sample.Ts >= activeSince {
				last = days - 1 // still reporting, so hold its size to the right edge
			}
		}
		if first < 0 {
			continue
		}
		for i := first + 1; i <= last; i++ {
			if !reported[i] {
				disk[i] = disk[i-1]
			}
		}
		series.Clusters = append(series.Clusters, ClusterSeries{ClusterId: id, Disk: disk})
		series.TotalDisk += disk[days-1]
	}
	series.ClusterCount = len(series.Clusters)

	// Rank by the latest day so the stack reads largest-first at its right
	// edge, tie-breaking on id to keep the order stable across refreshes.
	sort.Slice(series.Clusters, func(i, j int) bool {
		a, b := series.Clusters[i], series.Clusters[j]
		if a.Disk[days-1] != b.Disk[days-1] {
			return a.Disk[days-1] > b.Disk[days-1]
		}
		return a.ClusterId < b.ClusterId
	})

	if limit > 0 && len(series.Clusters) > limit {
		other := OtherSeries{
			Count: len(series.Clusters) - limit,
			Disk:  make([]uint64, days),
		}
		for _, c := range series.Clusters[limit:] {
			for i, v := range c.Disk {
				other.Disk[i] += v
			}
		}
		series.Clusters = series.Clusters[:limit]
		series.Other = &other
	}
	return series
}
