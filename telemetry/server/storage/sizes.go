package storage

import (
	"sort"
	"time"
)

// ClusterSeries is one cluster's daily disk usage and volume server count,
// aligned to the shared date axis of the enclosing ClusterSizeSeries.
type ClusterSeries struct {
	ClusterId string   `json:"cluster_id"`
	Disk      []uint64 `json:"disk"`
	Servers   []uint64 `json:"servers"`
}

// OtherSeries is the clusters beyond the caller's limit, summed per day so a
// stacked chart still adds up to the fleet total.
type OtherSeries struct {
	Count   int      `json:"count"`
	Disk    []uint64 `json:"disk"`
	Servers []uint64 `json:"servers"`
}

// ClusterSizeSeries is per-cluster disk usage and volume server count over
// time: one value per cluster per day, largest cluster first, ranked by disk on
// their most recent day. Both metrics share one ranking so a cluster keeps its
// place, and its colour, across the charts drawn from this.
type ClusterSizeSeries struct {
	Dates        []string        `json:"dates"`
	Clusters     []ClusterSeries `json:"clusters"`
	Other        *OtherSeries    `json:"other,omitempty"`
	ClusterCount int             `json:"cluster_count"`
	TotalDisk    uint64          `json:"total_disk"`    // across all clusters on the last day
	TotalServers uint64          `json:"total_servers"` // across all clusters on the last day
}

// GetClusterSizeSeries returns the last `days` days of per-cluster disk usage
// and volume server counts across confirmed clusters. Clusters beyond `limit`
// are folded into Other.
func (s *PrometheusStorage) GetClusterSizeSeries(days, limit int) ClusterSizeSeries {
	s.mu.RLock()
	defer s.mu.RUnlock()

	histories := s.seriesHistories()
	axis := newDailySeries(days, histories)
	activeSince := time.Now().UTC().AddDate(0, 0, -activeDays).Unix()
	last := len(axis.dates) - 1
	series := ClusterSizeSeries{Dates: axis.dates}

	for id, history := range histories {
		disk, ok := axis.align(history, activeSince, diskBytes)
		if !ok {
			continue
		}
		servers, _ := axis.align(history, activeSince, serverCount)
		series.Clusters = append(series.Clusters, ClusterSeries{ClusterId: id, Disk: disk, Servers: servers})
		series.TotalDisk += disk[last]
		series.TotalServers += servers[last]
	}
	series.ClusterCount = len(series.Clusters)

	// Rank by the latest day so the stack reads largest-first at its right
	// edge, tie-breaking on id to keep the order stable across refreshes.
	sort.Slice(series.Clusters, func(i, j int) bool {
		a, b := series.Clusters[i], series.Clusters[j]
		if a.Disk[last] != b.Disk[last] {
			return a.Disk[last] > b.Disk[last]
		}
		return a.ClusterId < b.ClusterId
	})

	if limit > 0 && len(series.Clusters) > limit {
		other := OtherSeries{
			Count:   len(series.Clusters) - limit,
			Disk:    make([]uint64, len(axis.dates)),
			Servers: make([]uint64, len(axis.dates)),
		}
		for _, c := range series.Clusters[limit:] {
			for i, v := range c.Disk {
				other.Disk[i] += v
			}
			for i, v := range c.Servers {
				other.Servers[i] += v
			}
		}
		series.Clusters = series.Clusters[:limit]
		series.Other = &other
	}
	return series
}
