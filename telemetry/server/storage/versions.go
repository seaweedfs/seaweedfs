package storage

import (
	"sort"
	"strconv"
	"strings"
	"time"
)

// VersionCounts is how many clusters ran one version per day, aligned to the
// shared date axis of the enclosing VersionSeries.
type VersionCounts struct {
	Version  string   `json:"version"`
	Clusters []uint64 `json:"clusters"`
}

// OtherVersions is the versions beyond the caller's limit, summed per day so a
// stacked chart still adds up to the cluster total.
type OtherVersions struct {
	Count    int      `json:"count"`
	Clusters []uint64 `json:"clusters"`
}

// VersionSeries is the version make-up of the fleet over time: one cluster
// count per version per day, oldest release first.
type VersionSeries struct {
	Dates         []string        `json:"dates"`
	Versions      []VersionCounts `json:"versions"`
	Other         *OtherVersions  `json:"other,omitempty"`
	TotalClusters uint64          `json:"total_clusters"` // across all versions on the last day
}

// GetVersionSeries returns the last `days` days of per-version cluster counts
// across confirmed clusters. Versions beyond `limit` are folded into Other,
// keeping the ones with the most clusters on the last day.
func (s *PrometheusStorage) GetVersionSeries(days, limit int) VersionSeries {
	s.mu.RLock()
	defer s.mu.RUnlock()

	histories := versionedHistories(s.seriesHistories())
	axis := newDailySeries(days, histories)
	activeSince := time.Now().UTC().AddDate(0, 0, -activeDays).Unix()
	last := len(axis.dates) - 1
	series := VersionSeries{Dates: axis.dates}

	daily := make(map[string][]uint64)
	for _, history := range histories {
		versions, ok := align(axis, history, activeSince, sampleVersion)
		if !ok {
			continue
		}
		for i, v := range versions {
			if v == "" {
				continue // before the cluster's first day in the window
			}
			counts, ok := daily[v]
			if !ok {
				counts = make([]uint64, len(axis.dates))
				daily[v] = counts
			}
			counts[i]++
		}
	}

	for v, counts := range daily {
		series.Versions = append(series.Versions, VersionCounts{Version: v, Clusters: counts})
		series.TotalClusters += counts[last]
	}
	// Ordered by release rather than by size: the caller stacks them, and a
	// stack whose order changes with the counts is unreadable over time.
	sort.Slice(series.Versions, func(i, j int) bool {
		return versionLess(series.Versions[i].Version, series.Versions[j].Version)
	})

	if limit > 0 && len(series.Versions) > limit {
		// Old releases are a long tail of one-cluster bands; keep the versions
		// most of the fleet is on now and sum the rest into one band.
		ranked := append([]VersionCounts(nil), series.Versions...)
		sort.Slice(ranked, func(i, j int) bool {
			if ranked[i].Clusters[last] != ranked[j].Clusters[last] {
				return ranked[i].Clusters[last] > ranked[j].Clusters[last]
			}
			return versionLess(ranked[j].Version, ranked[i].Version)
		})
		other := OtherVersions{Count: len(ranked) - limit, Clusters: make([]uint64, len(axis.dates))}
		for _, v := range ranked[limit:] {
			for i, c := range v.Clusters {
				other.Clusters[i] += c
			}
		}
		kept := make(map[string]bool, limit)
		for _, v := range ranked[:limit] {
			kept[v.Version] = true
		}
		versions := series.Versions[:0]
		for _, v := range series.Versions {
			if kept[v.Version] {
				versions = append(versions, v)
			}
		}
		series.Versions = versions
		series.Other = &other
	}
	return series
}

// versionedHistories drops the samples recorded before the reported version was
// kept in history, so the version series spans the days it actually knows a
// version for instead of climbing out of a run of blank days.
func versionedHistories(histories map[string][]HistorySample) map[string][]HistorySample {
	out := make(map[string][]HistorySample, len(histories))
	for id, history := range histories {
		var kept []HistorySample
		for _, sample := range history {
			if sample.Version != "" {
				kept = append(kept, sample)
			}
		}
		if len(kept) > 0 {
			out[id] = kept
		}
	}
	return out
}

// versionLess orders release strings like "3.97" and "4.40" by their numeric
// components rather than lexically, which would sort "10.02" before "9.99". A
// build suffix sits next to the release it was built from. Anything that
// doesn't parse, such as the "unknown" a client with no version compiled in
// reports, sorts first.
func versionLess(a, b string) bool {
	an, aSuffix, aok := versionParts(a)
	bn, bSuffix, bok := versionParts(b)
	if aok != bok {
		return !aok
	}
	if !aok {
		return a < b
	}
	for i := 0; i < len(an) && i < len(bn); i++ {
		if an[i] != bn[i] {
			return an[i] < bn[i]
		}
	}
	if len(an) != len(bn) {
		return len(an) < len(bn)
	}
	return aSuffix < bSuffix
}

func versionParts(v string) ([]int, string, bool) {
	number, suffix, _ := strings.Cut(v, "-")
	fields := strings.Split(number, ".")
	parts := make([]int, 0, len(fields))
	for _, field := range fields {
		n, err := strconv.Atoi(field)
		if err != nil {
			return nil, "", false
		}
		parts = append(parts, n)
	}
	return parts, suffix, len(parts) > 0
}
