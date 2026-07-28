package storage

import (
	"time"

	"github.com/seaweedfs/seaweedfs/telemetry/proto"
)

// confirmDays is how many distinct UTC days a cluster must have reported
// on before it counts as confirmed in the aggregated stats.
const confirmDays = 2

// activeDays is how recently a cluster must have reported to count as active.
const activeDays = 7

// HistorySample is one retained data point of a cluster's daily reports.
// Tags are kept short because thousands of samples end up in the state file.
type HistorySample struct {
	Ts                int64  `json:"ts"` // unix seconds the report was received
	TotalDiskBytes    uint64 `json:"disk"`
	TotalVolumeCount  int32  `json:"volumes"`
	VolumeServerCount int32  `json:"servers"`
}

// appendHistory records the report as the cluster's sample for the day,
// replacing an earlier sample from the same UTC day. Callers must hold s.mu.
func (s *PrometheusStorage) appendHistory(data *proto.TelemetryData, receivedAt time.Time) {
	sample := HistorySample{
		Ts:                receivedAt.Unix(),
		TotalDiskBytes:    data.TotalDiskBytes,
		TotalVolumeCount:  data.TotalVolumeCount,
		VolumeServerCount: data.VolumeServerCount,
	}
	h := s.histories[data.TopologyId]
	if n := len(h); n > 0 && sameUTCDay(h[n-1].Ts, sample.Ts) {
		h[n-1] = sample
	} else {
		h = append(h, sample)
	}
	s.histories[data.TopologyId] = h
}

func sameUTCDay(a, b int64) bool {
	ta, tb := time.Unix(a, 0).UTC(), time.Unix(b, 0).UTC()
	return ta.Year() == tb.Year() && ta.YearDay() == tb.YearDay()
}

// dailySeries is the shared date axis of the fleet-wide time series: one slot
// per UTC day, ending today.
type dailySeries struct {
	dates []string
	dayOf map[string]int
}

// newDailySeries builds the axis of UTC days ending today, spanning `days` days
// but starting no earlier than the first day any cluster reported on: a fresh
// server is asked for more days than it has history for, and padding those days
// with zeros draws a climb out of nothing that never happened.
func newDailySeries(days int, histories map[string][]HistorySample) dailySeries {
	today := utcDay(time.Now().Unix())
	requested := today.AddDate(0, 0, 1-days)

	// Samples are appended in receive order, so [0] is a cluster's oldest.
	var start time.Time
	for _, history := range histories {
		if len(history) == 0 {
			continue
		}
		if first := utcDay(history[0].Ts); start.IsZero() || first.Before(start) {
			start = first
		}
	}
	if start.IsZero() || start.Before(requested) {
		start = requested
	}

	n := int(today.Sub(start)/(24*time.Hour)) + 1
	d := dailySeries{
		dates: make([]string, n),
		dayOf: make(map[string]int, n),
	}
	for i := range d.dates {
		d.dates[i] = start.AddDate(0, 0, i).Format("2006-01-02")
		d.dayOf[d.dates[i]] = i
	}
	return d
}

func utcDay(ts int64) time.Time {
	t := time.Unix(ts, 0).UTC()
	return time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, time.UTC)
}

func diskBytes(s HistorySample) uint64   { return s.TotalDiskBytes }
func serverCount(s HistorySample) uint64 { return uint64(s.VolumeServerCount) }

// align lays one cluster's history onto the axis, picking `value` out of each
// sample. Clusters report roughly once a day at no fixed hour, so a day without
// a report carries the previous value forward rather than dropping to zero; a
// cluster that stopped reporting altogether ends at its last sample instead of
// holding capacity forever. Reports false when the cluster has nothing in range.
func (d dailySeries) align(history []HistorySample, activeSince int64, value func(HistorySample) uint64) ([]uint64, bool) {
	out := make([]uint64, len(d.dates))
	reported := make([]bool, len(d.dates))
	first, last := -1, -1
	for _, sample := range history {
		i, ok := d.dayOf[time.Unix(sample.Ts, 0).UTC().Format("2006-01-02")]
		if !ok {
			continue
		}
		out[i], reported[i] = value(sample), true
		if first < 0 {
			first = i
		}
		last = i
		if sample.Ts >= activeSince {
			last = len(d.dates) - 1 // still reporting, so hold to the right edge
		}
	}
	if first < 0 {
		return nil, false
	}
	for i := first + 1; i <= last; i++ {
		if !reported[i] {
			out[i] = out[i-1]
		}
	}
	return out, true
}

// GetHistory returns the cluster's samples from the last `days` days.
// The second return value reports whether the cluster is known at all.
func (s *PrometheusStorage) GetHistory(clusterId string, days int) ([]HistorySample, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	h, ok := s.histories[clusterId]
	if !ok {
		return nil, false
	}
	cutoff := time.Now().AddDate(0, 0, -days).Unix()
	samples := make([]HistorySample, 0, len(h))
	for _, sample := range h {
		if sample.Ts >= cutoff {
			samples = append(samples, sample)
		}
	}
	return samples, true
}
