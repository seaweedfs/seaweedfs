package storage

import (
	"time"

	"github.com/seaweedfs/seaweedfs/telemetry/proto"
)

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
