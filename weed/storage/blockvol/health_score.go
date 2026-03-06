package blockvol

import (
	"sync/atomic"
	"time"
)

// HealthStats contains health score details for reporting.
type HealthStats struct {
	Score          float64
	ScrubErrors    int64
	LastScrubError int64 // unix timestamp, 0 = never
	LastScrubTime  int64 // unix timestamp, 0 = never
}

// HealthScore tracks the health of a block volume based on scrub results.
// WAL lag is NOT part of engine health — it is tracked at the registry level only.
type HealthScore struct {
	scrubErrors    atomic.Int64
	lastScrubError atomic.Int64 // unix seconds
	lastScrubTime  atomic.Int64 // unix seconds
}

// NewHealthScore creates a new HealthScore with perfect health (1.0).
func NewHealthScore() *HealthScore {
	return &HealthScore{}
}

// RecordScrubError increments the scrub error counter and records timestamp.
func (h *HealthScore) RecordScrubError() {
	h.scrubErrors.Add(1)
	h.lastScrubError.Store(time.Now().Unix())
}

// RecordScrubComplete records when a scrub pass finishes.
func (h *HealthScore) RecordScrubComplete() {
	h.lastScrubTime.Store(time.Now().Unix())
}

// Score returns the current health score in [0.0, 1.0].
// Penalties:
//   - Scrub errors > 0: penalty = min(0.5, errors × 0.1)
//   - Last scrub > 48h ago: penalty = 0.1
func (h *HealthScore) Score() float64 {
	score := 1.0

	// Scrub error penalty.
	errs := h.scrubErrors.Load()
	if errs > 0 {
		penalty := float64(errs) * 0.1
		if penalty > 0.5 {
			penalty = 0.5
		}
		score -= penalty
	}

	// Stale scrub penalty.
	lastScrub := h.lastScrubTime.Load()
	if lastScrub > 0 && time.Since(time.Unix(lastScrub, 0)) > 48*time.Hour {
		score -= 0.1
	}

	if score < 0.0 {
		score = 0.0
	}
	return score
}

// Stats returns detailed health statistics.
func (h *HealthScore) Stats() HealthStats {
	return HealthStats{
		Score:          h.Score(),
		ScrubErrors:    h.scrubErrors.Load(),
		LastScrubError: h.lastScrubError.Load(),
		LastScrubTime:  h.lastScrubTime.Load(),
	}
}
