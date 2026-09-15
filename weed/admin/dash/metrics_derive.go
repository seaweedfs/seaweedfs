package dash

import (
	"math"
	"sync"
	"time"
)

// Suffixes for series derived from raw scrapes. Counters become per-second
// rates and histograms become latency quantiles, both computed from the delta
// against the previous scrape so the values reflect the last interval rather
// than process lifetime totals.
const (
	suffixRate = ":rate"
	suffixP50  = ":p50"
	suffixP95  = ":p95"
	suffixP99  = ":p99"
)

type prevScrape struct {
	t       time.Time
	value   float64
	buckets []histogramBucket
}

type metricsDeriver struct {
	mu   sync.Mutex
	prev map[string]prevScrape
}

func newMetricsDeriver() *metricsDeriver {
	return &metricsDeriver{prev: make(map[string]prevScrape)}
}

// record stores the raw value and, for counters and histograms, the derived
// rate/quantile series for this interval.
func (d *metricsDeriver) record(store *metricsStore, source string, m scrapedMetric, now time.Time) {
	key := source + "/" + m.name + "/" + labelKey(m.labels)

	d.mu.Lock()
	prev, hadPrev := d.prev[key]
	d.prev[key] = prevScrape{t: now, value: m.value, buckets: m.buckets}
	d.mu.Unlock()

	switch m.kind {
	case kindGauge:
		store.recordLabeled(source, m.name, m.labels, m.value, now)
	case kindCounter:
		if !hadPrev {
			return
		}
		dt := now.Sub(prev.t).Seconds()
		if dt <= 0 {
			return
		}
		delta := m.value - prev.value
		if delta < 0 {
			// Counter reset (process restart); skip this interval.
			return
		}
		store.recordLabeled(source, m.name+suffixRate, m.labels, delta/dt, now)
	case kindHistogram:
		if !hadPrev {
			return
		}
		delta := bucketDelta(prev.buckets, m.buckets)
		if len(delta) == 0 {
			return
		}
		for suffix, q := range map[string]float64{suffixP50: 0.5, suffixP95: 0.95, suffixP99: 0.99} {
			store.recordLabeled(source, m.name+suffix, m.labels, histogramQuantile(delta, q), now)
		}
	}
}

// bucketDelta subtracts cumulative bucket counts, yielding the distribution
// observed during the interval. Returns nil on a reset or bucket mismatch.
func bucketDelta(prev, cur []histogramBucket) []histogramBucket {
	if len(prev) != len(cur) {
		return nil
	}
	out := make([]histogramBucket, len(cur))
	for i := range cur {
		if cur[i].upperBound != prev[i].upperBound {
			return nil
		}
		c := cur[i].count - prev[i].count
		if c < 0 {
			return nil
		}
		out[i] = histogramBucket{upperBound: cur[i].upperBound, count: c}
	}
	if out[len(out)-1].count == 0 {
		return nil
	}
	return out
}

// histogramQuantile estimates a quantile from cumulative buckets by linear
// interpolation within the matching bucket, matching Prometheus' approach.
func histogramQuantile(buckets []histogramBucket, q float64) float64 {
	total := buckets[len(buckets)-1].count
	if total == 0 {
		return 0
	}
	rank := q * total
	prevCount, prevBound := 0.0, 0.0
	for _, b := range buckets {
		if b.count < rank {
			prevCount, prevBound = b.count, b.upperBound
			continue
		}
		if math.IsInf(b.upperBound, 1) {
			return prevBound
		}
		span := b.count - prevCount
		if span <= 0 {
			return b.upperBound
		}
		return prevBound + (b.upperBound-prevBound)*(rank-prevCount)/span
	}
	return buckets[len(buckets)-1].upperBound
}
