package dash

import (
	"math"
	"testing"
	"time"
)

func TestHistogramQuantile(t *testing.T) {
	// 100 observations spread evenly across 0-1s in 10 buckets.
	buckets := []histogramBucket{
		{0.1, 10}, {0.2, 20}, {0.3, 30}, {0.4, 40}, {0.5, 50},
		{0.6, 60}, {0.7, 70}, {0.8, 80}, {0.9, 90}, {1.0, 100},
		{math.Inf(1), 100},
	}
	for _, tc := range []struct {
		q    float64
		want float64
	}{
		{0.5, 0.5},
		{0.95, 0.95},
		{0.99, 0.99},
	} {
		got := histogramQuantile(buckets, tc.q)
		if math.Abs(got-tc.want) > 1e-9 {
			t.Errorf("quantile(%v) = %v, want %v", tc.q, got, tc.want)
		}
	}
}

func TestHistogramQuantileEmpty(t *testing.T) {
	if got := histogramQuantile([]histogramBucket{{math.Inf(1), 0}}, 0.99); got != 0 {
		t.Errorf("empty histogram quantile = %v, want 0", got)
	}
}

func TestBucketDeltaResetAndMismatch(t *testing.T) {
	prev := []histogramBucket{{0.1, 5}, {math.Inf(1), 10}}
	if got := bucketDelta(prev, []histogramBucket{{0.1, 1}, {math.Inf(1), 2}}); got != nil {
		t.Errorf("counter reset should yield nil, got %v", got)
	}
	if got := bucketDelta(prev, []histogramBucket{{0.2, 5}, {math.Inf(1), 10}}); got != nil {
		t.Errorf("bound mismatch should yield nil, got %v", got)
	}
	if got := bucketDelta(prev, prev); got != nil {
		t.Errorf("no new observations should yield nil, got %v", got)
	}
	got := bucketDelta(prev, []histogramBucket{{0.1, 7}, {math.Inf(1), 14}})
	if len(got) != 2 || got[0].count != 2 || got[1].count != 4 {
		t.Errorf("unexpected delta %v", got)
	}
}

func TestDeriveCounterRate(t *testing.T) {
	store := newMetricsStore()
	d := newMetricsDeriver()
	t0 := time.Now()
	m := scrapedMetric{name: "reqs", kind: kindCounter, value: 100}

	d.record(store, "volume/a", m, t0)
	if got := store.match("volume", "reqs"+suffixRate); len(got) != 0 {
		t.Fatalf("first scrape should not emit a rate, got %d series", len(got))
	}

	m.value = 250
	d.record(store, "volume/a", m, t0.Add(15*time.Second))
	series := store.match("volume", "reqs"+suffixRate)
	if len(series) != 1 {
		t.Fatalf("expected 1 rate series, got %d", len(series))
	}
	samples := series[0].snapshot()
	if len(samples) != 1 {
		t.Fatalf("expected 1 sample, got %d", len(samples))
	}
	if want := 10.0; math.Abs(samples[0].values[""]-want) > 1e-9 {
		t.Errorf("rate = %v, want %v", samples[0].values[""], want)
	}
}

func TestDeriveCounterResetSkipped(t *testing.T) {
	store := newMetricsStore()
	d := newMetricsDeriver()
	t0 := time.Now()
	m := scrapedMetric{name: "reqs", kind: kindCounter, value: 100}
	d.record(store, "volume/a", m, t0)
	m.value = 5
	d.record(store, "volume/a", m, t0.Add(15*time.Second))
	if got := store.match("volume", "reqs"+suffixRate); len(got) != 0 {
		t.Errorf("counter reset should emit no rate, got %d series", len(got))
	}
}

func TestMetricsEndpoint(t *testing.T) {
	for _, tc := range []struct {
		node string
		port uint32
		want string
	}{
		// The advertised port replaces the node's service port.
		{"127.0.0.1:8080", 9327, "127.0.0.1:9327"},
		{"127.0.0.1:8888.18888", 9327, "127.0.0.1:9327"},
		{"[::1]:8080", 9327, "[::1]:9327"},
		// A node without -metricsPort must never be scraped.
		{"127.0.0.1:8080", 0, ""},
	} {
		if got := metricsEndpoint(tc.node, tc.port); got != tc.want {
			t.Errorf("metricsEndpoint(%q, %d) = %q, want %q", tc.node, tc.port, got, tc.want)
		}
	}
}

// Series scraped at different times must not be paired by index.
func TestChartAlignsSeriesByTimestamp(t *testing.T) {
	base := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	t0, t1, t2 := base, base.Add(15*time.Second), base.Add(30*time.Second)

	dense := ChartSeries{Name: "dense", Data: []Point{{t0, 1}, {t1, 2}, {t2, 3}}}
	// Sparse series missing the middle sample; it must land on t2, not t1.
	sparse := ChartSeries{Name: "sparse", Data: []Point{{t0, 10}, {t2, 30}}}

	times := unionTimes([]ChartSeries{dense, sparse})
	if len(times) != 3 {
		t.Fatalf("unionTimes = %d, want 3", len(times))
	}
	slot := map[time.Time]int{t0: 0, t1: 1, t2: 2}
	runs := contiguousRuns(sparse.Data, slot)
	if len(runs) != 2 {
		t.Fatalf("gap should split into 2 runs, got %d", len(runs))
	}
	if runs[0][0].T != t0 || runs[1][0].T != t2 {
		t.Errorf("runs landed on wrong timestamps: %v", runs)
	}
}

func TestDiskUsagePctJoinsOnTimestamp(t *testing.T) {
	s := &AdminServer{metricsStore: newMetricsStore()}
	base := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	t0, t1 := base, base.Add(15*time.Second)

	used := map[string]string{"type": "used", "name": "/data"}
	all := map[string]string{"type": "all", "name": "/data"}

	// t0 has both used and capacity; t1 captured only used.
	s.metricsStore.recordLabeled("volume/a", mVolumeResource, used, 50, t0)
	s.metricsStore.recordLabeled("volume/a", mVolumeResource, all, 200, t0)
	s.metricsStore.recordLabeled("volume/a", mVolumeResource, used, 80, t1)

	got := s.diskUsagePct("volume")
	if len(got) != 1 {
		t.Fatalf("want 1 point (only t0 has both series), got %d: %v", len(got), got)
	}
	if got[0].T != t0 || got[0].V != 25 {
		t.Errorf("got %v, want 25%% at t0", got[0])
	}
}

func TestLatestValueUsesNewestTimestamp(t *testing.T) {
	base := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	// Deliberately out of order.
	data := []Point{{base.Add(30 * time.Second), 3}, {base, 1}}
	if got := LatestValue(data); got != 3 {
		t.Errorf("LatestValue = %v, want 3", got)
	}
	if got := LatestValue(nil); got != 0 {
		t.Errorf("LatestValue(nil) = %v, want 0", got)
	}
}

func TestStoreRingIsBounded(t *testing.T) {
	s := newMetricsSeries("volume/a", "reqs", nil)
	for i := 0; i < metricsMaxSamples+50; i++ {
		s.record(time.Now(), map[string]float64{"": float64(i)})
	}
	got := s.snapshot()
	if len(got) != metricsMaxSamples {
		t.Fatalf("len = %d, want %d", len(got), metricsMaxSamples)
	}
	if got[len(got)-1].values[""] != float64(metricsMaxSamples+49) {
		t.Errorf("newest sample not retained: %v", got[len(got)-1].values[""])
	}
}
