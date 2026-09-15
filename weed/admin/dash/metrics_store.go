package dash

import (
	"strings"
	"sync"
	"time"
)

const metricsMaxSamples = 240

type metricsSample struct {
	t      time.Time
	values map[string]float64
}

type metricsSeries struct {
	mu      sync.Mutex
	samples []metricsSample
}

func newMetricsSeries() *metricsSeries {
	return &metricsSeries{samples: make([]metricsSample, 0, metricsMaxSamples)}
}

func (s *metricsSeries) record(t time.Time, values map[string]float64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.samples = append(s.samples, metricsSample{t: t, values: values})
	if len(s.samples) > metricsMaxSamples {
		s.samples = s.samples[len(s.samples)-metricsMaxSamples:]
	}
}

func (s *metricsSeries) snapshot() []metricsSample {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]metricsSample, len(s.samples))
	copy(out, s.samples)
	return out
}

type metricsStore struct {
	mu     sync.Mutex
	series map[string]*metricsSeries
}

func newMetricsStore() *metricsStore {
	return &metricsStore{series: make(map[string]*metricsSeries)}
}

func (s *metricsStore) record(source, name string, value float64, t time.Time) {
	s.mu.Lock()
	key := source + "/" + name
	ser, ok := s.series[key]
	if !ok {
		ser = newMetricsSeries()
		s.series[key] = ser
	}
	s.mu.Unlock()
	ser.record(t, map[string]float64{"": value})
}

func (s *metricsStore) recordLabeled(source, name string, labels map[string]string, value float64, t time.Time) {
	s.mu.Lock()
	key := source + "/" + name + "/" + labelKey(labels)
	ser, ok := s.series[key]
	if !ok {
		ser = newMetricsSeries()
		s.series[key] = ser
	}
	s.mu.Unlock()
	ser.record(t, map[string]float64{"": value})
}

func (s *metricsStore) get(source, name string) []metricsSample {
	s.mu.Lock()
	key := source + "/" + name
	ser, ok := s.series[key]
	s.mu.Unlock()
	if !ok {
		return nil
	}
	return ser.snapshot()
}

func (s *metricsStore) getLabeled(source, name string, labels map[string]string) []metricsSample {
	s.mu.Lock()
	key := source + "/" + name + "/" + labelKey(labels)
	ser, ok := s.series[key]
	s.mu.Unlock()
	if !ok {
		return nil
	}
	return ser.snapshot()
}

// match returns every series whose source has the given prefix and whose
// metric name matches exactly.
func (s *metricsStore) match(sourcePrefix, name string) []*metricsSeries {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []*metricsSeries
	for k, ser := range s.series {
		if !strings.HasPrefix(k, sourcePrefix) {
			continue
		}
		rest := k[len(sourcePrefix):]
		if !strings.HasPrefix(rest, "/") {
			continue
		}
		rest = rest[1:]
		// rest is either "<addr>/<metric>[/<labels>]" or "<metric>[/<labels>]".
		if rest == name || strings.HasPrefix(rest, name+"/") {
			out = append(out, ser)
			continue
		}
		if i := strings.Index(rest, "/"); i >= 0 {
			tail := rest[i+1:]
			if tail == name || strings.HasPrefix(tail, name+"/") {
				out = append(out, ser)
			}
		}
	}
	return out
}

func labelKey(labels map[string]string) string {
	if len(labels) == 0 {
		return ""
	}
	keys := make([]string, 0, len(labels))
	for k := range labels {
		keys = append(keys, k)
	}
	for i := 1; i < len(keys); i++ {
		for j := i; j > 0 && keys[j] < keys[j-1]; j-- {
			keys[j], keys[j-1] = keys[j-1], keys[j]
		}
	}
	out := ""
	for i, k := range keys {
		if i > 0 {
			out += ","
		}
		out += k + "=" + labels[k]
	}
	return out
}
