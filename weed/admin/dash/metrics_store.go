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
	source string
	name   string
	labels map[string]string

	mu      sync.Mutex
	samples []metricsSample
}

func newMetricsSeries(source, name string, labels map[string]string) *metricsSeries {
	return &metricsSeries{
		source:  source,
		name:    name,
		labels:  labels,
		samples: make([]metricsSample, 0, metricsMaxSamples),
	}
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

func (s *metricsStore) recordLabeled(source, name string, labels map[string]string, value float64, t time.Time) {
	key := source + "/" + name + "/" + labelKey(labels)
	s.mu.Lock()
	ser, ok := s.series[key]
	if !ok {
		ser = newMetricsSeries(source, name, labels)
		s.series[key] = ser
	}
	s.mu.Unlock()
	ser.record(t, map[string]float64{"": value})
}

// match returns every series whose source equals or is prefixed by
// sourcePrefix (at a "/" boundary) and whose metric name matches exactly.
// Passing a component such as "volume" matches every server of that type;
// passing "volume/10.0.0.1:8080" matches one server.
func (s *metricsStore) match(sourcePrefix, name string) []*metricsSeries {
	return s.matchFiltered(sourcePrefix, name, nil)
}

// matchFiltered is match restricted to series whose labels satisfy keep.
func (s *metricsStore) matchFiltered(sourcePrefix, name string, keep func(map[string]string) bool) []*metricsSeries {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []*metricsSeries
	for _, ser := range s.series {
		if ser.name != name {
			continue
		}
		if ser.source != sourcePrefix && !strings.HasPrefix(ser.source, sourcePrefix+"/") {
			continue
		}
		if keep != nil && !keep(ser.labels) {
			continue
		}
		out = append(out, ser)
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
