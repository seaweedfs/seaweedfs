package testrunner

import (
	"bufio"
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"
)

// MetricsSample holds a snapshot of Prometheus-format metrics from a target.
type MetricsSample struct {
	Target  string             `json:"target"`
	Metrics map[string]float64 `json:"metrics"`
}

// ParsePrometheusText parses the text exposition format from /metrics into
// a flat map of metric_name → value. Histograms/summaries produce multiple
// entries (e.g. "metric_bucket{le=\"0.01\"}" → value).
func ParsePrometheusText(body string) map[string]float64 {
	metrics := make(map[string]float64)
	scanner := bufio.NewScanner(strings.NewReader(body))
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		name, value, ok := parseMetricLine(line)
		if ok {
			metrics[name] = value
		}
	}
	return metrics
}

// parseMetricLine parses "metric_name{labels} value" or "metric_name value".
func parseMetricLine(line string) (string, float64, bool) {
	// Find the last space that separates the key from the value.
	// Handle possible timestamp after value.
	parts := strings.Fields(line)
	if len(parts) < 2 {
		return "", 0, false
	}
	name := parts[0]
	val, err := strconv.ParseFloat(parts[1], 64)
	if err != nil {
		return "", 0, false
	}
	return name, val, true
}

// PerfStats summarizes latency or throughput samples.
type PerfStats struct {
	Count  int     `json:"count"`
	Min    float64 `json:"min"`
	Max    float64 `json:"max"`
	Mean   float64 `json:"mean"`
	StdDev float64 `json:"stddev"`
	P50    float64 `json:"p50"`
	P90    float64 `json:"p90"`
	P99    float64 `json:"p99"`
}

// ComputeStats computes percentile and summary statistics from a slice of values.
func ComputeStats(values []float64) PerfStats {
	if len(values) == 0 {
		return PerfStats{}
	}

	sorted := make([]float64, len(values))
	copy(sorted, values)
	sort.Float64s(sorted)

	n := len(sorted)
	sum := 0.0
	for _, v := range sorted {
		sum += v
	}
	mean := sum / float64(n)

	variance := 0.0
	for _, v := range sorted {
		d := v - mean
		variance += d * d
	}
	variance /= float64(n)

	return PerfStats{
		Count:  n,
		Min:    sorted[0],
		Max:    sorted[n-1],
		Mean:   mean,
		StdDev: math.Sqrt(variance),
		P50:    percentile(sorted, 0.50),
		P90:    percentile(sorted, 0.90),
		P99:    percentile(sorted, 0.99),
	}
}

func percentile(sorted []float64, p float64) float64 {
	if len(sorted) == 0 {
		return 0
	}
	if len(sorted) == 1 {
		return sorted[0]
	}
	idx := p * float64(len(sorted)-1)
	lower := int(idx)
	upper := lower + 1
	if upper >= len(sorted) {
		return sorted[len(sorted)-1]
	}
	frac := idx - float64(lower)
	return sorted[lower]*(1-frac) + sorted[upper]*frac
}

// FormatStats returns a one-line summary of PerfStats.
func FormatStats(name string, s PerfStats) string {
	return fmt.Sprintf("%s: n=%d mean=%.2f stddev=%.2f p50=%.2f p90=%.2f p99=%.2f min=%.2f max=%.2f",
		name, s.Count, s.Mean, s.StdDev, s.P50, s.P90, s.P99, s.Min, s.Max)
}

// ParsePerfLogLines extracts numeric values from PERF[5s] log lines.
// Expected format: "PERF[5s] iops=1234 lat_us=567 ..."
// Returns map of field_name → []values across all matching lines.
func ParsePerfLogLines(logContent string) map[string][]float64 {
	result := make(map[string][]float64)
	scanner := bufio.NewScanner(strings.NewReader(logContent))
	for scanner.Scan() {
		line := scanner.Text()
		idx := strings.Index(line, "PERF[")
		if idx < 0 {
			continue
		}
		// Extract everything after "PERF[Xs] "
		rest := line[idx:]
		closeBracket := strings.Index(rest, "]")
		if closeBracket < 0 {
			continue
		}
		fields := strings.Fields(rest[closeBracket+1:])
		for _, f := range fields {
			parts := strings.SplitN(f, "=", 2)
			if len(parts) != 2 {
				continue
			}
			val, err := strconv.ParseFloat(parts[1], 64)
			if err != nil {
				continue
			}
			result[parts[0]] = append(result[parts[0]], val)
		}
	}
	return result
}
