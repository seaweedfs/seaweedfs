package testrunner

import (
	"math"
	"testing"
)

func TestParsePrometheusText(t *testing.T) {
	body := `# HELP blockvol_write_ops_total Total write operations.
# TYPE blockvol_write_ops_total counter
blockvol_write_ops_total 1234
blockvol_read_ops_total 5678
blockvol_write_latency_seconds_bucket{le="0.001"} 100
blockvol_write_latency_seconds_bucket{le="0.01"} 900
blockvol_write_latency_seconds_bucket{le="+Inf"} 1234
blockvol_write_latency_seconds_sum 12.34
blockvol_write_latency_seconds_count 1234
`
	m := ParsePrometheusText(body)

	if m["blockvol_write_ops_total"] != 1234 {
		t.Errorf("write_ops_total = %v, want 1234", m["blockvol_write_ops_total"])
	}
	if m["blockvol_read_ops_total"] != 5678 {
		t.Errorf("read_ops_total = %v, want 5678", m["blockvol_read_ops_total"])
	}
	if m[`blockvol_write_latency_seconds_bucket{le="0.001"}`] != 100 {
		t.Errorf("bucket le=0.001 = %v, want 100", m[`blockvol_write_latency_seconds_bucket{le="0.001"}`])
	}
}

func TestParsePrometheusText_Empty(t *testing.T) {
	m := ParsePrometheusText("")
	if len(m) != 0 {
		t.Errorf("expected empty map, got %d entries", len(m))
	}
}

func TestComputeStats(t *testing.T) {
	values := []float64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}
	s := ComputeStats(values)

	if s.Count != 10 {
		t.Errorf("count = %d, want 10", s.Count)
	}
	if s.Min != 1 {
		t.Errorf("min = %v, want 1", s.Min)
	}
	if s.Max != 10 {
		t.Errorf("max = %v, want 10", s.Max)
	}
	if math.Abs(s.Mean-5.5) > 0.01 {
		t.Errorf("mean = %v, want 5.5", s.Mean)
	}
	if math.Abs(s.P50-5.5) > 0.01 {
		t.Errorf("p50 = %v, want ~5.5", s.P50)
	}
	if s.P99 < 9.5 {
		t.Errorf("p99 = %v, want >= 9.5", s.P99)
	}
}

func TestComputeStats_Empty(t *testing.T) {
	s := ComputeStats(nil)
	if s.Count != 0 {
		t.Errorf("expected count=0 for empty input, got %d", s.Count)
	}
}

func TestComputeStats_Single(t *testing.T) {
	s := ComputeStats([]float64{42.0})
	if s.Count != 1 || s.Min != 42 || s.Max != 42 || s.P50 != 42 || s.P99 != 42 {
		t.Errorf("single value stats wrong: %+v", s)
	}
}

func TestParsePerfLogLines(t *testing.T) {
	log := `2024-01-01 12:00:00 INFO starting target
2024-01-01 12:00:05 PERF[5s] iops=1234 lat_us=567 bw_mb=48.5
2024-01-01 12:00:10 PERF[5s] iops=1300 lat_us=520 bw_mb=50.2
2024-01-01 12:00:15 PERF[5s] iops=1250 lat_us=540 bw_mb=49.0
2024-01-01 12:00:20 INFO done
`
	result := ParsePerfLogLines(log)

	if len(result["iops"]) != 3 {
		t.Fatalf("expected 3 iops samples, got %d", len(result["iops"]))
	}
	if result["iops"][0] != 1234 {
		t.Errorf("first iops = %v, want 1234", result["iops"][0])
	}
	if len(result["lat_us"]) != 3 {
		t.Fatalf("expected 3 lat_us samples, got %d", len(result["lat_us"]))
	}
	if len(result["bw_mb"]) != 3 {
		t.Fatalf("expected 3 bw_mb samples, got %d", len(result["bw_mb"]))
	}
}

func TestParsePerfLogLines_Empty(t *testing.T) {
	result := ParsePerfLogLines("no perf lines here\njust regular log output\n")
	if len(result) != 0 {
		t.Errorf("expected empty result, got %d entries", len(result))
	}
}

func TestFormatStats(t *testing.T) {
	s := PerfStats{Count: 100, Mean: 5.5, StdDev: 2.87, P50: 5.0, P90: 9.0, P99: 10.0, Min: 1, Max: 10}
	str := FormatStats("iops", s)
	if str == "" {
		t.Error("FormatStats returned empty string")
	}
	// Just verify it contains the name and key stats.
	if !contains(str, "iops") || !contains(str, "n=100") || !contains(str, "p99=10.00") {
		t.Errorf("unexpected format: %s", str)
	}
}

func contains(s, sub string) bool {
	return len(s) >= len(sub) && findSubstring(s, sub)
}

func findSubstring(s, sub string) bool {
	for i := 0; i <= len(s)-len(sub); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}
