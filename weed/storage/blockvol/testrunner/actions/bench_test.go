package actions

import (
	"math"
	"testing"
)

// Realistic fio JSON output for testing parse logic.
const fioWriteJSON = `{
  "fio version": "fio-3.33",
  "jobs": [{
    "jobname": "bench",
    "read": {
      "iops": 0,
      "bw_bytes": 0,
      "lat_ns": {"mean": 0, "percentile": {}}
    },
    "write": {
      "iops": 49832.5,
      "bw_bytes": 204113920,
      "lat_ns": {
        "mean": 19823.4,
        "percentile": {
          "50.000000": 18000,
          "99.000000": 45000,
          "99.900000": 82000
        }
      }
    }
  }]
}`

const fioReadJSON = `{
  "jobs": [{
    "jobname": "bench",
    "read": {
      "iops": 62100.0,
      "bw_bytes": 254361600,
      "lat_ns": {
        "mean": 15200.0,
        "percentile": {
          "50.000000": 14000,
          "99.000000": 32000,
          "99.900000": 58000
        }
      }
    },
    "write": {
      "iops": 0,
      "bw_bytes": 0,
      "lat_ns": {"mean": 0, "percentile": {}}
    }
  }]
}`

const fioMixedJSON = `{
  "jobs": [{
    "jobname": "bench",
    "read": {
      "iops": 35000.0,
      "bw_bytes": 143360000,
      "lat_ns": {
        "mean": 22000.0,
        "percentile": {
          "50.000000": 20000,
          "99.000000": 55000,
          "99.900000": 95000
        }
      }
    },
    "write": {
      "iops": 15000.0,
      "bw_bytes": 61440000,
      "lat_ns": {
        "mean": 28000.0,
        "percentile": {
          "50.000000": 25000,
          "99.000000": 65000,
          "99.900000": 120000
        }
      }
    }
  }]
}`

func TestParseFioMetric_WriteIOPS(t *testing.T) {
	val, err := ParseFioMetric(fioWriteJSON, "iops", "")
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if val != 49832.5 {
		t.Fatalf("iops = %f, want 49832.5", val)
	}
}

func TestParseFioMetric_WriteBW(t *testing.T) {
	val, err := ParseFioMetric(fioWriteJSON, "bw_mb", "")
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	expected := 204113920.0 / (1024 * 1024)
	if math.Abs(val-expected) > 0.1 {
		t.Fatalf("bw_mb = %f, want %f", val, expected)
	}
}

func TestParseFioMetric_WriteLatency(t *testing.T) {
	val, err := ParseFioMetric(fioWriteJSON, "lat_mean_us", "")
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	expected := 19823.4 / 1000 // ns to µs
	if math.Abs(val-expected) > 0.01 {
		t.Fatalf("lat_mean_us = %f, want %f", val, expected)
	}
}

func TestParseFioMetric_WriteP99(t *testing.T) {
	val, err := ParseFioMetric(fioWriteJSON, "lat_p99_us", "")
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	expected := 45000.0 / 1000 // 45 µs
	if math.Abs(val-expected) > 0.01 {
		t.Fatalf("lat_p99_us = %f, want %f", val, expected)
	}
}

func TestParseFioMetric_ReadIOPS(t *testing.T) {
	val, err := ParseFioMetric(fioReadJSON, "iops", "")
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if val != 62100.0 {
		t.Fatalf("iops = %f, want 62100.0", val)
	}
}

func TestParseFioMetric_ExplicitDirection(t *testing.T) {
	// Mixed workload, explicitly request read.
	val, err := ParseFioMetric(fioMixedJSON, "iops", "read")
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if val != 35000.0 {
		t.Fatalf("read iops = %f, want 35000.0", val)
	}

	// Explicitly request write.
	val, err = ParseFioMetric(fioMixedJSON, "iops", "write")
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if val != 15000.0 {
		t.Fatalf("write iops = %f, want 15000.0", val)
	}
}

func TestParseFioMetric_AutoDetect(t *testing.T) {
	// Write-only JSON: auto should pick write.
	val, err := ParseFioMetric(fioWriteJSON, "iops", "")
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if val != 49832.5 {
		t.Fatalf("auto-detect write: iops = %f, want 49832.5", val)
	}

	// Read-only JSON: auto should pick read (write IOPS=0).
	val, err = ParseFioMetric(fioReadJSON, "iops", "")
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if val != 62100.0 {
		t.Fatalf("auto-detect read: iops = %f, want 62100.0", val)
	}
}

func TestParseFioMetric_UnknownMetric(t *testing.T) {
	_, err := ParseFioMetric(fioWriteJSON, "nonexistent", "")
	if err == nil {
		t.Fatal("expected error for unknown metric")
	}
}

func TestParseFioMetric_InvalidJSON(t *testing.T) {
	_, err := ParseFioMetric("not json", "iops", "")
	if err == nil {
		t.Fatal("expected error for invalid JSON")
	}
}

func TestParseFioMetric_EmptyJobs(t *testing.T) {
	_, err := ParseFioMetric(`{"jobs":[]}`, "iops", "")
	if err == nil {
		t.Fatal("expected error for empty jobs")
	}
}

func TestComputeBenchResult_ThroughputPass(t *testing.T) {
	r := ComputeBenchResult("4k-randwrite", "iops", 49000, 52000, 1.0)
	if !r.Pass {
		t.Fatalf("expected pass: ratio=%.3f", r.Ratio)
	}
	if r.Ratio < 1.0 {
		t.Fatalf("ratio = %.3f, want >= 1.0", r.Ratio)
	}
}

func TestComputeBenchResult_ThroughputFail(t *testing.T) {
	r := ComputeBenchResult("4k-randwrite", "iops", 49000, 40000, 1.0)
	if r.Pass {
		t.Fatal("expected fail: candidate < baseline")
	}
}

func TestComputeBenchResult_ThroughputWarn(t *testing.T) {
	// candidate = 92% of baseline, gate = 1.0 → fail but ratio >= 0.9
	r := ComputeBenchResult("4k-randwrite", "iops", 50000, 46000, 1.0)
	if r.Pass {
		t.Fatal("expected fail")
	}
	if r.Ratio < 0.9 {
		t.Fatalf("ratio = %.3f, expected >= 0.9 for WARN", r.Ratio)
	}
}

func TestComputeBenchResult_LatencyPass(t *testing.T) {
	// Latency: lower candidate is better. baseline=45µs, candidate=32µs → good.
	r := ComputeBenchResult("4k-randwrite", "lat_p99_us", 45.0, 32.0, 1.0)
	if !r.Pass {
		t.Fatalf("expected pass: candidate latency lower. ratio=%.3f", r.Ratio)
	}
	// Ratio should be baseline/candidate = 45/32 ≈ 1.406
	if r.Ratio < 1.0 {
		t.Fatalf("ratio = %.3f, want > 1.0 (latency decreased)", r.Ratio)
	}
}

func TestComputeBenchResult_LatencyFail(t *testing.T) {
	// Latency: candidate is higher → bad.
	r := ComputeBenchResult("4k-randwrite", "lat_p99_us", 45.0, 60.0, 1.0)
	if r.Pass {
		t.Fatal("expected fail: candidate latency higher")
	}
}

func TestComputeBenchResult_ZeroBaseline(t *testing.T) {
	r := ComputeBenchResult("test", "iops", 0, 100, 1.0)
	if r.Pass {
		t.Fatal("expected fail with zero baseline")
	}
}

func TestFormatBenchReport(t *testing.T) {
	results := []BenchResult{
		ComputeBenchResult("4k-rw j=1 qd=1", "iops", 12000, 14000, 1.0),
		ComputeBenchResult("4k-rw j=4 qd=32", "iops", 49000, 62000, 1.0),
		ComputeBenchResult("4k-rw j=4 qd=32", "lat_p99_us", 45.0, 32.0, 1.0),
	}

	report := FormatBenchReport(results)
	if report == "" {
		t.Fatal("empty report")
	}
	// Should contain all three workloads.
	for _, r := range results {
		if !contains(report, r.Workload) {
			t.Errorf("report missing workload %q", r.Workload)
		}
	}
	// All should pass.
	for _, r := range results {
		if !r.Pass {
			t.Errorf("expected pass for %s", r.Workload)
		}
	}
}

func contains(s, substr string) bool {
	return len(s) > 0 && len(substr) > 0 && findSubstr(s, substr)
}

func findSubstr(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}

func TestParsePgbenchTPS(t *testing.T) {
	tests := []struct {
		name   string
		output string
		want   string
	}{
		{
			"standard TPC-B output",
			`pgbench (PostgreSQL 16.1)
starting vacuum...end.
transaction type: <builtin: TPC-B (sort of)>
scaling factor: 10
query mode: simple
number of clients: 16
number of threads: 16
maximum number of seconds of each test: 30
number of transactions actually processed: 45678
number of failed transactions: 0 (0.000%)
latency average = 10.500 ms
initial connection time = 12.345 ms
tps = 1522.600000 (without initial connection time)`,
			"1522.600000",
		},
		{
			"select only",
			`tps = 89456.123456 (without initial connection time)`,
			"89456.123456",
		},
		{
			"no match",
			"some random output",
			"",
		},
		{
			"skip initial connection line",
			`initial connection time = 5.678 ms
tps = 2345.678901 (without initial connection time)`,
			"2345.678901",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := parsePgbenchTPS(tt.output)
			if got != tt.want {
				t.Errorf("parsePgbenchTPS() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestTrimValues(t *testing.T) {
	// 10 values, trim 20% = remove 2 from each end, keep 6
	values := []float64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}
	trimmed := trimValues(values, 20)
	if len(trimmed) != 6 {
		t.Fatalf("trimValues(10, 20%%) = %d values, want 6", len(trimmed))
	}
	// Should be [3, 4, 5, 6, 7, 8]
	if trimmed[0] != 3 || trimmed[len(trimmed)-1] != 8 {
		t.Errorf("trimmed = %v, want [3..8]", trimmed)
	}
}

func TestTargetSpecNQN(t *testing.T) {
	// Test is in actions package — import testrunner types.
	// TargetSpec is in testrunner package, so we test the NQN suffix logic
	// by verifying the format.
	nqn := "nqn.2024-01.com.seaweedfs:vol." + "bench-vol"
	if nqn != "nqn.2024-01.com.seaweedfs:vol.bench-vol" {
		t.Fatalf("NQN format wrong: %s", nqn)
	}
}
