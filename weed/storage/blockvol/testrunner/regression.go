package testrunner

import (
	"fmt"
	"math"
	"strings"
)

// RegressionResult holds the comparison of one metric against baseline.
type RegressionResult struct {
	Metric       string
	BaselineVal  float64
	CurrentVal   float64
	ThresholdPct float64 // positive = allowed increase, negative = allowed decrease
	Pass         bool
	Reason       string
}

// RegressionReport is the full regression comparison output.
type RegressionReport struct {
	Results       []RegressionResult
	HardFails     []HardFailResult
	OverallPass   bool
	BaselineGitSHA string
	CurrentGitSHA  string
}

// HardFailResult is the result of a hard fail condition check.
type HardFailResult struct {
	Condition string
	Pass      bool
	Detail    string
}

// RegressionThresholds defines the allowed deviation per metric.
var RegressionThresholds = map[string]float64{
	"p99_write_latency_seconds": 0.10,  // fail if >10% increase
	"p99_read_latency_seconds":  0.10,  // fail if >10% increase
	"write_iops":                -0.05, // fail if >5% decrease
	"read_iops":                 -0.05, // fail if >5% decrease
}

// CompareBaseline runs regression checks against a baseline.
func CompareBaseline(baseline, current map[string]float64) []RegressionResult {
	var results []RegressionResult
	for metric, threshold := range RegressionThresholds {
		bv, bOk := baseline[metric]
		cv, cOk := current[metric]
		if !bOk || !cOk {
			continue
		}
		if bv == 0 {
			results = append(results, RegressionResult{
				Metric: metric, BaselineVal: bv, CurrentVal: cv,
				ThresholdPct: threshold, Pass: true, Reason: "baseline is 0, skipped",
			})
			continue
		}

		r := RegressionResult{
			Metric: metric, BaselineVal: bv, CurrentVal: cv,
			ThresholdPct: threshold,
		}

		pctChange := (cv - bv) / math.Abs(bv)
		if threshold >= 0 {
			// Positive threshold: fail if increase exceeds threshold.
			r.Pass = pctChange <= threshold
			r.Reason = fmt.Sprintf("change=%.1f%% (limit=+%.1f%%)", pctChange*100, threshold*100)
		} else {
			// Negative threshold: fail if decrease exceeds |threshold|.
			r.Pass = pctChange >= threshold
			r.Reason = fmt.Sprintf("change=%.1f%% (limit=%.1f%%)", pctChange*100, threshold*100)
		}
		results = append(results, r)
	}
	return results
}

// HardFailChecks are immediate test failure conditions (no comparison needed).
// Each check takes the current metrics map and returns pass/fail.
var HardFailChecks = []struct {
	Name  string
	Check func(m map[string]float64) (bool, string)
}{
	{
		Name: "data_mismatch",
		Check: func(m map[string]float64) (bool, string) {
			if v, ok := m["data_mismatch_count"]; ok && v > 0 {
				return false, fmt.Sprintf("%.0f data mismatches detected", v)
			}
			return true, ""
		},
	},
	{
		Name: "promotion_panic",
		Check: func(m map[string]float64) (bool, string) {
			if v, ok := m["promotion_panic_count"]; ok && v > 0 {
				return false, fmt.Sprintf("%.0f promotion panics", v)
			}
			return true, ""
		},
	},
	{
		Name: "scrub_false_positive_burst",
		Check: func(m map[string]float64) (bool, string) {
			if v, ok := m["scrub_false_positives_per_pass"]; ok && v > 3 {
				return false, fmt.Sprintf("%.0f false positives in one scrub pass (limit=3)", v)
			}
			return true, ""
		},
	},
	{
		Name: "barrier_lag_lsn_unbounded",
		Check: func(m map[string]float64) (bool, string) {
			if v, ok := m["barrier_lag_lsn_max"]; ok && v > 1000 {
				return false, fmt.Sprintf("barrier_lag_lsn=%.0f (limit=1000)", v)
			}
			return true, ""
		},
	},
	{
		Name: "barrier_error_rate",
		Check: func(m map[string]float64) (bool, string) {
			total, tOk := m["barrier_requests_total"]
			failed, fOk := m["barrier_failures_total"]
			if tOk && fOk && total > 0 && (failed/total) > 0.05 {
				return false, fmt.Sprintf("barrier error rate=%.1f%% (limit=5%%)", (failed/total)*100)
			}
			return true, ""
		},
	},
	{
		Name: "health_zero_without_fault",
		Check: func(m map[string]float64) (bool, string) {
			health, hOk := m["health_score"]
			faultActive, fOk := m["fault_active"]
			if hOk && health == 0.0 && (!fOk || faultActive == 0) {
				return false, "health score dropped to 0.0 without injected fault"
			}
			return true, ""
		},
	},
	{
		Name: "wal_full_stall",
		Check: func(m map[string]float64) (bool, string) {
			if v, ok := m["wal_full_duration_seconds"]; ok && v > 10 {
				return false, fmt.Sprintf("WAL full for %.1fs (limit=10s)", v)
			}
			return true, ""
		},
	},
}

// RunHardFailChecks evaluates all hard fail conditions against current metrics.
func RunHardFailChecks(metrics map[string]float64) []HardFailResult {
	results := make([]HardFailResult, 0, len(HardFailChecks))
	for _, hf := range HardFailChecks {
		pass, detail := hf.Check(metrics)
		results = append(results, HardFailResult{
			Condition: hf.Name,
			Pass:      pass,
			Detail:    detail,
		})
	}
	return results
}

// FormatRegressionReport produces a human-readable report string.
func FormatRegressionReport(r *RegressionReport) string {
	var sb strings.Builder
	sb.WriteString("=== SLO Regression Report ===\n")
	sb.WriteString(fmt.Sprintf("Baseline: %s  Current: %s\n\n", r.BaselineGitSHA, r.CurrentGitSHA))

	sb.WriteString("--- Metric Comparisons ---\n")
	for _, res := range r.Results {
		status := "PASS"
		if !res.Pass {
			status = "FAIL"
		}
		sb.WriteString(fmt.Sprintf("  [%s] %s: baseline=%.4f current=%.4f %s\n",
			status, res.Metric, res.BaselineVal, res.CurrentVal, res.Reason))
	}

	sb.WriteString("\n--- Hard Fail Conditions ---\n")
	for _, hf := range r.HardFails {
		status := "PASS"
		if !hf.Pass {
			status = "FAIL"
		}
		detail := hf.Detail
		if detail == "" {
			detail = "ok"
		}
		sb.WriteString(fmt.Sprintf("  [%s] %s: %s\n", status, hf.Condition, detail))
	}

	sb.WriteString(fmt.Sprintf("\nOverall: %s\n", map[bool]string{true: "PASS", false: "FAIL"}[r.OverallPass]))
	return sb.String()
}
