package testrunner

import (
	"testing"
)

func TestCompareBaseline_P99Increase(t *testing.T) {
	baseline := map[string]float64{"p99_write_latency_seconds": 0.005}
	current := map[string]float64{"p99_write_latency_seconds": 0.006} // +20%

	results := CompareBaseline(baseline, current)
	if len(results) == 0 {
		t.Fatal("expected results")
	}
	for _, r := range results {
		if r.Metric == "p99_write_latency_seconds" && r.Pass {
			t.Error("expected FAIL for 20% latency increase (limit 10%)")
		}
	}
}

func TestCompareBaseline_P99WithinLimit(t *testing.T) {
	baseline := map[string]float64{"p99_write_latency_seconds": 0.005}
	current := map[string]float64{"p99_write_latency_seconds": 0.0054} // +8%

	results := CompareBaseline(baseline, current)
	for _, r := range results {
		if r.Metric == "p99_write_latency_seconds" && !r.Pass {
			t.Errorf("expected PASS for 8%% increase (limit 10%%): %s", r.Reason)
		}
	}
}

func TestCompareBaseline_IOPSDecrease(t *testing.T) {
	baseline := map[string]float64{"write_iops": 50000}
	current := map[string]float64{"write_iops": 46000} // -8%

	results := CompareBaseline(baseline, current)
	for _, r := range results {
		if r.Metric == "write_iops" && r.Pass {
			t.Error("expected FAIL for 8% IOPS decrease (limit 5%)")
		}
	}
}

func TestCompareBaseline_IOPSWithinLimit(t *testing.T) {
	baseline := map[string]float64{"write_iops": 50000}
	current := map[string]float64{"write_iops": 48000} // -4%

	results := CompareBaseline(baseline, current)
	for _, r := range results {
		if r.Metric == "write_iops" && !r.Pass {
			t.Errorf("expected PASS for 4%% decrease (limit 5%%): %s", r.Reason)
		}
	}
}

func TestHardFail_DataMismatch(t *testing.T) {
	metrics := map[string]float64{"data_mismatch_count": 1}
	results := RunHardFailChecks(metrics)
	for _, r := range results {
		if r.Condition == "data_mismatch" && r.Pass {
			t.Error("expected FAIL for data mismatch")
		}
	}
}

func TestHardFail_BarrierLagUnbounded(t *testing.T) {
	metrics := map[string]float64{"barrier_lag_lsn_max": 1500}
	results := RunHardFailChecks(metrics)
	for _, r := range results {
		if r.Condition == "barrier_lag_lsn_unbounded" && r.Pass {
			t.Error("expected FAIL for barrier_lag_lsn=1500")
		}
	}
}

func TestHardFail_BarrierLagOK(t *testing.T) {
	metrics := map[string]float64{"barrier_lag_lsn_max": 50}
	results := RunHardFailChecks(metrics)
	for _, r := range results {
		if r.Condition == "barrier_lag_lsn_unbounded" && !r.Pass {
			t.Error("expected PASS for barrier_lag_lsn=50")
		}
	}
}

func TestHardFail_BarrierErrorRate(t *testing.T) {
	metrics := map[string]float64{
		"barrier_requests_total": 100,
		"barrier_failures_total": 10, // 10% error rate
	}
	results := RunHardFailChecks(metrics)
	for _, r := range results {
		if r.Condition == "barrier_error_rate" && r.Pass {
			t.Error("expected FAIL for 10% barrier error rate")
		}
	}
}

func TestHardFail_HealthZero(t *testing.T) {
	metrics := map[string]float64{"health_score": 0.0, "fault_active": 0}
	results := RunHardFailChecks(metrics)
	for _, r := range results {
		if r.Condition == "health_zero_without_fault" && r.Pass {
			t.Error("expected FAIL for health=0 without fault")
		}
	}
}

func TestHardFail_HealthZeroDuringFault_OK(t *testing.T) {
	metrics := map[string]float64{"health_score": 0.0, "fault_active": 1}
	results := RunHardFailChecks(metrics)
	for _, r := range results {
		if r.Condition == "health_zero_without_fault" && !r.Pass {
			t.Error("expected PASS for health=0 during active fault")
		}
	}
}

func TestHardFail_WALFullStall(t *testing.T) {
	metrics := map[string]float64{"wal_full_duration_seconds": 15}
	results := RunHardFailChecks(metrics)
	for _, r := range results {
		if r.Condition == "wal_full_stall" && r.Pass {
			t.Error("expected FAIL for WAL full 15s")
		}
	}
}

func TestHardFail_AllPass(t *testing.T) {
	metrics := map[string]float64{
		"health_score":      1.0,
		"barrier_lag_lsn_max": 10,
	}
	results := RunHardFailChecks(metrics)
	for _, r := range results {
		if !r.Pass {
			t.Errorf("expected all PASS, got FAIL for %s: %s", r.Condition, r.Detail)
		}
	}
}

func TestFormatRegressionReport(t *testing.T) {
	report := &RegressionReport{
		Results: []RegressionResult{
			{Metric: "p99_write_latency_seconds", BaselineVal: 0.005, CurrentVal: 0.006, Pass: false, Reason: "change=+20%"},
		},
		HardFails: []HardFailResult{
			{Condition: "data_mismatch", Pass: true},
		},
		OverallPass:    false,
		BaselineGitSHA: "abc123",
		CurrentGitSHA:  "def456",
	}
	s := FormatRegressionReport(report)
	if s == "" {
		t.Error("expected non-empty report")
	}
	if len(s) < 50 {
		t.Errorf("report too short: %s", s)
	}
}
