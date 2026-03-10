package actions

import (
	"context"
	"encoding/json"
	"math"
	"strings"
	"testing"
	"time"

	tr "github.com/seaweedfs/seaweedfs/weed/storage/blockvol/testrunner"
)

// ============================================================
// NVMe Action Registration
// ============================================================

func TestNVMeActions_Registration(t *testing.T) {
	registry := tr.NewRegistry()
	RegisterNVMeActions(registry)

	expected := []string{
		"nvme_connect",
		"nvme_disconnect",
		"nvme_get_device",
		"nvme_cleanup",
	}

	for _, name := range expected {
		if _, err := registry.Get(name); err != nil {
			t.Errorf("action %q not registered: %v", name, err)
		}
	}

	byTier := registry.ListByTier()
	if n := len(byTier[tr.TierBlock]); n != 4 {
		t.Errorf("block tier has %d nvme actions, want 4", n)
	}
}

func TestNVMeActions_TierGating(t *testing.T) {
	registry := tr.NewRegistry()
	RegisterNVMeActions(registry)

	// Without gating, all accessible.
	if _, err := registry.Get("nvme_connect"); err != nil {
		t.Errorf("ungated: %v", err)
	}

	// Enable only core tier — block actions should be blocked.
	registry.EnableTiers([]string{tr.TierCore})
	if _, err := registry.Get("nvme_connect"); err == nil {
		t.Error("expected error when block tier is disabled")
	}

	// Enable block tier — should work again.
	registry.EnableTiers([]string{tr.TierBlock})
	if _, err := registry.Get("nvme_connect"); err != nil {
		t.Errorf("block enabled: %v", err)
	}
}

func TestBenchActions_Registration(t *testing.T) {
	registry := tr.NewRegistry()
	RegisterBenchActions(registry)

	expected := []string{"fio_json", "fio_parse", "bench_compare"}
	for _, name := range expected {
		if _, err := registry.Get(name); err != nil {
			t.Errorf("action %q not registered: %v", name, err)
		}
	}
}

// ============================================================
// findNVMeDevice JSON Parsing (nvme list-subsys output)
// ============================================================

// parseAndFind is a test helper that parses nvme list-subsys JSON and
// finds the device for a given NQN, replicating findNVMeDevice logic
// without SSH.
func parseAndFind(t *testing.T, jsonStr, nqn string) string {
	t.Helper()
	var parsed nvmeSubsysOutput
	if err := json.Unmarshal([]byte(jsonStr), &parsed); err != nil {
		t.Fatalf("parse: %v", err)
	}
	for _, ss := range parsed.Subsystems {
		if ss.NQN != nqn {
			continue
		}
		for _, p := range ss.Paths {
			if p.Name == "" {
				continue
			}
			if strings.EqualFold(p.Transport, "tcp") && strings.EqualFold(p.State, "live") {
				return "/dev/" + p.Name + "n1"
			}
		}
		for _, p := range ss.Paths {
			if p.Name != "" {
				return "/dev/" + p.Name + "n1"
			}
		}
	}
	return ""
}

func TestFindNVMeDevice_Parse_LiveTCP(t *testing.T) {
	dev := parseAndFind(t, `{
		"Subsystems": [{
			"NQN": "nqn.2024-01.com.seaweedfs:vol.test-vol",
			"Paths": [{"Name": "nvme0", "Transport": "tcp", "State": "live"}]
		}]
	}`, "nqn.2024-01.com.seaweedfs:vol.test-vol")
	if dev != "/dev/nvme0n1" {
		t.Fatalf("device = %q, want /dev/nvme0n1", dev)
	}
}

func TestFindNVMeDevice_Parse_NoMatch(t *testing.T) {
	dev := parseAndFind(t, `{
		"Subsystems": [{
			"NQN": "nqn.2024-01.com.seaweedfs:vol.other",
			"Paths": [{"Name": "nvme0", "Transport": "tcp", "State": "live"}]
		}]
	}`, "nqn.2024-01.com.seaweedfs:vol.test-vol")
	if dev != "" {
		t.Fatalf("expected empty, got %q", dev)
	}
}

func TestFindNVMeDevice_Parse_MultipleSubsystems(t *testing.T) {
	jsonStr := `{
		"Subsystems": [
			{"NQN": "nqn.test:vol-a", "Paths": [{"Name": "nvme0", "Transport": "tcp", "State": "live"}]},
			{"NQN": "nqn.test:vol-b", "Paths": [{"Name": "nvme1", "Transport": "tcp", "State": "live"}]}
		]
	}`
	if d := parseAndFind(t, jsonStr, "nqn.test:vol-a"); d != "/dev/nvme0n1" {
		t.Fatalf("vol-a: %q", d)
	}
	if d := parseAndFind(t, jsonStr, "nqn.test:vol-b"); d != "/dev/nvme1n1" {
		t.Fatalf("vol-b: %q", d)
	}
}

func TestFindNVMeDevice_Parse_PreferLiveTCP(t *testing.T) {
	dev := parseAndFind(t, `{
		"Subsystems": [{
			"NQN": "nqn.test:vol",
			"Paths": [
				{"Name": "nvme0", "Transport": "rdma", "State": "live"},
				{"Name": "nvme1", "Transport": "tcp", "State": "connecting"},
				{"Name": "nvme2", "Transport": "tcp", "State": "live"}
			]
		}]
	}`, "nqn.test:vol")
	if dev != "/dev/nvme2n1" {
		t.Fatalf("device = %q, want /dev/nvme2n1 (live TCP preferred)", dev)
	}
}

func TestFindNVMeDevice_Parse_FallbackNonLive(t *testing.T) {
	dev := parseAndFind(t, `{
		"Subsystems": [{
			"NQN": "nqn.test:vol",
			"Paths": [{"Name": "nvme3", "Transport": "tcp", "State": "connecting"}]
		}]
	}`, "nqn.test:vol")
	if dev != "/dev/nvme3n1" {
		t.Fatalf("device = %q, want /dev/nvme3n1 (fallback)", dev)
	}
}

func TestFindNVMeDevice_Parse_EmptyPaths(t *testing.T) {
	dev := parseAndFind(t, `{
		"Subsystems": [{"NQN": "nqn.test:vol", "Paths": []}]
	}`, "nqn.test:vol")
	if dev != "" {
		t.Fatalf("expected empty for no paths, got %q", dev)
	}
}

func TestFindNVMeDevice_Parse_EmptyName(t *testing.T) {
	dev := parseAndFind(t, `{
		"Subsystems": [{
			"NQN": "nqn.test:vol",
			"Paths": [{"Name": "", "Transport": "tcp", "State": "live"}]
		}]
	}`, "nqn.test:vol")
	if dev != "" {
		t.Fatalf("expected empty for nameless path, got %q", dev)
	}
}

func TestFindNVMeDevice_Parse_EmptySubsystems(t *testing.T) {
	dev := parseAndFind(t, `{"Subsystems": []}`, "nqn.test:vol")
	if dev != "" {
		t.Fatalf("expected empty, got %q", dev)
	}
}

func TestFindNVMeDevice_Parse_CaseInsensitive(t *testing.T) {
	dev := parseAndFind(t, `{
		"Subsystems": [{
			"NQN": "nqn.test:vol",
			"Paths": [{"Name": "nvme5", "Transport": "TCP", "State": "Live"}]
		}]
	}`, "nqn.test:vol")
	if dev != "/dev/nvme5n1" {
		t.Fatalf("device = %q, want /dev/nvme5n1 (case insensitive)", dev)
	}
}

// ============================================================
// TargetSpec NVMe fields
// ============================================================

func TestTargetSpec_NQN_WithNQNSuffix(t *testing.T) {
	spec := tr.TargetSpec{NQNSuffix: "my-vol", IQNSuffix: "fallback"}
	want := "nqn.2024-01.com.seaweedfs:vol.my-vol"
	if got := spec.NQN(); got != want {
		t.Fatalf("NQN() = %q, want %q", got, want)
	}
}

func TestTargetSpec_NQN_FallbackToIQN(t *testing.T) {
	spec := tr.TargetSpec{IQNSuffix: "iqn-vol"}
	want := "nqn.2024-01.com.seaweedfs:vol.iqn-vol"
	if got := spec.NQN(); got != want {
		t.Fatalf("NQN() = %q, want %q (fallback to IQN suffix)", got, want)
	}
}

func TestTargetSpec_NQN_BothEmpty(t *testing.T) {
	spec := tr.TargetSpec{}
	got := spec.NQN()
	// Should return prefix + empty string.
	if got != "nqn.2024-01.com.seaweedfs:vol." {
		t.Fatalf("NQN() = %q", got)
	}
}

// ============================================================
// ParseFioMetric — additional edge cases
// ============================================================

func TestParseFioMetric_MixedAutoDetectPicksWrite(t *testing.T) {
	// When both have IOPS > 0, auto-detect picks write (checked first).
	val, err := ParseFioMetric(fioMixedJSON, "iops", "")
	if err != nil {
		t.Fatal(err)
	}
	if val != 15000.0 {
		t.Fatalf("auto-detect mixed iops = %f, want 15000 (write)", val)
	}
}

func TestParseFioMetric_AllLatencyMetrics(t *testing.T) {
	metrics := []struct {
		name string
		want float64
	}{
		{"lat_mean_us", 19823.4 / 1000},
		{"lat_p50_us", 18000.0 / 1000},
		{"lat_p99_us", 45000.0 / 1000},
		{"lat_p999_us", 82000.0 / 1000},
	}
	for _, m := range metrics {
		val, err := ParseFioMetric(fioWriteJSON, m.name, "")
		if err != nil {
			t.Fatalf("%s: %v", m.name, err)
		}
		if math.Abs(val-m.want) > 0.01 {
			t.Fatalf("%s = %f, want %f", m.name, val, m.want)
		}
	}
}

func TestParseFioMetric_BWBytes(t *testing.T) {
	val, err := ParseFioMetric(fioWriteJSON, "bw_bytes", "")
	if err != nil {
		t.Fatal(err)
	}
	if val != 204113920.0 {
		t.Fatalf("bw_bytes = %f, want 204113920", val)
	}
}

func TestParseFioMetric_MissingPercentile(t *testing.T) {
	jsonStr := `{
		"jobs": [{"jobname": "bench",
			"read": {"iops": 0, "bw_bytes": 0, "lat_ns": {"mean": 0, "percentile": {}}},
			"write": {"iops": 100, "bw_bytes": 409600, "lat_ns": {"mean": 5000, "percentile": {}}}
		}]
	}`
	val, err := ParseFioMetric(jsonStr, "lat_p99_us", "")
	if err != nil {
		t.Fatal(err)
	}
	if val != 0 {
		t.Fatalf("lat_p99_us = %f, want 0 (missing key)", val)
	}
}

func TestParseFioMetric_NilPercentile(t *testing.T) {
	jsonStr := `{
		"jobs": [{"jobname": "bench",
			"read": {"iops": 0, "bw_bytes": 0, "lat_ns": {"mean": 0}},
			"write": {"iops": 100, "bw_bytes": 409600, "lat_ns": {"mean": 5000}}
		}]
	}`
	val, err := ParseFioMetric(jsonStr, "lat_p99_us", "")
	if err != nil {
		t.Fatal(err)
	}
	if val != 0 {
		t.Fatalf("lat_p99_us = %f, want 0 (nil percentile)", val)
	}
}

// ============================================================
// ComputeBenchResult — additional edge cases
// ============================================================

func TestComputeBenchResult_LatencyWarn(t *testing.T) {
	// Candidate latency slightly higher: ratio=40/42=0.952, > 0.9 but < 1.0.
	r := ComputeBenchResult("lat-test", "lat_p99_us", 40.0, 42.0, 1.0)
	if r.Pass {
		t.Fatal("expected fail: candidate latency higher")
	}
	if r.Ratio < 0.9 {
		t.Fatalf("ratio = %.3f, expected >= 0.9 (WARN territory)", r.Ratio)
	}
}

func TestComputeBenchResult_LatencyMuchWorse(t *testing.T) {
	r := ComputeBenchResult("lat-test", "lat_p99_us", 40.0, 120.0, 1.0)
	if r.Pass {
		t.Fatal("expected fail")
	}
	if r.Ratio >= 0.9 {
		t.Fatalf("ratio = %.3f, expected < 0.9", r.Ratio)
	}
}

func TestComputeBenchResult_ExactGate(t *testing.T) {
	r := ComputeBenchResult("exact", "iops", 100, 90, 0.9)
	if !r.Pass {
		t.Fatalf("expected pass: ratio=%.3f == gate=0.9", r.Ratio)
	}
}

func TestComputeBenchResult_JustBelowGate(t *testing.T) {
	r := ComputeBenchResult("below", "iops", 100, 89, 0.9)
	if r.Pass {
		t.Fatal("expected fail: ratio < gate")
	}
}

func TestComputeBenchResult_ZeroCandidate(t *testing.T) {
	r := ComputeBenchResult("zero-cand", "iops", 100, 0, 1.0)
	if r.Pass {
		t.Fatal("expected fail: zero candidate")
	}
	if r.Ratio != 0 {
		t.Fatalf("ratio = %f, want 0", r.Ratio)
	}
}

func TestComputeBenchResult_BothZero(t *testing.T) {
	r := ComputeBenchResult("both-zero", "iops", 0, 0, 1.0)
	if r.Pass {
		t.Fatal("expected fail: both zero")
	}
}

func TestComputeBenchResult_LatencyZeroCandidate(t *testing.T) {
	r := ComputeBenchResult("lat-zero", "lat_p99_us", 40.0, 0.0, 1.0)
	if !r.Pass {
		t.Fatal("expected pass: candidate latency=0 is infinitely good")
	}
	if !math.IsInf(r.Ratio, 1) {
		t.Fatalf("ratio = %f, want +Inf", r.Ratio)
	}
}

func TestComputeBenchResult_DeltaSign_ThroughputUp(t *testing.T) {
	r := ComputeBenchResult("up", "iops", 1000, 1200, 1.0)
	if r.Delta != "+20.0%" {
		t.Fatalf("delta = %q, want +20.0%%", r.Delta)
	}
}

func TestComputeBenchResult_DeltaSign_ThroughputDown(t *testing.T) {
	r := ComputeBenchResult("down", "iops", 1000, 800, 1.0)
	if r.Delta != "-20.0%" {
		t.Fatalf("delta = %q, want -20.0%%", r.Delta)
	}
}

func TestComputeBenchResult_DeltaSign_LatencyDown(t *testing.T) {
	r := ComputeBenchResult("lat-down", "lat_p99_us", 100, 80, 1.0)
	if r.Delta != "-20.0%" {
		t.Fatalf("delta = %q, want -20.0%%", r.Delta)
	}
}

func TestComputeBenchResult_DeltaSign_LatencyUp(t *testing.T) {
	r := ComputeBenchResult("lat-up", "lat_p99_us", 100, 120, 1.0)
	if r.Delta != "+20.0%" {
		t.Fatalf("delta = %q, want +20.0%%", r.Delta)
	}
}

// ============================================================
// FormatBenchReport edge cases
// ============================================================

func TestFormatBenchReport_EmptyResults(t *testing.T) {
	report := FormatBenchReport(nil)
	if report == "" {
		t.Fatal("expected non-empty report even with no results")
	}
}

func TestFormatBenchReport_MixedPassFail(t *testing.T) {
	results := []BenchResult{
		ComputeBenchResult("good", "iops", 100, 120, 1.0),
		ComputeBenchResult("bad", "iops", 100, 50, 1.0),
		ComputeBenchResult("warn", "iops", 100, 92, 1.0),
	}
	report := FormatBenchReport(results)

	if !contains(report, "PASS") {
		t.Error("report missing PASS")
	}
	if !contains(report, "FAIL") {
		t.Error("report missing FAIL")
	}
	if !contains(report, "WARN") {
		t.Error("report missing WARN")
	}
}

// ============================================================
// benchCompare action param validation
// ============================================================

func TestBenchCompare_MissingParams(t *testing.T) {
	registry := tr.NewRegistry()
	RegisterBenchActions(registry)

	handler, err := registry.Get("bench_compare")
	if err != nil {
		t.Fatal(err)
	}

	actx := &tr.ActionContext{
		Vars: map[string]string{},
		Log:  func(string, ...interface{}) {},
	}

	tests := []struct {
		name   string
		params map[string]string
	}{
		{"missing_a_var", map[string]string{"b_var": "b", "metric": "iops"}},
		{"missing_b_var", map[string]string{"a_var": "a", "metric": "iops"}},
		{"missing_metric", map[string]string{"a_var": "a", "b_var": "b"}},
	}
	for _, tt := range tests {
		act := tr.Action{Params: tt.params}
		_, err := handler.Execute(context.Background(), actx, act)
		if err == nil {
			t.Errorf("%s: expected error", tt.name)
		}
	}
}

func TestBenchCompare_EmptyVarValues(t *testing.T) {
	registry := tr.NewRegistry()
	RegisterBenchActions(registry)
	handler, _ := registry.Get("bench_compare")

	actx := &tr.ActionContext{
		Vars: map[string]string{"a_fio": fioWriteJSON},
		Log:  func(string, ...interface{}) {},
	}

	act := tr.Action{Params: map[string]string{
		"a_var": "a_fio", "b_var": "b_fio", "metric": "iops",
	}}
	_, err := handler.Execute(context.Background(), actx, act)
	if err == nil {
		t.Fatal("expected error for empty b_var value")
	}
}

func TestBenchCompare_InvalidGate(t *testing.T) {
	registry := tr.NewRegistry()
	RegisterBenchActions(registry)
	handler, _ := registry.Get("bench_compare")

	actx := &tr.ActionContext{
		Vars: map[string]string{"a": fioWriteJSON, "b": fioWriteJSON},
		Log:  func(string, ...interface{}) {},
	}

	act := tr.Action{Params: map[string]string{
		"a_var": "a", "b_var": "b", "metric": "iops", "gate": "not-a-number",
	}}
	_, err := handler.Execute(context.Background(), actx, act)
	if err == nil {
		t.Fatal("expected error for invalid gate")
	}
}

func TestBenchCompare_PassWithDirection(t *testing.T) {
	registry := tr.NewRegistry()
	RegisterBenchActions(registry)
	handler, _ := registry.Get("bench_compare")

	actx := &tr.ActionContext{
		Vars: map[string]string{"a": fioMixedJSON, "b": fioMixedJSON},
		Log:  func(string, ...interface{}) {},
	}

	act := tr.Action{Params: map[string]string{
		"a_var": "a", "b_var": "b", "metric": "iops",
		"direction": "read", "gate": "0.9",
	}}
	result, err := handler.Execute(context.Background(), actx, act)
	if err != nil {
		t.Fatalf("expected pass: %v", err)
	}
	if result["value"] != "+0.0%" {
		t.Fatalf("delta = %q, want +0.0%%", result["value"])
	}
}

func TestBenchCompare_LatencyGatePass(t *testing.T) {
	registry := tr.NewRegistry()
	RegisterBenchActions(registry)
	handler, _ := registry.Get("bench_compare")

	// Candidate has lower latency → better → should pass.
	betterJSON := `{"jobs":[{"jobname":"b","read":{"iops":0,"bw_bytes":0,"lat_ns":{"mean":0,"percentile":{}}},
		"write":{"iops":50000,"bw_bytes":204800000,"lat_ns":{"mean":15000,"percentile":{"99.000000":30000}}}}]}`

	actx := &tr.ActionContext{
		Vars: map[string]string{"baseline": fioWriteJSON, "candidate": betterJSON},
		Log:  func(string, ...interface{}) {},
	}

	act := tr.Action{Params: map[string]string{
		"a_var": "baseline", "b_var": "candidate",
		"metric": "lat_p99_us", "gate": "0.9",
	}}
	_, err := handler.Execute(context.Background(), actx, act)
	if err != nil {
		t.Fatalf("expected pass for lower latency candidate: %v", err)
	}
}

// ============================================================
// fioParse action
// ============================================================

func TestFioParse_Action(t *testing.T) {
	registry := tr.NewRegistry()
	RegisterBenchActions(registry)
	handler, _ := registry.Get("fio_parse")

	actx := &tr.ActionContext{
		Vars: map[string]string{"my_fio": fioWriteJSON},
		Log:  func(string, ...interface{}) {},
	}

	act := tr.Action{Params: map[string]string{"json_var": "my_fio", "metric": "iops"}}
	result, err := handler.Execute(context.Background(), actx, act)
	if err != nil {
		t.Fatal(err)
	}
	if result["value"] != "49832.50" {
		t.Fatalf("value = %q, want 49832.50", result["value"])
	}
}

func TestFioParse_MissingVar(t *testing.T) {
	registry := tr.NewRegistry()
	RegisterBenchActions(registry)
	handler, _ := registry.Get("fio_parse")

	actx := &tr.ActionContext{
		Vars: map[string]string{},
		Log:  func(string, ...interface{}) {},
	}

	act := tr.Action{Params: map[string]string{"json_var": "missing", "metric": "iops"}}
	_, err := handler.Execute(context.Background(), actx, act)
	if err == nil {
		t.Fatal("expected error for missing var")
	}
}

func TestFioParse_MissingParams(t *testing.T) {
	registry := tr.NewRegistry()
	RegisterBenchActions(registry)
	handler, _ := registry.Get("fio_parse")

	actx := &tr.ActionContext{
		Vars: map[string]string{"x": fioWriteJSON},
		Log:  func(string, ...interface{}) {},
	}

	// Missing json_var.
	_, err := handler.Execute(context.Background(), actx,
		tr.Action{Params: map[string]string{"metric": "iops"}})
	if err == nil {
		t.Fatal("expected error for missing json_var")
	}

	// Missing metric.
	_, err = handler.Execute(context.Background(), actx,
		tr.Action{Params: map[string]string{"json_var": "x"}})
	if err == nil {
		t.Fatal("expected error for missing metric")
	}
}

func TestFioParse_WithDirection(t *testing.T) {
	registry := tr.NewRegistry()
	RegisterBenchActions(registry)
	handler, _ := registry.Get("fio_parse")

	actx := &tr.ActionContext{
		Vars: map[string]string{"m": fioMixedJSON},
		Log:  func(string, ...interface{}) {},
	}

	act := tr.Action{Params: map[string]string{
		"json_var": "m", "metric": "iops", "direction": "read",
	}}
	result, err := handler.Execute(context.Background(), actx, act)
	if err != nil {
		t.Fatal(err)
	}
	if result["value"] != "35000.00" {
		t.Fatalf("read iops = %q, want 35000.00", result["value"])
	}
}

// ============================================================
// Engine-level integration: bench_compare with mocks
// ============================================================

// mockTestHandler is a simple mock for engine-level tests.
type mockTestHandler struct {
	calls   []tr.Action
	outputs map[string]string
	err     error
}

func (m *mockTestHandler) Execute(_ context.Context, _ *tr.ActionContext, act tr.Action) (map[string]string, error) {
	m.calls = append(m.calls, act)
	if m.err != nil {
		return nil, m.err
	}
	return m.outputs, nil
}

func TestEngine_NVMeBenchScenario(t *testing.T) {
	registry := tr.NewRegistry()

	RegisterBenchActions(registry) // registers fio_json, fio_parse, bench_compare
	// Mock fio_json AFTER RegisterBenchActions to override the real handler.
	fioAction := &mockTestHandler{outputs: map[string]string{"value": fioWriteJSON}}
	registry.Register("fio_json", tr.TierBlock, fioAction)

	scenario := &tr.Scenario{
		Name:    "mini-bench",
		Timeout: tr.Duration{Duration: 30 * time.Second},
		Phases: []tr.Phase{
			{
				Name: "iscsi-bench",
				Actions: []tr.Action{
					{Action: "fio_json", SaveAs: "iscsi_result"},
				},
			},
			{
				Name: "nvme-bench",
				Actions: []tr.Action{
					{Action: "fio_json", SaveAs: "nvme_result"},
				},
			},
			{
				Name: "compare",
				Actions: []tr.Action{
					{
						Action: "bench_compare",
						SaveAs: "cmp_iops",
						Params: map[string]string{
							"a_var": "iscsi_result", "b_var": "nvme_result",
							"metric": "iops", "gate": "0.9",
						},
					},
					{
						Action: "bench_compare",
						SaveAs: "cmp_lat",
						Params: map[string]string{
							"a_var": "iscsi_result", "b_var": "nvme_result",
							"metric": "lat_p99_us", "gate": "0.9",
						},
					},
				},
			},
		},
	}

	engine := tr.NewEngine(registry, nil)
	actx := &tr.ActionContext{
		Scenario: scenario,
		Vars:     make(map[string]string),
		Log:      func(string, ...interface{}) {},
	}
	result := engine.Run(context.Background(), scenario, actx)

	if result.Status != tr.StatusPass {
		t.Fatalf("status = %s, want PASS. error: %s", result.Status, result.Error)
	}
	if len(result.Phases) != 3 {
		t.Fatalf("phases = %d, want 3", len(result.Phases))
	}

	// Same JSON → ratio=1.0, gate=0.9 → pass, delta=+0.0%.
	if actx.Vars["cmp_iops"] != "+0.0%" {
		t.Fatalf("cmp_iops = %q, want +0.0%%", actx.Vars["cmp_iops"])
	}
	// Same latency → ratio=1.0, delta=-0.0%.
	if actx.Vars["cmp_lat"] != "-0.0%" {
		t.Fatalf("cmp_lat = %q, want -0.0%%", actx.Vars["cmp_lat"])
	}
}

func TestEngine_BenchCompare_FailsGate(t *testing.T) {
	registry := tr.NewRegistry()
	RegisterBenchActions(registry)

	highJSON := `{"jobs":[{"jobname":"b","read":{"iops":0,"bw_bytes":0,"lat_ns":{"mean":0,"percentile":{}}},
		"write":{"iops":50000,"bw_bytes":204800000,"lat_ns":{"mean":20000,"percentile":{"99.000000":45000}}}}]}`
	lowJSON := `{"jobs":[{"jobname":"b","read":{"iops":0,"bw_bytes":0,"lat_ns":{"mean":0,"percentile":{}}},
		"write":{"iops":30000,"bw_bytes":122880000,"lat_ns":{"mean":30000,"percentile":{"99.000000":60000}}}}]}`

	scenario := &tr.Scenario{
		Name:    "fail-gate",
		Timeout: tr.Duration{Duration: 10 * time.Second},
		Phases: []tr.Phase{
			{
				Name: "compare",
				Actions: []tr.Action{
					{
						Action: "bench_compare",
						Params: map[string]string{
							"a_var": "baseline", "b_var": "candidate",
							"metric": "iops", "gate": "0.9",
						},
					},
				},
			},
		},
	}

	engine := tr.NewEngine(registry, nil)
	actx := &tr.ActionContext{
		Scenario: scenario,
		Vars:     map[string]string{"baseline": highJSON, "candidate": lowJSON},
		Log:      func(string, ...interface{}) {},
	}
	result := engine.Run(context.Background(), scenario, actx)

	if result.Status != tr.StatusFail {
		t.Fatalf("status = %s, want FAIL (30k/50k = 0.6 < gate 0.9)", result.Status)
	}
}

func TestEngine_BenchCompare_LatencyFails(t *testing.T) {
	registry := tr.NewRegistry()
	RegisterBenchActions(registry)

	goodLat := `{"jobs":[{"jobname":"b","read":{"iops":0,"bw_bytes":0,"lat_ns":{"mean":0,"percentile":{}}},
		"write":{"iops":50000,"bw_bytes":204800000,"lat_ns":{"mean":20000,"percentile":{"99.000000":30000}}}}]}`
	badLat := `{"jobs":[{"jobname":"b","read":{"iops":0,"bw_bytes":0,"lat_ns":{"mean":0,"percentile":{}}},
		"write":{"iops":50000,"bw_bytes":204800000,"lat_ns":{"mean":40000,"percentile":{"99.000000":90000}}}}]}`

	scenario := &tr.Scenario{
		Name:    "lat-fail",
		Timeout: tr.Duration{Duration: 10 * time.Second},
		Phases: []tr.Phase{
			{
				Name: "compare",
				Actions: []tr.Action{
					{
						Action: "bench_compare",
						Params: map[string]string{
							"a_var": "baseline", "b_var": "candidate",
							"metric": "lat_p99_us", "gate": "0.9",
						},
					},
				},
			},
		},
	}

	engine := tr.NewEngine(registry, nil)
	actx := &tr.ActionContext{
		Scenario: scenario,
		Vars:     map[string]string{"baseline": goodLat, "candidate": badLat},
		Log:      func(string, ...interface{}) {},
	}
	result := engine.Run(context.Background(), scenario, actx)

	if result.Status != tr.StatusFail {
		t.Fatalf("status = %s, want FAIL (lat 90µs vs 30µs baseline)", result.Status)
	}
}

// ============================================================
// warn_gate behavior
// ============================================================

func TestBenchCompare_WarnGate_InWarnBand(t *testing.T) {
	registry := tr.NewRegistry()
	RegisterBenchActions(registry)
	handler, _ := registry.Get("bench_compare")

	// Candidate = 85% of baseline → below gate (0.9) but above warn_gate (0.8).
	highJSON := `{"jobs":[{"jobname":"b","read":{"iops":0,"bw_bytes":0,"lat_ns":{"mean":0,"percentile":{}}},
		"write":{"iops":10000,"bw_bytes":40960000,"lat_ns":{"mean":20000,"percentile":{"99.000000":45000}}}}]}`
	lowJSON := `{"jobs":[{"jobname":"b","read":{"iops":0,"bw_bytes":0,"lat_ns":{"mean":0,"percentile":{}}},
		"write":{"iops":8500,"bw_bytes":34816000,"lat_ns":{"mean":20000,"percentile":{"99.000000":45000}}}}]}`

	actx := &tr.ActionContext{
		Vars: map[string]string{"a": highJSON, "b": lowJSON},
		Log:  func(string, ...interface{}) {},
	}

	act := tr.Action{Params: map[string]string{
		"a_var": "a", "b_var": "b", "metric": "iops",
		"gate": "0.9", "warn_gate": "0.8",
	}}
	result, err := handler.Execute(context.Background(), actx, act)
	if err != nil {
		t.Fatalf("expected success with WARN, got error: %v", err)
	}
	if !strings.HasPrefix(result["value"], "WARN:") {
		t.Fatalf("value = %q, want WARN: prefix", result["value"])
	}
}

func TestBenchCompare_WarnGate_BelowWarnGate(t *testing.T) {
	registry := tr.NewRegistry()
	RegisterBenchActions(registry)
	handler, _ := registry.Get("bench_compare")

	// Candidate = 70% of baseline → below both gate and warn_gate.
	highJSON := `{"jobs":[{"jobname":"b","read":{"iops":0,"bw_bytes":0,"lat_ns":{"mean":0,"percentile":{}}},
		"write":{"iops":10000,"bw_bytes":40960000,"lat_ns":{"mean":20000,"percentile":{"99.000000":45000}}}}]}`
	lowJSON := `{"jobs":[{"jobname":"b","read":{"iops":0,"bw_bytes":0,"lat_ns":{"mean":0,"percentile":{}}},
		"write":{"iops":7000,"bw_bytes":28672000,"lat_ns":{"mean":20000,"percentile":{"99.000000":45000}}}}]}`

	actx := &tr.ActionContext{
		Vars: map[string]string{"a": highJSON, "b": lowJSON},
		Log:  func(string, ...interface{}) {},
	}

	act := tr.Action{Params: map[string]string{
		"a_var": "a", "b_var": "b", "metric": "iops",
		"gate": "0.9", "warn_gate": "0.8",
	}}
	_, err := handler.Execute(context.Background(), actx, act)
	if err == nil {
		t.Fatal("expected hard fail below warn_gate")
	}
	if !strings.Contains(err.Error(), "FAIL") {
		t.Fatalf("error = %q, want FAIL", err.Error())
	}
}

func TestBenchCompare_WarnGate_AboveGate(t *testing.T) {
	registry := tr.NewRegistry()
	RegisterBenchActions(registry)
	handler, _ := registry.Get("bench_compare")

	// Candidate = 100% of baseline → above gate → normal PASS, no WARN prefix.
	actx := &tr.ActionContext{
		Vars: map[string]string{"a": fioWriteJSON, "b": fioWriteJSON},
		Log:  func(string, ...interface{}) {},
	}

	act := tr.Action{Params: map[string]string{
		"a_var": "a", "b_var": "b", "metric": "iops",
		"gate": "0.9", "warn_gate": "0.8",
	}}
	result, err := handler.Execute(context.Background(), actx, act)
	if err != nil {
		t.Fatalf("expected pass: %v", err)
	}
	if strings.HasPrefix(result["value"], "WARN:") {
		t.Fatalf("value = %q, want no WARN prefix (above gate)", result["value"])
	}
}

func TestBenchCompare_WarnGate_InvalidValue(t *testing.T) {
	registry := tr.NewRegistry()
	RegisterBenchActions(registry)
	handler, _ := registry.Get("bench_compare")

	actx := &tr.ActionContext{
		Vars: map[string]string{"a": fioWriteJSON, "b": fioWriteJSON},
		Log:  func(string, ...interface{}) {},
	}

	act := tr.Action{Params: map[string]string{
		"a_var": "a", "b_var": "b", "metric": "iops",
		"gate": "0.9", "warn_gate": "bad",
	}}
	_, err := handler.Execute(context.Background(), actx, act)
	if err == nil {
		t.Fatal("expected error for invalid warn_gate")
	}
}

func TestBenchCompare_WarnGate_LatencyInWarnBand(t *testing.T) {
	registry := tr.NewRegistry()
	RegisterBenchActions(registry)
	handler, _ := registry.Get("bench_compare")

	// Baseline lat 30µs, candidate lat 35µs → ratio=30/35=0.857, below gate 0.9 but above warn_gate 0.8.
	baseJSON := `{"jobs":[{"jobname":"b","read":{"iops":0,"bw_bytes":0,"lat_ns":{"mean":0,"percentile":{}}},
		"write":{"iops":50000,"bw_bytes":204800000,"lat_ns":{"mean":20000,"percentile":{"99.000000":30000}}}}]}`
	candJSON := `{"jobs":[{"jobname":"b","read":{"iops":0,"bw_bytes":0,"lat_ns":{"mean":0,"percentile":{}}},
		"write":{"iops":50000,"bw_bytes":204800000,"lat_ns":{"mean":25000,"percentile":{"99.000000":35000}}}}]}`

	actx := &tr.ActionContext{
		Vars: map[string]string{"a": baseJSON, "b": candJSON},
		Log:  func(string, ...interface{}) {},
	}

	act := tr.Action{Params: map[string]string{
		"a_var": "a", "b_var": "b", "metric": "lat_p99_us",
		"gate": "0.9", "warn_gate": "0.8",
	}}
	result, err := handler.Execute(context.Background(), actx, act)
	if err != nil {
		t.Fatalf("expected WARN success for latency in warn band: %v", err)
	}
	if !strings.HasPrefix(result["value"], "WARN:") {
		t.Fatalf("value = %q, want WARN: prefix", result["value"])
	}
}

// ============================================================
// TargetSpec sanitization (Finding 3)
// ============================================================

func TestTargetSpec_NQN_Sanitized(t *testing.T) {
	spec := tr.TargetSpec{NQNSuffix: "My_Volume"}
	got := spec.NQN()
	want := "nqn.2024-01.com.seaweedfs:vol.my-volume"
	if got != want {
		t.Fatalf("NQN() = %q, want %q (sanitized)", got, want)
	}
}

func TestTargetSpec_IQN_Sanitized(t *testing.T) {
	spec := tr.TargetSpec{IQNSuffix: "My_Volume"}
	got := spec.IQN()
	want := "iqn.2024.com.seaweedfs:my-volume"
	if got != want {
		t.Fatalf("IQN() = %q, want %q (sanitized)", got, want)
	}
}

func TestTargetSpec_NQN_LongNameTruncated(t *testing.T) {
	long := strings.Repeat("a", 100)
	spec := tr.TargetSpec{NQNSuffix: long}
	got := spec.NQN()
	// SanitizeIQN truncates to 64 chars with hash suffix.
	prefix := "nqn.2024-01.com.seaweedfs:vol."
	suffix := got[len(prefix):]
	if len(suffix) > 64 {
		t.Fatalf("suffix len = %d, want <= 64", len(suffix))
	}
}

// ============================================================
// paramDefault helper
// ============================================================

func TestParamDefault(t *testing.T) {
	params := map[string]string{"key": "val"}
	if got := paramDefault(params, "key", "def"); got != "val" {
		t.Fatalf("got %q, want val", got)
	}
	if got := paramDefault(params, "missing", "def"); got != "def" {
		t.Fatalf("got %q, want def", got)
	}
	if got := paramDefault(nil, "key", "def"); got != "def" {
		t.Fatalf("got %q, want def", got)
	}
}
