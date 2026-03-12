package testrunner

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"
)

// mockHandler records calls and returns configured outputs.
type mockHandler struct {
	calls   []Action
	outputs map[string]string
	err     error
}

func (m *mockHandler) Execute(ctx context.Context, actx *ActionContext, act Action) (map[string]string, error) {
	m.calls = append(m.calls, act)
	if m.err != nil {
		return nil, m.err
	}
	return m.outputs, nil
}

func TestEngine_BasicFlow(t *testing.T) {
	registry := NewRegistry()

	step1 := &mockHandler{outputs: map[string]string{"value": "hello"}}
	step2 := &mockHandler{outputs: map[string]string{"value": "world"}}

	registry.Register("step1", TierCore, step1)
	registry.Register("step2", TierCore, step2)

	scenario := &Scenario{
		Name:    "basic-test",
		Timeout: Duration{5 * time.Second},
		Phases: []Phase{
			{
				Name: "phase1",
				Actions: []Action{
					{Action: "step1", SaveAs: "v1"},
					{Action: "step2", SaveAs: "v2"},
				},
			},
		},
	}

	engine := NewEngine(registry, nil)
	actx := &ActionContext{
		Scenario: scenario,
		Vars:     make(map[string]string),
		Log:      func(string, ...interface{}) {},
	}
	result := engine.Run(context.Background(), scenario, actx)

	if result.Status != StatusPass {
		t.Errorf("status = %s, want PASS", result.Status)
	}
	if len(result.Phases) != 1 {
		t.Fatalf("phases = %d, want 1", len(result.Phases))
	}
	if len(result.Phases[0].Actions) != 2 {
		t.Fatalf("actions = %d, want 2", len(result.Phases[0].Actions))
	}
	if actx.Vars["v1"] != "hello" {
		t.Errorf("v1 = %q, want %q", actx.Vars["v1"], "hello")
	}
	if actx.Vars["v2"] != "world" {
		t.Errorf("v2 = %q, want %q", actx.Vars["v2"], "world")
	}
}

func TestEngine_FailureStopsPhase(t *testing.T) {
	registry := NewRegistry()

	step1 := &mockHandler{err: fmt.Errorf("boom")}
	step2 := &mockHandler{outputs: map[string]string{"value": "ok"}}

	registry.Register("step1", TierCore, step1)
	registry.Register("step2", TierCore, step2)

	scenario := &Scenario{
		Name:    "fail-test",
		Timeout: Duration{5 * time.Second},
		Phases: []Phase{
			{
				Name: "phase1",
				Actions: []Action{
					{Action: "step1"},
					{Action: "step2"}, // should not run
				},
			},
		},
	}

	engine := NewEngine(registry, nil)
	actx := &ActionContext{
		Scenario: scenario,
		Vars:     make(map[string]string),
		Log:      func(string, ...interface{}) {},
	}
	result := engine.Run(context.Background(), scenario, actx)

	if result.Status != StatusFail {
		t.Errorf("status = %s, want FAIL", result.Status)
	}
	if len(result.Phases[0].Actions) != 1 {
		t.Errorf("actions = %d, want 1 (should stop after failure)", len(result.Phases[0].Actions))
	}
	if len(step2.calls) != 0 {
		t.Error("step2 should not have been called")
	}
}

func TestEngine_IgnoreError(t *testing.T) {
	registry := NewRegistry()

	step1 := &mockHandler{err: fmt.Errorf("expected error")}
	step2 := &mockHandler{outputs: map[string]string{"value": "ok"}}

	registry.Register("step1", TierCore, step1)
	registry.Register("step2", TierCore, step2)

	scenario := &Scenario{
		Name:    "ignore-test",
		Timeout: Duration{5 * time.Second},
		Phases: []Phase{
			{
				Name: "phase1",
				Actions: []Action{
					{Action: "step1", IgnoreError: true},
					{Action: "step2"},
				},
			},
		},
	}

	engine := NewEngine(registry, nil)
	actx := &ActionContext{
		Scenario: scenario,
		Vars:     make(map[string]string),
		Log:      func(string, ...interface{}) {},
	}
	result := engine.Run(context.Background(), scenario, actx)

	if result.Status != StatusPass {
		t.Errorf("status = %s, want PASS (error was ignored)", result.Status)
	}
	if len(step2.calls) != 1 {
		t.Error("step2 should have been called despite step1 error")
	}
}

func TestEngine_AlwaysPhaseRunsAfterFailure(t *testing.T) {
	registry := NewRegistry()

	failStep := &mockHandler{err: fmt.Errorf("fail")}
	cleanStep := &mockHandler{}

	registry.Register("fail_step", TierCore, failStep)
	registry.Register("clean_step", TierCore, cleanStep)

	scenario := &Scenario{
		Name:    "always-test",
		Timeout: Duration{5 * time.Second},
		Phases: []Phase{
			{
				Name: "main",
				Actions: []Action{
					{Action: "fail_step"},
				},
			},
			{
				Name:   "cleanup",
				Always: true,
				Actions: []Action{
					{Action: "clean_step"},
				},
			},
		},
	}

	engine := NewEngine(registry, nil)
	actx := &ActionContext{
		Scenario: scenario,
		Vars:     make(map[string]string),
		Log:      func(string, ...interface{}) {},
	}
	result := engine.Run(context.Background(), scenario, actx)

	if result.Status != StatusFail {
		t.Errorf("status = %s, want FAIL", result.Status)
	}
	if len(result.Phases) != 2 {
		t.Fatalf("phases = %d, want 2 (cleanup should still run)", len(result.Phases))
	}
	if len(cleanStep.calls) != 1 {
		t.Error("cleanup step should have been called")
	}
}

func TestEngine_VarSubstitution(t *testing.T) {
	registry := NewRegistry()

	step1 := &mockHandler{outputs: map[string]string{"value": "/dev/sda"}}
	step2 := &mockHandler{}

	registry.Register("step1", TierCore, step1)
	registry.Register("step2", TierCore, step2)

	scenario := &Scenario{
		Name:    "var-test",
		Timeout: Duration{5 * time.Second},
		Phases: []Phase{
			{
				Name: "phase1",
				Actions: []Action{
					{Action: "step1", SaveAs: "device"},
					{Action: "step2", Params: map[string]string{"device": "{{ device }}"}},
				},
			},
		},
	}

	engine := NewEngine(registry, nil)
	actx := &ActionContext{
		Scenario: scenario,
		Vars:     make(map[string]string),
		Log:      func(string, ...interface{}) {},
	}
	engine.Run(context.Background(), scenario, actx)

	if len(step2.calls) != 1 {
		t.Fatalf("step2 not called")
	}
	if step2.calls[0].Params["device"] != "/dev/sda" {
		t.Errorf("device = %q, want /dev/sda", step2.calls[0].Params["device"])
	}
}

func TestEngine_EnvVars(t *testing.T) {
	registry := NewRegistry()

	step := &mockHandler{}
	registry.Register("step", TierCore, step)

	scenario := &Scenario{
		Name:    "env-test",
		Timeout: Duration{5 * time.Second},
		Env:     map[string]string{"repo_dir": "/tmp/repo"},
		Phases: []Phase{
			{
				Name: "phase1",
				Actions: []Action{
					{Action: "step", Params: map[string]string{"dir": "{{ repo_dir }}"}},
				},
			},
		},
	}

	engine := NewEngine(registry, nil)
	actx := &ActionContext{
		Scenario: scenario,
		Vars:     make(map[string]string),
		Log:      func(string, ...interface{}) {},
	}
	engine.Run(context.Background(), scenario, actx)

	if len(step.calls) != 1 {
		t.Fatalf("step not called")
	}
	if step.calls[0].Params["dir"] != "/tmp/repo" {
		t.Errorf("dir = %q, want /tmp/repo", step.calls[0].Params["dir"])
	}
}

func TestEngine_Timeout(t *testing.T) {
	registry := NewRegistry()

	slowStep := ActionHandlerFunc(func(ctx context.Context, actx *ActionContext, act Action) (map[string]string, error) {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(10 * time.Second):
			return nil, nil
		}
	})
	registry.Register("slow", TierCore, slowStep)

	scenario := &Scenario{
		Name:    "timeout-test",
		Timeout: Duration{100 * time.Millisecond},
		Phases: []Phase{
			{
				Name: "phase1",
				Actions: []Action{
					{Action: "slow"},
				},
			},
		},
	}

	engine := NewEngine(registry, nil)
	actx := &ActionContext{
		Scenario: scenario,
		Vars:     make(map[string]string),
		Log:      func(string, ...interface{}) {},
	}

	start := time.Now()
	result := engine.Run(context.Background(), scenario, actx)
	elapsed := time.Since(start)

	if result.Status != StatusFail {
		t.Errorf("status = %s, want FAIL", result.Status)
	}
	if elapsed > 2*time.Second {
		t.Errorf("took %v, expected timeout at ~100ms", elapsed)
	}
}

func TestEngine_UnknownAction(t *testing.T) {
	registry := NewRegistry()
	scenario := &Scenario{
		Name:    "unknown-test",
		Timeout: Duration{5 * time.Second},
		Phases: []Phase{
			{
				Name: "phase1",
				Actions: []Action{
					{Action: "nonexistent"},
				},
			},
		},
	}

	engine := NewEngine(registry, nil)
	actx := &ActionContext{
		Scenario: scenario,
		Vars:     make(map[string]string),
		Log:      func(string, ...interface{}) {},
	}
	result := engine.Run(context.Background(), scenario, actx)

	if result.Status != StatusFail {
		t.Errorf("status = %s, want FAIL for unknown action", result.Status)
	}
}

func TestEngine_ParallelPhase(t *testing.T) {
	registry := NewRegistry()

	callCount := 0
	step := ActionHandlerFunc(func(ctx context.Context, actx *ActionContext, act Action) (map[string]string, error) {
		callCount++
		time.Sleep(50 * time.Millisecond)
		return nil, nil
	})
	registry.Register("step", TierCore, step)

	scenario := &Scenario{
		Name:    "parallel-test",
		Timeout: Duration{5 * time.Second},
		Phases: []Phase{
			{
				Name:     "phase1",
				Parallel: true,
				Actions: []Action{
					{Action: "step"},
					{Action: "step"},
					{Action: "step"},
				},
			},
		},
	}

	engine := NewEngine(registry, nil)
	actx := &ActionContext{
		Scenario: scenario,
		Vars:     make(map[string]string),
		Log:      func(string, ...interface{}) {},
	}

	start := time.Now()
	result := engine.Run(context.Background(), scenario, actx)
	elapsed := time.Since(start)

	if result.Status != StatusPass {
		t.Errorf("status = %s, want PASS", result.Status)
	}
	// Parallel: should take ~50ms, not 150ms.
	if elapsed > 200*time.Millisecond {
		t.Errorf("took %v, parallel should be faster", elapsed)
	}
}

func TestResolveVars(t *testing.T) {
	vars := map[string]string{
		"device": "/dev/sda",
		"md5":    "abc123",
	}

	tests := []struct {
		input string
		want  string
	}{
		{"{{ device }}", "/dev/sda"},
		{"{{md5}}", "abc123"},
		{"dev={{ device }}, hash={{ md5 }}", "dev=/dev/sda, hash=abc123"},
		{"no vars", "no vars"},
		{"{{ unknown }}", "{{ unknown }}"}, // unresolved stays
	}

	for _, tt := range tests {
		got := resolveVars(tt.input, vars)
		if got != tt.want {
			t.Errorf("resolveVars(%q) = %q, want %q", tt.input, got, tt.want)
		}
	}
}

func TestEngine_VarsInResult(t *testing.T) {
	registry := NewRegistry()
	step := &mockHandler{outputs: map[string]string{"value": "saved-data"}}
	registry.Register("step", TierCore, step)

	scenario := &Scenario{
		Name:    "vars-result-test",
		Timeout: Duration{5 * time.Second},
		Env:     map[string]string{"env_key": "env_val"},
		Phases: []Phase{
			{
				Name: "phase1",
				Actions: []Action{
					{Action: "step", SaveAs: "my_var"},
				},
			},
		},
	}

	engine := NewEngine(registry, nil)
	actx := &ActionContext{
		Scenario: scenario,
		Vars:     make(map[string]string),
		Log:      func(string, ...interface{}) {},
	}
	result := engine.Run(context.Background(), scenario, actx)

	if result.Status != StatusPass {
		t.Fatalf("status = %s: %s", result.Status, result.Error)
	}

	// Verify vars are copied into result.
	if result.Vars == nil {
		t.Fatal("result.Vars is nil")
	}
	if result.Vars["my_var"] != "saved-data" {
		t.Errorf("result.Vars[my_var] = %q, want saved-data", result.Vars["my_var"])
	}
	if result.Vars["env_key"] != "env_val" {
		t.Errorf("result.Vars[env_key] = %q, want env_val", result.Vars["env_key"])
	}
}

func TestEngine_Repeat3Pass(t *testing.T) {
	registry := NewRegistry()

	callCount := 0
	step := ActionHandlerFunc(func(ctx context.Context, actx *ActionContext, act Action) (map[string]string, error) {
		callCount++
		return map[string]string{"value": fmt.Sprintf("iter%d", callCount)}, nil
	})
	registry.Register("step", TierCore, step)

	scenario := &Scenario{
		Name:    "repeat-3-test",
		Timeout: Duration{5 * time.Second},
		Phases: []Phase{
			{
				Name:   "repeating",
				Repeat: 3,
				Actions: []Action{
					{Action: "step"},
				},
			},
		},
	}

	engine := NewEngine(registry, nil)
	actx := &ActionContext{
		Scenario: scenario,
		Vars:     make(map[string]string),
		Log:      func(string, ...interface{}) {},
	}
	result := engine.Run(context.Background(), scenario, actx)

	if result.Status != StatusPass {
		t.Fatalf("status = %s, want PASS: %s", result.Status, result.Error)
	}
	if callCount != 3 {
		t.Errorf("step called %d times, want 3", callCount)
	}
	if len(result.Phases) != 3 {
		t.Fatalf("phases = %d, want 3", len(result.Phases))
	}
	// Check decorated names.
	for i, pr := range result.Phases {
		expected := fmt.Sprintf("repeating[%d/3]", i+1)
		if pr.Name != expected {
			t.Errorf("phase[%d].Name = %q, want %q", i, pr.Name, expected)
		}
	}
}

func TestEngine_RepeatFailStopsEarly(t *testing.T) {
	registry := NewRegistry()

	callCount := 0
	step := ActionHandlerFunc(func(ctx context.Context, actx *ActionContext, act Action) (map[string]string, error) {
		callCount++
		if callCount == 2 {
			return nil, fmt.Errorf("fail on iter 2")
		}
		return nil, nil
	})
	registry.Register("step", TierCore, step)

	scenario := &Scenario{
		Name:    "repeat-fail-test",
		Timeout: Duration{5 * time.Second},
		Phases: []Phase{
			{
				Name:   "repeating",
				Repeat: 5,
				Actions: []Action{
					{Action: "step"},
				},
			},
		},
	}

	engine := NewEngine(registry, nil)
	actx := &ActionContext{
		Scenario: scenario,
		Vars:     make(map[string]string),
		Log:      func(string, ...interface{}) {},
	}
	result := engine.Run(context.Background(), scenario, actx)

	if result.Status != StatusFail {
		t.Errorf("status = %s, want FAIL", result.Status)
	}
	if callCount != 2 {
		t.Errorf("step called %d times, want 2 (should stop on first failure)", callCount)
	}
	if len(result.Phases) != 2 {
		t.Errorf("phases = %d, want 2", len(result.Phases))
	}
}

func TestEngine_RepeatAggregateMedian(t *testing.T) {
	registry := NewRegistry()

	iter := 0
	values := []string{"100", "200", "150", "180", "170"}
	step := ActionHandlerFunc(func(ctx context.Context, actx *ActionContext, act Action) (map[string]string, error) {
		v := values[iter]
		iter++
		return map[string]string{"value": v}, nil
	})
	registry.Register("step", TierCore, step)

	scenario := &Scenario{
		Name:    "aggregate-test",
		Timeout: Duration{5 * time.Second},
		Phases: []Phase{
			{
				Name:      "bench",
				Repeat:    5,
				Aggregate: "median",
				TrimPct:   20,
				Actions: []Action{
					{Action: "step", SaveAs: "iops"},
				},
			},
		},
	}

	engine := NewEngine(registry, nil)
	actx := &ActionContext{
		Scenario: scenario,
		Vars:     make(map[string]string),
		Log:      func(string, ...interface{}) {},
	}
	result := engine.Run(context.Background(), scenario, actx)

	if result.Status != StatusPass {
		t.Fatalf("status = %s: %s", result.Status, result.Error)
	}
	if iter != 5 {
		t.Fatalf("step called %d times, want 5", iter)
	}

	// Verify aggregated vars exist.
	if v := actx.Vars["iops_median"]; v == "" {
		t.Fatal("iops_median not set")
	}
	if v := actx.Vars["iops_mean"]; v == "" {
		t.Fatal("iops_mean not set")
	}
	if v := actx.Vars["iops_all"]; v == "" {
		t.Fatal("iops_all not set")
	}
	if v := actx.Vars["iops_n"]; v == "" {
		t.Fatal("iops_n not set")
	}

	// The primary var should be overwritten with the median.
	// Values: [100, 200, 150, 180, 170], trim 20% = remove 1 from each end
	// Sorted: [100, 150, 170, 180, 200], trimmed: [150, 170, 180]
	// Median of [150, 170, 180] = 170
	if actx.Vars["iops"] != "170.00" {
		t.Errorf("iops = %q, want 170.00 (median after trim)", actx.Vars["iops"])
	}
}

func TestEngine_RepeatAggregateMean(t *testing.T) {
	registry := NewRegistry()

	iter := 0
	values := []string{"100", "200", "150", "180", "170"}
	step := ActionHandlerFunc(func(ctx context.Context, actx *ActionContext, act Action) (map[string]string, error) {
		v := values[iter]
		iter++
		return map[string]string{"value": v}, nil
	})
	registry.Register("step", TierCore, step)

	scenario := &Scenario{
		Name:    "aggregate-mean-test",
		Timeout: Duration{5 * time.Second},
		Phases: []Phase{
			{
				Name:      "bench",
				Repeat:    5,
				Aggregate: "mean",
				TrimPct:   20,
				Actions: []Action{
					{Action: "step", SaveAs: "iops"},
				},
			},
		},
	}

	engine := NewEngine(registry, nil)
	actx := &ActionContext{
		Scenario: scenario,
		Vars:     make(map[string]string),
		Log:      func(string, ...interface{}) {},
	}
	result := engine.Run(context.Background(), scenario, actx)

	if result.Status != StatusPass {
		t.Fatalf("status = %s: %s", result.Status, result.Error)
	}

	// Trimmed: [150, 170, 180], mean = 166.67
	if actx.Vars["iops"] != "166.67" {
		t.Errorf("iops = %q, want 166.67 (mean after trim)", actx.Vars["iops"])
	}
}

func TestEngine_RepeatAggregateNone(t *testing.T) {
	registry := NewRegistry()

	iter := 0
	step := ActionHandlerFunc(func(ctx context.Context, actx *ActionContext, act Action) (map[string]string, error) {
		iter++
		return map[string]string{"value": fmt.Sprintf("%d", iter*100)}, nil
	})
	registry.Register("step", TierCore, step)

	scenario := &Scenario{
		Name:    "aggregate-none-test",
		Timeout: Duration{5 * time.Second},
		Phases: []Phase{
			{
				Name:      "bench",
				Repeat:    3,
				Aggregate: "none",
				Actions: []Action{
					{Action: "step", SaveAs: "iops"},
				},
			},
		},
	}

	engine := NewEngine(registry, nil)
	actx := &ActionContext{
		Scenario: scenario,
		Vars:     make(map[string]string),
		Log:      func(string, ...interface{}) {},
	}
	result := engine.Run(context.Background(), scenario, actx)

	if result.Status != StatusPass {
		t.Fatalf("status = %s: %s", result.Status, result.Error)
	}

	// With aggregate: none, the var should hold the last iteration's value.
	if actx.Vars["iops"] != "300" {
		t.Errorf("iops = %q, want 300 (last iteration, no aggregation)", actx.Vars["iops"])
	}
	// And no aggregate vars should be set.
	if _, ok := actx.Vars["iops_median"]; ok {
		t.Error("iops_median should not be set with aggregate: none")
	}
}

func TestTrimOutliers(t *testing.T) {
	tests := []struct {
		name   string
		values []float64
		pct    int
		want   int // expected length after trim
	}{
		{"5 values trim 20%", []float64{1, 2, 3, 4, 5}, 20, 3},
		{"10 values trim 10%", []float64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}, 10, 8},
		{"3 values trim 20%", []float64{1, 2, 3}, 20, 1},
		{"2 values no trim", []float64{1, 2}, 20, 2},
		{"empty no trim", []float64{}, 20, 0},
		{"no trim pct 0", []float64{1, 2, 3, 4, 5}, 0, 5},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := trimOutliers(tt.values, tt.pct)
			if len(got) != tt.want {
				t.Errorf("trimOutliers(%v, %d) len = %d, want %d", tt.values, tt.pct, len(got), tt.want)
			}
		})
	}
}

// TestParse_InlineParams verifies that YAML fields not in the Action struct
// are captured into Params via the inline tag. This is a regression test for
// the snapshot-stress failure where `id: "1"` was not captured.
func TestParse_InlineParams(t *testing.T) {
	yaml := `
name: inline-test
timeout: 5m
topology:
  nodes:
    node1:
      host: "127.0.0.1"
      is_local: true
targets:
  primary:
    node: node1
    iscsi_port: 3260
    admin_port: 8080
    iqn_suffix: test-primary
phases:
  - name: test_phase
    actions:
      - action: snapshot_create
        target: primary
        id: "42"
      - action: dd_write
        node: node1
        device: "/dev/sda"
        bs: 4k
        count: "10"
      - action: kubectl_apply
        node: node1
        file: "/tmp/cr.yaml"
        namespace: "sw-block"
`

	s, err := Parse([]byte(yaml))
	if err != nil {
		t.Fatalf("parse: %v", err)
	}

	// Verify inline params are captured for each action type.
	phase := s.Phases[0]

	// snapshot_create: id should be in Params
	snapAct := phase.Actions[0]
	if snapAct.Params["id"] != "42" {
		t.Errorf("snapshot_create: id = %q, want %q (inline param not captured)",
			snapAct.Params["id"], "42")
	}

	// dd_write: device, bs, count should be in Params
	ddAct := phase.Actions[1]
	if ddAct.Params["device"] != "/dev/sda" {
		t.Errorf("dd_write: device = %q, want /dev/sda", ddAct.Params["device"])
	}
	if ddAct.Params["bs"] != "4k" {
		t.Errorf("dd_write: bs = %q, want 4k", ddAct.Params["bs"])
	}
	if ddAct.Params["count"] != "10" {
		t.Errorf("dd_write: count = %q, want 10", ddAct.Params["count"])
	}

	// kubectl_apply: file, namespace should be in Params
	k8sAct := phase.Actions[2]
	if k8sAct.Params["file"] != "/tmp/cr.yaml" {
		t.Errorf("kubectl_apply: file = %q, want /tmp/cr.yaml", k8sAct.Params["file"])
	}
	if k8sAct.Params["namespace"] != "sw-block" {
		t.Errorf("kubectl_apply: namespace = %q, want sw-block", k8sAct.Params["namespace"])
	}
}

// TestResolveAction_PreservesInlineParams verifies that resolveAction doesn't
// lose inline params when copying the action.
func TestResolveAction_PreservesInlineParams(t *testing.T) {
	act := Action{
		Action: "snapshot_create",
		Target: "primary",
		Params: map[string]string{
			"id":     "5",
			"device": "{{ dev }}",
		},
	}

	vars := map[string]string{"dev": "/dev/sdb"}
	resolved := resolveAction(act, vars)

	if resolved.Params["id"] != "5" {
		t.Errorf("id = %q, want 5", resolved.Params["id"])
	}
	if resolved.Params["device"] != "/dev/sdb" {
		t.Errorf("device = %q, want /dev/sdb (should resolve var)", resolved.Params["device"])
	}
}

func TestEngine_CleanupVars(t *testing.T) {
	registry := NewRegistry()

	inject := &mockHandler{outputs: map[string]string{
		"__cleanup_netem": "tc qdisc del dev eth0 root",
	}}
	clear := ActionHandlerFunc(func(ctx context.Context, actx *ActionContext, act Action) (map[string]string, error) {
		cmd := actx.Vars["__cleanup_netem"]
		if cmd == "" {
			return nil, fmt.Errorf("cleanup var not found")
		}
		return map[string]string{"value": cmd}, nil
	})

	registry.Register("inject", TierCore, inject)
	registry.Register("clear", TierCore, clear)

	scenario := &Scenario{
		Name:    "cleanup-var-test",
		Timeout: Duration{5 * time.Second},
		Phases: []Phase{
			{
				Name: "inject",
				Actions: []Action{
					{Action: "inject"},
				},
			},
			{
				Name: "clear",
				Actions: []Action{
					{Action: "clear", SaveAs: "result"},
				},
			},
		},
	}

	engine := NewEngine(registry, nil)
	actx := &ActionContext{
		Scenario: scenario,
		Vars:     make(map[string]string),
		Log:      func(string, ...interface{}) {},
	}
	result := engine.Run(context.Background(), scenario, actx)

	if result.Status != StatusPass {
		t.Errorf("status = %s, want PASS: %s", result.Status, result.Error)
	}
	if actx.Vars["result"] != "tc qdisc del dev eth0 root" {
		t.Errorf("result = %q", actx.Vars["result"])
	}
}

func TestEngine_ActionTimeout_Enforced(t *testing.T) {
	registry := NewRegistry()

	// Action that sleeps forever, should be killed by action-level timeout.
	slowStep := ActionHandlerFunc(func(ctx context.Context, actx *ActionContext, act Action) (map[string]string, error) {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(30 * time.Second):
			return nil, nil
		}
	})
	registry.Register("slow", TierCore, slowStep)

	scenario := &Scenario{
		Name:    "action-timeout-test",
		Timeout: Duration{10 * time.Second}, // scenario timeout is generous
		Phases: []Phase{
			{
				Name: "phase1",
				Actions: []Action{
					{Action: "slow", Timeout: "150ms"},
				},
			},
		},
	}

	engine := NewEngine(registry, nil)
	actx := &ActionContext{
		Scenario: scenario,
		Vars:     make(map[string]string),
		Log:      func(string, ...interface{}) {},
	}

	start := time.Now()
	result := engine.Run(context.Background(), scenario, actx)
	elapsed := time.Since(start)

	if result.Status != StatusFail {
		t.Errorf("status = %s, want FAIL", result.Status)
	}
	// Should timeout at ~150ms, not 10s.
	if elapsed > 2*time.Second {
		t.Errorf("took %v, action timeout should have fired at ~150ms", elapsed)
	}
	// Error message should mention the action name and timeout.
	if len(result.Phases) > 0 && len(result.Phases[0].Actions) > 0 {
		errMsg := result.Phases[0].Actions[0].Error
		if !strings.Contains(errMsg, "slow") || !strings.Contains(errMsg, "timed out") {
			t.Errorf("error = %q, should mention action name and timeout", errMsg)
		}
	}
}

func TestEngine_TempRoot_UniquePerRun(t *testing.T) {
	registry := NewRegistry()
	step := &mockHandler{}
	registry.Register("step", TierCore, step)

	scenario := &Scenario{
		Name:    "tempdir-test",
		Timeout: Duration{5 * time.Second},
		Phases: []Phase{
			{
				Name:    "phase1",
				Actions: []Action{{Action: "step"}},
			},
		},
	}

	engine := NewEngine(registry, nil)

	// Run 1
	actx1 := &ActionContext{
		Scenario: scenario,
		Vars:     make(map[string]string),
		Log:      func(string, ...interface{}) {},
	}
	engine.Run(context.Background(), scenario, actx1)

	// Small delay so timestamp differs.
	time.Sleep(2 * time.Millisecond)

	// Run 2
	actx2 := &ActionContext{
		Scenario: scenario,
		Vars:     make(map[string]string),
		Log:      func(string, ...interface{}) {},
	}
	engine.Run(context.Background(), scenario, actx2)

	// Both should have TempRoot set and they should differ.
	if actx1.TempRoot == "" {
		t.Fatal("run 1: TempRoot is empty")
	}
	if actx2.TempRoot == "" {
		t.Fatal("run 2: TempRoot is empty")
	}
	if actx1.TempRoot == actx2.TempRoot {
		t.Errorf("TempRoot should be unique per run: both = %q", actx1.TempRoot)
	}

	// __temp_dir var should be set.
	if actx1.Vars["__temp_dir"] != actx1.TempRoot {
		t.Errorf("__temp_dir = %q, want %q", actx1.Vars["__temp_dir"], actx1.TempRoot)
	}

	// Should contain scenario name.
	if !strings.Contains(actx1.TempRoot, "tempdir-test") {
		t.Errorf("TempRoot %q should contain scenario name", actx1.TempRoot)
	}
}

func TestEngine_TempRoot_PreservedIfSet(t *testing.T) {
	registry := NewRegistry()
	step := &mockHandler{}
	registry.Register("step", TierCore, step)

	scenario := &Scenario{
		Name:    "tempdir-preset-test",
		Timeout: Duration{5 * time.Second},
		Phases: []Phase{
			{
				Name:    "phase1",
				Actions: []Action{{Action: "step"}},
			},
		},
	}

	engine := NewEngine(registry, nil)
	actx := &ActionContext{
		Scenario: scenario,
		Vars:     make(map[string]string),
		Log:      func(string, ...interface{}) {},
		TempRoot: "/custom/temp/path",
	}
	engine.Run(context.Background(), scenario, actx)

	if actx.TempRoot != "/custom/temp/path" {
		t.Errorf("TempRoot = %q, want /custom/temp/path (should preserve caller-set value)", actx.TempRoot)
	}
}

func TestParse_AggregateValidation(t *testing.T) {
	base := `
name: validate-test
timeout: 5m
topology:
  nodes:
    node1:
      host: "127.0.0.1"
      is_local: true
targets:
  primary:
    node: node1
    iscsi_port: 3260
    admin_port: 8080
    iqn_suffix: test
phases:
  - name: bench
    repeat: 5
    aggregate: "%s"
    trim_pct: %d
    actions:
      - action: exec
        node: node1
        cmd: "echo 1"
`

	tests := []struct {
		name      string
		aggregate string
		trimPct   int
		wantErr   bool
	}{
		{"valid median", "median", 20, false},
		{"valid mean", "mean", 10, false},
		{"valid none", "none", 0, false},
		{"valid empty", "", 0, false},
		{"invalid aggregate", "invalid", 0, true},
		{"trim_pct too high", "median", 50, true},
		{"trim_pct negative", "median", -1, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			yaml := fmt.Sprintf(base, tt.aggregate, tt.trimPct)
			_, err := Parse([]byte(yaml))
			if tt.wantErr && err == nil {
				t.Error("expected error")
			}
			if !tt.wantErr && err != nil {
				t.Errorf("unexpected error: %v", err)
			}
		})
	}
}
