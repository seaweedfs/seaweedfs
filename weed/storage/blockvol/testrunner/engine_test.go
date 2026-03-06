package testrunner

import (
	"context"
	"fmt"
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
