package testrunner

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"testing"
	"time"
)

// newTestCoordinator creates a coordinator on a random port for testing.
func newTestCoordinator(t *testing.T, expected map[string]string) *Coordinator {
	t.Helper()
	c := NewCoordinator(CoordinatorConfig{
		Port:     0, // random
		Token:    "test-token",
		Expected: expected,
	})
	if err := c.Start(); err != nil {
		t.Fatalf("start coordinator: %v", err)
	}
	t.Cleanup(func() { c.Stop() })
	return c
}

func TestCoordinator_RegisterAgent(t *testing.T) {
	c := newTestCoordinator(t, map[string]string{"agent1": "127.0.0.1:9100"})

	c.RegisterAgent("agent1", "127.0.0.1:9100")

	if !c.IsAgentRegistered("agent1") {
		t.Error("agent1 should be registered")
	}
	if c.IsAgentRegistered("agent2") {
		t.Error("agent2 should not be registered")
	}
	if c.RegisteredAgentCount() != 1 {
		t.Errorf("count = %d, want 1", c.RegisteredAgentCount())
	}
}

func TestCoordinator_WaitForAgents_AlreadyRegistered(t *testing.T) {
	c := newTestCoordinator(t, map[string]string{"a1": "host:9100"})
	c.RegisterAgent("a1", "host:9100")

	ctx := context.Background()
	if err := c.WaitForAgents(ctx, 1*time.Second); err != nil {
		t.Fatalf("WaitForAgents: %v", err)
	}
}

func TestCoordinator_WaitForAgents_Timeout(t *testing.T) {
	c := newTestCoordinator(t, map[string]string{"a1": "host:9100", "a2": "host:9200"})
	c.RegisterAgent("a1", "host:9100")
	// a2 never registers

	ctx := context.Background()
	err := c.WaitForAgents(ctx, 100*time.Millisecond)
	if err == nil {
		t.Fatal("expected timeout error")
	}
}

func TestCoordinator_WaitForAgents_NoExpected(t *testing.T) {
	c := newTestCoordinator(t, map[string]string{})
	ctx := context.Background()
	if err := c.WaitForAgents(ctx, 100*time.Millisecond); err != nil {
		t.Fatalf("WaitForAgents with empty expected: %v", err)
	}
}

func TestCoordinator_BuildNodeAgentMap(t *testing.T) {
	c := newTestCoordinator(t, map[string]string{"tp01": "host:9100"})
	c.RegisterAgent("tp01", "host:9100")

	s := &Scenario{
		Topology: Topology{
			Agents: map[string]string{"tp01": "host:9100"},
			Nodes: map[string]NodeSpec{
				"node1": {Host: "host", Agent: "tp01"},
				"node2": {Host: "host2"},
			},
		},
	}

	m := c.buildNodeAgentMap(s)
	if m["node1"] != "tp01" {
		t.Errorf("node1 → %q, want tp01", m["node1"])
	}
}

func TestCoordinator_ResolveActionAgent(t *testing.T) {
	c := newTestCoordinator(t, map[string]string{"tp01": "h:9100", "m01": "h:9200"})
	c.RegisterAgent("tp01", "h:9100")
	c.RegisterAgent("m01", "h:9200")

	s := &Scenario{
		Targets: map[string]TargetSpec{
			"primary": {Node: "node_tp01"},
		},
		Topology: Topology{
			Nodes: map[string]NodeSpec{
				"node_tp01":  {Agent: "tp01"},
				"node_m01":   {Agent: "m01"},
			},
		},
	}
	nodeToAgent := map[string]string{"node_tp01": "tp01", "node_m01": "m01"}

	// By node.
	agent := c.resolveActionAgent(s, Action{Node: "node_tp01"}, nodeToAgent)
	if agent != "tp01" {
		t.Errorf("by node: got %q, want tp01", agent)
	}

	// By target.
	agent = c.resolveActionAgent(s, Action{Target: "primary"}, nodeToAgent)
	if agent != "tp01" {
		t.Errorf("by target: got %q, want tp01", agent)
	}

	// Fallback.
	agent = c.resolveActionAgent(s, Action{}, nodeToAgent)
	if agent == "" {
		t.Error("fallback should return some agent")
	}
}

// --- Integration: Coordinator + Agent ---

func TestCoordinatorAgent_PhaseDispatch(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}

	// Create a test action that returns a value.
	registry := NewRegistry()
	registry.RegisterFunc("echo_test", TierCore, func(ctx context.Context, actx *ActionContext, act Action) (map[string]string, error) {
		msg := act.Params["msg"]
		if msg == "" {
			msg = "default"
		}
		return map[string]string{"value": msg}, nil
	})
	registry.RegisterFunc("fail_test", TierCore, func(ctx context.Context, actx *ActionContext, act Action) (map[string]string, error) {
		return nil, fmt.Errorf("intentional failure")
	})

	// Start agent.
	agent := NewAgent(AgentConfig{
		Port:     0,
		Token:    "tok",
		Nodes:    []string{"test_node"},
		Registry: registry,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	go agent.Start(ctx)
	// Wait for listener.
	for i := 0; i < 50; i++ {
		if agent.ListenAddr() != "" {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if agent.ListenAddr() == "" {
		t.Fatal("agent didn't start")
	}

	// Start coordinator.
	coord := NewCoordinator(CoordinatorConfig{
		Port:     0,
		Token:    "tok",
		Expected: map[string]string{"test_node": agent.ListenAddr()},
	})
	if err := coord.Start(); err != nil {
		t.Fatalf("start coordinator: %v", err)
	}
	defer coord.Stop()

	// Register agent.
	coord.RegisterAgent("test_node", agent.ListenAddr())

	// Define scenario.
	s := &Scenario{
		Name:    "test-dispatch",
		Timeout: Duration{30 * time.Second},
		Env:     map[string]string{"greeting": "hello"},
		Topology: Topology{
			Agents: map[string]string{"test_node": agent.ListenAddr()},
			Nodes:  map[string]NodeSpec{"test_node": {Host: "127.0.0.1", Agent: "test_node"}},
		},
		Phases: []Phase{
			{
				Name: "echo_phase",
				Actions: []Action{
					{Action: "echo_test", Node: "test_node", SaveAs: "echo_result", Params: map[string]string{"msg": "world"}},
				},
			},
		},
	}

	result := coord.RunScenario(ctx, s, registry)
	if result.Status != StatusPass {
		t.Fatalf("scenario failed: %s", result.Error)
	}
	if len(result.Phases) != 1 {
		t.Fatalf("expected 1 phase, got %d", len(result.Phases))
	}
	if len(result.Phases[0].Actions) != 1 {
		t.Fatalf("expected 1 action result, got %d", len(result.Phases[0].Actions))
	}
	if result.Phases[0].Actions[0].Status != StatusPass {
		t.Errorf("action status = %s", result.Phases[0].Actions[0].Status)
	}
}

func TestCoordinatorAgent_VarMerge(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}

	// Two actions: first produces a var, second uses it.
	registry := NewRegistry()
	callCount := 0
	registry.RegisterFunc("produce_var", TierCore, func(ctx context.Context, actx *ActionContext, act Action) (map[string]string, error) {
		callCount++
		return map[string]string{"value": fmt.Sprintf("produced-%d", callCount)}, nil
	})
	registry.RegisterFunc("check_var", TierCore, func(ctx context.Context, actx *ActionContext, act Action) (map[string]string, error) {
		expected := act.Params["expected"]
		got := act.Params["actual"]
		if got != expected {
			return nil, fmt.Errorf("var mismatch: got %q, want %q", got, expected)
		}
		return map[string]string{"value": "ok"}, nil
	})

	agent := NewAgent(AgentConfig{Port: 0, Token: "tok", Nodes: []string{"n1"}, Registry: registry})
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	go agent.Start(ctx)
	for i := 0; i < 50; i++ {
		if agent.ListenAddr() != "" {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}

	coord := NewCoordinator(CoordinatorConfig{Port: 0, Token: "tok", Expected: map[string]string{"n1": agent.ListenAddr()}})
	coord.Start()
	defer coord.Stop()
	coord.RegisterAgent("n1", agent.ListenAddr())

	s := &Scenario{
		Name:    "var-merge",
		Timeout: Duration{30 * time.Second},
		Topology: Topology{
			Agents: map[string]string{"n1": agent.ListenAddr()},
			Nodes:  map[string]NodeSpec{"n1": {Host: "127.0.0.1", Agent: "n1"}},
		},
		Phases: []Phase{
			{
				Name: "produce",
				Actions: []Action{
					{Action: "produce_var", Node: "n1", SaveAs: "my_var"},
				},
			},
			{
				Name: "consume",
				Actions: []Action{
					{Action: "check_var", Node: "n1", Params: map[string]string{"expected": "produced-1", "actual": "{{ my_var }}"}},
				},
			},
		},
	}

	result := coord.RunScenario(ctx, s, registry)
	if result.Status != StatusPass {
		t.Fatalf("scenario failed: %s", result.Error)
	}
}

func TestCoordinatorAgent_FailureStopsNormalPhases(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}

	registry := NewRegistry()
	registry.RegisterFunc("pass_action", TierCore, func(ctx context.Context, actx *ActionContext, act Action) (map[string]string, error) {
		return nil, nil
	})
	registry.RegisterFunc("fail_action", TierCore, func(ctx context.Context, actx *ActionContext, act Action) (map[string]string, error) {
		return nil, fmt.Errorf("boom")
	})

	agent := NewAgent(AgentConfig{Port: 0, Token: "tok", Nodes: []string{"n1"}, Registry: registry})
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	go agent.Start(ctx)
	for i := 0; i < 50; i++ {
		if agent.ListenAddr() != "" {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}

	coord := NewCoordinator(CoordinatorConfig{Port: 0, Token: "tok", Expected: map[string]string{"n1": agent.ListenAddr()}})
	coord.Start()
	defer coord.Stop()
	coord.RegisterAgent("n1", agent.ListenAddr())

	s := &Scenario{
		Name:    "fail-test",
		Timeout: Duration{30 * time.Second},
		Topology: Topology{
			Agents: map[string]string{"n1": agent.ListenAddr()},
			Nodes:  map[string]NodeSpec{"n1": {Host: "127.0.0.1", Agent: "n1"}},
		},
		Phases: []Phase{
			{Name: "p1", Actions: []Action{{Action: "fail_action", Node: "n1"}}},
			{Name: "p2", Actions: []Action{{Action: "pass_action", Node: "n1"}}}, // should be skipped
			{Name: "cleanup", Always: true, Actions: []Action{{Action: "pass_action", Node: "n1"}}},
		},
	}

	result := coord.RunScenario(ctx, s, registry)
	if result.Status != StatusFail {
		t.Fatal("expected FAIL")
	}
	// p1 (failed) + cleanup (always) = 2 phases, p2 skipped.
	if len(result.Phases) != 2 {
		t.Errorf("expected 2 phases (p1+cleanup), got %d", len(result.Phases))
	}
}

func TestCoordinatorAgent_RetryOnFailure(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}

	registry := NewRegistry()
	callCount := 0
	registry.RegisterFunc("flaky_action", TierCore, func(ctx context.Context, actx *ActionContext, act Action) (map[string]string, error) {
		callCount++
		if callCount < 3 {
			return nil, fmt.Errorf("flaky error %d", callCount)
		}
		return map[string]string{"value": "success"}, nil
	})

	agent := NewAgent(AgentConfig{Port: 0, Token: "tok", Nodes: []string{"n1"}, Registry: registry})
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	go agent.Start(ctx)
	for i := 0; i < 50; i++ {
		if agent.ListenAddr() != "" {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}

	coord := NewCoordinator(CoordinatorConfig{Port: 0, Token: "tok", Expected: map[string]string{"n1": agent.ListenAddr()}})
	coord.Start()
	defer coord.Stop()
	coord.RegisterAgent("n1", agent.ListenAddr())

	s := &Scenario{
		Name:    "retry-test",
		Timeout: Duration{30 * time.Second},
		Topology: Topology{
			Agents: map[string]string{"n1": agent.ListenAddr()},
			Nodes:  map[string]NodeSpec{"n1": {Host: "127.0.0.1", Agent: "n1"}},
		},
		Phases: []Phase{
			{Name: "p1", Actions: []Action{{Action: "flaky_action", Node: "n1", Retry: 3}}},
		},
	}

	result := coord.RunScenario(ctx, s, registry)
	if result.Status != StatusPass {
		t.Fatalf("expected PASS after retries, got %s: %s", result.Status, result.Error)
	}
}

func TestCoordinatorAgent_VarsInResult(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}

	registry := NewRegistry()
	registry.RegisterFunc("produce", TierCore, func(ctx context.Context, actx *ActionContext, act Action) (map[string]string, error) {
		return map[string]string{"value": "produced-value"}, nil
	})

	agent := NewAgent(AgentConfig{Port: 0, Token: "tok", Nodes: []string{"n1"}, Registry: registry})
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	go agent.Start(ctx)
	for i := 0; i < 50; i++ {
		if agent.ListenAddr() != "" {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}

	coord := NewCoordinator(CoordinatorConfig{Port: 0, Token: "tok", Expected: map[string]string{"n1": agent.ListenAddr()}})
	coord.Start()
	defer coord.Stop()
	coord.RegisterAgent("n1", agent.ListenAddr())

	s := &Scenario{
		Name:    "vars-result",
		Timeout: Duration{30 * time.Second},
		Env:     map[string]string{"env_key": "env_val"},
		Topology: Topology{
			Agents: map[string]string{"n1": agent.ListenAddr()},
			Nodes:  map[string]NodeSpec{"n1": {Host: "127.0.0.1", Agent: "n1"}},
		},
		Phases: []Phase{
			{Name: "p1", Actions: []Action{{Action: "produce", Node: "n1", SaveAs: "out_var"}}},
		},
	}

	result := coord.RunScenario(ctx, s, registry)
	if result.Status != StatusPass {
		t.Fatalf("expected PASS, got %s: %s", result.Status, result.Error)
	}

	// Verify vars are in the result.
	if result.Vars == nil {
		t.Fatal("result.Vars is nil")
	}
	if result.Vars["out_var"] != "produced-value" {
		t.Errorf("result.Vars[out_var] = %q, want produced-value", result.Vars["out_var"])
	}
	if result.Vars["env_key"] != "env_val" {
		t.Errorf("result.Vars[env_key] = %q, want env_val", result.Vars["env_key"])
	}
}

func TestCoordinatorAgent_DryRun(t *testing.T) {
	coord := NewCoordinator(CoordinatorConfig{
		Port:     0,
		DryRun:   true,
		Expected: map[string]string{"n1": "h:9100"},
	})
	coord.Start()
	defer coord.Stop()
	coord.RegisterAgent("n1", "h:9100")

	s := &Scenario{
		Name: "dry-run-test",
		Topology: Topology{
			Agents: map[string]string{"n1": "h:9100"},
			Nodes:  map[string]NodeSpec{"n1": {Host: "h", Agent: "n1"}},
		},
		Phases: []Phase{
			{Name: "p1", Actions: []Action{{Action: "echo", Node: "n1"}}},
		},
	}

	result := coord.RunScenario(context.Background(), s, NewRegistry())
	// Dry run should always pass (no execution).
	if result.Status != StatusPass {
		t.Errorf("dry run status = %s", result.Status)
	}
}

func TestCoordinator_RegisterTokenValidation(t *testing.T) {
	// Coordinator with token should reject unauthenticated /register.
	c := NewCoordinator(CoordinatorConfig{
		Port:     0,
		Token:    "secret",
		Expected: map[string]string{"n1": "h:9100"},
	})
	if err := c.Start(); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer c.Stop()

	regReq := RegisterRequest{
		AgentID:  "a1",
		Hostname: "h",
		IP:       "127.0.0.1",
		Port:     9100,
		Nodes:    []string{"n1"},
	}
	body, _ := json.Marshal(regReq)

	// No token → 401.
	resp, err := http.Post("http://"+c.ListenAddr()+"/register", "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("POST /register: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Errorf("no token: expected 401, got %d", resp.StatusCode)
	}

	// Wrong token → 401.
	req, _ := http.NewRequest("POST", "http://"+c.ListenAddr()+"/register", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set(AuthTokenHeader, "wrong")
	resp2, _ := http.DefaultClient.Do(req)
	resp2.Body.Close()
	if resp2.StatusCode != http.StatusUnauthorized {
		t.Errorf("wrong token: expected 401, got %d", resp2.StatusCode)
	}

	// Correct token → 200.
	req3, _ := http.NewRequest("POST", "http://"+c.ListenAddr()+"/register", bytes.NewReader(body))
	req3.Header.Set("Content-Type", "application/json")
	req3.Header.Set(AuthTokenHeader, "secret")
	resp3, _ := http.DefaultClient.Do(req3)
	defer resp3.Body.Close()
	if resp3.StatusCode != http.StatusOK {
		t.Errorf("correct token: expected 200, got %d", resp3.StatusCode)
	}
	var rr RegisterResponse
	json.NewDecoder(resp3.Body).Decode(&rr)
	if !rr.OK {
		t.Errorf("registration should succeed with correct token, error: %s", rr.Error)
	}
}

func TestCoordinator_RegisterNoToken(t *testing.T) {
	// Coordinator without token should accept any /register.
	c := NewCoordinator(CoordinatorConfig{
		Port:     0,
		Token:    "", // no token
		Expected: map[string]string{"n1": "h:9100"},
	})
	if err := c.Start(); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer c.Stop()

	regReq := RegisterRequest{
		AgentID:  "a1",
		Hostname: "h",
		IP:       "127.0.0.1",
		Port:     9100,
		Nodes:    []string{"n1"},
	}
	body, _ := json.Marshal(regReq)

	resp, err := http.Post("http://"+c.ListenAddr()+"/register", "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("POST /register: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("expected 200 without token requirement, got %d", resp.StatusCode)
	}
}

func TestAgent_PersistentReRegistration(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}

	registry := NewRegistry()
	registry.RegisterFunc("noop", TierCore, func(ctx context.Context, actx *ActionContext, act Action) (map[string]string, error) {
		return nil, nil
	})

	// Start persistent agent.
	agent := NewAgent(AgentConfig{
		Port:       0,
		Token:      "tok",
		Persistent: true,
		Nodes:      []string{"n1"},
		Registry:   registry,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	go agent.Start(ctx)
	for i := 0; i < 50; i++ {
		if agent.ListenAddr() != "" {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if agent.ListenAddr() == "" {
		t.Fatal("agent didn't start")
	}

	// Start first coordinator — agent should register.
	coord1 := NewCoordinator(CoordinatorConfig{
		Port:     0,
		Token:    "tok",
		Expected: map[string]string{"n1": agent.ListenAddr()},
	})
	// Update agent's coordinator URL to point at coord1.
	agent.CoordinatorURL = "http://" + coord1.ListenAddr()
	// Since agent is already running with persistent loop pointing to original URL,
	// we need a different approach: start coordinator first, then the agent connects.

	// Actually, let's restart: stop agent, start coord, then start agent pointing at it.
	cancel()
	time.Sleep(100 * time.Millisecond)

	// Start coordinator 1.
	coord1 = NewCoordinator(CoordinatorConfig{
		Port:     0,
		Token:    "tok",
		Expected: map[string]string{"n1": "will-be-set"},
	})
	if err := coord1.Start(); err != nil {
		t.Fatalf("start coord1: %v", err)
	}

	// Start persistent agent pointing at coord1.
	ctx2, cancel2 := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel2()

	agent2 := NewAgent(AgentConfig{
		Port:           0,
		CoordinatorURL: "http://" + coord1.ListenAddr(),
		Token:          "tok",
		Persistent:     true,
		Nodes:          []string{"n1"},
		Registry:       registry,
	})
	go agent2.Start(ctx2)
	for i := 0; i < 50; i++ {
		if agent2.ListenAddr() != "" {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}

	// Wait for registration.
	if err := coord1.WaitForAgents(ctx2, 10*time.Second); err != nil {
		t.Fatalf("agent didn't register with coord1: %v", err)
	}
	if !coord1.IsAgentRegistered("n1") {
		t.Fatal("n1 not registered on coord1")
	}

	// Stop coord1, start coord2 on the SAME port.
	addr := coord1.ListenAddr()
	coord1.Stop()
	time.Sleep(200 * time.Millisecond)

	// Extract port from addr.
	var port int
	fmt.Sscanf(addr, "[::]:%d", &port)
	if port == 0 {
		fmt.Sscanf(addr, "0.0.0.0:%d", &port)
	}
	if port == 0 {
		fmt.Sscanf(addr, "127.0.0.1:%d", &port)
	}

	coord2 := NewCoordinator(CoordinatorConfig{
		Port:     port,
		Token:    "tok",
		Expected: map[string]string{"n1": agent2.ListenAddr()},
	})
	if err := coord2.Start(); err != nil {
		t.Fatalf("start coord2: %v", err)
	}
	defer coord2.Stop()

	// Agent should re-register with coord2 within ~10s.
	if err := coord2.WaitForAgents(ctx2, 12*time.Second); err != nil {
		t.Fatalf("agent didn't re-register with coord2: %v", err)
	}
	if !coord2.IsAgentRegistered("n1") {
		t.Fatal("n1 not registered on coord2 after re-registration")
	}
}
