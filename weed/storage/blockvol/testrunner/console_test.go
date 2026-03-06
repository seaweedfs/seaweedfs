package testrunner

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestConsole_ScenariosEndpoint(t *testing.T) {
	dir := t.TempDir()
	writeTestScenario(t, dir, "smoke.yaml", "Smoke Test", 2)
	writeTestScenario(t, dir, "ha.yaml", "HA Test", 4)

	console, baseURL := startTestConsole(t, dir)
	_ = console

	resp, err := http.Get(baseURL + "/api/scenarios")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		t.Fatalf("status = %d", resp.StatusCode)
	}

	var scenarios []scenarioInfo
	json.NewDecoder(resp.Body).Decode(&scenarios)

	if len(scenarios) != 2 {
		t.Fatalf("scenarios = %d, want 2", len(scenarios))
	}
}

func TestConsole_StatusEndpoint_NoRun(t *testing.T) {
	dir := t.TempDir()
	_, baseURL := startTestConsole(t, dir)

	resp, err := http.Get(baseURL + "/api/status")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	var st statusResponse
	json.NewDecoder(resp.Body).Decode(&st)

	if st.Running {
		t.Error("should not be running")
	}
}

func TestConsole_RunEndpoint_MissingScenario(t *testing.T) {
	dir := t.TempDir()
	_, baseURL := startTestConsole(t, dir)

	resp, err := http.Post(baseURL+"/api/run", "application/json",
		strings.NewReader(`{"scenario":""}`))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != 400 {
		t.Errorf("status = %d, want 400", resp.StatusCode)
	}
}

func TestConsole_RunEndpoint_InvalidScenario(t *testing.T) {
	dir := t.TempDir()
	_, baseURL := startTestConsole(t, dir)

	resp, err := http.Post(baseURL+"/api/run", "application/json",
		strings.NewReader(`{"scenario":"nonexistent.yaml"}`))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != 400 {
		t.Errorf("status = %d, want 400", resp.StatusCode)
	}
}

func TestConsole_RunAndPollStatus(t *testing.T) {
	dir := t.TempDir()
	// Write a scenario with a registered action.
	writeTestScenarioWithAction(t, dir, "test.yaml", "Test Run", "console_noop")

	registry := NewRegistry()
	registry.RegisterFunc("console_noop", TierCore, func(ctx context.Context, actx *ActionContext, act Action) (map[string]string, error) {
		return map[string]string{"value": "done"}, nil
	})

	console, baseURL := startTestConsoleWithRegistry(t, dir, registry)
	_ = console

	// Run the scenario.
	resp, err := http.Post(baseURL+"/api/run", "application/json",
		strings.NewReader(`{"scenario":"test.yaml"}`))
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()

	if resp.StatusCode != 202 {
		t.Fatalf("status = %d, want 202", resp.StatusCode)
	}

	// Poll until done.
	deadline := time.After(5 * time.Second)
	for {
		select {
		case <-deadline:
			t.Fatal("timeout waiting for scenario to complete")
		case <-time.After(100 * time.Millisecond):
		}

		statusResp, err := http.Get(baseURL + "/api/status")
		if err != nil {
			continue
		}
		var st statusResponse
		json.NewDecoder(statusResp.Body).Decode(&st)
		statusResp.Body.Close()

		if !st.Running && st.Status != "" {
			if st.Status != "done" {
				t.Errorf("status = %s, want done", st.Status)
			}
			break
		}
	}

	// Check result endpoint.
	resultResp, err := http.Get(baseURL + "/api/result/test")
	if err != nil {
		t.Fatal(err)
	}
	defer resultResp.Body.Close()

	if resultResp.StatusCode != 200 {
		t.Fatalf("result status = %d", resultResp.StatusCode)
	}

	var result ScenarioResult
	json.NewDecoder(resultResp.Body).Decode(&result)
	if result.Status != StatusPass {
		t.Errorf("result.Status = %s, want PASS", result.Status)
	}
}

func TestConsole_ConflictOnDoubleRun(t *testing.T) {
	dir := t.TempDir()
	writeTestScenarioWithAction(t, dir, "slow.yaml", "Slow Test", "console_slow")

	registry := NewRegistry()
	registry.RegisterFunc("console_slow", TierCore, func(ctx context.Context, actx *ActionContext, act Action) (map[string]string, error) {
		time.Sleep(2 * time.Second)
		return nil, nil
	})

	_, baseURL := startTestConsoleWithRegistry(t, dir, registry)

	// Start first run.
	resp1, _ := http.Post(baseURL+"/api/run", "application/json",
		strings.NewReader(`{"scenario":"slow.yaml"}`))
	resp1.Body.Close()
	if resp1.StatusCode != 202 {
		t.Fatalf("first run status = %d", resp1.StatusCode)
	}

	// Wait a moment for it to start.
	time.Sleep(100 * time.Millisecond)

	// Try second run — should get 409.
	resp2, _ := http.Post(baseURL+"/api/run", "application/json",
		strings.NewReader(`{"scenario":"slow.yaml"}`))
	resp2.Body.Close()
	if resp2.StatusCode != 409 {
		t.Errorf("second run status = %d, want 409", resp2.StatusCode)
	}
}

func TestConsole_AgentsEndpoint(t *testing.T) {
	dir := t.TempDir()
	_, baseURL := startTestConsole(t, dir)

	resp, err := http.Get(baseURL + "/api/agents")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	var agents []agentInfo
	json.NewDecoder(resp.Body).Decode(&agents)

	// No agents connected by default.
	if len(agents) != 0 {
		t.Errorf("agents = %d, want 0", len(agents))
	}
}

func TestConsole_TiersEndpoint(t *testing.T) {
	dir := t.TempDir()
	registry := NewRegistry()
	registry.RegisterFunc("a", TierCore, func(ctx context.Context, actx *ActionContext, act Action) (map[string]string, error) {
		return nil, nil
	})
	registry.RegisterFunc("b", TierBlock, func(ctx context.Context, actx *ActionContext, act Action) (map[string]string, error) {
		return nil, nil
	})

	_, baseURL := startTestConsoleWithRegistry(t, dir, registry)

	resp, err := http.Get(baseURL + "/api/tiers")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	var tiers map[string][]string
	json.NewDecoder(resp.Body).Decode(&tiers)

	if len(tiers["core"]) != 1 || tiers["core"][0] != "a" {
		t.Errorf("core tier = %v", tiers["core"])
	}
	if len(tiers["block"]) != 1 || tiers["block"][0] != "b" {
		t.Errorf("block tier = %v", tiers["block"])
	}
}

func TestConsole_ReportNotFound(t *testing.T) {
	dir := t.TempDir()
	_, baseURL := startTestConsole(t, dir)

	resp, err := http.Get(baseURL + "/api/report/nonexistent")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != 404 {
		t.Errorf("status = %d, want 404", resp.StatusCode)
	}
}

func TestConsole_IndexPage(t *testing.T) {
	dir := t.TempDir()
	_, baseURL := startTestConsole(t, dir)

	resp, err := http.Get(baseURL + "/")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		t.Fatalf("status = %d", resp.StatusCode)
	}

	ct := resp.Header.Get("Content-Type")
	if !strings.Contains(ct, "text/html") {
		t.Errorf("content-type = %s, want text/html", ct)
	}
}

// --- Helpers ---

func startTestConsole(t *testing.T, scenariosDir string) (*Console, string) {
	t.Helper()
	return startTestConsoleWithRegistry(t, scenariosDir, NewRegistry())
}

func startTestConsoleWithRegistry(t *testing.T, scenariosDir string, registry *Registry) (*Console, string) {
	t.Helper()

	console := NewConsole(ConsoleConfig{
		Port:        0,
		ScenarioDir: scenariosDir,
		Registry:    registry,
	})

	// Override to use ephemeral port — bind directly.
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	go console.Start(ctx)

	// Wait for listener.
	for i := 0; i < 50; i++ {
		if console.ListenAddr() != "" {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}

	addr := console.ListenAddr()
	if addr == "" {
		t.Fatal("console failed to start")
	}

	return console, fmt.Sprintf("http://%s", addr)
}

func writeTestScenario(t *testing.T, dir, filename, name string, phases int) {
	t.Helper()
	content := fmt.Sprintf("name: %s\ntimeout: 30s\nphases:\n", name)
	for i := 0; i < phases; i++ {
		content += fmt.Sprintf("  - name: phase%d\n    actions:\n      - action: noop\n", i)
	}
	os.WriteFile(filepath.Join(dir, filename), []byte(content), 0644)
}

func writeTestScenarioWithAction(t *testing.T, dir, filename, name, action string) {
	t.Helper()
	content := fmt.Sprintf(`name: %s
timeout: 30s
phases:
  - name: main
    actions:
      - action: %s
        save_as: result
`, name, action)
	os.WriteFile(filepath.Join(dir, filename), []byte(content), 0644)
}
