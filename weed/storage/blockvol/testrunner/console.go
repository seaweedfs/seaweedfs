package testrunner

import (
	"context"
	"encoding/json"
	"fmt"
	"html/template"
	"log"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"
)

// Console serves a web UI for interactive scenario execution.
type Console struct {
	Port        int
	Token       string
	ScenarioDir string
	Registry    *Registry
	Log         *log.Logger

	coordinator *Coordinator
	mu          sync.Mutex
	running     bool
	currentRun  *runState
	results     map[string]*ScenarioResult
	server      *http.Server
	listener    net.Listener
}

type runState struct {
	ScenarioName string    `json:"scenario"`
	StartedAt    time.Time `json:"started_at"`
	Status       string    `json:"status"` // "running", "done", "failed"
	Result       *ScenarioResult
}

// ConsoleConfig holds configuration for creating a Console.
type ConsoleConfig struct {
	Port        int
	Token       string
	ScenarioDir string
	Registry    *Registry
	Logger      *log.Logger
}

// NewConsole creates a new Console server.
func NewConsole(cfg ConsoleConfig) *Console {
	logger := cfg.Logger
	if logger == nil {
		logger = log.New(os.Stderr, "[console] ", log.LstdFlags)
	}

	// Create an internal coordinator for agent management.
	coord := NewCoordinator(CoordinatorConfig{
		Port:     cfg.Port + 1, // agents register on port+1
		Token:    cfg.Token,
		Expected: make(map[string]string), // no expected agents by default
		Logger:   log.New(os.Stderr, "[coord] ", log.LstdFlags),
	})

	return &Console{
		Port:        cfg.Port,
		Token:       cfg.Token,
		ScenarioDir: cfg.ScenarioDir,
		Registry:    cfg.Registry,
		Log:         logger,
		coordinator: coord,
		results:     make(map[string]*ScenarioResult),
	}
}

// Start begins serving the console web UI. Blocks until stopped.
func (c *Console) Start(ctx context.Context) error {
	mux := http.NewServeMux()
	mux.HandleFunc("/", c.handleIndex)
	mux.HandleFunc("/api/scenarios", c.handleScenarios)
	mux.HandleFunc("/api/run", c.handleRun)
	mux.HandleFunc("/api/status", c.handleStatus)
	mux.HandleFunc("/api/result/", c.handleResult)
	mux.HandleFunc("/api/report/", c.handleReport)
	mux.HandleFunc("/api/agents", c.handleAgents)
	mux.HandleFunc("/api/tiers", c.handleTiers)
	mux.HandleFunc("/register", c.coordinator.handleRegister)

	addr := fmt.Sprintf(":%d", c.Port)
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("listen %s: %w", addr, err)
	}

	c.mu.Lock()
	c.listener = ln
	c.server = &http.Server{Handler: mux}
	c.mu.Unlock()

	c.Log.Printf("console listening on http://localhost:%d", c.Port)
	c.Log.Printf("scenarios dir: %s", c.ScenarioDir)

	go func() {
		<-ctx.Done()
		c.Stop()
	}()

	if err := c.server.Serve(ln); err != nil && err != http.ErrServerClosed {
		return err
	}
	return nil
}

// Stop gracefully shuts down the console server.
func (c *Console) Stop() {
	c.mu.Lock()
	srv := c.server
	c.mu.Unlock()

	if srv != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		srv.Shutdown(ctx)
	}
}

// ListenAddr returns the address the console is listening on.
func (c *Console) ListenAddr() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.listener != nil {
		return c.listener.Addr().String()
	}
	return ""
}

// --- API Handlers ---

type scenarioInfo struct {
	Name   string `json:"name"`
	File   string `json:"file"`
	Phases int    `json:"phases"`
}

// GET /api/scenarios
func (c *Console) handleScenarios(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	files, err := filepath.Glob(filepath.Join(c.ScenarioDir, "*.yaml"))
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}

	var scenarios []scenarioInfo
	for _, f := range files {
		s, err := ParseFile(f)
		if err != nil {
			continue // skip invalid files
		}
		scenarios = append(scenarios, scenarioInfo{
			Name:   s.Name,
			File:   filepath.Base(f),
			Phases: len(s.Phases),
		})
	}

	sort.Slice(scenarios, func(i, j int) bool { return scenarios[i].File < scenarios[j].File })
	writeJSON(w, http.StatusOK, scenarios)
}

type runRequest struct {
	Scenario string `json:"scenario"`
}

// POST /api/run
func (c *Console) handleRun(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req runRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": fmt.Sprintf("decode: %v", err)})
		return
	}

	if req.Scenario == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "scenario field required"})
		return
	}

	c.mu.Lock()
	if c.running {
		c.mu.Unlock()
		writeJSON(w, http.StatusConflict, map[string]string{"error": "a scenario is already running"})
		return
	}
	c.running = true
	c.currentRun = &runState{
		ScenarioName: req.Scenario,
		StartedAt:    time.Now(),
		Status:       "running",
	}
	c.mu.Unlock()

	// Parse and validate.
	scenarioPath := filepath.Join(c.ScenarioDir, req.Scenario)
	scenario, err := ParseFile(scenarioPath)
	if err != nil {
		c.mu.Lock()
		c.running = false
		c.currentRun = nil
		c.mu.Unlock()
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": fmt.Sprintf("parse: %v", err)})
		return
	}

	// Launch in background.
	go c.executeScenario(scenario, req.Scenario)

	writeJSON(w, http.StatusAccepted, map[string]string{"status": "started", "scenario": req.Scenario})
}

func (c *Console) executeScenario(scenario *Scenario, fileName string) {
	ctx := context.Background()
	if scenario.Timeout.Duration > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, scenario.Timeout.Duration)
		defer cancel()
	}

	logFunc := func(format string, args ...interface{}) {
		c.Log.Printf(format, args...)
	}

	engine := NewEngine(c.Registry, logFunc)
	actx := &ActionContext{
		Scenario: scenario,
		Nodes:    make(map[string]NodeRunner),
		Targets:  make(map[string]TargetRunner),
		Vars:     make(map[string]string),
		Log:      logFunc,
	}

	result := engine.Run(ctx, scenario, actx)

	name := strings.TrimSuffix(fileName, ".yaml")
	c.mu.Lock()
	c.results[name] = result
	if c.currentRun != nil {
		c.currentRun.Status = "done"
		if result.Status == StatusFail {
			c.currentRun.Status = "failed"
		}
		c.currentRun.Result = result
	}
	c.running = false
	c.mu.Unlock()

	c.Log.Printf("scenario %s completed: %s (%s)", fileName, result.Status, result.Duration)
}

type statusResponse struct {
	Running  bool   `json:"running"`
	Scenario string `json:"scenario,omitempty"`
	Status   string `json:"status,omitempty"`
	Elapsed  string `json:"elapsed,omitempty"`
}

// GET /api/status
func (c *Console) handleStatus(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	resp := statusResponse{Running: c.running}
	if c.currentRun != nil {
		resp.Scenario = c.currentRun.ScenarioName
		resp.Status = c.currentRun.Status
		resp.Elapsed = time.Since(c.currentRun.StartedAt).Round(time.Second).String()
	}

	writeJSON(w, http.StatusOK, resp)
}

// GET /api/result/{name}
func (c *Console) handleResult(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	name := strings.TrimPrefix(r.URL.Path, "/api/result/")
	if name == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "name required"})
		return
	}

	c.mu.Lock()
	result, ok := c.results[name]
	c.mu.Unlock()
	if !ok {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "no result for " + name})
		return
	}

	writeJSON(w, http.StatusOK, result)
}

// GET /api/report/{name}
func (c *Console) handleReport(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	name := strings.TrimPrefix(r.URL.Path, "/api/report/")
	if name == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "name required"})
		return
	}

	c.mu.Lock()
	result, ok := c.results[name]
	c.mu.Unlock()
	if !ok {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "no result for " + name})
		return
	}

	// Render HTML report inline.
	data := buildHTMLData(result)
	tmpl, err := template.New("report").Parse(htmlTemplate)
	if err != nil {
		http.Error(w, "template error: "+err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	tmpl.Execute(w, data)
}

type agentInfo struct {
	Name    string `json:"name"`
	Addr    string `json:"addr"`
	Healthy bool   `json:"healthy"`
}

// GET /api/agents
func (c *Console) handleAgents(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	c.coordinator.mu.Lock()
	var agents []agentInfo
	for name, info := range c.coordinator.agents {
		agents = append(agents, agentInfo{
			Name:    name,
			Addr:    info.Addr,
			Healthy: info.Healthy,
		})
	}
	c.coordinator.mu.Unlock()

	sort.Slice(agents, func(i, j int) bool { return agents[i].Name < agents[j].Name })
	writeJSON(w, http.StatusOK, agents)
}

// GET /api/tiers
func (c *Console) handleTiers(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	writeJSON(w, http.StatusOK, c.Registry.ListByTier())
}

// GET /
func (c *Console) handleIndex(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" {
		http.NotFound(w, r)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Write([]byte(consoleSPA))
}

// consoleSPA is the embedded single-page application.
var consoleSPA = strings.TrimSpace(`<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<title>sw-test-runner Console</title>
<style>
  * { margin: 0; padding: 0; box-sizing: border-box; }
  body { font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, monospace; background: #1a1a2e; color: #e0e0e0; display: flex; flex-direction: column; height: 100vh; }
  header { background: #16213e; padding: 12px 24px; display: flex; align-items: center; gap: 16px; border-bottom: 1px solid #2a2a4a; }
  header h1 { font-size: 1.2em; color: #a0a0c0; }
  .main { display: flex; flex: 1; overflow: hidden; }
  .sidebar { width: 280px; border-right: 1px solid #2a2a4a; overflow-y: auto; padding: 12px; }
  .content { flex: 1; display: flex; flex-direction: column; }
  .scenario-item { padding: 8px 12px; margin: 4px 0; border-radius: 4px; cursor: pointer; border: 1px solid transparent; }
  .scenario-item:hover { background: #2a2a4a; }
  .scenario-item.selected { border-color: #4a9eff; background: #1a2a4a; }
  .scenario-name { font-weight: bold; font-size: 0.9em; }
  .scenario-meta { font-size: 0.75em; color: #888; margin-top: 2px; }
  .toolbar { padding: 12px 24px; background: #16213e; border-bottom: 1px solid #2a2a4a; display: flex; align-items: center; gap: 12px; }
  button { background: #2d6a4f; color: #b7e4c7; border: none; padding: 8px 16px; border-radius: 4px; cursor: pointer; font-size: 0.85em; font-weight: bold; }
  button:hover { background: #3d7a5f; }
  button:disabled { background: #333; color: #666; cursor: not-allowed; }
  .status-text { font-size: 0.85em; color: #a0a0c0; }
  .badge { display: inline-block; padding: 2px 8px; border-radius: 3px; font-size: 0.7em; font-weight: bold; }
  .badge-core { background: #2d6a4f; color: #b7e4c7; }
  .badge-block { background: #4a4a8a; color: #b0b0e0; }
  .badge-devops { background: #7d6608; color: #f9e79f; }
  .badge-chaos { background: #922b21; color: #f5b7b1; }
  .report-frame { flex: 1; border: none; background: #1a1a2e; }
  .placeholder { flex: 1; display: flex; align-items: center; justify-content: center; color: #555; font-size: 1.1em; }
  footer { background: #16213e; padding: 8px 24px; border-top: 1px solid #2a2a4a; font-size: 0.75em; color: #666; display: flex; gap: 16px; }
  .agent-badge { color: #2d6a4f; }
  .agent-badge.offline { color: #922b21; }
</style>
</head>
<body>
<header>
  <h1>sw-test-runner</h1>
  <span style="color:#666;font-size:0.8em">Console</span>
</header>
<div class="main">
  <div class="sidebar" id="sidebar">
    <div style="color:#888;font-size:0.8em;margin-bottom:8px">SCENARIOS</div>
    <div id="scenario-list">Loading...</div>
  </div>
  <div class="content">
    <div class="toolbar">
      <button id="run-btn" disabled onclick="runScenario()">Run</button>
      <span class="status-text" id="status-text"></span>
    </div>
    <div id="report-container" class="placeholder">
      Select a scenario and click Run
    </div>
  </div>
</div>
<footer>
  <span id="agent-status">Agents: checking...</span>
  <span id="tier-info"></span>
</footer>
<script>
let selected = null;
let polling = null;

async function loadScenarios() {
  try {
    const resp = await fetch('/api/scenarios');
    const data = await resp.json();
    const list = document.getElementById('scenario-list');
    if (!data || data.length === 0) {
      list.innerHTML = '<div style="color:#666">No scenarios found</div>';
      return;
    }
    list.innerHTML = '';
    data.forEach(s => {
      const div = document.createElement('div');
      div.className = 'scenario-item';
      div.innerHTML = '<div class="scenario-name">' + s.name + '</div>' +
        '<div class="scenario-meta">' + s.file + ' &middot; ' + s.phases + ' phases</div>';
      div.onclick = () => selectScenario(s, div);
      list.appendChild(div);
    });
  } catch (e) {
    document.getElementById('scenario-list').textContent = 'Error: ' + e.message;
  }
}

function selectScenario(s, el) {
  document.querySelectorAll('.scenario-item').forEach(e => e.classList.remove('selected'));
  el.classList.add('selected');
  selected = s;
  document.getElementById('run-btn').disabled = false;
}

async function runScenario() {
  if (!selected) return;
  const btn = document.getElementById('run-btn');
  btn.disabled = true;
  document.getElementById('status-text').textContent = 'Starting ' + selected.file + '...';
  document.getElementById('report-container').innerHTML = '<div class="placeholder">Running...</div>';

  try {
    const resp = await fetch('/api/run', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({scenario: selected.file})
    });
    if (!resp.ok) {
      const err = await resp.json();
      document.getElementById('status-text').textContent = 'Error: ' + (err.error || resp.statusText);
      btn.disabled = false;
      return;
    }
    startPolling();
  } catch (e) {
    document.getElementById('status-text').textContent = 'Error: ' + e.message;
    btn.disabled = false;
  }
}

function startPolling() {
  if (polling) clearInterval(polling);
  polling = setInterval(async () => {
    try {
      const resp = await fetch('/api/status');
      const data = await resp.json();
      if (data.running) {
        document.getElementById('status-text').textContent = data.scenario + ' — ' + data.status + ' (' + data.elapsed + ')';
      } else {
        clearInterval(polling);
        polling = null;
        document.getElementById('status-text').textContent = data.scenario + ' — ' + data.status + (data.elapsed ? ' (' + data.elapsed + ')' : '');
        document.getElementById('run-btn').disabled = false;
        if (data.scenario) {
          const name = data.scenario.replace('.yaml', '');
          document.getElementById('report-container').innerHTML = '<iframe class="report-frame" src="/api/report/' + name + '"></iframe>';
        }
      }
    } catch (e) { /* ignore polling errors */ }
  }, 2000);
}

async function loadAgents() {
  try {
    const resp = await fetch('/api/agents');
    const data = await resp.json();
    const el = document.getElementById('agent-status');
    if (!data || data.length === 0) {
      el.textContent = 'Agents: none connected';
    } else {
      el.innerHTML = 'Agents: ' + data.map(a =>
        '<span class="agent-badge' + (a.healthy ? '' : ' offline') + '">' + a.name + '</span>'
      ).join(' ');
    }
  } catch (e) {
    document.getElementById('agent-status').textContent = 'Agents: error';
  }
}

async function loadTiers() {
  try {
    const resp = await fetch('/api/tiers');
    const data = await resp.json();
    let count = 0;
    for (const tier in data) count += data[tier].length;
    document.getElementById('tier-info').textContent = count + ' actions registered';
  } catch (e) { /* ignore */ }
}

// Check if a run was already in progress.
async function checkStatus() {
  try {
    const resp = await fetch('/api/status');
    const data = await resp.json();
    if (data.running) {
      document.getElementById('status-text').textContent = data.scenario + ' — ' + data.status + ' (' + data.elapsed + ')';
      document.getElementById('run-btn').disabled = true;
      startPolling();
    }
  } catch (e) { /* ignore */ }
}

loadScenarios();
loadAgents();
loadTiers();
checkStatus();
setInterval(loadAgents, 10000);
</script>
</body>
</html>`)
