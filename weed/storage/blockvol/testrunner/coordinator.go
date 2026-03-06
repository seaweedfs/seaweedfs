package testrunner

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

// Coordinator manages multi-node test execution by dispatching phases to remote agents.
type Coordinator struct {
	Port    int
	Token   string
	DryRun  bool
	log     *log.Logger

	mu       sync.Mutex
	agents   map[string]*AgentInfo // agent name → info
	expected map[string]string     // agent name → "host:port" from topology
	server   *http.Server
	listener net.Listener
	ready    chan struct{} // closed when all agents registered

	registry *Registry // set during RunScenario for coordinator-local actions
}

// AgentInfo holds state for a registered agent.
type AgentInfo struct {
	RegisterRequest
	Addr     string // "host:port" for HTTP requests
	Healthy  bool
}

// CoordinatorConfig holds configuration for creating a Coordinator.
type CoordinatorConfig struct {
	Port     int
	Token    string
	DryRun   bool
	Expected map[string]string // agent name → "host:port"
	Logger   *log.Logger
}

// NewCoordinator creates a new Coordinator.
func NewCoordinator(cfg CoordinatorConfig) *Coordinator {
	logger := cfg.Logger
	if logger == nil {
		logger = log.New(os.Stderr, "[coord] ", log.LstdFlags)
	}
	return &Coordinator{
		Port:     cfg.Port,
		Token:    cfg.Token,
		DryRun:   cfg.DryRun,
		log:      logger,
		agents:   make(map[string]*AgentInfo),
		expected: cfg.Expected,
		ready:    make(chan struct{}),
	}
}

// Start begins listening for agent registrations.
func (c *Coordinator) Start() error {
	mux := http.NewServeMux()
	mux.HandleFunc("/register", c.handleRegister)

	addr := fmt.Sprintf(":%d", c.Port)
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("listen %s: %w", addr, err)
	}

	c.mu.Lock()
	c.listener = ln
	c.server = &http.Server{Handler: mux}
	c.mu.Unlock()

	c.log.Printf("coordinator listening on %s, expecting %d agents", addr, len(c.expected))

	go func() {
		if err := c.server.Serve(ln); err != nil && err != http.ErrServerClosed {
			c.log.Printf("serve error: %v", err)
		}
	}()

	return nil
}

// Stop shuts down the coordinator HTTP server.
func (c *Coordinator) Stop() {
	c.mu.Lock()
	srv := c.server
	c.mu.Unlock()

	if srv != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		srv.Shutdown(ctx)
	}
}

// ListenAddr returns the address the coordinator is listening on.
func (c *Coordinator) ListenAddr() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.listener != nil {
		return c.listener.Addr().String()
	}
	return ""
}

// WaitForAgents waits until all expected agents have registered.
func (c *Coordinator) WaitForAgents(ctx context.Context, timeout time.Duration) error {
	if len(c.expected) == 0 {
		return nil
	}

	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	select {
	case <-c.ready:
		return nil
	case <-ctx.Done():
		c.mu.Lock()
		registered := make([]string, 0, len(c.agents))
		for name := range c.agents {
			registered = append(registered, name)
		}
		missing := make([]string, 0)
		for name := range c.expected {
			if _, ok := c.agents[name]; !ok {
				missing = append(missing, name)
			}
		}
		c.mu.Unlock()
		return fmt.Errorf("timeout waiting for agents: registered=%v missing=%v", registered, missing)
	}
}

// RegisterAgent directly registers an agent (used by tests and manual setup).
func (c *Coordinator) RegisterAgent(name string, addr string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.agents[name] = &AgentInfo{
		RegisterRequest: RegisterRequest{AgentID: name, Hostname: name},
		Addr:            addr,
		Healthy:         true,
	}
	c.checkAllRegistered()
}

// POST /register
func (c *Coordinator) handleRegister(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	if c.Token != "" {
		token := r.Header.Get(AuthTokenHeader)
		if token != c.Token {
			writeJSON(w, http.StatusUnauthorized, RegisterResponse{Error: "invalid auth token"})
			return
		}
	}

	var req RegisterRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, RegisterResponse{Error: fmt.Sprintf("decode: %v", err)})
		return
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	// Match agent to expected agents by node names.
	agentName := ""
	for _, nodeName := range req.Nodes {
		if _, ok := c.expected[nodeName]; ok {
			agentName = nodeName
			break
		}
	}
	if agentName == "" && req.Hostname != "" {
		if _, ok := c.expected[req.Hostname]; ok {
			agentName = req.Hostname
		}
	}
	if agentName == "" {
		writeJSON(w, http.StatusBadRequest, RegisterResponse{Error: fmt.Sprintf("no matching agent for nodes=%v hostname=%s", req.Nodes, req.Hostname)})
		return
	}

	addr := fmt.Sprintf("%s:%d", req.IP, req.Port)
	c.agents[agentName] = &AgentInfo{
		RegisterRequest: req,
		Addr:            addr,
		Healthy:         true,
	}

	c.log.Printf("agent %q registered (id=%s, addr=%s, nodes=%v)", agentName, req.AgentID, addr, req.Nodes)

	resp := RegisterResponse{
		OK:          true,
		AgentIndex:  len(c.agents),
		TotalAgents: len(c.expected),
	}
	c.checkAllRegistered()

	writeJSON(w, http.StatusOK, resp)
}

func (c *Coordinator) checkAllRegistered() {
	for name := range c.expected {
		if _, ok := c.agents[name]; !ok {
			return
		}
	}
	// All registered — close the ready channel (once).
	select {
	case <-c.ready:
	default:
		close(c.ready)
	}
}

// RunScenario executes a scenario by dispatching phases to agents.
func (c *Coordinator) RunScenario(ctx context.Context, s *Scenario, registry *Registry) *ScenarioResult {
	c.registry = registry
	start := time.Now()
	result := &ScenarioResult{
		Name:   s.Name,
		Status: StatusPass,
	}

	// Apply scenario timeout.
	if s.Timeout.Duration > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, s.Timeout.Duration)
		defer cancel()
	}

	// Seed global vars from env.
	globalVars := make(map[string]string)
	for k, v := range s.Env {
		globalVars[k] = v
	}

	// Build node→agent mapping.
	nodeToAgent := c.buildNodeAgentMap(s)

	// Separate always-phases for deferred cleanup.
	var normalPhases, alwaysPhases []Phase
	for _, p := range s.Phases {
		if p.Always {
			alwaysPhases = append(alwaysPhases, p)
		} else {
			normalPhases = append(normalPhases, p)
		}
	}

	if c.DryRun {
		c.printDryRun(s, nodeToAgent, normalPhases, alwaysPhases)
		result.Duration = time.Since(start)
		return result
	}

	// Execute normal phases.
	failed := false
	for i, phase := range normalPhases {
		pr := c.runPhase(ctx, s, i, phase, globalVars, nodeToAgent)
		result.Phases = append(result.Phases, pr)
		if pr.Status == StatusFail {
			failed = true
			result.Status = StatusFail
			result.Error = fmt.Sprintf("phase %q failed: %s", phase.Name, pr.Error)
			break
		}
	}

	// Always-phases run on every agent regardless of failure.
	for i, phase := range alwaysPhases {
		pr := c.runPhase(ctx, s, len(normalPhases)+i, phase, globalVars, nodeToAgent)
		result.Phases = append(result.Phases, pr)
	}

	result.Duration = time.Since(start)
	if !failed {
		result.Status = StatusPass
	}

	// Preserve all final vars in the result for downstream reporting.
	if len(globalVars) > 0 {
		result.Vars = make(map[string]string, len(globalVars))
		for k, v := range globalVars {
			result.Vars[k] = v
		}
	}

	return result
}

// buildNodeAgentMap creates a mapping from node name → agent name.
func (c *Coordinator) buildNodeAgentMap(s *Scenario) map[string]string {
	m := make(map[string]string)
	for nodeName, nodeSpec := range s.Topology.Nodes {
		if nodeSpec.Agent != "" {
			m[nodeName] = nodeSpec.Agent
		} else {
			// Try to match by hostname in registered agents.
			c.mu.Lock()
			for agentName := range c.agents {
				if agentName == nodeName {
					m[nodeName] = agentName
					break
				}
			}
			c.mu.Unlock()
		}
	}
	return m
}

// resolveActionAgent determines which agent should execute an action.
func (c *Coordinator) resolveActionAgent(s *Scenario, act Action, nodeToAgent map[string]string) string {
	// 1. Explicit node reference.
	if act.Node != "" {
		if agent, ok := nodeToAgent[act.Node]; ok {
			return agent
		}
	}
	// 2. Target reference → target's node → agent.
	if act.Target != "" {
		if tSpec, ok := s.Targets[act.Target]; ok {
			if agent, ok := nodeToAgent[tSpec.Node]; ok {
				return agent
			}
		}
	}
	// 3. Fallback: first agent.
	c.mu.Lock()
	defer c.mu.Unlock()
	for name := range c.agents {
		return name
	}
	return ""
}

// runPhase executes a single phase across agents.
func (c *Coordinator) runPhase(ctx context.Context, s *Scenario, phaseIdx int, phase Phase, globalVars map[string]string, nodeToAgent map[string]string) PhaseResult {
	start := time.Now()
	c.log.Printf("[phase %d] %s (%d actions)", phaseIdx, phase.Name, len(phase.Actions))

	var pr PhaseResult
	if phase.Parallel {
		pr = c.runPhaseParallel(ctx, s, phaseIdx, phase, globalVars, nodeToAgent)
	} else {
		pr = c.runPhaseSequential(ctx, s, phaseIdx, phase, globalVars, nodeToAgent)
	}

	pr.Name = phase.Name
	pr.Duration = time.Since(start)
	return pr
}

// runPhaseSequential dispatches actions one by one, merging vars between them.
func (c *Coordinator) runPhaseSequential(ctx context.Context, s *Scenario, phaseIdx int, phase Phase, globalVars map[string]string, nodeToAgent map[string]string) PhaseResult {
	pr := PhaseResult{Status: StatusPass}

	// Check if all actions target the same agent, none have retries, and none are
	// coordinator-local — batch optimization.
	hasRetry := false
	hasLocal := false
	for _, act := range phase.Actions {
		if act.Retry > 0 {
			hasRetry = true
		}
		if c.isCoordinatorLocalAction(act) {
			hasLocal = true
		}
	}
	if !hasRetry && !hasLocal {
		if agent := c.allSameAgent(s, phase.Actions, nodeToAgent); agent != "" {
			return c.dispatchBatch(ctx, s, phaseIdx, phase, globalVars, agent)
		}
	}

	// Otherwise, dispatch one action at a time.
	for i, act := range phase.Actions {
		// Coordinator-local actions: run on coordinator, not dispatched to agents.
		if c.isCoordinatorLocalAction(act) {
			ar := c.runCoordinatorLocalAction(ctx, s, act, globalVars)
			pr.Actions = append(pr.Actions, ar)
			if ar.Status == StatusFail && !act.IgnoreError {
				pr.Status = StatusFail
				pr.Error = fmt.Sprintf("action %d (%s) failed: %s", i, act.Action, ar.Error)
				return pr
			}
			continue
		}

		agentName := c.resolveActionAgent(s, act, nodeToAgent)
		if agentName == "" {
			ar := ActionResult{
				Action: act.Action,
				Status: StatusFail,
				Error:  "no agent found for action",
			}
			pr.Actions = append(pr.Actions, ar)
			if !act.IgnoreError {
				pr.Status = StatusFail
				pr.Error = fmt.Sprintf("action %d (%s): no agent", i, act.Action)
				return pr
			}
			continue
		}

		ar := c.dispatchSingleAction(ctx, s, phaseIdx, phase.Name, act, globalVars, agentName)
		pr.Actions = append(pr.Actions, ar)

		if ar.Status == StatusFail && !act.IgnoreError {
			pr.Status = StatusFail
			pr.Error = fmt.Sprintf("action %d (%s) failed: %s", i, act.Action, ar.Error)
			return pr
		}
	}

	return pr
}

// runPhaseParallel dispatches all actions to their agents in parallel.
func (c *Coordinator) runPhaseParallel(ctx context.Context, s *Scenario, phaseIdx int, phase Phase, globalVars map[string]string, nodeToAgent map[string]string) PhaseResult {
	pr := PhaseResult{Status: StatusPass}

	// Group actions by agent.
	type agentWork struct {
		agent  string
		actions []Action
	}
	groups := make(map[string]*agentWork)
	for _, act := range phase.Actions {
		agentName := c.resolveActionAgent(s, act, nodeToAgent)
		if agentName == "" {
			agentName = "__unresolved"
		}
		if _, ok := groups[agentName]; !ok {
			groups[agentName] = &agentWork{agent: agentName}
		}
		groups[agentName].actions = append(groups[agentName].actions, act)
	}

	type groupResult struct {
		agent string
		resp  *PhaseResponse
		err   error
	}

	results := make(chan groupResult, len(groups))
	for _, g := range groups {
		go func(aw *agentWork) {
			if aw.agent == "__unresolved" {
				results <- groupResult{agent: aw.agent, err: fmt.Errorf("no agent for actions")}
				return
			}
			resp, err := c.sendPhaseRequest(ctx, aw.agent, &PhaseRequest{
				PhaseIndex: phaseIdx,
				PhaseName:  phase.Name,
				Actions:    aw.actions,
				GlobalVars: globalVars,
				Scenario:   s,
			})
			results <- groupResult{agent: aw.agent, resp: resp, err: err}
		}(g)
	}

	for range groups {
		gr := <-results
		if gr.err != nil {
			pr.Status = StatusFail
			if pr.Error == "" {
				pr.Error = fmt.Sprintf("agent %s: %v", gr.agent, gr.err)
			}
			continue
		}
		if gr.resp != nil {
			pr.Actions = append(pr.Actions, gr.resp.Results...)
			// Merge new vars.
			for k, v := range gr.resp.NewVars {
				globalVars[k] = v
			}
			if gr.resp.Error != "" {
				pr.Status = StatusFail
				if pr.Error == "" {
					pr.Error = fmt.Sprintf("agent %s: %s", gr.agent, gr.resp.Error)
				}
			}
		}
	}

	return pr
}

// allSameAgent returns the agent name if all actions target the same agent, or "".
func (c *Coordinator) allSameAgent(s *Scenario, actions []Action, nodeToAgent map[string]string) string {
	if len(actions) == 0 {
		return ""
	}
	first := c.resolveActionAgent(s, actions[0], nodeToAgent)
	if first == "" {
		return ""
	}
	for _, act := range actions[1:] {
		if c.resolveActionAgent(s, act, nodeToAgent) != first {
			return ""
		}
	}
	return first
}

// dispatchBatch sends an entire phase to a single agent.
func (c *Coordinator) dispatchBatch(ctx context.Context, s *Scenario, phaseIdx int, phase Phase, globalVars map[string]string, agent string) PhaseResult {
	pr := PhaseResult{Status: StatusPass}

	resp, err := c.sendPhaseRequest(ctx, agent, &PhaseRequest{
		PhaseIndex: phaseIdx,
		PhaseName:  phase.Name,
		Actions:    phase.Actions,
		GlobalVars: globalVars,
		Scenario:   s,
	})

	if err != nil {
		pr.Status = StatusFail
		pr.Error = fmt.Sprintf("agent %s: %v", agent, err)
		return pr
	}

	pr.Actions = resp.Results
	for k, v := range resp.NewVars {
		globalVars[k] = v
	}
	if resp.Error != "" {
		pr.Status = StatusFail
		pr.Error = resp.Error
	}
	return pr
}

// dispatchSingleAction sends a single action to an agent with retry support.
func (c *Coordinator) dispatchSingleAction(ctx context.Context, s *Scenario, phaseIdx int, phaseName string, act Action, globalVars map[string]string, agentName string) ActionResult {
	maxRetries := act.Retry
	var actionTimeout time.Duration
	if act.Timeout != "" {
		if d, err := time.ParseDuration(act.Timeout); err == nil {
			actionTimeout = d
		}
	}

	var lastResult ActionResult

	for attempt := 0; attempt <= maxRetries; attempt++ {
		if attempt > 0 {
			c.log.Printf("  [retry] action %s attempt %d/%d", act.Action, attempt+1, maxRetries+1)
			// Backoff: 2s between retries.
			select {
			case <-time.After(2 * time.Second):
			case <-ctx.Done():
				return ActionResult{Action: act.Action, Status: StatusFail, Error: ctx.Err().Error()}
			}
		}

		reqCtx := ctx
		if actionTimeout > 0 {
			var cancel context.CancelFunc
			reqCtx, cancel = context.WithTimeout(ctx, actionTimeout+5*time.Second) // buffer
			defer cancel()
		}

		resp, err := c.sendPhaseRequest(reqCtx, agentName, &PhaseRequest{
			PhaseIndex: phaseIdx,
			PhaseName:  phaseName,
			Actions:    []Action{act},
			GlobalVars: globalVars,
			Scenario:   s,
		})

		if err != nil {
			lastResult = ActionResult{Action: act.Action, Status: StatusFail, Error: fmt.Sprintf("agent %s: %v", agentName, err)}
			continue
		}

		// Merge vars regardless of status.
		for k, v := range resp.NewVars {
			globalVars[k] = v
		}

		if len(resp.Results) > 0 {
			lastResult = resp.Results[0]
		} else {
			lastResult = ActionResult{Action: act.Action, Status: StatusPass}
		}

		if resp.Error == "" && lastResult.Status != StatusFail {
			return lastResult
		}

		if resp.Error != "" && lastResult.Error == "" {
			lastResult.Error = resp.Error
		}
	}

	return lastResult
}

// sendPhaseRequest sends a POST /phase to an agent and returns the response.
func (c *Coordinator) sendPhaseRequest(ctx context.Context, agentName string, req *PhaseRequest) (*PhaseResponse, error) {
	c.mu.Lock()
	agent, ok := c.agents[agentName]
	c.mu.Unlock()
	if !ok {
		return nil, fmt.Errorf("agent %q not registered", agentName)
	}

	body, err := json.Marshal(req)
	if err != nil {
		return nil, fmt.Errorf("marshal phase request: %w", err)
	}

	url := fmt.Sprintf("http://%s/phase", agent.Addr)
	httpReq, err := http.NewRequestWithContext(ctx, "POST", url, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	httpReq.Header.Set("Content-Type", "application/json")
	if c.Token != "" {
		httpReq.Header.Set(AuthTokenHeader, c.Token)
	}

	resp, err := http.DefaultClient.Do(httpReq)
	if err != nil {
		return nil, fmt.Errorf("POST %s: %w", url, err)
	}
	defer resp.Body.Close()

	var phaseResp PhaseResponse
	if err := json.NewDecoder(resp.Body).Decode(&phaseResp); err != nil {
		return nil, fmt.Errorf("decode response from %s: %w", agentName, err)
	}

	return &phaseResp, nil
}

// UploadToAgent sends a file to an agent via POST /upload.
func (c *Coordinator) UploadToAgent(ctx context.Context, agentName, localPath, remotePath string) error {
	c.mu.Lock()
	agent, ok := c.agents[agentName]
	c.mu.Unlock()
	if !ok {
		return fmt.Errorf("agent %q not registered", agentName)
	}

	f, err := os.Open(localPath)
	if err != nil {
		return fmt.Errorf("open %s: %w", localPath, err)
	}
	defer f.Close()

	url := fmt.Sprintf("http://%s/upload?path=%s", agent.Addr, remotePath)
	httpReq, err := http.NewRequestWithContext(ctx, "POST", url, f)
	if err != nil {
		return err
	}
	httpReq.Header.Set("Content-Type", "application/octet-stream")
	if c.Token != "" {
		httpReq.Header.Set(AuthTokenHeader, c.Token)
	}

	resp, err := http.DefaultClient.Do(httpReq)
	if err != nil {
		return fmt.Errorf("upload to %s: %w", agentName, err)
	}
	defer resp.Body.Close()

	var uploadResp UploadResponse
	if err := json.NewDecoder(resp.Body).Decode(&uploadResp); err != nil {
		return fmt.Errorf("decode upload response: %w", err)
	}
	if !uploadResp.OK {
		return fmt.Errorf("upload rejected: %s", uploadResp.Error)
	}

	c.log.Printf("uploaded %s to %s:%s (%d bytes)", localPath, agentName, remotePath, uploadResp.Size)
	return nil
}

// ExecOnAgent runs a command on an agent via POST /exec.
func (c *Coordinator) ExecOnAgent(ctx context.Context, agentName string, cmd string, root bool) (*ExecResponse, error) {
	c.mu.Lock()
	agent, ok := c.agents[agentName]
	c.mu.Unlock()
	if !ok {
		return nil, fmt.Errorf("agent %q not registered", agentName)
	}

	reqBody, _ := json.Marshal(ExecRequest{Cmd: cmd, Root: root})
	url := fmt.Sprintf("http://%s/exec", agent.Addr)

	httpReq, err := http.NewRequestWithContext(ctx, "POST", url, bytes.NewReader(reqBody))
	if err != nil {
		return nil, err
	}
	httpReq.Header.Set("Content-Type", "application/json")
	if c.Token != "" {
		httpReq.Header.Set(AuthTokenHeader, c.Token)
	}

	resp, err := http.DefaultClient.Do(httpReq)
	if err != nil {
		return nil, fmt.Errorf("exec on %s: %w", agentName, err)
	}
	defer resp.Body.Close()

	var execResp ExecResponse
	if err := json.NewDecoder(resp.Body).Decode(&execResp); err != nil {
		return nil, fmt.Errorf("decode exec response: %w", err)
	}
	return &execResp, nil
}

// HealthCheck checks if an agent is reachable.
func (c *Coordinator) HealthCheck(ctx context.Context, agentName string) (*HealthResponse, error) {
	c.mu.Lock()
	agent, ok := c.agents[agentName]
	c.mu.Unlock()
	if !ok {
		return nil, fmt.Errorf("agent %q not registered", agentName)
	}

	url := fmt.Sprintf("http://%s/health", agent.Addr)
	httpReq, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return nil, err
	}

	resp, err := http.DefaultClient.Do(httpReq)
	if err != nil {
		return nil, fmt.Errorf("health check %s: %w", agentName, err)
	}
	defer resp.Body.Close()

	var hr HealthResponse
	body, _ := io.ReadAll(resp.Body)
	if err := json.Unmarshal(body, &hr); err != nil {
		return nil, fmt.Errorf("decode health response: %w", err)
	}
	return &hr, nil
}

// DownloadArtifacts downloads artifacts from an agent's directory as a tar.gz stream,
// extracts them into localDir/agentName/, and returns the collected entries.
func (c *Coordinator) DownloadArtifacts(ctx context.Context, agentName, remoteDir, localDir string) ([]ArtifactEntry, error) {
	c.mu.Lock()
	agent, ok := c.agents[agentName]
	c.mu.Unlock()
	if !ok {
		return nil, fmt.Errorf("agent %q not registered", agentName)
	}

	url := fmt.Sprintf("http://%s/artifacts?dir=%s", agent.Addr, remoteDir)
	httpReq, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return nil, err
	}
	if c.Token != "" {
		httpReq.Header.Set(AuthTokenHeader, c.Token)
	}

	resp, err := http.DefaultClient.Do(httpReq)
	if err != nil {
		return nil, fmt.Errorf("GET artifacts from %s: %w", agentName, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("artifacts from %s: HTTP %d: %s", agentName, resp.StatusCode, string(body))
	}

	// Extract tar.gz into localDir/agentName/.
	destDir := filepath.Join(localDir, agentName)
	if err := os.MkdirAll(destDir, 0755); err != nil {
		return nil, fmt.Errorf("mkdir %s: %w", destDir, err)
	}

	gr, err := gzip.NewReader(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("gzip reader: %w", err)
	}
	defer gr.Close()

	tr := tar.NewReader(gr)
	var entries []ArtifactEntry

	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return entries, fmt.Errorf("tar read: %w", err)
		}

		// Safety: reject absolute paths and traversals.
		name := filepath.FromSlash(hdr.Name)
		if strings.Contains(name, "..") || filepath.IsAbs(name) {
			continue
		}

		target := filepath.Join(destDir, name)
		if hdr.Typeflag == tar.TypeDir {
			os.MkdirAll(target, 0755)
			continue
		}

		// Ensure parent dir exists.
		os.MkdirAll(filepath.Dir(target), 0755)

		f, err := os.Create(target)
		if err != nil {
			c.log.Printf("create %s: %v", target, err)
			continue
		}
		n, _ := io.Copy(f, tr)
		f.Close()

		entries = append(entries, ArtifactEntry{
			Agent: agentName,
			Path:  target,
			Size:  n,
		})
	}

	c.log.Printf("downloaded %d artifacts from %s to %s", len(entries), agentName, destDir)
	return entries, nil
}

// DownloadAllArtifacts downloads artifacts from all agents and appends entries to the result.
func (c *Coordinator) DownloadAllArtifacts(ctx context.Context, remoteDir, localDir string, result *ScenarioResult) {
	c.mu.Lock()
	names := make([]string, 0, len(c.agents))
	for name := range c.agents {
		names = append(names, name)
	}
	c.mu.Unlock()

	for _, name := range names {
		entries, err := c.DownloadArtifacts(ctx, name, remoteDir, localDir)
		if err != nil {
			c.log.Printf("artifact download from %s: %v", name, err)
			continue
		}
		result.Artifacts = append(result.Artifacts, entries...)
	}
}

// printDryRun prints the execution plan without executing.
func (c *Coordinator) printDryRun(s *Scenario, nodeToAgent map[string]string, normalPhases, alwaysPhases []Phase) {
	fmt.Fprintf(os.Stdout, "\n=== DRY RUN: %s ===\n\n", s.Name)
	fmt.Fprintf(os.Stdout, "Agents:\n")
	c.mu.Lock()
	for name, info := range c.agents {
		fmt.Fprintf(os.Stdout, "  %s → %s (nodes=%v)\n", name, info.Addr, info.Nodes)
	}
	c.mu.Unlock()

	fmt.Fprintf(os.Stdout, "\nNode→Agent mapping:\n")
	for node, agent := range nodeToAgent {
		fmt.Fprintf(os.Stdout, "  %s → %s\n", node, agent)
	}

	printPhases := func(label string, phases []Phase) {
		if len(phases) == 0 {
			return
		}
		fmt.Fprintf(os.Stdout, "\n%s:\n", label)
		for i, phase := range phases {
			mode := "sequential"
			if phase.Parallel {
				mode = "parallel"
			}
			fmt.Fprintf(os.Stdout, "  Phase %d: %s (%s, %d actions)\n", i, phase.Name, mode, len(phase.Actions))
			for j, act := range phase.Actions {
				agent := c.resolveActionAgent(s, act, nodeToAgent)
				fmt.Fprintf(os.Stdout, "    [%d] %s → agent=%s", j, act.Action, agent)
				if act.Target != "" {
					fmt.Fprintf(os.Stdout, " target=%s", act.Target)
				}
				if act.Node != "" {
					fmt.Fprintf(os.Stdout, " node=%s", act.Node)
				}
				if act.SaveAs != "" {
					fmt.Fprintf(os.Stdout, " save_as=%s", act.SaveAs)
				}
				fmt.Fprintln(os.Stdout)
			}
		}
	}

	printPhases("Normal Phases", normalPhases)
	printPhases("Always Phases (cleanup)", alwaysPhases)

	fmt.Fprintln(os.Stdout, "\n=== END DRY RUN ===")
}

// AgentNames returns the names of all registered agents (for use by build_deploy).
func (c *Coordinator) AgentNames() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	names := make([]string, 0, len(c.agents))
	for name := range c.agents {
		names = append(names, name)
	}
	return names
}

// IsAgentRegistered returns whether an agent is registered.
func (c *Coordinator) IsAgentRegistered(name string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	_, ok := c.agents[name]
	return ok
}

// RegisteredAgentCount returns the number of registered agents.
func (c *Coordinator) RegisteredAgentCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.agents)
}

// coordinatorLocalActions lists actions that run on the coordinator itself,
// not dispatched to agents (e.g., cross-compile + upload).
var coordinatorLocalActions = map[string]bool{
	"build_deploy":      true,
	"build_deploy_weed": true,
}

// isCoordinatorLocalAction returns true if the action should run on the
// coordinator rather than being dispatched to an agent.
func (c *Coordinator) isCoordinatorLocalAction(act Action) bool {
	return coordinatorLocalActions[act.Action]
}

// runCoordinatorLocalAction executes an action locally on the coordinator,
// using the action registry. Returns an ActionResult.
func (c *Coordinator) runCoordinatorLocalAction(ctx context.Context, s *Scenario, act Action, globalVars map[string]string) ActionResult {
	start := time.Now()
	ar := ActionResult{
		Action: act.Action,
		Status: StatusPass,
	}

	if c.registry == nil {
		ar.Status = StatusFail
		ar.Error = "no registry available for coordinator-local action"
		ar.Duration = time.Since(start)
		return ar
	}

	handler, err := c.registry.Get(act.Action)
	if err != nil {
		ar.Status = StatusFail
		ar.Error = fmt.Sprintf("registry lookup: %v", err)
		ar.Duration = time.Since(start)
		return ar
	}

	// Build an ActionContext for coordinator-local execution.
	actx := &ActionContext{
		Scenario:    s,
		Nodes:       make(map[string]NodeRunner),
		Targets:     make(map[string]TargetRunner),
		Vars:        make(map[string]string),
		Coordinator: c,
		Log:         func(format string, args ...interface{}) { c.log.Printf(format, args...) },
	}
	// Copy global vars.
	for k, v := range globalVars {
		actx.Vars[k] = v
	}

	// Resolve variable references in action params.
	resolved := resolveAction(act, actx.Vars)

	c.log.Printf("  [coordinator-local] %s", resolved.Action)

	vars, err := handler.Execute(ctx, actx, resolved)
	if err != nil {
		ar.Status = StatusFail
		ar.Error = err.Error()
		if act.IgnoreError {
			ar.Status = StatusPass
			c.log.Printf("  [coordinator-local] %s failed (ignored): %v", resolved.Action, err)
		}
	}

	// Merge produced vars back to global.
	for k, v := range vars {
		globalVars[k] = v
	}

	ar.Duration = time.Since(start)
	return ar
}
