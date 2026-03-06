package testrunner

import (
	"archive/tar"
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

	"github.com/seaweedfs/seaweedfs/weed/storage/blockvol/testrunner/infra"
)

// Agent runs on each test node, executing actions locally on behalf of the coordinator.
type Agent struct {
	ID            string
	Hostname      string
	Port          int
	CoordinatorURL string
	Token         string
	AllowExec     bool
	Persistent    bool     // stay running across coordinator restarts
	Nodes         []string // node names this agent handles

	registry  *Registry
	localNode *LocalNode
	startTime time.Time
	log       *log.Logger

	mu       sync.Mutex
	server   *http.Server
	listener net.Listener
}

// AgentConfig holds configuration for creating an Agent.
type AgentConfig struct {
	Hostname       string
	Port           int
	CoordinatorURL string
	Token          string
	AllowExec      bool
	Persistent     bool // stay running, re-register with coordinator
	Nodes          []string
	Registry       *Registry
	Logger         *log.Logger
}

// NewAgent creates a new Agent with the given configuration.
func NewAgent(cfg AgentConfig) *Agent {
	hostname := cfg.Hostname
	if hostname == "" {
		hostname, _ = os.Hostname()
	}

	agentID := fmt.Sprintf("agent-%s-%d", hostname, os.Getpid())

	logger := cfg.Logger
	if logger == nil {
		logger = log.New(os.Stderr, "[agent] ", log.LstdFlags)
	}

	return &Agent{
		ID:             agentID,
		Hostname:       hostname,
		Port:           cfg.Port,
		CoordinatorURL: cfg.CoordinatorURL,
		Token:          cfg.Token,
		AllowExec:      cfg.AllowExec,
		Persistent:     cfg.Persistent,
		Nodes:          cfg.Nodes,
		registry:       cfg.Registry,
		localNode:      NewLocalNode(hostname),
		startTime:      time.Now(),
		log:            logger,
	}
}

// Start begins listening for coordinator requests. Blocks until the server is shut down.
func (a *Agent) Start(ctx context.Context) error {
	mux := http.NewServeMux()
	mux.HandleFunc("/health", a.handleHealth)
	mux.HandleFunc("/phase", a.authMiddleware(a.handlePhase))
	mux.HandleFunc("/upload", a.authMiddleware(a.handleUpload))
	mux.HandleFunc("/artifacts", a.authMiddleware(a.handleArtifacts))
	mux.HandleFunc("/exec", a.authMiddleware(a.handleExec))

	addr := fmt.Sprintf(":%d", a.Port)
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("listen %s: %w", addr, err)
	}

	a.mu.Lock()
	a.listener = ln
	a.server = &http.Server{Handler: mux}
	a.mu.Unlock()

	a.log.Printf("agent %s listening on %s (root=%v, exec=%v)", a.ID, addr, a.localNode.IsRoot(), a.AllowExec)

	// Register with coordinator if URL is set.
	if a.CoordinatorURL != "" {
		if a.Persistent {
			go a.registrationLoop(ctx)
		} else {
			go a.registerWithCoordinator()
		}
	}

	// Shutdown on context cancellation.
	go func() {
		<-ctx.Done()
		a.Stop()
	}()

	if err := a.server.Serve(ln); err != nil && err != http.ErrServerClosed {
		return err
	}
	return nil
}

// Stop gracefully shuts down the agent HTTP server.
func (a *Agent) Stop() {
	a.mu.Lock()
	srv := a.server
	a.mu.Unlock()

	if srv != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		srv.Shutdown(ctx)
	}
}

// ListenAddr returns the address the agent is listening on (useful in tests).
func (a *Agent) ListenAddr() string {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.listener != nil {
		return a.listener.Addr().String()
	}
	return ""
}

func (a *Agent) registerWithCoordinator() {
	req := RegisterRequest{
		AgentID:  a.ID,
		Hostname: a.Hostname,
		IP:       a.detectIP(),
		Port:     a.Port,
		Nodes:    a.Nodes,
	}

	// Detect capabilities.
	var caps []string
	if a.localNode.IsRoot() {
		caps = append(caps, "root")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, _, code, err := a.localNode.Run(ctx, "which iscsiadm"); err == nil && code == 0 {
		caps = append(caps, "iscsi")
	}
	if _, _, code, err := a.localNode.Run(ctx, "which fio"); err == nil && code == 0 {
		caps = append(caps, "fio")
	}
	req.Capabilities = caps

	body, _ := json.Marshal(req)
	url := strings.TrimRight(a.CoordinatorURL, "/") + "/register"

	// Retry registration up to 60s with 2s intervals.
	deadline := time.Now().Add(60 * time.Second)
	for attempt := 1; ; attempt++ {
		httpReq, _ := http.NewRequest("POST", url, strings.NewReader(string(body)))
		httpReq.Header.Set("Content-Type", "application/json")
		if a.Token != "" {
			httpReq.Header.Set(AuthTokenHeader, a.Token)
		}

		resp, err := http.DefaultClient.Do(httpReq)
		if err != nil {
			if time.Now().After(deadline) {
				a.log.Printf("register failed after retries: %v", err)
				return
			}
			a.log.Printf("register attempt %d failed: %v (retrying...)", attempt, err)
			time.Sleep(2 * time.Second)
			continue
		}

		var rr RegisterResponse
		json.NewDecoder(resp.Body).Decode(&rr)
		resp.Body.Close()
		if rr.OK {
			a.log.Printf("registered with coordinator (index=%d, total=%d)", rr.AgentIndex, rr.TotalAgents)
		} else {
			a.log.Printf("registration rejected: %s", rr.Error)
		}
		return
	}
}

// registrationLoop continuously re-registers with the coordinator.
// Used in persistent mode so the agent survives coordinator restarts.
func (a *Agent) registrationLoop(ctx context.Context) {
	req := a.buildRegisterRequest()
	body, _ := json.Marshal(req)
	url := strings.TrimRight(a.CoordinatorURL, "/") + "/register"

	registered := false
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		httpReq, _ := http.NewRequestWithContext(ctx, "POST", url, strings.NewReader(string(body)))
		httpReq.Header.Set("Content-Type", "application/json")
		if a.Token != "" {
			httpReq.Header.Set(AuthTokenHeader, a.Token)
		}

		resp, err := http.DefaultClient.Do(httpReq)
		if err != nil {
			if registered {
				a.log.Printf("coordinator unreachable, will retry: %v", err)
				registered = false
			}
			sleepCtx(ctx, 5*time.Second)
			continue
		}

		var rr RegisterResponse
		json.NewDecoder(resp.Body).Decode(&rr)
		resp.Body.Close()

		if rr.OK {
			if !registered {
				a.log.Printf("registered with coordinator (index=%d, total=%d)", rr.AgentIndex, rr.TotalAgents)
				registered = true
			}
			sleepCtx(ctx, 10*time.Second)
		} else {
			a.log.Printf("registration rejected: %s", rr.Error)
			sleepCtx(ctx, 5*time.Second)
		}
	}
}

// buildRegisterRequest creates the registration payload with capability detection.
func (a *Agent) buildRegisterRequest() RegisterRequest {
	req := RegisterRequest{
		AgentID:  a.ID,
		Hostname: a.Hostname,
		IP:       a.detectIP(),
		Port:     a.Port,
		Nodes:    a.Nodes,
	}
	var caps []string
	if a.localNode.IsRoot() {
		caps = append(caps, "root")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, _, code, err := a.localNode.Run(ctx, "which iscsiadm"); err == nil && code == 0 {
		caps = append(caps, "iscsi")
	}
	if _, _, code, err := a.localNode.Run(ctx, "which fio"); err == nil && code == 0 {
		caps = append(caps, "fio")
	}
	req.Capabilities = caps
	return req
}

// sleepCtx sleeps for d or until ctx is cancelled.
func sleepCtx(ctx context.Context, d time.Duration) {
	select {
	case <-time.After(d):
	case <-ctx.Done():
	}
}

func (a *Agent) detectIP() string {
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		return ""
	}
	for _, addr := range addrs {
		if ipNet, ok := addr.(*net.IPNet); ok && !ipNet.IP.IsLoopback() && ipNet.IP.To4() != nil {
			return ipNet.IP.String()
		}
	}
	return ""
}

// authMiddleware wraps a handler with token authentication.
func (a *Agent) authMiddleware(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if a.Token != "" {
			token := r.Header.Get(AuthTokenHeader)
			if token != a.Token {
				writeJSON(w, http.StatusUnauthorized, map[string]string{"error": "invalid auth token"})
				return
			}
		}
		next(w, r)
	}
}

// GET /health
func (a *Agent) handleHealth(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	resp := HealthResponse{
		OK:       true,
		AgentID:  a.ID,
		UptimeS:  int64(time.Since(a.startTime).Seconds()),
		HasRoot:  a.localNode.IsRoot(),
		Hostname: a.Hostname,
	}
	writeJSON(w, http.StatusOK, resp)
}

// POST /phase
func (a *Agent) handlePhase(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req PhaseRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, PhaseResponse{Error: fmt.Sprintf("decode: %v", err)})
		return
	}

	a.log.Printf("phase %d (%s): %d actions", req.PhaseIndex, req.PhaseName, len(req.Actions))

	resp := a.executePhase(r.Context(), &req)
	writeJSON(w, http.StatusOK, resp)
}

// executePhase runs actions sequentially using the local engine.
func (a *Agent) executePhase(ctx context.Context, req *PhaseRequest) PhaseResponse {
	resp := PhaseResponse{
		PhaseIndex: req.PhaseIndex,
		NewVars:    make(map[string]string),
	}

	// Build action context.
	actx := &ActionContext{
		Scenario: req.Scenario,
		Nodes:    make(map[string]NodeRunner),
		Targets:  make(map[string]TargetRunner),
		Vars:     make(map[string]string),
		Log:      func(format string, args ...interface{}) { a.log.Printf(format, args...) },
	}

	// Copy global vars.
	for k, v := range req.GlobalVars {
		actx.Vars[k] = v
	}

	// Create infra.Node in native mode for local command execution.
	nativeNode := &infra.Node{IsNative: true}

	// Map agent's nodes to native node runner.
	for _, nodeName := range a.Nodes {
		actx.Nodes[nodeName] = nativeNode
	}

	// Set up targets for nodes this agent handles.
	myNodes := make(map[string]bool)
	for _, n := range a.Nodes {
		myNodes[n] = true
	}
	if req.Scenario != nil {
		// Map node names to agents via topology.
		nodeToAgentName := make(map[string]string)
		for nodeName, nodeSpec := range req.Scenario.Topology.Nodes {
			nodeToAgentName[nodeName] = nodeSpec.Agent
		}
		for tgtName, tgtSpec := range req.Scenario.Targets {
			agentName := nodeToAgentName[tgtSpec.Node]
			if !myNodes[agentName] {
				continue
			}
			htSpec := infra.HATargetSpec{
				VolSize:         tgtSpec.VolSize,
				WALSize:         tgtSpec.WALSize,
				IQN:             tgtSpec.IQN(),
				ISCSIPort:       tgtSpec.ISCSIPort,
				AdminPort:       tgtSpec.AdminPort,
				ReplicaDataPort: tgtSpec.ReplicaDataPort,
				ReplicaCtrlPort: tgtSpec.ReplicaCtrlPort,
				RebuildPort:     tgtSpec.RebuildPort,
				TPGID:           tgtSpec.TPGID,
			}
			actx.Targets[tgtName] = infra.NewHATargetFromSpec(nativeNode, tgtName, htSpec)
		}
		// Also map topology node names to native node if this agent handles them.
		for nodeName, nodeSpec := range req.Scenario.Topology.Nodes {
			if myNodes[nodeSpec.Agent] {
				actx.Nodes[nodeName] = nativeNode
			}
		}
	}

	engine := NewEngine(a.registry, actx.Log)

	for _, act := range req.Actions {
		start := time.Now()
		resolved := resolveAction(act, actx.Vars)
		yamlDef := marshalActionYAML(resolved)

		handler, err := engine.registry.Get(resolved.Action)
		if err != nil {
			resp.Results = append(resp.Results, ActionResult{
				Action:   resolved.Action,
				Status:   StatusFail,
				Duration: time.Since(start),
				Error:    err.Error(),
			})
			resp.Error = err.Error()
			return resp
		}

		// Handle delay param.
		if d, ok := resolved.Params["delay"]; ok {
			if dur, err := time.ParseDuration(d); err == nil {
				select {
				case <-time.After(dur):
				case <-ctx.Done():
					resp.Error = ctx.Err().Error()
					return resp
				}
			}
		}

		output, execErr := handler.Execute(ctx, actx, resolved)

		ar := ActionResult{
			Action:   resolved.Action,
			Duration: time.Since(start),
			YAML:     yamlDef,
		}

		if execErr != nil {
			ar.Status = StatusFail
			ar.Error = execErr.Error()
			if !act.IgnoreError {
				resp.Results = append(resp.Results, ar)
				resp.Error = execErr.Error()
				return resp
			}
			ar.Status = StatusPass
		} else {
			ar.Status = StatusPass
		}

		// Store save_as and __ vars.
		if resolved.SaveAs != "" && output != nil {
			if v, ok := output["value"]; ok {
				actx.Vars[resolved.SaveAs] = v
				resp.NewVars[resolved.SaveAs] = v
			}
		}
		if output != nil {
			for k, v := range output {
				if strings.HasPrefix(k, "__") {
					actx.Vars[k] = v
					resp.NewVars[k] = v
				}
			}
			if v, ok := output["value"]; ok {
				ar.Output = truncate(v, 4096)
			}
		}

		resp.Results = append(resp.Results, ar)
	}

	return resp
}

// POST /upload — streaming binary upload with path safety.
func (a *Agent) handleUpload(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	path := r.URL.Query().Get("path")
	if path == "" {
		writeJSON(w, http.StatusBadRequest, UploadResponse{Error: "path query parameter required"})
		return
	}

	// Path safety: must start with UploadBasePath and no traversal.
	if !isPathSafe(path) {
		writeJSON(w, http.StatusForbidden, UploadResponse{Error: fmt.Sprintf("path must start with %s and contain no '..'", UploadBasePath)})
		return
	}

	// Ensure directory exists.
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0755); err != nil {
		writeJSON(w, http.StatusInternalServerError, UploadResponse{Error: fmt.Sprintf("mkdir: %v", err)})
		return
	}

	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0755)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, UploadResponse{Error: fmt.Sprintf("create file: %v", err)})
		return
	}

	n, err := io.Copy(f, r.Body)
	f.Close()
	if err != nil {
		os.Remove(path)
		writeJSON(w, http.StatusInternalServerError, UploadResponse{Error: fmt.Sprintf("write: %v", err)})
		return
	}

	a.log.Printf("uploaded %d bytes to %s", n, path)
	writeJSON(w, http.StatusOK, UploadResponse{OK: true, Size: n, Path: path})
}

// GET /artifacts — stream a directory as tar.gz.
func (a *Agent) handleArtifacts(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	dir := r.URL.Query().Get("dir")
	if dir == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "dir query parameter required"})
		return
	}

	// Path safety: must be under UploadBasePath, no traversal.
	if !isPathSafe(dir) {
		writeJSON(w, http.StatusForbidden, map[string]string{"error": fmt.Sprintf("dir must be under %s with no '..'", UploadBasePath)})
		return
	}

	info, err := os.Stat(dir)
	if err != nil || !info.IsDir() {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "directory not found"})
		return
	}

	// Check if directory has any files.
	entries, err := os.ReadDir(dir)
	if err != nil || len(entries) == 0 {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "directory empty"})
		return
	}

	w.Header().Set("Content-Type", "application/gzip")
	w.WriteHeader(http.StatusOK)

	gw := gzip.NewWriter(w)
	defer gw.Close()
	tw := tar.NewWriter(gw)
	defer tw.Close()

	filepath.Walk(dir, func(path string, fi os.FileInfo, err error) error {
		if err != nil {
			return nil // skip unreadable files
		}
		if fi.IsDir() {
			return nil
		}

		// Relative path inside the tar.
		rel, err := filepath.Rel(dir, path)
		if err != nil {
			return nil
		}
		// Normalize to forward slashes for tar.
		rel = filepath.ToSlash(rel)

		hdr := &tar.Header{
			Name:    rel,
			Size:    fi.Size(),
			Mode:    int64(fi.Mode()),
			ModTime: fi.ModTime(),
		}
		if err := tw.WriteHeader(hdr); err != nil {
			return err
		}

		f, err := os.Open(path)
		if err != nil {
			return nil
		}
		defer f.Close()
		io.Copy(tw, f)
		return nil
	})
}

// POST /exec — ad-hoc command execution (disabled by default).
func (a *Agent) handleExec(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	if !a.AllowExec {
		writeJSON(w, http.StatusForbidden, ExecResponse{Error: "exec endpoint disabled; start agent with --allow-exec"})
		return
	}

	var req ExecRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, ExecResponse{Error: fmt.Sprintf("decode: %v", err)})
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 60*time.Second)
	defer cancel()

	var stdout, stderr string
	var exitCode int
	var err error

	if req.Root {
		stdout, stderr, exitCode, err = a.localNode.RunRoot(ctx, req.Cmd)
	} else {
		stdout, stderr, exitCode, err = a.localNode.Run(ctx, req.Cmd)
	}

	if err != nil {
		writeJSON(w, http.StatusOK, ExecResponse{
			Stdout:   stdout,
			Stderr:   stderr,
			ExitCode: -1,
			Error:    err.Error(),
		})
		return
	}

	writeJSON(w, http.StatusOK, ExecResponse{
		Stdout:   stdout,
		Stderr:   stderr,
		ExitCode: exitCode,
	})
}

// isPathSafe checks that a path is under UploadBasePath and has no traversal.
func isPathSafe(p string) bool {
	if strings.Contains(p, "..") {
		return false
	}
	// Use forward-slash path cleaning (Unix paths, even on Windows coordinator).
	cleaned := posixClean(p)
	return strings.HasPrefix(cleaned, UploadBasePath)
}

// posixClean normalizes a path using forward slashes (Unix convention).
func posixClean(p string) string {
	// Replace backslashes, then clean.
	p = strings.ReplaceAll(p, "\\", "/")
	// Remove double slashes.
	for strings.Contains(p, "//") {
		p = strings.ReplaceAll(p, "//", "/")
	}
	// Remove trailing slash (unless root).
	if len(p) > 1 && strings.HasSuffix(p, "/") {
		p = p[:len(p)-1]
	}
	return p
}

func writeJSON(w http.ResponseWriter, status int, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(v)
}
