package testrunner

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"
)

// startTestAgent starts an agent on a random port and returns its base URL.
func startTestAgent(t *testing.T, cfg AgentConfig) (*Agent, string) {
	t.Helper()
	if cfg.Registry == nil {
		cfg.Registry = NewRegistry()
	}
	agent := NewAgent(cfg)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

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
	baseURL := "http://" + agent.ListenAddr()
	return agent, baseURL
}

func TestAgent_Health(t *testing.T) {
	_, baseURL := startTestAgent(t, AgentConfig{Port: 0})

	resp, err := http.Get(baseURL + "/health")
	if err != nil {
		t.Fatalf("GET /health: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		t.Fatalf("status = %d", resp.StatusCode)
	}

	var hr HealthResponse
	json.NewDecoder(resp.Body).Decode(&hr)
	if !hr.OK {
		t.Error("health not OK")
	}
	if hr.AgentID == "" {
		t.Error("empty agent ID")
	}
	if hr.Hostname == "" {
		t.Error("empty hostname")
	}
}

func TestAgent_Health_NoAuth(t *testing.T) {
	// /health should not require auth.
	_, baseURL := startTestAgent(t, AgentConfig{Port: 0, Token: "secret"})

	resp, err := http.Get(baseURL + "/health")
	if err != nil {
		t.Fatalf("GET /health: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		t.Fatalf("/health without token should work, got %d", resp.StatusCode)
	}
}

func TestAgent_Auth_Rejection(t *testing.T) {
	_, baseURL := startTestAgent(t, AgentConfig{Port: 0, Token: "secret"})

	// POST /phase without token → 401.
	body := bytes.NewReader([]byte(`{}`))
	resp, err := http.Post(baseURL+"/phase", "application/json", body)
	if err != nil {
		t.Fatalf("POST /phase: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusUnauthorized {
		t.Errorf("expected 401, got %d", resp.StatusCode)
	}
}

func TestAgent_Auth_ValidToken(t *testing.T) {
	_, baseURL := startTestAgent(t, AgentConfig{Port: 0, Token: "secret"})

	req, _ := http.NewRequest("POST", baseURL+"/phase", bytes.NewReader([]byte(`{"phase_index":0, "actions":[], "global_vars":{}}`)))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set(AuthTokenHeader, "secret")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("POST /phase: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		t.Errorf("expected 200 with valid token, got %d", resp.StatusCode)
	}
}

func TestAgent_Phase_EchoAction(t *testing.T) {
	registry := NewRegistry()
	registry.RegisterFunc("echo_val", TierCore, func(ctx context.Context, actx *ActionContext, act Action) (map[string]string, error) {
		return map[string]string{"value": act.Params["msg"]}, nil
	})

	_, baseURL := startTestAgent(t, AgentConfig{Port: 0, Registry: registry})

	phaseReq := PhaseRequest{
		PhaseIndex: 0,
		PhaseName:  "test",
		Actions: []Action{
			{Action: "echo_val", SaveAs: "result", Params: map[string]string{"msg": "hello"}},
		},
		GlobalVars: map[string]string{},
	}

	body, _ := json.Marshal(phaseReq)
	resp, err := http.Post(baseURL+"/phase", "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("POST /phase: %v", err)
	}
	defer resp.Body.Close()

	var phaseResp PhaseResponse
	json.NewDecoder(resp.Body).Decode(&phaseResp)

	if phaseResp.Error != "" {
		t.Fatalf("phase error: %s", phaseResp.Error)
	}
	if len(phaseResp.Results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(phaseResp.Results))
	}
	if phaseResp.Results[0].Status != StatusPass {
		t.Errorf("action status = %s", phaseResp.Results[0].Status)
	}
	if phaseResp.NewVars["result"] != "hello" {
		t.Errorf("new_vars[result] = %q, want hello", phaseResp.NewVars["result"])
	}
}

func TestAgent_Phase_FailStopsExecution(t *testing.T) {
	registry := NewRegistry()
	callOrder := []string{}
	registry.RegisterFunc("a1", TierCore, func(ctx context.Context, actx *ActionContext, act Action) (map[string]string, error) {
		callOrder = append(callOrder, "a1")
		return nil, fmt.Errorf("fail")
	})
	registry.RegisterFunc("a2", TierCore, func(ctx context.Context, actx *ActionContext, act Action) (map[string]string, error) {
		callOrder = append(callOrder, "a2")
		return nil, nil
	})

	_, baseURL := startTestAgent(t, AgentConfig{Port: 0, Registry: registry})

	phaseReq := PhaseRequest{
		PhaseName: "test",
		Actions: []Action{
			{Action: "a1"},
			{Action: "a2"}, // should not execute
		},
		GlobalVars: map[string]string{},
	}

	body, _ := json.Marshal(phaseReq)
	resp, _ := http.Post(baseURL+"/phase", "application/json", bytes.NewReader(body))
	defer resp.Body.Close()

	var phaseResp PhaseResponse
	json.NewDecoder(resp.Body).Decode(&phaseResp)

	if phaseResp.Error == "" {
		t.Error("expected error from failed action")
	}
	if len(callOrder) != 1 {
		t.Errorf("expected only a1 to run, got %v", callOrder)
	}
}

func TestAgent_Upload_PathSafety(t *testing.T) {
	_, baseURL := startTestAgent(t, AgentConfig{Port: 0})

	// Attempt path traversal → should be rejected.
	req, _ := http.NewRequest("POST", baseURL+"/upload?path=/tmp/sw-test-runner/../etc/passwd", bytes.NewReader([]byte("evil")))
	req.Header.Set("Content-Type", "application/octet-stream")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("upload: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusForbidden {
		t.Errorf("expected 403 for path traversal, got %d", resp.StatusCode)
	}

	// Attempt outside base path.
	req2, _ := http.NewRequest("POST", baseURL+"/upload?path=/etc/evil", bytes.NewReader([]byte("evil")))
	req2.Header.Set("Content-Type", "application/octet-stream")
	resp2, _ := http.DefaultClient.Do(req2)
	defer resp2.Body.Close()

	if resp2.StatusCode != http.StatusForbidden {
		t.Errorf("expected 403 for outside base path, got %d", resp2.StatusCode)
	}
}

func TestAgent_Upload_ValidPath(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("upload test requires /tmp on Unix")
	}

	_, baseURL := startTestAgent(t, AgentConfig{Port: 0})

	// Create a temp subdir under /tmp/sw-test-runner/.
	uploadDir := "/tmp/sw-test-runner/test-upload"
	os.MkdirAll(uploadDir, 0755)
	t.Cleanup(func() { os.RemoveAll(uploadDir) })

	uploadPath := filepath.Join(uploadDir, "testfile.bin")
	content := []byte("test binary content")

	req, _ := http.NewRequest("POST", baseURL+"/upload?path="+uploadPath, bytes.NewReader(content))
	req.Header.Set("Content-Type", "application/octet-stream")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("upload: %v", err)
	}
	defer resp.Body.Close()

	var ur UploadResponse
	json.NewDecoder(resp.Body).Decode(&ur)
	if !ur.OK {
		t.Fatalf("upload not OK: %s", ur.Error)
	}
	if ur.Size != int64(len(content)) {
		t.Errorf("size = %d, want %d", ur.Size, len(content))
	}

	// Verify file contents.
	got, _ := os.ReadFile(uploadPath)
	if string(got) != string(content) {
		t.Errorf("content mismatch")
	}
}

func TestAgent_Exec_DisabledByDefault(t *testing.T) {
	_, baseURL := startTestAgent(t, AgentConfig{Port: 0, AllowExec: false})

	body, _ := json.Marshal(ExecRequest{Cmd: "echo hi"})
	resp, err := http.Post(baseURL+"/exec", "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("POST /exec: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusForbidden {
		t.Errorf("expected 403 when exec disabled, got %d", resp.StatusCode)
	}
}

func TestAgent_Exec_Enabled(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("exec test requires Unix shell")
	}

	_, baseURL := startTestAgent(t, AgentConfig{Port: 0, AllowExec: true})

	body, _ := json.Marshal(ExecRequest{Cmd: "echo hello-exec"})
	resp, err := http.Post(baseURL+"/exec", "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("POST /exec: %v", err)
	}
	defer resp.Body.Close()

	var er ExecResponse
	json.NewDecoder(resp.Body).Decode(&er)
	if er.ExitCode != 0 {
		t.Errorf("exit code = %d, stderr: %s, error: %s", er.ExitCode, er.Stderr, er.Error)
	}
	if er.Stdout != "hello-exec\n" {
		t.Errorf("stdout = %q", er.Stdout)
	}
}

func TestAgent_Artifacts_PathSafety(t *testing.T) {
	_, baseURL := startTestAgent(t, AgentConfig{Port: 0, Token: "secret"})

	// Attempt with traversal.
	req, _ := http.NewRequest("GET", baseURL+"/artifacts?dir=/tmp/sw-test-runner/../etc", nil)
	req.Header.Set(AuthTokenHeader, "secret")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("GET /artifacts: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusForbidden {
		t.Errorf("expected 403 for traversal, got %d", resp.StatusCode)
	}

	// Attempt outside base path.
	req2, _ := http.NewRequest("GET", baseURL+"/artifacts?dir=/etc", nil)
	req2.Header.Set(AuthTokenHeader, "secret")
	resp2, err := http.DefaultClient.Do(req2)
	if err != nil {
		t.Fatalf("GET /artifacts: %v", err)
	}
	defer resp2.Body.Close()

	if resp2.StatusCode != http.StatusForbidden {
		t.Errorf("expected 403 for outside base path, got %d", resp2.StatusCode)
	}
}

func TestAgent_Artifacts_MissingDir(t *testing.T) {
	_, baseURL := startTestAgent(t, AgentConfig{Port: 0})

	req, _ := http.NewRequest("GET", baseURL+"/artifacts?dir=/tmp/sw-test-runner/nonexistent-"+fmt.Sprintf("%d", time.Now().UnixNano()), nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("GET /artifacts: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("expected 404 for missing dir, got %d", resp.StatusCode)
	}
}

func TestAgent_Artifacts_NoAuth(t *testing.T) {
	_, baseURL := startTestAgent(t, AgentConfig{Port: 0, Token: "secret"})

	// No auth header should be rejected.
	resp, err := http.Get(baseURL + "/artifacts?dir=/tmp/sw-test-runner/test")
	if err != nil {
		t.Fatalf("GET /artifacts: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusUnauthorized {
		t.Errorf("expected 401, got %d", resp.StatusCode)
	}
}

func TestAgent_Artifacts_ValidDir(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("artifacts test requires /tmp on Unix")
	}

	_, baseURL := startTestAgent(t, AgentConfig{Port: 0})

	// Create test directory with files.
	dir := fmt.Sprintf("/tmp/sw-test-runner/test-artifacts-%d", time.Now().UnixNano())
	os.MkdirAll(dir, 0755)
	t.Cleanup(func() { os.RemoveAll(dir) })

	os.WriteFile(filepath.Join(dir, "log.txt"), []byte("test log content"), 0644)
	os.WriteFile(filepath.Join(dir, "dmesg.txt"), []byte("kernel messages"), 0644)

	req, _ := http.NewRequest("GET", baseURL+"/artifacts?dir="+dir, nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("GET /artifacts: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("expected 200, got %d: %s", resp.StatusCode, string(body))
	}

	if ct := resp.Header.Get("Content-Type"); ct != "application/gzip" {
		t.Errorf("Content-Type = %q, want application/gzip", ct)
	}

	// Verify it's valid gzip+tar.
	body, _ := io.ReadAll(resp.Body)
	if len(body) == 0 {
		t.Fatal("empty response body")
	}
}

func TestAgent_Phase_VarSubstitution(t *testing.T) {
	registry := NewRegistry()
	registry.RegisterFunc("concat", TierCore, func(ctx context.Context, actx *ActionContext, act Action) (map[string]string, error) {
		return map[string]string{"value": act.Params["a"] + "-" + act.Params["b"]}, nil
	})

	_, baseURL := startTestAgent(t, AgentConfig{Port: 0, Registry: registry})

	phaseReq := PhaseRequest{
		PhaseName: "test",
		Actions: []Action{
			{Action: "concat", SaveAs: "out", Params: map[string]string{"a": "{{ x }}", "b": "{{ y }}"}},
		},
		GlobalVars: map[string]string{"x": "hello", "y": "world"},
	}

	body, _ := json.Marshal(phaseReq)
	resp, _ := http.Post(baseURL+"/phase", "application/json", bytes.NewReader(body))
	defer resp.Body.Close()

	respBody, _ := io.ReadAll(resp.Body)
	var phaseResp PhaseResponse
	json.Unmarshal(respBody, &phaseResp)

	if phaseResp.NewVars["out"] != "hello-world" {
		t.Errorf("var substitution failed: out = %q", phaseResp.NewVars["out"])
	}
}
