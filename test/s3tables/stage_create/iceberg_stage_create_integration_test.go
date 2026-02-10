package stage_create

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

type miniEnv struct {
	weedBinary  string
	dataDir     string
	icebergPort int
	masterPort  int
	masterGrpc  int
	filerPort   int
	filerGrpc   int
	volumePort  int
	volumeGrpc  int
	cmd         *exec.Cmd
	cancel      context.CancelFunc
}

func getFreePort(t *testing.T) int {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to allocate free port: %v", err)
	}
	defer listener.Close()
	return listener.Addr().(*net.TCPAddr).Port
}

func findRepoRoot(t *testing.T) string {
	t.Helper()
	wd, err := os.Getwd()
	if err != nil {
		t.Fatalf("failed to get working directory: %v", err)
	}
	root := wd
	for i := 0; i < 8; i++ {
		if _, err := os.Stat(filepath.Join(root, "go.mod")); err == nil {
			return root
		}
		root = filepath.Dir(root)
	}
	t.Fatalf("failed to locate repo root from %s", wd)
	return ""
}

func startMini(t *testing.T) *miniEnv {
	t.Helper()
	root := findRepoRoot(t)

	weedBinary := filepath.Join(root, "weed", "weed")
	if _, err := os.Stat(weedBinary); os.IsNotExist(err) {
		weedBinary = "weed"
		if _, err := exec.LookPath(weedBinary); err != nil {
			t.Skip("weed binary not found; skipping integration test")
		}
	}

	dataDir, err := os.MkdirTemp("", "seaweed-stage-create-it-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}

	env := &miniEnv{
		weedBinary:  weedBinary,
		dataDir:     dataDir,
		icebergPort: getFreePort(t),
		masterPort:  getFreePort(t),
		masterGrpc:  getFreePort(t),
		filerPort:   getFreePort(t),
		filerGrpc:   getFreePort(t),
		volumePort:  getFreePort(t),
		volumeGrpc:  getFreePort(t),
	}

	ctx, cancel := context.WithCancel(context.Background())
	env.cancel = cancel

	env.cmd = exec.CommandContext(ctx, env.weedBinary, "mini",
		"-master.port", fmt.Sprintf("%d", env.masterPort),
		"-master.port.grpc", fmt.Sprintf("%d", env.masterGrpc),
		"-filer.port", fmt.Sprintf("%d", env.filerPort),
		"-filer.port.grpc", fmt.Sprintf("%d", env.filerGrpc),
		"-volume.port", fmt.Sprintf("%d", env.volumePort),
		"-volume.port.grpc", fmt.Sprintf("%d", env.volumeGrpc),
		"-s3.port", "0",
		"-s3.port.iceberg", fmt.Sprintf("%d", env.icebergPort),
		"-ip.bind", "0.0.0.0",
		"-dir", env.dataDir,
	)
	env.cmd.Stdout = os.Stdout
	env.cmd.Stderr = os.Stderr
	if err := env.cmd.Start(); err != nil {
		t.Fatalf("failed to start weed mini: %v", err)
	}

	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		resp, err := http.Get(fmt.Sprintf("http://127.0.0.1:%d/v1/config", env.icebergPort))
		if err == nil {
			resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				return env
			}
		}
		time.Sleep(300 * time.Millisecond)
	}

	env.cleanup(t)
	t.Fatalf("iceberg endpoint not ready")
	return nil
}

func (e *miniEnv) cleanup(t *testing.T) {
	t.Helper()
	if e.cancel != nil {
		e.cancel()
	}
	if e.cmd != nil {
		_, _ = e.cmd.Process.Wait()
	}
	if e.dataDir != "" {
		_ = os.RemoveAll(e.dataDir)
	}
}

func TestStageCreateMissingNameReturnsBadRequest(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	env := startMini(t)
	defer env.cleanup(t)

	reqBody := `{"stage-create":true}`
	url := fmt.Sprintf("http://127.0.0.1:%d/v1/namespaces/ns1/tables", env.icebergPort)
	req, err := http.NewRequest(http.MethodPost, url, strings.NewReader(reqBody))
	if err != nil {
		t.Fatalf("failed to build request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusBadRequest {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("status = %d, want %d, body=%s", resp.StatusCode, http.StatusBadRequest, string(body))
	}

	var decoded map[string]map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&decoded); err != nil {
		t.Fatalf("failed to decode error response: %v", err)
	}
	errorObj, ok := decoded["error"]
	if !ok {
		t.Fatalf("response missing error object: %#v", decoded)
	}
	if got := errorObj["type"]; got != "BadRequestException" {
		t.Fatalf("error.type = %v, want BadRequestException", got)
	}
	msg, _ := errorObj["message"].(string)
	if !strings.Contains(strings.ToLower(msg), "table name is required") {
		t.Fatalf("error.message = %v, want it to include %q", errorObj["message"], "table name is required")
	}
}
