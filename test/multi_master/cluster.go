package multi_master

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/test/testutil"
)

const (
	waitTimeout = 30 * time.Second
	waitTick    = 200 * time.Millisecond
)

// masterNode represents a single master process in the cluster.
type masterNode struct {
	port     int
	grpcPort int
	dataDir  string
	cmd      *exec.Cmd
	logFile  string
	stopped  bool
}

// MasterCluster manages a 3-node master raft cluster for integration tests.
type MasterCluster struct {
	t          testing.TB
	weedBinary string
	baseDir    string
	logsDir    string
	keepLogs   bool

	nodes [3]*masterNode
	mu    sync.Mutex

	// peers string shared by all nodes, e.g. "127.0.0.1:9333,127.0.0.1:9334,127.0.0.1:9335"
	peersStr string
}

// clusterStatus is the JSON returned by /cluster/status.
type clusterStatus struct {
	IsLeader bool     `json:"IsLeader"`
	Leader   string   `json:"Leader"`
	Peers    []string `json:"Peers"`
}

// StartMasterCluster boots a 3-node master raft cluster and waits for a leader.
func StartMasterCluster(t testing.TB) *MasterCluster {
	t.Helper()

	weedBinary, err := findOrBuildWeedBinary()
	if err != nil {
		t.Fatalf("resolve weed binary: %v", err)
	}

	keepLogs := os.Getenv("MULTI_MASTER_IT_KEEP_LOGS") == "1"
	baseDir, err := os.MkdirTemp("", "seaweedfs_multi_master_it_")
	if err != nil {
		t.Fatalf("create temp dir: %v", err)
	}
	logsDir := filepath.Join(baseDir, "logs")
	os.MkdirAll(logsDir, 0o755)

	// Allocate 3 mini-safe ports (each guarantees port+10000 is also free).
	httpPorts, err := testutil.AllocateMiniPorts(3)
	if err != nil {
		t.Fatalf("allocate ports: %v", err)
	}
	var nodes [3]*masterNode
	var peerParts []string
	for i, hp := range httpPorts {
		dataDir := filepath.Join(baseDir, fmt.Sprintf("m%d", i))
		os.MkdirAll(dataDir, 0o755)
		nodes[i] = &masterNode{
			port:     hp,
			grpcPort: hp + testutil.GrpcPortOffset,
			dataDir:  dataDir,
			logFile:  filepath.Join(logsDir, fmt.Sprintf("master%d.log", i)),
		}
		peerParts = append(peerParts, fmt.Sprintf("127.0.0.1:%d", hp))
	}

	mc := &MasterCluster{
		t:          t,
		weedBinary: weedBinary,
		baseDir:    baseDir,
		logsDir:    logsDir,
		keepLogs:   keepLogs,
		nodes:      nodes,
		peersStr:   strings.Join(peerParts, ","),
	}

	for i := range 3 {
		mc.StartNode(i)
	}

	if err := mc.WaitForLeader(waitTimeout); err != nil {
		mc.DumpLogs()
		mc.StopAll()
		t.Fatalf("cluster did not elect a leader: %v", err)
	}

	// Wait for TopologyId to be generated and propagated. This is async
	// after leader election, and we need it committed before tests can
	// reliably stop/restart nodes.
	if err := mc.WaitForTopologyId(waitTimeout); err != nil {
		mc.DumpLogs()
		mc.StopAll()
		t.Fatalf("TopologyId not generated: %v", err)
	}

	t.Cleanup(func() {
		mc.StopAll()
	})
	return mc
}

// StartNode starts the master process at the given index (0–2).
func (mc *MasterCluster) StartNode(i int) {
	mc.t.Helper()
	mc.mu.Lock()
	defer mc.mu.Unlock()

	n := mc.nodes[i]
	if n.cmd != nil && !n.stopped {
		return // already running
	}

	logFile, err := os.OpenFile(n.logFile, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		mc.t.Fatalf("create log for node %d: %v", i, err)
	}

	args := []string{
		"master",
		"-ip=127.0.0.1",
		"-port=" + strconv.Itoa(n.port),
		"-port.grpc=" + strconv.Itoa(n.grpcPort),
		"-mdir=" + n.dataDir,
		"-peers=" + mc.peersStr,
		"-electionTimeout=3s",
		"-volumeSizeLimitMB=32",
		"-defaultReplication=000",
	}

	n.cmd = exec.Command(mc.weedBinary, args...)
	n.cmd.Dir = mc.baseDir
	n.cmd.Stdout = logFile
	n.cmd.Stderr = logFile
	n.stopped = false
	if err := n.cmd.Start(); err != nil {
		mc.t.Fatalf("start node %d: %v", i, err)
	}
}

// StopNode gracefully stops the master at the given index.
func (mc *MasterCluster) StopNode(i int) {
	mc.mu.Lock()
	defer mc.mu.Unlock()
	mc.stopNodeLocked(i)
}

func (mc *MasterCluster) stopNodeLocked(i int) {
	n := mc.nodes[i]
	if n.cmd == nil || n.stopped {
		return
	}
	n.stopped = true
	_ = n.cmd.Process.Signal(os.Interrupt)
	done := make(chan error, 1)
	go func() { done <- n.cmd.Wait() }()
	select {
	case <-time.After(10 * time.Second):
		_ = n.cmd.Process.Kill()
		<-done
	case <-done:
	}
}

// StopAll stops all running master nodes.
func (mc *MasterCluster) StopAll() {
	mc.mu.Lock()
	defer mc.mu.Unlock()
	for i := range 3 {
		mc.stopNodeLocked(i)
	}
	if !mc.keepLogs && !mc.t.Failed() {
		os.RemoveAll(mc.baseDir)
	} else if mc.baseDir != "" {
		mc.t.Logf("multi-master logs kept at %s", mc.baseDir)
	}
}

// NodeURL returns the HTTP URL for node i.
func (mc *MasterCluster) NodeURL(i int) string {
	return fmt.Sprintf("http://127.0.0.1:%d", mc.nodes[i].port)
}

// NodeAddress returns "127.0.0.1:port" for node i.
func (mc *MasterCluster) NodeAddress(i int) string {
	return fmt.Sprintf("127.0.0.1:%d", mc.nodes[i].port)
}

// NodeGRPCAddress returns "127.0.0.1:grpcPort" for node i.
func (mc *MasterCluster) NodeGRPCAddress(i int) string {
	return fmt.Sprintf("127.0.0.1:%d", mc.nodes[i].grpcPort)
}

// IsNodeRunning returns true if the node at index i has a live process.
func (mc *MasterCluster) IsNodeRunning(i int) bool {
	mc.mu.Lock()
	defer mc.mu.Unlock()
	n := mc.nodes[i]
	return n.cmd != nil && !n.stopped
}

// GetClusterStatus fetches /cluster/status from node i.
func (mc *MasterCluster) GetClusterStatus(i int) (*clusterStatus, error) {
	client := &http.Client{Timeout: 2 * time.Second}
	resp, err := client.Get(mc.NodeURL(i) + "/cluster/status")
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	var cs clusterStatus
	if err := json.Unmarshal(body, &cs); err != nil {
		return nil, fmt.Errorf("parse cluster/status: %w (body: %s)", err, string(body))
	}
	return &cs, nil
}

// GetTopologyId fetches the TopologyId from /dir/status on node i.
func (mc *MasterCluster) GetTopologyId(i int) (string, error) {
	client := &http.Client{Timeout: 2 * time.Second}
	resp, err := client.Get(mc.NodeURL(i) + "/dir/status")
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	var raw map[string]any
	if err := json.Unmarshal(body, &raw); err != nil {
		return "", err
	}
	if id, ok := raw["TopologyId"].(string); ok {
		return id, nil
	}
	return "", nil
}

// FindLeader returns the index of the leader node and its address.
// Returns -1 if no leader is found.
func (mc *MasterCluster) FindLeader() (int, string) {
	for i := range 3 {
		if !mc.IsNodeRunning(i) {
			continue
		}
		cs, err := mc.GetClusterStatus(i)
		if err != nil {
			continue
		}
		if cs.IsLeader {
			return i, mc.NodeAddress(i)
		}
	}
	return -1, ""
}

// WaitForLeader polls until a leader is elected or timeout.
func (mc *MasterCluster) WaitForLeader(timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if idx, _ := mc.FindLeader(); idx >= 0 {
			return nil
		}
		time.Sleep(waitTick)
	}
	return fmt.Errorf("no leader elected within %v", timeout)
}

// WaitForNewLeader waits for a leader that is different from the given address.
func (mc *MasterCluster) WaitForNewLeader(oldLeaderAddr string, timeout time.Duration) (int, string, error) {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		idx, addr := mc.FindLeader()
		if idx >= 0 && addr != oldLeaderAddr {
			return idx, addr, nil
		}
		time.Sleep(waitTick)
	}
	return -1, "", fmt.Errorf("no new leader (different from %s) within %v", oldLeaderAddr, timeout)
}

// WaitForTopologyId waits until the leader reports a non-empty TopologyId.
func (mc *MasterCluster) WaitForTopologyId(timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if idx, _ := mc.FindLeader(); idx >= 0 {
			if id, err := mc.GetTopologyId(idx); err == nil && id != "" {
				return nil
			}
		}
		time.Sleep(waitTick)
	}
	return fmt.Errorf("TopologyId not available within %v", timeout)
}

// WaitForNodeReady waits for node i to respond to HTTP.
func (mc *MasterCluster) WaitForNodeReady(i int, timeout time.Duration) error {
	client := &http.Client{Timeout: 1 * time.Second}
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		resp, err := client.Get(mc.NodeURL(i) + "/cluster/status")
		if err == nil {
			resp.Body.Close()
			return nil
		}
		time.Sleep(waitTick)
	}
	return fmt.Errorf("node %d not ready within %v", i, timeout)
}

// DumpLogs prints the tail of all master logs.
func (mc *MasterCluster) DumpLogs() {
	for i := range 3 {
		mc.t.Logf("=== master%d log tail ===\n%s", i, mc.tailLog(i))
	}
}

func (mc *MasterCluster) tailLog(i int) string {
	f, err := os.Open(mc.nodes[i].logFile)
	if err != nil {
		return "(no log)"
	}
	defer f.Close()
	scanner := bufio.NewScanner(f)
	lines := make([]string, 0, 50)
	for scanner.Scan() {
		lines = append(lines, scanner.Text())
		if len(lines) > 50 {
			lines = lines[1:]
		}
	}
	return strings.Join(lines, "\n")
}

func findOrBuildWeedBinary() (string, error) {
	if fromEnv := os.Getenv("WEED_BINARY"); fromEnv != "" {
		if isExecutableFile(fromEnv) {
			return fromEnv, nil
		}
		return "", fmt.Errorf("WEED_BINARY not executable: %s", fromEnv)
	}

	repoRoot := ""
	if _, file, _, ok := runtime.Caller(0); ok {
		repoRoot = filepath.Clean(filepath.Join(filepath.Dir(file), "..", ".."))
	}
	if repoRoot == "" {
		return "", fmt.Errorf("unable to detect repository root")
	}

	// Check if already built
	binDir := filepath.Join(os.TempDir(), "seaweedfs_multi_master_it_bin")
	os.MkdirAll(binDir, 0o755)
	binPath := filepath.Join(binDir, "weed")
	if isExecutableFile(binPath) {
		return binPath, nil
	}

	cmd := exec.Command("go", "build", "-o", binPath, ".")
	cmd.Dir = filepath.Join(repoRoot, "weed")
	var out bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = &out
	if err := cmd.Run(); err != nil {
		return "", fmt.Errorf("build weed binary: %w\n%s", err, out.String())
	}
	return binPath, nil
}

func isExecutableFile(path string) bool {
	info, err := os.Stat(path)
	if err != nil || info.IsDir() {
		return false
	}
	return info.Mode().Perm()&0o111 != 0
}
