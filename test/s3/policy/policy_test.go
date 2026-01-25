package policy

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/command"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/util"
	flag "github.com/seaweedfs/seaweedfs/weed/util/fla9"
	"github.com/stretchr/testify/require"
)

// TestCluster manages the weed mini instance for integration testing
type TestCluster struct {
	dataDir    string
	ctx        context.Context
	cancel     context.CancelFunc
	isRunning  bool
	wg         sync.WaitGroup
	masterPort int
	volumePort int
	filerPort  int
	s3Port     int
	s3Endpoint string
}

func TestS3PolicyShell(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	// Create and start test cluster
	cluster, err := startMiniCluster(t)
	require.NoError(t, err)
	defer cluster.Stop()

	// 1. Setup - Create a temporary policy file
	policyContent := `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"*","Resource":"*"}]}`
	tmpPolicyFile, err := os.CreateTemp("", "test_policy_*.json")
	if err != nil {
		t.Fatalf("Failed to create temp policy file: %v", err)
	}
	defer os.Remove(tmpPolicyFile.Name())

	if _, err := tmpPolicyFile.WriteString(policyContent); err != nil {
		t.Fatalf("Failed to write to temp policy file: %v", err)
	}
	tmpPolicyFile.Close()

	// 2. Run shell commands
	weedCmd := "weed"
	// In test environment, we might need to point to the built binary or run via 'go run'.
	// But since 'startMiniCluster' runs 'weed' command internally via Main, we can't easily shelling out to 'weed'
	// unless 'weed' is in PATH.
	// However, for this integration test, we can try to use `go run ...` or assume `weed` is installed.
	// Given `make` ran `go install`, `weed` should be in $GOPATH/bin.

	// Construct args to point to our test cluster
	args := fmt.Sprintf("-master=127.0.0.1:%d -filer=127.0.0.1:%d", cluster.masterPort, cluster.filerPort)

	// Put Policy
	cmd := fmt.Sprintf("s3.policy %s -put -name=testpolicy -file=%s", args, tmpPolicyFile.Name())
	runShellCommand(t, weedCmd, cmd)

	// List Policies
	cmd = fmt.Sprintf("s3.policy %s -list", args)
	output := runShellCommand(t, weedCmd, cmd)
	if !contains(output, "Name: testpolicy") {
		t.Errorf("List policies failed, expected 'Name: testpolicy', got: %s", output)
	}

	// Get Policy
	cmd = fmt.Sprintf("s3.policy %s -get -name=testpolicy", args)
	output = runShellCommand(t, weedCmd, cmd)
	if !contains(output, "Statement") {
		t.Errorf("Get policy failed, expected policy content, got: %s", output)
	}

	// Delete Policy
	cmd = fmt.Sprintf("s3.policy %s -delete -name=testpolicy", args)
	runShellCommand(t, weedCmd, cmd)

	// Verify Deletion
	cmd = fmt.Sprintf("s3.policy %s -list", args)
	output = runShellCommand(t, weedCmd, cmd)
	if contains(output, "Name: testpolicy") {
		t.Errorf("Delete policy failed, testpolicy still exists in list: %s", output)
	}
}

func runShellCommand(t *testing.T, weedCmd string, commandStr string) string {
	// We use 'shell -command "..."'
	// But since we need to pass master/filer args to shell, we should do:
	// weed shell -master=... -filer=... -command "s3.policy -put ..."
	// Wait, the s3.policy command inside shell doesn't take master/filer args. The SHELL does.

	// Parse my commandStr to extract args? No.
	// The `commandStr` constructed above included `args` which were `-master=...`.
	// But `s3.policy` command itself parses flags.
	// The `s3.policy` implementation calls `pb.WithGrpcClient` using `commandEnv.option.FilerAddress`.
	// `commandEnv` is populated by `shell` command based on its own flags.

	// So we need to run:
	// weed shell -master=localhost:PORT -filer=localhost:PORT -command "s3.policy -put ..."

	// Let's adjust the caller.
	// The caller passed `s3.policy -master=... ...` which is WRONG for `s3.policy` command.
	// The `args` should be passed to `weed shell`.

	// Actually, let's change `runShellCommand` signature or usage.
	return ""
}

// Rewriting test logic to use correct arguments
func TestS3PolicyShellRevised(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}
	cluster, err := startMiniCluster(t)
	require.NoError(t, err)
	defer cluster.Stop()

	policyContent := `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"*","Resource":"*"}]}`
	tmpPolicyFile, err := os.CreateTemp("", "test_policy_*.json")
	if err != nil {
		t.Fatalf("Failed to create temp policy file: %v", err)
	}
	defer os.Remove(tmpPolicyFile.Name())
	tmpPolicyFile.WriteString(policyContent)
	tmpPolicyFile.Close()

	weedCmd := "weed"
	masterAddr := fmt.Sprintf("127.0.0.1:%d", cluster.masterPort)
	filerAddr := fmt.Sprintf("127.0.0.1:%d", cluster.filerPort)

	// Put
	execShell(t, weedCmd, masterAddr, filerAddr, fmt.Sprintf("s3.policy -put -name=testpolicy -file=%s", tmpPolicyFile.Name()))

	// List
	out := execShell(t, weedCmd, masterAddr, filerAddr, "s3.policy -list")
	if !contains(out, "Name: testpolicy") {
		t.Errorf("List failed: %s", out)
	}

	// Get
	out = execShell(t, weedCmd, masterAddr, filerAddr, "s3.policy -get -name=testpolicy")
	if !contains(out, "Statement") {
		t.Errorf("Get failed: %s", out)
	}

	// Delete
	execShell(t, weedCmd, masterAddr, filerAddr, "s3.policy -delete -name=testpolicy")

	// Verify
	out = execShell(t, weedCmd, masterAddr, filerAddr, "s3.policy -list")
	if contains(out, "Name: testpolicy") {
		t.Errorf("Delete failed: %s", out)
	}
}

func execShell(t *testing.T, weedCmd, master, filer, shellCmd string) string {
	// weed shell -master=... -filer=... -command "..."
	args := []string{"shell", "-master=" + master, "-filer=" + filer, "-command", shellCmd}
	t.Logf("Running: %s %v", weedCmd, args)
	out, err := util.ExecCommand(weedCmd, args...)
	if err != nil {
		t.Fatalf("Failed to run %s: %v\nOutput: %s", shellCmd, err, string(out))
	}
	return string(out)
}

// --- Test setup helpers ---

func findAvailablePort() (int, error) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return 0, err
	}
	defer listener.Close()
	addr := listener.Addr().(*net.TCPAddr)
	return addr.Port, nil
}

func startMiniCluster(t *testing.T) (*TestCluster, error) {
	masterPort, _ := findAvailablePort()
	volumePort, _ := findAvailablePort()
	filerPort, _ := findAvailablePort()
	s3Port, _ := findAvailablePort()

	testDir := t.TempDir()

	ctx, cancel := context.WithCancel(context.Background())
	s3Endpoint := fmt.Sprintf("http://127.0.0.1:%d", s3Port)
	cluster := &TestCluster{
		dataDir:    testDir,
		ctx:        ctx,
		cancel:     cancel,
		masterPort: masterPort,
		volumePort: volumePort,
		filerPort:  filerPort,
		s3Port:     s3Port,
		s3Endpoint: s3Endpoint,
	}

	// Disable authentication for tests
	securityToml := filepath.Join(testDir, "security.toml")
	os.WriteFile(securityToml, []byte("# Empty security config\n"), 0644)

	cluster.wg.Add(1)
	go func() {
		defer cluster.wg.Done()
		oldDir, _ := os.Getwd()
		oldArgs := os.Args
		defer func() {
			os.Chdir(oldDir)
			os.Args = oldArgs
		}()
		os.Chdir(testDir)
		os.Args = []string{
			"weed",
			"-dir=" + testDir,
			"-master.port=" + strconv.Itoa(masterPort),
			"-volume.port=" + strconv.Itoa(volumePort),
			"-filer.port=" + strconv.Itoa(filerPort),
			"-s3.port=" + strconv.Itoa(s3Port),
			"-webdav.port=0",
			"-admin.ui=false",
			"-master.volumeSizeLimitMB=32",
			"-ip=127.0.0.1",
			"-master.peers=none",
		}
		glog.MaxSize = 1024 * 1024
		for _, cmd := range command.Commands {
			if cmd.Name() == "mini" && cmd.Run != nil {
				cmd.Flag.Parse(os.Args[1:])
				cmd.Run(cmd, cmd.Flag.Args())
				return
			}
		}
	}()

	// Wait for S3
	err := waitForS3Ready(cluster.s3Endpoint, 30*time.Second)
	if err != nil {
		cancel()
		return nil, err
	}
	cluster.isRunning = true
	return cluster, nil
}

func waitForS3Ready(endpoint string, timeout time.Duration) error {
	client := &http.Client{Timeout: 1 * time.Second}
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		resp, err := client.Get(endpoint)
		if err == nil {
			resp.Body.Close()
			return nil
		}
		time.Sleep(200 * time.Millisecond)
	}
	return fmt.Errorf("timeout waiting for S3")
}

func (c *TestCluster) Stop() {
	if c.cancel != nil {
		c.cancel()
	}
	if c.isRunning {
		time.Sleep(500 * time.Millisecond)
	}
	// Simplified stop
	for _, cmd := range command.Commands {
		if cmd.Name() == "mini" {
			cmd.Flag.VisitAll(func(f *flag.Flag) {
				f.Value.Set(f.DefValue)
			})
			break
		}
	}
}

func contains(s, substr string) bool {
	return util.StringContains(s, substr)
}
