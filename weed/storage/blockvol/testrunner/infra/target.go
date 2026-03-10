package infra

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"time"
)

// TargetConfig configures an iSCSI target instance.
type TargetConfig struct {
	VolSize string // e.g. "100M"
	WALSize string // e.g. "64M" (default), "4M" for WAL pressure tests
	IQN     string
	Port    int
}

// DefaultTargetConfig returns a default target config for integration tests.
func DefaultTargetConfig() TargetConfig {
	return TargetConfig{
		VolSize: "100M",
		WALSize: "64M",
		IQN:     "iqn.2024.com.seaweedfs:test1",
		Port:    3260,
	}
}

// Target manages the lifecycle of an iscsi-target process on a remote node.
type Target struct {
	Node    *Node
	Config  TargetConfig
	BinPath string // remote path to iscsi-target binary
	Pid     int
	LogFile string // remote path to target's stderr log
	VolFile string // remote path to volume file
}

// NewTarget creates a Target bound to a node.
func NewTarget(node *Node, config TargetConfig) *Target {
	return &Target{
		Node:    node,
		Config:  config,
		BinPath: "/tmp/iscsi-target-test",
		VolFile: "/tmp/blockvol-test.blk",
		LogFile: "/tmp/iscsi-target-test.log",
	}
}

// SetBinPath overrides the remote binary path.
func (t *Target) SetBinPath(p string) { t.BinPath = p }

// SetVolFile overrides the remote volume file path.
func (t *Target) SetVolFile(p string) { t.VolFile = p }

// SetLogFile overrides the remote log file path.
func (t *Target) SetLogFile(p string) { t.LogFile = p }

// Build cross-compiles the iscsi-target binary for linux/amd64.
func (t *Target) Build(ctx context.Context, repoDir string) error {
	binDir := repoDir + "/weed/storage/blockvol/iscsi/cmd/iscsi-target"
	outPath := repoDir + "/iscsi-target-linux"

	cmd := exec.CommandContext(ctx, "go", "build", "-o", outPath, ".")
	cmd.Dir = binDir
	cmd.Env = append(os.Environ(), "GOOS=linux", "GOARCH=amd64", "CGO_ENABLED=0")
	out, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("build failed: %s\n%w", out, err)
	}
	return nil
}

// Deploy uploads the pre-built binary to the target node.
func (t *Target) Deploy(localBin string) error {
	return t.Node.Upload(localBin, t.BinPath)
}

// Start launches the target process. If create is true, a new volume is created.
func (t *Target) Start(ctx context.Context, create bool) error {
	// Pre-flight: check if iSCSI port is already in use.
	stdout, _, code, _ := t.Node.Run(ctx, fmt.Sprintf("ss -tln | grep ':%d '", t.Config.Port))
	if code == 0 && strings.TrimSpace(stdout) != "" {
		owner, _, _, _ := t.Node.Run(ctx, fmt.Sprintf("ss -tlnp | grep ':%d ' | head -1", t.Config.Port))
		return fmt.Errorf("port %d already in use on %s: %s",
			t.Config.Port, t.Node.Host, strings.TrimSpace(owner))
	}

	// Remove old log
	t.Node.Run(ctx, fmt.Sprintf("rm -f %s", t.LogFile))

	args := fmt.Sprintf("-vol %s -addr :%d -iqn %s",
		t.VolFile, t.Config.Port, t.Config.IQN)

	if create {
		if err := CheckDiskSpace(ctx, t.Node, t.VolFile, t.Config.VolSize, t.Config.WALSize); err != nil {
			return err
		}
		t.Node.Run(ctx, fmt.Sprintf("rm -f %s %s.wal", t.VolFile, t.VolFile))
		args += fmt.Sprintf(" -create -size %s", t.Config.VolSize)
		if t.Config.WALSize != "" {
			args += fmt.Sprintf(" -wal-size %s", t.Config.WALSize)
		}
	}

	cmd := fmt.Sprintf("setsid -f %s %s >%s 2>&1", t.BinPath, args, t.LogFile)
	_, stderr, code, err := t.Node.Run(ctx, cmd)
	if err != nil || code != 0 {
		return fmt.Errorf("start target: code=%d stderr=%s err=%v", code, stderr, err)
	}

	if err := t.WaitForPort(ctx); err != nil {
		return err
	}

	// Discover PID by matching the binary name
	stdout, _, _, _ = t.Node.Run(ctx, fmt.Sprintf("ps -eo pid,args | grep '%s' | grep -v grep | awk '{print $1}'", t.BinPath))
	pidStr := strings.TrimSpace(stdout)
	if idx := strings.IndexByte(pidStr, '\n'); idx > 0 {
		pidStr = pidStr[:idx]
	}
	pid, err := strconv.Atoi(pidStr)
	if err != nil {
		return fmt.Errorf("find target PID: %q: %w", pidStr, err)
	}
	t.Pid = pid
	return nil
}

// Stop sends SIGTERM, waits up to 10s, then Kill9.
func (t *Target) Stop(ctx context.Context) error {
	if t.Pid == 0 {
		return nil
	}

	t.Node.Run(ctx, fmt.Sprintf("kill %d", t.Pid))

	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		_, _, code, _ := t.Node.Run(ctx, fmt.Sprintf("kill -0 %d 2>/dev/null", t.Pid))
		if code != 0 {
			t.Pid = 0
			return nil
		}
		time.Sleep(500 * time.Millisecond)
	}

	return t.Kill9()
}

// Kill9 sends SIGKILL immediately.
func (t *Target) Kill9() error {
	if t.Pid == 0 {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	t.Node.Run(ctx, fmt.Sprintf("kill -9 %d", t.Pid))
	t.Pid = 0
	return nil
}

// Restart stops the target and starts it again (preserving the volume).
func (t *Target) Restart(ctx context.Context) error {
	if err := t.Stop(ctx); err != nil {
		return fmt.Errorf("restart stop: %w", err)
	}
	return t.Start(ctx, false)
}

// WaitForPort polls until the target port is listening.
func (t *Target) WaitForPort(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return fmt.Errorf("wait for port %d: %w", t.Config.Port, ctx.Err())
		default:
		}

		stdout, _, code, _ := t.Node.Run(ctx, fmt.Sprintf("ss -tln | grep :%d", t.Config.Port))
		if code == 0 && strings.Contains(stdout, fmt.Sprintf(":%d", t.Config.Port)) {
			return nil
		}
		time.Sleep(200 * time.Millisecond)
	}
}

// CollectLog downloads the target's log file contents.
func (t *Target) CollectLog() (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	stdout, _, _, err := t.Node.Run(ctx, fmt.Sprintf("cat %s 2>/dev/null", t.LogFile))
	if err != nil {
		return "", err
	}
	return stdout, nil
}

// Cleanup removes the volume file, WAL, and log from the target node.
func (t *Target) Cleanup(ctx context.Context) {
	t.Node.Run(ctx, fmt.Sprintf("rm -f %s %s.wal %s", t.VolFile, t.VolFile, t.LogFile))
}

// PID returns the current target process ID.
func (t *Target) PID() int { return t.Pid }

// VolFilePath returns the remote volume file path.
func (t *Target) VolFilePath() string { return t.VolFile }

// CheckDiskSpace verifies a node has enough space for a volume + WAL.
// volSize/walSize are human-readable strings like "100M", "64M".
func CheckDiskSpace(ctx context.Context, node *Node, volFile, volSize, walSize string) error {
	// Parse sizes to MB.
	volMB := parseSizeMB(volSize)
	walMB := parseSizeMB(walSize)
	if walMB == 0 {
		walMB = 64 // default WAL
	}
	neededMB := volMB + walMB + 50 // headroom for metadata/journal

	// Get available space on the directory containing the volume file.
	dir := volFile
	if idx := strings.LastIndex(dir, "/"); idx > 0 {
		dir = dir[:idx]
	}
	stdout, _, code, _ := node.Run(ctx, fmt.Sprintf("df -BM %s 2>/dev/null | tail -1 | awk '{print $4}'", dir))
	if code != 0 {
		return nil // can't check, proceed anyway
	}
	availStr := strings.TrimSpace(stdout)
	availStr = strings.TrimSuffix(availStr, "M")
	availMB, err := strconv.Atoi(availStr)
	if err != nil {
		return nil // can't parse, proceed anyway
	}

	if availMB < neededMB {
		return fmt.Errorf("insufficient disk space on %s: %dMB available, need %dMB (vol=%s wal=%s + 50MB headroom)",
			node.Host, availMB, neededMB, volSize, walSize)
	}
	return nil
}

// parseSizeMB parses a human-readable size string (e.g. "100M", "1G", "1073741824") to megabytes.
// Raw numbers >= 1048576 are treated as bytes.
func parseSizeMB(s string) int {
	s = strings.TrimSpace(s)
	if s == "" {
		return 0
	}
	s = strings.ToUpper(s)
	multiplier := 1
	if strings.HasSuffix(s, "G") {
		multiplier = 1024
		s = strings.TrimSuffix(s, "G")
	} else if strings.HasSuffix(s, "M") {
		s = strings.TrimSuffix(s, "M")
	} else if strings.HasSuffix(s, "K") {
		s = strings.TrimSuffix(s, "K")
		v, _ := strconv.Atoi(s)
		return v / 1024
	}
	v, _ := strconv.Atoi(s)
	result := v * multiplier
	// Raw numbers >= 1MB are assumed to be in bytes.
	if multiplier == 1 && result >= 1048576 {
		return result / (1024 * 1024)
	}
	return result
}
