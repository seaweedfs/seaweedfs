//go:build integration

package test

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"time"
)

// WeedTarget manages the lifecycle of a `weed volume --block.listen` process.
// Unlike Target (standalone iscsi-target binary), this builds and runs the
// full weed binary with block volume support.
type WeedTarget struct {
	node      *Node
	config    TargetConfig
	binPath   string // remote path to weed binary
	pid       int
	logFile   string
	blockDir  string // remote dir containing .blk files
	volFile   string // remote path to the .blk file
	iqnPrefix string
}

// NewWeedTarget creates a WeedTarget bound to a node.
func NewWeedTarget(node *Node, config TargetConfig) *WeedTarget {
	return &WeedTarget{
		node:      node,
		config:    config,
		binPath:   "/tmp/weed-test",
		logFile:   "/tmp/weed-test.log",
		blockDir:  "/tmp/blockvol-weedtest",
		iqnPrefix: "iqn.2024-01.com.seaweedfs:vol.",
	}
}

// Build cross-compiles the weed binary for linux/amd64.
func (t *WeedTarget) Build(ctx context.Context, repoDir string) error {
	binDir := repoDir + "/weed"
	outPath := repoDir + "/weed-linux"

	cmd := exec.CommandContext(ctx, "go", "build", "-o", outPath, ".")
	cmd.Dir = binDir
	cmd.Env = append(os.Environ(), "GOOS=linux", "GOARCH=amd64", "CGO_ENABLED=0")
	out, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("build weed failed: %s\n%w", out, err)
	}
	return nil
}

// Deploy uploads the pre-built weed binary to the target node.
func (t *WeedTarget) Deploy(localBin string) error {
	return t.node.Upload(localBin, t.binPath)
}

// Start launches `weed volume --block.listen`. If create is true, creates
// the block directory and volume file first.
func (t *WeedTarget) Start(ctx context.Context, create bool) error {
	// Remove old log
	t.node.Run(ctx, fmt.Sprintf("rm -f %s", t.logFile))

	if create {
		// Create block directory and volume file
		t.node.Run(ctx, fmt.Sprintf("rm -rf %s", t.blockDir))
		t.node.Run(ctx, fmt.Sprintf("mkdir -p %s", t.blockDir))

		// Derive volume name from IQN suffix
		volName := t.volName()
		t.volFile = fmt.Sprintf("%s/%s.blk", t.blockDir, volName)

		// Create the .blk file (truncate to size)
		_, _, code, err := t.node.Run(ctx,
			fmt.Sprintf("truncate -s %s %s", t.config.VolSize, t.volFile))
		if err != nil || code != 0 {
			return fmt.Errorf("create volume file: code=%d err=%v", code, err)
		}
	}

	// Start weed volume with block support
	args := fmt.Sprintf("volume -port=19333 -block.listen=:%d -block.dir=%s",
		t.config.Port, t.blockDir)

	cmd := fmt.Sprintf("setsid -f %s %s >%s 2>&1", t.binPath, args, t.logFile)
	_, stderr, code, err := t.node.Run(ctx, cmd)
	if err != nil || code != 0 {
		return fmt.Errorf("start weed volume: code=%d stderr=%s err=%v", code, stderr, err)
	}

	// Wait for iSCSI port
	if err := t.WaitForPort(ctx); err != nil {
		return err
	}

	// Discover PID
	stdout, _, _, _ := t.node.Run(ctx,
		fmt.Sprintf("ps -eo pid,args | grep '%s' | grep -v grep | awk '{print $1}'", t.binPath))
	pidStr := strings.TrimSpace(stdout)
	if idx := strings.IndexByte(pidStr, '\n'); idx > 0 {
		pidStr = pidStr[:idx]
	}
	pid, err := strconv.Atoi(pidStr)
	if err != nil {
		return fmt.Errorf("find weed PID: %q: %w", pidStr, err)
	}
	t.pid = pid
	return nil
}

// Stop sends SIGTERM, waits up to 10s, then Kill9.
func (t *WeedTarget) Stop(ctx context.Context) error {
	if t.pid == 0 {
		return nil
	}

	t.node.Run(ctx, fmt.Sprintf("kill %d", t.pid))

	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		_, _, code, _ := t.node.Run(ctx, fmt.Sprintf("kill -0 %d 2>/dev/null", t.pid))
		if code != 0 {
			t.pid = 0
			return nil
		}
		time.Sleep(500 * time.Millisecond)
	}

	return t.Kill9()
}

// Kill9 sends SIGKILL immediately.
func (t *WeedTarget) Kill9() error {
	if t.pid == 0 {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	t.node.Run(ctx, fmt.Sprintf("kill -9 %d", t.pid))
	t.pid = 0
	return nil
}

// Restart stops and starts weed volume (preserving the volume file).
func (t *WeedTarget) Restart(ctx context.Context) error {
	if err := t.Stop(ctx); err != nil {
		return fmt.Errorf("restart stop: %w", err)
	}
	return t.Start(ctx, false)
}

// WaitForPort polls until the iSCSI port is listening.
func (t *WeedTarget) WaitForPort(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return fmt.Errorf("wait for port %d: %w", t.config.Port, ctx.Err())
		default:
		}

		stdout, _, code, _ := t.node.Run(ctx, fmt.Sprintf("ss -tln | grep :%d", t.config.Port))
		if code == 0 && strings.Contains(stdout, fmt.Sprintf(":%d", t.config.Port)) {
			return nil
		}
		time.Sleep(200 * time.Millisecond)
	}
}

// CollectLog downloads the log file contents.
func (t *WeedTarget) CollectLog() (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	stdout, _, _, err := t.node.Run(ctx, fmt.Sprintf("cat %s 2>/dev/null", t.logFile))
	if err != nil {
		return "", err
	}
	return stdout, nil
}

// Cleanup removes the block directory, volume files, and log.
func (t *WeedTarget) Cleanup(ctx context.Context) {
	t.node.Run(ctx, fmt.Sprintf("rm -rf %s %s", t.blockDir, t.logFile))
}

// IQN returns the expected IQN for the volume.
func (t *WeedTarget) IQN() string {
	return t.iqnPrefix + t.volName()
}

// volName derives the volume name from the config IQN or a default.
func (t *WeedTarget) volName() string {
	// Use IQN suffix if set, otherwise "test"
	if t.config.IQN != "" {
		parts := strings.Split(t.config.IQN, ":")
		if len(parts) > 1 {
			return parts[len(parts)-1]
		}
	}
	return "test"
}

// PID returns the current process ID.
func (t *WeedTarget) PID() int { return t.pid }

// VolFilePath returns the remote volume file path.
func (t *WeedTarget) VolFilePath() string { return t.volFile }

// LogFile returns the remote log file path.
func (t *WeedTarget) LogFile() string { return t.logFile }
