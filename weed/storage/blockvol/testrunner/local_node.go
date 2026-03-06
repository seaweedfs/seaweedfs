package testrunner

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
)

// LocalNode implements NodeRunner by executing commands locally via os/exec.
// Used by agents that run on the same machine as the targets they manage.
type LocalNode struct {
	hostname string
	isRoot   bool
}

// NewLocalNode creates a LocalNode, detecting root status.
func NewLocalNode(hostname string) *LocalNode {
	n := &LocalNode{hostname: hostname}
	n.isRoot = n.detectRoot()
	return n
}

// Hostname returns the node's hostname.
func (n *LocalNode) Hostname() string { return n.hostname }

// IsRoot returns whether the process is running as root.
func (n *LocalNode) IsRoot() bool { return n.isRoot }

func (n *LocalNode) detectRoot() bool {
	// Try "id -u" to check if running as root (uid 0).
	ctx, cancel := context.WithTimeout(context.Background(), 5*secondDuration)
	defer cancel()
	out, _, code, err := n.Run(ctx, "id -u")
	if err != nil || code != 0 {
		return false
	}
	return strings.TrimSpace(out) == "0"
}

// secondDuration avoids importing time in the constant.
const secondDuration = 1_000_000_000 // time.Second

// Run executes a command locally via bash -c.
func (n *LocalNode) Run(ctx context.Context, cmd string) (stdout, stderr string, exitCode int, err error) {
	c := exec.CommandContext(ctx, "bash", "-c", cmd)
	var outBuf, errBuf bytes.Buffer
	c.Stdout = &outBuf
	c.Stderr = &errBuf

	runErr := c.Run()
	if ctx.Err() != nil {
		return outBuf.String(), errBuf.String(), -1, fmt.Errorf("command timed out: %w", ctx.Err())
	}
	if runErr != nil {
		if exitErr, ok := runErr.(*exec.ExitError); ok {
			return outBuf.String(), errBuf.String(), exitErr.ExitCode(), nil
		}
		return outBuf.String(), errBuf.String(), -1, runErr
	}
	return outBuf.String(), errBuf.String(), 0, nil
}

// RunRoot executes a command as root. If already root, runs directly.
// Otherwise uses sudo -n (non-interactive, fails if password required).
func (n *LocalNode) RunRoot(ctx context.Context, cmd string) (stdout, stderr string, exitCode int, err error) {
	if n.isRoot {
		return n.Run(ctx, cmd)
	}
	return n.Run(ctx, "sudo -n "+cmd)
}

// Upload copies a file from local source to a local destination path.
func (n *LocalNode) Upload(local, remote string) error {
	// Ensure destination directory exists.
	dir := filepath.Dir(remote)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return fmt.Errorf("mkdir %s: %w", dir, err)
	}

	src, err := os.Open(local)
	if err != nil {
		return fmt.Errorf("open source %s: %w", local, err)
	}
	defer src.Close()

	dst, err := os.OpenFile(remote, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0755)
	if err != nil {
		return fmt.Errorf("create dest %s: %w", remote, err)
	}
	defer dst.Close()

	if _, err := io.Copy(dst, src); err != nil {
		return fmt.Errorf("copy %s → %s: %w", local, remote, err)
	}
	return nil
}

// Close is a no-op for LocalNode (no connection to close).
func (n *LocalNode) Close() {}
