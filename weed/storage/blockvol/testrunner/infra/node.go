package infra

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"runtime"
	"strings"
	"sync"
	"time"

	"golang.org/x/crypto/ssh"
)

// Node represents an SSH-accessible (or local WSL2/native) machine.
type Node struct {
	Host     string
	User     string
	KeyFile  string
	IsLocal  bool // WSL2 mode: use exec.CommandContext via wsl instead of SSH
	IsNative bool // Native local mode: use bash -c directly (for Linux agents)

	mu     sync.Mutex
	client *ssh.Client
}

// Connect establishes the SSH connection (no-op for local/native mode).
func (n *Node) Connect() error {
	if n.IsLocal || n.IsNative {
		return nil
	}

	key, err := os.ReadFile(n.KeyFile)
	if err != nil {
		return fmt.Errorf("read SSH key %s: %w", n.KeyFile, err)
	}
	signer, err := ssh.ParsePrivateKey(key)
	if err != nil {
		return fmt.Errorf("parse SSH key: %w", err)
	}

	config := &ssh.ClientConfig{
		User:            n.User,
		Auth:            []ssh.AuthMethod{ssh.PublicKeys(signer)},
		HostKeyCallback: ssh.InsecureIgnoreHostKey(),
		Timeout:         10 * time.Second,
	}

	addr := n.Host
	if !strings.Contains(addr, ":") {
		addr += ":22"
	}

	n.mu.Lock()
	defer n.mu.Unlock()
	n.client, err = ssh.Dial("tcp", addr, config)
	if err != nil {
		return fmt.Errorf("SSH dial %s: %w", addr, err)
	}
	return nil
}

// Run executes a command and returns stdout, stderr, exit code.
func (n *Node) Run(ctx context.Context, cmd string) (stdout, stderr string, exitCode int, err error) {
	if n.IsNative {
		return n.runNative(ctx, cmd)
	}
	if n.IsLocal {
		return n.runLocal(ctx, cmd)
	}
	return n.runSSH(ctx, cmd)
}

func (n *Node) runNative(ctx context.Context, cmd string) (string, string, int, error) {
	c := exec.CommandContext(ctx, "bash", "-c", cmd)
	var outBuf, errBuf bytes.Buffer
	c.Stdout = &outBuf
	c.Stderr = &errBuf

	err := c.Run()
	if ctx.Err() != nil {
		return outBuf.String(), errBuf.String(), -1, fmt.Errorf("command timed out: %w", ctx.Err())
	}
	if err != nil {
		if exitErr, ok := err.(*exec.ExitError); ok {
			return outBuf.String(), errBuf.String(), exitErr.ExitCode(), nil
		}
		return outBuf.String(), errBuf.String(), -1, err
	}
	return outBuf.String(), errBuf.String(), 0, nil
}

func (n *Node) runLocal(ctx context.Context, cmd string) (string, string, int, error) {
	var c *exec.Cmd
	if runtime.GOOS == "windows" {
		c = exec.CommandContext(ctx, "wsl", "-e", "bash", "-c", cmd)
	} else {
		c = exec.CommandContext(ctx, "bash", "-c", cmd)
	}
	var outBuf, errBuf bytes.Buffer
	c.Stdout = &outBuf
	c.Stderr = &errBuf

	err := c.Run()
	if ctx.Err() != nil {
		return outBuf.String(), errBuf.String(), -1, fmt.Errorf("command timed out: %w", ctx.Err())
	}
	if err != nil {
		if exitErr, ok := err.(*exec.ExitError); ok {
			return outBuf.String(), errBuf.String(), exitErr.ExitCode(), nil
		}
		return outBuf.String(), errBuf.String(), -1, err
	}
	return outBuf.String(), errBuf.String(), 0, nil
}

func (n *Node) runSSH(ctx context.Context, cmd string) (string, string, int, error) {
	n.mu.Lock()
	if n.client == nil {
		n.mu.Unlock()
		return "", "", -1, fmt.Errorf("SSH not connected")
	}
	session, err := n.client.NewSession()
	n.mu.Unlock()
	if err != nil {
		return "", "", -1, fmt.Errorf("new SSH session: %w", err)
	}
	defer session.Close()

	var outBuf, errBuf bytes.Buffer
	session.Stdout = &outBuf
	session.Stderr = &errBuf

	done := make(chan error, 1)
	go func() { done <- session.Run(cmd) }()

	select {
	case <-ctx.Done():
		_ = session.Signal(ssh.SIGKILL)
		return outBuf.String(), errBuf.String(), -1, fmt.Errorf("command timed out: %w", ctx.Err())
	case err := <-done:
		if err != nil {
			if exitErr, ok := err.(*ssh.ExitError); ok {
				return outBuf.String(), errBuf.String(), exitErr.ExitStatus(), nil
			}
			return outBuf.String(), errBuf.String(), -1, err
		}
		return outBuf.String(), errBuf.String(), 0, nil
	}
}

// RunRoot executes a command with sudo -n (non-interactive).
// Compound commands (containing ; && || |) are wrapped in sh -c '...'
// to ensure the entire command runs under sudo, not just the first part.
func (n *Node) RunRoot(ctx context.Context, cmd string) (string, string, int, error) {
	if strings.ContainsAny(cmd, ";|&") {
		// Escape single quotes in cmd for sh -c wrapping.
		escaped := strings.ReplaceAll(cmd, "'", "'\"'\"'")
		return n.Run(ctx, "sudo -n sh -c '"+escaped+"'")
	}
	return n.Run(ctx, "sudo -n "+cmd)
}

// Upload copies a local file to the remote node via SCP.
func (n *Node) Upload(local, remote string) error {
	if n.IsNative {
		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer cancel()
		_, stderr, code, err := n.Run(ctx, fmt.Sprintf("cp %s %s && chmod +x %s", local, remote, remote))
		if err != nil || code != 0 {
			return fmt.Errorf("native upload: code=%d stderr=%s err=%v", code, stderr, err)
		}
		return nil
	}
	if n.IsLocal {
		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer cancel()
		src := local
		if runtime.GOOS == "windows" {
			src = ToWSLPath(local)
		}
		_, stderr, code, err := n.Run(ctx, fmt.Sprintf("cp %s %s && chmod +x %s", src, remote, remote))
		if err != nil || code != 0 {
			return fmt.Errorf("local upload: code=%d stderr=%s err=%v", code, stderr, err)
		}
		return nil
	}
	return n.scpUpload(local, remote)
}

func (n *Node) scpUpload(local, remote string) error {
	data, err := os.ReadFile(local)
	if err != nil {
		return fmt.Errorf("read local file %s: %w", local, err)
	}

	n.mu.Lock()
	if n.client == nil {
		n.mu.Unlock()
		return fmt.Errorf("SSH not connected")
	}
	session, err := n.client.NewSession()
	n.mu.Unlock()
	if err != nil {
		return fmt.Errorf("new SSH session: %w", err)
	}
	defer session.Close()

	// Use cat+chmod over stdin — simpler and more reliable than SCP protocol.
	w, err := session.StdinPipe()
	if err != nil {
		return fmt.Errorf("stdin pipe: %w", err)
	}

	go func() {
		w.Write(data)
		w.Close()
	}()

	cmd := fmt.Sprintf("cat > %s && chmod +x %s", remote, remote)
	if err := session.Run(cmd); err != nil {
		return fmt.Errorf("upload run: %w", err)
	}
	return nil
}

// Download copies a remote file to local via SCP.
func (n *Node) Download(remote, local string) error {
	if n.IsNative {
		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer cancel()
		_, stderr, code, err := n.Run(ctx, fmt.Sprintf("cp %s %s", remote, local))
		if err != nil || code != 0 {
			return fmt.Errorf("native download: code=%d stderr=%s err=%v", code, stderr, err)
		}
		return nil
	}
	if n.IsLocal {
		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer cancel()
		dst := local
		if runtime.GOOS == "windows" {
			dst = ToWSLPath(local)
		}
		_, stderr, code, err := n.Run(ctx, fmt.Sprintf("cp %s %s", remote, dst))
		if err != nil || code != 0 {
			return fmt.Errorf("local download: code=%d stderr=%s err=%v", code, stderr, err)
		}
		return nil
	}
	return n.scpDownload(remote, local)
}

func (n *Node) scpDownload(remote, local string) error {
	n.mu.Lock()
	if n.client == nil {
		n.mu.Unlock()
		return fmt.Errorf("SSH not connected")
	}
	session, err := n.client.NewSession()
	n.mu.Unlock()
	if err != nil {
		return fmt.Errorf("new SSH session: %w", err)
	}
	defer session.Close()

	var buf bytes.Buffer
	session.Stdout = &buf
	if err := session.Run(fmt.Sprintf("cat %s", remote)); err != nil {
		return fmt.Errorf("read remote %s: %w", remote, err)
	}
	return os.WriteFile(local, buf.Bytes(), 0644)
}

// Kill sends SIGKILL to a process by PID.
func (n *Node) Kill(pid int) error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_, _, _, err := n.RunRoot(ctx, fmt.Sprintf("kill -9 %d", pid))
	return err
}

// HasCommand checks if a command is available on the node.
func (n *Node) HasCommand(cmd string) bool {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, _, code, err := n.Run(ctx, fmt.Sprintf("which %s", cmd))
	return err == nil && code == 0
}

// Close closes the SSH connection.
func (n *Node) Close() {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.client != nil {
		n.client.Close()
		n.client = nil
	}
}

// DialTCP opens a direct TCP connection through the SSH tunnel.
func (n *Node) DialTCP(addr string) (net.Conn, error) {
	if n.IsLocal || n.IsNative {
		return net.DialTimeout("tcp", addr, 5*time.Second)
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.client == nil {
		return nil, fmt.Errorf("SSH not connected")
	}
	return n.client.Dial("tcp", addr)
}

// StreamRun executes a command and streams stdout to the writer.
func (n *Node) StreamRun(ctx context.Context, cmd string, w io.Writer) error {
	if n.IsNative {
		c := exec.CommandContext(ctx, "bash", "-c", cmd)
		c.Stdout = w
		c.Stderr = w
		return c.Run()
	}
	if n.IsLocal {
		var c *exec.Cmd
		if runtime.GOOS == "windows" {
			c = exec.CommandContext(ctx, "wsl", "-e", "bash", "-c", cmd)
		} else {
			c = exec.CommandContext(ctx, "bash", "-c", cmd)
		}
		c.Stdout = w
		c.Stderr = w
		return c.Run()
	}

	n.mu.Lock()
	if n.client == nil {
		n.mu.Unlock()
		return fmt.Errorf("SSH not connected")
	}
	session, err := n.client.NewSession()
	n.mu.Unlock()
	if err != nil {
		return err
	}
	defer session.Close()

	session.Stdout = w
	session.Stderr = w

	done := make(chan error, 1)
	go func() { done <- session.Run(cmd) }()

	select {
	case <-ctx.Done():
		_ = session.Signal(ssh.SIGKILL)
		return ctx.Err()
	case err := <-done:
		return err
	}
}

// ToWSLPath converts a Windows path to a WSL path.
func ToWSLPath(winPath string) string {
	p := strings.ReplaceAll(winPath, "\\", "/")
	if len(p) >= 2 && p[1] == ':' {
		drive := strings.ToLower(string(p[0]))
		p = "/mnt/" + drive + p[2:]
	}
	return p
}

func remoteName(path string) string {
	parts := strings.Split(path, "/")
	return parts[len(parts)-1]
}

func remoteDir(path string) string {
	idx := strings.LastIndex(path, "/")
	if idx < 0 {
		return "."
	}
	return path[:idx]
}
