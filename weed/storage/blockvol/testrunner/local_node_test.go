package testrunner

import (
	"context"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"
)

func skipIfWindows(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("LocalNode tests require Unix shell")
	}
}

func TestLocalNode_Run_Echo(t *testing.T) {
	skipIfWindows(t)
	n := NewLocalNode("test")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	stdout, stderr, code, err := n.Run(ctx, "echo hello")
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if code != 0 {
		t.Fatalf("exit code %d, stderr: %s", code, stderr)
	}
	if got := stdout; got != "hello\n" {
		t.Errorf("stdout = %q, want %q", got, "hello\n")
	}
}

func TestLocalNode_Run_ExitCode(t *testing.T) {
	skipIfWindows(t)
	n := NewLocalNode("test")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, _, code, err := n.Run(ctx, "exit 42")
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if code != 42 {
		t.Errorf("exit code = %d, want 42", code)
	}
}

func TestLocalNode_Run_Timeout(t *testing.T) {
	skipIfWindows(t)
	n := NewLocalNode("test")
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	_, _, _, err := n.Run(ctx, "sleep 10")
	if err == nil {
		t.Fatal("expected timeout error")
	}
}

func TestLocalNode_RunRoot_NonRoot(t *testing.T) {
	skipIfWindows(t)
	n := NewLocalNode("test")
	// Force non-root to test sudo path.
	n.isRoot = false
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// sudo -n will likely fail without sudoers config, but should not panic.
	_, _, _, err := n.RunRoot(ctx, "echo test")
	// We only care that it doesn't panic — error is expected on most CI.
	_ = err
}

func TestLocalNode_Upload(t *testing.T) {
	skipIfWindows(t)
	n := NewLocalNode("test")

	dir := t.TempDir()
	src := filepath.Join(dir, "src.bin")
	dst := filepath.Join(dir, "subdir", "dst.bin")

	// Create source file.
	content := []byte("test binary content 12345")
	if err := os.WriteFile(src, content, 0644); err != nil {
		t.Fatalf("write src: %v", err)
	}

	// Upload (should create subdir).
	if err := n.Upload(src, dst); err != nil {
		t.Fatalf("Upload: %v", err)
	}

	// Verify.
	got, err := os.ReadFile(dst)
	if err != nil {
		t.Fatalf("read dst: %v", err)
	}
	if string(got) != string(content) {
		t.Errorf("content mismatch: got %q", got)
	}

	// Verify executable permission.
	info, _ := os.Stat(dst)
	if info.Mode()&0100 == 0 {
		t.Error("file is not executable")
	}
}

func TestLocalNode_Close(t *testing.T) {
	n := NewLocalNode("test")
	n.Close() // should be no-op, no panic
}

func TestLocalNode_DetectRoot(t *testing.T) {
	skipIfWindows(t)
	n := NewLocalNode("test")
	// We just verify it doesn't panic and returns a bool.
	_ = n.IsRoot()
}
