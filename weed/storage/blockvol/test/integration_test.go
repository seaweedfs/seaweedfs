//go:build integration

package test

import (
	"context"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

var (
	flagEnv        = flag.String("env", "wsl2", "wsl2 or remote")
	flagTargetHost = flag.String("target-host", "127.0.0.1", "target node IP (SSH)")
	flagClientHost = flag.String("client-host", "127.0.0.1", "initiator node IP (SSH)")
	flagISCSIHost  = flag.String("iscsi-host", "", "iSCSI target IP for discovery/login (defaults to target-host)")
	flagSSHKey     = flag.String("ssh-key", "", "SSH private key path")
	flagSSHUser    = flag.String("ssh-user", "testdev", "SSH user")
	flagRepoDir    = flag.String("repo-dir", "C:/work/seaweedfs", "seaweedfs repo path")
)

// Global state shared across tests.
var (
	targetNode *Node
	clientNode *Node
	artifacts  *ArtifactCollector
)

const iqnPrefix = "iqn.2024.com.seaweedfs:test"

func TestMain(m *testing.M) {
	flag.Parse()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	// Setup nodes
	if *flagEnv == "wsl2" {
		targetNode = &Node{IsLocal: true}
		clientNode = targetNode // same node for WSL2
	} else {
		targetNode = &Node{Host: *flagTargetHost, User: *flagSSHUser, KeyFile: *flagSSHKey}
		clientNode = &Node{Host: *flagClientHost, User: *flagSSHUser, KeyFile: *flagSSHKey}
	}

	if err := targetNode.Connect(); err != nil {
		fmt.Fprintf(os.Stderr, "FATAL: target connect: %v\n", err)
		os.Exit(1)
	}
	if clientNode != targetNode {
		if err := clientNode.Connect(); err != nil {
			fmt.Fprintf(os.Stderr, "FATAL: client connect: %v\n", err)
			os.Exit(1)
		}
	}

	// Preflight: print versions
	preflight(ctx)

	// Build target binary
	fmt.Println("=== Building iscsi-target binary ===")
	tgt := NewTarget(targetNode, DefaultTargetConfig())
	if err := tgt.Build(ctx, *flagRepoDir); err != nil {
		fmt.Fprintf(os.Stderr, "FATAL: build target: %v\n", err)
		os.Exit(1)
	}
	if err := tgt.Deploy(*flagRepoDir + "/iscsi-target-linux"); err != nil {
		fmt.Fprintf(os.Stderr, "FATAL: deploy target: %v\n", err)
		os.Exit(1)
	}
	fmt.Println("=== Build + deploy complete ===")

	// Setup artifact collector (no Target -- each test provides its own)
	artDir, _ := filepath.Abs("artifacts")
	artifacts = NewArtifactCollector(artDir, clientNode)

	// Run tests
	code := m.Run()

	// Global cleanup (unconditional)
	cleanup()

	os.Exit(code)
}

func preflight(ctx context.Context) {
	fmt.Println("=== Preflight ===")
	checks := []struct {
		name string
		cmd  string
		node *Node
	}{
		{"fio", "fio --version", clientNode},
		{"iscsiadm", "iscsiadm --version 2>&1", clientNode},
		{"go", "go version", targetNode},
		{"kernel", "uname -r", targetNode},
	}
	for _, c := range checks {
		stdout, _, code, err := c.node.Run(ctx, c.cmd)
		if err != nil || code != 0 {
			fmt.Printf("  %-10s MISSING\n", c.name)
		} else {
			fmt.Printf("  %-10s %s\n", c.name, firstLine(stdout))
		}
	}
	fmt.Println("=== End Preflight ===")
}

func cleanup() {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	fmt.Println("=== Global Cleanup ===")

	iscsi := NewISCSIClient(clientNode)
	iscsi.CleanupAll(ctx, iqnPrefix)

	// Unmount any test mount points
	clientNode.RunRoot(ctx, "umount -f /tmp/blockvol-mnt 2>/dev/null")

	// Kill any leftover target process
	targetNode.Run(ctx, "pkill -f iscsi-target-test 2>/dev/null")

	// Remove temp files
	targetNode.Run(ctx, "rm -f /tmp/blockvol-test.blk /tmp/blockvol-test.blk.wal /tmp/iscsi-target-test /tmp/iscsi-target-test.log")

	if clientNode != targetNode {
		clientNode.Close()
	}
	targetNode.Close()

	fmt.Println("=== Cleanup Done ===")
}

// TestHarnessSelfCheck validates the test framework itself.
// Run first: go test -tags integration -run TestHarnessSelfCheck -v
func TestHarnessSelfCheck(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	cfg := DefaultTargetConfig()
	cfg.IQN = iqnPrefix + "-harness"
	cfg.VolSize = "50M"
	tgt := NewTarget(targetNode, cfg)
	iscsi := NewISCSIClient(clientNode)
	host := targetHost()

	t.Cleanup(func() {
		iscsi.Logout(ctx, cfg.IQN)
		tgt.Stop(ctx)
		tgt.Cleanup(ctx)
	})
	t.Cleanup(func() { artifacts.Collect(t, tgt) })

	// Start target
	t.Log("starting target...")
	if err := tgt.Start(ctx, true); err != nil {
		t.Fatalf("start target: %v", err)
	}

	// Discovery
	t.Log("discovering...")
	iqns, err := iscsi.Discover(ctx, host, cfg.Port)
	if err != nil {
		t.Fatalf("discover: %v", err)
	}
	found := false
	for _, iqn := range iqns {
		if iqn == cfg.IQN {
			found = true
		}
	}
	if !found {
		t.Fatalf("IQN %s not in discovery: %v", cfg.IQN, iqns)
	}

	// Login
	t.Log("logging in...")
	dev, err := iscsi.Login(ctx, cfg.IQN)
	if err != nil {
		t.Fatalf("login: %v", err)
	}
	t.Logf("device: %s", dev)

	// DD 1MB write + read + verify
	t.Log("dd write/read verify...")
	_, _, code, err := clientNode.RunRoot(ctx,
		fmt.Sprintf("dd if=/dev/urandom of=%s bs=1M count=1 oflag=direct 2>/dev/null", dev))
	if err != nil || code != 0 {
		t.Fatalf("dd write failed: code=%d err=%v", code, err)
	}

	wSum, _, _, _ := clientNode.RunRoot(ctx,
		fmt.Sprintf("dd if=%s bs=1M count=1 iflag=direct 2>/dev/null | md5sum", dev))
	t.Logf("md5: %s", firstLine(wSum))

	// Logout
	t.Log("logging out...")
	if err := iscsi.Logout(ctx, cfg.IQN); err != nil {
		t.Fatalf("logout: %v", err)
	}

	// Stop target
	t.Log("stopping target...")
	if err := tgt.Stop(ctx); err != nil {
		t.Fatalf("stop: %v", err)
	}

	t.Log("harness self-check passed")
}

// targetHost returns the iSCSI target address for discovery/login from the initiator.
// Uses -iscsi-host if set, otherwise falls back to -target-host.
func targetHost() string {
	if *flagEnv == "wsl2" {
		return "127.0.0.1"
	}
	if *flagISCSIHost != "" {
		return *flagISCSIHost
	}
	return *flagTargetHost
}

func firstLine(s string) string {
	for i, c := range s {
		if c == '\n' || c == '\r' {
			return s[:i]
		}
	}
	return s
}

// newTestTarget creates a target with test-specific IQN, unique vol file, and cleanup.
// Tests must not run in parallel -- they share the same target node and port.
func newTestTarget(t *testing.T, volSize, walSize string) (*Target, *ISCSIClient, string) {
	cfg := DefaultTargetConfig()
	// Sanitize test name for IQN -- replace / with - (subtests use /)
	name := strings.ReplaceAll(t.Name(), "/", "-")
	cfg.IQN = iqnPrefix + "-" + strings.ToLower(name)
	if volSize != "" {
		cfg.VolSize = volSize
	}
	if walSize != "" {
		cfg.WALSize = walSize
	}

	tgt := NewTarget(targetNode, cfg)
	iscsi := NewISCSIClient(clientNode)
	host := targetHost()

	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		iscsi.Logout(ctx, cfg.IQN)
		tgt.Stop(ctx)
		tgt.Cleanup(ctx)
	})
	t.Cleanup(func() { artifacts.Collect(t, tgt) })

	return tgt, iscsi, host
}

// startAndLogin creates volume, starts target, discovers, logs in, returns device.
func startAndLogin(t *testing.T, ctx context.Context, tgt *Target, iscsi *ISCSIClient, host string) string {
	t.Helper()
	if err := tgt.Start(ctx, true); err != nil {
		t.Fatalf("start target: %v", err)
	}
	if _, err := iscsi.Discover(ctx, host, tgt.config.Port); err != nil {
		t.Fatalf("discover: %v", err)
	}
	dev, err := iscsi.Login(ctx, tgt.config.IQN)
	if err != nil {
		t.Fatalf("login: %v", err)
	}
	return dev
}
