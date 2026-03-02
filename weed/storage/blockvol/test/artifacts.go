//go:build integration

package test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// LogCollector is implemented by any target that can provide a log file.
type LogCollector interface {
	CollectLog() (string, error)
}

// ArtifactCollector gathers diagnostic info on test failure.
type ArtifactCollector struct {
	dir  string // base artifacts directory
	node *Node  // initiator node for dmesg/lsblk
}

// NewArtifactCollector creates a collector rooted at the given directory.
func NewArtifactCollector(dir string, node *Node) *ArtifactCollector {
	return &ArtifactCollector{
		dir:  dir,
		node: node,
	}
}

// Collect gathers diagnostics for a failed test. Call from t.Cleanup().
// Pass any LogCollector (Target or WeedTarget) for the correct log file.
func (a *ArtifactCollector) Collect(t *testing.T, tgt LogCollector) {
	a.CollectLabeled(t, tgt, "target")
}

// CollectLabeled is like Collect but uses the given label for the log filename
// (e.g. "primary" -> "primary.log"). Use this when collecting logs from
// multiple targets in the same test to avoid overwriting.
func (a *ArtifactCollector) CollectLabeled(t *testing.T, tgt LogCollector, label string) {
	if !t.Failed() {
		return
	}

	ts := time.Now().Format("20060102-150405")
	testDir := filepath.Join(a.dir, ts, t.Name())
	if err := os.MkdirAll(testDir, 0755); err != nil {
		t.Logf("artifacts: mkdir failed: %v", err)
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Target log (from the test's own target instance)
	if tgt != nil {
		if log, err := tgt.CollectLog(); err == nil && log != "" {
			writeArtifact(t, filepath.Join(testDir, label+".log"), log)
		}
	}

	// iSCSI session state
	if stdout, _, _, err := a.node.RunRoot(ctx, "iscsiadm -m session 2>&1"); err == nil {
		writeArtifact(t, filepath.Join(testDir, "iscsi-session.txt"), stdout)
	}

	// Kernel messages (iSCSI errors)
	if stdout, _, _, err := a.node.RunRoot(ctx, "dmesg | tail -200"); err == nil {
		writeArtifact(t, filepath.Join(testDir, "dmesg.txt"), stdout)
	}

	// Block devices
	if stdout, _, _, err := a.node.Run(ctx, "lsblk 2>&1"); err == nil {
		writeArtifact(t, filepath.Join(testDir, "lsblk.txt"), stdout)
	}

	t.Logf("artifacts saved to %s", testDir)
}

func writeArtifact(t *testing.T, path, content string) {
	if err := os.WriteFile(path, []byte(content), 0644); err != nil {
		t.Logf("artifacts: write %s: %v", path, err)
	} else {
		t.Logf("artifacts: wrote %s (%d bytes)", filepath.Base(path), len(content))
	}
}

// CollectPerf saves performance results to a timestamped JSON file.
func (a *ArtifactCollector) CollectPerf(t *testing.T, name string, data string) {
	ts := time.Now().Format("20060102-150405")
	path := filepath.Join(a.dir, fmt.Sprintf("perf-%s-%s.json", name, ts))
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		t.Logf("artifacts: mkdir failed: %v", err)
		return
	}
	writeArtifact(t, path, data)
}
