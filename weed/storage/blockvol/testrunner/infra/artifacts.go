package infra

import (
	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"time"
)

// LogCollector is implemented by any target that can provide a log file.
type LogCollector interface {
	CollectLog() (string, error)
}

// ArtifactCollector gathers diagnostic info on failure.
type ArtifactCollector struct {
	Dir  string // base artifacts directory
	Node *Node  // initiator node for dmesg/lsblk
	Log  *log.Logger
}

// NewArtifactCollector creates a collector rooted at the given directory.
func NewArtifactCollector(dir string, node *Node, logger *log.Logger) *ArtifactCollector {
	if logger == nil {
		logger = log.Default()
	}
	return &ArtifactCollector{
		Dir:  dir,
		Node: node,
		Log:  logger,
	}
}

// Collect gathers diagnostics when failed is true.
func (a *ArtifactCollector) Collect(failed bool, tgt LogCollector, label string) {
	if !failed {
		return
	}
	a.CollectLabeled(tgt, label)
}

// CollectLabeled gathers diagnostics unconditionally.
func (a *ArtifactCollector) CollectLabeled(tgt LogCollector, label string) {
	ts := time.Now().Format("20060102-150405")
	testDir := filepath.Join(a.Dir, ts)
	if err := os.MkdirAll(testDir, 0755); err != nil {
		a.Log.Printf("artifacts: mkdir failed: %v", err)
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if tgt != nil {
		if logContent, err := tgt.CollectLog(); err == nil && logContent != "" {
			a.writeArtifact(filepath.Join(testDir, label+".log"), logContent)
		}
	}

	if stdout, _, _, err := a.Node.RunRoot(ctx, "iscsiadm -m session 2>&1"); err == nil {
		a.writeArtifact(filepath.Join(testDir, "iscsi-session.txt"), stdout)
	}

	if stdout, _, _, err := a.Node.RunRoot(ctx, "dmesg | tail -200"); err == nil {
		a.writeArtifact(filepath.Join(testDir, "dmesg.txt"), stdout)
	}

	if stdout, _, _, err := a.Node.Run(ctx, "lsblk 2>&1"); err == nil {
		a.writeArtifact(filepath.Join(testDir, "lsblk.txt"), stdout)
	}

	a.Log.Printf("artifacts saved to %s", testDir)
}

func (a *ArtifactCollector) writeArtifact(path, content string) {
	if err := os.WriteFile(path, []byte(content), 0644); err != nil {
		a.Log.Printf("artifacts: write %s: %v", path, err)
	} else {
		a.Log.Printf("artifacts: wrote %s (%d bytes)", filepath.Base(path), len(content))
	}
}

// CollectPerf saves performance results to a timestamped JSON file.
func (a *ArtifactCollector) CollectPerf(name string, data string) {
	ts := time.Now().Format("20060102-150405")
	path := filepath.Join(a.Dir, fmt.Sprintf("perf-%s-%s.json", name, ts))
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		a.Log.Printf("artifacts: mkdir failed: %v", err)
		return
	}
	a.writeArtifact(path, data)
}
