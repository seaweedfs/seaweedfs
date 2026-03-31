package testrunner

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestCreateRunBundle_CreatesDirectoryAndFiles(t *testing.T) {
	tmpDir := t.TempDir()

	// Write a minimal scenario file.
	scenarioContent := "name: test-bundle\ntimeout: 1m\nphases:\n- name: test\n  actions:\n  - action: print\n    msg: hello\n"
	scenarioFile := filepath.Join(tmpDir, "test.yaml")
	os.WriteFile(scenarioFile, []byte(scenarioContent), 0644)

	bundle, err := CreateRunBundle(filepath.Join(tmpDir, "results"), scenarioFile, []string{"run", "test.yaml"})
	if err != nil {
		t.Fatalf("CreateRunBundle: %v", err)
	}

	// Run directory exists.
	if _, err := os.Stat(bundle.Dir); err != nil {
		t.Fatalf("run dir missing: %v", err)
	}

	// Artifacts subdirectory exists.
	if _, err := os.Stat(bundle.ArtifactsDir()); err != nil {
		t.Fatalf("artifacts dir missing: %v", err)
	}

	// manifest.json exists and is valid.
	manifestData, err := os.ReadFile(filepath.Join(bundle.Dir, "manifest.json"))
	if err != nil {
		t.Fatalf("read manifest: %v", err)
	}
	var manifest RunManifest
	if err := json.Unmarshal(manifestData, &manifest); err != nil {
		t.Fatalf("parse manifest: %v", err)
	}
	if manifest.RunID == "" {
		t.Error("RunID is empty")
	}
	if manifest.ScenarioName != "test-bundle" {
		t.Errorf("ScenarioName = %q, want test-bundle", manifest.ScenarioName)
	}
	if manifest.ScenarioSHA256 == "" {
		t.Error("ScenarioSHA256 is empty")
	}
	if manifest.StartedAt == "" {
		t.Error("StartedAt is empty")
	}

	// scenario.yaml is a frozen copy.
	copied, err := os.ReadFile(filepath.Join(bundle.Dir, "scenario.yaml"))
	if err != nil {
		t.Fatalf("read scenario copy: %v", err)
	}
	if string(copied) != scenarioContent {
		t.Errorf("scenario copy mismatch: got %q", string(copied))
	}

	// Run ID matches directory name.
	dirName := filepath.Base(bundle.Dir)
	if dirName != manifest.RunID {
		t.Errorf("dir name %q != RunID %q", dirName, manifest.RunID)
	}
}

func TestRunBundle_Finalize_WritesAllOutputs(t *testing.T) {
	tmpDir := t.TempDir()

	scenarioFile := filepath.Join(tmpDir, "test.yaml")
	os.WriteFile(scenarioFile, []byte("name: finalize-test\ntimeout: 1m\nphases:\n- name: test\n  actions:\n  - action: print\n    msg: hello\n"), 0644)

	bundle, err := CreateRunBundle(filepath.Join(tmpDir, "results"), scenarioFile, []string{"run"})
	if err != nil {
		t.Fatalf("CreateRunBundle: %v", err)
	}

	result := &ScenarioResult{
		Name:     "finalize-test",
		Status:   StatusPass,
		Duration: 5 * time.Second,
		Phases: []PhaseResult{
			{Name: "setup", Status: StatusPass, Duration: 1 * time.Second},
		},
	}

	if err := bundle.Finalize(result); err != nil {
		t.Fatalf("Finalize: %v", err)
	}

	// result.json exists.
	if _, err := os.Stat(filepath.Join(bundle.Dir, "result.json")); err != nil {
		t.Error("result.json missing")
	}
	// result.xml exists.
	if _, err := os.Stat(filepath.Join(bundle.Dir, "result.xml")); err != nil {
		t.Error("result.xml missing")
	}
	// result.html exists.
	if _, err := os.Stat(filepath.Join(bundle.Dir, "result.html")); err != nil {
		t.Error("result.html missing")
	}

	// manifest.json updated with FinishedAt and Status.
	manifestData, _ := os.ReadFile(filepath.Join(bundle.Dir, "manifest.json"))
	var manifest RunManifest
	json.Unmarshal(manifestData, &manifest)
	if manifest.FinishedAt == "" {
		t.Error("FinishedAt not set after Finalize")
	}
	if manifest.Status != "PASS" {
		t.Errorf("Status = %q, want PASS", manifest.Status)
	}
}

func TestRunBundle_UniqueRunIDs(t *testing.T) {
	tmpDir := t.TempDir()
	scenarioFile := filepath.Join(tmpDir, "test.yaml")
	os.WriteFile(scenarioFile, []byte("name: unique-test\ntimeout: 1m\nphases:\n- name: test\n  actions:\n  - action: print\n    msg: hello\n"), 0644)

	ids := make(map[string]bool)
	for i := 0; i < 10; i++ {
		bundle, err := CreateRunBundle(filepath.Join(tmpDir, "results"), scenarioFile, nil)
		if err != nil {
			t.Fatalf("iteration %d: %v", i, err)
		}
		id := bundle.Manifest.RunID
		if ids[id] {
			t.Fatalf("duplicate RunID: %s", id)
		}
		ids[id] = true
	}
}

func TestRunBundle_CommandLineRecorded(t *testing.T) {
	tmpDir := t.TempDir()
	scenarioFile := filepath.Join(tmpDir, "test.yaml")
	os.WriteFile(scenarioFile, []byte("name: cmd-test\ntimeout: 1m\nphases:\n- name: test\n  actions:\n  - action: print\n    msg: hello\n"), 0644)

	bundle, err := CreateRunBundle(filepath.Join(tmpDir, "results"), scenarioFile,
		[]string{"sw-test-runner", "run", "--tiers", "block", "test.yaml"})
	if err != nil {
		t.Fatalf("CreateRunBundle: %v", err)
	}

	if !strings.Contains(bundle.Manifest.CommandLine, "--tiers") {
		t.Errorf("CommandLine = %q, want to contain --tiers", bundle.Manifest.CommandLine)
	}
}
