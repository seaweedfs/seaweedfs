package testrunner

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"
)

// RunManifest records the identity and provenance of a single test run.
// Written to manifest.json in the run bundle directory.
type RunManifest struct {
	RunID          string `json:"run_id"`
	StartedAt      string `json:"started_at"`
	FinishedAt     string `json:"finished_at,omitempty"`
	ScenarioName   string `json:"scenario_name"`
	ScenarioFile   string `json:"scenario_file"`
	ScenarioSHA256 string `json:"scenario_sha256"`
	RunnerVersion  string `json:"runner_version,omitempty"`
	GitSHA         string `json:"git_sha,omitempty"`
	Host           string `json:"host,omitempty"`
	Status         string `json:"status,omitempty"`
	CommandLine    string `json:"command_line,omitempty"`
}

// RunBundle manages the per-run output directory.
type RunBundle struct {
	Dir          string // absolute path to the run directory
	Manifest     RunManifest
	scenarioData []byte // frozen copy of the input YAML
}

// CreateRunBundle creates a timestamped run directory under resultsRoot.
// Directory name: YYYYMMDD-HHMMSS-<short-id>
// Creates: manifest.json (partial), scenario.yaml (frozen copy).
func CreateRunBundle(resultsRoot, scenarioFile string, cmdLine []string) (*RunBundle, error) {
	now := time.Now()

	// Read and hash the scenario file.
	scenarioData, err := os.ReadFile(scenarioFile)
	if err != nil {
		return nil, fmt.Errorf("read scenario: %w", err)
	}
	h := sha256.Sum256(scenarioData)
	scenarioHash := hex.EncodeToString(h[:])

	// Parse scenario name from the file (with correct base dir for includes).
	scenario, err := ParseWithBase(scenarioData, filepath.Dir(scenarioFile))
	if err != nil {
		return nil, fmt.Errorf("parse scenario for manifest: %w", err)
	}

	// Generate run ID: timestamp + short hash of (scenario + time).
	ts := now.Format("20060102-150405")
	idSeed := sha256.Sum256([]byte(fmt.Sprintf("%s-%d", scenarioFile, now.UnixNano())))
	shortID := hex.EncodeToString(idSeed[:2]) // 4 hex chars
	runID := ts + "-" + shortID

	// Create directory.
	runDir := filepath.Join(resultsRoot, runID)
	if err := os.MkdirAll(runDir, 0755); err != nil {
		return nil, fmt.Errorf("create run dir: %w", err)
	}
	if err := os.MkdirAll(filepath.Join(runDir, "artifacts"), 0755); err != nil {
		return nil, fmt.Errorf("create artifacts dir: %w", err)
	}

	// Build manifest.
	manifest := RunManifest{
		RunID:          runID,
		StartedAt:      now.UTC().Format(time.RFC3339),
		ScenarioName:   scenario.Name,
		ScenarioFile:   scenarioFile,
		ScenarioSHA256: scenarioHash,
		RunnerVersion:  Version(),
		GitSHA:         gitSHA(),
		Host:           hostname(),
		CommandLine:    strings.Join(cmdLine, " "),
	}

	b := &RunBundle{
		Dir:          runDir,
		Manifest:     manifest,
		scenarioData: scenarioData,
	}

	// Write frozen scenario copy.
	scenarioDst := filepath.Join(runDir, "scenario.yaml")
	if err := os.WriteFile(scenarioDst, scenarioData, 0644); err != nil {
		return nil, fmt.Errorf("write scenario copy: %w", err)
	}

	// Write initial manifest (will be updated at finalize).
	if err := b.writeManifest(); err != nil {
		return nil, err
	}

	return b, nil
}

// Finalize writes the final result files into the run bundle.
func (b *RunBundle) Finalize(result *ScenarioResult) error {
	// Update manifest with final status and time.
	b.Manifest.FinishedAt = time.Now().UTC().Format(time.RFC3339)
	b.Manifest.Status = string(result.Status)
	if err := b.writeManifest(); err != nil {
		return err
	}

	// Write result.json.
	if err := WriteJSON(result, filepath.Join(b.Dir, "result.json")); err != nil {
		return fmt.Errorf("write result.json: %w", err)
	}

	// Write result.xml (JUnit).
	if err := WriteJUnitXML(result, filepath.Join(b.Dir, "result.xml")); err != nil {
		return fmt.Errorf("write result.xml: %w", err)
	}

	// Write result.html.
	if err := WriteHTMLReport(result, filepath.Join(b.Dir, "result.html")); err != nil {
		return fmt.Errorf("write result.html: %w", err)
	}

	return nil
}

// ArtifactsDir returns the path to the artifacts subdirectory.
func (b *RunBundle) ArtifactsDir() string {
	return filepath.Join(b.Dir, "artifacts")
}

func (b *RunBundle) writeManifest() error {
	data, err := json.MarshalIndent(b.Manifest, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal manifest: %w", err)
	}
	return os.WriteFile(filepath.Join(b.Dir, "manifest.json"), data, 0644)
}

// CopyArtifact copies a file into the run bundle's artifacts directory.
func (b *RunBundle) CopyArtifact(src, name string) error {
	dst := filepath.Join(b.ArtifactsDir(), name)
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()
	out, err := os.Create(dst)
	if err != nil {
		return err
	}
	defer out.Close()
	_, err = io.Copy(out, in)
	return err
}

func hostname() string {
	h, _ := os.Hostname()
	return h
}

func gitSHA() string {
	out, err := exec.Command("git", "rev-parse", "--short", "HEAD").Output()
	if err != nil {
		return ""
	}
	return strings.TrimSpace(string(out))
}

// Version returns the runner version. Set at build time via ldflags.
var version = "dev"

func Version() string {
	return version
}
