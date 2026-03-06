package testrunner

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"time"
)

// Baseline is an immutable, versioned snapshot of SLO metrics from a test run.
// Stored as JSON in testrunner/baselines/ — append-only, git-tracked.
type Baseline struct {
	Version       int                    `json:"version"`
	GitSHA        string                 `json:"git_sha"`
	Scenario      string                 `json:"scenario"`
	ScenarioSHA   string                 `json:"scenario_sha256"`
	Topology      BaselineTopology       `json:"topology"`
	Config        BaselineConfig         `json:"config"`
	Timestamp     time.Time              `json:"timestamp"`
	Metrics       map[string]float64     `json:"metrics"`
	DurationSec   float64                `json:"duration_sec"`
	HardFailsPassed bool                 `json:"hard_fails_passed"`
}

// BaselineTopology describes the test cluster topology.
type BaselineTopology struct {
	Nodes   int      `json:"nodes"`
	Servers []string `json:"servers"`
}

// BaselineConfig describes the volume configuration under test.
type BaselineConfig struct {
	ReplicaFactor int    `json:"rf"`
	Durability    string `json:"durability"`
	WALSize       string `json:"wal_size"`
}

// SaveBaseline writes a baseline to the baselines directory.
// Filename: <date>-<git_sha>.json (date-first for chronological sort).
func SaveBaseline(dir string, b *Baseline) (string, error) {
	if err := os.MkdirAll(dir, 0755); err != nil {
		return "", fmt.Errorf("baseline: mkdir %s: %w", dir, err)
	}

	sha := b.GitSHA
	if len(sha) > 10 {
		sha = sha[:10]
	}
	date := b.Timestamp.Format("20060102")
	filename := fmt.Sprintf("%s-%s.json", date, sha)
	path := filepath.Join(dir, filename)

	// Append-only: fail if file already exists.
	if _, err := os.Stat(path); err == nil {
		return path, nil // idempotent: already saved
	}

	data, err := json.MarshalIndent(b, "", "  ")
	if err != nil {
		return "", fmt.Errorf("baseline: marshal: %w", err)
	}

	if err := os.WriteFile(path, data, 0644); err != nil {
		return "", fmt.Errorf("baseline: write %s: %w", path, err)
	}
	return path, nil
}

// LoadLatestBaseline loads the most recent baseline from the directory.
// Baselines are sorted by filename (which encodes date).
func LoadLatestBaseline(dir string) (*Baseline, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, fmt.Errorf("baseline: read dir %s: %w", dir, err)
	}

	var jsonFiles []string
	for _, e := range entries {
		if !e.IsDir() && filepath.Ext(e.Name()) == ".json" {
			jsonFiles = append(jsonFiles, e.Name())
		}
	}
	if len(jsonFiles) == 0 {
		return nil, fmt.Errorf("baseline: no baselines found in %s", dir)
	}

	sort.Strings(jsonFiles)
	latest := jsonFiles[len(jsonFiles)-1]

	data, err := os.ReadFile(filepath.Join(dir, latest))
	if err != nil {
		return nil, fmt.Errorf("baseline: read %s: %w", latest, err)
	}

	var b Baseline
	if err := json.Unmarshal(data, &b); err != nil {
		return nil, fmt.Errorf("baseline: unmarshal %s: %w", latest, err)
	}
	return &b, nil
}
