package testrunner

import (
	"fmt"
	"os"

	"gopkg.in/yaml.v3"
)

// SuiteConfig is the top-level YAML structure for a test suite.
// A suite deploys once, runs N scenarios, and collects evidence.
//
// Usage: sw-test-runner suite <suite.yaml>
//
// Example:
//
//	name: phase20-t6-stage1
//	topology:
//	  nodes:
//	    m01: {host: 192.168.1.181, user: testdev, key: /opt/work/testdev_key}
//	    m02: {host: 192.168.1.184, user: testdev, key: /opt/work/testdev_key}
//
//	deploy:
//	  build:
//	    goos: linux
//	    goarch: amd64
//	    targets: [weed, sw-test-runner]
//	  kill_ports: [9433, 18480, 3295]
//	  clean_dirs: ["/tmp/sw-fo-*"]
//	  binaries:
//	    - local: weed-linux
//	      remote: /tmp/sw-test-runner/weed
//	    - local: sw-test-runner-linux
//	      remote: /opt/work/sw-test-runner
//
//	scenarios:
//	  - path: scenarios/internal/recovery-baseline-failover.yaml
//	    id: P20-T6-H1A
//	  - path: scenarios/internal/suite-ha-failover.yaml
//	    id: P20-T6-H1B
//
//	evidence:
//	  glog_patterns: ["/tmp/weed.*.INFO.*"]
//	  debug_endpoints:
//	    - "http://10.0.0.1:18480/debug/block/shipper"
//	    - "http://10.0.0.3:18480/debug/block/shipper"
//	  save_to: results/phase20-t6
type SuiteConfig struct {
	Name      string            `yaml:"name"`
	Topology  Topology          `yaml:"topology"`
	Deploy    SuiteDeploy       `yaml:"deploy"`
	Scenarios []SuiteScenario   `yaml:"scenarios"`
	Evidence  SuiteEvidence     `yaml:"evidence"`
	Env       map[string]string `yaml:"env"`
}

// SuiteDeploy configures the one-time deploy stage.
type SuiteDeploy struct {
	Build     *SuiteBuild    `yaml:"build,omitempty"`
	KillPorts []int          `yaml:"kill_ports"`
	CleanDirs []string       `yaml:"clean_dirs"`
	Binaries  []SuiteBinary  `yaml:"binaries"`
	Nodes     []string       `yaml:"nodes"` // which nodes to deploy to (default: all)
}

// SuiteBuild configures cross-compilation.
type SuiteBuild struct {
	GOOS    string   `yaml:"goos"`
	GOARCH  string   `yaml:"goarch"`
	Targets []string `yaml:"targets"` // ["weed", "sw-test-runner"]
	RepoDir string   `yaml:"repo_dir,omitempty"`
}

// SuiteBinary maps a local built binary to a remote path.
type SuiteBinary struct {
	Local  string `yaml:"local"`
	Remote string `yaml:"remote"`
}

// SuiteScenario references a scenario YAML file to run.
type SuiteScenario struct {
	Path string `yaml:"path"`
	ID   string `yaml:"id"`
}

// SuiteEvidence configures post-run evidence collection.
type SuiteEvidence struct {
	GlogPatterns   []string `yaml:"glog_patterns"`
	DebugEndpoints []string `yaml:"debug_endpoints"`
	SaveTo         string   `yaml:"save_to"`
	RunNode        string   `yaml:"run_node"` // node where scenarios run (for result pull)
}

// ParseSuiteFile reads and parses a suite YAML file.
func ParseSuiteFile(path string) (*SuiteConfig, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read suite %s: %w", path, err)
	}
	return ParseSuite(data)
}

// ParseSuite parses YAML bytes into a SuiteConfig.
func ParseSuite(data []byte) (*SuiteConfig, error) {
	var suite SuiteConfig
	if err := yaml.Unmarshal(data, &suite); err != nil {
		return nil, fmt.Errorf("parse suite: %w", err)
	}
	if suite.Name == "" {
		return nil, fmt.Errorf("suite name is required")
	}
	if len(suite.Scenarios) == 0 {
		return nil, fmt.Errorf("suite must have at least one scenario")
	}
	return &suite, nil
}

// ScenarioResult with suite context.
type SuiteScenarioResult struct {
	ID     string          `json:"id"`
	Path   string          `json:"path"`
	Result *ScenarioResult `json:"result"`
}

// SuiteResult is the output of a suite run.
type SuiteResult struct {
	Name      string                `json:"name"`
	Status    string                `json:"status"` // PASS or FAIL
	Scenarios []SuiteScenarioResult `json:"scenarios"`
	Deploy    *PhaseResult          `json:"deploy,omitempty"`
	Evidence  *PhaseResult          `json:"evidence,omitempty"`
}
