package testrunner

import (
	"time"
)

// Scenario is the top-level YAML structure for a test scenario.
type Scenario struct {
	Name      string            `yaml:"name"`
	Timeout   Duration          `yaml:"timeout"`
	Env       map[string]string `yaml:"env"`
	Cluster   *ClusterSpec      `yaml:"cluster,omitempty"`
	Topology  Topology          `yaml:"topology"`
	Targets   map[string]TargetSpec `yaml:"targets"`
	Phases    []Phase           `yaml:"phases"`
	Artifacts ArtifactSpec      `yaml:"artifacts"`
}

// ClusterSpec declares what cluster the scenario needs.
// If omitted, the scenario manages its own cluster lifecycle via phases.
type ClusterSpec struct {
	Require  ClusterRequire `yaml:"require"`
	Fallback string         `yaml:"fallback"` // "managed" (default), "fail", "skip"
	Cleanup  string         `yaml:"cleanup"`  // "auto" (default), "keep", "destroy"
	Managed  ManagedCluster `yaml:"managed"`
}

// ClusterRequire specifies minimum cluster requirements for attach.
type ClusterRequire struct {
	Servers      int `yaml:"servers"`       // minimum volume servers
	BlockCapable int `yaml:"block_capable"` // minimum block-capable servers (0 = don't need block)
}

// ManagedCluster defines how to create a cluster if attach fails.
type ManagedCluster struct {
	MasterPort int             `yaml:"master_port"`
	Volumes    []ManagedVolume `yaml:"volumes"`
	Node       string          `yaml:"node"` // topology node name to start processes on
	IP         string          `yaml:"ip"`   // advertised IP (default: node host)
}

// ManagedVolume defines one volume server in a managed cluster.
type ManagedVolume struct {
	Port        int    `yaml:"port"`
	BlockListen string `yaml:"block_listen"` // e.g. ":3350", empty = no block
	ExtraArgs   string `yaml:"extra_args"`
}

// Duration wraps time.Duration for YAML unmarshaling (e.g. "5m", "30s").
type Duration struct {
	time.Duration
}

func (d *Duration) UnmarshalYAML(unmarshal func(interface{}) error) error {
	var s string
	if err := unmarshal(&s); err != nil {
		return err
	}
	dur, err := time.ParseDuration(s)
	if err != nil {
		return err
	}
	d.Duration = dur
	return nil
}

func (d Duration) MarshalYAML() (interface{}, error) {
	return d.Duration.String(), nil
}

// Topology defines the set of nodes available.
type Topology struct {
	Agents map[string]string   `yaml:"agents"` // agent_name → "host:port" (coordinator mode)
	Nodes  map[string]NodeSpec `yaml:"nodes"`
}

// NodeSpec defines a remote (or local) machine.
type NodeSpec struct {
	Host    string   `yaml:"host"`
	AltIPs  []string `yaml:"alt_ips"` // additional IPs (e.g. RDMA) for node identification
	User    string   `yaml:"user"`
	KeyFile string   `yaml:"key"`
	IsLocal bool     `yaml:"is_local"`
	Agent   string   `yaml:"agent"` // maps node to an agent (coordinator mode)
}

// TargetSpec defines an iSCSI/NVMe target instance.
type TargetSpec struct {
	Node            string `yaml:"node"`
	VolSize         string `yaml:"vol_size"`
	WALSize         string `yaml:"wal_size"`
	ISCSIPort       int    `yaml:"iscsi_port"`
	AdminPort       int    `yaml:"admin_port"`
	ReplicaDataPort int    `yaml:"replica_data_port"`
	ReplicaCtrlPort int    `yaml:"replica_ctrl_port"`
	RebuildPort     int    `yaml:"rebuild_port"`
	IQNSuffix       string `yaml:"iqn_suffix"`
	TPGID           int    `yaml:"tpg_id"`
	NvmePort             int    `yaml:"nvme_port"`
	NQNSuffix            string `yaml:"nqn_suffix"`
	MaxConcurrentWrites  int    `yaml:"max_concurrent_writes"`
	NvmeIOQueues         int    `yaml:"nvme_io_queues"`
}

// IQN returns the full IQN from the suffix, sanitized via the shared naming helper.
func (ts TargetSpec) IQN() string {
	return "iqn.2024.com.seaweedfs:" + SanitizeIQN(ts.IQNSuffix)
}

// NQN returns the full NQN from the suffix, using the shared BuildNQN helper
// so that testrunner identifiers always match what the runtime registers.
func (ts TargetSpec) NQN() string {
	suffix := ts.NQNSuffix
	if suffix == "" {
		suffix = ts.IQNSuffix
	}
	return BuildNQN("nqn.2024-01.com.seaweedfs:vol.", suffix)
}

// Phase is a sequential group of actions.
type Phase struct {
	Name      string `yaml:"name"`
	Always    bool   `yaml:"always"`
	Parallel  bool   `yaml:"parallel"`
	Repeat    int    `yaml:"repeat"`
	Aggregate string `yaml:"aggregate"` // "median" (default when repeat>1), "mean", "none"
	TrimPct   int    `yaml:"trim_pct"`  // percentage of outliers to trim from each end (default: 20)
	Actions   []Action `yaml:"actions"`
	// Include pulls phases from another YAML file.
	// The included file's phases replace this phase entry.
	// Params are passed as variable overrides to the included phases.
	Include       string            `yaml:"include,omitempty"`
	IncludeParams map[string]string `yaml:"include_params,omitempty"`
}

// Action is a single step within a phase.
type Action struct {
	Action      string            `yaml:"action" json:"action"`
	Target      string            `yaml:"target" json:"target,omitempty"`
	Replica     string            `yaml:"replica" json:"replica,omitempty"`
	Node        string            `yaml:"node" json:"node,omitempty"`
	SaveAs      string            `yaml:"save_as" json:"save_as,omitempty"`
	IgnoreError bool              `yaml:"ignore_error" json:"ignore_error,omitempty"`
	Retry       int               `yaml:"retry" json:"retry,omitempty"`
	Timeout     string            `yaml:"timeout" json:"timeout,omitempty"`
	Params      map[string]string `yaml:"params,inline" json:"params,omitempty"`
}

// ArtifactSpec configures what to collect on failure.
type ArtifactSpec struct {
	OnFailure []string `yaml:"on_failure"`
	Dir       string   `yaml:"dir"`
}

// --- Result types ---

// ScenarioResult is the final output of a scenario run.
type ScenarioResult struct {
	Name      string            `json:"name"`
	Status    ResultStatus      `json:"status"`
	Duration  time.Duration     `json:"duration_ms"`
	Phases    []PhaseResult     `json:"phases"`
	Error     string            `json:"error,omitempty"`
	Vars      map[string]string `json:"vars,omitempty"`
	Artifacts []ArtifactEntry   `json:"artifacts,omitempty"`
}

// ArtifactEntry records a collected artifact file.
type ArtifactEntry struct {
	Agent string `json:"agent"`
	Path  string `json:"path"`
	Size  int64  `json:"size"`
}

// PhaseResult captures the outcome of one phase.
type PhaseResult struct {
	Name     string         `json:"name"`
	Status   ResultStatus   `json:"status"`
	Duration time.Duration  `json:"duration_ms"`
	Actions  []ActionResult `json:"actions"`
	Error    string         `json:"error,omitempty"`
}

// ActionResult captures the outcome of one action.
type ActionResult struct {
	Action   string        `json:"action"`
	Status   ResultStatus  `json:"status"`
	Duration time.Duration `json:"duration_ms"`
	Output   string        `json:"output,omitempty"`
	Error    string        `json:"error,omitempty"`
	YAML     string        `json:"yaml,omitempty"`
}

// ResultStatus is the status of a result.
type ResultStatus string

const (
	StatusPass    ResultStatus = "PASS"
	StatusFail    ResultStatus = "FAIL"
	StatusSkip    ResultStatus = "SKIP"
	StatusRunning ResultStatus = "RUNNING"
)
