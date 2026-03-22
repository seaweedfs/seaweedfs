package blockvol

import "fmt"

// PresetName identifies a named provisioning preset.
type PresetName string

const (
	PresetDatabase   PresetName = "database"
	PresetGeneral    PresetName = "general"
	PresetThroughput PresetName = "throughput"
)

// VolumePolicy is the fully resolved configuration for a block volume.
type VolumePolicy struct {
	Preset             PresetName `json:"preset,omitempty"`
	DurabilityMode     string     `json:"durability_mode"`
	ReplicaFactor      int        `json:"replica_factor"`
	DiskType           string     `json:"disk_type,omitempty"`
	TransportPref      string     `json:"transport_preference"`
	WorkloadHint       string     `json:"workload_hint"`
	WALSizeRecommended uint64     `json:"wal_size_recommended"`
	StorageProfile     string     `json:"storage_profile"`
}

// ResolvedPolicy is the result of resolving a preset + user overrides.
type ResolvedPolicy struct {
	Policy    VolumePolicy `json:"policy"`
	Overrides []string     `json:"overrides,omitempty"`
	Warnings  []string     `json:"warnings,omitempty"`
	Errors    []string     `json:"errors,omitempty"`
}

// EnvironmentInfo provides cluster state to the resolver.
type EnvironmentInfo struct {
	NVMeAvailable    bool
	ServerCount      int
	WALSizeDefault   uint64
	BlockSizeDefault uint32
}

// presetDefaults holds the default policy for each named preset.
type presetDefaults struct {
	DurabilityMode string
	ReplicaFactor  int
	DiskType       string
	TransportPref  string
	WorkloadHint   string
	WALSizeRec     uint64
}

var presets = map[PresetName]presetDefaults{
	PresetDatabase: {
		DurabilityMode: "sync_all",
		ReplicaFactor:  2,
		DiskType:       "ssd",
		TransportPref:  "nvme",
		WorkloadHint:   WorkloadDatabase,
		WALSizeRec:     128 << 20,
	},
	PresetGeneral: {
		DurabilityMode: "best_effort",
		ReplicaFactor:  2,
		DiskType:       "",
		TransportPref:  "iscsi",
		WorkloadHint:   WorkloadGeneral,
		WALSizeRec:     64 << 20,
	},
	PresetThroughput: {
		DurabilityMode: "best_effort",
		ReplicaFactor:  2,
		DiskType:       "",
		TransportPref:  "iscsi",
		WorkloadHint:   WorkloadThroughput,
		WALSizeRec:     128 << 20,
	},
}

// system defaults when no preset is specified
var systemDefaults = presetDefaults{
	DurabilityMode: "best_effort",
	ReplicaFactor:  2,
	DiskType:       "",
	TransportPref:  "iscsi",
	WorkloadHint:   WorkloadGeneral,
	WALSizeRec:     64 << 20,
}

// ResolvePolicy resolves a preset + explicit request fields into a final policy.
// Pure function — no side effects, no server dependencies.
func ResolvePolicy(preset PresetName, durabilityMode string, replicaFactor int,
	diskType string, env EnvironmentInfo) ResolvedPolicy {

	var result ResolvedPolicy

	// Step 1: Look up preset defaults.
	defaults := systemDefaults
	if preset != "" {
		pd, ok := presets[preset]
		if !ok {
			result.Errors = append(result.Errors, fmt.Sprintf("unknown preset %q", preset))
			return result
		}
		defaults = pd
	}

	// Start with defaults.
	policy := VolumePolicy{
		Preset:             preset,
		DurabilityMode:     defaults.DurabilityMode,
		ReplicaFactor:      defaults.ReplicaFactor,
		DiskType:           defaults.DiskType,
		TransportPref:      defaults.TransportPref,
		WorkloadHint:       defaults.WorkloadHint,
		WALSizeRecommended: defaults.WALSizeRec,
		StorageProfile:     "single",
	}

	// Step 2: Apply overrides.
	if durabilityMode != "" {
		policy.DurabilityMode = durabilityMode
		result.Overrides = append(result.Overrides, "durability_mode")
	}
	if replicaFactor != 0 {
		policy.ReplicaFactor = replicaFactor
		result.Overrides = append(result.Overrides, "replica_factor")
	}
	if diskType != "" {
		policy.DiskType = diskType
		result.Overrides = append(result.Overrides, "disk_type")
	}

	// Step 3: Normalize + validate durability_mode.
	durMode, err := ParseDurabilityMode(policy.DurabilityMode)
	if err != nil {
		result.Errors = append(result.Errors, fmt.Sprintf("invalid durability_mode: %s", err))
		result.Policy = policy
		return result
	}

	// Step 4: Cross-validate durability vs RF.
	if err := durMode.Validate(policy.ReplicaFactor); err != nil {
		result.Errors = append(result.Errors, err.Error())
		result.Policy = policy
		return result
	}

	// Step 5: Advisory warnings.
	if policy.ReplicaFactor == 1 && durMode == DurabilitySyncAll {
		result.Warnings = append(result.Warnings,
			"sync_all with replica_factor=1 provides no replication benefit")
	}
	if env.ServerCount > 0 && policy.ReplicaFactor > env.ServerCount {
		result.Warnings = append(result.Warnings,
			fmt.Sprintf("replica_factor=%d exceeds available servers (%d)", policy.ReplicaFactor, env.ServerCount))
	}

	// Step 6: Transport advisory.
	if policy.TransportPref == "nvme" && !env.NVMeAvailable {
		result.Warnings = append(result.Warnings,
			"preset recommends NVMe transport but no NVMe-capable servers are available")
	}

	// Step 7: WAL sizing advisory.
	// Check engine default against preset recommendation.
	if env.WALSizeDefault > 0 && env.WALSizeDefault < policy.WALSizeRecommended {
		result.Warnings = append(result.Warnings,
			fmt.Sprintf("preset recommends %dMB WAL but engine default is %dMB",
				policy.WALSizeRecommended>>20, env.WALSizeDefault>>20))
	}

	result.Policy = policy
	return result
}
