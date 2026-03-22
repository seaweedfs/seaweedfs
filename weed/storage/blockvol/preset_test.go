package blockvol

import (
	"strings"
	"testing"
)

func TestResolvePolicy_DatabaseDefaults(t *testing.T) {
	r := ResolvePolicy(PresetDatabase, "", 0, "", EnvironmentInfo{
		NVMeAvailable: true, ServerCount: 3, WALSizeDefault: 128 << 20, BlockSizeDefault: 4096,
	})
	if len(r.Errors) > 0 {
		t.Fatalf("unexpected errors: %v", r.Errors)
	}
	p := r.Policy
	if p.DurabilityMode != "sync_all" {
		t.Errorf("durability_mode = %q, want sync_all", p.DurabilityMode)
	}
	if p.ReplicaFactor != 2 {
		t.Errorf("replica_factor = %d, want 2", p.ReplicaFactor)
	}
	if p.DiskType != "ssd" {
		t.Errorf("disk_type = %q, want ssd", p.DiskType)
	}
	if p.TransportPref != "nvme" {
		t.Errorf("transport_preference = %q, want nvme", p.TransportPref)
	}
	if p.WALSizeRecommended != 128<<20 {
		t.Errorf("wal_size_recommended = %d, want %d", p.WALSizeRecommended, 128<<20)
	}
	if p.WorkloadHint != "database" {
		t.Errorf("workload_hint = %q, want database", p.WorkloadHint)
	}
	if p.StorageProfile != "single" {
		t.Errorf("storage_profile = %q, want single", p.StorageProfile)
	}
	if len(r.Overrides) != 0 {
		t.Errorf("overrides = %v, want empty", r.Overrides)
	}
}

func TestResolvePolicy_GeneralDefaults(t *testing.T) {
	r := ResolvePolicy(PresetGeneral, "", 0, "", EnvironmentInfo{
		ServerCount: 2, WALSizeDefault: 64 << 20, BlockSizeDefault: 4096,
	})
	if len(r.Errors) > 0 {
		t.Fatalf("unexpected errors: %v", r.Errors)
	}
	p := r.Policy
	if p.DurabilityMode != "best_effort" {
		t.Errorf("durability_mode = %q, want best_effort", p.DurabilityMode)
	}
	if p.ReplicaFactor != 2 {
		t.Errorf("replica_factor = %d, want 2", p.ReplicaFactor)
	}
	if p.DiskType != "" {
		t.Errorf("disk_type = %q, want empty", p.DiskType)
	}
	if p.TransportPref != "iscsi" {
		t.Errorf("transport_preference = %q, want iscsi", p.TransportPref)
	}
	if p.WALSizeRecommended != 64<<20 {
		t.Errorf("wal_size_recommended = %d, want %d", p.WALSizeRecommended, 64<<20)
	}
}

func TestResolvePolicy_ThroughputDefaults(t *testing.T) {
	r := ResolvePolicy(PresetThroughput, "", 0, "", EnvironmentInfo{
		ServerCount: 2, WALSizeDefault: 64 << 20, BlockSizeDefault: 4096,
	})
	if len(r.Errors) > 0 {
		t.Fatalf("unexpected errors: %v", r.Errors)
	}
	p := r.Policy
	if p.DurabilityMode != "best_effort" {
		t.Errorf("durability_mode = %q, want best_effort", p.DurabilityMode)
	}
	if p.WALSizeRecommended != 128<<20 {
		t.Errorf("wal_size_recommended = %d, want %d", p.WALSizeRecommended, 128<<20)
	}
	if p.WorkloadHint != "throughput" {
		t.Errorf("workload_hint = %q, want throughput", p.WorkloadHint)
	}
}

func TestResolvePolicy_OverrideDurability(t *testing.T) {
	r := ResolvePolicy(PresetDatabase, "best_effort", 0, "", EnvironmentInfo{
		NVMeAvailable: true, ServerCount: 2, WALSizeDefault: 128 << 20, BlockSizeDefault: 4096,
	})
	if len(r.Errors) > 0 {
		t.Fatalf("unexpected errors: %v", r.Errors)
	}
	if r.Policy.DurabilityMode != "best_effort" {
		t.Errorf("durability_mode = %q, want best_effort (override)", r.Policy.DurabilityMode)
	}
	if !containsStr(r.Overrides, "durability_mode") {
		t.Errorf("overrides = %v, want durability_mode present", r.Overrides)
	}
}

func TestResolvePolicy_OverrideRF(t *testing.T) {
	r := ResolvePolicy(PresetGeneral, "", 3, "", EnvironmentInfo{
		ServerCount: 3, WALSizeDefault: 64 << 20, BlockSizeDefault: 4096,
	})
	if len(r.Errors) > 0 {
		t.Fatalf("unexpected errors: %v", r.Errors)
	}
	if r.Policy.ReplicaFactor != 3 {
		t.Errorf("replica_factor = %d, want 3 (override)", r.Policy.ReplicaFactor)
	}
	if !containsStr(r.Overrides, "replica_factor") {
		t.Errorf("overrides = %v, want replica_factor present", r.Overrides)
	}
}

func TestResolvePolicy_NoPreset_SystemDefaults(t *testing.T) {
	r := ResolvePolicy("", "", 0, "", EnvironmentInfo{
		ServerCount: 2, WALSizeDefault: 64 << 20, BlockSizeDefault: 4096,
	})
	if len(r.Errors) > 0 {
		t.Fatalf("unexpected errors: %v", r.Errors)
	}
	p := r.Policy
	if p.DurabilityMode != "best_effort" {
		t.Errorf("durability_mode = %q, want best_effort", p.DurabilityMode)
	}
	if p.ReplicaFactor != 2 {
		t.Errorf("replica_factor = %d, want 2", p.ReplicaFactor)
	}
	if p.TransportPref != "iscsi" {
		t.Errorf("transport_preference = %q, want iscsi", p.TransportPref)
	}
	if p.Preset != "" {
		t.Errorf("preset = %q, want empty", p.Preset)
	}
}

func TestResolvePolicy_InvalidPreset(t *testing.T) {
	r := ResolvePolicy("nosuch", "", 0, "", EnvironmentInfo{})
	if len(r.Errors) == 0 {
		t.Fatal("expected error for unknown preset")
	}
	if !strings.Contains(r.Errors[0], "unknown preset") {
		t.Errorf("error = %q, want to contain 'unknown preset'", r.Errors[0])
	}
}

func TestResolvePolicy_IncompatibleCombo(t *testing.T) {
	r := ResolvePolicy("", "sync_quorum", 2, "", EnvironmentInfo{
		ServerCount: 2, WALSizeDefault: 64 << 20, BlockSizeDefault: 4096,
	})
	if len(r.Errors) == 0 {
		t.Fatal("expected error for sync_quorum + RF=2")
	}
	if !strings.Contains(r.Errors[0], "replica_factor >= 3") {
		t.Errorf("error = %q, want sync_quorum RF constraint", r.Errors[0])
	}
}

func TestResolvePolicy_WALWarning(t *testing.T) {
	r := ResolvePolicy(PresetDatabase, "", 0, "", EnvironmentInfo{
		NVMeAvailable: true, ServerCount: 2, WALSizeDefault: 64 << 20, BlockSizeDefault: 4096,
	})
	if len(r.Errors) > 0 {
		t.Fatalf("unexpected errors: %v", r.Errors)
	}
	found := false
	for _, w := range r.Warnings {
		if strings.Contains(w, "128MB WAL") && strings.Contains(w, "64MB") {
			found = true
		}
	}
	if !found {
		t.Errorf("expected WAL sizing warning, got warnings: %v", r.Warnings)
	}
}

func containsStr(ss []string, target string) bool {
	for _, s := range ss {
		if s == target {
			return true
		}
	}
	return false
}
