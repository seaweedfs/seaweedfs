package actions

import (
	"sort"
	"strings"
	"testing"

	tr "github.com/seaweedfs/seaweedfs/weed/storage/blockvol/testrunner"
	"github.com/seaweedfs/seaweedfs/weed/storage/blockvol/testrunner/internal/blockapi"
)

func TestDevOpsActions_Registration(t *testing.T) {
	registry := tr.NewRegistry()
	RegisterDevOpsActions(registry)

	expected := []string{
		"build_deploy_weed",
		"start_weed_master",
		"start_weed_volume",
		"stop_weed",
		"wait_cluster_ready",
		"create_block_volume",
		"expand_block_volume",
		"lookup_block_volume",
		"delete_block_volume",
		"wait_block_servers",
		"cluster_status",
		"wait_block_primary",
		"assert_block_field",
		"block_status",
		"block_promote",
		"wait_volume_healthy",
		"discover_primary",
	}

	for _, name := range expected {
		if _, err := registry.Get(name); err != nil {
			t.Errorf("action %q not registered: %v", name, err)
		}
	}
}

func TestDevOpsActions_Tier(t *testing.T) {
	registry := tr.NewRegistry()
	RegisterDevOpsActions(registry)

	byTier := registry.ListByTier()
	devopsActions := byTier[tr.TierDevOps]

	if len(devopsActions) != 17 {
		t.Errorf("devops tier has %d actions, want 17", len(devopsActions))
	}

	// Verify all are in devops tier.
	sort.Strings(devopsActions)
	for _, name := range devopsActions {
		if tier := registry.ActionTier(name); tier != tr.TierDevOps {
			t.Errorf("action %q has tier %q, want devops", name, tier)
		}
	}
}

func TestDevOpsActions_TierGating(t *testing.T) {
	registry := tr.NewRegistry()
	RegisterDevOpsActions(registry)

	// Without gating, all should be accessible.
	if _, err := registry.Get("start_weed_master"); err != nil {
		t.Errorf("ungated: %v", err)
	}

	// Enable only core tier — devops should be blocked.
	registry.EnableTiers([]string{tr.TierCore})
	if _, err := registry.Get("start_weed_master"); err == nil {
		t.Error("expected error when devops tier is disabled")
	}

	// Enable devops tier — should work again.
	registry.EnableTiers([]string{tr.TierDevOps})
	if _, err := registry.Get("start_weed_master"); err != nil {
		t.Errorf("devops enabled: %v", err)
	}
}

func TestAllActions_Registration(t *testing.T) {
	registry := tr.NewRegistry()
	RegisterCore(registry)
	RegisterBlockActions(registry)
	RegisterISCSIActions(registry)
	RegisterNVMeActions(registry)
	RegisterIOActions(registry)
	RegisterDevOpsActions(registry)
	RegisterSnapshotActions(registry)
	RegisterDatabaseActions(registry)
	RegisterMetricsActions(registry)
	RegisterK8sActions(registry)

	byTier := registry.ListByTier()

	// Verify tier counts.
	if n := len(byTier[tr.TierCore]); n != 17 {
		t.Errorf("core: %d, want 17", n)
	}
	if n := len(byTier[tr.TierBlock]); n != 64 {
		t.Errorf("block: %d, want 64", n)
	}
	if n := len(byTier[tr.TierDevOps]); n != 17 {
		t.Errorf("devops: %d, want 17", n)
	}
	if n := len(byTier[tr.TierChaos]); n != 5 {
		t.Errorf("chaos: %d, want 5", n)
	}
	if n := len(byTier[TierK8s]); n != 14 {
		t.Errorf("k8s: %d, want 14", n)
	}

	// Total should be 116 (115 prev + 1 recovery: measure_rebuild).
	total := 0
	for _, actions := range byTier {
		total += len(actions)
	}
	if total != 117 {
		t.Errorf("total actions: %d, want 117", total)
	}
}

func TestK8sActions_Registration(t *testing.T) {
	registry := tr.NewRegistry()
	RegisterK8sActions(registry)

	expected := []string{
		"kubectl_apply",
		"kubectl_delete",
		"kubectl_get_field",
		"kubectl_wait_condition",
		"kubectl_set_image",
		"kubectl_assert_exists",
		"kubectl_assert_not_exists",
		"kubectl_logs",
		"kubectl_rollout_status",
		"kubectl_exec",
		"kubectl_delete_pod",
		"kubectl_pod_ready_count",
		"kubectl_label",
		"kubectl_get_condition",
	}

	for _, name := range expected {
		if _, err := registry.Get(name); err != nil {
			t.Errorf("action %q not registered: %v", name, err)
		}
	}

	byTier := registry.ListByTier()
	if n := len(byTier[TierK8s]); n != 14 {
		t.Errorf("k8s tier has %d actions, want 14", n)
	}
}

func TestK8sActions_TierGating(t *testing.T) {
	registry := tr.NewRegistry()
	RegisterK8sActions(registry)

	// Without gating, all should be accessible.
	if _, err := registry.Get("kubectl_apply"); err != nil {
		t.Errorf("ungated: %v", err)
	}

	// Enable only core tier — k8s should be blocked.
	registry.EnableTiers([]string{tr.TierCore})
	if _, err := registry.Get("kubectl_apply"); err == nil {
		t.Error("expected error when k8s tier is disabled")
	}

	// Enable k8s tier — should work again.
	registry.EnableTiers([]string{TierK8s})
	if _, err := registry.Get("kubectl_apply"); err != nil {
		t.Errorf("k8s enabled: %v", err)
	}
}

func TestVolumeHealthyReady_AllowsSyncAllOnlyAfterPublishHealthy(t *testing.T) {
	tests := []struct {
		name       string
		info       *blockapi.VolumeInfo
		wantReady  bool
		wantReason string
	}{
		{
			name: "sync_all_waits_for_publish_healthy",
			info: &blockapi.VolumeInfo{
				ReplicaFactor:    2,
				DurabilityMode:   "sync_all",
				VolumeMode:       "bootstrap_pending",
				VolumeModeReason: "awaiting_shipper_connected",
			},
			wantReady:  false,
			wantReason: "publish_healthy",
		},
		{
			name: "sync_all_publish_healthy_passes",
			info: &blockapi.VolumeInfo{
				ReplicaFactor:  2,
				DurabilityMode: "sync_all",
				VolumeMode:     "publish_healthy",
			},
			wantReady: true,
		},
		{
			name: "best_effort_not_blocked_by_publish_mode",
			info: &blockapi.VolumeInfo{
				ReplicaFactor:  2,
				DurabilityMode: "best_effort",
				VolumeMode:     "bootstrap_pending",
			},
			wantReady: true,
		},
		{
			name: "nil_info_rejected",
			info: nil,
			wantReady: false,
			wantReason: "missing",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotReady, gotReason := volumeHealthyReady(tt.info)
			if gotReady != tt.wantReady {
				t.Fatalf("ready=%v, want %v (reason=%q)", gotReady, tt.wantReady, gotReason)
			}
			if tt.wantReason != "" && !strings.Contains(gotReason, tt.wantReason) {
				t.Fatalf("reason=%q, want substring %q", gotReason, tt.wantReason)
			}
		})
	}
}
