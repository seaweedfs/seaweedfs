package actions

import (
	"sort"
	"testing"

	tr "github.com/seaweedfs/seaweedfs/weed/storage/blockvol/testrunner"
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
		"cluster_status",
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

	if len(devopsActions) != 7 {
		t.Errorf("devops tier has %d actions, want 7", len(devopsActions))
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
	RegisterAll(registry)

	byTier := registry.ListByTier()

	// Verify tier counts.
	if n := len(byTier[tr.TierCore]); n != 7 {
		t.Errorf("core: %d, want 7", n)
	}
	if n := len(byTier[tr.TierBlock]); n != 33 {
		t.Errorf("block: %d, want 33", n)
	}
	if n := len(byTier[tr.TierDevOps]); n != 7 {
		t.Errorf("devops: %d, want 7", n)
	}
	if n := len(byTier[tr.TierChaos]); n != 5 {
		t.Errorf("chaos: %d, want 5", n)
	}

	// Total should be 52.
	total := 0
	for _, actions := range byTier {
		total += len(actions)
	}
	if total != 52 {
		t.Errorf("total actions: %d, want 52", total)
	}
}
