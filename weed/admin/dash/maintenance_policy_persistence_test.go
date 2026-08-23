package dash

import (
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/worker/tasks/balance"
	"github.com/seaweedfs/seaweedfs/weed/worker/tasks/vacuum"
)

// TestLoadMaintenanceConfigHonoursPersistedTaskConfigs guards against the regression in
// https://github.com/seaweedfs/seaweedfs/issues/10874: buildPolicyFromTaskConfigs used to call
// LoadConfigFromPersistence(nil), which can never satisfy the loaders' type assertion, so every
// task silently fell back to its compiled-in defaults (Enabled: true) and a task disabled in the
// admin UI kept being scheduled.
func TestLoadMaintenanceConfigHonoursPersistedTaskConfigs(t *testing.T) {
	dir := t.TempDir()
	cp := NewConfigPersistence(dir)

	// A maintenance.pb must exist, otherwise LoadMaintenanceConfig returns early with defaults.
	if err := cp.SaveMaintenanceConfig(DefaultMaintenanceConfig()); err != nil {
		t.Fatalf("save maintenance config: %v", err)
	}

	// Disable balance and vacuum the way the admin UI does, and change a value that is not a bool
	// so a fallback to defaults cannot pass by coincidence.
	disabledBalance := balance.NewDefaultConfig()
	disabledBalance.Enabled = false
	disabledBalance.MinServerCount = 7
	if err := cp.SaveBalanceTaskPolicy(disabledBalance.ToTaskPolicy()); err != nil {
		t.Fatalf("save balance policy: %v", err)
	}

	disabledVacuum := vacuum.NewDefaultConfig()
	disabledVacuum.Enabled = false
	if err := cp.SaveVacuumTaskPolicy(disabledVacuum.ToTaskPolicy()); err != nil {
		t.Fatalf("save vacuum policy: %v", err)
	}

	config, err := cp.LoadMaintenanceConfig()
	if err != nil {
		t.Fatalf("load maintenance config: %v", err)
	}
	if config.Policy == nil {
		t.Fatal("policy is nil, want it populated from the persisted task configs")
	}

	balancePolicy := config.Policy.TaskPolicies["balance"]
	if balancePolicy == nil {
		t.Fatal("no balance task policy in the built maintenance policy")
	}
	if balancePolicy.Enabled {
		t.Error("balance enabled = true, want false from the persisted config")
	}
	if got := balancePolicy.GetBalanceConfig().GetMinServerCount(); got != 7 {
		t.Errorf("balance min server count = %d, want persisted 7", got)
	}

	vacuumPolicy := config.Policy.TaskPolicies["vacuum"]
	if vacuumPolicy == nil {
		t.Fatal("no vacuum task policy in the built maintenance policy")
	}
	if vacuumPolicy.Enabled {
		t.Error("vacuum enabled = true, want false from the persisted config")
	}

	// erasure_coding was never saved, so it keeps the loader's default of enabled.
	ecPolicy := config.Policy.TaskPolicies["erasure_coding"]
	if ecPolicy == nil {
		t.Fatal("no erasure_coding task policy in the built maintenance policy")
	}
	if !ecPolicy.Enabled {
		t.Error("erasure_coding enabled = false, want the default true for a config never saved")
	}
}
