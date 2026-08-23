package dash

import (
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/admin/maintenance"
	"github.com/seaweedfs/seaweedfs/weed/worker/tasks"
	"github.com/seaweedfs/seaweedfs/weed/worker/tasks/balance"
	"github.com/seaweedfs/seaweedfs/weed/worker/tasks/vacuum"
	"github.com/seaweedfs/seaweedfs/weed/worker/types"
)

// The task definitions these tests configure are process-global, so put them back the way a
// fresh process would have them. Passing no config store makes every task fall back to its
// own NewDefaultConfig, which is exactly the state package init left them in.
func restoreGlobalTaskState(t *testing.T) {
	t.Helper()

	t.Cleanup(func() {
		tasks.GetGlobalConfigUpdateRegistry().UpdateAllConfigs(nil)
	})
}

// TestDisabledTaskIsNotScannedAfterStartup walks the admin server's startup sequence over a
// data directory that has a disabled balance task saved in it, and checks the end state that
// actually matters: the balance detector reports disabled, so ScanWithTaskDetectors skips it.
//
// This is the whole reported bug in one test. The reporter disabled balance, and the
// scanner kept detecting balance tasks, cancelling them and re-detecting them. Two separate
// defects had to line up for the disabled flag to survive to here: the policy had to be built
// from the persisted configs rather than from a nil store, and the policy had to reach
// detector.IsEnabled() rather than dying in a failed type assertion.
func TestDisabledTaskIsNotScannedAfterStartup(t *testing.T) {
	restoreGlobalTaskState(t)

	dir := t.TempDir()
	cp := NewConfigPersistence(dir)

	// What the admin writes when a user turns balance off, and leaves vacuum on.
	disabledBalance := balance.NewDefaultConfig()
	disabledBalance.Enabled = false
	if err := cp.SaveBalanceTaskPolicy(disabledBalance.ToTaskPolicy()); err != nil {
		t.Fatalf("save balance policy: %v", err)
	}

	enabledVacuum := vacuum.NewDefaultConfig()
	enabledVacuum.Enabled = true
	if err := cp.SaveVacuumTaskPolicy(enabledVacuum.ToTaskPolicy()); err != nil {
		t.Fatalf("save vacuum policy: %v", err)
	}

	// The admin server's startup sequence, in order:
	// loadTaskConfigurationsFromPersistence, then InitMaintenanceManager.
	tasks.GetGlobalConfigUpdateRegistry().UpdateAllConfigs(cp)

	maintenanceConfig, err := cp.LoadMaintenanceConfig()
	if err != nil {
		t.Fatalf("load maintenance config: %v", err)
	}
	manager := maintenance.NewMaintenanceManager(nil, maintenanceConfig, cp)
	if manager == nil {
		t.Fatal("NewMaintenanceManager returned nil")
	}

	registry := tasks.GetGlobalTypesRegistry()

	balanceDetector := registry.GetDetector(types.TaskTypeBalance)
	if balanceDetector == nil {
		t.Fatal("no balance detector registered")
	}
	if balanceDetector.IsEnabled() {
		t.Error("balance detector reports enabled after startup over a data directory where " +
			"balance is saved as disabled; the scanner will keep detecting and cancelling balance tasks")
	}

	vacuumDetector := registry.GetDetector(types.TaskTypeVacuum)
	if vacuumDetector == nil {
		t.Fatal("no vacuum detector registered")
	}
	if !vacuumDetector.IsEnabled() {
		t.Error("vacuum detector reports disabled although vacuum is saved as enabled; " +
			"the fix must not switch off tasks the user left on")
	}

	// Tasks the user never touched keep their compiled-in default of enabled rather than
	// being switched off by a policy entry built from a config that was never saved.
	for _, taskType := range []types.TaskType{types.TaskTypeErasureCoding, types.TaskTypeECBalance} {
		detector := registry.GetDetector(taskType)
		if detector == nil {
			t.Fatalf("no %s detector registered", taskType)
		}
		if !detector.IsEnabled() {
			t.Errorf("%s detector reports disabled although its config was never saved", taskType)
		}
	}
}

// TestPolicyMirrorsWhatTheDetectorsReport checks that the maintenance policy the queue and
// the scanner run on agrees with the detectors. A disagreement means one of the two paths
// into the task configs has gone stale again.
func TestPolicyMirrorsWhatTheDetectorsReport(t *testing.T) {
	restoreGlobalTaskState(t)

	dir := t.TempDir()
	cp := NewConfigPersistence(dir)

	disabledBalance := balance.NewDefaultConfig()
	disabledBalance.Enabled = false
	if err := cp.SaveBalanceTaskPolicy(disabledBalance.ToTaskPolicy()); err != nil {
		t.Fatalf("save balance policy: %v", err)
	}

	tasks.GetGlobalConfigUpdateRegistry().UpdateAllConfigs(cp)
	policy := cp.buildPolicyFromTaskConfigs()

	for taskType, detector := range tasks.GetGlobalTypesRegistry().GetAllDetectors() {
		policyEnabled := maintenance.IsTaskEnabled(policy, maintenance.MaintenanceTaskType(taskType))
		if policyEnabled != detector.IsEnabled() {
			t.Errorf("%s: policy says enabled=%v but the detector says enabled=%v",
				taskType, policyEnabled, detector.IsEnabled())
		}
	}
}
