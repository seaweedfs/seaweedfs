package maintenance

import (
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb/worker_pb"
	"github.com/seaweedfs/seaweedfs/weed/worker/tasks"
	"github.com/seaweedfs/seaweedfs/weed/worker/types"
)

// The detectors and schedulers live in a process-global registry, so these tests restore
// whatever they change. Otherwise a test that disables a task leaks that state into every
// later test in the package.
func snapshotDetectorState(t *testing.T) {
	t.Helper()

	registry := tasks.GetGlobalTypesRegistry()
	enabled := make(map[types.TaskType]bool)
	maxConcurrent := make(map[types.TaskType]int)
	for taskType, detector := range registry.GetAllDetectors() {
		enabled[taskType] = detector.IsEnabled()
	}
	for taskType, scheduler := range registry.GetAllSchedulers() {
		maxConcurrent[taskType] = scheduler.GetMaxConcurrent()
	}

	t.Cleanup(func() {
		for taskType, detector := range registry.GetAllDetectors() {
			if setter, ok := detector.(interface{ SetEnabled(bool) }); ok {
				setter.SetEnabled(enabled[taskType])
			}
		}
		for taskType, scheduler := range registry.GetAllSchedulers() {
			if setter, ok := scheduler.(interface{ SetMaxConcurrent(int) }); ok {
				setter.SetMaxConcurrent(maxConcurrent[taskType])
			}
		}
	})
}

// TestPolicyReachesRegisteredDetectors is the regression test for the half of issue #10874
// that a corrected policy alone did not fix: MaintenanceIntegration pushes the policy into
// detectors and schedulers through interface{ SetEnabled(bool) } and
// interface{ SetMaxConcurrent(int) } type assertions, but every task is backed by
// base.GenericDetector/base.GenericScheduler, which implemented neither. The assertions
// failed silently for every task on every startup, so the policy never reached
// detector.IsEnabled() - which is what ScanWithTaskDetectors gates scanning on.
func TestPolicyReachesRegisteredDetectors(t *testing.T) {
	snapshotDetectorState(t)

	registry := tasks.GetGlobalTypesRegistry()
	if len(registry.GetAllDetectors()) == 0 {
		t.Fatal("no detectors registered, the test cannot prove anything")
	}

	// Enable everything first so the disabling below cannot pass by accident.
	policy := policyWithAllTasks(t, true)
	NewMaintenanceIntegration(NewMaintenanceQueue(policy), policy)

	for taskType, detector := range registry.GetAllDetectors() {
		if !detector.IsEnabled() {
			t.Fatalf("detector %s is disabled after an all-enabled policy was applied", taskType)
		}
	}

	// Now disable everything through the policy and check it lands on the detectors.
	policy = policyWithAllTasks(t, false)
	NewMaintenanceIntegration(NewMaintenanceQueue(policy), policy)

	for taskType, detector := range registry.GetAllDetectors() {
		if detector.IsEnabled() {
			t.Errorf("detector %s still reports enabled after the policy disabled it; "+
				"the policy is not reaching the flag ScanWithTaskDetectors gates on", taskType)
		}
	}
	for taskType, scheduler := range registry.GetAllSchedulers() {
		if scheduler.IsEnabled() {
			t.Errorf("scheduler %s still reports enabled after the policy disabled it", taskType)
		}
	}
}

// TestPolicyMaxConcurrentReachesSchedulers covers the SetMaxConcurrent half of the same
// wiring. GetMaxConcurrent is what MaintenanceQueue.getMaxConcurrentForTaskType asks before
// starting another task of a type.
func TestPolicyMaxConcurrentReachesSchedulers(t *testing.T) {
	snapshotDetectorState(t)

	const wantMaxConcurrent = 7

	policy := policyWithAllTasks(t, true)
	for _, taskPolicy := range policy.TaskPolicies {
		taskPolicy.MaxConcurrent = wantMaxConcurrent
	}
	NewMaintenanceIntegration(NewMaintenanceQueue(policy), policy)

	registry := tasks.GetGlobalTypesRegistry()
	for taskType, scheduler := range registry.GetAllSchedulers() {
		if _, covered := policy.TaskPolicies[string(taskType)]; !covered {
			continue
		}
		if got := scheduler.GetMaxConcurrent(); got != wantMaxConcurrent {
			t.Errorf("scheduler %s max concurrent = %d, want %d from the policy", taskType, got, wantMaxConcurrent)
		}
	}
}

// TestPolicyWithoutEntryLeavesTaskAlone guards the direction that would have been a silent
// outage: IsTaskEnabled reports false for a task type the policy does not list, so applying
// it unconditionally would disable every task the policy has no entry for.
func TestPolicyWithoutEntryLeavesTaskAlone(t *testing.T) {
	snapshotDetectorState(t)

	registry := tasks.GetGlobalTypesRegistry()

	// Start from a policy that enables everything.
	enabling := policyWithAllTasks(t, true)
	NewMaintenanceIntegration(NewMaintenanceQueue(enabling), enabling)

	// An empty policy has an entry for nothing at all.
	empty := &MaintenancePolicy{TaskPolicies: make(map[string]*worker_pb.TaskPolicy)}
	NewMaintenanceIntegration(NewMaintenanceQueue(empty), empty)

	for taskType, detector := range registry.GetAllDetectors() {
		if !detector.IsEnabled() {
			t.Errorf("detector %s was disabled by a policy that has no entry for it; "+
				"a missing entry means no opinion, not disabled", taskType)
		}
	}
}

// TestBuildPolicyCoversEveryRegisteredTask keeps BuildPolicyFromTaskConfigs and the task
// registry in step. A registered task with no policy entry has no enabled flag and no
// concurrency limit of its own, which is the state ec_balance was in.
func TestBuildPolicyCoversEveryRegisteredTask(t *testing.T) {
	policy := BuildPolicyFromTaskConfigs(nil)

	for taskType := range tasks.GetGlobalTypesRegistry().GetAllDetectors() {
		if _, ok := policy.TaskPolicies[string(taskType)]; !ok {
			t.Errorf("task %s is registered as a detector but BuildPolicyFromTaskConfigs "+
				"builds no policy entry for it, so IsTaskEnabled reports false for it", taskType)
		}
	}
}

// TestBuildPolicyIncludesEcBalance pins the specific gap above.
func TestBuildPolicyIncludesEcBalance(t *testing.T) {
	policy := BuildPolicyFromTaskConfigs(nil)

	ecBalance := policy.TaskPolicies[string(types.TaskTypeECBalance)]
	if ecBalance == nil {
		t.Fatal("no ec_balance entry in the built maintenance policy")
	}
	if !IsTaskEnabled(policy, MaintenanceTaskType(types.TaskTypeECBalance)) {
		t.Error("ec_balance reports disabled with no persisted config, want the compiled-in default of enabled")
	}
	if ecBalance.GetEcBalanceConfig() == nil {
		t.Error("ec_balance policy entry carries no EcBalanceConfig")
	}
}

// policyWithAllTasks builds a policy that has an entry for every registered task type,
// all sharing the same enabled flag.
func policyWithAllTasks(t *testing.T, enabled bool) *MaintenancePolicy {
	t.Helper()

	policy := &MaintenancePolicy{
		GlobalMaxConcurrent: 4,
		TaskPolicies:        make(map[string]*worker_pb.TaskPolicy),
	}
	for taskType := range tasks.GetGlobalTypesRegistry().GetAllDetectors() {
		policy.TaskPolicies[string(taskType)] = &worker_pb.TaskPolicy{
			Enabled:               enabled,
			MaxConcurrent:         1,
			RepeatIntervalSeconds: 3600,
		}
	}
	return policy
}
