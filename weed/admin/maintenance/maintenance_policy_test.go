package maintenance

import (
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb/worker_pb"
)

// stubConfigPersistence implements the LoadXTaskPolicy accessors that the task config loaders
// type-assert on. The real implementation is *dash.ConfigPersistence, which this package cannot
// import: weed/admin/dash already imports weed/admin/maintenance, so the dependency only runs one
// way and the persistence argument has to stay duck-typed.
type stubConfigPersistence struct {
	vacuum    *worker_pb.TaskPolicy
	ec        *worker_pb.TaskPolicy
	balance   *worker_pb.TaskPolicy
	ecBalance *worker_pb.TaskPolicy
}

func (s *stubConfigPersistence) LoadVacuumTaskPolicy() (*worker_pb.TaskPolicy, error) {
	return s.vacuum, nil
}

func (s *stubConfigPersistence) LoadErasureCodingTaskPolicy() (*worker_pb.TaskPolicy, error) {
	return s.ec, nil
}

func (s *stubConfigPersistence) LoadBalanceTaskPolicy() (*worker_pb.TaskPolicy, error) {
	return s.balance, nil
}

func (s *stubConfigPersistence) LoadEcBalanceTaskPolicy() (*worker_pb.TaskPolicy, error) {
	return s.ecBalance, nil
}

func disabledStub() *stubConfigPersistence {
	return &stubConfigPersistence{
		vacuum: &worker_pb.TaskPolicy{
			Enabled:               false,
			MaxConcurrent:         2,
			RepeatIntervalSeconds: 2 * 3600,
			TaskConfig: &worker_pb.TaskPolicy_VacuumConfig{
				VacuumConfig: &worker_pb.VacuumTaskConfig{GarbageThreshold: 0.3, MinVolumeAgeHours: 24},
			},
		},
		ec: &worker_pb.TaskPolicy{
			Enabled:               false,
			MaxConcurrent:         1,
			RepeatIntervalSeconds: 3600,
			TaskConfig: &worker_pb.TaskPolicy_ErasureCodingConfig{
				ErasureCodingConfig: &worker_pb.ErasureCodingTaskConfig{FullnessRatio: 0.95, QuietForSeconds: 3600, MinVolumeSizeMb: 30},
			},
		},
		balance: &worker_pb.TaskPolicy{
			Enabled:               false,
			MaxConcurrent:         1,
			RepeatIntervalSeconds: 30 * 60,
			TaskConfig: &worker_pb.TaskPolicy_BalanceConfig{
				BalanceConfig: &worker_pb.BalanceTaskConfig{ImbalanceThreshold: 0.2, MinServerCount: 7},
			},
		},
		ecBalance: &worker_pb.TaskPolicy{
			Enabled:               false,
			MaxConcurrent:         1,
			RepeatIntervalSeconds: 60 * 60,
			TaskConfig: &worker_pb.TaskPolicy_EcBalanceConfig{
				EcBalanceConfig: &worker_pb.EcBalanceTaskConfig{ImbalanceThreshold: 0.2, MinServerCount: 5},
			},
		},
	}
}

// TestBuildPolicyFromTaskConfigsUsesPersistence covers the bug reported in
// https://github.com/seaweedfs/seaweedfs/issues/10874: the persistence argument used to be a
// literal nil, which no type assertion can satisfy, so a task disabled on disk came back enabled.
func TestBuildPolicyFromTaskConfigsUsesPersistence(t *testing.T) {
	policy := BuildPolicyFromTaskConfigs(disabledStub())

	for _, taskType := range []string{"vacuum", "erasure_coding", "balance", "ec_balance"} {
		taskPolicy := policy.TaskPolicies[taskType]
		if taskPolicy == nil {
			t.Fatalf("no %s task policy built", taskType)
		}
		if taskPolicy.Enabled {
			t.Errorf("%s enabled = true, want false from the persisted config", taskType)
		}
	}

	if got := policy.TaskPolicies["balance"].GetBalanceConfig().GetMinServerCount(); got != 7 {
		t.Errorf("balance min server count = %d, want persisted 7", got)
	}
}

// TestBuildPolicyFromTaskConfigsWithoutPersistence keeps the documented fallback: with no config
// store there is nothing to read, so the compiled-in defaults apply.
func TestBuildPolicyFromTaskConfigsWithoutPersistence(t *testing.T) {
	policy := BuildPolicyFromTaskConfigs(nil)

	for _, taskType := range []string{"vacuum", "erasure_coding", "balance", "ec_balance"} {
		taskPolicy := policy.TaskPolicies[taskType]
		if taskPolicy == nil {
			t.Fatalf("no %s task policy built", taskType)
		}
		if !taskPolicy.Enabled {
			t.Errorf("%s enabled = false, want the compiled-in default true", taskType)
		}
	}
}

// TestNewMaintenanceManagerUsesPersistenceForPolicyFallback covers the path the admin server takes
// when no maintenance.pb has been written yet: the config carries no policy, so the manager builds
// one itself and must read the persisted task configs to do it.
func TestNewMaintenanceManagerUsesPersistenceForPolicyFallback(t *testing.T) {
	config := DefaultMaintenanceConfig()
	if config.Policy != nil {
		t.Fatal("default config unexpectedly carries a policy, test no longer covers the fallback")
	}

	manager := NewMaintenanceManager(nil, config, disabledStub())

	policy := manager.queue.policy
	if policy == nil {
		t.Fatal("queue policy is nil")
	}
	if IsTaskEnabled(policy, MaintenanceTaskType("balance")) {
		t.Error("balance enabled = true, want false from the persisted config")
	}
	if IsTaskEnabled(policy, MaintenanceTaskType("vacuum")) {
		t.Error("vacuum enabled = true, want false from the persisted config")
	}
	if manager.scanner.policy != policy {
		t.Error("scanner and queue disagree on the policy")
	}
}
