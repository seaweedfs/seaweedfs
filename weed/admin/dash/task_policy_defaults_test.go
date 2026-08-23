package dash

import (
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb/worker_pb"
	"github.com/seaweedfs/seaweedfs/weed/worker/tasks/balance"
	"github.com/seaweedfs/seaweedfs/weed/worker/tasks/ec_balance"
	"github.com/seaweedfs/seaweedfs/weed/worker/tasks/erasure_coding"
	"github.com/seaweedfs/seaweedfs/weed/worker/tasks/vacuum"
	"google.golang.org/protobuf/proto"
)

// TestLoadTaskPolicyDefaultsMatchTaskDefaults pins the persistence layer's "nothing saved
// yet" defaults to each task's own NewDefaultConfig(). They used to be a second, hand-written
// copy and had drifted: with a data directory but no config file on disk, vacuum ran on a 24h
// scan interval instead of 2h, balance on 6h with a 0.1 imbalance threshold instead of 30m
// with 0.2, and erasure coding on 168h with a 0.90 fullness ratio and a 1024MB minimum volume
// size instead of 1h with 0.95 and 30MB - none of which is what the admin UI shows as the
// default for those fields.
func TestLoadTaskPolicyDefaultsMatchTaskDefaults(t *testing.T) {
	cases := []struct {
		name string
		want *worker_pb.TaskPolicy
		load func(cp *ConfigPersistence) (*worker_pb.TaskPolicy, error)
	}{
		{
			name: "vacuum",
			want: vacuum.NewDefaultConfig().ToTaskPolicy(),
			load: func(cp *ConfigPersistence) (*worker_pb.TaskPolicy, error) { return cp.LoadVacuumTaskPolicy() },
		},
		{
			name: "erasure_coding",
			want: erasure_coding.NewDefaultConfig().ToTaskPolicy(),
			load: func(cp *ConfigPersistence) (*worker_pb.TaskPolicy, error) {
				return cp.LoadErasureCodingTaskPolicy()
			},
		},
		{
			name: "balance",
			want: balance.NewDefaultConfig().ToTaskPolicy(),
			load: func(cp *ConfigPersistence) (*worker_pb.TaskPolicy, error) { return cp.LoadBalanceTaskPolicy() },
		},
		{
			name: "ec_balance",
			want: ec_balance.NewDefaultConfig().ToTaskPolicy(),
			load: func(cp *ConfigPersistence) (*worker_pb.TaskPolicy, error) { return cp.LoadEcBalanceTaskPolicy() },
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			// Both no-file branches have to agree with the task's own defaults: no data
			// directory at all, and a data directory that has never been written to.
			for _, cp := range []*ConfigPersistence{NewConfigPersistence(""), NewConfigPersistence(t.TempDir())} {
				got, err := tc.load(cp)
				if err != nil {
					t.Fatalf("load %s policy: %v", tc.name, err)
				}
				if !proto.Equal(got, tc.want) {
					t.Errorf("%s default policy (dataDir=%q) =\n  %v\nwant NewDefaultConfig().ToTaskPolicy() =\n  %v",
						tc.name, cp.GetDataDir(), got, tc.want)
				}
			}
		})
	}
}

// TestLoadTaskConfigDefaultsMatchTaskDefaults covers the narrower Load*TaskConfig accessors,
// which carried a third copy of the same defaults.
func TestLoadTaskConfigDefaultsMatchTaskDefaults(t *testing.T) {
	cp := NewConfigPersistence(t.TempDir())

	vacuumConfig, err := cp.LoadVacuumTaskConfig()
	if err != nil {
		t.Fatalf("load vacuum config: %v", err)
	}
	if want := vacuum.NewDefaultConfig().ToTaskPolicy().GetVacuumConfig(); !proto.Equal(vacuumConfig, want) {
		t.Errorf("vacuum default config = %v, want %v", vacuumConfig, want)
	}

	ecConfig, err := cp.LoadErasureCodingTaskConfig()
	if err != nil {
		t.Fatalf("load erasure coding config: %v", err)
	}
	if want := erasure_coding.NewDefaultConfig().ToTaskPolicy().GetErasureCodingConfig(); !proto.Equal(ecConfig, want) {
		t.Errorf("erasure coding default config = %v, want %v", ecConfig, want)
	}

	balanceConfig, err := cp.LoadBalanceTaskConfig()
	if err != nil {
		t.Fatalf("load balance config: %v", err)
	}
	if want := balance.NewDefaultConfig().ToTaskPolicy().GetBalanceConfig(); !proto.Equal(balanceConfig, want) {
		t.Errorf("balance default config = %v, want %v", balanceConfig, want)
	}
}

// TestEcBalanceTaskPolicyRoundTrip checks the accessor ec_balance.LoadConfigFromPersistence
// asserts on. Before it existed, ec_balance was the one registered maintenance task whose
// configuration could not be persisted at all.
func TestEcBalanceTaskPolicyRoundTrip(t *testing.T) {
	cp := NewConfigPersistence(t.TempDir())

	saved := ec_balance.NewDefaultConfig()
	saved.Enabled = false
	saved.MinServerCount = 9
	saved.ImbalanceThreshold = 0.42
	saved.CollectionFilter = "pictures"

	if err := cp.SaveEcBalanceTaskPolicy(saved.ToTaskPolicy()); err != nil {
		t.Fatalf("save ec_balance policy: %v", err)
	}

	loaded := ec_balance.LoadConfigFromPersistence(cp)
	if loaded == nil {
		t.Fatal("ec_balance.LoadConfigFromPersistence returned nil")
	}
	if loaded.Enabled {
		t.Error("ec_balance enabled = true, want the persisted false")
	}
	if loaded.MinServerCount != 9 {
		t.Errorf("ec_balance min server count = %d, want the persisted 9", loaded.MinServerCount)
	}
	if loaded.ImbalanceThreshold != 0.42 {
		t.Errorf("ec_balance imbalance threshold = %v, want the persisted 0.42", loaded.ImbalanceThreshold)
	}
	if loaded.CollectionFilter != "pictures" {
		t.Errorf("ec_balance collection filter = %q, want the persisted %q", loaded.CollectionFilter, "pictures")
	}

	// The generic dispatcher the maintenance manager uses has to know the type too.
	if err := cp.SaveTaskPolicy("ec_balance", saved.ToTaskPolicy()); err != nil {
		t.Errorf("SaveTaskPolicy(ec_balance): %v", err)
	}
}

// TestBuildPolicyKeepsTaskSpecificFields guards the fields the hand-written policy builder
// used to drop on the floor: the erasure coding preferred tags and replica placement, and
// the balance IO rate limit. Building each entry from the task's own ToTaskPolicy() keeps
// them, so a value set in admin.toml survives into the maintenance policy.
func TestBuildPolicyKeepsTaskSpecificFields(t *testing.T) {
	cp := NewConfigPersistence(t.TempDir())

	ecConfig := erasure_coding.NewDefaultConfig()
	ecConfig.PreferredTags = []string{"ssd", "archive"}
	ecConfig.ReplicaPlacement = "020"
	if err := cp.SaveErasureCodingTaskPolicy(ecConfig.ToTaskPolicy()); err != nil {
		t.Fatalf("save erasure coding policy: %v", err)
	}

	balanceConfig := balance.NewDefaultConfig()
	balanceConfig.IoBytePerSecond = 5 << 20
	if err := cp.SaveBalanceTaskPolicy(balanceConfig.ToTaskPolicy()); err != nil {
		t.Fatalf("save balance policy: %v", err)
	}

	policy := cp.buildPolicyFromTaskConfigs()

	ecPolicy := policy.TaskPolicies["erasure_coding"].GetErasureCodingConfig()
	if ecPolicy == nil {
		t.Fatal("no erasure coding config in the built policy")
	}
	if got := ecPolicy.GetReplicaPlacement(); got != "020" {
		t.Errorf("erasure coding replica placement = %q, want the persisted %q", got, "020")
	}
	if got := ecPolicy.GetPreferredTags(); len(got) != 2 {
		t.Errorf("erasure coding preferred tags = %v, want the persisted 2 entries", got)
	}

	balancePolicy := policy.TaskPolicies["balance"].GetBalanceConfig()
	if balancePolicy == nil {
		t.Fatal("no balance config in the built policy")
	}
	if got := balancePolicy.GetIoBytePerSecond(); got != 5<<20 {
		t.Errorf("balance IO limit = %d, want the persisted %d", got, 5<<20)
	}
}
