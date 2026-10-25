package vacuum

import (
	"errors"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb/worker_pb"
)

type stubVacuumStore struct {
	policy *worker_pb.TaskPolicy
	err    error
}

func (s *stubVacuumStore) LoadVacuumTaskPolicy() (*worker_pb.TaskPolicy, error) {
	return s.policy, s.err
}

// wrongShapedStore is what the maintenance manager used to be handed: a non-nil value that
// does not satisfy the accessor the loader asserts on. It has to keep falling back to the
// defaults, but no longer silently: a fallback the operator cannot see reads as a disabled task
// that keeps running.
type wrongShapedStore struct{}

func (wrongShapedStore) SomethingElse() {}

func TestLoadConfigFromPersistenceUsesPersistedPolicy(t *testing.T) {
	persisted := NewDefaultConfig()
	persisted.Enabled = false
	persisted.GarbageThreshold = 0.75
	persisted.MaxConcurrent = 5

	loaded := LoadConfigFromPersistence(&stubVacuumStore{policy: persisted.ToTaskPolicy()})
	if loaded == nil {
		t.Fatal("LoadConfigFromPersistence returned nil")
	}
	if loaded.Enabled {
		t.Error("enabled = true, want the persisted false")
	}
	if loaded.GarbageThreshold != 0.75 {
		t.Errorf("garbage threshold = %v, want the persisted 0.75", loaded.GarbageThreshold)
	}
	if loaded.MaxConcurrent != 5 {
		t.Errorf("max concurrent = %d, want the persisted 5", loaded.MaxConcurrent)
	}
}

func TestLoadConfigFromPersistenceFallsBackToDefaults(t *testing.T) {
	defaults := NewDefaultConfig()

	cases := map[string]interface{}{
		"no store configured":     nil,
		"store without the value": &stubVacuumStore{},
		"store that errors":       &stubVacuumStore{err: errors.New("disk on fire")},
		"store of the wrong type": wrongShapedStore{},
	}

	for name, store := range cases {
		t.Run(name, func(t *testing.T) {
			loaded := LoadConfigFromPersistence(store)
			if loaded == nil {
				t.Fatal("LoadConfigFromPersistence returned nil")
			}
			if !loaded.Enabled {
				t.Error("enabled = false, want the compiled-in default true")
			}
			if loaded.GarbageThreshold != defaults.GarbageThreshold {
				t.Errorf("garbage threshold = %v, want the default %v", loaded.GarbageThreshold, defaults.GarbageThreshold)
			}
			if loaded.ScanIntervalSeconds != defaults.ScanIntervalSeconds {
				t.Errorf("scan interval = %d, want the default %d", loaded.ScanIntervalSeconds, defaults.ScanIntervalSeconds)
			}
		})
	}
}
