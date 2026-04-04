package runtime

import (
	"testing"

	engine "github.com/seaweedfs/seaweedfs/sw-block/engine/replication"
)

type fakeCallbacks struct {
	catchUpCalled bool
	catchUpVol    string
	catchUpLSN    uint64

	rebuildCalled bool
	rebuildVol    string
	rebuildPlan   *engine.RecoveryPlan
}

func (f *fakeCallbacks) OnCatchUpCompleted(volumeID string, achievedLSN uint64) {
	f.catchUpCalled = true
	f.catchUpVol = volumeID
	f.catchUpLSN = achievedLSN
}

func (f *fakeCallbacks) OnRebuildCompleted(volumeID string, plan *engine.RecoveryPlan) {
	f.rebuildCalled = true
	f.rebuildVol = volumeID
	f.rebuildPlan = plan
}

func setupDriver(t *testing.T, replicaID string) *engine.RecoveryDriver {
	t.Helper()
	orch := engine.NewRecoveryOrchestrator()
	orch.ProcessAssignment(engine.AssignmentIntent{
		Epoch: 1,
		Replicas: []engine.ReplicaAssignment{{
			ReplicaID: replicaID,
			Endpoint:  engine.Endpoint{DataAddr: "10.0.0.1:9333", CtrlAddr: "10.0.0.1:9334", Version: 1},
		}},
		RecoveryTargets: map[string]engine.SessionKind{replicaID: engine.SessionCatchUp},
	})
	return &engine.RecoveryDriver{Orchestrator: orch, Storage: newFakeStorage()}
}

func TestExecuteCatchUpPlan_CallsbackOnSuccess(t *testing.T) {
	cb := &fakeCallbacks{}
	driver := setupDriver(t, "vol1/vs2")
	plan, err := driver.PlanRecovery("vol1/vs2", 50)
	if err != nil {
		t.Fatal(err)
	}

	err = ExecuteCatchUpPlan(driver, plan, &noopCatchUpIO{}, "vol1", cb)
	if err != nil {
		t.Fatal(err)
	}
	if !cb.catchUpCalled {
		t.Fatal("callback not called")
	}
	if cb.catchUpVol != "vol1" {
		t.Fatalf("vol=%s", cb.catchUpVol)
	}
	if cb.catchUpLSN != 100 {
		t.Fatalf("achievedLSN=%d", cb.catchUpLSN)
	}
}

func TestExecuteCatchUpPlan_AchievedLSNMatchesTarget(t *testing.T) {
	cb := &fakeCallbacks{}
	driver := setupDriver(t, "vol1/vs2")
	plan, err := driver.PlanRecovery("vol1/vs2", 50)
	if err != nil {
		t.Fatal(err)
	}
	// The plan's CatchUpTarget is derived from storage state.
	// The callback should receive that same target as achievedLSN.
	err = ExecuteCatchUpPlan(driver, plan, &noopCatchUpIO{}, "vol1", cb)
	if err != nil {
		t.Fatal(err)
	}
	if cb.catchUpLSN != plan.CatchUpTarget {
		t.Fatalf("achievedLSN=%d, want plan target %d", cb.catchUpLSN, plan.CatchUpTarget)
	}
}

func TestExecuteRebuildPlan_CallsbackOnSuccess(t *testing.T) {
	cb := &fakeCallbacks{}
	orch := engine.NewRecoveryOrchestrator()
	orch.ProcessAssignment(engine.AssignmentIntent{
		Epoch: 1,
		Replicas: []engine.ReplicaAssignment{{
			ReplicaID: "vol2/vs2",
			Endpoint:  engine.Endpoint{DataAddr: "10.0.0.1:9333", Version: 1},
		}},
		RecoveryTargets: map[string]engine.SessionKind{"vol2/vs2": engine.SessionRebuild},
	})
	driver := &engine.RecoveryDriver{Orchestrator: orch, Storage: newFakeStorage()}
	plan, err := driver.PlanRebuild("vol2/vs2")
	if err != nil {
		t.Fatal(err)
	}

	err = ExecuteRebuildPlan(driver, plan, &noopRebuildIO{}, "vol2", cb)
	if err != nil {
		t.Fatal(err)
	}
	if !cb.rebuildCalled {
		t.Fatal("rebuild callback not called")
	}
	if cb.rebuildVol != "vol2" {
		t.Fatalf("vol=%s", cb.rebuildVol)
	}
	if cb.rebuildPlan == nil {
		t.Fatal("rebuild plan not passed to callback")
	}
}

func TestExecuteCatchUpPlan_NilCallbacksSafe(t *testing.T) {
	driver := setupDriver(t, "vol1/vs2")
	plan, err := driver.PlanRecovery("vol1/vs2", 50)
	if err != nil {
		t.Fatal(err)
	}
	// nil callbacks should not panic.
	if err := ExecuteCatchUpPlan(driver, plan, &noopCatchUpIO{}, "vol1", nil); err != nil {
		t.Fatal(err)
	}
}

// --- test helpers ---

type noopCatchUpIO struct{}

func (noopCatchUpIO) StreamWALEntries(start, end uint64) (uint64, error) { return end, nil }
func (noopCatchUpIO) TruncateWAL(lsn uint64) error                      { return nil }

type noopRebuildIO struct{}

func (noopRebuildIO) StreamWALEntries(start, end uint64) (uint64, error) { return end, nil }
func (noopRebuildIO) TruncateWAL(lsn uint64) error                      { return nil }
func (noopRebuildIO) TransferSnapshot(lsn uint64) error                  { return nil }
func (noopRebuildIO) TransferFullBase(lsn uint64) (uint64, error)        { return lsn, nil }

type fakeStorage struct{}

func newFakeStorage() *fakeStorage { return &fakeStorage{} }

func (fakeStorage) GetRetainedHistory() engine.RetainedHistory {
	return engine.RetainedHistory{HeadLSN: 100, TailLSN: 0, CommittedLSN: 100, CheckpointLSN: 50, CheckpointTrusted: true}
}
func (fakeStorage) PinWALRetention(lsn uint64) (engine.RetentionPin, error) {
	return engine.RetentionPin{StartLSN: lsn, Valid: true}, nil
}
func (fakeStorage) ReleaseWALRetention(engine.RetentionPin) {}
func (fakeStorage) PinSnapshot(lsn uint64) (engine.SnapshotPin, error) {
	return engine.SnapshotPin{LSN: lsn, Valid: true}, nil
}
func (fakeStorage) ReleaseSnapshot(engine.SnapshotPin) {}
func (fakeStorage) PinFullBase(lsn uint64) (engine.FullBasePin, error) {
	return engine.FullBasePin{CommittedLSN: lsn, Valid: true}, nil
}
func (fakeStorage) ReleaseFullBase(engine.FullBasePin) {}
