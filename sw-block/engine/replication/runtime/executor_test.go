package runtime

import (
	"errors"
	"fmt"
	"testing"

	engine "github.com/seaweedfs/seaweedfs/sw-block/engine/replication"
)

type fakeCallbacks struct {
	progressCalled bool
	progressLSN    uint64

	catchUpCalled  bool
	catchUpVol     string
	catchUpReplica string
	catchUpLSN     uint64

	rebuildCalled  bool
	rebuildVol     string
	rebuildReplica string
	rebuildPlan    *engine.RecoveryPlan

	catchUpFailedCalled bool
	catchUpFailedReason string
}

func (f *fakeCallbacks) OnRecoveryProgress(volumeID, replicaID string, achievedLSN uint64) {
	f.progressCalled = true
	f.progressLSN = achievedLSN
}

func (f *fakeCallbacks) OnCatchUpCompleted(volumeID, replicaID string, achievedLSN uint64) {
	f.catchUpCalled = true
	f.catchUpVol = volumeID
	f.catchUpReplica = replicaID
	f.catchUpLSN = achievedLSN
}

func (f *fakeCallbacks) OnCatchUpFailed(volumeID, replicaID, reason string) {
	f.catchUpFailedCalled = true
	f.catchUpFailedReason = reason
}

func (f *fakeCallbacks) OnRebuildCompleted(volumeID, replicaID string, plan *engine.RecoveryPlan) {
	f.rebuildCalled = true
	f.rebuildVol = volumeID
	f.rebuildReplica = replicaID
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

	err = ExecuteCatchUpPlan(driver, plan, &noopCatchUpIO{}, "vol1", "vol1/vs2", cb)
	if err != nil {
		t.Fatal(err)
	}
	if !cb.catchUpCalled {
		t.Fatal("callback not called")
	}
	if !cb.progressCalled || cb.progressLSN != cb.catchUpLSN {
		t.Fatalf("progress callback mismatch: called=%v progress=%d catchup=%d", cb.progressCalled, cb.progressLSN, cb.catchUpLSN)
	}
	if cb.catchUpVol != "vol1" {
		t.Fatalf("vol=%s", cb.catchUpVol)
	}
	if cb.catchUpReplica != "vol1/vs2" {
		t.Fatalf("replica=%s", cb.catchUpReplica)
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
	err = ExecuteCatchUpPlan(driver, plan, &noopCatchUpIO{}, "vol1", "vol1/vs2", cb)
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

	err = ExecuteRebuildPlan(driver, plan, &noopRebuildIO{}, "vol2", "vol2/vs2", cb)
	if err != nil {
		t.Fatal(err)
	}
	if !cb.rebuildCalled {
		t.Fatal("rebuild callback not called")
	}
	if cb.rebuildVol != "vol2" {
		t.Fatalf("vol=%s", cb.rebuildVol)
	}
	if cb.rebuildReplica != "vol2/vs2" {
		t.Fatalf("replica=%s", cb.rebuildReplica)
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
	if err := ExecuteCatchUpPlan(driver, plan, &noopCatchUpIO{}, "vol1", "vol1/vs2", nil); err != nil {
		t.Fatal(err)
	}
}

func TestExecuteCatchUpPlan_CallsbackOnFailureWithClassification(t *testing.T) {
	cb := &fakeCallbacks{}
	driver := setupDriver(t, "vol1/vs2")
	plan, err := driver.PlanRecovery("vol1/vs2", 50)
	if err != nil {
		t.Fatal(err)
	}

	err = ExecuteCatchUpPlan(driver, plan, failingCatchUpIO{err: errors.New("WAL recycled before catch-up could complete")}, "vol1", "vol1/vs2", cb)
	if err == nil {
		t.Fatal("expected catch-up failure")
	}
	if !cb.catchUpFailedCalled {
		t.Fatal("expected failure callback")
	}
	if cb.catchUpFailedReason != "retention_lost" {
		t.Fatalf("failure reason=%q, want retention_lost", cb.catchUpFailedReason)
	}
	if cb.progressCalled {
		t.Fatal("progress callback should not fire on failure")
	}
	if cb.catchUpCalled {
		t.Fatal("completion callback should not fire on failure")
	}
}

func TestClassifyCatchUpFailure(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want string
	}{
		{name: "nil", err: nil, want: ""},
		{name: "truncation unsafe", err: fmt.Errorf("wrapped: %w", engine.ErrTruncationUnsafe), want: "truncation_unsafe"},
		{name: "retention lost", err: errors.New("WAL recycled while replaying"), want: "retention_lost"},
		{name: "duration exceeded", err: errors.New("duration_exceeded"), want: "catchup_duration_exceeded"},
		{name: "progress stalled", err: errors.New("progress_stalled"), want: "catchup_progress_stalled"},
		{name: "entries limit", err: errors.New("entries_limit_exceeded"), want: "catchup_entries_limit_exceeded"},
		{name: "budget exceeded", err: errors.New("budget violation"), want: "catchup_budget_exceeded"},
		{name: "unknown", err: errors.New("boom"), want: ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := classifyCatchUpFailure(tt.err); got != tt.want {
				t.Fatalf("classifyCatchUpFailure(%v)=%q, want %q", tt.err, got, tt.want)
			}
		})
	}
}

// --- test helpers ---

type noopCatchUpIO struct{}

func (noopCatchUpIO) StreamWALEntries(start, end uint64) (uint64, error) { return end, nil }
func (noopCatchUpIO) TruncateWAL(lsn uint64) error                       { return nil }

type failingCatchUpIO struct{ err error }

func (f failingCatchUpIO) StreamWALEntries(start, end uint64) (uint64, error) { return 0, f.err }
func (f failingCatchUpIO) TruncateWAL(lsn uint64) error                        { return nil }

type noopRebuildIO struct{}

func (noopRebuildIO) StreamWALEntries(start, end uint64) (uint64, error) { return end, nil }
func (noopRebuildIO) TruncateWAL(lsn uint64) error                       { return nil }
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
