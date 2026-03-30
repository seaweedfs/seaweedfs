package replication

import (
	"fmt"
	"sync/atomic"
	"testing"
)

// ============================================================
// Phase 06 P0/P1: Recovery driver tests with mock storage adapter
// ============================================================

// --- Mock storage adapter ---

type mockStorage struct {
	history         RetainedHistory
	nextPinID       atomic.Uint64
	pinnedSnaps     map[uint64]bool
	pinnedWAL       map[uint64]bool
	pinnedFullBase  map[uint64]bool
	failSnapshotPin bool
	failWALPin      bool
	failFullBasePin bool
}

func newMockStorage(history RetainedHistory) *mockStorage {
	return &mockStorage{
		history:        history,
		pinnedSnaps:    map[uint64]bool{},
		pinnedWAL:      map[uint64]bool{},
		pinnedFullBase: map[uint64]bool{},
	}
}

func (m *mockStorage) GetRetainedHistory() RetainedHistory { return m.history }

func (m *mockStorage) PinSnapshot(lsn uint64) (SnapshotPin, error) {
	if m.failSnapshotPin {
		return SnapshotPin{}, fmt.Errorf("snapshot pin refused")
	}
	id := m.nextPinID.Add(1)
	m.pinnedSnaps[id] = true
	return SnapshotPin{LSN: lsn, PinID: id, Valid: true}, nil
}

func (m *mockStorage) ReleaseSnapshot(pin SnapshotPin) {
	delete(m.pinnedSnaps, pin.PinID)
}

func (m *mockStorage) PinWALRetention(startLSN uint64) (RetentionPin, error) {
	if m.failWALPin {
		return RetentionPin{}, fmt.Errorf("WAL retention pin refused")
	}
	id := m.nextPinID.Add(1)
	m.pinnedWAL[id] = true
	return RetentionPin{StartLSN: startLSN, PinID: id, Valid: true}, nil
}

func (m *mockStorage) ReleaseWALRetention(pin RetentionPin) {
	delete(m.pinnedWAL, pin.PinID)
}

func (m *mockStorage) PinFullBase(committedLSN uint64) (FullBasePin, error) {
	if m.failFullBasePin {
		return FullBasePin{}, fmt.Errorf("full base pin refused")
	}
	id := m.nextPinID.Add(1)
	m.pinnedFullBase[id] = true
	return FullBasePin{CommittedLSN: committedLSN, PinID: id, Valid: true}, nil
}

func (m *mockStorage) ReleaseFullBase(pin FullBasePin) {
	delete(m.pinnedFullBase, pin.PinID)
}

// --- Plan + execute: catch-up ---

func TestDriver_PlanRecovery_CatchUp(t *testing.T) {
	storage := newMockStorage(RetainedHistory{
		HeadLSN: 100, TailLSN: 30, CommittedLSN: 100,
	})
	driver := NewRecoveryDriver(storage)

	driver.Orchestrator.ProcessAssignment(AssignmentIntent{
		Replicas: []ReplicaAssignment{
			{ReplicaID: "r1", Endpoint: Endpoint{DataAddr: "r1:9333", Version: 1}},
		},
		Epoch:           1,
		RecoveryTargets: map[string]SessionKind{"r1": SessionCatchUp},
	})

	plan, err := driver.PlanRecovery("r1", 70)
	if err != nil {
		t.Fatal(err)
	}
	if plan.Outcome != OutcomeCatchUp {
		t.Fatalf("outcome=%s", plan.Outcome)
	}
	if plan.RetentionPin == nil {
		t.Fatal("WAL retention should be pinned")
	}
	if plan.CatchUpTarget != 100 {
		t.Fatalf("target=%d", plan.CatchUpTarget)
	}
	if !plan.Proof.Recoverable {
		t.Fatalf("proof: %s", plan.Proof.Reason)
	}

	// WAL is pinned.
	if len(storage.pinnedWAL) != 1 {
		t.Fatalf("expected 1 WAL pin, got %d", len(storage.pinnedWAL))
	}

	// Execute catch-up via orchestrator.
	driver.Orchestrator.CompleteCatchUp("r1", CatchUpOptions{TargetLSN: plan.CatchUpTarget})

	// Release resources.
	driver.ReleasePlan(plan)
	if len(storage.pinnedWAL) != 0 {
		t.Fatal("WAL pin should be released")
	}
}

// --- Plan + execute: zero-gap ---

func TestDriver_PlanRecovery_ZeroGap(t *testing.T) {
	storage := newMockStorage(RetainedHistory{
		HeadLSN: 100, TailLSN: 0, CommittedLSN: 100,
	})
	driver := NewRecoveryDriver(storage)

	driver.Orchestrator.ProcessAssignment(AssignmentIntent{
		Replicas: []ReplicaAssignment{
			{ReplicaID: "r1", Endpoint: Endpoint{DataAddr: "r1:9333", Version: 1}},
		},
		Epoch:           1,
		RecoveryTargets: map[string]SessionKind{"r1": SessionCatchUp},
	})

	plan, err := driver.PlanRecovery("r1", 100)
	if err != nil {
		t.Fatal(err)
	}
	if plan.Outcome != OutcomeZeroGap {
		t.Fatalf("outcome=%s", plan.Outcome)
	}

	// Zero-gap: no resources pinned.
	if plan.RetentionPin != nil {
		t.Fatal("zero-gap should not pin WAL")
	}

	// Already completed.
	if driver.Orchestrator.Registry.Sender("r1").State() != StateInSync {
		t.Fatalf("state=%s", driver.Orchestrator.Registry.Sender("r1").State())
	}
}

// --- Plan + execute: needs rebuild ---

func TestDriver_PlanRecovery_NeedsRebuild_ThenRebuild(t *testing.T) {
	storage := newMockStorage(RetainedHistory{
		HeadLSN: 100, TailLSN: 60, CommittedLSN: 100,
		CheckpointLSN: 50, CheckpointTrusted: true,
	})
	driver := NewRecoveryDriver(storage)

	driver.Orchestrator.ProcessAssignment(AssignmentIntent{
		Replicas: []ReplicaAssignment{
			{ReplicaID: "r1", Endpoint: Endpoint{DataAddr: "r1:9333", Version: 1}},
		},
		Epoch:           1,
		RecoveryTargets: map[string]SessionKind{"r1": SessionCatchUp},
	})

	// Plan: catch-up fails.
	plan, err := driver.PlanRecovery("r1", 30)
	if err != nil {
		t.Fatal(err)
	}
	if plan.Outcome != OutcomeNeedsRebuild {
		t.Fatalf("outcome=%s", plan.Outcome)
	}

	// Rebuild assignment.
	driver.Orchestrator.ProcessAssignment(AssignmentIntent{
		Replicas: []ReplicaAssignment{
			{ReplicaID: "r1", Endpoint: Endpoint{DataAddr: "r1:9333", Version: 1}},
		},
		Epoch:           1,
		RecoveryTargets: map[string]SessionKind{"r1": SessionRebuild},
	})

	// Plan rebuild with resource acquisition.
	rebuildPlan, err := driver.PlanRebuild("r1")
	if err != nil {
		t.Fatal(err)
	}
	// Checkpoint at 50, tail at 60 → unreplayable → full base.
	if rebuildPlan.RebuildSource != RebuildFullBase {
		t.Fatalf("source=%s (checkpoint at 50 but tail at 60)", rebuildPlan.RebuildSource)
	}

	// Execute rebuild via orchestrator.
	driver.Orchestrator.CompleteRebuild("r1", &storage.history)

	if driver.Orchestrator.Registry.Sender("r1").State() != StateInSync {
		t.Fatalf("state=%s", driver.Orchestrator.Registry.Sender("r1").State())
	}
}

// --- Resource failure: WAL pin refused ---

func TestDriver_PlanRecovery_WALPinFailure(t *testing.T) {
	storage := newMockStorage(RetainedHistory{
		HeadLSN: 100, TailLSN: 30, CommittedLSN: 100,
	})
	storage.failWALPin = true
	driver := NewRecoveryDriver(storage)

	driver.Orchestrator.ProcessAssignment(AssignmentIntent{
		Replicas: []ReplicaAssignment{
			{ReplicaID: "r1", Endpoint: Endpoint{DataAddr: "r1:9333", Version: 1}},
		},
		Epoch:           1,
		RecoveryTargets: map[string]SessionKind{"r1": SessionCatchUp},
	})

	_, err := driver.PlanRecovery("r1", 70)
	if err == nil {
		t.Fatal("should fail when WAL pin is refused")
	}

	// Log should show the failure.
	hasFailure := false
	for _, e := range driver.Orchestrator.Log.EventsFor("r1") {
		if e.Event == "wal_pin_failed" {
			hasFailure = true
		}
	}
	if !hasFailure {
		t.Fatal("log should contain wal_pin_failed")
	}
}

// --- Resource failure: snapshot pin refused → fallback ---

func TestDriver_PlanRebuild_SnapshotPinFailure(t *testing.T) {
	storage := newMockStorage(RetainedHistory{
		HeadLSN: 100, TailLSN: 30, CommittedLSN: 100,
		CheckpointLSN: 50, CheckpointTrusted: true,
	})
	storage.failSnapshotPin = true
	driver := NewRecoveryDriver(storage)

	driver.Orchestrator.ProcessAssignment(AssignmentIntent{
		Replicas: []ReplicaAssignment{
			{ReplicaID: "r1", Endpoint: Endpoint{DataAddr: "r1:9333", Version: 1}},
		},
		Epoch:           1,
		RecoveryTargets: map[string]SessionKind{"r1": SessionRebuild},
	})

	_, err := driver.PlanRebuild("r1")
	if err == nil {
		t.Fatal("should fail when snapshot pin is refused")
	}

	hasFailure := false
	for _, e := range driver.Orchestrator.Log.EventsFor("r1") {
		if e.Event == "snapshot_pin_failed" {
			hasFailure = true
		}
	}
	if !hasFailure {
		t.Fatal("log should contain snapshot_pin_failed")
	}
}

// --- Replica-ahead with truncation through driver ---

func TestDriver_PlanRecovery_ReplicaAhead_Truncation(t *testing.T) {
	storage := newMockStorage(RetainedHistory{
		HeadLSN: 100, TailLSN: 0, CommittedLSN: 100,
	})
	driver := NewRecoveryDriver(storage)

	driver.Orchestrator.ProcessAssignment(AssignmentIntent{
		Replicas: []ReplicaAssignment{
			{ReplicaID: "r1", Endpoint: Endpoint{DataAddr: "r1:9333", Version: 1}},
		},
		Epoch:           1,
		RecoveryTargets: map[string]SessionKind{"r1": SessionCatchUp},
	})

	plan, err := driver.PlanRecovery("r1", 105) // replica ahead
	if err != nil {
		t.Fatal(err)
	}
	if plan.Outcome != OutcomeCatchUp {
		t.Fatalf("outcome=%s", plan.Outcome)
	}
	if plan.TruncateLSN != 100 {
		t.Fatalf("truncate=%d, want 100", plan.TruncateLSN)
	}

	// Execute with truncation.
	err = driver.Orchestrator.CompleteCatchUp("r1", CatchUpOptions{
		TargetLSN:   plan.CatchUpTarget,
		TruncateLSN: plan.TruncateLSN,
	})
	if err != nil {
		t.Fatalf("catch-up with truncation: %v", err)
	}

	driver.ReleasePlan(plan)
}

// --- Full-base rebuild pin ---

func TestDriver_PlanRebuild_FullBase_PinsBaseImage(t *testing.T) {
	storage := newMockStorage(RetainedHistory{
		HeadLSN: 100, TailLSN: 60, CommittedLSN: 100,
		CheckpointLSN: 40, CheckpointTrusted: true,
	})
	driver := NewRecoveryDriver(storage)

	driver.Orchestrator.ProcessAssignment(AssignmentIntent{
		Replicas: []ReplicaAssignment{
			{ReplicaID: "r1", Endpoint: Endpoint{DataAddr: "r1:9333", Version: 1}},
		},
		Epoch:           1,
		RecoveryTargets: map[string]SessionKind{"r1": SessionRebuild},
	})

	plan, err := driver.PlanRebuild("r1")
	if err != nil {
		t.Fatal(err)
	}
	// Checkpoint at 40, tail at 60 → unreplayable → full base.
	if plan.RebuildSource != RebuildFullBase {
		t.Fatalf("source=%s", plan.RebuildSource)
	}
	if plan.FullBasePin == nil {
		t.Fatal("full_base rebuild must have a pinned base image")
	}
	if len(storage.pinnedFullBase) != 1 {
		t.Fatalf("expected 1 full base pin, got %d", len(storage.pinnedFullBase))
	}

	driver.ReleasePlan(plan)
	if len(storage.pinnedFullBase) != 0 {
		t.Fatal("full base pin should be released")
	}
}

func TestDriver_PlanRebuild_FullBase_PinFailure(t *testing.T) {
	storage := newMockStorage(RetainedHistory{
		HeadLSN: 100, TailLSN: 60, CommittedLSN: 100,
		CheckpointLSN: 40, CheckpointTrusted: true,
	})
	storage.failFullBasePin = true
	driver := NewRecoveryDriver(storage)

	driver.Orchestrator.ProcessAssignment(AssignmentIntent{
		Replicas: []ReplicaAssignment{
			{ReplicaID: "r1", Endpoint: Endpoint{DataAddr: "r1:9333", Version: 1}},
		},
		Epoch:           1,
		RecoveryTargets: map[string]SessionKind{"r1": SessionRebuild},
	})

	_, err := driver.PlanRebuild("r1")
	if err == nil {
		t.Fatal("should fail when full base pin is refused")
	}

	hasFailure := false
	for _, e := range driver.Orchestrator.Log.EventsFor("r1") {
		if e.Event == "full_base_pin_failed" {
			hasFailure = true
		}
	}
	if !hasFailure {
		t.Fatal("log should contain full_base_pin_failed")
	}
}

// --- WAL pin failure cleans up session ---

func TestDriver_PlanRecovery_WALPinFailure_CleansUpSession(t *testing.T) {
	storage := newMockStorage(RetainedHistory{
		HeadLSN: 100, TailLSN: 30, CommittedLSN: 100,
	})
	storage.failWALPin = true
	driver := NewRecoveryDriver(storage)

	driver.Orchestrator.ProcessAssignment(AssignmentIntent{
		Replicas: []ReplicaAssignment{
			{ReplicaID: "r1", Endpoint: Endpoint{DataAddr: "r1:9333", Version: 1}},
		},
		Epoch:           1,
		RecoveryTargets: map[string]SessionKind{"r1": SessionCatchUp},
	})

	_, err := driver.PlanRecovery("r1", 70)
	if err == nil {
		t.Fatal("should fail when WAL pin is refused")
	}

	// Session must be invalidated — no dangling live session.
	s := driver.Orchestrator.Registry.Sender("r1")
	if s.HasActiveSession() {
		t.Fatal("session should be invalidated after WAL pin failure")
	}
	if s.State() != StateDisconnected {
		t.Fatalf("sender should be disconnected after pin failure, got %s", s.State())
	}
}

// --- Cross-layer contract: storage proves recoverability ---

func TestDriver_CrossLayer_StorageProvesRecoverability(t *testing.T) {
	// The engine asks "is this recoverable?" and the storage adapter
	// answers from real state — not from test-reconstructed inputs.
	storage := newMockStorage(RetainedHistory{
		HeadLSN: 100, TailLSN: 50, CommittedLSN: 100,
		CheckpointLSN: 40, CheckpointTrusted: true,
	})
	driver := NewRecoveryDriver(storage)

	driver.Orchestrator.ProcessAssignment(AssignmentIntent{
		Replicas: []ReplicaAssignment{
			{ReplicaID: "r1", Endpoint: Endpoint{DataAddr: "r1:9333", Version: 1}},
		},
		Epoch:           1,
		RecoveryTargets: map[string]SessionKind{"r1": SessionCatchUp},
	})

	// Engine asks storage for recoverability proof.
	history := storage.GetRetainedHistory()
	proof := history.ProveRecoverability(60) // gap 60→100

	if !proof.Recoverable {
		t.Fatalf("storage should prove recoverable: %s", proof.Reason)
	}

	// Engine asks for rebuild source decision.
	source, snapLSN := history.RebuildSourceDecision()
	// Checkpoint at 40, tail at 50 → checkpoint < tail → unreplayable.
	if source != RebuildFullBase {
		t.Fatalf("source=%s snap=%d (checkpoint 40 < tail 50)", source, snapLSN)
	}

	// Failure is observable: log from PlanRecovery.
	plan, _ := driver.PlanRecovery("r1", 60)
	if plan.Proof == nil || !plan.Proof.Recoverable {
		t.Fatal("plan should carry proof from storage")
	}

	driver.ReleasePlan(plan)
}
