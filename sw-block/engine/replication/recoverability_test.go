package replication

import "testing"

// ============================================================
// Phase 05 Slice 3: Engine Data / Recoverability Core
//
// Tests validate that recovery decisions are backed by actual
// retained-history state, not just policy assertions.
// ============================================================

// --- Recoverable vs unrecoverable gap ---

func TestHistory_Recoverable_GapWithinRetention(t *testing.T) {
	rh := RetainedHistory{
		HeadLSN:      100,
		TailLSN:      30,
		CommittedLSN: 100,
	}

	if !rh.IsRecoverable(50, 100) {
		t.Fatal("gap 50→100 should be recoverable (tail=30)")
	}

	proof := rh.ProveRecoverability(50)
	if !proof.Recoverable {
		t.Fatalf("proof: %s", proof.Reason)
	}
}

func TestHistory_Unrecoverable_GapBeyondRetention(t *testing.T) {
	rh := RetainedHistory{
		HeadLSN:      100,
		TailLSN:      60,
		CommittedLSN: 100,
	}

	if rh.IsRecoverable(50, 100) {
		t.Fatal("gap 50→100 should NOT be recoverable (tail=60)")
	}

	proof := rh.ProveRecoverability(50)
	if proof.Recoverable {
		t.Fatal("proof should show unrecoverable")
	}
}

func TestHistory_ExactBoundary(t *testing.T) {
	rh := RetainedHistory{
		HeadLSN:      100,
		TailLSN:      50,
		CommittedLSN: 100,
	}

	// AT boundary: recoverable.
	if !rh.IsRecoverable(50, 100) {
		t.Fatal("exact boundary should be recoverable")
	}
	// ONE BELOW: unrecoverable.
	if rh.IsRecoverable(49, 100) {
		t.Fatal("below boundary should NOT be recoverable")
	}
}

func TestHistory_BeyondHead_Unrecoverable(t *testing.T) {
	rh := RetainedHistory{
		HeadLSN:      80,
		TailLSN:      0,
		CommittedLSN: 100,
	}

	if rh.IsRecoverable(0, 100) {
		t.Fatal("gap beyond head should NOT be recoverable")
	}
}

// --- Trusted base vs no trusted base ---

func TestHistory_RebuildSource_TrustedCheckpoint(t *testing.T) {
	rh := RetainedHistory{
		HeadLSN:           100,
		TailLSN:           30, // tail covers checkpoint→committed range
		CommittedLSN:      100,
		CheckpointLSN:     50,
		CheckpointTrusted: true,
	}

	source, snapLSN := rh.RebuildSourceDecision()
	if source != RebuildSnapshotTail {
		t.Fatalf("source=%s, want snapshot_tail", source)
	}
	if snapLSN != 50 {
		t.Fatalf("snapshot LSN=%d, want 50", snapLSN)
	}
}

func TestHistory_RebuildSource_NoCheckpoint(t *testing.T) {
	rh := RetainedHistory{
		CommittedLSN: 100,
	}

	source, snapLSN := rh.RebuildSourceDecision()
	if source != RebuildFullBase {
		t.Fatalf("source=%s, want full_base", source)
	}
	if snapLSN != 0 {
		t.Fatalf("snapshot LSN=%d, want 0", snapLSN)
	}
}

func TestHistory_RebuildSource_TrustedCheckpoint_UnreplayableTail(t *testing.T) {
	// Trusted checkpoint at 50, but TailLSN advanced to 80.
	// WAL from 50 to 100 is NOT fully retained (51-80 recycled).
	// Must fall back to full base, not snapshot+tail.
	rh := RetainedHistory{
		HeadLSN:           100,
		TailLSN:           80,
		CommittedLSN:      100,
		CheckpointLSN:     50,
		CheckpointTrusted: true,
	}

	source, _ := rh.RebuildSourceDecision()
	if source != RebuildFullBase {
		t.Fatalf("trusted checkpoint with unreplayable tail: source=%s, want full_base", source)
	}
}

func TestHistory_RebuildSource_UntrustedCheckpoint(t *testing.T) {
	rh := RetainedHistory{
		CheckpointLSN:     50,
		CheckpointTrusted: false,
		CommittedLSN:      100,
	}

	source, _ := rh.RebuildSourceDecision()
	if source != RebuildFullBase {
		t.Fatalf("untrusted checkpoint: source=%s, want full_base", source)
	}
}

// --- Handshake result from retained history ---

func TestHistory_MakeHandshakeResult(t *testing.T) {
	rh := RetainedHistory{
		HeadLSN:      100,
		TailLSN:      30,
		CommittedLSN: 90,
	}

	hr := rh.MakeHandshakeResult(70)
	if hr.ReplicaFlushedLSN != 70 {
		t.Fatalf("replica=%d", hr.ReplicaFlushedLSN)
	}
	if hr.CommittedLSN != 90 {
		t.Fatalf("committed=%d", hr.CommittedLSN)
	}
	if hr.RetentionStartLSN != 31 {
		t.Fatalf("retention=%d, want 31", hr.RetentionStartLSN)
	}

	outcome := ClassifyRecoveryOutcome(hr)
	if outcome != OutcomeCatchUp {
		t.Fatalf("outcome=%s", outcome)
	}
}

// --- Recoverability proof ---

func TestHistory_Proof_ZeroGap(t *testing.T) {
	rh := RetainedHistory{CommittedLSN: 100}
	proof := rh.ProveRecoverability(100)
	if !proof.Recoverable || proof.Reason != "zero_gap" {
		t.Fatalf("proof: recoverable=%v reason=%s", proof.Recoverable, proof.Reason)
	}
}

func TestHistory_Proof_ReplicaAhead(t *testing.T) {
	rh := RetainedHistory{CommittedLSN: 100}
	proof := rh.ProveRecoverability(105)
	if !proof.Recoverable || proof.Reason != "replica_ahead_needs_truncation" {
		t.Fatalf("proof: recoverable=%v reason=%s", proof.Recoverable, proof.Reason)
	}
}

func TestHistory_Proof_GapWithinRetention(t *testing.T) {
	rh := RetainedHistory{HeadLSN: 100, TailLSN: 30, CommittedLSN: 100}
	proof := rh.ProveRecoverability(50)
	if !proof.Recoverable {
		t.Fatalf("proof: %s", proof.Reason)
	}
}

func TestHistory_Proof_GapBeyondRetention(t *testing.T) {
	rh := RetainedHistory{HeadLSN: 100, TailLSN: 60, CommittedLSN: 100}
	proof := rh.ProveRecoverability(50)
	if proof.Recoverable {
		t.Fatal("should not be recoverable")
	}
}

// --- End-to-end: retained-history-driven recovery flow ---

func TestHistory_E2E_RecoveryDrivenByRetainedHistory(t *testing.T) {
	// Primary's retained history.
	primary := RetainedHistory{
		HeadLSN:           100,
		TailLSN:           30,
		CommittedLSN:      100,
		CheckpointLSN:     50,
		CheckpointTrusted: true,
	}

	r := NewRegistry()
	r.ApplyAssignment(AssignmentIntent{
		Replicas: []ReplicaAssignment{
			{ReplicaID: "r1", Endpoint: Endpoint{DataAddr: "10.0.0.1:9333", Version: 1}},
			{ReplicaID: "r2", Endpoint: Endpoint{DataAddr: "10.0.0.2:9333", Version: 1}},
		},
		Epoch: 1,
		RecoveryTargets: map[string]SessionKind{
			"r1": SessionCatchUp,
			"r2": SessionCatchUp,
		},
	})

	// r1: replica at LSN 70 — catch-up (within retention).
	r1 := r.Sender("r1")
	id1 := r1.SessionID()
	r1.BeginConnect(id1)

	proof1 := primary.ProveRecoverability(70)
	if !proof1.Recoverable {
		t.Fatalf("r1: %s", proof1.Reason)
	}

	hr1 := primary.MakeHandshakeResult(70)
	o1, _ := r1.RecordHandshakeWithOutcome(id1, hr1)
	if o1 != OutcomeCatchUp {
		t.Fatalf("r1: outcome=%s", o1)
	}
	r1.BeginCatchUp(id1)
	r1.RecordCatchUpProgress(id1, 100)
	r1.CompleteSessionByID(id1)

	// r2: replica at LSN 20 — needs rebuild (beyond retention).
	r2 := r.Sender("r2")
	id2 := r2.SessionID()
	r2.BeginConnect(id2)

	proof2 := primary.ProveRecoverability(20)
	if proof2.Recoverable {
		t.Fatal("r2: should not be recoverable")
	}

	hr2 := primary.MakeHandshakeResult(20)
	o2, _ := r2.RecordHandshakeWithOutcome(id2, hr2)
	if o2 != OutcomeNeedsRebuild {
		t.Fatalf("r2: outcome=%s", o2)
	}

	// r2 needs rebuild — use history to choose source.
	source, snapLSN := primary.RebuildSourceDecision()
	if source != RebuildSnapshotTail || snapLSN != 50 {
		t.Fatalf("rebuild source=%s snap=%d", source, snapLSN)
	}

	// New rebuild session for r2.
	r.ApplyAssignment(AssignmentIntent{
		Replicas: []ReplicaAssignment{
			{ReplicaID: "r2", Endpoint: Endpoint{DataAddr: "10.0.0.2:9333", Version: 1}},
		},
		Epoch:           1,
		RecoveryTargets: map[string]SessionKind{"r2": SessionRebuild},
	})

	id2b := r2.SessionID()
	r2.BeginConnect(id2b)
	r2.RecordHandshake(id2b, 0, 100)
	r2.SelectRebuildSource(id2b, snapLSN, true, primary.CommittedLSN)
	r2.BeginRebuildTransfer(id2b)
	r2.RecordRebuildTransferProgress(id2b, snapLSN)
	r2.BeginRebuildTailReplay(id2b)
	r2.RecordRebuildTailProgress(id2b, 100)
	r2.CompleteRebuild(id2b)

	if r1.State() != StateInSync || r2.State() != StateInSync {
		t.Fatalf("r1=%s r2=%s", r1.State(), r2.State())
	}
	t.Log("e2e: r1 caught up (proof: gap within retention), r2 rebuilt (proof: gap beyond retention, snapshot+tail)")
}

// --- Sender-level history-driven APIs ---

func TestHistory_SenderDriven_CatchUp(t *testing.T) {
	primary := RetainedHistory{HeadLSN: 100, TailLSN: 30, CommittedLSN: 100}

	s := NewSender("r1", Endpoint{DataAddr: "r1:9333", Version: 1}, 1)
	id, _ := s.AttachSession(1, SessionCatchUp)
	s.BeginConnect(id)

	// Use history-driven API.
	outcome, proof, err := s.RecordHandshakeFromHistory(id, 70, &primary)
	if err != nil {
		t.Fatal(err)
	}
	if outcome != OutcomeCatchUp {
		t.Fatalf("outcome=%s", outcome)
	}
	if !proof.Recoverable {
		t.Fatalf("proof should be recoverable: %s", proof.Reason)
	}

	s.BeginCatchUp(id)
	s.RecordCatchUpProgress(id, 100)
	s.CompleteSessionByID(id)

	if s.State() != StateInSync {
		t.Fatalf("state=%s", s.State())
	}
}

func TestHistory_SenderDriven_Rebuild_SnapshotTail(t *testing.T) {
	primary := RetainedHistory{
		HeadLSN: 100, TailLSN: 30, CommittedLSN: 100,
		CheckpointLSN: 50, CheckpointTrusted: true,
	}

	s := NewSender("r1", Endpoint{DataAddr: "r1:9333", Version: 1}, 1)
	id, _ := s.AttachSession(1, SessionRebuild)
	s.BeginConnect(id)
	s.RecordHandshake(id, 0, 100)

	// Use history-driven rebuild source selection.
	if err := s.SelectRebuildFromHistory(id, &primary); err != nil {
		t.Fatal(err)
	}

	// Should have selected snapshot+tail (checkpoint at 50, tail at 30, replayable).
	s.BeginRebuildTransfer(id)
	s.RecordRebuildTransferProgress(id, 50)
	s.BeginRebuildTailReplay(id)
	s.RecordRebuildTailProgress(id, 100)
	s.CompleteRebuild(id)

	if s.State() != StateInSync {
		t.Fatalf("state=%s", s.State())
	}
}

func TestHistory_SenderDriven_Rebuild_FallsBackToFullBase(t *testing.T) {
	// Trusted checkpoint at 50 but tail advanced to 80 — tail unreplayable.
	primary := RetainedHistory{
		HeadLSN: 100, TailLSN: 80, CommittedLSN: 100,
		CheckpointLSN: 50, CheckpointTrusted: true,
	}

	s := NewSender("r1", Endpoint{DataAddr: "r1:9333", Version: 1}, 1)
	id, _ := s.AttachSession(1, SessionRebuild)
	s.BeginConnect(id)
	s.RecordHandshake(id, 0, 100)

	// History-driven: should fall back to full base.
	if err := s.SelectRebuildFromHistory(id, &primary); err != nil {
		t.Fatal(err)
	}

	// Full base: transfer to 100, no tail replay.
	s.BeginRebuildTransfer(id)
	s.RecordRebuildTransferProgress(id, 100)
	s.CompleteRebuild(id)

	if s.State() != StateInSync {
		t.Fatalf("state=%s", s.State())
	}
}

// --- Truncation / safe-boundary handling ---

func TestHistory_Proof_TruncationRequired(t *testing.T) {
	rh := RetainedHistory{CommittedLSN: 100}

	// Replica ahead → truncation required.
	proof := rh.ProveRecoverability(105)
	if proof.Reason != "replica_ahead_needs_truncation" {
		t.Fatalf("reason=%s", proof.Reason)
	}

	// Sender execution: handshake sets truncation requirement.
	s := NewSender("r1", Endpoint{DataAddr: "r1:9333", Version: 1}, 1)
	id, _ := s.AttachSession(1, SessionCatchUp)
	s.BeginConnect(id)

	hr := rh.MakeHandshakeResult(105)
	outcome, _ := s.RecordHandshakeWithOutcome(id, hr)
	if outcome != OutcomeCatchUp {
		t.Fatalf("outcome=%s", outcome)
	}

	// Session should require truncation.
	snap := s.SessionSnapshot()
	if snap == nil {
		t.Fatal("session should exist")
	}

	// Completion without truncation rejected.
	if s.CompleteSessionByID(id) {
		t.Fatal("should reject completion without truncation")
	}

	// Record truncation.
	s.RecordTruncation(id, 100)

	// Now completion works (zero-gap after truncation).
	if !s.CompleteSessionByID(id) {
		t.Fatal("completion after truncation should succeed")
	}
}
