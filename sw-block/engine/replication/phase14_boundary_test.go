package replication

import "testing"

func TestPhase14_BoundaryTruthsStayDistinct(t *testing.T) {
	core := NewCoreEngine()

	core.ApplyEvent(CommittedLSNAdvanced{ID: "vol-boundary", CommittedLSN: 20})
	core.ApplyEvent(CheckpointAdvanced{ID: "vol-boundary", CheckpointLSN: 12})
	core.ApplyEvent(DiagnosticShippedAdvanced{ID: "vol-boundary", ShippedLSN: 30})
	result := core.ApplyEvent(BarrierAccepted{ID: "vol-boundary", FlushedLSN: 8})

	if result.Projection.Boundary.CommittedLSN != 20 {
		t.Fatalf("committed_lsn=%d", result.Projection.Boundary.CommittedLSN)
	}
	if result.Projection.Boundary.CheckpointLSN != 12 {
		t.Fatalf("checkpoint_lsn=%d", result.Projection.Boundary.CheckpointLSN)
	}
	if result.Projection.Boundary.DurableLSN != 8 {
		t.Fatalf("durable_lsn=%d", result.Projection.Boundary.DurableLSN)
	}
	if result.Projection.Boundary.DiagnosticShippedLSN != 30 {
		t.Fatalf("diagnostic_shipped_lsn=%d", result.Projection.Boundary.DiagnosticShippedLSN)
	}
}

func TestPhase14_RecoveryCatchUpBlocksReplicaReadyOverclaim(t *testing.T) {
	core := NewCoreEngine()

	core.ApplyEvent(AssignmentDelivered{
		ID:    "vol-recovery",
		Epoch: 4,
		Role:  RoleReplica,
		Replicas: []ReplicaAssignment{
			{ReplicaID: "replica-1", Endpoint: Endpoint{DataAddr: "10.0.0.20:9333", CtrlAddr: "10.0.0.20:9334", Version: 1}},
		},
	})
	core.ApplyEvent(RoleApplied{ID: "vol-recovery"})
	core.ApplyEvent(ReceiverReadyObserved{ID: "vol-recovery"})

	result := core.ApplyEvent(CatchUpPlanned{ID: "vol-recovery", TargetLSN: 40})
	if result.Projection.Mode.Name != ModeBootstrapPending {
		t.Fatalf("mode=%s", result.Projection.Mode.Name)
	}
	if result.Projection.Publication.Reason != "recovery_in_progress" {
		t.Fatalf("publication_reason=%q", result.Projection.Publication.Reason)
	}
	if result.State.Recovery.Phase != RecoveryCatchingUp {
		t.Fatalf("recovery_phase=%s", result.State.Recovery.Phase)
	}
	if result.Projection.Recovery.Phase != RecoveryCatchingUp {
		t.Fatalf("projection_recovery_phase=%s", result.Projection.Recovery.Phase)
	}
	if result.Projection.Boundary.TargetLSN != 40 {
		t.Fatalf("target_lsn=%d", result.Projection.Boundary.TargetLSN)
	}

	result = core.ApplyEvent(RecoveryProgressObserved{ID: "vol-recovery", AchievedLSN: 25})
	if result.Projection.Mode.Name != ModeBootstrapPending {
		t.Fatalf("mode=%s", result.Projection.Mode.Name)
	}
	if result.Projection.Publication.Healthy {
		t.Fatal("catch-up progress must not establish healthy publication")
	}
	if result.Projection.Boundary.AchievedLSN != 25 {
		t.Fatalf("achieved_lsn=%d", result.Projection.Boundary.AchievedLSN)
	}

	result = core.ApplyEvent(CatchUpCompleted{ID: "vol-recovery", AchievedLSN: 40})
	if result.State.Recovery.Phase != RecoveryIdle {
		t.Fatalf("recovery_phase=%s", result.State.Recovery.Phase)
	}
	if result.Projection.Recovery.AchievedLSN != 40 {
		t.Fatalf("projection_achieved_lsn=%d", result.Projection.Recovery.AchievedLSN)
	}
	if result.Projection.Boundary.DurableLSN != 40 {
		t.Fatalf("durable_lsn=%d", result.Projection.Boundary.DurableLSN)
	}
	if result.Projection.Mode.Name != ModeReplicaReady {
		t.Fatalf("mode=%s", result.Projection.Mode.Name)
	}
	if result.Projection.Publication.Healthy {
		t.Fatal("catch-up completion on replica must not overclaim publish healthy")
	}
}

func TestPhase14_RebuildCommitClosesRecoveryBoundary(t *testing.T) {
	core := NewCoreEngine()

	core.ApplyEvent(AssignmentDelivered{
		ID:    "vol-rebuild",
		Epoch: 8,
		Role:  RoleReplica,
		Replicas: []ReplicaAssignment{
			{ReplicaID: "replica-1", Endpoint: Endpoint{DataAddr: "10.0.0.21:9333", CtrlAddr: "10.0.0.21:9334", Version: 1}},
		},
	})
	core.ApplyEvent(RoleApplied{ID: "vol-rebuild"})
	core.ApplyEvent(ReceiverReadyObserved{ID: "vol-rebuild"})

	result := core.ApplyEvent(NeedsRebuildObserved{ID: "vol-rebuild", Reason: "gap_too_large"})
	if result.State.Recovery.Phase != RecoveryNeedsRebuild {
		t.Fatalf("recovery_phase=%s", result.State.Recovery.Phase)
	}
	if result.Projection.Mode.Name != ModeNeedsRebuild {
		t.Fatalf("mode=%s", result.Projection.Mode.Name)
	}

	result = core.ApplyEvent(RebuildStarted{ID: "vol-rebuild", TargetLSN: 64})
	if result.State.Recovery.Phase != RecoveryRebuilding {
		t.Fatalf("recovery_phase=%s", result.State.Recovery.Phase)
	}
	if result.Projection.Recovery.Phase != RecoveryRebuilding {
		t.Fatalf("projection_recovery_phase=%s", result.Projection.Recovery.Phase)
	}
	if result.Projection.Boundary.TargetLSN != 64 {
		t.Fatalf("target_lsn=%d", result.Projection.Boundary.TargetLSN)
	}
	if result.Projection.Mode.Name != ModeNeedsRebuild {
		t.Fatalf("mode=%s", result.Projection.Mode.Name)
	}

	result = core.ApplyEvent(RebuildCommitted{
		ID:            "vol-rebuild",
		AchievedLSN:   64,
		FlushedLSN:    64,
		CheckpointLSN: 64,
	})
	if result.State.Recovery.Phase != RecoveryIdle {
		t.Fatalf("recovery_phase=%s", result.State.Recovery.Phase)
	}
	if result.Projection.Recovery.Phase != RecoveryIdle {
		t.Fatalf("projection_recovery_phase=%s", result.Projection.Recovery.Phase)
	}
	if result.Projection.Boundary.AchievedLSN != 64 {
		t.Fatalf("achieved_lsn=%d", result.Projection.Boundary.AchievedLSN)
	}
	if result.Projection.Boundary.DurableLSN != 64 {
		t.Fatalf("durable_lsn=%d", result.Projection.Boundary.DurableLSN)
	}
	if result.Projection.Boundary.CheckpointLSN != 64 {
		t.Fatalf("checkpoint_lsn=%d", result.Projection.Boundary.CheckpointLSN)
	}
	if result.Projection.Mode.Name != ModeReplicaReady {
		t.Fatalf("mode=%s", result.Projection.Mode.Name)
	}
	if result.Projection.Publication.Healthy {
		t.Fatal("replica rebuild completion must not overclaim healthy publication")
	}
}

func TestPhase14_AssignmentChangeClearsStaleRecoveryTruth(t *testing.T) {
	core := NewCoreEngine()

	core.ApplyEvent(AssignmentDelivered{
		ID:    "vol-reassign",
		Epoch: 1,
		Role:  RoleReplica,
		Replicas: []ReplicaAssignment{
			{ReplicaID: "replica-1", Endpoint: Endpoint{DataAddr: "10.0.0.30:9333", CtrlAddr: "10.0.0.30:9334", Version: 1}},
		},
	})
	core.ApplyEvent(RoleApplied{ID: "vol-reassign"})
	core.ApplyEvent(ReceiverReadyObserved{ID: "vol-reassign"})
	core.ApplyEvent(CatchUpPlanned{ID: "vol-reassign", TargetLSN: 70})
	core.ApplyEvent(RecoveryProgressObserved{ID: "vol-reassign", AchievedLSN: 41})

	result := core.ApplyEvent(AssignmentDelivered{
		ID:    "vol-reassign",
		Epoch: 2,
		Role:  RoleReplica,
		Replicas: []ReplicaAssignment{
			{ReplicaID: "replica-1", Endpoint: Endpoint{DataAddr: "10.0.0.31:9333", CtrlAddr: "10.0.0.31:9334", Version: 2}},
		},
	})

	if result.State.Recovery.Phase != RecoveryIdle {
		t.Fatalf("recovery_phase=%s", result.State.Recovery.Phase)
	}
	if result.Projection.Boundary.TargetLSN != 0 {
		t.Fatalf("target_lsn=%d", result.Projection.Boundary.TargetLSN)
	}
	if result.Projection.Boundary.AchievedLSN != 0 {
		t.Fatalf("achieved_lsn=%d", result.Projection.Boundary.AchievedLSN)
	}
	if result.Projection.Mode.Name != ModeBootstrapPending {
		t.Fatalf("mode=%s", result.Projection.Mode.Name)
	}
	if result.Projection.Publication.Reason != "awaiting_role_apply" {
		t.Fatalf("publication_reason=%q", result.Projection.Publication.Reason)
	}
}
