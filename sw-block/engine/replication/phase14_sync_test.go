package replication

import "testing"

func TestPhase14_SyncAckObserved_AckUpdatesViewAndDurability(t *testing.T) {
	core := NewCoreEngine()

	core.ApplyEvent(AssignmentDelivered{
		ID:             "vol-sync-ack",
		Epoch:          1,
		Role:           RolePrimary,
		RecoveryTarget: SessionCatchUp,
		Replicas: []ReplicaAssignment{
			{ReplicaID: "replica-1", Endpoint: Endpoint{DataAddr: "10.0.0.50:9333", CtrlAddr: "10.0.0.50:9334", Version: 1}},
		},
	})
	core.ApplyEvent(RoleApplied{ID: "vol-sync-ack"})
	core.ApplyEvent(ShipperConfiguredObserved{ID: "vol-sync-ack"})
	core.ApplyEvent(ShipperConnectedObserved{ID: "vol-sync-ack"})

	result := core.ApplyEvent(SyncAckObserved{
		ID:         "vol-sync-ack",
		ReplicaID:  "replica-1",
		AckKind:    SyncAckQuorum,
		TargetLSN:  120,
		DurableLSN: 120,
		AppliedLSN: 120,
	})

	if result.Projection.Sync.AckKind != SyncAckQuorum {
		t.Fatalf("sync_ack_kind=%s", result.Projection.Sync.AckKind)
	}
	if result.Projection.Sync.Action != SyncActionKeepUp {
		t.Fatalf("sync_action=%s", result.Projection.Sync.Action)
	}
	if result.Projection.Boundary.DurableLSN != 120 {
		t.Fatalf("durable_lsn=%d", result.Projection.Boundary.DurableLSN)
	}
	if result.Projection.Mode.Name != ModePublishHealthy {
		t.Fatalf("mode=%s", result.Projection.Mode.Name)
	}
	if !result.Projection.Publication.Healthy {
		t.Fatal("ack should establish healthy publication on ready primary")
	}
}

func TestPhase14_SyncAckObserved_TimedOutFactsStartCatchUp(t *testing.T) {
	core := NewCoreEngine()

	core.ApplyEvent(AssignmentDelivered{
		ID:             "vol-sync-catchup",
		Epoch:          1,
		Role:           RolePrimary,
		RecoveryTarget: SessionCatchUp,
		Replicas: []ReplicaAssignment{
			{ReplicaID: "replica-1", Endpoint: Endpoint{DataAddr: "10.0.0.51:9333", CtrlAddr: "10.0.0.51:9334", Version: 1}},
		},
	})
	core.ApplyEvent(RoleApplied{ID: "vol-sync-catchup"})
	core.ApplyEvent(ShipperConfiguredObserved{ID: "vol-sync-catchup"})
	core.ApplyEvent(ShipperConnectedObserved{ID: "vol-sync-catchup"})

	result := core.ApplyEvent(SyncAckObserved{
		ID:             "vol-sync-catchup",
		ReplicaID:      "replica-1",
		AckKind:        SyncAckTimedOut,
		TargetLSN:      1000,
		PrimaryTailLSN: 700,
		DurableLSN:     500,
		AppliedLSN:     900,
		Reason:         "gap_within_retention",
	})

	assertCommandNames(t, result.Commands, []string{
		"start_catchup",
		"publish_projection",
	})
	if result.State.Recovery.Phase != RecoveryCatchingUp {
		t.Fatalf("recovery_phase=%s", result.State.Recovery.Phase)
	}
	if result.Projection.Recovery.TargetLSN != 1000 {
		t.Fatalf("target_lsn=%d", result.Projection.Recovery.TargetLSN)
	}
	if result.Projection.Recovery.AchievedLSN != 900 {
		t.Fatalf("achieved_lsn=%d", result.Projection.Recovery.AchievedLSN)
	}
	if result.Projection.Sync.AckKind != SyncAckTimedOut {
		t.Fatalf("sync_ack_kind=%s", result.Projection.Sync.AckKind)
	}
	if result.Projection.Sync.Action != SyncActionCatchUp {
		t.Fatalf("sync_action=%s", result.Projection.Sync.Action)
	}
	if result.Projection.Mode.Name != ModeBootstrapPending {
		t.Fatalf("mode=%s", result.Projection.Mode.Name)
	}
	if result.Projection.Publication.Healthy {
		t.Fatal("recoverable catch-up should not overclaim healthy publication")
	}
}

func TestPhase14_SyncAckObserved_TimedOutStillPreservesCatchUpContract(t *testing.T) {
	core := NewCoreEngine()

	core.ApplyEvent(AssignmentDelivered{
		ID:             "vol-sync-timeout",
		Epoch:          1,
		Role:           RolePrimary,
		RecoveryTarget: SessionCatchUp,
		Replicas: []ReplicaAssignment{
			{ReplicaID: "replica-1", Endpoint: Endpoint{DataAddr: "10.0.0.52:9333", CtrlAddr: "10.0.0.52:9334", Version: 1}},
		},
	})
	core.ApplyEvent(RoleApplied{ID: "vol-sync-timeout"})
	core.ApplyEvent(ShipperConfiguredObserved{ID: "vol-sync-timeout"})
	core.ApplyEvent(ShipperConnectedObserved{ID: "vol-sync-timeout"})

	result := core.ApplyEvent(SyncAckObserved{
		ID:             "vol-sync-timeout",
		ReplicaID:      "replica-1",
		AckKind:        SyncAckTimedOut,
		TargetLSN:      1000,
		PrimaryTailLSN: 700,
		DurableLSN:     500,
		AppliedLSN:     880,
		Reason:         "deadline_exceeded",
	})

	assertCommandNames(t, result.Commands, []string{
		"start_catchup",
		"publish_projection",
	})
	if result.Projection.Mode.Name != ModeBootstrapPending {
		t.Fatalf("mode=%s", result.Projection.Mode.Name)
	}
	if result.Projection.Recovery.Phase != RecoveryCatchingUp {
		t.Fatalf("recovery_phase=%s", result.Projection.Recovery.Phase)
	}
	if result.Projection.Recovery.AchievedLSN != 880 {
		t.Fatalf("achieved_lsn=%d", result.Projection.Recovery.AchievedLSN)
	}
	if result.Projection.Boundary.LastBarrierReason != "deadline_exceeded" {
		t.Fatalf("last_barrier_reason=%q", result.Projection.Boundary.LastBarrierReason)
	}
}

func TestPhase14_SyncAckObserved_TransportLostEscalatesTargetedReplica(t *testing.T) {
	core := NewCoreEngine()

	core.ApplyEvent(AssignmentDelivered{
		ID:             "vol-sync-rebuild",
		Epoch:          1,
		Role:           RolePrimary,
		RecoveryTarget: SessionCatchUp,
		Replicas: []ReplicaAssignment{
			{ReplicaID: "replica-1", Endpoint: Endpoint{DataAddr: "10.0.0.53:9333", CtrlAddr: "10.0.0.53:9334", Version: 1}},
			{ReplicaID: "replica-2", Endpoint: Endpoint{DataAddr: "10.0.0.54:9333", CtrlAddr: "10.0.0.54:9334", Version: 1}},
		},
	})

	result := core.ApplyEvent(SyncAckObserved{
		ID:         "vol-sync-rebuild",
		ReplicaID:  "replica-2",
		AckKind:    SyncAckTransportLost,
		TargetLSN:  1200,
		DurableLSN: 512,
		AppliedLSN: 900,
		Reason:     "recoverability_lost",
	})

	assertCommandNames(t, result.Commands, []string{
		"invalidate_session",
		"publish_projection",
	})
	invalidate, ok := result.Commands[0].(InvalidateSessionCommand)
	if !ok {
		t.Fatalf("cmd0=%T", result.Commands[0])
	}
	if invalidate.ReplicaID != "replica-2" {
		t.Fatalf("invalidate_replica=%q", invalidate.ReplicaID)
	}
	if result.Projection.Mode.Name != ModeNeedsRebuild {
		t.Fatalf("mode=%s", result.Projection.Mode.Name)
	}
	if result.Projection.Recovery.Phase != RecoveryNeedsRebuild {
		t.Fatalf("recovery_phase=%s", result.Projection.Recovery.Phase)
	}
	if result.Projection.Sync.Action != SyncActionRebuild {
		t.Fatalf("sync_action=%s", result.Projection.Sync.Action)
	}
}

func TestPhase14_SyncAckObserved_TracksPerReplicaSyncFacts(t *testing.T) {
	core := NewCoreEngine()

	core.ApplyEvent(AssignmentDelivered{
		ID:             "vol-sync-multi",
		Epoch:          1,
		Role:           RolePrimary,
		RecoveryTarget: SessionCatchUp,
		Replicas: []ReplicaAssignment{
			{ReplicaID: "replica-1", Endpoint: Endpoint{DataAddr: "10.0.0.55:9333", CtrlAddr: "10.0.0.55:9334", Version: 1}},
			{ReplicaID: "replica-2", Endpoint: Endpoint{DataAddr: "10.0.0.56:9333", CtrlAddr: "10.0.0.56:9334", Version: 1}},
		},
	})
	core.ApplyEvent(RoleApplied{ID: "vol-sync-multi"})
	core.ApplyEvent(ShipperConfiguredObserved{ID: "vol-sync-multi"})
	core.ApplyEvent(ShipperConnectedObserved{ID: "vol-sync-multi"})

	core.ApplyEvent(SyncAckObserved{
		ID:             "vol-sync-multi",
		ReplicaID:      "replica-1",
		AckKind:        SyncAckTimedOut,
		TargetLSN:      1000,
		PrimaryTailLSN: 700,
		DurableLSN:     400,
		AppliedLSN:     850,
		Reason:         "deadline_exceeded",
	})
	result := core.ApplyEvent(SyncAckObserved{
		ID:         "vol-sync-multi",
		ReplicaID:  "replica-2",
		AckKind:    SyncAckQuorum,
		TargetLSN:  1000,
		DurableLSN: 1000,
		AppliedLSN: 1000,
	})

	if len(result.Projection.ReplicaSync) != 2 {
		t.Fatalf("replica_sync_len=%d", len(result.Projection.ReplicaSync))
	}
	if got := result.Projection.ReplicaSync["replica-1"].AckKind; got != SyncAckTimedOut {
		t.Fatalf("replica1_ack_kind=%s", got)
	}
	if got := result.Projection.ReplicaSync["replica-1"].Action; got != SyncActionCatchUp {
		t.Fatalf("replica1_action=%s", got)
	}
	if got := result.Projection.ReplicaSync["replica-2"].AckKind; got != SyncAckQuorum {
		t.Fatalf("replica2_ack_kind=%s", got)
	}
	if got := result.Projection.ReplicaSync["replica-2"].Action; got != SyncActionKeepUp {
		t.Fatalf("replica2_action=%s", got)
	}
}

func TestPhase14_SyncAckObserved_ReplicaAckDoesNotClearOtherReplicaRebuild(t *testing.T) {
	core := NewCoreEngine()

	core.ApplyEvent(AssignmentDelivered{
		ID:             "vol-sync-rebuild-aggregate",
		Epoch:          1,
		Role:           RolePrimary,
		RecoveryTarget: SessionCatchUp,
		Replicas: []ReplicaAssignment{
			{ReplicaID: "replica-1", Endpoint: Endpoint{DataAddr: "10.0.0.61:9333", CtrlAddr: "10.0.0.61:9334", Version: 1}},
			{ReplicaID: "replica-2", Endpoint: Endpoint{DataAddr: "10.0.0.62:9333", CtrlAddr: "10.0.0.62:9334", Version: 1}},
		},
	})

	core.ApplyEvent(SyncAckObserved{
		ID:             "vol-sync-rebuild-aggregate",
		ReplicaID:      "replica-1",
		AckKind:        SyncAckTimedOut,
		TargetLSN:      1000,
		PrimaryTailLSN: 700,
		DurableLSN:     500,
		AppliedLSN:     600,
		Reason:         "gap_beyond_retention",
	})

	result := core.ApplyEvent(SyncAckObserved{
		ID:         "vol-sync-rebuild-aggregate",
		ReplicaID:  "replica-2",
		AckKind:    SyncAckQuorum,
		TargetLSN:  1000,
		DurableLSN: 1000,
		AppliedLSN: 1000,
	})

	if result.Projection.Mode.Name != ModeNeedsRebuild {
		t.Fatalf("mode=%s", result.Projection.Mode.Name)
	}
	if result.Projection.Recovery.Phase != RecoveryNeedsRebuild {
		t.Fatalf("recovery_phase=%s", result.Projection.Recovery.Phase)
	}
	if got := result.Projection.ReplicaSync["replica-1"].Action; got != SyncActionRebuild {
		t.Fatalf("replica1_action=%s", got)
	}
	if got := result.Projection.ReplicaSync["replica-2"].Action; got != SyncActionKeepUp {
		t.Fatalf("replica2_action=%s", got)
	}
}

func TestPhase14_SyncAckObserved_AssignmentChangeClearsReplicaSyncFacts(t *testing.T) {
	core := NewCoreEngine()

	core.ApplyEvent(AssignmentDelivered{
		ID:             "vol-sync-reset",
		Epoch:          1,
		Role:           RolePrimary,
		RecoveryTarget: SessionCatchUp,
		Replicas: []ReplicaAssignment{
			{ReplicaID: "replica-1", Endpoint: Endpoint{DataAddr: "10.0.0.57:9333", CtrlAddr: "10.0.0.57:9334", Version: 1}},
		},
	})
	core.ApplyEvent(SyncAckObserved{
		ID:         "vol-sync-reset",
		ReplicaID:  "replica-1",
		AckKind:    SyncAckQuorum,
		TargetLSN:  30,
		DurableLSN: 30,
		AppliedLSN: 30,
	})

	result := core.ApplyEvent(AssignmentDelivered{
		ID:             "vol-sync-reset",
		Epoch:          2,
		Role:           RolePrimary,
		RecoveryTarget: SessionCatchUp,
		Replicas: []ReplicaAssignment{
			{ReplicaID: "replica-1", Endpoint: Endpoint{DataAddr: "10.0.0.58:9333", CtrlAddr: "10.0.0.58:9334", Version: 2}},
		},
	})

	if result.Projection.Sync.AckKind != SyncAckUnknown {
		t.Fatalf("sync_ack_kind=%s", result.Projection.Sync.AckKind)
	}
	if len(result.Projection.ReplicaSync) != 0 {
		t.Fatalf("replica_sync_len=%d", len(result.Projection.ReplicaSync))
	}
}

func TestPhase14_SyncAckObserved_PrimaryTailFactsDriveCatchUp(t *testing.T) {
	core := NewCoreEngine()

	core.ApplyEvent(AssignmentDelivered{
		ID:             "vol-sync-facts-catchup",
		Epoch:          1,
		Role:           RolePrimary,
		RecoveryTarget: SessionCatchUp,
		Replicas: []ReplicaAssignment{
			{ReplicaID: "replica-1", Endpoint: Endpoint{DataAddr: "10.0.0.59:9333", CtrlAddr: "10.0.0.59:9334", Version: 1}},
		},
	})
	core.ApplyEvent(RoleApplied{ID: "vol-sync-facts-catchup"})
	core.ApplyEvent(ShipperConfiguredObserved{ID: "vol-sync-facts-catchup"})
	core.ApplyEvent(ShipperConnectedObserved{ID: "vol-sync-facts-catchup"})

	result := core.ApplyEvent(SyncAckObserved{
		ID:             "vol-sync-facts-catchup",
		ReplicaID:      "replica-1",
		AckKind:        SyncAckTimedOut,
		TargetLSN:      1000,
		PrimaryTailLSN: 700,
		DurableLSN:     500,
		AppliedLSN:     900,
		Reason:         "gap_within_retention",
	})

	assertCommandNames(t, result.Commands, []string{
		"start_catchup",
		"publish_projection",
	})
	if result.Projection.Sync.Action != SyncActionCatchUp {
		t.Fatalf("sync_action=%s", result.Projection.Sync.Action)
	}
	if result.Projection.Recovery.Phase != RecoveryCatchingUp {
		t.Fatalf("recovery_phase=%s", result.Projection.Recovery.Phase)
	}
}

func TestPhase14_SyncAckObserved_PrimaryTailFactsDriveRebuild(t *testing.T) {
	core := NewCoreEngine()

	core.ApplyEvent(AssignmentDelivered{
		ID:             "vol-sync-facts-rebuild",
		Epoch:          1,
		Role:           RolePrimary,
		RecoveryTarget: SessionCatchUp,
		Replicas: []ReplicaAssignment{
			{ReplicaID: "replica-1", Endpoint: Endpoint{DataAddr: "10.0.0.60:9333", CtrlAddr: "10.0.0.60:9334", Version: 1}},
		},
	})

	result := core.ApplyEvent(SyncAckObserved{
		ID:             "vol-sync-facts-rebuild",
		ReplicaID:      "replica-1",
		AckKind:        SyncAckTimedOut,
		TargetLSN:      1000,
		PrimaryTailLSN: 700,
		DurableLSN:     500,
		AppliedLSN:     600,
		Reason:         "gap_beyond_retention",
	})

	assertCommandNames(t, result.Commands, []string{
		"invalidate_session",
		"publish_projection",
	})
	if result.Projection.Sync.Action != SyncActionRebuild {
		t.Fatalf("sync_action=%s", result.Projection.Sync.Action)
	}
	if result.Projection.Mode.Name != ModeNeedsRebuild {
		t.Fatalf("mode=%s", result.Projection.Mode.Name)
	}
}

func TestPhase14_SessionStarted_CatchUpUsesUnifiedPath(t *testing.T) {
	core := NewCoreEngine()

	core.ApplyEvent(AssignmentDelivered{
		ID:             "vol-session-catchup",
		Epoch:          1,
		Role:           RolePrimary,
		RecoveryTarget: SessionCatchUp,
		Replicas: []ReplicaAssignment{
			{ReplicaID: "replica-1", Endpoint: Endpoint{DataAddr: "10.0.0.63:9333", CtrlAddr: "10.0.0.63:9334", Version: 1}},
		},
	})

	result := core.ApplyEvent(SessionStarted{
		ID:        "vol-session-catchup",
		ReplicaID: "replica-1",
		Kind:      SessionCatchUp,
		TargetLSN: 77,
	})

	assertCommandNames(t, result.Commands, []string{
		"start_catchup",
		"publish_projection",
	})
	if result.Projection.Recovery.Phase != RecoveryCatchingUp {
		t.Fatalf("recovery_phase=%s", result.Projection.Recovery.Phase)
	}
	if result.Projection.Recovery.TargetLSN != 77 {
		t.Fatalf("target_lsn=%d", result.Projection.Recovery.TargetLSN)
	}
}

func TestPhase14_SessionCompleted_RebuildClearsNeedsRebuild(t *testing.T) {
	core := NewCoreEngine()

	core.ApplyEvent(AssignmentDelivered{
		ID:             "vol-session-rebuild",
		Epoch:          1,
		Role:           RolePrimary,
		RecoveryTarget: SessionRebuild,
		Replicas: []ReplicaAssignment{
			{ReplicaID: "replica-1", Endpoint: Endpoint{DataAddr: "10.0.0.64:9333", CtrlAddr: "10.0.0.64:9334", Version: 1}},
		},
	})
	core.ApplyEvent(NeedsRebuildObserved{
		ID:        "vol-session-rebuild",
		ReplicaID: "replica-1",
		Reason:    "gap_too_large",
	})
	core.ApplyEvent(SessionStarted{
		ID:        "vol-session-rebuild",
		ReplicaID: "replica-1",
		Kind:      SessionRebuild,
		TargetLSN: 120,
	})

	result := core.ApplyEvent(SessionCompleted{
		ID:            "vol-session-rebuild",
		ReplicaID:     "replica-1",
		Kind:          SessionRebuild,
		AchievedLSN:   120,
		FlushedLSN:    120,
		CheckpointLSN: 120,
	})

	if result.Projection.Recovery.Phase != RecoveryIdle {
		t.Fatalf("recovery_phase=%s", result.Projection.Recovery.Phase)
	}
	if result.Projection.Mode.Name == ModeNeedsRebuild {
		t.Fatalf("mode=%s", result.Projection.Mode.Name)
	}
	if result.Projection.Boundary.DurableLSN != 120 {
		t.Fatalf("durable_lsn=%d", result.Projection.Boundary.DurableLSN)
	}
}

func TestPhase14_SessionFailed_CatchUpFallsBackToDegraded(t *testing.T) {
	core := NewCoreEngine()

	core.ApplyEvent(AssignmentDelivered{
		ID:             "vol-session-failed",
		Epoch:          1,
		Role:           RolePrimary,
		RecoveryTarget: SessionCatchUp,
		Replicas: []ReplicaAssignment{
			{ReplicaID: "replica-1", Endpoint: Endpoint{DataAddr: "10.0.0.65:9333", CtrlAddr: "10.0.0.65:9334", Version: 1}},
		},
	})
	core.ApplyEvent(SessionStarted{
		ID:        "vol-session-failed",
		ReplicaID: "replica-1",
		Kind:      SessionCatchUp,
		TargetLSN: 80,
	})

	result := core.ApplyEvent(SessionFailed{
		ID:        "vol-session-failed",
		ReplicaID: "replica-1",
		Kind:      SessionCatchUp,
		Reason:    "transport_lost",
	})

	assertCommandNames(t, result.Commands, []string{
		"invalidate_session",
		"publish_projection",
	})
	if result.Projection.Mode.Name != ModeDegraded {
		t.Fatalf("mode=%s", result.Projection.Mode.Name)
	}
	if result.Projection.Boundary.LastBarrierReason != "transport_lost" {
		t.Fatalf("last_barrier_reason=%q", result.Projection.Boundary.LastBarrierReason)
	}
}

func TestPhase14_SessionProgressObserved_KindMismatchIgnored(t *testing.T) {
	core := NewCoreEngine()

	core.ApplyEvent(AssignmentDelivered{
		ID:             "vol-session-progress-mismatch",
		Epoch:          1,
		Role:           RolePrimary,
		RecoveryTarget: SessionCatchUp,
		Replicas: []ReplicaAssignment{
			{ReplicaID: "replica-1", Endpoint: Endpoint{DataAddr: "10.0.0.66:9333", CtrlAddr: "10.0.0.66:9334", Version: 1}},
		},
	})
	core.ApplyEvent(SessionStarted{
		ID:        "vol-session-progress-mismatch",
		ReplicaID: "replica-1",
		Kind:      SessionCatchUp,
		TargetLSN: 80,
	})
	core.ApplyEvent(SessionProgressObserved{
		ID:          "vol-session-progress-mismatch",
		ReplicaID:   "replica-1",
		Kind:        SessionCatchUp,
		AchievedLSN: 20,
	})

	result := core.ApplyEvent(SessionProgressObserved{
		ID:          "vol-session-progress-mismatch",
		ReplicaID:   "replica-1",
		Kind:        SessionRebuild,
		AchievedLSN: 70,
	})

	assertCommandNames(t, result.Commands, nil)
	if result.Projection.Recovery.Phase != RecoveryCatchingUp {
		t.Fatalf("recovery_phase=%s", result.Projection.Recovery.Phase)
	}
	if result.Projection.Recovery.AchievedLSN != 20 {
		t.Fatalf("achieved_lsn=%d, want 20", result.Projection.Recovery.AchievedLSN)
	}
	if result.Projection.Boundary.AchievedLSN != 20 {
		t.Fatalf("boundary_achieved=%d, want 20", result.Projection.Boundary.AchievedLSN)
	}
}

func TestPhase14_SessionFailed_KindMismatchIgnored(t *testing.T) {
	core := NewCoreEngine()

	core.ApplyEvent(AssignmentDelivered{
		ID:             "vol-session-failed-mismatch",
		Epoch:          1,
		Role:           RolePrimary,
		RecoveryTarget: SessionCatchUp,
		Replicas: []ReplicaAssignment{
			{ReplicaID: "replica-1", Endpoint: Endpoint{DataAddr: "10.0.0.67:9333", CtrlAddr: "10.0.0.67:9334", Version: 1}},
		},
	})
	core.ApplyEvent(SessionStarted{
		ID:        "vol-session-failed-mismatch",
		ReplicaID: "replica-1",
		Kind:      SessionCatchUp,
		TargetLSN: 90,
	})

	result := core.ApplyEvent(SessionFailed{
		ID:        "vol-session-failed-mismatch",
		ReplicaID: "replica-1",
		Kind:      SessionRebuild,
		Reason:    "stale_rebuild_failure",
	})

	assertCommandNames(t, result.Commands, nil)
	if result.Projection.Recovery.Phase != RecoveryCatchingUp {
		t.Fatalf("recovery_phase=%s", result.Projection.Recovery.Phase)
	}
	if result.Projection.Mode.Name != ModeBootstrapPending {
		t.Fatalf("mode=%s", result.Projection.Mode.Name)
	}
	if result.Projection.Boundary.LastBarrierReason != "" {
		t.Fatalf("last_barrier_reason=%q", result.Projection.Boundary.LastBarrierReason)
	}
}
