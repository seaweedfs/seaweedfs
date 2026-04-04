package replication

import (
	"reflect"
	"testing"
)

func TestPhase14_CommandSequence_PrimaryAssignmentIsBounded(t *testing.T) {
	core := NewCoreEngine()
	ev := AssignmentDelivered{
		ID:             "vol-cmd-primary",
		Epoch:          1,
		Role:           RolePrimary,
		RecoveryTarget: SessionCatchUp,
		Replicas: []ReplicaAssignment{
			{ReplicaID: "replica-1", Endpoint: Endpoint{DataAddr: "10.0.0.10:9333", CtrlAddr: "10.0.0.10:9334", Version: 1}},
		},
	}

	result := core.ApplyEvent(ev)
	assertCommandNames(t, result.Commands, []string{
		"apply_role",
		"configure_shipper",
		"start_recovery_task",
		"publish_projection",
	})

	result = core.ApplyEvent(ev)
	assertCommandNames(t, result.Commands, nil)
}

func TestPhase14_CommandSequence_ReplicaAssignmentIsBounded(t *testing.T) {
	core := NewCoreEngine()
	ev := AssignmentDelivered{
		ID:    "vol-cmd-replica",
		Epoch: 3,
		Role:  RoleReplica,
		Replicas: []ReplicaAssignment{
			{ReplicaID: "replica-1", Endpoint: Endpoint{DataAddr: "10.0.0.11:9333", CtrlAddr: "10.0.0.11:9334", Version: 1}},
		},
	}

	result := core.ApplyEvent(ev)
	assertCommandNames(t, result.Commands, []string{
		"apply_role",
		"start_receiver",
		"publish_projection",
	})

	result = core.ApplyEvent(ev)
	assertCommandNames(t, result.Commands, nil)
}

func TestPhase14_CommandSequence_AssignmentChangeReissuesNeededCommand(t *testing.T) {
	core := NewCoreEngine()

	initial := AssignmentDelivered{
		ID:    "vol-cmd-change",
		Epoch: 5,
		Role:  RolePrimary,
		Replicas: []ReplicaAssignment{
			{ReplicaID: "replica-1", Endpoint: Endpoint{DataAddr: "10.0.0.12:9333", CtrlAddr: "10.0.0.12:9334", Version: 1}},
		},
	}
	core.ApplyEvent(initial)
	core.ApplyEvent(RoleApplied{ID: "vol-cmd-change"})
	core.ApplyEvent(ShipperConfiguredObserved{ID: "vol-cmd-change"})
	core.ApplyEvent(ShipperConnectedObserved{ID: "vol-cmd-change"})

	changed := AssignmentDelivered{
		ID:    "vol-cmd-change",
		Epoch: 5,
		Role:  RolePrimary,
		Replicas: []ReplicaAssignment{
			{ReplicaID: "replica-1", Endpoint: Endpoint{DataAddr: "10.0.0.13:9333", CtrlAddr: "10.0.0.13:9334", Version: 2}},
		},
	}
	result := core.ApplyEvent(changed)
	assertCommandNames(t, result.Commands, []string{
		"configure_shipper",
		"publish_projection",
	})
}

func TestPhase14_CommandSequence_InvalidateOnlyOnNewFailureTransition(t *testing.T) {
	core := NewCoreEngine()

	core.ApplyEvent(AssignmentDelivered{
		ID:    "vol-cmd-failure",
		Epoch: 1,
		Role:  RolePrimary,
		Replicas: []ReplicaAssignment{
			{ReplicaID: "replica-1", Endpoint: Endpoint{DataAddr: "10.0.0.14:9333", CtrlAddr: "10.0.0.14:9334", Version: 1}},
		},
	})
	core.ApplyEvent(RoleApplied{ID: "vol-cmd-failure"})
	core.ApplyEvent(ShipperConfiguredObserved{ID: "vol-cmd-failure"})
	core.ApplyEvent(ShipperConnectedObserved{ID: "vol-cmd-failure"})
	core.ApplyEvent(BarrierAccepted{ID: "vol-cmd-failure", FlushedLSN: 9})

	result := core.ApplyEvent(BarrierRejected{ID: "vol-cmd-failure", Reason: "timeout"})
	assertCommandNames(t, result.Commands, []string{
		"invalidate_session",
		"publish_projection",
	})

	result = core.ApplyEvent(BarrierRejected{ID: "vol-cmd-failure", Reason: "timeout"})
	assertCommandNames(t, result.Commands, nil)
}

func TestPhase14_CommandSequence_PublishOnlyWhenProjectionChanges(t *testing.T) {
	core := NewCoreEngine()

	core.ApplyEvent(AssignmentDelivered{
		ID:    "vol-cmd-publish",
		Epoch: 2,
		Role:  RolePrimary,
		Replicas: []ReplicaAssignment{
			{ReplicaID: "replica-1", Endpoint: Endpoint{DataAddr: "10.0.0.15:9333", CtrlAddr: "10.0.0.15:9334", Version: 1}},
		},
	})

	result := core.ApplyEvent(RoleApplied{ID: "vol-cmd-publish"})
	assertCommandNames(t, result.Commands, []string{
		"publish_projection",
	})

	result = core.ApplyEvent(RoleApplied{ID: "vol-cmd-publish"})
	assertCommandNames(t, result.Commands, nil)
}

func TestPhase14_CommandSequence_CatchUpStartIsBounded(t *testing.T) {
	core := NewCoreEngine()

	core.ApplyEvent(AssignmentDelivered{
		ID:             "vol-cmd-catchup",
		Epoch:          6,
		Role:           RolePrimary,
		RecoveryTarget: SessionCatchUp,
		Replicas: []ReplicaAssignment{
			{ReplicaID: "replica-1", Endpoint: Endpoint{DataAddr: "10.0.0.16:9333", CtrlAddr: "10.0.0.16:9334", Version: 1}},
		},
	})
	core.ApplyEvent(RoleApplied{ID: "vol-cmd-catchup"})
	core.ApplyEvent(ShipperConfiguredObserved{ID: "vol-cmd-catchup"})
	core.ApplyEvent(ShipperConnectedObserved{ID: "vol-cmd-catchup"})

	result := core.ApplyEvent(CatchUpPlanned{ID: "vol-cmd-catchup", ReplicaID: "replica-1", TargetLSN: 55})
	assertCommandNames(t, result.Commands, []string{
		"start_catchup",
		"publish_projection",
	})
	start, ok := result.Commands[0].(StartCatchUpCommand)
	if !ok {
		t.Fatalf("cmd0=%T", result.Commands[0])
	}
	if start.ReplicaID != "replica-1" {
		t.Fatalf("replica_id=%q", start.ReplicaID)
	}

	result = core.ApplyEvent(CatchUpPlanned{ID: "vol-cmd-catchup", TargetLSN: 55})
	assertCommandNames(t, result.Commands, nil)
}

func TestPhase14_CommandSequence_RebuildStartIsBounded(t *testing.T) {
	core := NewCoreEngine()

	core.ApplyEvent(AssignmentDelivered{
		ID:             "vol-cmd-rebuild",
		Epoch:          7,
		Role:           RoleReplica,
		RecoveryTarget: SessionRebuild,
		Replicas: []ReplicaAssignment{
			{ReplicaID: "replica-1", Endpoint: Endpoint{DataAddr: "10.0.0.17:9333", CtrlAddr: "10.0.0.17:9334", Version: 1}},
		},
	})
	core.ApplyEvent(NeedsRebuildObserved{ID: "vol-cmd-rebuild", Reason: "gap_too_large"})

	result := core.ApplyEvent(RebuildStarted{ID: "vol-cmd-rebuild", ReplicaID: "replica-1", TargetLSN: 80})
	assertCommandNames(t, result.Commands, []string{
		"start_rebuild",
		"publish_projection",
	})
	start, ok := result.Commands[0].(StartRebuildCommand)
	if !ok {
		t.Fatalf("cmd0=%T", result.Commands[0])
	}
	if start.ReplicaID != "replica-1" {
		t.Fatalf("replica_id=%q", start.ReplicaID)
	}

	result = core.ApplyEvent(RebuildStarted{ID: "vol-cmd-rebuild", TargetLSN: 80})
	assertCommandNames(t, result.Commands, nil)
}

func TestPhase14_CommandSequence_RebuildingAssignmentStartsRecoveryTaskWithoutReceiver(t *testing.T) {
	core := NewCoreEngine()

	result := core.ApplyEvent(AssignmentDelivered{
		ID:             "vol-cmd-rebuild-assign",
		Epoch:          8,
		Role:           RoleReplica,
		RecoveryTarget: SessionRebuild,
		Replicas: []ReplicaAssignment{
			{ReplicaID: "replica-1", Endpoint: Endpoint{DataAddr: "10.0.0.20:9333", CtrlAddr: "10.0.0.20:9334", Version: 1}},
		},
	})
	assertCommandNames(t, result.Commands, []string{
		"apply_role",
		"start_recovery_task",
		"publish_projection",
	})
}

func TestPhase14_CommandSequence_AssignmentChangeAllowsFreshRecoveryStart(t *testing.T) {
	core := NewCoreEngine()

	core.ApplyEvent(AssignmentDelivered{
		ID:             "vol-cmd-reassign",
		Epoch:          1,
		Role:           RolePrimary,
		RecoveryTarget: SessionCatchUp,
		Replicas: []ReplicaAssignment{
			{ReplicaID: "replica-1", Endpoint: Endpoint{DataAddr: "10.0.0.18:9333", CtrlAddr: "10.0.0.18:9334", Version: 1}},
		},
	})
	core.ApplyEvent(RoleApplied{ID: "vol-cmd-reassign"})
	core.ApplyEvent(ShipperConfiguredObserved{ID: "vol-cmd-reassign"})
	core.ApplyEvent(ShipperConnectedObserved{ID: "vol-cmd-reassign"})

	result := core.ApplyEvent(CatchUpPlanned{ID: "vol-cmd-reassign", TargetLSN: 90})
	assertCommandNames(t, result.Commands, []string{
		"start_catchup",
		"publish_projection",
	})

	result = core.ApplyEvent(AssignmentDelivered{
		ID:             "vol-cmd-reassign",
		Epoch:          2,
		Role:           RolePrimary,
		RecoveryTarget: SessionCatchUp,
		Replicas: []ReplicaAssignment{
			{ReplicaID: "replica-1", Endpoint: Endpoint{DataAddr: "10.0.0.19:9333", CtrlAddr: "10.0.0.19:9334", Version: 2}},
		},
	})
	assertCommandNames(t, result.Commands, []string{
		"apply_role",
		"configure_shipper",
		"start_recovery_task",
		"publish_projection",
	})

	result = core.ApplyEvent(CatchUpPlanned{ID: "vol-cmd-reassign", TargetLSN: 90})
	assertCommandNames(t, result.Commands, []string{
		"start_catchup",
		"publish_projection",
	})
}

func assertCommandNames(t *testing.T, cmds []Command, want []string) {
	t.Helper()
	got := make([]string, 0, len(cmds))
	for _, cmd := range cmds {
		got = append(got, cmd.commandName())
	}
	if want == nil {
		want = []string{}
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("commands=%v, want %v", got, want)
	}
}
