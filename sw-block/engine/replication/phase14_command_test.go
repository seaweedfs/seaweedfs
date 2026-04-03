package replication

import (
	"reflect"
	"testing"
)

func TestPhase14_CommandSequence_PrimaryAssignmentIsBounded(t *testing.T) {
	core := NewCoreEngine()
	ev := AssignmentDelivered{
		ID:    "vol-cmd-primary",
		Epoch: 1,
		Role:  RolePrimary,
		Replicas: []ReplicaAssignment{
			{ReplicaID: "replica-1", Endpoint: Endpoint{DataAddr: "10.0.0.10:9333", CtrlAddr: "10.0.0.10:9334", Version: 1}},
		},
	}

	result := core.ApplyEvent(ev)
	assertCommandNames(t, result.Commands, []string{
		"apply_role",
		"configure_shipper",
		"publish_projection",
	})

	result = core.ApplyEvent(ev)
	assertCommandNames(t, result.Commands, []string{
		"publish_projection",
	})
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
	assertCommandNames(t, result.Commands, []string{
		"publish_projection",
	})
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
	assertCommandNames(t, result.Commands, []string{
		"publish_projection",
	})
}

func assertCommandNames(t *testing.T, cmds []Command, want []string) {
	t.Helper()
	got := make([]string, 0, len(cmds))
	for _, cmd := range cmds {
		got = append(got, cmd.commandName())
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("commands=%v, want %v", got, want)
	}
}
