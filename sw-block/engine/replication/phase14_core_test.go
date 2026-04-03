package replication

import "testing"

func TestPhase14_Identity_StableReplicaIDPreservesOwnership(t *testing.T) {
	r := NewRegistry()
	r.ApplyAssignment(AssignmentIntent{
		Replicas: []ReplicaAssignment{
			{
				ReplicaID: "replica-1",
				Endpoint: Endpoint{
					DataAddr: "10.0.0.1:9333",
					CtrlAddr: "10.0.0.1:9334",
					Version:  1,
				},
			},
		},
		Epoch:           1,
		RecoveryTargets: map[string]SessionKind{"replica-1": SessionCatchUp},
	})

	sender := r.Sender("replica-1")
	oldSessionID := sender.SessionID()
	if err := sender.BeginConnect(oldSessionID); err != nil {
		t.Fatalf("begin connect: %v", err)
	}

	r.Reconcile([]ReplicaAssignment{
		{
			ReplicaID: "replica-1",
			Endpoint: Endpoint{
				DataAddr: "10.0.0.2:9333",
				CtrlAddr: "10.0.0.2:9334",
				Version:  2,
			},
		},
	}, 1)

	if got := r.Sender("replica-1"); got != sender {
		t.Fatal("stable ReplicaID should preserve sender ownership")
	}
	if sender.HasActiveSession() {
		t.Fatal("endpoint change should invalidate active ownership session")
	}
	if sender.Endpoint().DataAddr != "10.0.0.2:9333" {
		t.Fatalf("endpoint not updated: %s", sender.Endpoint().DataAddr)
	}
}

func TestPhase14_CorePublishHealthyRequiresBarrierDurability(t *testing.T) {
	core := NewCoreEngine()

	result := core.ApplyEvent(AssignmentDelivered{
		ID:    "vol-a",
		Epoch: 7,
		Role:  RolePrimary,
		Replicas: []ReplicaAssignment{
			{ReplicaID: "replica-1", Endpoint: Endpoint{DataAddr: "10.0.0.2:9333", CtrlAddr: "10.0.0.2:9334", Version: 1}},
		},
	})
	if result.Projection.Mode.Name != ModeBootstrapPending {
		t.Fatalf("mode=%s", result.Projection.Mode.Name)
	}
	if result.Projection.Publication.Reason != "awaiting_role_apply" {
		t.Fatalf("publication_reason=%q", result.Projection.Publication.Reason)
	}
	if result.Projection.PublishHealthy {
		t.Fatal("fresh assignment must not publish healthy")
	}

	result = core.ApplyEvent(RoleApplied{ID: "vol-a"})
	if result.Projection.Publication.Reason != "awaiting_shipper_configured" {
		t.Fatalf("publication_reason=%q", result.Projection.Publication.Reason)
	}
	result = core.ApplyEvent(ShipperConfiguredObserved{ID: "vol-a"})
	if result.Projection.Publication.Reason != "awaiting_shipper_connected" {
		t.Fatalf("publication_reason=%q", result.Projection.Publication.Reason)
	}
	result = core.ApplyEvent(ShipperConnectedObserved{ID: "vol-a"})
	if result.Projection.PublishHealthy {
		t.Fatal("connected shipper without barrier durability must stay non-healthy")
	}
	if result.Projection.Mode.Name != ModeBootstrapPending {
		t.Fatalf("mode=%s", result.Projection.Mode.Name)
	}
	if result.Projection.Publication.Reason != "awaiting_barrier_durability" {
		t.Fatalf("publication_reason=%q", result.Projection.Publication.Reason)
	}

	result = core.ApplyEvent(BarrierAccepted{ID: "vol-a", FlushedLSN: 12})
	if !result.Projection.PublishHealthy {
		t.Fatal("barrier durability should enable publish healthy on eligible primary path")
	}
	if result.Projection.Mode.Name != ModePublishHealthy {
		t.Fatalf("mode=%s", result.Projection.Mode.Name)
	}
	if result.Projection.Boundary.DurableLSN != 12 {
		t.Fatalf("durable_lsn=%d", result.Projection.Boundary.DurableLSN)
	}
	if result.Projection.Publication.Reason != "" {
		t.Fatalf("publication_reason=%q", result.Projection.Publication.Reason)
	}
}

func TestPhase14_CoreDiagnosticShippedDoesNotCreateDurableTruth(t *testing.T) {
	core := NewCoreEngine()

	core.ApplyEvent(AssignmentDelivered{
		ID:       "vol-b",
		Epoch:    3,
		Role:     RolePrimary,
		Replicas: []ReplicaAssignment{{ReplicaID: "replica-1", Endpoint: Endpoint{DataAddr: "10.0.0.3:9333", Version: 1}}},
	})
	core.ApplyEvent(RoleApplied{ID: "vol-b"})
	core.ApplyEvent(ShipperConfiguredObserved{ID: "vol-b"})
	core.ApplyEvent(ShipperConnectedObserved{ID: "vol-b"})

	result := core.ApplyEvent(DiagnosticShippedAdvanced{ID: "vol-b", ShippedLSN: 99})
	if result.Projection.Boundary.DurableLSN != 0 {
		t.Fatalf("durable_lsn=%d, want 0", result.Projection.Boundary.DurableLSN)
	}
	if result.Projection.Boundary.DiagnosticShippedLSN != 99 {
		t.Fatalf("diagnostic_shipped=%d", result.Projection.Boundary.DiagnosticShippedLSN)
	}
	if result.Projection.PublishHealthy {
		t.Fatal("diagnostic shipped progress must not establish durable healthy publication")
	}
	if result.Projection.Mode.Name != ModeBootstrapPending {
		t.Fatalf("mode=%s", result.Projection.Mode.Name)
	}
}

func TestPhase14_CoreFailClosedModesStayDistinct(t *testing.T) {
	core := NewCoreEngine()

	core.ApplyEvent(AssignmentDelivered{
		ID:       "vol-c",
		Epoch:    1,
		Role:     RolePrimary,
		Replicas: []ReplicaAssignment{{ReplicaID: "replica-1", Endpoint: Endpoint{DataAddr: "10.0.0.4:9333", Version: 1}}},
	})
	core.ApplyEvent(RoleApplied{ID: "vol-c"})
	core.ApplyEvent(ShipperConfiguredObserved{ID: "vol-c"})
	core.ApplyEvent(ShipperConnectedObserved{ID: "vol-c"})
	core.ApplyEvent(BarrierAccepted{ID: "vol-c", FlushedLSN: 8})

	result := core.ApplyEvent(BarrierRejected{ID: "vol-c", Reason: "shipper_timeout"})
	if result.Projection.Mode.Name != ModeDegraded {
		t.Fatalf("mode=%s", result.Projection.Mode.Name)
	}
	if result.Projection.PublishHealthy {
		t.Fatal("degraded mode must fail closed")
	}

	result = core.ApplyEvent(NeedsRebuildObserved{ID: "vol-c", Reason: "gap_too_large"})
	if result.Projection.Mode.Name != ModeNeedsRebuild {
		t.Fatalf("mode=%s", result.Projection.Mode.Name)
	}
	if result.Projection.PublishHealthy {
		t.Fatal("needs_rebuild must fail closed")
	}
	if result.Projection.Mode.Reason != "gap_too_large" {
		t.Fatalf("reason=%q", result.Projection.Mode.Reason)
	}
}

func TestPhase14_CoreProjectionMarksConstrainedRuntimeAuthority(t *testing.T) {
	core := NewCoreEngine()

	result := core.ApplyEvent(AssignmentDelivered{
		ID:    "vol-d",
		Epoch: 9,
		Role:  RoleReplica,
		Replicas: []ReplicaAssignment{
			{ReplicaID: "replica-1", Endpoint: Endpoint{DataAddr: "10.0.0.5:9333", Version: 1}},
		},
	})

	if result.Projection.Mode.Authority != RuntimeAuthorityConstrainedV1 {
		t.Fatalf("authority=%s", result.Projection.Mode.Authority)
	}
	if result.Projection.Mode.Name != ModeBootstrapPending {
		t.Fatalf("mode=%s", result.Projection.Mode.Name)
	}

	result = core.ApplyEvent(RoleApplied{ID: "vol-d"})
	result = core.ApplyEvent(ReceiverReadyObserved{ID: "vol-d"})
	if result.Projection.Mode.Name != ModeReplicaReady {
		t.Fatalf("mode=%s", result.Projection.Mode.Name)
	}
	if result.Projection.PublishHealthy {
		t.Fatal("replica-ready is not the same as publish-healthy")
	}
	if result.Projection.Publication.Reason != "replica_not_primary" {
		t.Fatalf("publication_reason=%q", result.Projection.Publication.Reason)
	}
}

func TestPhase14_CoreAllocatedOnlyWithoutReplicas(t *testing.T) {
	core := NewCoreEngine()

	result := core.ApplyEvent(AssignmentDelivered{
		ID:    "vol-rf1",
		Epoch: 1,
		Role:  RolePrimary,
	})

	if result.Projection.Mode.Name != ModeAllocatedOnly {
		t.Fatalf("mode=%s", result.Projection.Mode.Name)
	}
	if result.Projection.PublishHealthy {
		t.Fatal("allocated-only volume must not publish healthy")
	}
	if result.Projection.Publication.Reason != "allocated_only" {
		t.Fatalf("publication_reason=%q", result.Projection.Publication.Reason)
	}
}
