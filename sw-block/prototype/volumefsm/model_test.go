package volumefsm

import (
	"strings"
	"testing"

	fsmv2 "github.com/seaweedfs/seaweedfs/sw-block/prototype/fsmv2"
)

type scriptedPlanner struct {
	decision RecoveryDecision
}

func (s scriptedPlanner) PlanReconnect(replicaID string, flushedLSN, targetLSN uint64) RecoveryDecision {
	return s.decision
}

func mustApply(t *testing.T, m *Model, evt Event) {
	t.Helper()
	if err := m.Apply(evt); err != nil {
		t.Fatalf("apply %s: %v", evt.Kind, err)
	}
}

func TestModelSyncAllBlocksOnLaggingReplica(t *testing.T) {
	m := New("p1", ModeSyncAll, 1, "r1", "r2")
	mustApply(t, m, Event{Kind: EventBootstrapReplica, ReplicaID: "r1", ReplicaFlushedLSN: 1})
	mustApply(t, m, Event{Kind: EventBootstrapReplica, ReplicaID: "r2", ReplicaFlushedLSN: 1})
	if !m.CanServeWrite() {
		t.Fatal("sync_all should serve when all replicas are in sync")
	}
	mustApply(t, m, Event{Kind: EventReplicaDisconnect, ReplicaID: "r2"})
	if m.CanServeWrite() {
		t.Fatal("sync_all should block when one required replica lags")
	}
}

func TestModelSyncQuorumSurvivesOneLaggingReplica(t *testing.T) {
	m := New("p1", ModeSyncQuorum, 1, "r1", "r2")
	mustApply(t, m, Event{Kind: EventBootstrapReplica, ReplicaID: "r1", ReplicaFlushedLSN: 1})
	mustApply(t, m, Event{Kind: EventBootstrapReplica, ReplicaID: "r2", ReplicaFlushedLSN: 1})
	mustApply(t, m, Event{Kind: EventReplicaDisconnect, ReplicaID: "r2"})
	if !m.CanServeWrite() {
		t.Fatal("sync_quorum should still serve with primary + one in-sync replica")
	}
}

func TestModelCatchupFlowRestoresEligibility(t *testing.T) {
	m := New("p1", ModeSyncAll, 1, "r1")
	mustApply(t, m, Event{Kind: EventBootstrapReplica, ReplicaID: "r1", ReplicaFlushedLSN: 1})
	mustApply(t, m, Event{Kind: EventReplicaDisconnect, ReplicaID: "r1"})
	mustApply(t, m, Event{Kind: EventWriteCommitted, LSN: 10})
	mustApply(t, m, Event{Kind: EventReplicaReconnect, ReplicaID: "r1", ReplicaFlushedLSN: 1, TargetLSN: 10, ReservationID: "res-1", ReservationTTL: 100})
	if got := m.Replica("r1").FSM.State; got != fsmv2.StateCatchingUp {
		t.Fatalf("expected catching up, got %s", got)
	}
	mustApply(t, m, Event{Kind: EventReplicaCatchupProgress, ReplicaID: "r1", ReplicaFlushedLSN: 10, HoldUntil: 20})
	mustApply(t, m, Event{Kind: EventReplicaPromotionHealthy, ReplicaID: "r1", Now: 20})
	if got := m.Replica("r1").FSM.State; got != fsmv2.StateInSync {
		t.Fatalf("expected in sync, got %s", got)
	}
	if !m.CanServeWrite() {
		t.Fatal("sync_all should serve after replica returns to in-sync")
	}
}

func TestModelLongGapRebuildFlow(t *testing.T) {
	m := New("p1", ModeBestEffort, 1, "r1")
	mustApply(t, m, Event{Kind: EventReplicaDisconnect, ReplicaID: "r1"})
	mustApply(t, m, Event{Kind: EventReplicaNeedsRebuild, ReplicaID: "r1"})
	mustApply(t, m, Event{Kind: EventReplicaStartRebuild, ReplicaID: "r1", SnapshotID: "snap1", SnapshotCpLSN: 100, ReservationID: "rebuild-1", ReservationTTL: 200})
	mustApply(t, m, Event{Kind: EventReplicaRebuildBaseApplied, ReplicaID: "r1", TargetLSN: 130})
	mustApply(t, m, Event{Kind: EventReplicaCatchupProgress, ReplicaID: "r1", ReplicaFlushedLSN: 130, HoldUntil: 150})
	mustApply(t, m, Event{Kind: EventReplicaPromotionHealthy, ReplicaID: "r1", Now: 150})
	if got := m.Replica("r1").FSM.State; got != fsmv2.StateInSync {
		t.Fatalf("expected in sync after rebuild, got %s", got)
	}
}

func TestModelPrimaryLeaseLostFencesRecovery(t *testing.T) {
	m := New("p1", ModeSyncAll, 1, "r1")
	mustApply(t, m, Event{Kind: EventBootstrapReplica, ReplicaID: "r1", ReplicaFlushedLSN: 1})
	mustApply(t, m, Event{Kind: EventReplicaDisconnect, ReplicaID: "r1"})
	mustApply(t, m, Event{Kind: EventReplicaReconnect, ReplicaID: "r1", ReplicaFlushedLSN: 1, TargetLSN: 5, ReservationID: "res-2", ReservationTTL: 100})
	mustApply(t, m, Event{Kind: EventPrimaryLeaseLost})
	if m.PrimaryState != PrimaryLost {
		t.Fatalf("expected lost primary, got %s", m.PrimaryState)
	}
	if got := m.Replica("r1").FSM.State; got != fsmv2.StateLagging {
		t.Fatalf("expected lagging after fencing, got %s", got)
	}
}

func TestModelPromoteReplicaChangesEpoch(t *testing.T) {
	m := New("p1", ModeSyncQuorum, 1, "r1", "r2")
	mustApply(t, m, Event{Kind: EventBootstrapReplica, ReplicaID: "r1", ReplicaFlushedLSN: 10})
	oldEpoch := m.Epoch
	mustApply(t, m, Event{Kind: EventPromoteReplica, ReplicaID: "r1"})
	if m.PrimaryID != "r1" {
		t.Fatalf("expected promoted primary r1, got %s", m.PrimaryID)
	}
	if m.Epoch != oldEpoch+1 {
		t.Fatalf("expected epoch increment, got %d want %d", m.Epoch, oldEpoch+1)
	}
}

func TestModelSyncQuorumWithThreeReplicasMixedStates(t *testing.T) {
	m := New("p1", ModeSyncQuorum, 1, "r1", "r2", "r3")
	mustApply(t, m, Event{Kind: EventBootstrapReplica, ReplicaID: "r1", ReplicaFlushedLSN: 1})
	mustApply(t, m, Event{Kind: EventBootstrapReplica, ReplicaID: "r2", ReplicaFlushedLSN: 1})
	mustApply(t, m, Event{Kind: EventBootstrapReplica, ReplicaID: "r3", ReplicaFlushedLSN: 1})

	mustApply(t, m, Event{Kind: EventReplicaDisconnect, ReplicaID: "r2"})

	if !m.CanServeWrite() {
		t.Fatal("sync_quorum should serve with primary + two in-sync replicas out of RF=4")
	}
}

func TestModelFailoverFencesMixedReplicaStates(t *testing.T) {
	m := New("p1", ModeSyncQuorum, 10, "r1", "r2", "r3")
	mustApply(t, m, Event{Kind: EventBootstrapReplica, ReplicaID: "r1", ReplicaFlushedLSN: 8})
	mustApply(t, m, Event{Kind: EventBootstrapReplica, ReplicaID: "r2", ReplicaFlushedLSN: 8})
	mustApply(t, m, Event{Kind: EventBootstrapReplica, ReplicaID: "r3", ReplicaFlushedLSN: 6})
	mustApply(t, m, Event{Kind: EventReplicaDisconnect, ReplicaID: "r2"})
	mustApply(t, m, Event{Kind: EventReplicaReconnect, ReplicaID: "r2", ReplicaFlushedLSN: 8, TargetLSN: 12, ReservationID: "catch-r2", ReservationTTL: 100})
	mustApply(t, m, Event{Kind: EventReplicaDisconnect, ReplicaID: "r3"})
	mustApply(t, m, Event{Kind: EventReplicaNeedsRebuild, ReplicaID: "r3"})
	mustApply(t, m, Event{Kind: EventReplicaStartRebuild, ReplicaID: "r3", SnapshotID: "snap-x", SnapshotCpLSN: 6, ReservationID: "rebuild-r3", ReservationTTL: 200})

	mustApply(t, m, Event{Kind: EventPrimaryLeaseLost})
	mustApply(t, m, Event{Kind: EventPromoteReplica, ReplicaID: "r1"})

	if m.PrimaryID != "r1" {
		t.Fatalf("expected r1 promoted, got %s", m.PrimaryID)
	}
	if got := m.Replica("r2").FSM.State; got != fsmv2.StateLagging {
		t.Fatalf("expected r2 fenced back to lagging, got %s", got)
	}
	if got := m.Replica("r3").FSM.State; got != fsmv2.StateLagging {
		t.Fatalf("expected r3 fenced back to lagging, got %s", got)
	}
}

func TestModelRebuildInterruptedByEpochChange(t *testing.T) {
	m := New("p1", ModeBestEffort, 1, "r1")
	mustApply(t, m, Event{Kind: EventReplicaDisconnect, ReplicaID: "r1"})
	mustApply(t, m, Event{Kind: EventReplicaNeedsRebuild, ReplicaID: "r1"})
	mustApply(t, m, Event{Kind: EventReplicaStartRebuild, ReplicaID: "r1", SnapshotID: "snap-2", SnapshotCpLSN: 100, ReservationID: "rebuild-2", ReservationTTL: 200})
	if got := m.Replica("r1").FSM.State; got != fsmv2.StateRebuilding {
		t.Fatalf("expected rebuilding, got %s", got)
	}

	mustApply(t, m, Event{Kind: EventPromoteReplica, ReplicaID: "r1"})
	if got := m.Replica("r1").FSM.State; got != fsmv2.StateLagging {
		t.Fatalf("expected lagging after epoch change fencing, got %s", got)
	}
}

func TestModelReservationLostDuringCatchupAfterRebuild(t *testing.T) {
	m := New("p1", ModeBestEffort, 1, "r1")
	mustApply(t, m, Event{Kind: EventReplicaDisconnect, ReplicaID: "r1"})
	mustApply(t, m, Event{Kind: EventReplicaNeedsRebuild, ReplicaID: "r1"})
	mustApply(t, m, Event{Kind: EventReplicaStartRebuild, ReplicaID: "r1", SnapshotID: "snap-3", SnapshotCpLSN: 50, ReservationID: "rebuild-3", ReservationTTL: 200})
	mustApply(t, m, Event{Kind: EventReplicaRebuildBaseApplied, ReplicaID: "r1", TargetLSN: 80})
	if got := m.Replica("r1").FSM.State; got != fsmv2.StateCatchUpAfterBuild {
		t.Fatalf("expected catch-up-after-rebuild, got %s", got)
	}

	mustApply(t, m, Event{Kind: EventReplicaReservationLost, ReplicaID: "r1"})
	if got := m.Replica("r1").FSM.State; got != fsmv2.StateNeedsRebuild {
		t.Fatalf("expected needs rebuild after reservation loss, got %s", got)
	}
}

func TestModelSyncAllBarrierAcknowledgeTargetLSN(t *testing.T) {
	m := New("p1", ModeSyncAll, 1, "r1", "r2")
	mustApply(t, m, Event{Kind: EventBootstrapReplica, ReplicaID: "r1", ReplicaFlushedLSN: 5})
	mustApply(t, m, Event{Kind: EventBootstrapReplica, ReplicaID: "r2", ReplicaFlushedLSN: 5})
	mustApply(t, m, Event{Kind: EventWriteCommitted, LSN: 10})

	if m.CanAcknowledgeLSN(10) {
		t.Fatal("sync_all should not acknowledge target LSN before barriers advance replica durability")
	}

	mustApply(t, m, Event{Kind: EventBarrierCompleted, ReplicaID: "r1", ReplicaFlushedLSN: 10})
	if m.CanAcknowledgeLSN(10) {
		t.Fatal("sync_all should still wait for second replica durability")
	}

	mustApply(t, m, Event{Kind: EventBarrierCompleted, ReplicaID: "r2", ReplicaFlushedLSN: 10})
	if !m.CanAcknowledgeLSN(10) {
		t.Fatal("sync_all should acknowledge once all required replicas are durable at target LSN")
	}
}

func TestModelSyncQuorumBarrierAcknowledgeTargetLSN(t *testing.T) {
	m := New("p1", ModeSyncQuorum, 1, "r1", "r2", "r3")
	mustApply(t, m, Event{Kind: EventBootstrapReplica, ReplicaID: "r1", ReplicaFlushedLSN: 5})
	mustApply(t, m, Event{Kind: EventBootstrapReplica, ReplicaID: "r2", ReplicaFlushedLSN: 5})
	mustApply(t, m, Event{Kind: EventBootstrapReplica, ReplicaID: "r3", ReplicaFlushedLSN: 5})
	mustApply(t, m, Event{Kind: EventWriteCommitted, LSN: 9})

	if m.CanAcknowledgeLSN(9) {
		t.Fatal("sync_quorum should not acknowledge before any replica reaches target durability")
	}

	mustApply(t, m, Event{Kind: EventBarrierCompleted, ReplicaID: "r1", ReplicaFlushedLSN: 9})
	if m.CanAcknowledgeLSN(9) {
		t.Fatal("sync_quorum should still wait because RF=4 quorum needs primary + two durable replicas")
	}
	mustApply(t, m, Event{Kind: EventBarrierCompleted, ReplicaID: "r2", ReplicaFlushedLSN: 9})
	if !m.CanAcknowledgeLSN(9) {
		t.Fatal("sync_quorum should acknowledge with primary + two durable replicas in RF=4")
	}
}

func TestModelWriteAdmissionReasons(t *testing.T) {
	m := New("p1", ModeSyncAll, 1, "r1")
	dec := m.WriteAdmission()
	if dec.Allowed || dec.Reason != "required_replica_not_in_sync" {
		t.Fatalf("unexpected admission before bootstrap: %+v", dec)
	}

	mustApply(t, m, Event{Kind: EventBootstrapReplica, ReplicaID: "r1", ReplicaFlushedLSN: 1})
	dec = m.WriteAdmission()
	if !dec.Allowed || dec.Reason != "all_replicas_sync_eligible" {
		t.Fatalf("unexpected admission after bootstrap: %+v", dec)
	}
}

func TestModelEvaluateReconnectUsesPlanner(t *testing.T) {
	m := New("p1", ModeBestEffort, 1, "r1")
	mustApply(t, m, Event{Kind: EventBootstrapReplica, ReplicaID: "r1", ReplicaFlushedLSN: 2})
	mustApply(t, m, Event{Kind: EventReplicaDisconnect, ReplicaID: "r1"})

	decision, err := m.EvaluateReconnect("r1", 2, 8)
	if err != nil {
		t.Fatalf("evaluate reconnect: %v", err)
	}
	if decision.Disposition != RecoveryCatchup {
		t.Fatalf("expected catchup decision, got %+v", decision)
	}
	if got := m.Replica("r1").FSM.State; got != fsmv2.StateCatchingUp {
		t.Fatalf("expected catching up, got %s", got)
	}
}

func TestModelEvaluateReconnectNeedsRebuildFromPlanner(t *testing.T) {
	m := New("p1", ModeBestEffort, 1, "r1")
	m.Planner = scriptedPlanner{decision: RecoveryDecision{
		Disposition: RecoveryNeedsRebuild,
		Reason:      "payload_not_resolvable",
	}}
	mustApply(t, m, Event{Kind: EventBootstrapReplica, ReplicaID: "r1", ReplicaFlushedLSN: 2})
	mustApply(t, m, Event{Kind: EventReplicaDisconnect, ReplicaID: "r1"})

	decision, err := m.EvaluateReconnect("r1", 2, 8)
	if err != nil {
		t.Fatalf("evaluate reconnect: %v", err)
	}
	if decision.Disposition != RecoveryNeedsRebuild || decision.Reason != "payload_not_resolvable" {
		t.Fatalf("unexpected decision: %+v", decision)
	}
	if got := m.Replica("r1").FSM.State; got != fsmv2.StateNeedsRebuild {
		t.Fatalf("expected needs rebuild, got %s", got)
	}
}

func TestModelEvaluateReconnectCarriesRecoveryClasses(t *testing.T) {
	m := New("p1", ModeBestEffort, 1, "r1")
	m.Planner = scriptedPlanner{decision: RecoveryDecision{
		Disposition:   RecoveryCatchup,
		ReservationID: "extent-resv",
		ReservationTTL: 42,
		Reason:        "extent_payload_resolvable",
		Classes:       []RecoveryClass{RecoveryClassExtentReferenced},
	}}
	mustApply(t, m, Event{Kind: EventBootstrapReplica, ReplicaID: "r1", ReplicaFlushedLSN: 3})
	mustApply(t, m, Event{Kind: EventReplicaDisconnect, ReplicaID: "r1"})

	decision, err := m.EvaluateReconnect("r1", 3, 9)
	if err != nil {
		t.Fatalf("evaluate reconnect: %v", err)
	}
	if len(decision.Classes) != 1 || decision.Classes[0] != RecoveryClassExtentReferenced {
		t.Fatalf("unexpected recovery classes: %+v", decision.Classes)
	}
	if got := m.Replica("r1").FSM.State; got != fsmv2.StateCatchingUp {
		t.Fatalf("expected catching up, got %s", got)
	}
	if got := m.Replica("r1").FSM.RecoveryReservationID; got != "extent-resv" {
		t.Fatalf("expected reservation extent-resv, got %q", got)
	}
}

func TestModelEvaluateReconnectCanChangeOverTime(t *testing.T) {
	m := New("p1", ModeBestEffort, 1, "r1")
	mustApply(t, m, Event{Kind: EventBootstrapReplica, ReplicaID: "r1", ReplicaFlushedLSN: 4})
	mustApply(t, m, Event{Kind: EventReplicaDisconnect, ReplicaID: "r1"})

	m.Planner = scriptedPlanner{decision: RecoveryDecision{
		Disposition:   RecoveryCatchup,
		ReservationID: "resv-1",
		ReservationTTL: 10,
		Reason:        "temporarily_recoverable",
		Classes:       []RecoveryClass{RecoveryClassWALInline},
	}}
	decision, err := m.EvaluateReconnect("r1", 4, 12)
	if err != nil {
		t.Fatalf("first evaluate reconnect: %v", err)
	}
	if decision.Disposition != RecoveryCatchup {
		t.Fatalf("expected catchup on first evaluation, got %+v", decision)
	}

	mustApply(t, m, Event{Kind: EventReplicaReservationLost, ReplicaID: "r1"})
	if got := m.Replica("r1").FSM.State; got != fsmv2.StateNeedsRebuild {
		t.Fatalf("expected needs rebuild after reservation loss, got %s", got)
	}

	m.Planner = scriptedPlanner{decision: RecoveryDecision{
		Disposition: RecoveryNeedsRebuild,
		Reason:      "recoverability_expired",
	}}
	decision, err = m.EvaluateReconnect("r1", 4, 12)
	if err != nil {
		t.Fatalf("second evaluate reconnect: %v", err)
	}
	if decision.Reason != "recoverability_expired" {
		t.Fatalf("unexpected second decision: %+v", decision)
	}
}

func TestRunScenarioProducesStateTrace(t *testing.T) {
	m := New("p1", ModeSyncAll, 1, "r1")
	trace, err := RunScenario(m, []ScenarioStep{
		{Name: "bootstrap", Event: Event{Kind: EventBootstrapReplica, ReplicaID: "r1", ReplicaFlushedLSN: 1}},
		{Name: "write10", Event: Event{Kind: EventWriteCommitted, LSN: 10}},
		{Name: "barrier10", Event: Event{Kind: EventBarrierCompleted, ReplicaID: "r1", ReplicaFlushedLSN: 10}},
	})
	if err != nil {
		t.Fatalf("run scenario: %v", err)
	}
	if len(trace) != 4 {
		t.Fatalf("expected 4 snapshots, got %d", len(trace))
	}
	last := trace[len(trace)-1]
	if last.HeadLSN != 10 {
		t.Fatalf("expected head 10, got %d", last.HeadLSN)
	}
	if got := last.Replicas["r1"].FlushedLSN; got != 10 {
		t.Fatalf("expected replica flushed 10, got %d", got)
	}
	if !last.AckGate.Allowed {
		t.Fatalf("expected ack gate allowed at final step, got %+v", last.AckGate)
	}
}

func TestScriptedRecoveryPlannerChangesDecisionOverTime(t *testing.T) {
	m := New("p1", ModeBestEffort, 1, "r1")
	m.Planner = &ScriptedRecoveryPlanner{
		Decisions: []RecoveryDecision{
			{
				Disposition:   RecoveryCatchup,
				ReservationID: "resv-a",
				ReservationTTL: 10,
				Reason:        "recoverable_now",
				Classes:       []RecoveryClass{RecoveryClassWALInline},
			},
			{
				Disposition: RecoveryNeedsRebuild,
				Reason:      "recoverability_expired",
			},
		},
	}
	mustApply(t, m, Event{Kind: EventBootstrapReplica, ReplicaID: "r1", ReplicaFlushedLSN: 4})
	mustApply(t, m, Event{Kind: EventReplicaDisconnect, ReplicaID: "r1"})

	first, err := m.EvaluateReconnect("r1", 4, 12)
	if err != nil {
		t.Fatalf("first reconnect: %v", err)
	}
	if first.Disposition != RecoveryCatchup {
		t.Fatalf("unexpected first decision: %+v", first)
	}
	mustApply(t, m, Event{Kind: EventReplicaReservationLost, ReplicaID: "r1"})

	second, err := m.EvaluateReconnect("r1", 4, 12)
	if err != nil {
		t.Fatalf("second reconnect: %v", err)
	}
	if second.Disposition != RecoveryNeedsRebuild || second.Reason != "recoverability_expired" {
		t.Fatalf("unexpected second decision: %+v", second)
	}
}

func TestFormatTraceIncludesReplicaStatesAndGates(t *testing.T) {
	m := New("p1", ModeSyncAll, 1, "r1")
	trace, err := RunScenario(m, []ScenarioStep{
		{Name: "bootstrap", Event: Event{Kind: EventBootstrapReplica, ReplicaID: "r1", ReplicaFlushedLSN: 1}},
		{Name: "write10", Event: Event{Kind: EventWriteCommitted, LSN: 10}},
		{Name: "barrier10", Event: Event{Kind: EventBarrierCompleted, ReplicaID: "r1", ReplicaFlushedLSN: 10}},
	})
	if err != nil {
		t.Fatalf("run scenario: %v", err)
	}
	got := FormatTrace(trace)
	wantParts := []string{
		"step=bootstrap",
		"write=true:all_replicas_sync_eligible",
		"step=barrier10",
		"ack=true:all_replicas_durable",
		"r1=InSync@10",
	}
	for _, part := range wantParts {
		if !strings.Contains(got, part) {
			t.Fatalf("trace missing %q:\n%s", part, got)
		}
	}
}
