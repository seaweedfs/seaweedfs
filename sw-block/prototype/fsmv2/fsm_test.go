package fsmv2

import "testing"

func mustApply(t *testing.T, f *FSM, evt Event) []Action {
	t.Helper()
	actions, err := f.Apply(evt)
	if err != nil {
		t.Fatalf("apply %s: %v", evt.Kind, err)
	}
	return actions
}

func TestFSMBootstrapToInSync(t *testing.T) {
	f := New(7)
	mustApply(t, f, Event{Kind: EventBootstrapComplete, ReplicaFlushedLSN: 10})
	if f.State != StateInSync || !f.IsSyncEligible() || f.ReplicaFlushedLSN != 10 {
		t.Fatalf("unexpected bootstrap result: state=%s eligible=%v lsn=%d", f.State, f.IsSyncEligible(), f.ReplicaFlushedLSN)
	}
}

func TestFSMCatchupPromotionHoldFlow(t *testing.T) {
	f := New(3)
	mustApply(t, f, Event{Kind: EventBootstrapComplete, ReplicaFlushedLSN: 5})
	mustApply(t, f, Event{Kind: EventDisconnect})
	mustApply(t, f, Event{Kind: EventReconnectCatchup, ReplicaFlushedLSN: 5, TargetLSN: 20, ReservationID: "r1", ReservationTTL: 100})
	if f.State != StateCatchingUp {
		t.Fatalf("expected catching up, got %s", f.State)
	}
	mustApply(t, f, Event{Kind: EventCatchupProgress, ReplicaFlushedLSN: 20, PromotionHoldTill: 30})
	if f.State != StatePromotionHold {
		t.Fatalf("expected promotion hold, got %s", f.State)
	}
	mustApply(t, f, Event{Kind: EventPromotionHealthy, Now: 29})
	if f.State != StatePromotionHold {
		t.Fatalf("hold exited too early: %s", f.State)
	}
	mustApply(t, f, Event{Kind: EventPromotionHealthy, Now: 30})
	if f.State != StateInSync || !f.IsSyncEligible() {
		t.Fatalf("expected insync after hold, got %s eligible=%v", f.State, f.IsSyncEligible())
	}
}

func TestFSMRebuildFlow(t *testing.T) {
	f := New(11)
	mustApply(t, f, Event{Kind: EventDisconnect})
	mustApply(t, f, Event{Kind: EventReconnectRebuild})
	if f.State != StateNeedsRebuild {
		t.Fatalf("expected needs rebuild, got %s", f.State)
	}
	mustApply(t, f, Event{Kind: EventStartRebuild, SnapshotID: "snap-1", SnapshotCpLSN: 100, ReservationID: "rr", ReservationTTL: 200})
	if f.State != StateRebuilding {
		t.Fatalf("expected rebuilding, got %s", f.State)
	}
	mustApply(t, f, Event{Kind: EventRebuildBaseApplied, TargetLSN: 140})
	if f.State != StateCatchUpAfterBuild || f.ReplicaFlushedLSN != 100 {
		t.Fatalf("unexpected rebuild-base state=%s lsn=%d", f.State, f.ReplicaFlushedLSN)
	}
	mustApply(t, f, Event{Kind: EventCatchupProgress, ReplicaFlushedLSN: 140, PromotionHoldTill: 150})
	mustApply(t, f, Event{Kind: EventPromotionHealthy, Now: 150})
	if f.State != StateInSync || f.SnapshotID != "snap-1" {
		t.Fatalf("expected insync after rebuild, got state=%s snapshot=%q", f.State, f.SnapshotID)
	}
}

func TestFSMEpochChangeAbortsRecovery(t *testing.T) {
	f := New(1)
	mustApply(t, f, Event{Kind: EventBootstrapComplete, ReplicaFlushedLSN: 1})
	mustApply(t, f, Event{Kind: EventDisconnect})
	mustApply(t, f, Event{Kind: EventReconnectCatchup, ReplicaFlushedLSN: 1, TargetLSN: 5, ReservationID: "r1", ReservationTTL: 99})
	mustApply(t, f, Event{Kind: EventEpochChanged, Epoch: 2})
	if f.State != StateLagging || f.RecoveryReservationID != "" || f.IsSyncEligible() {
		t.Fatalf("unexpected state after epoch change: state=%s reservation=%q eligible=%v", f.State, f.RecoveryReservationID, f.IsSyncEligible())
	}
}

func TestFSMReservationLostNeedsRebuild(t *testing.T) {
	f := New(5)
	mustApply(t, f, Event{Kind: EventBootstrapComplete, ReplicaFlushedLSN: 9})
	mustApply(t, f, Event{Kind: EventDisconnect})
	mustApply(t, f, Event{Kind: EventReconnectCatchup, ReplicaFlushedLSN: 9, TargetLSN: 15, ReservationID: "r2", ReservationTTL: 80})
	mustApply(t, f, Event{Kind: EventRetentionLost})
	if f.State != StateNeedsRebuild {
		t.Fatalf("expected needs rebuild after reservation lost, got %s", f.State)
	}
}

func TestFSMDurableProgressWhileInSync(t *testing.T) {
	f := New(2)
	mustApply(t, f, Event{Kind: EventBootstrapComplete, ReplicaFlushedLSN: 4})
	mustApply(t, f, Event{Kind: EventDurableProgress, ReplicaFlushedLSN: 8})
	if f.ReplicaFlushedLSN != 8 || f.State != StateInSync {
		t.Fatalf("unexpected in-sync durable progress: state=%s lsn=%d", f.State, f.ReplicaFlushedLSN)
	}
}
