package runtime

import "testing"

func TestPendingCoordinator_TakeCatchUp_MatchSucceeds(t *testing.T) {
	pc := NewPendingCoordinator(nil)
	pc.Store("vol1", &PendingExecution{
		VolumeID:      "vol1",
		CatchUpTarget: 100,
	})

	pe := pc.TakeCatchUp("vol1", 100)
	if pe == nil {
		t.Fatal("matching take should succeed")
	}
	if pe.CatchUpTarget != 100 {
		t.Fatalf("target=%d", pe.CatchUpTarget)
	}

	// Second take should return nil (already consumed).
	if pc.TakeCatchUp("vol1", 100) != nil {
		t.Fatal("second take should return nil")
	}
}

func TestPendingCoordinator_TakeCatchUp_MismatchCancels(t *testing.T) {
	var cancelledReason string
	pc := NewPendingCoordinator(func(pe *PendingExecution, reason string) {
		cancelledReason = reason
	})
	pc.Store("vol1", &PendingExecution{
		VolumeID:      "vol1",
		CatchUpTarget: 100,
	})

	pe := pc.TakeCatchUp("vol1", 999) // wrong target
	if pe != nil {
		t.Fatal("mismatched take should return nil")
	}
	if cancelledReason != "start_catchup_target_mismatch" {
		t.Fatalf("cancel reason=%q", cancelledReason)
	}

	// Pending should be consumed (removed even on mismatch).
	if pc.Has("vol1") {
		t.Fatal("pending should be removed after mismatch")
	}
}

func TestPendingCoordinator_TakeRebuild_MatchSucceeds(t *testing.T) {
	pc := NewPendingCoordinator(nil)
	pc.Store("vol1", &PendingExecution{
		VolumeID:         "vol1",
		RebuildTargetLSN: 50,
	})

	pe := pc.TakeRebuild("vol1", 50)
	if pe == nil {
		t.Fatal("matching rebuild take should succeed")
	}
}

func TestPendingCoordinator_TakeRebuild_MismatchCancels(t *testing.T) {
	var cancelledReason string
	pc := NewPendingCoordinator(func(pe *PendingExecution, reason string) {
		cancelledReason = reason
	})
	pc.Store("vol1", &PendingExecution{
		VolumeID:         "vol1",
		RebuildTargetLSN: 50,
	})

	pe := pc.TakeRebuild("vol1", 999)
	if pe != nil {
		t.Fatal("mismatched rebuild take should return nil")
	}
	if cancelledReason != "start_rebuild_target_mismatch" {
		t.Fatalf("cancel reason=%q", cancelledReason)
	}
}

func TestPendingCoordinator_Cancel_ExplicitRemoval(t *testing.T) {
	var cancelledReason string
	pc := NewPendingCoordinator(func(pe *PendingExecution, reason string) {
		cancelledReason = reason
	})
	pc.Store("vol1", &PendingExecution{VolumeID: "vol1"})

	pc.Cancel("vol1", "superseded")
	if cancelledReason != "superseded" {
		t.Fatalf("cancel reason=%q", cancelledReason)
	}
	if pc.Has("vol1") {
		t.Fatal("should be removed after cancel")
	}
}

func TestPendingCoordinator_Cancel_NoopWhenEmpty(t *testing.T) {
	cancelled := false
	pc := NewPendingCoordinator(func(pe *PendingExecution, reason string) {
		cancelled = true
	})

	pc.Cancel("vol1", "noop")
	if cancelled {
		t.Fatal("cancel on empty should not invoke callback")
	}
}

func TestPendingCoordinator_Has_And_Peek(t *testing.T) {
	pc := NewPendingCoordinator(nil)

	if pc.Has("vol1") {
		t.Fatal("empty coordinator should not have vol1")
	}
	if pc.Peek("vol1") != nil {
		t.Fatal("peek on empty should return nil")
	}

	pc.Store("vol1", &PendingExecution{VolumeID: "vol1", CatchUpTarget: 42})

	if !pc.Has("vol1") {
		t.Fatal("should have vol1 after store")
	}
	pe := pc.Peek("vol1")
	if pe == nil || pe.CatchUpTarget != 42 {
		t.Fatal("peek should return stored execution")
	}

	// Peek does not consume.
	if !pc.Has("vol1") {
		t.Fatal("peek should not remove")
	}
}

func TestPendingCoordinator_StoreReplaces(t *testing.T) {
	pc := NewPendingCoordinator(nil)
	pc.Store("vol1", &PendingExecution{VolumeID: "vol1", CatchUpTarget: 10})
	pc.Store("vol1", &PendingExecution{VolumeID: "vol1", CatchUpTarget: 20})

	pe := pc.TakeCatchUp("vol1", 20)
	if pe == nil {
		t.Fatal("replaced store should be latest")
	}
}

func TestPendingCoordinator_TakeFromEmpty_ReturnsNil(t *testing.T) {
	pc := NewPendingCoordinator(nil)
	if pc.TakeCatchUp("vol1", 100) != nil {
		t.Fatal("take from empty should return nil")
	}
	if pc.TakeRebuild("vol1", 100) != nil {
		t.Fatal("take rebuild from empty should return nil")
	}
}
