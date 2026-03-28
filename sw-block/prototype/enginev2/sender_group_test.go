package enginev2

import "testing"

// === SenderGroup reconciliation ===

func TestSenderGroup_Reconcile_AddNew(t *testing.T) {
	sg := NewSenderGroup()

	eps := map[string]Endpoint{
		"r1:9333": {DataAddr: "r1:9333", Version: 1},
		"r2:9333": {DataAddr: "r2:9333", Version: 1},
	}
	added, removed := sg.Reconcile(eps, 1)

	if len(added) != 2 || len(removed) != 0 {
		t.Fatalf("added=%v removed=%v", added, removed)
	}
	if sg.Len() != 2 {
		t.Fatalf("len: got %d, want 2", sg.Len())
	}
}

func TestSenderGroup_Reconcile_RemoveStale(t *testing.T) {
	sg := NewSenderGroup()
	sg.Reconcile(map[string]Endpoint{
		"r1:9333": {DataAddr: "r1:9333", Version: 1},
		"r2:9333": {DataAddr: "r2:9333", Version: 1},
	}, 1)

	// Remove r2, keep r1.
	_, removed := sg.Reconcile(map[string]Endpoint{
		"r1:9333": {DataAddr: "r1:9333", Version: 1},
	}, 1)

	if len(removed) != 1 || removed[0] != "r2:9333" {
		t.Fatalf("removed=%v, want [r2:9333]", removed)
	}
	if sg.Sender("r2:9333") != nil {
		t.Fatal("r2 should be removed")
	}
	if sg.Sender("r1:9333") == nil {
		t.Fatal("r1 should be preserved")
	}
}

func TestSenderGroup_Reconcile_PreservesState(t *testing.T) {
	sg := NewSenderGroup()
	sg.Reconcile(map[string]Endpoint{
		"r1:9333": {DataAddr: "r1:9333", Version: 1},
	}, 1)

	// Attach session and advance.
	s := sg.Sender("r1:9333")
	sess, _ := s.AttachSession(1, SessionCatchUp)
	sess.SetRange(0, 100)
	sess.UpdateProgress(50)

	// Reconcile with same address — sender preserved.
	sg.Reconcile(map[string]Endpoint{
		"r1:9333": {DataAddr: "r1:9333", Version: 1},
	}, 1)

	s2 := sg.Sender("r1:9333")
	if s2 != s {
		t.Fatal("reconcile should preserve the same sender object")
	}
	if s2.Session() != sess {
		t.Fatal("reconcile should preserve the session")
	}
	if !sess.Active() {
		t.Fatal("session should still be active after same-address reconcile")
	}
}

func TestSenderGroup_Reconcile_MixedUpdate(t *testing.T) {
	sg := NewSenderGroup()
	sg.Reconcile(map[string]Endpoint{
		"r1:9333": {DataAddr: "r1:9333", Version: 1},
		"r2:9333": {DataAddr: "r2:9333", Version: 1},
	}, 1)

	// Keep r1, remove r2, add r3.
	added, removed := sg.Reconcile(map[string]Endpoint{
		"r1:9333": {DataAddr: "r1:9333", Version: 1},
		"r3:9333": {DataAddr: "r3:9333", Version: 1},
	}, 1)

	if len(added) != 1 || added[0] != "r3:9333" {
		t.Fatalf("added=%v, want [r3:9333]", added)
	}
	if len(removed) != 1 || removed[0] != "r2:9333" {
		t.Fatalf("removed=%v, want [r2:9333]", removed)
	}
	if sg.Len() != 2 {
		t.Fatalf("len=%d, want 2", sg.Len())
	}
}

func TestSenderGroup_Reconcile_EndpointChange_InvalidatesSession(t *testing.T) {
	sg := NewSenderGroup()
	sg.Reconcile(map[string]Endpoint{
		"r1:9333": {DataAddr: "r1:9333", Version: 1},
	}, 1)

	s := sg.Sender("r1:9333")
	sess, _ := s.AttachSession(1, SessionCatchUp)

	// Same ReplicaID but new endpoint version.
	sg.Reconcile(map[string]Endpoint{
		"r1:9333": {DataAddr: "r1:9333", Version: 2},
	}, 1)

	if sess.Active() {
		t.Fatal("endpoint version change should invalidate session")
	}
	if s.Session() != nil {
		t.Fatal("session should be nil after endpoint change")
	}
}

// === Epoch invalidation ===

func TestSenderGroup_InvalidateEpoch(t *testing.T) {
	sg := NewSenderGroup()
	sg.Reconcile(map[string]Endpoint{
		"r1:9333": {DataAddr: "r1:9333", Version: 1},
		"r2:9333": {DataAddr: "r2:9333", Version: 1},
	}, 1)

	// Both have sessions at epoch 1.
	s1 := sg.Sender("r1:9333")
	s2 := sg.Sender("r2:9333")
	sess1, _ := s1.AttachSession(1, SessionCatchUp)
	sess2, _ := s2.AttachSession(1, SessionCatchUp)

	// Epoch bumps to 2. Both sessions stale.
	count := sg.InvalidateEpoch(2)
	if count != 2 {
		t.Fatalf("should invalidate 2 sessions, got %d", count)
	}
	if sess1.Active() || sess2.Active() {
		t.Fatal("both sessions should be invalidated")
	}
	if s1.State != StateDisconnected || s2.State != StateDisconnected {
		t.Fatal("senders should be disconnected after epoch invalidation")
	}
}

func TestSenderGroup_InvalidateEpoch_SkipsCurrentEpoch(t *testing.T) {
	sg := NewSenderGroup()
	sg.Reconcile(map[string]Endpoint{
		"r1:9333": {DataAddr: "r1:9333", Version: 1},
	}, 2)

	s := sg.Sender("r1:9333")
	sess, _ := s.AttachSession(2, SessionCatchUp) // epoch 2 session

	// Invalidate epoch 2 — session AT epoch 2 should NOT be invalidated.
	count := sg.InvalidateEpoch(2)
	if count != 0 {
		t.Fatalf("should not invalidate current-epoch session, got %d", count)
	}
	if !sess.Active() {
		t.Fatal("current-epoch session should remain active")
	}
}

func TestSenderGroup_StopAll(t *testing.T) {
	sg := NewSenderGroup()
	sg.Reconcile(map[string]Endpoint{
		"r1:9333": {DataAddr: "r1:9333", Version: 1},
		"r2:9333": {DataAddr: "r2:9333", Version: 1},
	}, 1)

	sg.StopAll()

	for _, s := range sg.All() {
		if !s.Stopped() {
			t.Fatalf("%s should be stopped", s.ReplicaID)
		}
	}
}

func TestSenderGroup_All_DeterministicOrder(t *testing.T) {
	sg := NewSenderGroup()
	sg.Reconcile(map[string]Endpoint{
		"r3:9333": {DataAddr: "r3:9333", Version: 1},
		"r1:9333": {DataAddr: "r1:9333", Version: 1},
		"r2:9333": {DataAddr: "r2:9333", Version: 1},
	}, 1)

	all := sg.All()
	if len(all) != 3 {
		t.Fatalf("len=%d, want 3", len(all))
	}
	expected := []string{"r1:9333", "r2:9333", "r3:9333"}
	for i, exp := range expected {
		if all[i].ReplicaID != exp {
			t.Fatalf("all[%d]=%s, want %s", i, all[i].ReplicaID, exp)
		}
	}
}
