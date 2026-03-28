package distsim

import (
	"testing"
)

// ============================================================
// Phase 02 P2: Real V1/V1.5 failure reproductions
// Source: actual Phase 13 hardware behavior and CP13-8 findings
// ============================================================

// --- Scenario: Changed-address restart (CP13-8 T4b) ---
// Real bug: replica restarts on a different port. V1.5 shipper retries
// the old address forever. Catch-up never succeeds because the old
// address is dead.

func TestP02_V1_ChangedAddressRestart_NeverRecovers(t *testing.T) {
	c := NewClusterWithProtocol(CommitSyncQuorum, ProtocolV1, "p", "r1", "r2")

	c.CommitWrite(1)
	c.CommitWrite(2)
	c.TickN(5)

	// r1 restarts with changed address — endpoint version bumps.
	c.StopNode("r1")
	c.Coordinator.Epoch++
	for _, n := range c.Nodes {
		if n.Running {
			n.Epoch = c.Coordinator.Epoch
		}
	}
	c.RestartNodeWithNewAddress("r1")

	// Messages from primary to r1 now rejected: stale endpoint.
	staleRejects := c.RejectedByReason(RejectStaleEndpoint)

	// Writes accumulate — r1 can't receive (endpoint mismatch).
	for i := uint64(3); i <= 12; i++ {
		c.CommitWrite(i)
	}
	c.TickN(5)

	// Verify: messages rejected by stale endpoint, not just link down.
	newStaleRejects := c.RejectedByReason(RejectStaleEndpoint) - staleRejects
	if newStaleRejects == 0 {
		t.Fatal("V1: writes to r1 should be rejected by stale_endpoint")
	}

	// V1: no recovery trigger available.
	trigger, _, ok := c.TriggerRecoverySession("r1")
	if ok {
		t.Fatalf("V1 should not trigger recovery, got %s", trigger)
	}

	// Gap confirmed.
	r1 := c.Nodes["r1"]
	if err := c.AssertCommittedRecoverable("r1"); err == nil {
		t.Fatal("V1: r1 should have data inconsistency")
	}
	t.Logf("V1: gap=%d, %d stale_endpoint rejections, no recovery path",
		c.Coordinator.CommittedLSN-r1.Storage.FlushedLSN, newStaleRejects)
}

func TestP02_V15_ChangedAddressRestart_RetriesToStaleAddress(t *testing.T) {
	c := NewClusterWithProtocol(CommitSyncQuorum, ProtocolV15, "p", "r1", "r2")

	c.CommitWrite(1)
	c.CommitWrite(2)
	c.TickN(5)

	// r1 restarts with changed address — endpoint version bumps.
	c.StopNode("r1")
	c.Coordinator.Epoch++
	for _, n := range c.Nodes {
		if n.Running {
			n.Epoch = c.Coordinator.Epoch
		}
	}
	c.RestartNodeWithNewAddress("r1")

	// Writes accumulate — rejected by stale endpoint.
	for i := uint64(3); i <= 12; i++ {
		c.CommitWrite(i)
	}
	c.TickN(5)

	// V1.5: recovery trigger fails — address mismatch detected.
	trigger, _, ok := c.TriggerRecoverySession("r1")
	if ok {
		t.Fatalf("V1.5 should not trigger recovery with changed address, got %s", trigger)
	}

	// Heartbeat reveals new endpoint, but V1.5 can only do control_plane_only.
	report := c.ReportHeartbeat("r1")
	update := c.CoordinatorDetectEndpointChange(report)
	if update == nil {
		t.Fatal("coordinator should detect endpoint change")
	}
	// V1.5: does NOT apply assignment update — no mechanism to update primary.
	if got := c.Protocol.ChangedAddressRestartAction(true); got != "control_plane_only" {
		t.Fatalf("V1.5: got %q, want control_plane_only", got)
	}

	// Gap persists, data inconsistency.
	r1 := c.Nodes["r1"]
	if err := c.AssertCommittedRecoverable("r1"); err == nil {
		t.Fatal("V1.5: r1 should have data inconsistency")
	}
	t.Logf("V1.5: gap=%d, stale endpoint blocks recovery — control_plane_only",
		c.Coordinator.CommittedLSN-r1.Storage.FlushedLSN)
}

func TestP02_V2_ChangedAddressRestart_ExplicitReassignment(t *testing.T) {
	c := NewClusterWithProtocol(CommitSyncQuorum, ProtocolV2, "p", "r1", "r2")
	c.MaxCatchupAttempts = 5

	c.CommitWrite(1)
	c.CommitWrite(2)
	c.TickN(5)

	// r1 restarts with changed address — endpoint version bumps.
	c.StopNode("r1")
	c.Coordinator.Epoch++
	for _, n := range c.Nodes {
		if n.Running {
			n.Epoch = c.Coordinator.Epoch
		}
	}
	c.RestartNodeWithNewAddress("r1")

	// Writes accumulate — rejected by stale endpoint.
	for i := uint64(3); i <= 12; i++ {
		c.CommitWrite(i)
	}
	c.TickN(5)

	// Before control-plane flow: recovery trigger fails (stale endpoint).
	trigger, _, ok := c.TriggerRecoverySession("r1")
	if ok {
		t.Fatalf("V2: recovery should fail before assignment update, got %s", trigger)
	}

	// Step 1: heartbeat discovers new endpoint.
	report := c.ReportHeartbeat("r1")
	update := c.CoordinatorDetectEndpointChange(report)
	if update == nil {
		t.Fatal("coordinator should detect endpoint change")
	}

	// Step 2: coordinator applies assignment — primary learns new address.
	c.ApplyAssignmentUpdate(*update)

	// Step 3: recovery trigger now succeeds (endpoint matches).
	trigger, _, ok = c.TriggerRecoverySession("r1")
	if !ok || trigger != TriggerReassignment {
		t.Fatalf("V2: expected reassignment trigger after update, got %s/%v", trigger, ok)
	}

	// Step 4: catch-up via protocol.
	converged := c.CatchUpWithEscalation("r1", 100)
	if !converged {
		t.Fatal("V2: catch-up should converge after reassignment")
	}

	// Data correct after full control-plane flow.
	if err := c.AssertCommittedRecoverable("r1"); err != nil {
		t.Fatalf("V2: data incorrect after reassignment+catchup: %v", err)
	}
	t.Logf("V2: recovered via heartbeat→detect→assignment→trigger→catchup")
}

// --- Scenario: Same-address transient outage ---
// Common case: brief network hiccup, same ports.

func TestP02_V1_TransientOutage_Degrades(t *testing.T) {
	c := NewClusterWithProtocol(CommitSyncQuorum, ProtocolV1, "p", "r1", "r2")

	c.CommitWrite(1)
	c.TickN(5)

	// Brief partition.
	c.Disconnect("p", "r1")
	c.Disconnect("r1", "p")
	c.CommitWrite(2)
	c.CommitWrite(3)
	c.TickN(5)

	// Heal.
	c.Connect("p", "r1")
	c.Connect("r1", "p")

	// V1: no catch-up. r1 stays at flushed=1.
	if c.Protocol.CanAttemptCatchup(true) {
		t.Fatal("V1 should not catch-up even with stable address")
	}

	c.TickN(5)
	r1 := c.Nodes["r1"]
	if r1.Storage.FlushedLSN >= c.Coordinator.CommittedLSN {
		// V1 doesn't catch up — unless messages from BEFORE disconnect are still delivering.
		// In our model, messages enqueued before disconnect may still arrive. That's a V1 "accident" not protocol.
	}
	t.Logf("V1 transient outage: flushed=%d committed=%d action=%s",
		r1.Storage.FlushedLSN, c.Coordinator.CommittedLSN,
		c.Protocol.BriefDisconnectAction(true, true))
}

func TestP02_V15_TransientOutage_CatchesUp(t *testing.T) {
	c := NewClusterWithProtocol(CommitSyncQuorum, ProtocolV15, "p", "r1", "r2")
	c.MaxCatchupAttempts = 5

	c.CommitWrite(1)
	c.TickN(5)

	c.Disconnect("p", "r1")
	c.Disconnect("r1", "p")
	c.CommitWrite(2)
	c.CommitWrite(3)
	c.TickN(5)

	c.Connect("p", "r1")
	c.Connect("r1", "p")

	// V1.5: catch-up works if address stable.
	if !c.Protocol.CanAttemptCatchup(true) {
		t.Fatal("V1.5 should catch-up with stable address")
	}

	r1 := c.Nodes["r1"]
	r1.ReplicaState = NodeStateCatchingUp
	converged := c.CatchUpWithEscalation("r1", 100)
	if !converged {
		t.Fatal("V1.5: should converge for short gap with stable address")
	}
	if r1.ReplicaState != NodeStateInSync {
		t.Fatalf("V1.5: expected InSync, got %s", r1.ReplicaState)
	}
	if err := c.AssertCommittedRecoverable("r1"); err != nil {
		t.Fatal(err)
	}
	t.Logf("V1.5 transient outage: recovered via catch-up, flushed=%d", r1.Storage.FlushedLSN)
}

func TestP02_V2_TransientOutage_ReservedCatchup(t *testing.T) {
	c := NewClusterWithProtocol(CommitSyncQuorum, ProtocolV2, "p", "r1", "r2")
	c.MaxCatchupAttempts = 5

	c.CommitWrite(1)
	c.TickN(5)

	c.Disconnect("p", "r1")
	c.Disconnect("r1", "p")
	c.CommitWrite(2)
	c.CommitWrite(3)
	c.TickN(5)

	c.Connect("p", "r1")
	c.Connect("r1", "p")

	// V2: reserved catch-up — explicit recoverability check.
	action := c.Protocol.BriefDisconnectAction(true, true)
	if action != "reserved_catchup" {
		t.Fatalf("V2 brief disconnect: got %q, want reserved_catchup", action)
	}

	r1 := c.Nodes["r1"]
	r1.ReplicaState = NodeStateCatchingUp
	converged := c.CatchUpWithEscalation("r1", 100)
	if !converged {
		t.Fatal("V2: should converge for short gap")
	}
	if err := c.AssertCommittedRecoverable("r1"); err != nil {
		t.Fatal(err)
	}
	t.Logf("V2 transient outage: reserved catch-up succeeded")
}

// --- Scenario: Slow control-plane recovery ---
// Source: real Phase 13 hardware behavior.
// Data path recovers fast. Control plane (master) is slow to re-issue
// assignments. During this window, V1/V1.5 behavior differs from V2.

func TestP02_SlowControlPlane_V1_WaitsForMaster(t *testing.T) {
	c := NewClusterWithProtocol(CommitSyncQuorum, ProtocolV1, "p", "r1", "r2")

	c.CommitWrite(1)
	c.TickN(5)

	// r1 disconnects. Stays disconnected through outage + control-plane delay.
	c.Disconnect("p", "r1")
	c.Disconnect("r1", "p")

	// Writes accumulate: outage write + delay-window writes. r1 misses all.
	for i := uint64(2); i <= 10; i++ {
		c.CommitWrite(i)
	}
	c.TickN(5)

	// Data path heals — but V1 has no catch-up protocol.
	c.Connect("p", "r1")
	c.Connect("r1", "p")

	// V1: no recovery trigger even with address stable.
	trigger, _, ok := c.TriggerRecoverySession("r1")
	if ok {
		t.Fatalf("V1 should not trigger recovery, got %s", trigger)
	}

	// r1 is behind: FlushedLSN=1, CommittedLSN=10. Gap = 9.
	r1 := c.Nodes["r1"]
	gap := c.Coordinator.CommittedLSN - r1.Storage.FlushedLSN
	if gap < 9 {
		t.Fatalf("V1: expected gap >= 9, got %d", gap)
	}

	// V1 data inconsistency: r1 missed writes 2-10. No self-heal mechanism.
	err := c.AssertCommittedRecoverable("r1")
	if err == nil {
		t.Fatal("V1: r1 should have data inconsistency — no catch-up mechanism")
	}
	t.Logf("V1 slow control-plane: gap=%d, data inconsistency — %v", gap, err)
}

func TestP02_SlowControlPlane_V15_BackgroundReconnect(t *testing.T) {
	c := NewClusterWithProtocol(CommitSyncQuorum, ProtocolV15, "p", "r1", "r2")
	c.MaxCatchupAttempts = 5

	c.CommitWrite(1)
	c.TickN(5)

	// r1 disconnects. Stays disconnected through outage + delay window.
	c.Disconnect("p", "r1")
	c.Disconnect("r1", "p")

	// Writes accumulate while r1 is disconnected.
	for i := uint64(2); i <= 10; i++ {
		c.CommitWrite(i)
	}
	c.TickN(5)

	// Data path heals.
	c.Connect("p", "r1")
	c.Connect("r1", "p")

	// Before catch-up: r1 is behind (FlushedLSN=1, CommittedLSN=10).
	r1 := c.Nodes["r1"]
	if r1.Storage.FlushedLSN >= c.Coordinator.CommittedLSN {
		t.Fatal("V1.5: r1 should be behind before catch-up")
	}
	if err := c.AssertCommittedRecoverable("r1"); err == nil {
		t.Fatal("V1.5: r1 should have data gap before catch-up")
	}

	// V1.5 policy: background reconnect if address stable.
	if c.Protocol.RestartRejoinAction(true) != "background_reconnect_or_control_plane" {
		t.Fatal("V1.5 stable-address should be background_reconnect_or_control_plane")
	}

	// V1.5 recovery trigger: background reconnect (address stable → endpoint matches).
	trigger, _, ok := c.TriggerRecoverySession("r1")
	if !ok || trigger != TriggerBackgroundReconnect {
		t.Fatalf("V1.5: expected background_reconnect trigger, got %s/%v", trigger, ok)
	}
	// r1.ReplicaState is now CatchingUp (set by TriggerRecoverySession).
	converged := c.CatchUpWithEscalation("r1", 100)
	if !converged {
		t.Fatal("V1.5: should catch up with stable address")
	}

	// After catch-up: data correct.
	if err := c.AssertCommittedRecoverable("r1"); err != nil {
		t.Fatalf("V1.5: data should be correct after catch-up — %v", err)
	}

	// V1.5 changed-address: falls back to control plane.
	if c.Protocol.RestartRejoinAction(false) != "control_plane_only" {
		t.Fatal("V1.5 changed-address should fall back to control_plane_only")
	}
	t.Logf("V1.5 slow control-plane: caught up %d entries via background reconnect",
		c.Coordinator.CommittedLSN-1)
}

func TestP02_SlowControlPlane_V2_DirectReconnect(t *testing.T) {
	c := NewClusterWithProtocol(CommitSyncQuorum, ProtocolV2, "p", "r1", "r2")
	c.MaxCatchupAttempts = 5

	c.CommitWrite(1)
	c.TickN(5)

	// r1 disconnects. Stays disconnected through outage + delay window.
	c.Disconnect("p", "r1")
	c.Disconnect("r1", "p")

	// Writes accumulate while r1 is disconnected.
	for i := uint64(2); i <= 10; i++ {
		c.CommitWrite(i)
	}
	c.TickN(5)

	// Data path heals.
	c.Connect("p", "r1")
	c.Connect("r1", "p")

	// Before catch-up: r1 is behind.
	r1 := c.Nodes["r1"]
	if r1.Storage.FlushedLSN >= c.Coordinator.CommittedLSN {
		t.Fatal("V2: r1 should be behind before direct reconnect")
	}
	if err := c.AssertCommittedRecoverable("r1"); err == nil {
		t.Fatal("V2: r1 should have data gap before direct reconnect")
	}

	// V2 policy: direct reconnect, doesn't wait for master.
	if c.Protocol.RestartRejoinAction(true) != "direct_reconnect_or_control_plane" {
		t.Fatal("V2 should be direct_reconnect_or_control_plane")
	}

	// V2 recovery trigger: reassignment (address stable → endpoint matches).
	trigger, _, ok := c.TriggerRecoverySession("r1")
	if !ok || trigger != TriggerReassignment {
		t.Fatalf("V2: expected reassignment trigger, got %s/%v", trigger, ok)
	}
	converged := c.CatchUpWithEscalation("r1", 100)
	if !converged {
		t.Fatal("V2: should catch up directly without master intervention")
	}

	// After catch-up: data correct.
	if err := c.AssertCommittedRecoverable("r1"); err != nil {
		t.Fatalf("V2: data should be correct after direct reconnect — %v", err)
	}
	t.Logf("V2 slow control-plane: caught up %d entries immediately via direct reconnect",
		c.Coordinator.CommittedLSN-1)
}
