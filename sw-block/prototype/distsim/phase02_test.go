package distsim

import (
	"testing"
)

// ============================================================
// Phase 02: Protocol-state assertions + version comparison
// ============================================================

// --- P0: Protocol-level rejection assertions ---

func TestP02_EpochFencing_AllStaleTrafficRejected(t *testing.T) {
	c := NewCluster(CommitSyncQuorum, "p", "r1", "r2")

	c.CommitWrite(1)
	c.TickN(5)

	// Partition + promote.
	c.Disconnect("p", "r1")
	c.Disconnect("r1", "p")
	c.Disconnect("p", "r2")
	c.Disconnect("r2", "p")
	c.Promote("r1")
	staleEpoch := c.Coordinator.Epoch - 1
	c.Nodes["p"].Epoch = staleEpoch

	// Stale writes through protocol.
	delivered := c.StaleWrite("p", staleEpoch, 99)

	// Protocol-level assertion: zero accepted, all rejected by epoch.
	if delivered > 0 {
		t.Fatalf("stale traffic accepted: %d messages passed fencing", delivered)
	}
	epochRejects := c.RejectedByReason(RejectEpochMismatch)
	if epochRejects == 0 {
		t.Fatal("no epoch rejections recorded — fencing not tracked")
	}

	// Delivery log must show explicit rejections (protocol behavior, not just final state).
	totalRejected := 0
	for _, d := range c.Deliveries {
		if !d.Accepted {
			totalRejected++
		}
	}
	if totalRejected == 0 {
		t.Fatal("delivery log has no rejections — protocol behavior not recorded")
	}
	t.Logf("protocol-level: %d rejected, %d epoch_mismatch", totalRejected, epochRejects)
}

func TestP02_AcceptedDeliveries_Tracked(t *testing.T) {
	c := NewCluster(CommitSyncQuorum, "p", "r1", "r2")

	c.CommitWrite(1)
	c.TickN(5)

	// Should have accepted write + barrier deliveries.
	accepted := c.AcceptedCount()
	if accepted == 0 {
		t.Fatal("no accepted deliveries recorded")
	}
	acceptedWrites := c.AcceptedByKind(MsgWrite)
	if acceptedWrites == 0 {
		t.Fatal("no accepted write deliveries")
	}
	t.Logf("after 1 write: %d accepted total, %d writes", accepted, acceptedWrites)
}

// --- P1: S20 protocol-level closure ---

func TestP02_S20_StaleTraffic_CommittedPrefixUnchanged(t *testing.T) {
	c := NewCluster(CommitSyncQuorum, "A", "B", "C")

	c.CommitWrite(1)
	c.CommitWrite(2)
	c.TickN(5)

	// Partition A, promote B.
	c.Disconnect("A", "B")
	c.Disconnect("B", "A")
	c.Disconnect("A", "C")
	c.Disconnect("C", "A")
	c.Promote("B")
	c.Nodes["A"].Epoch = c.Coordinator.Epoch - 1

	// B writes (new epoch).
	c.CommitWrite(3)
	c.TickN(5)
	committedBefore := c.Coordinator.CommittedLSN

	// A stale writes through protocol.
	c.StaleWrite("A", c.Nodes["A"].Epoch, 99)

	// Protocol assertion: committed prefix unchanged by stale traffic.
	committedAfter := c.Coordinator.CommittedLSN
	if committedAfter != committedBefore {
		t.Fatalf("stale traffic changed committed prefix: before=%d after=%d", committedBefore, committedAfter)
	}

	// All stale messages rejected by epoch.
	if c.RejectedByReason(RejectEpochMismatch) == 0 {
		t.Fatal("no epoch rejections for stale traffic")
	}
}

// --- P1: S6 protocol-level closure ---

func TestP02_S6_NonConvergent_ExplicitStateTransition(t *testing.T) {
	c := NewCluster(CommitSyncQuorum, "p", "r1", "r2")
	c.MaxCatchupAttempts = 3

	for i := uint64(1); i <= 5; i++ {
		c.CommitWrite(i)
	}
	c.TickN(5)

	c.Disconnect("p", "r1")
	c.Disconnect("r1", "p")
	for i := uint64(6); i <= 100; i++ {
		c.CommitWrite(i % 8)
	}
	c.TickN(5)
	c.Connect("p", "r1")
	c.Connect("r1", "p")

	r1 := c.Nodes["r1"]
	r1.ReplicaState = NodeStateCatchingUp

	// Protocol assertion: state transitions are explicit.
	// Track the state at each step.
	var stateTrace []ReplicaNodeState
	for attempt := 0; attempt < 10; attempt++ {
		c.Disconnect("p", "r1")
		c.Disconnect("r1", "p")
		for w := 0; w < 20; w++ {
			c.CommitWrite(uint64(101+attempt*20+w) % 8)
		}
		c.TickN(2)
		c.Connect("p", "r1")
		c.Connect("r1", "p")

		c.CatchUpWithEscalation("r1", 1)
		stateTrace = append(stateTrace, r1.ReplicaState)

		if r1.ReplicaState == NodeStateNeedsRebuild {
			break
		}
	}

	// Must have explicit state transitions: CatchingUp → ... → NeedsRebuild.
	if r1.ReplicaState != NodeStateNeedsRebuild {
		t.Fatalf("expected NeedsRebuild, got %s", r1.ReplicaState)
	}
	// Trace must show CatchingUp before NeedsRebuild.
	hasCatchingUp := false
	for _, s := range stateTrace {
		if s == NodeStateCatchingUp {
			hasCatchingUp = true
		}
	}
	if !hasCatchingUp {
		t.Fatal("state trace should include CatchingUp before NeedsRebuild")
	}
	t.Logf("state trace: %v", stateTrace)
}

// --- P1: S18 protocol-level closure ---

func TestP02_S18_DelayedAck_ExplicitRejection(t *testing.T) {
	c := NewCluster(CommitSyncQuorum, "p", "r1", "r2")

	c.CommitWrite(1)
	c.TickN(5)

	// Write 2 without r1 ack.
	c.Disconnect("r1", "p")
	c.CommitWrite(2)
	c.TickN(3)

	// Restart primary with epoch bump.
	c.StopNode("p")
	c.Coordinator.Epoch++
	for _, n := range c.Nodes {
		if n.Running {
			n.Epoch = c.Coordinator.Epoch
		}
	}
	c.StartNode("p")

	committedBefore := c.Coordinator.CommittedLSN
	deliveriesBefore := len(c.Deliveries)

	// Reconnect r1, deliver stale ack.
	c.Connect("r1", "p")
	c.Connect("p", "r1")
	oldAck := Message{
		Kind: MsgBarrierAck, From: "r1", To: "p",
		Epoch: c.Coordinator.Epoch - 1, TargetLSN: 2,
	}
	c.deliver(oldAck)
	c.refreshCommits()

	// Protocol assertion 1: committed prefix unchanged.
	if c.Coordinator.CommittedLSN > committedBefore {
		t.Fatalf("stale ack advanced prefix: %d → %d", committedBefore, c.Coordinator.CommittedLSN)
	}

	// Protocol assertion 2: the delivery was explicitly recorded as rejected.
	newDeliveries := c.Deliveries[deliveriesBefore:]
	found := false
	for _, d := range newDeliveries {
		if !d.Accepted && d.Reason == RejectEpochMismatch && d.Msg.Kind == MsgBarrierAck {
			found = true
		}
	}
	if !found {
		t.Fatal("stale ack not recorded as epoch_mismatch rejection in delivery log")
	}
}

// --- P2: Version comparison ---

func TestP02_VersionComparison_BriefDisconnect(t *testing.T) {
	// Same scenario under V1, V1.5, V2 — different expected outcomes.
	for _, tc := range []struct {
		version         ProtocolVersion
		expectCatchup   bool
		expectRebuild   bool
	}{
		{ProtocolV1, false, false}, // V1: no catch-up, stays degraded
		{ProtocolV15, true, false}, // V1.5: catch-up possible if address stable
		{ProtocolV2, true, false},  // V2: catch-up allowed for this recoverable short-gap case
	} {
		t.Run(string(tc.version), func(t *testing.T) {
			c := NewClusterWithProtocol(CommitSyncQuorum, tc.version, "p", "r1", "r2")
			c.MaxCatchupAttempts = 5

			c.CommitWrite(1)
			c.CommitWrite(2)
			c.TickN(5)

			// Brief disconnect.
			c.Disconnect("p", "r1")
			c.Disconnect("r1", "p")
			c.CommitWrite(3)
			c.CommitWrite(4)
			c.TickN(5)
			c.Connect("p", "r1")
			c.Connect("r1", "p")

			canCatchup := c.Protocol.CanAttemptCatchup(true)
			if canCatchup != tc.expectCatchup {
				t.Fatalf("CanAttemptCatchup: got %v, want %v", canCatchup, tc.expectCatchup)
			}

			if canCatchup {
				// Catch up r1.
				r1 := c.Nodes["r1"]
				r1.ReplicaState = NodeStateCatchingUp
				converged := c.CatchUpWithEscalation("r1", 100)
				if !converged {
					t.Fatal("expected catch-up to converge for short gap")
				}
				if r1.ReplicaState != NodeStateInSync {
					t.Fatalf("expected InSync after catch-up, got %s", r1.ReplicaState)
				}
			}
		})
	}
}

func TestP02_VersionComparison_BriefDisconnectActions(t *testing.T) {
	for _, tc := range []struct {
		version      ProtocolVersion
		addrStable   bool
		recoverable  bool
		expectAction string
	}{
		{ProtocolV1, true, true, "degrade_or_rebuild"},
		{ProtocolV15, true, true, "catchup_if_history_survives"},
		{ProtocolV15, false, true, "stall_or_control_plane_recovery"},
		{ProtocolV2, true, true, "reserved_catchup"},
		{ProtocolV2, false, false, "explicit_rebuild"},
	} {
		t.Run(string(tc.version)+"_stable="+boolStr(tc.addrStable)+"_recoverable="+boolStr(tc.recoverable), func(t *testing.T) {
			policy := ProtocolPolicy{Version: tc.version}
			action := policy.BriefDisconnectAction(tc.addrStable, tc.recoverable)
			if action != tc.expectAction {
				t.Fatalf("BriefDisconnectAction(%v,%v): got %q, want %q", tc.addrStable, tc.recoverable, action, tc.expectAction)
			}
		})
	}
}

func TestP02_VersionComparison_TailChasing(t *testing.T) {
	for _, tc := range []struct {
		version        ProtocolVersion
		expectAction   string
	}{
		{ProtocolV1, "degrade"},
		{ProtocolV15, "stall_or_rebuild"},
		{ProtocolV2, "abort_to_rebuild"},
	} {
		t.Run(string(tc.version), func(t *testing.T) {
			policy := ProtocolPolicy{Version: tc.version}
			action := policy.TailChasingAction(false) // non-convergent
			if action != tc.expectAction {
				t.Fatalf("TailChasingAction(false): got %q, want %q", action, tc.expectAction)
			}
		})
	}
}

func TestP02_VersionComparison_RestartRejoin(t *testing.T) {
	for _, tc := range []struct {
		version       ProtocolVersion
		addrStable    bool
		expectAction  string
	}{
		{ProtocolV1, true, "control_plane_only"},
		{ProtocolV1, false, "control_plane_only"},
		{ProtocolV15, true, "background_reconnect_or_control_plane"},
		{ProtocolV15, false, "control_plane_only"},
		{ProtocolV2, true, "direct_reconnect_or_control_plane"},
		{ProtocolV2, false, "explicit_reassignment_or_rebuild"},
	} {
		t.Run(string(tc.version)+"_stable="+boolStr(tc.addrStable), func(t *testing.T) {
			policy := ProtocolPolicy{Version: tc.version}
			action := policy.RestartRejoinAction(tc.addrStable)
			if action != tc.expectAction {
				t.Fatalf("RestartRejoinAction(%v): got %q, want %q", tc.addrStable, action, tc.expectAction)
			}
		})
	}
}

func TestP02_VersionComparison_V15RestartAddressInstability(t *testing.T) {
	v15 := ProtocolPolicy{Version: ProtocolV15}
	v2 := ProtocolPolicy{Version: ProtocolV2}

	if got := v15.RestartRejoinAction(false); got != "control_plane_only" {
		t.Fatalf("v1.5 changed-address restart should fall back to control plane, got %q", got)
	}
	if got := v2.ChangedAddressRestartAction(true); got != "explicit_reassignment_then_catchup" {
		t.Fatalf("v2 changed-address recoverable restart should use explicit reassignment + catch-up, got %q", got)
	}
	if got := v2.ChangedAddressRestartAction(false); got != "explicit_reassignment_or_rebuild" {
		t.Fatalf("v2 changed-address unrecoverable restart should go to explicit reassignment/rebuild, got %q", got)
	}
}

func boolStr(b bool) string {
	if b {
		return "true"
	}
	return "false"
}
