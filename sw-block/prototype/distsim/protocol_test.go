package distsim

import "testing"

func TestProtocolV1CannotAttemptCatchup(t *testing.T) {
	p := ProtocolPolicy{Version: ProtocolV1}
	if p.CanAttemptCatchup(true) {
		t.Fatal("v1 should not expose meaningful catch-up path")
	}
}

func TestProtocolV15CatchupDependsOnStableAddress(t *testing.T) {
	p := ProtocolPolicy{Version: ProtocolV15}
	if !p.CanAttemptCatchup(true) {
		t.Fatal("v1.5 should allow catch-up when address is stable")
	}
	if p.CanAttemptCatchup(false) {
		t.Fatal("v1.5 should not assume reconnect with changed address")
	}
}

func TestProtocolV2AllowsCatchupByPolicy(t *testing.T) {
	p := ProtocolPolicy{Version: ProtocolV2}
	if !p.CanAttemptCatchup(true) || !p.CanAttemptCatchup(false) {
		t.Fatal("v2 policy should allow catch-up attempt subject to explicit recoverability checks")
	}
}

func TestProtocolBriefDisconnectActions(t *testing.T) {
	if got := (ProtocolPolicy{Version: ProtocolV1}).BriefDisconnectAction(true, true); got != "degrade_or_rebuild" {
		t.Fatalf("v1 brief-disconnect action = %s", got)
	}
	if got := (ProtocolPolicy{Version: ProtocolV15}).BriefDisconnectAction(true, true); got != "catchup_if_history_survives" {
		t.Fatalf("v1.5 brief-disconnect action = %s", got)
	}
	if got := (ProtocolPolicy{Version: ProtocolV15}).BriefDisconnectAction(false, true); got != "stall_or_control_plane_recovery" {
		t.Fatalf("v1.5 changed-address brief-disconnect action = %s", got)
	}
	if got := (ProtocolPolicy{Version: ProtocolV2}).BriefDisconnectAction(true, false); got != "explicit_rebuild" {
		t.Fatalf("v2 unrecoverable brief-disconnect action = %s", got)
	}
	if got := (ProtocolPolicy{Version: ProtocolV2}).BriefDisconnectAction(false, true); got != "reserved_catchup" {
		t.Fatalf("v2 recoverable brief-disconnect action = %s", got)
	}
}

func TestProtocolTailChasingActions(t *testing.T) {
	if got := (ProtocolPolicy{Version: ProtocolV1}).TailChasingAction(false); got != "degrade" {
		t.Fatalf("v1 tail-chasing action = %s", got)
	}
	if got := (ProtocolPolicy{Version: ProtocolV15}).TailChasingAction(false); got != "stall_or_rebuild" {
		t.Fatalf("v1.5 tail-chasing action = %s", got)
	}
	if got := (ProtocolPolicy{Version: ProtocolV2}).TailChasingAction(false); got != "abort_to_rebuild" {
		t.Fatalf("v2 tail-chasing action = %s", got)
	}
}

func TestProtocolRestartRejoinActions(t *testing.T) {
	if got := (ProtocolPolicy{Version: ProtocolV1}).RestartRejoinAction(true); got != "control_plane_only" {
		t.Fatalf("v1 restart action = %s", got)
	}
	if got := (ProtocolPolicy{Version: ProtocolV15}).RestartRejoinAction(false); got != "control_plane_only" {
		t.Fatalf("v1.5 changed-address restart action = %s", got)
	}
	if got := (ProtocolPolicy{Version: ProtocolV2}).RestartRejoinAction(false); got != "explicit_reassignment_or_rebuild" {
		t.Fatalf("v2 changed-address restart action = %s", got)
	}
}

func TestProtocolChangedAddressRestartActions(t *testing.T) {
	if got := (ProtocolPolicy{Version: ProtocolV1}).ChangedAddressRestartAction(true); got != "control_plane_only" {
		t.Fatalf("v1 changed-address restart action = %s", got)
	}
	if got := (ProtocolPolicy{Version: ProtocolV15}).ChangedAddressRestartAction(true); got != "control_plane_only" {
		t.Fatalf("v1.5 changed-address restart action = %s", got)
	}
	if got := (ProtocolPolicy{Version: ProtocolV2}).ChangedAddressRestartAction(true); got != "explicit_reassignment_then_catchup" {
		t.Fatalf("v2 recoverable changed-address restart action = %s", got)
	}
	if got := (ProtocolPolicy{Version: ProtocolV2}).ChangedAddressRestartAction(false); got != "explicit_reassignment_or_rebuild" {
		t.Fatalf("v2 unrecoverable changed-address restart action = %s", got)
	}
}
