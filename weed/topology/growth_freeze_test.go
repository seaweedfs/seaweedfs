package topology

import (
	"errors"
	"testing"
	"time"

	"github.com/seaweedfs/raft"
)

// appliedRaftServer answers as leader and records each command Do applies.
type appliedRaftServer struct {
	raft.Server
	applied chan raft.Command
}

func (appliedRaftServer) State() string { return raft.Leader }

func (s appliedRaftServer) Do(c raft.Command) (interface{}, error) {
	s.applied <- c
	return nil, nil
}

type failingRaftServer struct {
	raft.Server
}

func (failingRaftServer) State() string { return raft.Leader }

func (failingRaftServer) Do(raft.Command) (interface{}, error) {
	return nil, errors.New("not the leader")
}

func TestGrowthFreezeOnTopologyMismatch(t *testing.T) {
	topo := NewTopology("topo", nil, 128, 3, false)
	topo.SetGrowthFreezeConfig(true, 0)
	topo.SetTopologyId("cluster-a")

	topo.CheckReportedTopologyId("cluster-a")
	if topo.IsGrowthFrozen() {
		t.Fatal("matching topology id should not freeze growth")
	}

	topo.CheckReportedTopologyId("")
	topo.CheckReportedTopologyId("cluster-b")
	if !topo.IsGrowthFrozen() {
		t.Fatal("mismatching topology id should freeze growth")
	}

	if !topo.ResumeVolumeGrowth() {
		t.Fatal("resume should report the frozen state")
	}
	if topo.IsGrowthFrozen() {
		t.Fatal("growth should be unfrozen after resume")
	}
	if topo.ResumeVolumeGrowth() {
		t.Fatal("second resume should report no frozen state")
	}
}

func TestGrowthFreezeDisabled(t *testing.T) {
	topo := NewTopology("topo", nil, 128, 3, false)
	topo.SetGrowthFreezeConfig(false, 0)
	topo.SetTopologyId("cluster-a")

	topo.CheckReportedTopologyId("cluster-b")
	if topo.IsGrowthFrozen() {
		t.Fatal("freeze_on_topology_mismatch=false should not freeze")
	}
}

func TestGrowthFreezeAutoRecovery(t *testing.T) {
	topo := NewTopology("topo", nil, 128, 3, false)
	topo.SetGrowthFreezeConfig(true, 50*time.Millisecond)
	topo.SetTopologyId("cluster-a")

	topo.CheckReportedTopologyId("cluster-b")
	if !topo.IsGrowthFrozen() {
		t.Fatal("mismatching topology id should freeze growth")
	}
	time.Sleep(60 * time.Millisecond)
	if topo.IsGrowthFrozen() {
		t.Fatal("growth should auto-resume past the recovery wait")
	}
}

func TestGrowthFreezeAutoRecoveryReplicates(t *testing.T) {
	applied := make(chan raft.Command, 2)
	topo := NewTopology("topo", nil, 128, 3, false)
	topo.RaftServer = appliedRaftServer{applied: applied}
	topo.SetGrowthFreezeConfig(true, 50*time.Millisecond)
	topo.SetTopologyId("cluster-a")

	topo.CheckReportedTopologyId("cluster-b")
	if !topo.IsGrowthFrozen() {
		t.Fatal("mismatching topology id should freeze growth")
	}
	cmd := (<-applied).(*MaxVolumeIdCommand)
	if cmd.GrowthFrozen == nil || !*cmd.GrowthFrozen {
		t.Fatal("the freeze should be replicated")
	}

	time.Sleep(60 * time.Millisecond)
	if topo.IsGrowthFrozen() {
		t.Fatal("growth should auto-resume past the recovery wait")
	}
	select {
	case c := <-applied:
		lift := c.(*MaxVolumeIdCommand)
		if lift.GrowthFrozen == nil || *lift.GrowthFrozen {
			t.Fatal("the auto unfreeze should be replicated like volume.growth.resume")
		}
		if lift.GrowthUnfreezeFor != cmd.GrowthFrozenAt {
			t.Fatal("the unfreeze should be bound to the freeze instant it lifts")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("auto unfreeze was not replicated; a restart would re-apply the freeze")
	}
}

// A repeat mismatch while already frozen must keep the generation the
// replicated freeze carries: re-stamping locally would bind every later
// lift to a generation followers never saw, and the freeze would return on
// replay.
func TestRepeatMismatchKeepsFreezeGeneration(t *testing.T) {
	topo := NewTopology("topo", nil, 128, 3, false)
	topo.SetGrowthFreezeConfig(true, 0)
	topo.SetTopologyId("cluster-a")

	topo.CheckReportedTopologyId("cluster-b")
	gen := topo.growthFrozenState.Load()
	if gen == 0 {
		t.Fatal("mismatching topology id should freeze growth")
	}
	topo.CheckReportedTopologyId("cluster-c")
	if topo.growthFrozenState.Load() != gen {
		t.Fatal("a repeat mismatch must not re-stamp the armed freeze")
	}
}

// Snapshots persist the flag and the generation together, so the pair must
// come from one read: a lifted freeze reports (false, 0) and an armed one
// reports its generation.
func TestGrowthFreezeStateConsistentPair(t *testing.T) {
	topo := NewTopology("topo", nil, 128, 3, false)
	topo.SetGrowthFreezeConfig(true, 0)

	topo.SetGrowthFrozen(true, time.Now().UnixNano(), 0)
	armed := topo.growthFrozenState.Load()
	if frozen, at := topo.GrowthFreezeState(); !frozen || at != armed {
		t.Fatalf("armed freeze should report (true, %d), got (%v, %d)", armed, frozen, at)
	}
	topo.ResumeVolumeGrowth()
	if frozen, at := topo.GrowthFreezeState(); frozen || at != 0 {
		t.Fatalf("lifted freeze should report (false, 0), got (%v, %d)", frozen, at)
	}
}

func TestReplicateGrowthUnfreezeSkipsRefreeze(t *testing.T) {
	applied := make(chan raft.Command, 1)
	topo := NewTopology("topo", nil, 128, 3, false)
	topo.RaftServer = appliedRaftServer{applied: applied}

	topo.SetGrowthFrozen(true, 1000, 0)
	topo.SetGrowthFrozen(true, 2000, 0)
	topo.replicateGrowthUnfreeze(1000)
	select {
	case <-applied:
		t.Fatal("a stale unfreeze must not lift a freeze that landed after it")
	default:
	}
	if topo.growthFrozenState.Load() != 2000 {
		t.Fatal("the newer freeze must stay armed")
	}
}

func TestReplicateGrowthUnfreezeRearmsOnFailure(t *testing.T) {
	topo := NewTopology("topo", nil, 128, 3, false)
	topo.RaftServer = failingRaftServer{}

	topo.SetGrowthFrozen(true, 1000, 0)
	topo.growthFrozenState.CompareAndSwap(1000, 0) // the local lift
	topo.replicateGrowthUnfreeze(1000)
	if topo.growthFrozenState.Load() != 1000 {
		t.Fatal("a failed replication must re-arm the freeze so a later read retries the lift")
	}
}

func TestReplicateGrowthUnfreezeDoesNotRearmAfterResume(t *testing.T) {
	topo := NewTopology("topo", nil, 128, 3, false)

	topo.SetGrowthFrozen(true, 1000, 0)
	topo.SetGrowthFrozen(true, 2000, 0)
	topo.ResumeVolumeGrowth()
	topo.replicateGrowthUnfreeze(1000)
	if topo.growthFrozenState.Load() != 0 {
		t.Fatal("a superseded lift must not re-arm the freeze an operator resumed")
	}
}

func TestCheckReportedTopologyIdBeforeTopologyIdKnown(t *testing.T) {
	topo := NewTopology("topo", nil, 128, 3, false)
	topo.SetGrowthFreezeConfig(true, 0)

	topo.CheckReportedTopologyId("cluster-b")
	if topo.IsGrowthFrozen() {
		t.Fatal("empty local topology id should not trigger the freeze")
	}

	topo.SetTopologyId("cluster-c")
	topo.SetTopologyInitPending(false)
	if !topo.IsGrowthFrozen() {
		t.Fatal("a report stashed before the topology id was known should still freeze growth")
	}
}

func TestReportedTopologyIdStashDoesNotFreezeOnMatch(t *testing.T) {
	topo := NewTopology("topo", nil, 128, 3, false)
	topo.SetGrowthFreezeConfig(true, 0)

	topo.CheckReportedTopologyId("cluster-a")
	topo.SetTopologyId("cluster-a")
	topo.SetTopologyInitPending(false)
	if topo.IsGrowthFrozen() {
		t.Fatal("a stashed report matching the minted id should not freeze growth")
	}
}

func TestGrowthFreezeSurvivesMirrorRestore(t *testing.T) {
	dir := t.TempDir()
	file := dir + "/max_volume_id"

	frozen := NewTopology("topo", nil, 128, 3, false)
	frozen.SetGrowthFreezeConfig(true, 0)
	frozen.SetMaxVolumeIdFile(file)
	frozen.SetTopologyId("cluster-a")
	frozen.CheckReportedTopologyId("cluster-b")
	if !frozen.IsGrowthFrozen() {
		t.Fatal("expected frozen growth")
	}
	if err := frozen.PersistMaxVolumeId(42); err != nil {
		t.Fatalf("persist: %v", err)
	}

	restarted := NewTopology("topo", nil, 128, 3, false)
	restarted.SetGrowthFreezeConfig(true, 0)
	restarted.SetMaxVolumeIdFile(file)
	if !restarted.IsGrowthFrozen() {
		t.Fatal("freeze flag should survive a mirror restore")
	}
	if !restarted.ResumeVolumeGrowth() {
		t.Fatal("restored freeze should be resumable")
	}
}

func TestGrowthFreezeCommandApply(t *testing.T) {
	topo := NewTopology("topo", nil, 128, 3, false)
	topo.SetGrowthFreezeConfig(true, 0)

	frozen := true
	topo.SetGrowthFrozen(frozen, 0, 0)
	if !topo.IsGrowthFrozen() {
		t.Fatal("replicated freeze should set the flag")
	}
	topo.SetGrowthFrozen(false, 0, 0)
	if topo.IsGrowthFrozen() {
		t.Fatal("replicated unfreeze should clear the flag")
	}
}

// Freeze entries written before the instant field existed replay as their
// own stable generation: a lift bound to it still applies, a lift bound to a
// different generation cannot clear it, and the recovery window reads as
// already elapsed — a replay-time stamp would shadow every committed
// unfreeze and re-apply the freeze on each restart.
func TestLegacyFreezeReplayStaysAgeless(t *testing.T) {
	topo := NewTopology("topo", nil, 128, 3, false)
	topo.SetGrowthFreezeConfig(true, time.Minute)

	topo.SetGrowthFrozen(true, 0, 0)
	topo.SetGrowthFrozen(false, 0, 1234)
	if topo.growthFrozenState.Load() != -1 {
		t.Fatal("a lift bound to a different generation must not clear the ageless freeze")
	}
	topo.SetGrowthFrozen(false, 0, -1)
	if topo.growthFrozenState.Load() != 0 {
		t.Fatal("a lift bound to the ageless generation should clear it")
	}

	topo.SetGrowthFrozen(true, 0, 0)
	if topo.IsGrowthFrozen() {
		t.Fatal("an ageless freeze should read its recovery window as elapsed")
	}
}

// A lift bound to a freeze instant must not erase a newer freeze, so a stale
// unfreeze cannot overwrite a freeze that committed first but applied later.
func TestGrowthUnfreezeBoundToFreezeInstant(t *testing.T) {
	topo := NewTopology("topo", nil, 128, 3, false)

	topo.SetGrowthFrozen(true, 1000, 0)
	topo.SetGrowthFrozen(false, 0, 999)
	if !topo.IsGrowthFrozen() {
		t.Fatal("a lift for an older freeze instant must not clear a newer freeze")
	}
	topo.SetGrowthFrozen(false, 0, 1000)
	if topo.IsGrowthFrozen() {
		t.Fatal("a lift bound to the current freeze instant should clear it")
	}
}

func TestAllocationDelay(t *testing.T) {
	topo := NewTopology("topo", nil, 128, 3, false)

	if wait := topo.allocationDelay(); wait > 0 {
		t.Fatalf("unexpected deferral: %v", wait)
	}
	topo.DeferAllocation(50 * time.Millisecond)
	if wait := topo.allocationDelay(); wait <= 0 {
		t.Fatal("expected a positive deferral after DeferAllocation")
	}
	topo.DeferAllocation(-time.Second)
	if wait := topo.allocationDelay(); wait > 0 {
		t.Fatalf("expired deferral should not delay: %v", wait)
	}
}

func TestAllocationDelayEarlyRelease(t *testing.T) {
	topo := NewTopology("topo", nil, 128, 1, false)

	topo.DeferAllocation(time.Minute)
	wait := topo.allocationDelay()
	if wait <= 0 || wait > 2*time.Second {
		t.Fatalf("quiet window should bound the deferral near two pulses, got %v", wait)
	}

	time.Sleep(2100 * time.Millisecond)
	if wait := topo.allocationDelay(); wait > 0 {
		t.Fatalf("two quiet pulses should release the deferral early, got %v", wait)
	}
}

func TestAllocationDelayExtendedByEvidence(t *testing.T) {
	topo := NewTopology("topo", nil, 128, 1, false)

	topo.DeferAllocation(time.Minute)
	time.Sleep(1100 * time.Millisecond)
	topo.NoteAllocationEvidence()
	time.Sleep(1200 * time.Millisecond)
	if wait := topo.allocationDelay(); wait <= 0 {
		t.Fatal("evidence should extend the deferral past the original quiet deadline")
	}
}

func TestAllocationDeferralDuration(t *testing.T) {
	topo := NewTopology("topo", nil, 128, 3, false)

	if d := topo.AllocationDeferralDuration(); d != topo.WarmupDuration() {
		t.Fatalf("unset deferral should default to the warmup window, got %v", d)
	}
	topo.SetAllocationDeferral(0)
	if d := topo.AllocationDeferralDuration(); d != 0 {
		t.Fatalf("zero deferral should disable the hold, got %v", d)
	}
	topo.SetAllocationDeferral(2 * time.Second)
	if d := topo.AllocationDeferralDuration(); d != 2*time.Second {
		t.Fatalf("configured deferral should win, got %v", d)
	}
	topo.SetAllocationDeferral(-1)
	if d := topo.AllocationDeferralDuration(); d != topo.WarmupDuration() {
		t.Fatalf("negative deferral should restore the default, got %v", d)
	}
}

func TestAllocationDeferralReleasedByInventory(t *testing.T) {
	topo := NewTopology("topo", nil, 128, 3, false)
	topo.ReleaseAllocationOnFirstReport("10.0.0.1:8080")

	topo.DeferAllocation(time.Minute)
	topo.NoteInventoryApplied("10.0.0.1:8080")
	if wait := topo.allocationDelay(); wait > 0 {
		t.Fatalf("applied inventory should lift the deferral, still waiting %v", wait)
	}

	fresh := NewTopology("topo", nil, 128, 3, false)
	fresh.ReleaseAllocationOnFirstReport("10.0.0.1:8080")
	fresh.NoteInventoryApplied("10.0.0.1:8080")
	fresh.DeferAllocation(time.Minute)
	if wait := fresh.allocationDelay(); wait > 0 {
		t.Fatalf("a report applied before the mint should keep the hold unarmed, got %v", wait)
	}

	walled := NewTopology("topo", nil, 128, 3, false)
	walled.DeferAllocation(time.Minute)
	walled.NoteInventoryApplied("10.0.0.1:8080")
	if wait := walled.allocationDelay(); wait <= 0 {
		t.Fatal("without opt-in an applied inventory must not lift the deferral")
	}
}

func TestAllocationDeferralReleasedByExpectedReporter(t *testing.T) {
	topo := NewTopology("topo", nil, 128, 3, false)
	topo.ReleaseAllocationOnReport("10.0.0.1:8080")

	if !topo.AdmitVolumeReporter("10.0.0.2:8080") {
		t.Fatal("a named expected reporter must not refuse other reporters")
	}
	topo.DeferAllocation(time.Minute)
	topo.NoteInventoryApplied("10.0.0.2:8080")
	if wait := topo.allocationDelay(); wait <= 0 {
		t.Fatal("a foreign reporter's inventory must not lift the deferral")
	}
	topo.NoteInventoryApplied("10.0.0.1:8080")
	wait := topo.allocationDelay()
	if wait <= 0 || wait > 3*time.Second {
		t.Fatalf("the expected reporter's inventory should collapse the deferral to a confirm pulse, got %v", wait)
	}
}

func TestAllocationDeferralConfirmPulseAfterAppliedReport(t *testing.T) {
	topo := NewTopology("topo", nil, 128, 3, false)
	topo.ReleaseAllocationOnReport("10.0.0.1:8080")

	topo.DeferAllocation(time.Minute)
	topo.NoteInventoryApplied("10.0.0.1:8080")
	if wait := topo.allocationDelay(); wait <= 0 || wait > 3*time.Second {
		t.Fatalf("an unpinned release should leave a confirm pulse, got %v", wait)
	}

	// A later mint re-arms the confirm pulse rather than the full warmup or
	// nothing at all: outside reporters may still carry prior cluster state.
	topo.DeferAllocation(time.Minute)
	if wait := topo.allocationDelay(); wait <= 0 || wait > 3*time.Second {
		t.Fatalf("a re-mint after the applied report should arm the confirm pulse, got %v", wait)
	}
}

func TestQueuedTopologyReportFreezesBeforeGateOpens(t *testing.T) {
	topo := NewTopology("topo", nil, 128, 3, false)
	topo.SetGrowthFreezeConfig(true, 0)
	topo.SetTopologyInitPending(true)

	// A report arriving while the id is still being minted is stashed.
	topo.CheckReportedTopologyId("cluster-a")
	topo.SetTopologyId("cluster-b")

	topo.SetTopologyInitPending(false)
	if topo.topologyInitPending.Load() {
		t.Fatal("init gate should be open after pending=false")
	}
	if !topo.IsGrowthFrozen() {
		t.Fatal("a stashed mismatch must freeze growth before the gate opens")
	}
}

func TestSingleReporterAdmission(t *testing.T) {
	topo := NewTopology("topo", nil, 128, 3, false)
	if !topo.AdmitVolumeReporter("10.0.0.1:8080") {
		t.Fatal("without opt-in any reporter is admitted")
	}
	topo.ReleaseAllocationOnFirstReport("10.0.0.1:8080")
	if !topo.AdmitVolumeReporter("10.0.0.1:8080") {
		t.Fatal("the pinned reporter should be admitted")
	}
	if topo.AdmitVolumeReporter("10.0.0.2:8080") {
		t.Fatal("a foreign reporter should be refused")
	}
}
