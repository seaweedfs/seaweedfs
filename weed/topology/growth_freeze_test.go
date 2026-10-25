package topology

import (
	"testing"
	"time"
)

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
	topo.SetGrowthFrozen(frozen)
	if !topo.IsGrowthFrozen() {
		t.Fatal("replicated freeze should set the flag")
	}
	unfrozen := false
	topo.SetGrowthFrozen(unfrozen)
	if topo.IsGrowthFrozen() {
		t.Fatal("replicated unfreeze should clear the flag")
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
	topo.ReleaseAllocationOnFirstReport()

	topo.DeferAllocation(time.Minute)
	topo.NoteInventoryApplied()
	if wait := topo.allocationDelay(); wait > 0 {
		t.Fatalf("applied inventory should lift the deferral, still waiting %v", wait)
	}

	fresh := NewTopology("topo", nil, 128, 3, false)
	fresh.ReleaseAllocationOnFirstReport()
	fresh.NoteInventoryApplied()
	fresh.DeferAllocation(time.Minute)
	if wait := fresh.allocationDelay(); wait > 0 {
		t.Fatalf("a report applied before the mint should keep the hold unarmed, got %v", wait)
	}

	walled := NewTopology("topo", nil, 128, 3, false)
	walled.DeferAllocation(time.Minute)
	walled.NoteInventoryApplied()
	if wait := walled.allocationDelay(); wait <= 0 {
		t.Fatal("without opt-in an applied inventory must not lift the deferral")
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
