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
