package distsim

import (
	"testing"
)

// ============================================================
// Phase 02: Coordinator candidate-selection tests
// Verifies promotion ranking under mixed replica states.
// ============================================================

func TestP02_CandidateSelection_AllEqual_AlphabeticalTieBreak(t *testing.T) {
	c := NewCluster(CommitSyncQuorum, "p", "r1", "r2", "r3")
	c.CommitWrite(1)
	c.TickN(5)

	// All replicas InSync with same FlushedLSN → alphabetical tie-break.
	best := c.BestPromotionCandidate()
	if best != "r1" {
		t.Fatalf("all equal: expected r1 (alphabetical), got %q", best)
	}

	candidates := c.PromotionCandidates()
	if len(candidates) != 3 {
		t.Fatalf("expected 3 candidates, got %d", len(candidates))
	}
	for i, exp := range []string{"r1", "r2", "r3"} {
		if candidates[i].ID != exp {
			t.Fatalf("candidate[%d]: got %q, want %q", i, candidates[i].ID, exp)
		}
	}
}

func TestP02_CandidateSelection_HigherLSN_Wins(t *testing.T) {
	c := NewCluster(CommitSyncQuorum, "p", "r1", "r2", "r3")

	// Directly set FlushedLSN to simulate different progress.
	// All InSync — higher LSN wins.
	for _, id := range []string{"r1", "r2", "r3"} {
		c.Nodes[id].ReplicaState = NodeStateInSync
	}
	c.Nodes["r1"].Storage.FlushedLSN = 10
	c.Nodes["r2"].Storage.FlushedLSN = 20
	c.Nodes["r3"].Storage.FlushedLSN = 15

	best := c.BestPromotionCandidate()
	if best != "r2" {
		t.Fatalf("higher LSN: expected r2, got %q", best)
	}

	candidates := c.PromotionCandidates()
	if candidates[0].ID != "r2" || candidates[1].ID != "r3" || candidates[2].ID != "r1" {
		t.Fatalf("order: got [%s, %s, %s], want [r2, r3, r1]",
			candidates[0].ID, candidates[1].ID, candidates[2].ID)
	}
}

func TestP02_CandidateSelection_StoppedNode_Excluded(t *testing.T) {
	c := NewCluster(CommitSyncQuorum, "p", "r1", "r2")

	c.Nodes["r1"].Storage.FlushedLSN = 100
	c.Nodes["r2"].Storage.FlushedLSN = 50
	c.StopNode("r1") // highest LSN but stopped

	best := c.BestPromotionCandidate()
	if best != "r2" {
		t.Fatalf("stopped excluded: expected r2, got %q", best)
	}

	// r1 should be last in ranking (not running).
	candidates := c.PromotionCandidates()
	if candidates[0].ID != "r2" {
		t.Fatalf("first candidate should be r2, got %s", candidates[0].ID)
	}
	if candidates[1].Running {
		t.Fatal("r1 should be marked not running")
	}
}

func TestP02_CandidateSelection_InSync_Beats_CatchingUp(t *testing.T) {
	c := NewCluster(CommitSyncQuorum, "p", "r1", "r2", "r3")

	// r1: CatchingUp with highest LSN.
	c.Nodes["r1"].ReplicaState = NodeStateCatchingUp
	c.Nodes["r1"].Storage.FlushedLSN = 100

	// r2: InSync with lower LSN.
	c.Nodes["r2"].ReplicaState = NodeStateInSync
	c.Nodes["r2"].Storage.FlushedLSN = 50

	// r3: InSync with even lower LSN.
	c.Nodes["r3"].ReplicaState = NodeStateInSync
	c.Nodes["r3"].Storage.FlushedLSN = 40

	// InSync with lower LSN beats CatchingUp with higher LSN.
	best := c.BestPromotionCandidate()
	if best != "r2" {
		t.Fatalf("InSync beats CatchingUp: expected r2, got %q", best)
	}

	candidates := c.PromotionCandidates()
	// r2 (InSync, 50), r3 (InSync, 40), r1 (CatchingUp, 100)
	if candidates[0].ID != "r2" || candidates[1].ID != "r3" || candidates[2].ID != "r1" {
		t.Fatalf("order: got [%s, %s, %s]", candidates[0].ID, candidates[1].ID, candidates[2].ID)
	}
}

func TestP02_CandidateSelection_AllCatchingUp_HighestLSN_Wins(t *testing.T) {
	c := NewCluster(CommitSyncQuorum, "p", "r1", "r2", "r3")

	for _, id := range []string{"r1", "r2", "r3"} {
		c.Nodes[id].ReplicaState = NodeStateCatchingUp
	}
	c.Nodes["r1"].Storage.FlushedLSN = 30
	c.Nodes["r2"].Storage.FlushedLSN = 80
	c.Nodes["r3"].Storage.FlushedLSN = 50

	best := c.BestPromotionCandidate()
	if best != "r2" {
		t.Fatalf("all CatchingUp: expected r2 (highest LSN), got %q", best)
	}
}

func TestP02_CandidateSelection_NeedsRebuild_Skipped(t *testing.T) {
	c := NewCluster(CommitSyncQuorum, "p", "r1", "r2", "r3")

	// r1: NeedsRebuild with highest LSN.
	c.Nodes["r1"].ReplicaState = NodeStateNeedsRebuild
	c.Nodes["r1"].Storage.FlushedLSN = 100

	// r2: InSync with moderate LSN.
	c.Nodes["r2"].ReplicaState = NodeStateInSync
	c.Nodes["r2"].Storage.FlushedLSN = 50

	// r3: CatchingUp with low LSN.
	c.Nodes["r3"].ReplicaState = NodeStateCatchingUp
	c.Nodes["r3"].Storage.FlushedLSN = 20

	best := c.BestPromotionCandidate()
	if best != "r2" {
		t.Fatalf("NeedsRebuild skipped: expected r2, got %q", best)
	}

	candidates := c.PromotionCandidates()
	// r2 (InSync, 50), r3 (CatchingUp, 20), r1 (NeedsRebuild, 100)
	if candidates[0].ID != "r2" {
		t.Fatalf("first should be r2, got %s", candidates[0].ID)
	}
	if candidates[2].ID != "r1" {
		t.Fatalf("last should be r1 (NeedsRebuild), got %s", candidates[2].ID)
	}
}

func TestP02_CandidateSelection_NoRunning_ReturnsEmpty(t *testing.T) {
	c := NewCluster(CommitSyncQuorum, "p", "r1", "r2")
	c.StopNode("r1")
	c.StopNode("r2")

	best := c.BestPromotionCandidate()
	if best != "" {
		t.Fatalf("no running: expected empty, got %q", best)
	}
}

func TestP02_CandidateSelection_AfterPartition_RankingUpdates(t *testing.T) {
	c := NewCluster(CommitSyncQuorum, "p", "r1", "r2", "r3")

	c.CommitWrite(1)
	c.CommitWrite(2)
	c.TickN(5)

	// All InSync at FlushedLSN=2. Best = r1 (alphabetical).
	if best := c.BestPromotionCandidate(); best != "r1" {
		t.Fatalf("before partition: expected r1, got %q", best)
	}

	// Partition r1. Write more via p+r2+r3.
	c.Disconnect("p", "r1")
	c.Disconnect("r1", "p")
	// With 4 members (p, r1, r2, r3), quorum=3. p+r2+r3=3. OK.
	// Actually, quorum = 4/2+1=3. p+r2+r3=3. Marginal.
	c.CommitWrite(3)
	c.CommitWrite(4)
	c.CommitWrite(5)
	c.TickN(5)

	// r1 lagging, r2/r3 ahead.
	c.Nodes["r1"].ReplicaState = NodeStateCatchingUp

	// Now r2 or r3 should win (both InSync with higher LSN).
	best := c.BestPromotionCandidate()
	if best == "r1" {
		t.Fatal("after partition: r1 should not be best (CatchingUp)")
	}
	if best != "r2" {
		t.Fatalf("after partition: expected r2 (InSync, alphabetical tie-break), got %q", best)
	}
	t.Logf("after partition: best=%s", best)
}

func TestP02_CandidateSelection_MixedStates_FullRanking(t *testing.T) {
	c := NewCluster(CommitSyncQuorum, "p", "r1", "r2", "r3", "r4", "r5")

	// Set up a diverse state mix:
	// r1: InSync, LSN=50
	// r2: InSync, LSN=60 (highest InSync)
	// r3: CatchingUp, LSN=80 (highest overall but CatchingUp)
	// r4: NeedsRebuild, LSN=90 (highest but NeedsRebuild)
	// r5: stopped, LSN=100 (highest but not running)
	c.Nodes["r1"].ReplicaState = NodeStateInSync
	c.Nodes["r1"].Storage.FlushedLSN = 50
	c.Nodes["r2"].ReplicaState = NodeStateInSync
	c.Nodes["r2"].Storage.FlushedLSN = 60
	c.Nodes["r3"].ReplicaState = NodeStateCatchingUp
	c.Nodes["r3"].Storage.FlushedLSN = 80
	c.Nodes["r4"].ReplicaState = NodeStateNeedsRebuild
	c.Nodes["r4"].Storage.FlushedLSN = 90
	c.Nodes["r5"].Storage.FlushedLSN = 100
	c.StopNode("r5")

	best := c.BestPromotionCandidate()
	if best != "r2" {
		t.Fatalf("mixed states: expected r2 (InSync+highest among InSync), got %q", best)
	}

	candidates := c.PromotionCandidates()
	// Expected order: r2(InSync,60), r1(InSync,50), r3(CatchingUp,80),
	// r4(NeedsRebuild,90), r5(stopped,100)
	expected := []string{"r2", "r1", "r3", "r4", "r5"}
	for i, exp := range expected {
		if candidates[i].ID != exp {
			t.Fatalf("candidate[%d]: got %q, want %q", i, candidates[i].ID, exp)
		}
	}
	t.Logf("full ranking: %s(%s/%d) > %s(%s/%d) > %s(%s/%d) > %s(%s/%d) > %s(%s/%d)",
		candidates[0].ID, candidates[0].State, candidates[0].FlushedLSN,
		candidates[1].ID, candidates[1].State, candidates[1].FlushedLSN,
		candidates[2].ID, candidates[2].State, candidates[2].FlushedLSN,
		candidates[3].ID, candidates[3].State, candidates[3].FlushedLSN,
		candidates[4].ID, candidates[4].State, candidates[4].FlushedLSN)
}

func TestP02_CandidateSelection_AllNeedsRebuild_SafeDefaultEmpty(t *testing.T) {
	c := NewCluster(CommitSyncQuorum, "p", "r1", "r2")
	c.Nodes["r1"].ReplicaState = NodeStateNeedsRebuild
	c.Nodes["r1"].Storage.FlushedLSN = 50
	c.Nodes["r2"].ReplicaState = NodeStateNeedsRebuild
	c.Nodes["r2"].Storage.FlushedLSN = 80

	// Safe default: refuses NeedsRebuild candidates.
	safe := c.BestPromotionCandidate()
	if safe != "" {
		t.Fatalf("safe default should return empty for all-NeedsRebuild, got %q", safe)
	}
}

func TestP02_CandidateSelection_DesperationPromotion_ExplicitAPI(t *testing.T) {
	c := NewCluster(CommitSyncQuorum, "p", "r1", "r2", "r3")
	for _, id := range []string{"r1", "r2", "r3"} {
		c.Nodes[id].ReplicaState = NodeStateNeedsRebuild
	}
	c.Nodes["r1"].Storage.FlushedLSN = 10
	c.Nodes["r2"].Storage.FlushedLSN = 30
	c.Nodes["r3"].Storage.FlushedLSN = 20

	safe := c.BestPromotionCandidate()
	if safe != "" {
		t.Fatalf("safe default should return empty, got %q", safe)
	}

	desperate := c.BestPromotionCandidateDesperate()
	if desperate != "r2" {
		t.Fatalf("desperation: expected r2 (highest LSN), got %q", desperate)
	}
}

// === Candidate eligibility tests ===

func TestP02_CandidateEligibility_Running(t *testing.T) {
	c := NewCluster(CommitSyncQuorum, "p", "r1", "r2")
	c.CommitWrite(1)
	c.TickN(5)

	e := c.EvaluateCandidateEligibility("r1")
	if !e.Eligible {
		t.Fatalf("running InSync replica should be eligible, reasons: %v", e.Reasons)
	}

	c.StopNode("r1")
	e = c.EvaluateCandidateEligibility("r1")
	if e.Eligible {
		t.Fatal("stopped replica should not be eligible")
	}
	if e.Reasons[0] != "not_running" {
		t.Fatalf("expected not_running reason, got %v", e.Reasons)
	}
}

func TestP02_CandidateEligibility_EpochAlignment(t *testing.T) {
	c := NewCluster(CommitSyncQuorum, "p", "r1", "r2")
	c.CommitWrite(1)
	c.TickN(5)

	// Manually desync r1's epoch.
	c.Nodes["r1"].Epoch = c.Coordinator.Epoch - 1

	e := c.EvaluateCandidateEligibility("r1")
	if e.Eligible {
		t.Fatal("epoch-misaligned replica should not be eligible")
	}
	found := false
	for _, r := range e.Reasons {
		if r == "epoch_misaligned" {
			found = true
		}
	}
	if !found {
		t.Fatalf("expected epoch_misaligned reason, got %v", e.Reasons)
	}
}

func TestP02_CandidateEligibility_StateIneligible(t *testing.T) {
	c := NewCluster(CommitSyncQuorum, "p", "r1", "r2")
	c.CommitWrite(1)
	c.TickN(5)

	for _, state := range []ReplicaNodeState{NodeStateNeedsRebuild, NodeStateRebuilding} {
		c.Nodes["r1"].ReplicaState = state
		e := c.EvaluateCandidateEligibility("r1")
		if e.Eligible {
			t.Fatalf("%s should not be eligible", state)
		}
	}

	// CatchingUp IS eligible (data may be mostly current).
	c.Nodes["r1"].ReplicaState = NodeStateCatchingUp
	e := c.EvaluateCandidateEligibility("r1")
	if !e.Eligible {
		t.Fatalf("CatchingUp should be eligible, reasons: %v", e.Reasons)
	}
}

func TestP02_CandidateEligibility_InsufficientCommittedPrefix(t *testing.T) {
	c := NewCluster(CommitSyncQuorum, "p", "r1", "r2")
	c.CommitWrite(1)
	c.TickN(5)

	// r1 has FlushedLSN=1, CommittedLSN=1 → eligible.
	e := c.EvaluateCandidateEligibility("r1")
	if !e.Eligible {
		t.Fatalf("r1 at committed prefix should be eligible, reasons: %v", e.Reasons)
	}

	// Manually set r1 behind committed prefix.
	c.Nodes["r1"].Storage.FlushedLSN = 0
	e = c.EvaluateCandidateEligibility("r1")
	if e.Eligible {
		t.Fatal("FlushedLSN=0 with CommittedLSN=1 should not be eligible")
	}
	found := false
	for _, r := range e.Reasons {
		if r == "insufficient_committed_prefix" {
			found = true
		}
	}
	if !found {
		t.Fatalf("expected insufficient_committed_prefix reason, got %v", e.Reasons)
	}
}

func TestP02_CandidateEligibility_InSyncButLagging_Rejected(t *testing.T) {
	// Scenario from finding: r1 is InSync with correct epoch but FlushedLSN << CommittedLSN.
	// r2 is CatchingUp but has the committed prefix. r2 should be selected over r1.
	c := NewCluster(CommitSyncQuorum, "p", "r1", "r2", "r3")

	// Set committed prefix high.
	c.Coordinator.CommittedLSN = 100

	// r1: InSync, correct epoch, but FlushedLSN=1. Ineligible.
	c.Nodes["r1"].ReplicaState = NodeStateInSync
	c.Nodes["r1"].Storage.FlushedLSN = 1

	// r2: CatchingUp, correct epoch, FlushedLSN=100. Eligible.
	c.Nodes["r2"].ReplicaState = NodeStateCatchingUp
	c.Nodes["r2"].Storage.FlushedLSN = 100

	// r3: InSync, correct epoch, FlushedLSN=100. Eligible.
	c.Nodes["r3"].ReplicaState = NodeStateInSync
	c.Nodes["r3"].Storage.FlushedLSN = 100

	// r1 is ineligible despite being InSync.
	e1 := c.EvaluateCandidateEligibility("r1")
	if e1.Eligible {
		t.Fatal("r1 (InSync, FlushedLSN=1, CommittedLSN=100) should be ineligible")
	}

	// r2 and r3 are eligible.
	e2 := c.EvaluateCandidateEligibility("r2")
	if !e2.Eligible {
		t.Fatalf("r2 should be eligible, reasons: %v", e2.Reasons)
	}

	// BestPromotionCandidate should pick r3 (InSync with prefix) over r2 (CatchingUp).
	best := c.BestPromotionCandidate()
	if best != "r3" {
		t.Fatalf("expected r3 (InSync+prefix), got %q", best)
	}

	// r1 must NOT be in the eligible list at all.
	eligible := c.EligiblePromotionCandidates()
	for _, pc := range eligible {
		if pc.ID == "r1" {
			t.Fatal("r1 should not appear in eligible candidates")
		}
	}
	t.Logf("committed-prefix gate: r1(InSync/flushed=1) rejected, r3(InSync/flushed=100) selected")
}

func TestP02_CandidateEligibility_EligiblePromotionCandidates(t *testing.T) {
	c := NewCluster(CommitSyncQuorum, "p", "r1", "r2", "r3", "r4")
	c.CommitWrite(1)
	c.TickN(5)

	// r1: InSync, eligible
	// r2: NeedsRebuild, ineligible
	c.Nodes["r2"].ReplicaState = NodeStateNeedsRebuild
	// r3: stopped, ineligible
	c.StopNode("r3")
	// r4: epoch misaligned, ineligible
	c.Nodes["r4"].Epoch = 0

	eligible := c.EligiblePromotionCandidates()
	if len(eligible) != 1 {
		t.Fatalf("expected 1 eligible candidate, got %d", len(eligible))
	}
	if eligible[0].ID != "r1" {
		t.Fatalf("expected r1 as only eligible, got %s", eligible[0].ID)
	}

	// BestPromotionCandidate uses eligibility.
	best := c.BestPromotionCandidate()
	if best != "r1" {
		t.Fatalf("BestPromotionCandidate should return r1, got %q", best)
	}
}
