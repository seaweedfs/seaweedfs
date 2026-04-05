package volumev2

import (
	"testing"

	"github.com/seaweedfs/seaweedfs/sw-block/runtime/masterv2"
	"github.com/seaweedfs/seaweedfs/sw-block/runtime/protocolv2"
)

func TestPrimaryLoss_ReconstructsBoundedTruthFromReplicaSummaries(t *testing.T) {
	master := masterv2.New(masterv2.Config{})
	candidate, err := master.SelectPromotionCandidate([]masterv2.PromotionQueryResponse{
		{
			VolumeName:   "vol-a",
			NodeID:       "node-b",
			CommittedLSN: 15,
			WALHeadLSN:   18,
			Eligible:     true,
		},
		{
			VolumeName:   "vol-a",
			NodeID:       "node-c",
			CommittedLSN: 12,
			WALHeadLSN:   13,
			Eligible:     true,
		},
	})
	if err != nil {
		t.Fatalf("select candidate: %v", err)
	}

	truth, err := ReconstructPrimaryTruth(candidate.NodeID, []protocolv2.ReplicaSummaryResponse{
		{
			VolumeName:    "vol-a",
			NodeID:        "node-b",
			Epoch:         4,
			Role:          "replica",
			Mode:          "replica_ready",
			CommittedLSN:  15,
			DurableLSN:    15,
			CheckpointLSN: 10,
			TargetLSN:     40,
			AchievedLSN:   30,
			RecoveryPhase: "catching_up",
			LastBarrierOK: true,
			Eligible:      true,
		},
		{
			VolumeName:    "vol-a",
			NodeID:        "node-c",
			Epoch:         4,
			Role:          "replica",
			Mode:          "replica_ready",
			CommittedLSN:  12,
			DurableLSN:    12,
			CheckpointLSN: 8,
			TargetLSN:     40,
			AchievedLSN:   20,
			RecoveryPhase: "catching_up",
			LastBarrierOK: true,
			Eligible:      true,
		},
	})
	if err != nil {
		t.Fatalf("reconstruct truth: %v", err)
	}

	if truth.PrimaryNodeID != "node-b" {
		t.Fatalf("primary_node=%q, want node-b", truth.PrimaryNodeID)
	}
	if truth.CommittedLSN != 15 {
		t.Fatalf("committed_lsn=%d, want 15", truth.CommittedLSN)
	}
	if truth.DurableLSN != 15 {
		t.Fatalf("durable_lsn=%d, want 15", truth.DurableLSN)
	}
	if truth.CheckpointLSN != 10 {
		t.Fatalf("checkpoint_lsn=%d, want 10", truth.CheckpointLSN)
	}
	if truth.TargetLSN != 40 {
		t.Fatalf("target_lsn=%d, want 40", truth.TargetLSN)
	}
	if truth.AchievedLSN != 20 {
		t.Fatalf("achieved_lsn=%d, want 20", truth.AchievedLSN)
	}
	if truth.RecoveryPhase != "catching_up" {
		t.Fatalf("recovery_phase=%q, want catching_up", truth.RecoveryPhase)
	}
	if truth.Degraded {
		t.Fatalf("unexpected degraded truth: %+v", truth)
	}
}

func TestPrimaryLoss_ReconstructionFailsClosedOnMismatchAndNeedsRebuild(t *testing.T) {
	truth, err := ReconstructPrimaryTruth("node-b", []protocolv2.ReplicaSummaryResponse{
		{
			VolumeName:        "vol-a",
			NodeID:            "node-b",
			Epoch:             5,
			Role:              "replica",
			Mode:              "replica_ready",
			CommittedLSN:      15,
			DurableLSN:        15,
			CheckpointLSN:     10,
			RecoveryPhase:     "idle",
			LastBarrierOK:     true,
			Eligible:          true,
			LastBarrierReason: "",
		},
		{
			VolumeName:        "vol-a",
			NodeID:            "node-c",
			Epoch:             4,
			Role:              "replica",
			Mode:              "needs_rebuild",
			CommittedLSN:      12,
			DurableLSN:        10,
			CheckpointLSN:     8,
			RecoveryPhase:     "needs_rebuild",
			LastBarrierOK:     false,
			LastBarrierReason: "timeout",
			Eligible:          false,
			Reason:            "needs_rebuild",
		},
	})
	if err != nil {
		t.Fatalf("reconstruct truth: %v", err)
	}
	if !truth.Degraded {
		t.Fatalf("expected degraded truth: %+v", truth)
	}
	if !truth.NeedsRebuild {
		t.Fatalf("expected needs_rebuild truth: %+v", truth)
	}
	if truth.RecoveryPhase != "needs_rebuild" {
		t.Fatalf("recovery_phase=%q, want needs_rebuild", truth.RecoveryPhase)
	}
}
