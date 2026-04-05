package masterv2

import "testing"

func TestSelectPromotionCandidate_PrefersHighestCommittedLSN(t *testing.T) {
	master := New(Config{})
	selected, err := master.SelectPromotionCandidate([]PromotionQueryResponse{
		{
			VolumeName:   "vol-a",
			NodeID:       "node-b",
			CommittedLSN: 12,
			WALHeadLSN:   20,
			Eligible:     true,
		},
		{
			VolumeName:   "vol-a",
			NodeID:       "node-a",
			CommittedLSN: 15,
			WALHeadLSN:   16,
			Eligible:     true,
		},
	})
	if err != nil {
		t.Fatalf("select candidate: %v", err)
	}
	if selected.NodeID != "node-a" {
		t.Fatalf("selected node=%q, want node-a", selected.NodeID)
	}
}

func TestSelectPromotionCandidate_UsesWalHeadAsTiebreaker(t *testing.T) {
	master := New(Config{})
	selected, err := master.SelectPromotionCandidate([]PromotionQueryResponse{
		{
			VolumeName:   "vol-a",
			NodeID:       "node-a",
			CommittedLSN: 15,
			WALHeadLSN:   16,
			Eligible:     true,
		},
		{
			VolumeName:   "vol-a",
			NodeID:       "node-b",
			CommittedLSN: 15,
			WALHeadLSN:   19,
			Eligible:     true,
		},
	})
	if err != nil {
		t.Fatalf("select candidate: %v", err)
	}
	if selected.NodeID != "node-b" {
		t.Fatalf("selected node=%q, want node-b", selected.NodeID)
	}
}

func TestSelectPromotionCandidate_RejectsWhenNoEligibleCandidates(t *testing.T) {
	master := New(Config{})
	_, err := master.SelectPromotionCandidate([]PromotionQueryResponse{
		{NodeID: "node-a", Eligible: false, Reason: "needs_rebuild"},
		{NodeID: "node-b", Eligible: false, Reason: "epoch_mismatch"},
	})
	if err == nil {
		t.Fatal("expected error when no eligible candidates")
	}
}

func TestAuthorizePromotion_ReassignsPrimaryAndAdvancesEpoch(t *testing.T) {
	master := New(Config{})
	if err := master.DeclarePrimary(VolumeSpec{
		Name:          "vol-a",
		Path:          "/tmp/vol-a.blk",
		PrimaryNodeID: "node-a",
	}); err != nil {
		t.Fatalf("declare primary: %v", err)
	}

	assign, err := master.AuthorizePromotion("vol-a", []PromotionQueryResponse{
		{
			VolumeName:   "vol-a",
			NodeID:       "node-b",
			CommittedLSN: 18,
			WALHeadLSN:   19,
			Eligible:     true,
		},
		{
			VolumeName:   "vol-a",
			NodeID:       "node-c",
			CommittedLSN: 17,
			WALHeadLSN:   20,
			Eligible:     true,
		},
	})
	if err != nil {
		t.Fatalf("authorize promotion: %v", err)
	}
	if assign.NodeID != "node-b" {
		t.Fatalf("assignment node=%q, want node-b", assign.NodeID)
	}
	if assign.Epoch != 2 {
		t.Fatalf("assignment epoch=%d, want 2", assign.Epoch)
	}

	view, ok := master.Volume("vol-a")
	if !ok {
		t.Fatal("master view missing")
	}
	if view.PrimaryNodeID != "node-b" {
		t.Fatalf("view primary=%q, want node-b", view.PrimaryNodeID)
	}
	if view.DesiredEpoch != 2 {
		t.Fatalf("view desired epoch=%d, want 2", view.DesiredEpoch)
	}
}

func TestAuthorizePromotion_RejectsMismatchedVolume(t *testing.T) {
	master := New(Config{})
	if err := master.DeclarePrimary(VolumeSpec{
		Name:          "vol-a",
		Path:          "/tmp/vol-a.blk",
		PrimaryNodeID: "node-a",
	}); err != nil {
		t.Fatalf("declare primary: %v", err)
	}

	_, err := master.AuthorizePromotion("vol-a", []PromotionQueryResponse{
		{
			VolumeName:   "vol-b",
			NodeID:       "node-b",
			CommittedLSN: 18,
			WALHeadLSN:   19,
			Eligible:     true,
		},
	})
	if err == nil {
		t.Fatal("expected mismatched volume error")
	}
}
