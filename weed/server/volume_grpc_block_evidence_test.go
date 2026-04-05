package weed_server

import (
	"context"
	"fmt"
	"testing"

	engine "github.com/seaweedfs/seaweedfs/sw-block/engine/replication"
	"github.com/seaweedfs/seaweedfs/weed/storage/blockvol"
)

func TestBlockService_QueryBlockPromotionEvidence_ReturnsLiveFacts(t *testing.T) {
	bs := newTestBlockServiceDirect(t)
	path := createTestVolDirect(t, bs, "evidence-live")

	vol, ok := bs.blockStore.GetBlockVolume(path)
	if !ok {
		t.Fatal("volume not found")
	}
	if err := vol.HandleAssignment(5, blockvol.RolePrimary, 30000); err != nil {
		t.Fatalf("handle assignment: %v", err)
	}
	data := make([]byte, 4096)
	data[0] = 0xAB
	if err := vol.WriteLBA(0, data); err != nil {
		t.Fatalf("write: %v", err)
	}

	// Inject V2 core projection so evidence uses engine-derived facts.
	walHead := vol.Status().WALHeadLSN
	bs.coreProjMu.Lock()
	bs.coreProj[path] = engine.PublicationProjection{
		Mode: engine.ModeView{Name: engine.ModePublishHealthy},
		Boundary: engine.BoundaryView{
			CommittedLSN: walHead,
		},
	}
	bs.coreProjMu.Unlock()

	ev, err := bs.QueryBlockPromotionEvidence(path, 0)
	if err != nil {
		t.Fatalf("query evidence: %v", err)
	}
	if ev.Epoch != 5 {
		t.Fatalf("epoch=%d, want 5", ev.Epoch)
	}
	if ev.WALHeadLSN == 0 {
		t.Fatal("wal_head_lsn should be >0 after write")
	}
	if ev.CommittedLSN == 0 {
		t.Fatal("committed_lsn should be >0 from core projection")
	}
	if ev.HealthScore <= 0 {
		t.Fatalf("health_score=%f, want >0", ev.HealthScore)
	}
	if !ev.Eligible {
		t.Fatalf("expected eligible for publish_healthy with core, reason=%s", ev.Reason)
	}
	if ev.EngineProjectionMode != "publish_healthy" {
		t.Fatalf("engine_projection_mode=%q, want publish_healthy", ev.EngineProjectionMode)
	}
}

func TestBlockService_QueryBlockPromotionEvidence_NoCoreProjectionFailsClosed(t *testing.T) {
	bs := newTestBlockServiceDirect(t)
	path := createTestVolDirect(t, bs, "evidence-nocore")

	vol, ok := bs.blockStore.GetBlockVolume(path)
	if !ok {
		t.Fatal("volume not found")
	}
	if err := vol.HandleAssignment(3, blockvol.RolePrimary, 30000); err != nil {
		t.Fatalf("handle assignment: %v", err)
	}

	// No core projection injected — should fail closed.
	ev, err := bs.QueryBlockPromotionEvidence(path, 0)
	if err != nil {
		t.Fatalf("query evidence: %v", err)
	}
	if ev.Eligible {
		t.Fatal("expected ineligible without V2 core projection")
	}
	if ev.Reason != "missing_engine_projection" {
		t.Fatalf("reason=%q, want %q", ev.Reason, "missing_engine_projection")
	}
	if ev.EngineProjectionMode != "" {
		t.Fatalf("engine_projection_mode=%q, want empty without core", ev.EngineProjectionMode)
	}
	// Storage facts should still be present even when ineligible.
	if ev.WALHeadLSN == 0 && ev.Epoch == 0 {
		t.Fatal("expected storage facts even for ineligible evidence")
	}
}

func TestBlockService_QueryBlockPromotionEvidence_ReturnsCoreProjectionMode(t *testing.T) {
	bs := newTestBlockServiceDirect(t)
	path := createTestVolDirect(t, bs, "evidence-epm")

	vol, ok := bs.blockStore.GetBlockVolume(path)
	if !ok {
		t.Fatal("volume not found")
	}
	if err := vol.SetRole(blockvol.RolePrimary); err != nil {
		t.Fatalf("set role: %v", err)
	}

	// Inject a core projection for this path.
	bs.coreProjMu.Lock()
	bs.coreProj[path] = engine.PublicationProjection{
		Mode: engine.ModeView{
			Name:   engine.ModePublishHealthy,
			Reason: "all_ready",
		},
		Boundary: engine.BoundaryView{
			CommittedLSN: 42,
		},
	}
	bs.coreProjMu.Unlock()

	ev, err := bs.QueryBlockPromotionEvidence(path, 0)
	if err != nil {
		t.Fatalf("query evidence: %v", err)
	}
	if ev.EngineProjectionMode != "publish_healthy" {
		t.Fatalf("engine_projection_mode=%q, want %q", ev.EngineProjectionMode, "publish_healthy")
	}
	if ev.CommittedLSN != 42 {
		t.Fatalf("committed_lsn=%d, want 42 from core projection", ev.CommittedLSN)
	}
	if !ev.Eligible {
		t.Fatalf("expected eligible for publish_healthy, reason=%s", ev.Reason)
	}
}

func TestBlockService_QueryBlockPromotionEvidence_IneligibleForGatedStates(t *testing.T) {
	bs := newTestBlockServiceDirect(t)
	path := createTestVolDirect(t, bs, "evidence-gated")

	vol, ok := bs.blockStore.GetBlockVolume(path)
	if !ok {
		t.Fatal("volume not found")
	}
	if err := vol.SetRole(blockvol.RolePrimary); err != nil {
		t.Fatalf("set role: %v", err)
	}

	tests := []struct {
		mode   engine.ModeName
		reason string
	}{
		{engine.ModeDegraded, "barrier_timeout"},
		{engine.ModeNeedsRebuild, "gap_too_large"},
		{engine.ModeBootstrapPending, "awaiting_shipper"},
		{engine.ModeAllocatedOnly, "no_role"},
	}

	for _, tt := range tests {
		t.Run(string(tt.mode), func(t *testing.T) {
			bs.coreProjMu.Lock()
			bs.coreProj[path] = engine.PublicationProjection{
				Mode: engine.ModeView{
					Name:   tt.mode,
					Reason: tt.reason,
				},
			}
			bs.coreProjMu.Unlock()

			ev, err := bs.QueryBlockPromotionEvidence(path, 0)
			if err != nil {
				t.Fatalf("query evidence: %v", err)
			}
			if ev.Eligible {
				t.Fatalf("expected ineligible for mode=%s", tt.mode)
			}
			if ev.Reason == "" {
				t.Fatal("expected non-empty reason for ineligible candidate")
			}
			if ev.EngineProjectionMode != string(tt.mode) {
				t.Fatalf("engine_projection_mode=%q, want %q", ev.EngineProjectionMode, tt.mode)
			}
		})
	}
}

func TestBlockService_QueryBlockPromotionEvidence_MissingVolumeFailsCleanly(t *testing.T) {
	bs := newTestBlockServiceDirect(t)

	_, err := bs.QueryBlockPromotionEvidence("/nonexistent/path.blk", 0)
	if err == nil {
		t.Fatal("expected error for missing volume")
	}

	_, err = bs.QueryBlockPromotionEvidence("", 0)
	if err == nil {
		t.Fatal("expected error for empty path")
	}
}

func TestBlockService_QueryBlockPromotionEvidence_EpochMismatchIneligible(t *testing.T) {
	bs := newTestBlockServiceDirect(t)
	path := createTestVolDirect(t, bs, "evidence-epoch")

	vol, ok := bs.blockStore.GetBlockVolume(path)
	if !ok {
		t.Fatal("volume not found")
	}
	if err := vol.SetEpoch(3); err != nil {
		t.Fatalf("set epoch: %v", err)
	}
	if err := vol.SetRole(blockvol.RolePrimary); err != nil {
		t.Fatalf("set role: %v", err)
	}

	ev, err := bs.QueryBlockPromotionEvidence(path, 99)
	if err != nil {
		t.Fatalf("query evidence: %v", err)
	}
	if ev.Eligible {
		t.Fatal("expected ineligible for epoch mismatch")
	}
	if ev.Reason == "" {
		t.Fatal("expected reason for epoch mismatch")
	}
}

// Master-side durability-first selection logic tests.

func TestSelectDurabilityFirstCandidate_HighestCommittedLSNWins(t *testing.T) {
	evidence := []BlockPromotionEvidence{
		{Path: "/a.blk", CommittedLSN: 10, WALHeadLSN: 10, HealthScore: 1.0, Eligible: true},
		{Path: "/b.blk", CommittedLSN: 20, WALHeadLSN: 20, HealthScore: 0.5, Eligible: true},
		{Path: "/c.blk", CommittedLSN: 15, WALHeadLSN: 30, HealthScore: 1.0, Eligible: true},
	}
	best, err := selectDurabilityFirstCandidate(evidence)
	if err != nil {
		t.Fatalf("select: %v", err)
	}
	if best.Path != "/b.blk" {
		t.Fatalf("selected %q, want /b.blk (highest CommittedLSN)", best.Path)
	}
}

func TestSelectDurabilityFirstCandidate_WALHeadLSNBreaksTie(t *testing.T) {
	evidence := []BlockPromotionEvidence{
		{Path: "/a.blk", CommittedLSN: 20, WALHeadLSN: 25, HealthScore: 1.0, Eligible: true},
		{Path: "/b.blk", CommittedLSN: 20, WALHeadLSN: 30, HealthScore: 0.5, Eligible: true},
	}
	best, err := selectDurabilityFirstCandidate(evidence)
	if err != nil {
		t.Fatalf("select: %v", err)
	}
	if best.Path != "/b.blk" {
		t.Fatalf("selected %q, want /b.blk (higher WALHeadLSN on tie)", best.Path)
	}
}

func TestSelectDurabilityFirstCandidate_HealthScoreBreaksFinalTie(t *testing.T) {
	evidence := []BlockPromotionEvidence{
		{Path: "/a.blk", CommittedLSN: 20, WALHeadLSN: 30, HealthScore: 0.8, Eligible: true},
		{Path: "/b.blk", CommittedLSN: 20, WALHeadLSN: 30, HealthScore: 1.0, Eligible: true},
	}
	best, err := selectDurabilityFirstCandidate(evidence)
	if err != nil {
		t.Fatalf("select: %v", err)
	}
	if best.Path != "/b.blk" {
		t.Fatalf("selected %q, want /b.blk (higher HealthScore on full tie)", best.Path)
	}
}

func TestSelectDurabilityFirstCandidate_AllIneligibleFailsClosed(t *testing.T) {
	evidence := []BlockPromotionEvidence{
		{Path: "/a.blk", CommittedLSN: 10, Eligible: false, Reason: "degraded"},
		{Path: "/b.blk", CommittedLSN: 20, Eligible: false, Reason: "needs_rebuild"},
	}
	_, err := selectDurabilityFirstCandidate(evidence)
	if err == nil {
		t.Fatal("expected fail-closed error when all candidates ineligible")
	}
}

func TestSelectDurabilityFirstCandidate_EmptyEvidenceFailsClosed(t *testing.T) {
	_, err := selectDurabilityFirstCandidate(nil)
	if err == nil {
		t.Fatal("expected fail-closed error on empty evidence")
	}
}

func TestQueryAllCandidateEvidence_CollectsSuccessAndErrors(t *testing.T) {
	querier := func(_ context.Context, server, path string, epoch uint64) (BlockPromotionEvidence, error) {
		if server == "bad-server" {
			return BlockPromotionEvidence{}, fmt.Errorf("connection refused")
		}
		return BlockPromotionEvidence{
			Path:         path,
			Epoch:        epoch,
			CommittedLSN: 10,
			Eligible:     true,
		}, nil
	}

	candidates := []promotionCandidate{
		{server: "good-server", path: "/a.blk", expectedEpoch: 5},
		{server: "bad-server", path: "/b.blk", expectedEpoch: 5},
		{server: "good-server2", path: "/c.blk", expectedEpoch: 5},
	}

	evidence, errs := queryAllCandidateEvidence(querier, candidates)
	if len(evidence) != 2 {
		t.Fatalf("evidence count=%d, want 2", len(evidence))
	}
	if len(errs) != 1 {
		t.Fatalf("error count=%d, want 1", len(errs))
	}
}
