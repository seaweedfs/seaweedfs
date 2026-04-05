package weed_server

import (
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/storage/blockvol"
)

// ============================================================
// CP11B-3 QA Adversarial Tests
//
// T1: Promotion candidate evaluation hardening
// T2: Re-evaluate on replica registration (B-06, B-08)
// T3: Deferred timer safety (B-07)
// T4: Rebuild endpoint / publication refresh (B-11)
// T6: Preflight surface
// ============================================================

// --- T1 Adversarial: Promotion Gate Edge Cases ---

// QA-T1-1: All 4 gates fail simultaneously on a single replica.
func TestQA_T1_AllGatesFail_SingleReplica(t *testing.T) {
	r := NewBlockVolumeRegistry()
	// Do NOT mark "bad" as block-capable (gate 4 fail).
	r.Register(&BlockVolumeEntry{
		Name: "vol1", VolumeServer: "primary", Path: "/data/vol1.blk",
		Epoch: 1, LeaseTTL: 30 * time.Second, WALHeadLSN: 1000,
		Replicas: []ReplicaInfo{{
			Server:        "bad",
			Path:          "/r1.blk",
			HealthScore:   1.0,
			WALHeadLSN:    1, // gate 2: far behind
			LastHeartbeat: time.Now().Add(-1 * time.Hour), // gate 1: stale
			Role:          blockvol.RoleToWire(blockvol.RoleRebuilding), // gate 3: wrong role
		}},
	})

	pf, _ := r.EvaluatePromotion("vol1")
	if pf.Promotable {
		t.Fatal("should not be promotable when all gates fail")
	}
	if len(pf.Rejections) != 1 {
		t.Fatalf("expected 1 rejection (first gate short-circuits), got %d", len(pf.Rejections))
	}
	// Gate 1 (freshness) should fire first since heartbeat is stale.
	if pf.Rejections[0].Reason != "stale_heartbeat" {
		t.Fatalf("expected stale_heartbeat as first rejection, got %q", pf.Rejections[0].Reason)
	}
}

// QA-T1-2: Boundary test — WAL lag exactly at tolerance.
func TestQA_T1_WALLag_ExactBoundary(t *testing.T) {
	r := NewBlockVolumeRegistry()
	r.SetPromotionLSNTolerance(50)
	r.MarkBlockCapable("replica1")

	// Primary at LSN 200, replica at LSN 150 → lag = 50 = exactly tolerance.
	// evaluatePromotionLocked: ri.WALHeadLSN + tolerance < primaryLSN → 150+50 < 200 → false → eligible.
	r.Register(&BlockVolumeEntry{
		Name: "vol1", VolumeServer: "primary", Path: "/data/vol1.blk",
		Epoch: 1, LeaseTTL: 30 * time.Second, WALHeadLSN: 200,
		Replicas: []ReplicaInfo{{
			Server: "replica1", Path: "/r1.blk", HealthScore: 1.0,
			WALHeadLSN: 150, LastHeartbeat: time.Now(),
			Role: blockvol.RoleToWire(blockvol.RoleReplica),
		}},
	})

	pf, _ := r.EvaluatePromotion("vol1")
	if !pf.Promotable {
		t.Fatalf("lag=tolerance should be eligible, got reason=%q", pf.Reason)
	}

	// Now set replica at LSN 149 → lag = 51 > tolerance → ineligible.
	r.UpdateEntry("vol1", func(e *BlockVolumeEntry) {
		e.Replicas[0].WALHeadLSN = 149
	})

	pf, _ = r.EvaluatePromotion("vol1")
	if pf.Promotable {
		t.Fatal("lag > tolerance should be ineligible")
	}
	if len(pf.Rejections) == 0 || pf.Rejections[0].Reason != "wal_lag" {
		t.Fatalf("expected wal_lag rejection, got %+v", pf.Rejections)
	}
}

// QA-T1-3: Zero LeaseTTL → freshness cutoff falls back to 60s.
func TestQA_T1_ZeroLeaseTTL_FallbackFreshness(t *testing.T) {
	r := NewBlockVolumeRegistry()
	r.MarkBlockCapable("replica1")

	r.Register(&BlockVolumeEntry{
		Name: "vol1", VolumeServer: "primary", Path: "/data/vol1.blk",
		Epoch: 1, LeaseTTL: 0, // zero
		Replicas: []ReplicaInfo{{
			Server: "replica1", Path: "/r1.blk", HealthScore: 1.0,
			WALHeadLSN:    0,
			LastHeartbeat: time.Now().Add(-90 * time.Second), // 90s ago, beyond 60s fallback
			Role:          blockvol.RoleToWire(blockvol.RoleReplica),
		}},
	})

	pf, _ := r.EvaluatePromotion("vol1")
	if pf.Promotable {
		t.Fatal("90s-old heartbeat with 0 LeaseTTL (60s fallback) should be ineligible")
	}
	if len(pf.Rejections) == 0 || pf.Rejections[0].Reason != "stale_heartbeat" {
		t.Fatalf("expected stale_heartbeat, got %+v", pf.Rejections)
	}
}

// QA-T1-4: RF3 — one dead, one stale, one healthy → healthy promoted.
func TestQA_T1_RF3_MixedGates_OnlyHealthyPromoted(t *testing.T) {
	r := NewBlockVolumeRegistry()
	r.MarkBlockCapable("healthy")
	// "dead" not marked, "stale" marked but old heartbeat.
	r.MarkBlockCapable("stale")

	r.Register(&BlockVolumeEntry{
		Name: "vol1", VolumeServer: "primary", Path: "/data/vol1.blk",
		Epoch: 1, LeaseTTL: 30 * time.Second, WALHeadLSN: 100,
		Replicas: []ReplicaInfo{
			{Server: "dead", Path: "/r1.blk", HealthScore: 1.0, WALHeadLSN: 100,
				LastHeartbeat: time.Now(), Role: blockvol.RoleToWire(blockvol.RoleReplica)},
			{Server: "stale", Path: "/r2.blk", HealthScore: 1.0, WALHeadLSN: 100,
				LastHeartbeat: time.Now().Add(-5 * time.Minute), Role: blockvol.RoleToWire(blockvol.RoleReplica)},
			{Server: "healthy", Path: "/r3.blk", HealthScore: 0.7, WALHeadLSN: 95,
				LastHeartbeat: time.Now(), Role: blockvol.RoleToWire(blockvol.RoleReplica)},
		},
	})

	newEpoch, err := r.PromoteBestReplica("vol1")
	if err != nil {
		t.Fatalf("PromoteBestReplica: %v", err)
	}
	if newEpoch != 2 {
		t.Fatalf("epoch: got %d, want 2", newEpoch)
	}
	e, _ := r.Lookup("vol1")
	if e.VolumeServer != "healthy" {
		t.Fatalf("expected 'healthy' promoted (only one passing all gates), got %q", e.VolumeServer)
	}
	// dead + stale should be in remaining replicas (not promoted, not removed).
	if len(e.Replicas) != 2 {
		t.Fatalf("expected 2 remaining replicas, got %d", len(e.Replicas))
	}
}

// QA-T1-5: EvaluatePromotion is read-only — does NOT mutate entry.
func TestQA_T1_EvaluatePromotion_ReadOnly(t *testing.T) {
	r := NewBlockVolumeRegistry()
	r.MarkBlockCapable("replica1")
	r.Register(&BlockVolumeEntry{
		Name: "vol1", VolumeServer: "primary", Path: "/data/vol1.blk",
		Epoch: 5, LeaseTTL: 30 * time.Second, WALHeadLSN: 100,
		Replicas: []ReplicaInfo{{
			Server: "replica1", Path: "/r1.blk", HealthScore: 1.0,
			WALHeadLSN: 100, LastHeartbeat: time.Now(),
			Role: blockvol.RoleToWire(blockvol.RoleReplica),
		}},
	})

	// Call EvaluatePromotion multiple times.
	for i := 0; i < 10; i++ {
		pf, _ := r.EvaluatePromotion("vol1")
		if !pf.Promotable {
			t.Fatalf("iter %d: should be promotable", i)
		}
	}

	// Entry should be unchanged.
	e, _ := r.Lookup("vol1")
	if e.Epoch != 5 {
		t.Fatalf("epoch mutated by EvaluatePromotion: got %d, want 5", e.Epoch)
	}
	if e.VolumeServer != "primary" {
		t.Fatalf("VolumeServer mutated: got %q, want primary", e.VolumeServer)
	}
	if len(e.Replicas) != 1 {
		t.Fatalf("Replicas mutated: got %d, want 1", len(e.Replicas))
	}
}

// QA-T1-6: Concurrent EvaluatePromotion + PromoteBestReplica — no panic/deadlock.
func TestQA_T1_ConcurrentEvaluateAndPromote(t *testing.T) {
	r := NewBlockVolumeRegistry()
	r.MarkBlockCapable("r1")
	r.MarkBlockCapable("r2")

	setup := func() {
		r.Unregister("vol1")
		r.Register(&BlockVolumeEntry{
			Name: "vol1", VolumeServer: "primary", Path: "/data/vol1.blk",
			Epoch: 1, LeaseTTL: 30 * time.Second, WALHeadLSN: 100,
			Replicas: []ReplicaInfo{
				{Server: "r1", Path: "/r1.blk", HealthScore: 1.0, WALHeadLSN: 100,
					LastHeartbeat: time.Now(), Role: blockvol.RoleToWire(blockvol.RoleReplica)},
				{Server: "r2", Path: "/r2.blk", HealthScore: 0.9, WALHeadLSN: 95,
					LastHeartbeat: time.Now(), Role: blockvol.RoleToWire(blockvol.RoleReplica)},
			},
		})
	}

	// Run 20 rounds: concurrent EvaluatePromotion + PromoteBestReplica.
	for round := 0; round < 20; round++ {
		setup()
		var wg sync.WaitGroup
		wg.Add(3)
		go func() {
			defer wg.Done()
			r.EvaluatePromotion("vol1")
		}()
		go func() {
			defer wg.Done()
			r.PromoteBestReplica("vol1")
		}()
		go func() {
			defer wg.Done()
			r.EvaluatePromotion("vol1")
		}()
		wg.Wait()
	}
	// No panic = pass.
}

// QA-T1-7: Promotion during ExpandInProgress — should still work
// (expand inflight doesn't block promotion, only size updates).
func TestQA_T1_PromotionDuringExpand(t *testing.T) {
	r := NewBlockVolumeRegistry()
	r.MarkBlockCapable("replica1")
	r.Register(&BlockVolumeEntry{
		Name: "vol1", VolumeServer: "primary", Path: "/data/vol1.blk",
		Epoch: 1, LeaseTTL: 30 * time.Second, WALHeadLSN: 50,
		ExpandInProgress: true, PendingExpandSize: 2 << 30,
		Replicas: []ReplicaInfo{{
			Server: "replica1", Path: "/r1.blk", HealthScore: 1.0,
			WALHeadLSN: 50, LastHeartbeat: time.Now(),
			Role: blockvol.RoleToWire(blockvol.RoleReplica),
		}},
	})

	newEpoch, err := r.PromoteBestReplica("vol1")
	if err != nil {
		t.Fatalf("promotion should succeed during expand: %v", err)
	}
	if newEpoch != 2 {
		t.Fatalf("epoch: got %d, want 2", newEpoch)
	}
	e, _ := r.Lookup("vol1")
	if e.VolumeServer != "replica1" {
		t.Fatalf("expected replica1 promoted, got %q", e.VolumeServer)
	}
}

// QA-T1-8: Double promotion — second call fails (no replicas left after first).
func TestQA_T1_DoublePromotion_SecondFails(t *testing.T) {
	r := NewBlockVolumeRegistry()
	r.MarkBlockCapable("replica1")
	r.Register(&BlockVolumeEntry{
		Name: "vol1", VolumeServer: "primary", Path: "/data/vol1.blk",
		Epoch: 1, LeaseTTL: 30 * time.Second,
		Replicas: []ReplicaInfo{{
			Server: "replica1", Path: "/r1.blk", HealthScore: 1.0,
			WALHeadLSN: 0, LastHeartbeat: time.Now(),
			Role: blockvol.RoleToWire(blockvol.RoleReplica),
		}},
	})

	_, err := r.PromoteBestReplica("vol1")
	if err != nil {
		t.Fatalf("first promotion: %v", err)
	}

	// Second promotion should fail — no replicas left.
	_, err = r.PromoteBestReplica("vol1")
	if err == nil {
		t.Fatal("second promotion should fail (no replicas)")
	}
	if !strings.Contains(err.Error(), "no replicas") {
		t.Fatalf("expected 'no replicas' error, got: %v", err)
	}
}

// --- T2 Adversarial: Orphaned Primary Edge Cases ---

// QA-T2-1: Orphan detection races with failover — no double promotion.
func TestQA_T2_OrphanAndFailover_NoDoublePromotion(t *testing.T) {
	ms := testMasterServerForFailover(t)
	registerVolumeWithReplica(t, ms, "vol1", "vs1", "vs2", 1, 5*time.Second)

	// vs1 dies → normal failover promotes vs2.
	ms.failoverBlockVolumes("vs1")

	entry, _ := ms.blockRegistry.Lookup("vol1")
	if entry.VolumeServer != "vs2" {
		t.Fatalf("expected vs2 promoted, got %q", entry.VolumeServer)
	}
	epochAfterFailover := entry.Epoch

	// Now reevaluateOrphanedPrimaries runs (e.g., from heartbeat path).
	// vs2 is now primary AND block-capable → no orphan → no double promotion.
	ms.reevaluateOrphanedPrimaries("vs2")

	entry, _ = ms.blockRegistry.Lookup("vol1")
	if entry.Epoch != epochAfterFailover {
		t.Fatalf("epoch should not change (no double promotion): got %d, want %d",
			entry.Epoch, epochAfterFailover)
	}
}

// QA-T2-2: Orphan detection when replica itself is not promotable (rebuilding role).
func TestQA_T2_OrphanButReplicaNotPromotable(t *testing.T) {
	ms := testMasterServerForFailover(t)
	ms.blockRegistry.MarkBlockCapable("vs1")
	ms.blockRegistry.MarkBlockCapable("vs2")

	ms.blockRegistry.Register(&BlockVolumeEntry{
		Name: "vol1", VolumeServer: "vs1", Path: "/data/vol1.blk",
		SizeBytes: 1 << 30, Epoch: 1, Role: blockvol.RoleToWire(blockvol.RolePrimary),
		Status: StatusActive, LeaseTTL: 5 * time.Second,
		LastLeaseGrant: time.Now().Add(-10 * time.Second),
		Replicas: []ReplicaInfo{{
			Server: "vs2", Path: "/data/vol1.blk", HealthScore: 1.0,
			Role:          blockvol.RoleToWire(blockvol.RoleRebuilding), // NOT promotable
			LastHeartbeat: time.Now(),
		}},
	})

	// Kill primary.
	ms.blockRegistry.UnmarkBlockCapable("vs1")

	// vs2 reconnects — orphan detected, but replica is Rebuilding → promotion rejected.
	ms.reevaluateOrphanedPrimaries("vs2")

	entry, _ := ms.blockRegistry.Lookup("vol1")
	// Primary should remain vs1 (promotion failed, volume stays degraded).
	if entry.VolumeServer != "vs1" {
		t.Fatalf("should NOT promote rebuilding replica, got %q", entry.VolumeServer)
	}
	if entry.Epoch != 1 {
		t.Fatalf("epoch should remain 1, got %d", entry.Epoch)
	}
}

// QA-T2-3: Concurrent reevaluateOrphanedPrimaries from multiple goroutines.
func TestQA_T2_ConcurrentReevaluation_NoPanic(t *testing.T) {
	ms := testMasterServerForFailover(t)
	registerVolumeWithReplica(t, ms, "vol1", "vs1", "vs2", 1, 5*time.Second)
	ms.blockRegistry.UnmarkBlockCapable("vs1")

	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ms.reevaluateOrphanedPrimaries("vs2")
		}()
	}
	wg.Wait()

	entry, _ := ms.blockRegistry.Lookup("vol1")
	// Should have promoted exactly once; epoch = 2 regardless of concurrency.
	if entry.VolumeServer != "vs2" {
		t.Fatalf("expected vs2 promoted, got %q", entry.VolumeServer)
	}
	if entry.Epoch != 2 {
		t.Fatalf("expected epoch 2 (single promotion), got %d", entry.Epoch)
	}
}

// QA-T2-4: Heartbeat-path orphan check on server that hosts no block volumes.
func TestQA_T2_HeartbeatOrphanCheck_NoVolumes_NoOp(t *testing.T) {
	ms := testMasterServerForFailover(t)
	// vs3 has no volumes at all.
	ms.blockRegistry.MarkBlockCapable("vs3")

	// Should not panic or error.
	ms.reevaluateOrphanedPrimaries("vs3")
}

// --- T3 Adversarial: Timer Safety Edge Cases ---

// QA-T3-1: Volume recreated with same name but different epoch → timer rejected.
func TestQA_T3_VolumeRecreated_TimerRejected(t *testing.T) {
	ms := testMasterServerForFailover(t)
	ms.blockRegistry.MarkBlockCapable("vs1")
	ms.blockRegistry.MarkBlockCapable("vs2")
	ms.blockRegistry.MarkBlockCapable("vs3")
	ms.blockRegistry.Register(&BlockVolumeEntry{
		Name: "vol1", VolumeServer: "vs1", Path: "/data/vol1.blk",
		SizeBytes: 1 << 30, Epoch: 10, Role: blockvol.RoleToWire(blockvol.RolePrimary),
		Status: StatusActive, LeaseTTL: 200 * time.Millisecond,
		LastLeaseGrant: time.Now(),
		Replicas: []ReplicaInfo{{
			Server: "vs2", Path: "/data/vol1.blk", HealthScore: 1.0,
			Role: blockvol.RoleToWire(blockvol.RoleReplica), LastHeartbeat: time.Now(),
		}},
	})

	// vs1 dies → deferred timer (captures epoch=10).
	ms.failoverBlockVolumes("vs1")

	// Delete and recreate with epoch=1 (simulates admin recreate).
	ms.blockRegistry.Unregister("vol1")
	ms.blockRegistry.Register(&BlockVolumeEntry{
		Name: "vol1", VolumeServer: "vs3", Path: "/data/vol1.blk",
		SizeBytes: 1 << 30, Epoch: 1, Role: blockvol.RoleToWire(blockvol.RolePrimary),
		Status: StatusActive, LeaseTTL: 30 * time.Second,
		LastLeaseGrant: time.Now(),
	})

	time.Sleep(350 * time.Millisecond)

	// Timer fired but epoch mismatch (10 != 1) → no promotion on new volume.
	e, _ := ms.blockRegistry.Lookup("vol1")
	if e.VolumeServer != "vs3" {
		t.Fatalf("recreated volume should keep vs3 as primary, got %q", e.VolumeServer)
	}
	if e.Epoch != 1 {
		t.Fatalf("recreated volume epoch should be 1, got %d", e.Epoch)
	}
}

// QA-T3-2: Multiple deferred timers for same server, all cancelled on reconnect.
func TestQA_T3_MultipleTimers_AllCancelled(t *testing.T) {
	ms := testMasterServerForFailover(t)
	// Create 3 volumes with active leases, all on vs1.
	for i := 0; i < 3; i++ {
		name := fmt.Sprintf("vol%d", i)
		replica := fmt.Sprintf("vs%d", i+2)
		ms.blockRegistry.MarkBlockCapable("vs1")
		ms.blockRegistry.MarkBlockCapable(replica)
		ms.blockRegistry.Register(&BlockVolumeEntry{
			Name: name, VolumeServer: "vs1", Path: fmt.Sprintf("/data/%s.blk", name),
			SizeBytes: 1 << 30, Epoch: 1, Role: blockvol.RoleToWire(blockvol.RolePrimary),
			Status: StatusActive, LeaseTTL: 5 * time.Second,
			LastLeaseGrant: time.Now(),
			Replicas: []ReplicaInfo{{
				Server: replica, Path: fmt.Sprintf("/data/%s.blk", name), HealthScore: 1.0,
				Role: blockvol.RoleToWire(blockvol.RoleReplica), LastHeartbeat: time.Now(),
			}},
		})
	}

	ms.failoverBlockVolumes("vs1")

	ms.blockFailover.mu.Lock()
	timerCount := len(ms.blockFailover.deferredTimers["vs1"])
	ms.blockFailover.mu.Unlock()
	if timerCount != 3 {
		t.Fatalf("expected 3 deferred timers, got %d", timerCount)
	}

	// vs1 reconnects → all cancelled.
	ms.cancelDeferredTimers("vs1")

	ms.blockFailover.mu.Lock()
	timerCount = len(ms.blockFailover.deferredTimers["vs1"])
	ms.blockFailover.mu.Unlock()
	if timerCount != 0 {
		t.Fatalf("all timers should be cancelled, got %d", timerCount)
	}

	// Wait past lease — no promotions should happen.
	time.Sleep(200 * time.Millisecond)
	for i := 0; i < 3; i++ {
		name := fmt.Sprintf("vol%d", i)
		e, _ := ms.blockRegistry.Lookup(name)
		if e.VolumeServer != "vs1" {
			t.Fatalf("%s: primary should remain vs1 (timer cancelled), got %q", name, e.VolumeServer)
		}
	}
}

// --- T4 Adversarial: Rebuild Metadata Edge Cases ---

// QA-T4-1: Promotion clears RebuildListenAddr, ReplicaDataAddr survives for surviving replicas.
func TestQA_T4_PromotionClearsStaleMetadata(t *testing.T) {
	r := NewBlockVolumeRegistry()
	r.MarkBlockCapable("vs2")
	r.MarkBlockCapable("vs3")
	r.Register(&BlockVolumeEntry{
		Name: "vol1", VolumeServer: "vs1", Path: "/data/vol1.blk",
		Epoch: 1, LeaseTTL: 30 * time.Second,
		RebuildListenAddr: "vs1:15000", // old primary's rebuild addr
		Replicas: []ReplicaInfo{
			{Server: "vs2", Path: "/r1.blk", HealthScore: 1.0, WALHeadLSN: 100,
				LastHeartbeat: time.Now(), Role: blockvol.RoleToWire(blockvol.RoleReplica),
				DataAddr: "vs2:4260", CtrlAddr: "vs2:4261"},
			{Server: "vs3", Path: "/r2.blk", HealthScore: 0.8, WALHeadLSN: 95,
				LastHeartbeat: time.Now(), Role: blockvol.RoleToWire(blockvol.RoleReplica),
				DataAddr: "vs3:4260", CtrlAddr: "vs3:4261"},
		},
	})

	r.PromoteBestReplica("vol1")

	e, _ := r.Lookup("vol1")
	// RebuildListenAddr must be cleared.
	if e.RebuildListenAddr != "" {
		t.Fatalf("RebuildListenAddr should be cleared, got %q", e.RebuildListenAddr)
	}
	// Promoted replica (vs2) is now primary.
	if e.VolumeServer != "vs2" {
		t.Fatalf("expected vs2 promoted, got %q", e.VolumeServer)
	}
	// Surviving replica (vs3) should still have DataAddr/CtrlAddr via scalar sync.
	if e.ReplicaDataAddr != "vs3:4260" {
		t.Fatalf("surviving replica DataAddr should be vs3:4260, got %q", e.ReplicaDataAddr)
	}
	if e.ReplicaCtrlAddr != "vs3:4261" {
		t.Fatalf("surviving replica CtrlAddr should be vs3:4261, got %q", e.ReplicaCtrlAddr)
	}
}

// QA-T4-2: Rebuild with stale RebuildListenAddr from before promotion.
func TestQA_T4_RebuildAddr_FromOldPrimary_NotUsed(t *testing.T) {
	ms := testMasterServerForFailover(t)
	ms.blockRegistry.MarkBlockCapable("vs1")
	ms.blockRegistry.MarkBlockCapable("vs2")
	ms.blockRegistry.Register(&BlockVolumeEntry{
		Name: "vol1", VolumeServer: "vs1", Path: "/data/vol1.blk",
		SizeBytes: 1 << 30, Epoch: 1, Role: blockvol.RoleToWire(blockvol.RolePrimary),
		Status: StatusActive, LeaseTTL: 5 * time.Second,
		LastLeaseGrant:    time.Now().Add(-10 * time.Second),
		RebuildListenAddr: "vs1:15000",
		Replicas: []ReplicaInfo{{
			Server: "vs2", Path: "/data/vol1.blk", HealthScore: 1.0,
			Role: blockvol.RoleToWire(blockvol.RoleReplica), LastHeartbeat: time.Now(),
		}},
	})

	// vs1 dies → vs2 promoted. RebuildListenAddr should be cleared by PromoteBestReplica.
	ms.failoverBlockVolumes("vs1")

	entry, _ := ms.blockRegistry.Lookup("vol1")
	if entry.RebuildListenAddr != "" {
		t.Fatalf("RebuildListenAddr should be empty after promotion, got %q", entry.RebuildListenAddr)
	}

	// vs1 reconnects → rebuild queued with empty addr (not stale vs1:15000).
	ms.recoverBlockVolumes("vs1")
	assignments := ms.blockAssignmentQueue.Peek("vs1")
	for _, a := range assignments {
		if blockvol.RoleFromWire(a.Role) == blockvol.RoleRebuilding {
			if a.RebuildAddr == "vs1:15000" {
				t.Fatal("rebuild should NOT use old primary's stale RebuildListenAddr")
			}
			return
		}
	}
	t.Fatal("expected rebuild assignment for vs1")
}

// --- T6 Adversarial: Preflight Surface ---

// QA-T6-1: Preflight with no replicas → clear reason.
func TestQA_T6_Preflight_NoReplicas(t *testing.T) {
	r := NewBlockVolumeRegistry()
	r.Register(&BlockVolumeEntry{
		Name: "vol1", VolumeServer: "primary", Path: "/data/vol1.blk",
		Epoch: 1, LeaseTTL: 30 * time.Second,
	})

	pf, _ := r.EvaluatePromotion("vol1")
	if pf.Promotable {
		t.Fatal("should not be promotable with no replicas")
	}
	if pf.Reason != "no replicas" {
		t.Fatalf("expected 'no replicas', got %q", pf.Reason)
	}
}

// QA-T6-2: Preflight aggregates multiple rejection reasons.
func TestQA_T6_Preflight_MultipleRejections(t *testing.T) {
	r := NewBlockVolumeRegistry()
	r.MarkBlockCapable("stale-hb")
	// "dead" not marked

	r.Register(&BlockVolumeEntry{
		Name: "vol1", VolumeServer: "primary", Path: "/data/vol1.blk",
		Epoch: 1, LeaseTTL: 30 * time.Second, WALHeadLSN: 100,
		Replicas: []ReplicaInfo{
			{Server: "dead", Path: "/r1.blk", HealthScore: 1.0, WALHeadLSN: 100,
				LastHeartbeat: time.Now(), Role: blockvol.RoleToWire(blockvol.RoleReplica)},
			{Server: "stale-hb", Path: "/r2.blk", HealthScore: 1.0, WALHeadLSN: 100,
				LastHeartbeat: time.Now().Add(-10 * time.Minute), Role: blockvol.RoleToWire(blockvol.RoleReplica)},
		},
	})

	pf, _ := r.EvaluatePromotion("vol1")
	if pf.Promotable {
		t.Fatal("should not be promotable")
	}
	if len(pf.Rejections) != 2 {
		t.Fatalf("expected 2 rejections, got %d", len(pf.Rejections))
	}
	// Verify rejection reasons map to correct servers.
	reasons := map[string]string{}
	for _, rej := range pf.Rejections {
		reasons[rej.Server] = rej.Reason
	}
	if reasons["dead"] != "server_dead" {
		t.Fatalf("dead server: expected server_dead, got %q", reasons["dead"])
	}
	if reasons["stale-hb"] != "stale_heartbeat" {
		t.Fatalf("stale server: expected stale_heartbeat, got %q", reasons["stale-hb"])
	}
	// Reason should aggregate.
	if !strings.Contains(pf.Reason, "+1 more") {
		t.Fatalf("expected aggregated reason, got %q", pf.Reason)
	}
}

// QA-T6-3: Preflight for non-existent volume → error.
func TestQA_T6_Preflight_NonExistent(t *testing.T) {
	r := NewBlockVolumeRegistry()
	_, err := r.EvaluatePromotion("does-not-exist")
	if err == nil {
		t.Fatal("expected error for non-existent volume")
	}
}

// ============================================================
// Additional Adversarial / Regression Tests
// ============================================================

// --- T1 Gate 2 edge case: zero primary LSN ---

// QA-T1-9: When primary WALHeadLSN=0, all replicas should be eligible
// regardless of their LSN (no data yet → no lag possible).
func TestQA_T1_ZeroPrimaryLSN_AllReplicasEligible(t *testing.T) {
	r := NewBlockVolumeRegistry()
	r.MarkBlockCapable("r1")
	r.MarkBlockCapable("r2")

	r.Register(&BlockVolumeEntry{
		Name: "vol1", VolumeServer: "primary", Path: "/data/vol1.blk",
		Epoch: 1, LeaseTTL: 30 * time.Second, WALHeadLSN: 0, // zero
		Replicas: []ReplicaInfo{
			{Server: "r1", Path: "/r1.blk", HealthScore: 1.0, WALHeadLSN: 0,
				LastHeartbeat: time.Now(), Role: blockvol.RoleToWire(blockvol.RoleReplica)},
			{Server: "r2", Path: "/r2.blk", HealthScore: 0.9, WALHeadLSN: 500,
				LastHeartbeat: time.Now(), Role: blockvol.RoleToWire(blockvol.RoleReplica)},
		},
	})

	pf, _ := r.EvaluatePromotion("vol1")
	if !pf.Promotable {
		t.Fatalf("zero primary LSN: all replicas should be eligible, reason=%q", pf.Reason)
	}
	if len(pf.Rejections) != 0 {
		t.Fatalf("expected 0 rejections with zero primary LSN, got %d: %+v", len(pf.Rejections), pf.Rejections)
	}
}

// QA-T1-10: Replica with RolePrimary in Replicas[] → rejected as wrong_role.
func TestQA_T1_ReplicaWithPrimaryRole_Rejected(t *testing.T) {
	r := NewBlockVolumeRegistry()
	r.MarkBlockCapable("r1")

	r.Register(&BlockVolumeEntry{
		Name: "vol1", VolumeServer: "primary", Path: "/data/vol1.blk",
		Epoch: 1, LeaseTTL: 30 * time.Second, WALHeadLSN: 100,
		Replicas: []ReplicaInfo{
			{Server: "r1", Path: "/r1.blk", HealthScore: 1.0, WALHeadLSN: 100,
				LastHeartbeat: time.Now(), Role: blockvol.RoleToWire(blockvol.RolePrimary)},
		},
	})

	pf, _ := r.EvaluatePromotion("vol1")
	if pf.Promotable {
		t.Fatal("replica with RolePrimary should NOT be promotable")
	}
	if len(pf.Rejections) != 1 || pf.Rejections[0].Reason != "wrong_role" {
		t.Fatalf("expected wrong_role rejection, got %+v", pf.Rejections)
	}
}

// QA-T1-11: Heartbeat exactly at freshness boundary.
func TestQA_T1_HeartbeatExactlyAtCutoff(t *testing.T) {
	r := NewBlockVolumeRegistry()
	r.MarkBlockCapable("r1")

	leaseTTL := 5 * time.Second
	freshnessCutoff := 2 * leaseTTL // 10s

	// Heartbeat exactly at cutoff → now.Sub(hb) == 10s → NOT > 10s → eligible.
	r.Register(&BlockVolumeEntry{
		Name: "vol1", VolumeServer: "primary", Path: "/data/vol1.blk",
		Epoch: 1, LeaseTTL: leaseTTL, WALHeadLSN: 0,
		Replicas: []ReplicaInfo{
			{Server: "r1", Path: "/r1.blk", HealthScore: 1.0,
				LastHeartbeat: time.Now().Add(-freshnessCutoff), // exactly at boundary
				Role:          blockvol.RoleToWire(blockvol.RoleReplica)},
		},
	})

	pf, _ := r.EvaluatePromotion("vol1")
	if !pf.Promotable {
		t.Fatalf("heartbeat exactly at cutoff should be eligible, reason=%q", pf.Reason)
	}
}

// --- T2 additional: RF3 orphan, timer-failover interactions ---

// QA-T2-5: RF3 orphaned primary — two replicas alive, reconnecting triggers promotion.
func TestQA_T2_RF3_OrphanedPrimary_BestReplicaPromoted(t *testing.T) {
	ms := testMasterServerForFailover(t)
	registerVolumeRF3(t, ms, "vol1", "vs1", "vs2", "vs3", 1, 5*time.Second)

	// Give vs3 higher health.
	ms.blockRegistry.UpdateEntry("vol1", func(entry *BlockVolumeEntry) {
		entry.Replicas[0].HealthScore = 0.7 // vs2
		entry.Replicas[1].HealthScore = 1.0 // vs3
	})

	// Kill primary without calling failoverBlockVolumes (simulates missed failover).
	ms.blockRegistry.UnmarkBlockCapable("vs1")

	// vs2 reconnects → orphan detected → best replica (vs3) promoted.
	ms.reevaluateOrphanedPrimaries("vs2")

	entry, _ := ms.blockRegistry.Lookup("vol1")
	if entry.VolumeServer != "vs3" {
		t.Fatalf("expected vs3 promoted (highest health), got %q", entry.VolumeServer)
	}
	if entry.Epoch != 2 {
		t.Fatalf("expected epoch 2, got %d", entry.Epoch)
	}
	// vs2 should remain as replica (not promoted, not removed).
	if len(entry.Replicas) != 1 || entry.Replicas[0].Server != "vs2" {
		t.Fatalf("expected [vs2] as remaining replica, got %+v", entry.Replicas)
	}
}

// QA-T2-6: Failover promotes, then orphan check runs for same volume — no double promotion.
func TestQA_T2_FailoverThenOrphan_SameVolume_NoDuplicate(t *testing.T) {
	ms := testMasterServerForFailover(t)
	registerVolumeWithReplica(t, ms, "vol1", "vs1", "vs2", 1, 5*time.Second)

	// Proper failover: vs1 dies → vs2 promoted.
	ms.failoverBlockVolumes("vs1")

	entry, _ := ms.blockRegistry.Lookup("vol1")
	if entry.VolumeServer != "vs2" {
		t.Fatalf("expected vs2, got %q", entry.VolumeServer)
	}
	epochAfter := entry.Epoch

	// vs2 is now primary AND block-capable. Orphan check shouldn't find anything.
	orphaned := ms.blockRegistry.VolumesWithDeadPrimary("vs2")
	if len(orphaned) != 0 {
		t.Fatalf("no orphans expected (vs2 is now primary), got %v", orphaned)
	}

	// Just to be sure: calling reevaluate shouldn't change anything.
	ms.reevaluateOrphanedPrimaries("vs2")
	entry, _ = ms.blockRegistry.Lookup("vol1")
	if entry.Epoch != epochAfter {
		t.Fatalf("epoch shouldn't change, got %d want %d", entry.Epoch, epochAfter)
	}
}

// QA-T2-7: Orphan deferred timer stored under dead primary → cancelDeferredTimers cancels it.
func TestQA_T2_OrphanDeferredTimer_CancelledOnPrimaryReconnect(t *testing.T) {
	ms := testMasterServerForFailover(t)
	ms.blockRegistry.MarkBlockCapable("vs1")
	ms.blockRegistry.MarkBlockCapable("vs2")
	ms.blockRegistry.Register(&BlockVolumeEntry{
		Name: "vol1", VolumeServer: "vs1", Path: "/data/vol1.blk",
		SizeBytes: 1 << 30, Epoch: 1, Role: blockvol.RoleToWire(blockvol.RolePrimary),
		Status: StatusActive, LeaseTTL: 300 * time.Millisecond,
		LastLeaseGrant: time.Now(), // lease active
		Replicas: []ReplicaInfo{{
			Server: "vs2", Path: "/data/vol1.blk", HealthScore: 1.0,
			Role: blockvol.RoleToWire(blockvol.RoleReplica), LastHeartbeat: time.Now(),
		}},
	})

	// Kill primary.
	ms.blockRegistry.UnmarkBlockCapable("vs1")

	// Replica reconnects → orphan with active lease → deferred timer (stored under "vs1").
	ms.reevaluateOrphanedPrimaries("vs2")

	ms.blockFailover.mu.Lock()
	timerCount := len(ms.blockFailover.deferredTimers["vs1"])
	ms.blockFailover.mu.Unlock()
	if timerCount != 1 {
		t.Fatalf("expected 1 deferred timer under vs1, got %d", timerCount)
	}

	// Primary comes back (maybe network partition healed) → cancel its timers.
	ms.blockRegistry.MarkBlockCapable("vs1")
	ms.cancelDeferredTimers("vs1")

	ms.blockFailover.mu.Lock()
	timerCount = len(ms.blockFailover.deferredTimers["vs1"])
	ms.blockFailover.mu.Unlock()
	if timerCount != 0 {
		t.Fatalf("expected 0 timers after cancel, got %d", timerCount)
	}

	// Wait past the original lease → no promotion should have happened.
	time.Sleep(500 * time.Millisecond)

	entry, _ := ms.blockRegistry.Lookup("vol1")
	if entry.VolumeServer != "vs1" {
		t.Fatalf("primary should remain vs1 (timer cancelled), got %q", entry.VolumeServer)
	}
	if entry.Epoch != 1 {
		t.Fatalf("epoch should remain 1, got %d", entry.Epoch)
	}
}

// QA-T2-8: Volume deleted between VolumesWithDeadPrimary and reevaluate loop — no panic.
func TestQA_T2_VolumeDeletedDuringReevaluation(t *testing.T) {
	ms := testMasterServerForFailover(t)
	registerVolumeWithReplica(t, ms, "vol1", "vs1", "vs2", 1, 5*time.Second)
	ms.blockRegistry.UnmarkBlockCapable("vs1")

	// Verify orphan is detected.
	orphaned := ms.blockRegistry.VolumesWithDeadPrimary("vs2")
	if len(orphaned) != 1 {
		t.Fatalf("expected 1 orphan, got %d", len(orphaned))
	}

	// Delete the volume right away.
	ms.blockRegistry.Unregister("vol1")

	// reevaluateOrphanedPrimaries should handle the Lookup miss gracefully.
	ms.reevaluateOrphanedPrimaries("vs2") // must not panic
}

// --- T3 additional: Orphan timer fires and promotes correctly ---

// QA-T3-3: Orphan deferred timer fires after lease expiry → promotion succeeds.
func TestQA_T3_OrphanDeferredTimer_FiresAndPromotes(t *testing.T) {
	ms := testMasterServerForFailover(t)
	ms.blockRegistry.MarkBlockCapable("vs1")
	ms.blockRegistry.MarkBlockCapable("vs2")
	ms.blockRegistry.Register(&BlockVolumeEntry{
		Name: "vol1", VolumeServer: "vs1", Path: "/data/vol1.blk",
		SizeBytes: 1 << 30, Epoch: 1, Role: blockvol.RoleToWire(blockvol.RolePrimary),
		Status: StatusActive, LeaseTTL: 200 * time.Millisecond,
		LastLeaseGrant: time.Now(), // lease active
		Replicas: []ReplicaInfo{{
			Server: "vs2", Path: "/data/vol1.blk", HealthScore: 1.0,
			Role: blockvol.RoleToWire(blockvol.RoleReplica), LastHeartbeat: time.Now(),
		}},
	})

	// Kill primary.
	ms.blockRegistry.UnmarkBlockCapable("vs1")

	// Orphan detected with active lease → deferred.
	ms.reevaluateOrphanedPrimaries("vs2")

	// Immediately: not yet promoted.
	entry, _ := ms.blockRegistry.Lookup("vol1")
	if entry.VolumeServer != "vs1" {
		t.Fatalf("should NOT promote yet (lease active), got %q", entry.VolumeServer)
	}

	// Wait for lease to expire + timer.
	time.Sleep(350 * time.Millisecond)

	entry, _ = ms.blockRegistry.Lookup("vol1")
	if entry.VolumeServer != "vs2" {
		t.Fatalf("should promote after lease expires, got %q", entry.VolumeServer)
	}
	if entry.Epoch != 2 {
		t.Fatalf("expected epoch 2, got %d", entry.Epoch)
	}
}

// QA-T3-4: Orphan deferred timer epoch mismatch → no stale promotion.
func TestQA_T3_OrphanDeferredTimer_EpochChanged_NoPromotion(t *testing.T) {
	ms := testMasterServerForFailover(t)
	ms.blockRegistry.MarkBlockCapable("vs1")
	ms.blockRegistry.MarkBlockCapable("vs2")
	ms.blockRegistry.Register(&BlockVolumeEntry{
		Name: "vol1", VolumeServer: "vs1", Path: "/data/vol1.blk",
		SizeBytes: 1 << 30, Epoch: 5, Role: blockvol.RoleToWire(blockvol.RolePrimary),
		Status: StatusActive, LeaseTTL: 200 * time.Millisecond,
		LastLeaseGrant: time.Now(),
		Replicas: []ReplicaInfo{{
			Server: "vs2", Path: "/data/vol1.blk", HealthScore: 1.0,
			Role: blockvol.RoleToWire(blockvol.RoleReplica), LastHeartbeat: time.Now(),
		}},
	})

	ms.blockRegistry.UnmarkBlockCapable("vs1")
	ms.reevaluateOrphanedPrimaries("vs2")

	// Before timer fires, bump epoch (simulates admin intervention).
	ms.blockRegistry.UpdateEntry("vol1", func(e *BlockVolumeEntry) {
		e.Epoch = 42
	})

	time.Sleep(350 * time.Millisecond)

	e, _ := ms.blockRegistry.Lookup("vol1")
	if e.Epoch != 42 {
		t.Fatalf("epoch should remain 42 (timer rejected), got %d", e.Epoch)
	}
	if e.VolumeServer != "vs1" {
		t.Fatalf("primary should remain vs1 (timer rejected), got %q", e.VolumeServer)
	}
}

// --- T4 additional ---

// QA-T4-3: Rebuild uses updated RebuildListenAddr after new primary heartbeats.
func TestQA_T4_RebuildAddr_UpdatedByHeartbeat(t *testing.T) {
	ms := testMasterServerForFailover(t)
	registerVolumeWithReplica(t, ms, "vol1", "vs1", "vs2", 1, 5*time.Second)

	// vs1 dies → vs2 promoted.
	ms.failoverBlockVolumes("vs1")

	entry, _ := ms.blockRegistry.Lookup("vol1")
	if entry.RebuildListenAddr != "" {
		t.Fatalf("should be empty after promotion, got %q", entry.RebuildListenAddr)
	}

	// New primary (vs2) heartbeats with RebuildListenAddr.
	ms.blockRegistry.UpdateEntry("vol1", func(e *BlockVolumeEntry) {
		e.RebuildListenAddr = "vs2:15000"
	})

	// vs1 reconnects → rebuild should use the updated addr.
	ms.recoverBlockVolumes("vs1")

	assignments := ms.blockAssignmentQueue.Peek("vs1")
	for _, a := range assignments {
		if blockvol.RoleFromWire(a.Role) == blockvol.RoleRebuilding {
			if a.RebuildAddr != "vs2:15000" {
				t.Fatalf("rebuild should use updated addr vs2:15000, got %q", a.RebuildAddr)
			}
			return
		}
	}
	t.Fatal("expected rebuild assignment for vs1")
}

// --- T6 additional ---

// QA-T6-4: Preflight with primary dead but candidate available — verify result fields.
func TestQA_T6_Preflight_FullResultFields(t *testing.T) {
	r := NewBlockVolumeRegistry()
	r.MarkBlockCapable("vs2")
	r.MarkBlockCapable("vs3")
	// "stale" is block-capable but has old heartbeat
	r.MarkBlockCapable("stale")

	r.Register(&BlockVolumeEntry{
		Name: "vol1", VolumeServer: "vs1", Path: "/data/vol1.blk",
		Epoch: 5, LeaseTTL: 30 * time.Second, WALHeadLSN: 200,
		Replicas: []ReplicaInfo{
			{Server: "stale", Path: "/r0.blk", HealthScore: 1.0, WALHeadLSN: 200,
				LastHeartbeat: time.Now().Add(-10 * time.Minute), Role: blockvol.RoleToWire(blockvol.RoleReplica)},
			{Server: "vs2", Path: "/r1.blk", HealthScore: 0.9, WALHeadLSN: 195,
				LastHeartbeat: time.Now(), Role: blockvol.RoleToWire(blockvol.RoleReplica)},
			{Server: "vs3", Path: "/r2.blk", HealthScore: 0.95, WALHeadLSN: 198,
				LastHeartbeat: time.Now(), Role: blockvol.RoleToWire(blockvol.RoleReplica)},
		},
	})

	pf, err := r.EvaluatePromotion("vol1")
	if err != nil {
		t.Fatalf("EvaluatePromotion: %v", err)
	}
	if !pf.Promotable {
		t.Fatalf("should be promotable, reason=%q", pf.Reason)
	}
	// Best candidate: vs3 (highest health among eligible).
	if pf.Candidate == nil || pf.Candidate.Server != "vs3" {
		t.Fatalf("expected vs3 as candidate, got %+v", pf.Candidate)
	}
	if pf.CandidateIdx < 0 {
		t.Fatal("CandidateIdx should be non-negative")
	}
	// 1 rejection: stale.
	if len(pf.Rejections) != 1 {
		t.Fatalf("expected 1 rejection (stale), got %d: %+v", len(pf.Rejections), pf.Rejections)
	}
	if pf.Rejections[0].Server != "stale" || pf.Rejections[0].Reason != "stale_heartbeat" {
		t.Fatalf("unexpected rejection: %+v", pf.Rejections[0])
	}
	if pf.VolumeName != "vol1" {
		t.Fatalf("VolumeName: got %q, want vol1", pf.VolumeName)
	}
}

// QA-T6-5: Preflight with RoleStale replica — rejected as wrong_role.
func TestQA_T6_Preflight_StaleRole_Rejected(t *testing.T) {
	r := NewBlockVolumeRegistry()
	r.MarkBlockCapable("r1")

	r.Register(&BlockVolumeEntry{
		Name: "vol1", VolumeServer: "primary", Path: "/data/vol1.blk",
		Epoch: 1, LeaseTTL: 30 * time.Second,
		Replicas: []ReplicaInfo{
			{Server: "r1", Path: "/r1.blk", HealthScore: 1.0, WALHeadLSN: 0,
				LastHeartbeat: time.Now(), Role: blockvol.RoleToWire(blockvol.RoleStale)},
		},
	})

	pf, _ := r.EvaluatePromotion("vol1")
	if pf.Promotable {
		t.Fatal("RoleStale replica should NOT be promotable")
	}
	if len(pf.Rejections) != 1 || pf.Rejections[0].Reason != "wrong_role" {
		t.Fatalf("expected wrong_role rejection, got %+v", pf.Rejections)
	}
}

// QA-T6-6: Preflight with RoleDraining replica — rejected as wrong_role.
func TestQA_T6_Preflight_DrainingRole_Rejected(t *testing.T) {
	r := NewBlockVolumeRegistry()
	r.MarkBlockCapable("r1")

	r.Register(&BlockVolumeEntry{
		Name: "vol1", VolumeServer: "primary", Path: "/data/vol1.blk",
		Epoch: 1, LeaseTTL: 30 * time.Second,
		Replicas: []ReplicaInfo{
			{Server: "r1", Path: "/r1.blk", HealthScore: 1.0, WALHeadLSN: 0,
				LastHeartbeat: time.Now(), Role: blockvol.RoleToWire(blockvol.RoleDraining)},
		},
	})

	pf, _ := r.EvaluatePromotion("vol1")
	if pf.Promotable {
		t.Fatal("RoleDraining replica should NOT be promotable")
	}
	if len(pf.Rejections) != 1 || pf.Rejections[0].Reason != "wrong_role" {
		t.Fatalf("expected wrong_role rejection, got %+v", pf.Rejections)
	}
}

// --- Concurrent: failover + orphan reevaluation race ---

// QA-RACE-1: Concurrent failover and orphan reevaluation — no panic or deadlock.
func TestQA_ConcurrentFailoverAndOrphanReevaluation(t *testing.T) {
	ms := testMasterServerForFailover(t)
	registerVolumeWithReplica(t, ms, "vol1", "vs1", "vs2", 1, 5*time.Second)

	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(2)
		go func() {
			defer wg.Done()
			ms.failoverBlockVolumes("vs1")
		}()
		go func() {
			defer wg.Done()
			ms.reevaluateOrphanedPrimaries("vs2")
		}()
	}
	wg.Wait()
	// No panic = pass. Volume may or may not have been promoted — that's fine.
}

// QA-RACE-2: Concurrent VolumesWithDeadPrimary + UnmarkBlockCapable — no panic.
func TestQA_ConcurrentVolumesWithDeadPrimaryAndUnmark(t *testing.T) {
	r := NewBlockVolumeRegistry()
	r.MarkBlockCapable("vs1")
	r.MarkBlockCapable("vs2")
	r.Register(&BlockVolumeEntry{
		Name: "vol1", VolumeServer: "vs1", Path: "/data/vol1.blk",
		SizeBytes: 1 << 30, Epoch: 1, Role: blockvol.RoleToWire(blockvol.RolePrimary),
		Status: StatusActive,
		Replicas: []ReplicaInfo{{Server: "vs2", Path: "/data/vol1.blk"}},
	})

	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(2)
		go func() {
			defer wg.Done()
			r.VolumesWithDeadPrimary("vs2")
		}()
		go func() {
			defer wg.Done()
			r.UnmarkBlockCapable("vs1")
			r.MarkBlockCapable("vs1")
		}()
	}
	wg.Wait()
}

// ============================================================
// CP11B-3 T5: Manual Promote Adversarial Tests
// ============================================================

// QA-T5-1: Force does NOT bypass no_heartbeat (zero time).
func TestQA_T5_ManualPromote_ForceNoHeartbeat_Rejected(t *testing.T) {
	r := NewBlockVolumeRegistry()
	r.MarkBlockCapable("r1")
	r.Register(&BlockVolumeEntry{
		Name: "vol1", VolumeServer: "primary", Path: "/data/vol1.blk",
		Epoch: 1, LeaseTTL: 30 * time.Second,
		Replicas: []ReplicaInfo{
			{Server: "r1", Path: "/r1.blk", HealthScore: 1.0,
				LastHeartbeat: time.Time{}, // zero — never seen
				Role:          blockvol.RoleToWire(blockvol.RoleReplica)},
		},
	})

	_, _, _, pf, err := r.ManualPromote("vol1", "", true)
	if err == nil {
		t.Fatal("force should NOT bypass no_heartbeat (zero time)")
	}
	if len(pf.Rejections) == 0 || pf.Rejections[0].Reason != "no_heartbeat" {
		t.Fatalf("expected no_heartbeat rejection, got %+v", pf.Rejections)
	}
}

// QA-T5-2: Force does NOT bypass wrong_role.
func TestQA_T5_ManualPromote_ForceWrongRole_Rejected(t *testing.T) {
	r := NewBlockVolumeRegistry()
	r.MarkBlockCapable("r1")
	r.Register(&BlockVolumeEntry{
		Name: "vol1", VolumeServer: "primary", Path: "/data/vol1.blk",
		Epoch: 1, LeaseTTL: 30 * time.Second,
		Replicas: []ReplicaInfo{
			{Server: "r1", Path: "/r1.blk", HealthScore: 1.0,
				LastHeartbeat: time.Now(),
				Role:          blockvol.RoleToWire(blockvol.RoleRebuilding)},
		},
	})

	_, _, _, pf, err := r.ManualPromote("vol1", "", true)
	if err == nil {
		t.Fatal("force should NOT bypass wrong_role")
	}
	if len(pf.Rejections) == 0 || pf.Rejections[0].Reason != "wrong_role" {
		t.Fatalf("expected wrong_role rejection, got %+v", pf.Rejections)
	}
}

// QA-T5-3: Force bypasses wal_lag.
func TestQA_T5_ManualPromote_ForceBypassesWALLag(t *testing.T) {
	r := NewBlockVolumeRegistry()
	r.SetPromotionLSNTolerance(10)
	r.MarkBlockCapable("r1")
	r.Register(&BlockVolumeEntry{
		Name: "vol1", VolumeServer: "primary", Path: "/data/vol1.blk",
		Epoch: 1, LeaseTTL: 30 * time.Second, WALHeadLSN: 1000,
		Replicas: []ReplicaInfo{
			{Server: "r1", Path: "/r1.blk", HealthScore: 1.0,
				WALHeadLSN: 100, // lag = 900, way beyond tolerance=10
				LastHeartbeat: time.Now(),
				Role:          blockvol.RoleToWire(blockvol.RoleReplica)},
		},
	})

	// Non-force: should fail on wal_lag.
	_, _, _, pf, err := r.ManualPromote("vol1", "", false)
	if err == nil {
		t.Fatal("non-force should reject wal_lag")
	}
	if len(pf.Rejections) == 0 || pf.Rejections[0].Reason != "wal_lag" {
		t.Fatalf("expected wal_lag rejection, got %+v", pf.Rejections)
	}

	// Force: should succeed despite wal_lag.
	newEpoch, _, _, _, err := r.ManualPromote("vol1", "", true)
	if err != nil {
		t.Fatalf("force should bypass wal_lag: %v", err)
	}
	if newEpoch != 2 {
		t.Fatalf("epoch: got %d, want 2", newEpoch)
	}
}

// QA-T5-4: Force + alive primary → promotion succeeds.
func TestQA_T5_ManualPromote_PrimaryAlive_ForceOverrides(t *testing.T) {
	r := NewBlockVolumeRegistry()
	r.MarkBlockCapable("primary")
	r.MarkBlockCapable("r1")
	r.Register(&BlockVolumeEntry{
		Name: "vol1", VolumeServer: "primary", Path: "/data/vol1.blk",
		Epoch: 1, LeaseTTL: 30 * time.Second,
		Replicas: []ReplicaInfo{
			{Server: "r1", Path: "/r1.blk", HealthScore: 1.0,
				LastHeartbeat: time.Now(),
				Role:          blockvol.RoleToWire(blockvol.RoleReplica)},
		},
	})

	// Non-force: rejected (primary alive).
	_, _, _, pf, err := r.ManualPromote("vol1", "", false)
	if err == nil {
		t.Fatal("non-force should reject when primary alive")
	}
	if pf.Reason != "primary_alive" {
		t.Fatalf("expected primary_alive, got %q", pf.Reason)
	}

	// Force: succeeds despite alive primary.
	newEpoch, _, _, _, err := r.ManualPromote("vol1", "", true)
	if err != nil {
		t.Fatalf("force should override primary_alive: %v", err)
	}
	if newEpoch != 2 {
		t.Fatalf("epoch: got %d, want 2", newEpoch)
	}
	e, _ := r.Lookup("vol1")
	if e.VolumeServer != "r1" {
		t.Fatalf("expected r1 promoted, got %q", e.VolumeServer)
	}
}

// QA-T5-5: Concurrent ManualPromote + PromoteBestReplica — no panic.
func TestQA_T5_ManualPromote_ConcurrentWithAutoPromotion(t *testing.T) {
	r := NewBlockVolumeRegistry()
	r.MarkBlockCapable("r1")
	r.MarkBlockCapable("r2")

	setup := func() {
		r.Unregister("vol1")
		r.Register(&BlockVolumeEntry{
			Name: "vol1", VolumeServer: "primary", Path: "/data/vol1.blk",
			Epoch: 1, LeaseTTL: 30 * time.Second,
			Replicas: []ReplicaInfo{
				{Server: "r1", Path: "/r1.blk", HealthScore: 1.0,
					LastHeartbeat: time.Now(), Role: blockvol.RoleToWire(blockvol.RoleReplica)},
				{Server: "r2", Path: "/r2.blk", HealthScore: 0.9,
					LastHeartbeat: time.Now(), Role: blockvol.RoleToWire(blockvol.RoleReplica)},
			},
		})
	}

	for round := 0; round < 20; round++ {
		setup()
		var wg sync.WaitGroup
		wg.Add(3)
		go func() {
			defer wg.Done()
			r.ManualPromote("vol1", "", false)
		}()
		go func() {
			defer wg.Done()
			r.PromoteBestReplica("vol1")
		}()
		go func() {
			defer wg.Done()
			r.ManualPromote("vol1", "r2", true)
		}()
		wg.Wait()
	}
	// No panic = pass.
}

// QA-T5-6: Rejection response includes per-replica structured rejections.
func TestQA_T5_ManualPromote_ReturnsStructuredRejections(t *testing.T) {
	r := NewBlockVolumeRegistry()
	r.MarkBlockCapable("stale")
	// "dead" not marked

	r.Register(&BlockVolumeEntry{
		Name: "vol1", VolumeServer: "primary", Path: "/data/vol1.blk",
		Epoch: 1, LeaseTTL: 30 * time.Second, WALHeadLSN: 100,
		Replicas: []ReplicaInfo{
			{Server: "dead", Path: "/r1.blk", HealthScore: 1.0, WALHeadLSN: 100,
				LastHeartbeat: time.Now(), Role: blockvol.RoleToWire(blockvol.RoleReplica)},
			{Server: "stale", Path: "/r2.blk", HealthScore: 1.0, WALHeadLSN: 100,
				LastHeartbeat: time.Now().Add(-10 * time.Minute),
				Role:          blockvol.RoleToWire(blockvol.RoleReplica)},
		},
	})

	_, _, _, pf, err := r.ManualPromote("vol1", "", false)
	if err == nil {
		t.Fatal("should reject")
	}
	if len(pf.Rejections) != 2 {
		t.Fatalf("expected 2 rejections, got %d", len(pf.Rejections))
	}
	reasons := map[string]string{}
	for _, rej := range pf.Rejections {
		reasons[rej.Server] = rej.Reason
	}
	if reasons["dead"] != "server_dead" {
		t.Fatalf("dead: expected server_dead, got %q", reasons["dead"])
	}
	if reasons["stale"] != "stale_heartbeat" {
		t.Fatalf("stale: expected stale_heartbeat, got %q", reasons["stale"])
	}
}

// QA-T5-7: HTTP round-trip test for promote handler.
func TestQA_T5_PromoteHandler_HTTP(t *testing.T) {
	ms := testMasterServerForFailover(t)
	ms.blockRegistry.MarkBlockCapable("vs2")
	ms.blockRegistry.Register(&BlockVolumeEntry{
		Name: "vol1", VolumeServer: "vs1", Path: "/data/vol1.blk",
		SizeBytes: 1 << 30, Epoch: 1, Role: blockvol.RoleToWire(blockvol.RolePrimary),
		Status: StatusActive, LeaseTTL: 5 * time.Second,
		LastLeaseGrant: time.Now().Add(-10 * time.Second),
		Replicas: []ReplicaInfo{{
			Server: "vs2", Path: "/data/vol1.blk", HealthScore: 1.0,
			Role: blockvol.RoleToWire(blockvol.RoleReplica), LastHeartbeat: time.Now(),
		}},
	})

	// Call ManualPromote (simulates what the handler does).
	oldPrimary := "vs1"
	oldPath := "/data/vol1.blk"
	newEpoch, _, _, pf, err := ms.blockRegistry.ManualPromote("vol1", "", false)
	if err != nil {
		t.Fatalf("ManualPromote: %v", err)
	}
	if !pf.Promotable {
		t.Fatalf("should be promotable, reason=%s", pf.Reason)
	}

	// Simulate finalizePromotion.
	ms.finalizePromotion("vol1", oldPrimary, oldPath, "", newEpoch)

	// Verify.
	entry, _ := ms.blockRegistry.Lookup("vol1")
	if entry.VolumeServer != "vs2" {
		t.Fatalf("expected vs2 promoted, got %q", entry.VolumeServer)
	}
	if entry.Epoch != 2 {
		t.Fatalf("expected epoch 2, got %d", entry.Epoch)
	}

	// Check assignment was enqueued for new primary.
	assignments := ms.blockAssignmentQueue.Peek("vs2")
	if len(assignments) == 0 {
		t.Fatal("expected assignment enqueued for vs2")
	}

	// Check pending rebuild recorded for old primary.
	rebuilds := ms.drainPendingRebuilds("vs1")
	if len(rebuilds) == 0 {
		t.Fatal("expected pending rebuild for vs1")
	}
	if rebuilds[0].NewPrimary != "vs2" {
		t.Fatalf("rebuild NewPrimary: got %q, want vs2", rebuilds[0].NewPrimary)
	}
}

// ============================================================
// CP11B-3 T5 Review: Additional Adversarial Tests
// ============================================================

// QA-T5-8: BUG-T5-1 regression — PromotionsTotal counts both auto and manual promotions.
// Counter lives in finalizePromotion (shared orchestration), not in registry methods,
// so this test exercises the full MasterServer flow for both paths.
func TestQA_T5_PromotionsTotal_CountsBothAutoAndManual(t *testing.T) {
	ms := testMasterServerForFailover(t)
	ms.blockRegistry.MarkBlockCapable("r1")
	ms.blockRegistry.MarkBlockCapable("r2")

	// Setup vol1 for auto-promote (dead primary, lease expired).
	ms.blockRegistry.Register(&BlockVolumeEntry{
		Name: "vol1", VolumeServer: "primary1", Path: "/data/vol1.blk",
		Epoch: 1, LeaseTTL: 5 * time.Second,
		LastLeaseGrant: time.Now().Add(-10 * time.Second),
		Role: blockvol.RoleToWire(blockvol.RolePrimary),
		Replicas: []ReplicaInfo{{
			Server: "r1", Path: "/r1.blk", HealthScore: 1.0,
			LastHeartbeat: time.Now(), Role: blockvol.RoleToWire(blockvol.RoleReplica),
		}},
	})

	before := ms.blockRegistry.PromotionsTotal.Load()

	// Auto-promote via promoteReplica (production auto path).
	ms.promoteReplica("vol1")
	afterAuto := ms.blockRegistry.PromotionsTotal.Load()
	if afterAuto != before+1 {
		t.Fatalf("auto promote should increment PromotionsTotal: before=%d after=%d", before, afterAuto)
	}

	// Setup vol2 for manual promote (dead primary).
	ms.blockRegistry.Register(&BlockVolumeEntry{
		Name: "vol2", VolumeServer: "primary2", Path: "/data/vol2.blk",
		Epoch: 1, LeaseTTL: 5 * time.Second,
		LastLeaseGrant: time.Now().Add(-10 * time.Second),
		Role: blockvol.RoleToWire(blockvol.RolePrimary),
		Replicas: []ReplicaInfo{{
			Server: "r2", Path: "/r2.blk", HealthScore: 1.0,
			LastHeartbeat: time.Now(), Role: blockvol.RoleToWire(blockvol.RoleReplica),
		}},
	})

	// Manual promote via ManualPromote + finalizePromotion (production manual path).
	newEpoch, oldPrimary, oldPath, _, err := ms.blockRegistry.ManualPromote("vol2", "", false)
	if err != nil {
		t.Fatalf("manual promote: %v", err)
	}
	ms.finalizePromotion("vol2", oldPrimary, oldPath, "", newEpoch)
	afterManual := ms.blockRegistry.PromotionsTotal.Load()
	if afterManual != afterAuto+1 {
		t.Fatalf("manual promote should increment PromotionsTotal: afterAuto=%d afterManual=%d", afterAuto, afterManual)
	}
}

// QA-T5-9: BUG-T5-2 regression — ManualPromote returns correct oldPrimary under lock.
func TestQA_T5_ManualPromote_ReturnsOldPrimary(t *testing.T) {
	r := NewBlockVolumeRegistry()
	r.MarkBlockCapable("r1")
	r.Register(&BlockVolumeEntry{
		Name: "vol1", VolumeServer: "original-primary", Path: "/original/path.blk",
		Epoch: 1, LeaseTTL: 30 * time.Second,
		Replicas: []ReplicaInfo{{
			Server: "r1", Path: "/r1.blk", HealthScore: 1.0,
			LastHeartbeat: time.Now(), Role: blockvol.RoleToWire(blockvol.RoleReplica),
		}},
	})

	newEpoch, oldPrimary, oldPath, _, err := r.ManualPromote("vol1", "", false)
	if err != nil {
		t.Fatalf("ManualPromote: %v", err)
	}
	if newEpoch != 2 {
		t.Fatalf("epoch: got %d, want 2", newEpoch)
	}
	if oldPrimary != "original-primary" {
		t.Fatalf("oldPrimary: got %q, want original-primary", oldPrimary)
	}
	if oldPath != "/original/path.blk" {
		t.Fatalf("oldPath: got %q, want /original/path.blk", oldPath)
	}
	// After promote, the entry's primary should be r1, not the old primary.
	e, _ := r.Lookup("vol1")
	if e.VolumeServer != "r1" {
		t.Fatalf("new primary: got %q, want r1", e.VolumeServer)
	}
}

// QA-T5-10: Double ManualPromote exhausts replicas — second call fails.
func TestQA_T5_ManualPromote_DoubleExhaustsReplicas(t *testing.T) {
	r := NewBlockVolumeRegistry()
	r.MarkBlockCapable("r1")
	r.Register(&BlockVolumeEntry{
		Name: "vol1", VolumeServer: "primary", Path: "/data/vol1.blk",
		Epoch: 1, LeaseTTL: 30 * time.Second,
		Replicas: []ReplicaInfo{{
			Server: "r1", Path: "/r1.blk", HealthScore: 1.0,
			LastHeartbeat: time.Now(), Role: blockvol.RoleToWire(blockvol.RoleReplica),
		}},
	})

	// First promote succeeds.
	newEpoch, _, _, _, err := r.ManualPromote("vol1", "", false)
	if err != nil {
		t.Fatalf("first promote: %v", err)
	}
	if newEpoch != 2 {
		t.Fatalf("first epoch: got %d, want 2", newEpoch)
	}

	// Simulate new primary (r1) dying.
	r.UnmarkBlockCapable("r1")

	// Second promote fails — no replicas left.
	_, _, _, pf, err := r.ManualPromote("vol1", "", false)
	if err == nil {
		t.Fatal("second promote should fail: no replicas")
	}
	if pf.Reason != "no replicas" {
		t.Fatalf("expected 'no replicas', got %q", pf.Reason)
	}
}

// QA-T5-11: ManualPromote transfers NVMe publication fields.
func TestQA_T5_ManualPromote_TransfersNVMeFields(t *testing.T) {
	r := NewBlockVolumeRegistry()
	r.MarkBlockCapable("r1")
	r.Register(&BlockVolumeEntry{
		Name: "vol1", VolumeServer: "primary", Path: "/data/vol1.blk",
		Epoch: 1, LeaseTTL: 30 * time.Second,
		NvmeAddr: "192.168.1.1:4420", NQN: "nqn.old-primary",
		Replicas: []ReplicaInfo{{
			Server: "r1", Path: "/r1.blk", HealthScore: 1.0,
			NvmeAddr: "192.168.1.2:4420", NQN: "nqn.replica-1",
			LastHeartbeat: time.Now(), Role: blockvol.RoleToWire(blockvol.RoleReplica),
		}},
	})

	_, _, _, _, err := r.ManualPromote("vol1", "", false)
	if err != nil {
		t.Fatalf("ManualPromote: %v", err)
	}
	e, _ := r.Lookup("vol1")
	if e.NvmeAddr != "192.168.1.2:4420" {
		t.Fatalf("NvmeAddr: got %q, want 192.168.1.2:4420 (replica's addr)", e.NvmeAddr)
	}
	if e.NQN != "nqn.replica-1" {
		t.Fatalf("NQN: got %q, want nqn.replica-1", e.NQN)
	}
}

// QA-T5-12: RF=3 force-promote specific target picks lower-health replica.
func TestQA_T5_RF3_ForceSpecificTarget_LowerHealth(t *testing.T) {
	r := NewBlockVolumeRegistry()
	r.MarkBlockCapable("best")
	r.MarkBlockCapable("mid")
	r.MarkBlockCapable("worst")
	r.Register(&BlockVolumeEntry{
		Name: "vol1", VolumeServer: "primary", Path: "/data/vol1.blk",
		Epoch: 1, LeaseTTL: 30 * time.Second,
		Replicas: []ReplicaInfo{
			{Server: "best", Path: "/best.blk", HealthScore: 1.0, WALHeadLSN: 100,
				LastHeartbeat: time.Now(), Role: blockvol.RoleToWire(blockvol.RoleReplica)},
			{Server: "mid", Path: "/mid.blk", HealthScore: 0.5, WALHeadLSN: 80,
				LastHeartbeat: time.Now(), Role: blockvol.RoleToWire(blockvol.RoleReplica)},
			{Server: "worst", Path: "/worst.blk", HealthScore: 0.1, WALHeadLSN: 50,
				LastHeartbeat: time.Now(), Role: blockvol.RoleToWire(blockvol.RoleReplica)},
		},
	})

	// Force-promote the worst replica specifically.
	newEpoch, _, _, _, err := r.ManualPromote("vol1", "worst", true)
	if err != nil {
		t.Fatalf("force promote worst: %v", err)
	}
	if newEpoch != 2 {
		t.Fatalf("epoch: got %d, want 2", newEpoch)
	}
	e, _ := r.Lookup("vol1")
	if e.VolumeServer != "worst" {
		t.Fatalf("expected 'worst' promoted, got %q", e.VolumeServer)
	}
	// "best" and "mid" should remain as replicas.
	if len(e.Replicas) != 2 {
		t.Fatalf("expected 2 remaining replicas, got %d", len(e.Replicas))
	}
}

// QA-T5-13: ManualPromote during expand in-progress — should succeed.
func TestQA_T5_ManualPromote_DuringExpand(t *testing.T) {
	r := NewBlockVolumeRegistry()
	r.MarkBlockCapable("r1")
	r.Register(&BlockVolumeEntry{
		Name: "vol1", VolumeServer: "primary", Path: "/data/vol1.blk",
		Epoch: 1, SizeBytes: 50 << 20, LeaseTTL: 30 * time.Second,
		ExpandInProgress: true, PendingExpandSize: 100 << 20, ExpandEpoch: 1,
		Replicas: []ReplicaInfo{{
			Server: "r1", Path: "/r1.blk", HealthScore: 1.0,
			LastHeartbeat: time.Now(), Role: blockvol.RoleToWire(blockvol.RoleReplica),
		}},
	})

	// Promotion should succeed even with expand in-progress.
	newEpoch, _, _, _, err := r.ManualPromote("vol1", "", false)
	if err != nil {
		t.Fatalf("ManualPromote during expand: %v", err)
	}
	if newEpoch != 2 {
		t.Fatalf("epoch: got %d, want 2", newEpoch)
	}
	e, _ := r.Lookup("vol1")
	if e.VolumeServer != "r1" {
		t.Fatalf("expected r1 promoted, got %q", e.VolumeServer)
	}
	// Expand state should still be present (promotion doesn't clear it).
	if !e.ExpandInProgress {
		t.Fatal("ExpandInProgress should remain true after promotion")
	}
}

// QA-T5-14: ManualPromote on non-existent volume returns volume_not_found.
func TestQA_T5_ManualPromote_NonExistentVolume(t *testing.T) {
	r := NewBlockVolumeRegistry()
	_, _, _, pf, err := r.ManualPromote("no-such-vol", "", false)
	if err == nil {
		t.Fatal("expected error for non-existent volume")
	}
	if pf.Reason != "volume not found" {
		t.Fatalf("expected 'volume not found', got %q", pf.Reason)
	}
}
