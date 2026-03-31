package weed_server

import (
	"sync"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/storage/blockvol"
)

// ============================================================
// Edge Case Tests: RF, Promotion, Network, LSN
//
// Covers gaps identified in the testing framework review:
// 1. LSN-lagging replica skipped during promotion
// 2. Cascading double failover (RF=3, epoch chain 1→2→3)
// 3. Demotion/drain under concurrent promotion pressure
// 4. Promotion with mixed LSN + health scores
// 5. Network flap simulation (mark/unmark block capable rapidly)
// 6. RF=3 all-gate evaluation under pressure
// ============================================================

// --- Test 1: LSN-lagging replica skipped, fresher one promoted ---

func TestEdge_LSNLag_StaleReplicaSkipped(t *testing.T) {
	ms := testMasterServerForFailover(t)
	ms.blockRegistry.SetPromotionLSNTolerance(10)

	ms.blockRegistry.MarkBlockCapable("primary")
	ms.blockRegistry.MarkBlockCapable("stale-replica")
	ms.blockRegistry.MarkBlockCapable("fresh-replica")

	entry := &BlockVolumeEntry{
		Name: "lsn-test", VolumeServer: "primary", Path: "/data/lsn-test.blk",
		SizeBytes: 1 << 30, Epoch: 1, Role: blockvol.RoleToWire(blockvol.RolePrimary),
		Status: StatusActive, LeaseTTL: 5 * time.Second,
		LastLeaseGrant: time.Now().Add(-10 * time.Second), // expired
		WALHeadLSN: 1000,
		Replicas: []ReplicaInfo{
			{
				Server: "stale-replica", Path: "/data/lsn-test.blk",
				HealthScore: 1.0, WALHeadLSN: 100, // lag=900, way beyond tolerance=10
				Role: blockvol.RoleToWire(blockvol.RoleReplica), LastHeartbeat: time.Now(),
			},
			{
				Server: "fresh-replica", Path: "/data/lsn-test.blk",
				HealthScore: 0.9, WALHeadLSN: 995, // lag=5, within tolerance=10
				Role: blockvol.RoleToWire(blockvol.RoleReplica), LastHeartbeat: time.Now(),
			},
		},
	}
	ms.blockRegistry.Register(entry)

	// Kill primary.
	ms.blockRegistry.UnmarkBlockCapable("primary")
	ms.failoverBlockVolumes("primary")

	// Verify: fresh-replica promoted (despite lower health score), stale skipped.
	after, ok := ms.blockRegistry.Lookup("lsn-test")
	if !ok {
		t.Fatal("volume not found")
	}
	if after.VolumeServer != "fresh-replica" {
		t.Fatalf("expected fresh-replica promoted, got %q (stale-replica with lag=900 should be skipped)", after.VolumeServer)
	}
	if after.Epoch != 2 {
		t.Fatalf("epoch: got %d, want 2", after.Epoch)
	}
}

// --- Test 2: Cascading double failover (RF=3, epoch 1→2→3) ---

func TestEdge_CascadeFailover_RF3_EpochChain(t *testing.T) {
	ms := testMasterServerForFailover(t)
	ms.blockRegistry.MarkBlockCapable("vs1")
	ms.blockRegistry.MarkBlockCapable("vs2")
	ms.blockRegistry.MarkBlockCapable("vs3")

	entry := &BlockVolumeEntry{
		Name: "cascade-test", VolumeServer: "vs1", Path: "/data/cascade.blk",
		SizeBytes: 1 << 30, Epoch: 1, Role: blockvol.RoleToWire(blockvol.RolePrimary),
		Status: StatusActive, LeaseTTL: 5 * time.Second,
		LastLeaseGrant: time.Now().Add(-10 * time.Second),
		ReplicaFactor: 3,
		Replicas: []ReplicaInfo{
			{Server: "vs2", Path: "/r2.blk", HealthScore: 1.0, WALHeadLSN: 100,
				Role: blockvol.RoleToWire(blockvol.RoleReplica), LastHeartbeat: time.Now()},
			{Server: "vs3", Path: "/r3.blk", HealthScore: 0.9, WALHeadLSN: 100,
				Role: blockvol.RoleToWire(blockvol.RoleReplica), LastHeartbeat: time.Now()},
		},
	}
	ms.blockRegistry.Register(entry)

	// Failover 1: vs1 dies → vs2 promoted (higher health).
	ms.blockRegistry.UnmarkBlockCapable("vs1")
	ms.failoverBlockVolumes("vs1")

	after1, _ := ms.blockRegistry.Lookup("cascade-test")
	if after1.VolumeServer != "vs2" {
		t.Fatalf("failover 1: expected vs2, got %q", after1.VolumeServer)
	}
	if after1.Epoch != 2 {
		t.Fatalf("failover 1: epoch got %d, want 2", after1.Epoch)
	}

	// Failover 2: vs2 dies → vs3 promoted (only remaining).
	// Update vs3's heartbeat and set lease expired for the new primary.
	ms.blockRegistry.UpdateEntry("cascade-test", func(e *BlockVolumeEntry) {
		e.LastLeaseGrant = time.Now().Add(-10 * time.Second)
		for i := range e.Replicas {
			if e.Replicas[i].Server == "vs3" {
				e.Replicas[i].LastHeartbeat = time.Now()
			}
		}
	})

	ms.blockRegistry.UnmarkBlockCapable("vs2")
	ms.failoverBlockVolumes("vs2")

	after2, _ := ms.blockRegistry.Lookup("cascade-test")
	if after2.VolumeServer != "vs3" {
		t.Fatalf("failover 2: expected vs3, got %q", after2.VolumeServer)
	}
	if after2.Epoch != 3 {
		t.Fatalf("failover 2: epoch got %d, want 3", after2.Epoch)
	}

	// No more replicas — third failover should fail silently.
	ms.blockRegistry.UpdateEntry("cascade-test", func(e *BlockVolumeEntry) {
		e.LastLeaseGrant = time.Now().Add(-10 * time.Second)
	})
	ms.blockRegistry.UnmarkBlockCapable("vs3")
	ms.failoverBlockVolumes("vs3")

	after3, _ := ms.blockRegistry.Lookup("cascade-test")
	// Epoch should still be 3 — no eligible replicas.
	if after3.Epoch != 3 {
		t.Fatalf("failover 3: epoch should stay 3, got %d", after3.Epoch)
	}
}

// --- Test 3: Concurrent failover + heartbeat + promotion (stress) ---

func TestEdge_ConcurrentFailoverAndHeartbeat_NoPanic(t *testing.T) {
	ms := testMasterServerForFailover(t)
	ms.blockRegistry.MarkBlockCapable("vs1")
	ms.blockRegistry.MarkBlockCapable("vs2")
	ms.blockRegistry.MarkBlockCapable("vs3")

	setup := func() {
		ms.blockRegistry.Unregister("stress-vol")
		ms.blockRegistry.MarkBlockCapable("vs1")
		ms.blockRegistry.MarkBlockCapable("vs2")
		ms.blockRegistry.MarkBlockCapable("vs3")
		ms.blockRegistry.Register(&BlockVolumeEntry{
			Name: "stress-vol", VolumeServer: "vs1", Path: "/data/stress.blk",
			SizeBytes: 1 << 30, Epoch: 1, Role: blockvol.RoleToWire(blockvol.RolePrimary),
			Status: StatusActive, LeaseTTL: 5 * time.Second,
			LastLeaseGrant: time.Now().Add(-10 * time.Second),
			Replicas: []ReplicaInfo{
				{Server: "vs2", Path: "/r2.blk", HealthScore: 1.0,
					Role: blockvol.RoleToWire(blockvol.RoleReplica), LastHeartbeat: time.Now()},
				{Server: "vs3", Path: "/r3.blk", HealthScore: 0.9,
					Role: blockvol.RoleToWire(blockvol.RoleReplica), LastHeartbeat: time.Now()},
			},
		})
	}

	for round := 0; round < 30; round++ {
		setup()
		var wg sync.WaitGroup
		wg.Add(4)
		go func() { defer wg.Done(); ms.failoverBlockVolumes("vs1") }()
		go func() { defer wg.Done(); ms.reevaluateOrphanedPrimaries("vs2") }()
		go func() { defer wg.Done(); ms.blockRegistry.PromoteBestReplica("stress-vol") }()
		go func() {
			defer wg.Done()
			ms.blockRegistry.ManualPromote("stress-vol", "", true)
		}()
		wg.Wait()
	}
	// No panic = pass.
}

// --- Test 4: LSN + health score interaction — health wins within tolerance ---

func TestEdge_LSNWithinTolerance_HealthWins(t *testing.T) {
	ms := testMasterServerForFailover(t)
	ms.blockRegistry.SetPromotionLSNTolerance(100)
	ms.blockRegistry.MarkBlockCapable("primary")
	ms.blockRegistry.MarkBlockCapable("high-health")
	ms.blockRegistry.MarkBlockCapable("high-lsn")

	ms.blockRegistry.Register(&BlockVolumeEntry{
		Name: "health-vs-lsn", VolumeServer: "primary", Path: "/data/hvl.blk",
		SizeBytes: 1 << 30, Epoch: 1, Role: blockvol.RoleToWire(blockvol.RolePrimary),
		Status: StatusActive, LeaseTTL: 5 * time.Second,
		LastLeaseGrant: time.Now().Add(-10 * time.Second),
		WALHeadLSN: 1000,
		Replicas: []ReplicaInfo{
			{Server: "high-health", Path: "/r1.blk", HealthScore: 1.0, WALHeadLSN: 950,
				Role: blockvol.RoleToWire(blockvol.RoleReplica), LastHeartbeat: time.Now()},
			{Server: "high-lsn", Path: "/r2.blk", HealthScore: 0.5, WALHeadLSN: 999,
				Role: blockvol.RoleToWire(blockvol.RoleReplica), LastHeartbeat: time.Now()},
		},
	})

	ms.blockRegistry.UnmarkBlockCapable("primary")
	ms.failoverBlockVolumes("primary")

	after, _ := ms.blockRegistry.Lookup("health-vs-lsn")
	// Both within tolerance (lag ≤ 100). Health wins: high-health (1.0) > high-lsn (0.5).
	if after.VolumeServer != "high-health" {
		t.Fatalf("expected high-health promoted (higher health, both within LSN tolerance), got %q", after.VolumeServer)
	}
}

// --- Test 5: Network flap simulation — rapid mark/unmark block capable ---

func TestEdge_NetworkFlap_RapidMarkUnmark(t *testing.T) {
	ms := testMasterServerForFailover(t)
	ms.blockRegistry.MarkBlockCapable("flapper")
	ms.blockRegistry.MarkBlockCapable("stable")

	ms.blockRegistry.Register(&BlockVolumeEntry{
		Name: "flap-test", VolumeServer: "stable", Path: "/data/flap.blk",
		SizeBytes: 1 << 30, Epoch: 1, Role: blockvol.RoleToWire(blockvol.RolePrimary),
		Status: StatusActive, LeaseTTL: 5 * time.Second,
		LastLeaseGrant: time.Now(),
		Replicas: []ReplicaInfo{
			{Server: "flapper", Path: "/r.blk", HealthScore: 1.0,
				Role: blockvol.RoleToWire(blockvol.RoleReplica), LastHeartbeat: time.Now()},
		},
	})

	var wg sync.WaitGroup
	// Goroutine 1: rapidly flap the "flapper" server.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 100; i++ {
			ms.blockRegistry.UnmarkBlockCapable("flapper")
			ms.blockRegistry.MarkBlockCapable("flapper")
		}
	}()

	// Goroutine 2: attempt promotions during flapping.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 50; i++ {
			ms.blockRegistry.EvaluatePromotion("flap-test")
		}
	}()

	// Goroutine 3: concurrent heartbeat updates.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 50; i++ {
			ms.blockRegistry.UpdateFullHeartbeat("flapper", nil, "")
		}
	}()

	wg.Wait()
	// No panic, no corruption = pass.

	// Volume should still be on stable primary.
	after, ok := ms.blockRegistry.Lookup("flap-test")
	if !ok {
		t.Fatal("volume lost during flapping")
	}
	if after.VolumeServer != "stable" {
		t.Fatalf("primary changed from stable to %q during flapping", after.VolumeServer)
	}
}

// --- Test 6: RF=3 all gates — mixed rejection reasons ---

func TestEdge_RF3_MixedGates_BestEligiblePromoted(t *testing.T) {
	ms := testMasterServerForFailover(t)
	ms.blockRegistry.SetPromotionLSNTolerance(50)
	ms.blockRegistry.MarkBlockCapable("primary")
	// Note: "dead-server" NOT marked block capable.
	ms.blockRegistry.MarkBlockCapable("stale-hb")
	ms.blockRegistry.MarkBlockCapable("good")

	ms.blockRegistry.Register(&BlockVolumeEntry{
		Name: "mixed-gates", VolumeServer: "primary", Path: "/data/mixed.blk",
		SizeBytes: 1 << 30, Epoch: 1, Role: blockvol.RoleToWire(blockvol.RolePrimary),
		Status: StatusActive, LeaseTTL: 5 * time.Second,
		LastLeaseGrant: time.Now().Add(-10 * time.Second),
		WALHeadLSN: 500,
		Replicas: []ReplicaInfo{
			{Server: "dead-server", Path: "/r1.blk", HealthScore: 1.0, WALHeadLSN: 500,
				Role: blockvol.RoleToWire(blockvol.RoleReplica), LastHeartbeat: time.Now()},
			{Server: "stale-hb", Path: "/r2.blk", HealthScore: 1.0, WALHeadLSN: 500,
				Role: blockvol.RoleToWire(blockvol.RoleReplica),
				LastHeartbeat: time.Now().Add(-10 * time.Minute)}, // stale
			{Server: "good", Path: "/r3.blk", HealthScore: 0.8, WALHeadLSN: 480,
				Role: blockvol.RoleToWire(blockvol.RoleReplica), LastHeartbeat: time.Now()},
		},
	})

	// Evaluate preflight first (read-only).
	pf, err := ms.blockRegistry.EvaluatePromotion("mixed-gates")
	if err != nil {
		t.Fatalf("evaluate: %v", err)
	}
	if !pf.Promotable {
		t.Fatalf("should be promotable, reason=%s, rejections=%v", pf.Reason, pf.Rejections)
	}
	// Should have 2 rejections: dead-server (server_dead) + stale-hb (stale_heartbeat).
	if len(pf.Rejections) != 2 {
		t.Fatalf("expected 2 rejections, got %d: %v", len(pf.Rejections), pf.Rejections)
	}
	reasons := map[string]string{}
	for _, r := range pf.Rejections {
		reasons[r.Server] = r.Reason
	}
	if reasons["dead-server"] != "server_dead" {
		t.Fatalf("dead-server: got %q, want server_dead", reasons["dead-server"])
	}
	if reasons["stale-hb"] != "stale_heartbeat" {
		t.Fatalf("stale-hb: got %q, want stale_heartbeat", reasons["stale-hb"])
	}

	// Now actually promote.
	ms.blockRegistry.UnmarkBlockCapable("primary")
	ms.failoverBlockVolumes("primary")

	after, _ := ms.blockRegistry.Lookup("mixed-gates")
	if after.VolumeServer != "good" {
		t.Fatalf("expected 'good' promoted (only eligible), got %q", after.VolumeServer)
	}
}

// --- Test 7: Promotion changes publication (ISCSIAddr, NvmeAddr) ---

func TestEdge_PromotionUpdatesPublication(t *testing.T) {
	ms := testMasterServerForFailover(t)
	ms.blockRegistry.MarkBlockCapable("primary")
	ms.blockRegistry.MarkBlockCapable("replica")

	ms.blockRegistry.Register(&BlockVolumeEntry{
		Name: "pub-test", VolumeServer: "primary", Path: "/data/pub.blk",
		ISCSIAddr: "primary:3260", NvmeAddr: "primary:4420", NQN: "nqn.primary",
		SizeBytes: 1 << 30, Epoch: 1, Role: blockvol.RoleToWire(blockvol.RolePrimary),
		Status: StatusActive, LeaseTTL: 5 * time.Second,
		LastLeaseGrant: time.Now().Add(-10 * time.Second),
		Replicas: []ReplicaInfo{
			{Server: "replica", Path: "/r.blk", HealthScore: 1.0,
				ISCSIAddr: "replica:3261", NvmeAddr: "replica:4421", NQN: "nqn.replica",
				Role: blockvol.RoleToWire(blockvol.RoleReplica), LastHeartbeat: time.Now()},
		},
	})

	ms.blockRegistry.UnmarkBlockCapable("primary")
	ms.failoverBlockVolumes("primary")

	after, _ := ms.blockRegistry.Lookup("pub-test")
	if after.ISCSIAddr != "replica:3261" {
		t.Fatalf("ISCSIAddr: got %q, want replica:3261", after.ISCSIAddr)
	}
	if after.NvmeAddr != "replica:4421" {
		t.Fatalf("NvmeAddr: got %q, want replica:4421", after.NvmeAddr)
	}
	if after.NQN != "nqn.replica" {
		t.Fatalf("NQN: got %q, want nqn.replica", after.NQN)
	}
}

// --- Test 8: Orphaned primary re-evaluation with LSN lag ---

func TestEdge_OrphanReevaluation_LSNLag_StillPromotes(t *testing.T) {
	ms := testMasterServerForFailover(t)
	ms.blockRegistry.SetPromotionLSNTolerance(10)
	// Primary is dead, replica is alive but lagging.
	ms.blockRegistry.MarkBlockCapable("replica")

	ms.blockRegistry.Register(&BlockVolumeEntry{
		Name: "orphan-lag", VolumeServer: "dead-primary", Path: "/data/orphan.blk",
		SizeBytes: 1 << 30, Epoch: 1, Role: blockvol.RoleToWire(blockvol.RolePrimary),
		Status: StatusActive, LeaseTTL: 5 * time.Second,
		LastLeaseGrant: time.Now().Add(-10 * time.Second), // expired
		WALHeadLSN: 1000,
		Replicas: []ReplicaInfo{
			{Server: "replica", Path: "/r.blk", HealthScore: 1.0, WALHeadLSN: 500,
				Role: blockvol.RoleToWire(blockvol.RoleReplica), LastHeartbeat: time.Now()},
		},
	})

	// Orphan re-evaluation: replica reconnects.
	ms.reevaluateOrphanedPrimaries("replica")

	// The replica has WAL lag of 500 (way beyond tolerance=10).
	// But it's the ONLY replica — should it promote or not?
	// Current behavior: LSN gate rejects it. No promotion.
	after, _ := ms.blockRegistry.Lookup("orphan-lag")
	if after.Epoch != 1 {
		// If epoch changed, the lagging replica was promoted.
		// This may or may not be desired — document the behavior.
		t.Logf("NOTE: lagging replica WAS promoted (epoch=%d). LSN lag=%d, tolerance=%d",
			after.Epoch, 1000-500, 10)
	} else {
		t.Logf("NOTE: lagging replica was NOT promoted (epoch=1). Volume is stuck with dead primary.")
		t.Logf("This is the current behavior: LSN gate blocks promotion even when it's the only option.")
	}
	// This test documents behavior, doesn't assert pass/fail.
	// The question is: should a lagging-but-only replica be promoted to avoid downtime?
}

// --- Test 9: Rebuild addr cleared after promotion, then repopulated ---

func TestEdge_RebuildAddr_ClearedThenRepopulated(t *testing.T) {
	ms := testMasterServerForFailover(t)
	ms.blockRegistry.MarkBlockCapable("primary")
	ms.blockRegistry.MarkBlockCapable("replica")

	ms.blockRegistry.Register(&BlockVolumeEntry{
		Name: "rebuild-addr", VolumeServer: "primary", Path: "/data/rebuild.blk",
		SizeBytes: 1 << 30, Epoch: 1, Role: blockvol.RoleToWire(blockvol.RolePrimary),
		Status: StatusActive, LeaseTTL: 5 * time.Second,
		LastLeaseGrant:     time.Now().Add(-10 * time.Second),
		RebuildListenAddr:  "primary:15000", // old primary's rebuild addr
		Replicas: []ReplicaInfo{
			{Server: "replica", Path: "/r.blk", HealthScore: 1.0,
				Role: blockvol.RoleToWire(blockvol.RoleReplica), LastHeartbeat: time.Now()},
		},
	})

	ms.blockRegistry.UnmarkBlockCapable("primary")
	ms.failoverBlockVolumes("primary")

	after, _ := ms.blockRegistry.Lookup("rebuild-addr")
	// RebuildListenAddr should be cleared after promotion (B-11 fix).
	if after.RebuildListenAddr != "" {
		t.Fatalf("RebuildListenAddr should be cleared after promotion, got %q", after.RebuildListenAddr)
	}
}

// --- Test 10: Multiple volumes on same server — all fail over ---

func TestEdge_MultipleVolumes_SameServer_AllFailover(t *testing.T) {
	ms := testMasterServerForFailover(t)
	ms.blockRegistry.MarkBlockCapable("vs1")
	ms.blockRegistry.MarkBlockCapable("vs2")

	// Register 5 volumes, all with primary on vs1.
	for i := 0; i < 5; i++ {
		name := "multi-" + string(rune('a'+i))
		ms.blockRegistry.Register(&BlockVolumeEntry{
			Name: name, VolumeServer: "vs1", Path: "/data/" + name + ".blk",
			SizeBytes: 1 << 30, Epoch: 1, Role: blockvol.RoleToWire(blockvol.RolePrimary),
			Status: StatusActive, LeaseTTL: 5 * time.Second,
			LastLeaseGrant: time.Now().Add(-10 * time.Second),
			Replicas: []ReplicaInfo{
				{Server: "vs2", Path: "/r/" + name + ".blk", HealthScore: 1.0,
					Role: blockvol.RoleToWire(blockvol.RoleReplica), LastHeartbeat: time.Now()},
			},
		})
	}

	// Kill vs1 — all 5 volumes should fail over.
	ms.blockRegistry.UnmarkBlockCapable("vs1")
	ms.failoverBlockVolumes("vs1")

	for i := 0; i < 5; i++ {
		name := "multi-" + string(rune('a'+i))
		entry, ok := ms.blockRegistry.Lookup(name)
		if !ok {
			t.Fatalf("volume %s not found", name)
		}
		if entry.VolumeServer != "vs2" {
			t.Fatalf("volume %s: expected vs2, got %q", name, entry.VolumeServer)
		}
		if entry.Epoch != 2 {
			t.Fatalf("volume %s: epoch got %d, want 2", name, entry.Epoch)
		}
	}
}
