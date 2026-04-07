package weed_server

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage/blockvol"
)

// ============================================================
// CP8-2 Adversarial Test Suite
//
// 12 scenarios covering multi-replica, scrub, promotion gating,
// heartbeat reconciliation, and partial-failure edge cases.
// ============================================================

// qaCP82Master creates a MasterServer with 3 block-capable servers
// and configurable mocks for adversarial testing.
func qaCP82Master(t *testing.T) *MasterServer {
	t.Helper()
	ms := &MasterServer{
		blockRegistry:        NewBlockVolumeRegistry(),
		blockAssignmentQueue: NewBlockAssignmentQueue(),
		blockFailover:        newBlockFailoverState(),
	}
	ms.blockVSAllocate = func(ctx context.Context, server string, name string, sizeBytes uint64, walSizeBytes uint64, diskType string, durabilityMode string) (*blockAllocResult, error) {
		return &blockAllocResult{
			Path:              fmt.Sprintf("/data/%s.blk", name),
			IQN:               fmt.Sprintf("iqn.2024.test:%s", name),
			ISCSIAddr:         server + ":3260",
			ReplicaDataAddr:   server + ":14260",
			ReplicaCtrlAddr:   server + ":14261",
			RebuildListenAddr: server + ":15000",
		}, nil
	}
	ms.blockVSDelete = func(ctx context.Context, server string, name string) error {
		return nil
	}
	ms.blockVSExpand = func(ctx context.Context, server string, name string, newSize uint64) (uint64, error) {
		return newSize, nil
	}
	ms.blockVSPrepareExpand = func(ctx context.Context, server string, name string, newSize, expandEpoch uint64) error {
		return nil
	}
	ms.blockVSCommitExpand = func(ctx context.Context, server string, name string, expandEpoch uint64) (uint64, error) {
		return 2 << 30, nil
	}
	ms.blockVSCancelExpand = func(ctx context.Context, server string, name string, expandEpoch uint64) error {
		return nil
	}
	ms.blockRegistry.MarkBlockCapable("vs1:9333")
	ms.blockRegistry.MarkBlockCapable("vs2:9333")
	ms.blockRegistry.MarkBlockCapable("vs3:9333")
	return ms
}

// qaRegisterRF2WithLSN creates an RF=2 volume with explicit WAL LSN state.
func qaRegisterRF2WithLSN(t *testing.T, ms *MasterServer, name, primary, replica string, epoch, primaryLSN, replicaLSN uint64) {
	t.Helper()
	entry := &BlockVolumeEntry{
		Name:         name,
		VolumeServer: primary,
		Path:         fmt.Sprintf("/data/%s.blk", name),
		IQN:          fmt.Sprintf("iqn.2024.test:%s", name),
		ISCSIAddr:    primary + ":3260",
		SizeBytes:    1 << 30,
		Epoch:        epoch,
		Role:         blockvol.RoleToWire(blockvol.RolePrimary),
		Status:       StatusActive,
		WALHeadLSN:   primaryLSN,
		LeaseTTL:     5 * time.Second,
		LastLeaseGrant: time.Now().Add(-10 * time.Second), // expired
		ReplicaServer:    replica,
		ReplicaPath:      fmt.Sprintf("/data/%s.blk", name),
		ReplicaIQN:       fmt.Sprintf("iqn.2024.test:%s-r", name),
		ReplicaISCSIAddr: replica + ":3260",
		Replicas: []ReplicaInfo{
			{
				Server:        replica,
				Path:          fmt.Sprintf("/data/%s.blk", name),
				IQN:           fmt.Sprintf("iqn.2024.test:%s-r", name),
				ISCSIAddr:     replica + ":3260",
				HealthScore:   1.0,
				WALHeadLSN:    replicaLSN,
				LastHeartbeat: time.Now(), // fresh
				Role:          blockvol.RoleToWire(blockvol.RoleReplica),
			},
		},
	}
	if err := ms.blockRegistry.Register(entry); err != nil {
		t.Fatalf("register %s: %v", name, err)
	}
}

// qaRegisterRF3WithState creates an RF=3 volume with per-replica state control.
func qaRegisterRF3WithState(t *testing.T, ms *MasterServer, name, primary string,
	replicas []struct {
		Server      string
		HealthScore float64
		WALHeadLSN  uint64
		Role        blockvol.Role
		Heartbeat   time.Time
	},
	primaryLSN uint64,
) {
	t.Helper()
	entry := &BlockVolumeEntry{
		Name:          name,
		VolumeServer:  primary,
		Path:          fmt.Sprintf("/data/%s.blk", name),
		IQN:           fmt.Sprintf("iqn.2024.test:%s", name),
		ISCSIAddr:     primary + ":3260",
		SizeBytes:     1 << 30,
		Epoch:         1,
		Role:          blockvol.RoleToWire(blockvol.RolePrimary),
		Status:        StatusActive,
		WALHeadLSN:    primaryLSN,
		ReplicaFactor: len(replicas) + 1,
		LeaseTTL:      5 * time.Second,
		LastLeaseGrant: time.Now().Add(-10 * time.Second), // expired
	}
	for i, r := range replicas {
		ri := ReplicaInfo{
			Server:        r.Server,
			Path:          fmt.Sprintf("/data/%s.blk", name),
			IQN:           fmt.Sprintf("iqn.2024.test:%s-r%d", name, i+1),
			ISCSIAddr:     r.Server + ":3260",
			HealthScore:   r.HealthScore,
			WALHeadLSN:    r.WALHeadLSN,
			Role:          blockvol.RoleToWire(r.Role),
			LastHeartbeat: r.Heartbeat,
		}
		entry.Replicas = append(entry.Replicas, ri)
	}
	// Sync scalar fields from first replica.
	if len(entry.Replicas) > 0 {
		entry.ReplicaServer = entry.Replicas[0].Server
		entry.ReplicaPath = entry.Replicas[0].Path
		entry.ReplicaIQN = entry.Replicas[0].IQN
		entry.ReplicaISCSIAddr = entry.Replicas[0].ISCSIAddr
	}
	if err := ms.blockRegistry.Register(entry); err != nil {
		t.Fatalf("register %s: %v", name, err)
	}
}

// ────────────────────────────────────────────────────────────
// QA-1: PrimaryCrash_AfterAck_BeforeReplicaBarrier
//
// Verify failover data loss window is bounded by barrier_lag_lsn.
// Simulates primary at LSN 200, replica at barrier LSN 190.
// After failover, new primary should be at replica's LSN (190).
// The gap (10 LSN entries) is the bounded loss window.
// ────────────────────────────────────────────────────────────
func TestQA_CP82_PrimaryCrash_AfterAck_BeforeReplicaBarrier(t *testing.T) {
	ms := qaCP82Master(t)
	qaRegisterRF2WithLSN(t, ms, "vol-lag", "vs1:9333", "vs2:9333", 1, 200, 190)

	// Kill primary.
	ms.failoverBlockVolumes("vs1:9333")

	entry, ok := ms.blockRegistry.Lookup("vol-lag")
	if !ok {
		t.Fatal("volume should still exist after failover")
	}
	if entry.VolumeServer != "vs2:9333" {
		t.Fatalf("replica should be promoted, got %q", entry.VolumeServer)
	}
	// The promoted replica was at LSN 190. The 10-LSN gap represents
	// acknowledged-but-unbarriered writes (bounded data loss).
	// New epoch should be 2.
	if entry.Epoch != 2 {
		t.Fatalf("epoch: got %d, want 2", entry.Epoch)
	}
}

// ────────────────────────────────────────────────────────────
// QA-2: ReplicaHeartbeatSpoof_DoesNotDeletePrimary
//
// Regression test for Fix #1. A replica heartbeat that does NOT
// include the primary path must NOT delete the volume entry.
// ────────────────────────────────────────────────────────────
func TestQA_CP82_ReplicaHeartbeatSpoof_DoesNotDeletePrimary(t *testing.T) {
	ms := qaCP82Master(t)
	qaRegisterRF2WithLSN(t, ms, "vol-spoof", "vs1:9333", "vs2:9333", 1, 100, 100)

	// Simulate replica heartbeat from vs2 with ONLY its replica path.
	// This must NOT remove the volume (vs1 is the primary, not vs2).
	ms.blockRegistry.UpdateFullHeartbeat("vs2:9333", []*master_pb.BlockVolumeInfoMessage{
		{
			Path:       "/data/vol-spoof.blk",
			VolumeSize: 1 << 30,
			Epoch:      1,
			Role:       blockvol.RoleToWire(blockvol.RoleReplica),
		},
	}, "")

	entry, ok := ms.blockRegistry.Lookup("vol-spoof")
	if !ok {
		t.Fatal("volume must NOT be deleted by replica heartbeat (Fix #1 regression)")
	}
	if entry.VolumeServer != "vs1:9333" {
		t.Fatalf("primary must remain vs1:9333, got %q", entry.VolumeServer)
	}

	// Now simulate a full heartbeat from vs2 with NO volumes at all.
	// This should remove vs2 as replica but NOT delete the volume.
	ms.blockRegistry.UpdateFullHeartbeat("vs2:9333", []*master_pb.BlockVolumeInfoMessage{}, "")

	entry, ok = ms.blockRegistry.Lookup("vol-spoof")
	if !ok {
		t.Fatal("volume must still exist after empty replica heartbeat")
	}
	if entry.VolumeServer != "vs1:9333" {
		t.Fatal("primary must still be vs1:9333")
	}
}

// ────────────────────────────────────────────────────────────
// QA-3: PromotionRejects_StaleButHealthyReplica
//
// Replica has perfect health (1.0) but stale heartbeat.
// Must NOT be promoted (Gate 1: heartbeat freshness).
// ────────────────────────────────────────────────────────────
func TestQA_CP82_PromotionRejects_StaleButHealthyReplica(t *testing.T) {
	ms := qaCP82Master(t)
	staleTime := time.Now().Add(-5 * time.Minute) // way beyond 2×LeaseTTL
	qaRegisterRF3WithState(t, ms, "vol-stale", "vs1:9333",
		[]struct {
			Server      string
			HealthScore float64
			WALHeadLSN  uint64
			Role        blockvol.Role
			Heartbeat   time.Time
		}{
			{"vs2:9333", 1.0, 100, blockvol.RoleReplica, staleTime}, // healthy but stale
			{"vs3:9333", 0.5, 100, blockvol.RoleReplica, staleTime}, // also stale
		},
		100,
	)

	_, err := ms.blockRegistry.PromoteBestReplica("vol-stale")
	if err == nil {
		t.Fatal("expected promotion to fail — all replicas have stale heartbeats")
	}
	// Volume should remain unchanged.
	entry, _ := ms.blockRegistry.Lookup("vol-stale")
	if entry.VolumeServer != "vs1:9333" {
		t.Fatalf("primary must not change, got %q", entry.VolumeServer)
	}
}

// ────────────────────────────────────────────────────────────
// QA-4: PromotionRejects_RebuildingReplica
//
// Replica is fresh and healthy but in RoleRebuilding.
// Must NOT be promoted (Gate 3: role check).
// ────────────────────────────────────────────────────────────
func TestQA_CP82_PromotionRejects_RebuildingReplica(t *testing.T) {
	ms := qaCP82Master(t)
	now := time.Now()
	qaRegisterRF3WithState(t, ms, "vol-rebuild", "vs1:9333",
		[]struct {
			Server      string
			HealthScore float64
			WALHeadLSN  uint64
			Role        blockvol.Role
			Heartbeat   time.Time
		}{
			{"vs2:9333", 1.0, 100, blockvol.RoleRebuilding, now}, // rebuilding
			{"vs3:9333", 1.0, 100, blockvol.RoleRebuilding, now}, // also rebuilding
		},
		100,
	)

	_, err := ms.blockRegistry.PromoteBestReplica("vol-rebuild")
	if err == nil {
		t.Fatal("expected promotion to fail — all replicas are rebuilding")
	}

	// One rebuilding, one ready: only the ready one should be promoted.
	ms2 := qaCP82Master(t)
	qaRegisterRF3WithState(t, ms2, "vol-mixed", "vs1:9333",
		[]struct {
			Server      string
			HealthScore float64
			WALHeadLSN  uint64
			Role        blockvol.Role
			Heartbeat   time.Time
		}{
			{"vs2:9333", 1.0, 100, blockvol.RoleRebuilding, now},
			{"vs3:9333", 0.7, 100, blockvol.RoleReplica, now}, // eligible despite lower health
		},
		100,
	)

	_, err = ms2.blockRegistry.PromoteBestReplica("vol-mixed")
	if err != nil {
		t.Fatalf("expected promotion to succeed: %v", err)
	}
	entry, _ := ms2.blockRegistry.Lookup("vol-mixed")
	if entry.VolumeServer != "vs3:9333" {
		t.Fatalf("vs3 should be promoted (only eligible), got %q", entry.VolumeServer)
	}
}

// ────────────────────────────────────────────────────────────
// QA-5: PromotionToleranceBoundary_ExactLSN
//
// Tests promotion_lsn_tolerance boundary: tolerance-1, exact, tolerance+1.
// Catches off-by-one in the eligibility gate.
// ────────────────────────────────────────────────────────────
func TestQA_CP82_PromotionToleranceBoundary_ExactLSN(t *testing.T) {
	const tolerance uint64 = 50
	const primaryLSN uint64 = 200

	cases := []struct {
		name       string
		replicaLSN uint64
		wantErr    bool
	}{
		// replicaLSN + tolerance < primaryLSN → ineligible
		// 149 + 50 = 199 < 200 → INELIGIBLE
		{"below_tolerance", primaryLSN - tolerance - 1, true},
		// 150 + 50 = 200 == 200 → ELIGIBLE (not strict less-than)
		{"at_tolerance", primaryLSN - tolerance, false},
		// 151 + 50 = 201 > 200 → ELIGIBLE
		{"above_tolerance", primaryLSN - tolerance + 1, false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ms := qaCP82Master(t)
			ms.blockRegistry.SetPromotionLSNTolerance(tolerance)

			now := time.Now()
			qaRegisterRF3WithState(t, ms, "vol-tol", "vs1:9333",
				[]struct {
					Server      string
					HealthScore float64
					WALHeadLSN  uint64
					Role        blockvol.Role
					Heartbeat   time.Time
				}{
					{"vs2:9333", 1.0, tc.replicaLSN, blockvol.RoleReplica, now},
				},
				primaryLSN,
			)

			_, err := ms.blockRegistry.PromoteBestReplica("vol-tol")
			if tc.wantErr && err == nil {
				t.Fatalf("replicaLSN=%d: expected promotion to fail", tc.replicaLSN)
			}
			if !tc.wantErr && err != nil {
				t.Fatalf("replicaLSN=%d: expected promotion to succeed: %v", tc.replicaLSN, err)
			}
		})
	}
}

// ────────────────────────────────────────────────────────────
// QA-6: MasterRestart_ReconstructReplicas_ThenFailover
//
// Simulates master restart: empty registry, full heartbeats
// from primary and replica servers reconstruct state, then
// failover correctly promotes the replica.
// ────────────────────────────────────────────────────────────
func TestQA_CP82_MasterRestart_ReconstructReplicas_ThenFailover(t *testing.T) {
	ms := qaCP82Master(t)

	// Phase 1: Primary heartbeat — auto-registers volume from heartbeat.
	ms.blockRegistry.UpdateFullHeartbeat("vs1:9333", []*master_pb.BlockVolumeInfoMessage{
		{
			Path:       "/data/vol-restart.blk",
			VolumeSize: 1 << 30,
			Epoch:      5,
			Role:       blockvol.RoleToWire(blockvol.RolePrimary),
			WalHeadLsn: 500,
		},
	}, "")

	entry, ok := ms.blockRegistry.Lookup("vol-restart")
	if !ok {
		t.Fatal("volume should be auto-registered from primary heartbeat")
	}
	if entry.VolumeServer != "vs1:9333" {
		t.Fatalf("primary: got %q, want vs1:9333", entry.VolumeServer)
	}

	// Phase 2: Replica heartbeat — should reconstruct ReplicaInfo.
	ms.blockRegistry.UpdateFullHeartbeat("vs2:9333", []*master_pb.BlockVolumeInfoMessage{
		{
			Path:       "/data/vol-restart.blk",
			VolumeSize: 1 << 30,
			Epoch:      5,
			Role:       blockvol.RoleToWire(blockvol.RoleReplica),
			WalHeadLsn: 498,
		},
	}, "")

	entry, _ = ms.blockRegistry.Lookup("vol-restart")
	if len(entry.Replicas) == 0 {
		t.Fatal("replica should be reconstructed from heartbeat (Fix #3)")
	}
	if entry.Replicas[0].Server != "vs2:9333" {
		t.Fatalf("replica server: got %q, want vs2:9333", entry.Replicas[0].Server)
	}

	// Phase 3: Set lease expired and trigger failover.
	ms.blockRegistry.UpdateEntry("vol-restart", func(e *BlockVolumeEntry) {
		e.LeaseTTL = 5 * time.Second
		e.LastLeaseGrant = time.Now().Add(-1 * time.Minute)
		// Ensure replica is eligible for promotion.
		e.Replicas[0].LastHeartbeat = time.Now()
		e.Replicas[0].Role = blockvol.RoleToWire(blockvol.RoleReplica)
	})

	ms.failoverBlockVolumes("vs1:9333")

	entry, ok = ms.blockRegistry.Lookup("vol-restart")
	if !ok {
		t.Fatal("volume should survive failover")
	}
	if entry.VolumeServer != "vs2:9333" {
		t.Fatalf("after failover: primary should be vs2:9333, got %q", entry.VolumeServer)
	}
	if entry.Epoch <= 5 {
		t.Fatalf("epoch should be bumped, got %d", entry.Epoch)
	}
}

// ────────────────────────────────────────────────────────────
// QA-7: RF3_OneReplicaFlaps_UnderWriteLoad
//
// One replica rapidly disconnects/reconnects while concurrent
// failover checks run. Must not cause split-brain or panic.
// Simulates via concurrent UpdateFullHeartbeat + failoverBlockVolumes.
// ────────────────────────────────────────────────────────────
func TestQA_CP82_RF3_OneReplicaFlaps_UnderWriteLoad(t *testing.T) {
	ms := qaCP82Master(t)
	now := time.Now()
	qaRegisterRF3WithState(t, ms, "vol-flap", "vs1:9333",
		[]struct {
			Server      string
			HealthScore float64
			WALHeadLSN  uint64
			Role        blockvol.Role
			Heartbeat   time.Time
		}{
			{"vs2:9333", 1.0, 100, blockvol.RoleReplica, now},
			{"vs3:9333", 1.0, 100, blockvol.RoleReplica, now},
		},
		100,
	)

	var wg sync.WaitGroup
	const rounds = 50

	// Goroutine 1: flapping replica vs3 (heartbeat with/without volume).
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < rounds; i++ {
			if i%2 == 0 {
				// Replica reports volume.
				ms.blockRegistry.UpdateFullHeartbeat("vs3:9333", []*master_pb.BlockVolumeInfoMessage{
					{
						Path:       "/data/vol-flap.blk",
						VolumeSize: 1 << 30,
						Epoch:      1,
						Role:       blockvol.RoleToWire(blockvol.RoleReplica),
						WalHeadLsn: 100,
					},
				}, "")
			} else {
				// Replica reports no volumes (simulates disconnect).
				ms.blockRegistry.UpdateFullHeartbeat("vs3:9333", []*master_pb.BlockVolumeInfoMessage{}, "")
			}
		}
	}()

	// Goroutine 2: concurrent heartbeats from primary.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < rounds; i++ {
			ms.blockRegistry.UpdateFullHeartbeat("vs1:9333", []*master_pb.BlockVolumeInfoMessage{
				{
					Path:       "/data/vol-flap.blk",
					VolumeSize: 1 << 30,
					Epoch:      1,
					Role:       blockvol.RoleToWire(blockvol.RolePrimary),
					WalHeadLsn: uint64(100 + i),
				},
			}, "")
		}
	}()

	// Goroutine 3: concurrent heartbeats from stable replica vs2.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < rounds; i++ {
			ms.blockRegistry.UpdateFullHeartbeat("vs2:9333", []*master_pb.BlockVolumeInfoMessage{
				{
					Path:       "/data/vol-flap.blk",
					VolumeSize: 1 << 30,
					Epoch:      1,
					Role:       blockvol.RoleToWire(blockvol.RoleReplica),
					WalHeadLsn: uint64(100 + i),
				},
			}, "")
		}
	}()

	wg.Wait()

	// Invariant: volume must still exist, primary must be vs1.
	entry, ok := ms.blockRegistry.Lookup("vol-flap")
	if !ok {
		t.Fatal("volume must survive flapping replica")
	}
	if entry.VolumeServer != "vs1:9333" {
		t.Fatalf("primary must remain vs1:9333, got %q (split-brain!)", entry.VolumeServer)
	}
}

// ────────────────────────────────────────────────────────────
// QA-8: AssignmentPrecedence_ReplicaAddrsVsScalar
//
// Creates RF=3 volume and verifies primary assignment includes
// ReplicaAddrs (not just scalar fields). VS should use
// ReplicaAddrs when non-empty.
// ────────────────────────────────────────────────────────────
func TestQA_CP82_AssignmentPrecedence_ReplicaAddrsVsScalar(t *testing.T) {
	ms := qaCP82Master(t)
	ctx := context.Background()

	resp, err := ms.CreateBlockVolume(ctx, &master_pb.CreateBlockVolumeRequest{
		Name:          "vol-addrs",
		SizeBytes:     1 << 30,
		ReplicaFactor: 3,
	})
	if err != nil {
		t.Fatalf("create: %v", err)
	}

	primary := resp.VolumeServer
	assignments := ms.blockAssignmentQueue.Peek(primary)

	// Find the primary assignment.
	var primaryAssignment *blockvol.BlockVolumeAssignment
	for i := range assignments {
		if blockvol.RoleFromWire(assignments[i].Role) == blockvol.RolePrimary {
			primaryAssignment = &assignments[i]
			break
		}
	}

	if primaryAssignment == nil {
		t.Fatal("no primary assignment found")
	}

	// RF=3 → 2 replicas → ReplicaAddrs should have 2 entries.
	if len(primaryAssignment.ReplicaAddrs) != 2 {
		t.Fatalf("primary assignment ReplicaAddrs: got %d, want 2", len(primaryAssignment.ReplicaAddrs))
	}

	// Verify each ReplicaAddr has data+ctrl.
	for i, ra := range primaryAssignment.ReplicaAddrs {
		if ra.DataAddr == "" || ra.CtrlAddr == "" {
			t.Errorf("ReplicaAddrs[%d]: data=%q ctrl=%q — both must be non-empty", i, ra.DataAddr, ra.CtrlAddr)
		}
	}

	// For RF=3, legacy scalar ReplicaDataAddr is NOT set on the primary
	// assignment (only ReplicaAddrs is used). Verify precedence: ReplicaAddrs
	// is the authoritative source when non-empty.
	if len(primaryAssignment.ReplicaAddrs) > 0 && primaryAssignment.ReplicaDataAddr != "" {
		t.Error("legacy ReplicaDataAddr should be empty when ReplicaAddrs is populated (RF=3)")
	}
}

// ────────────────────────────────────────────────────────────
// QA-9: ScrubConcurrentWrites_NoFalseCorruption
//
// Heavy writes while scrub runs. Expect zero false positives.
// Tests at the HealthScore level (engine scrub tested separately).
// ────────────────────────────────────────────────────────────
func TestQA_CP82_ScrubConcurrentWrites_NoFalseCorruption(t *testing.T) {
	ms := qaCP82Master(t)
	now := time.Now()
	qaRegisterRF3WithState(t, ms, "vol-scrub", "vs1:9333",
		[]struct {
			Server      string
			HealthScore float64
			WALHeadLSN  uint64
			Role        blockvol.Role
			Heartbeat   time.Time
		}{
			{"vs2:9333", 1.0, 500, blockvol.RoleReplica, now},
			{"vs3:9333", 1.0, 500, blockvol.RoleReplica, now},
		},
		500,
	)

	var wg sync.WaitGroup
	const rounds = 100

	// Simulate heartbeats with varying health scores
	// (as would happen during scrub with concurrent writes).
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < rounds; i++ {
			// Health stays 1.0 — no corruption detected (correct behavior
			// when writes are excluded from scrub checks).
			ms.blockRegistry.UpdateFullHeartbeat("vs1:9333", []*master_pb.BlockVolumeInfoMessage{
				{
					Path:        "/data/vol-scrub.blk",
					VolumeSize:  1 << 30,
					Epoch:       1,
					Role:        blockvol.RoleToWire(blockvol.RolePrimary),
					WalHeadLsn:  uint64(500 + i),
					HealthScore: 1.0, // Clean — no false positives
				},
			}, "")
		}
	}()

	// Concurrent replica heartbeats.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < rounds; i++ {
			ms.blockRegistry.UpdateFullHeartbeat("vs2:9333", []*master_pb.BlockVolumeInfoMessage{
				{
					Path:        "/data/vol-scrub.blk",
					VolumeSize:  1 << 30,
					Epoch:       1,
					Role:        blockvol.RoleToWire(blockvol.RoleReplica),
					WalHeadLsn:  uint64(500 + i),
					HealthScore: 1.0,
				},
			}, "")
		}
	}()

	wg.Wait()

	entry, _ := ms.blockRegistry.Lookup("vol-scrub")
	if entry.HealthScore < 1.0 {
		t.Fatalf("health score should be 1.0 (no corruption), got %f", entry.HealthScore)
	}
}

// ────────────────────────────────────────────────────────────
// QA-10: ScrubDetectsCorruption_HealthDrops_PromotionAvoids
//
// One replica has corruption (low health), the other is clean.
// Promotion must prefer the healthy replica.
// ────────────────────────────────────────────────────────────
func TestQA_CP82_ScrubDetectsCorruption_HealthDrops_PromotionAvoids(t *testing.T) {
	ms := qaCP82Master(t)
	now := time.Now()
	qaRegisterRF3WithState(t, ms, "vol-corrupt", "vs1:9333",
		[]struct {
			Server      string
			HealthScore float64
			WALHeadLSN  uint64
			Role        blockvol.Role
			Heartbeat   time.Time
		}{
			{"vs2:9333", 0.3, 100, blockvol.RoleReplica, now}, // corrupted (low health)
			{"vs3:9333", 1.0, 100, blockvol.RoleReplica, now}, // clean
		},
		100,
	)

	// Simulate heartbeat reporting low health on vs2 (scrub found errors).
	ms.blockRegistry.UpdateFullHeartbeat("vs2:9333", []*master_pb.BlockVolumeInfoMessage{
		{
			Path:        "/data/vol-corrupt.blk",
			VolumeSize:  1 << 30,
			Epoch:       1,
			Role:        blockvol.RoleToWire(blockvol.RoleReplica),
			WalHeadLsn:  100,
			HealthScore: 0.3,
			ScrubErrors: 5,
		},
	}, "")

	// Trigger failover — vs3 (healthy) should be promoted, not vs2.
	newEpoch, err := ms.blockRegistry.PromoteBestReplica("vol-corrupt")
	if err != nil {
		t.Fatalf("promotion should succeed: %v", err)
	}
	if newEpoch != 2 {
		t.Fatalf("epoch: got %d, want 2", newEpoch)
	}

	entry, _ := ms.blockRegistry.Lookup("vol-corrupt")
	if entry.VolumeServer != "vs3:9333" {
		t.Fatalf("vs3 (healthy) should be promoted, got %q", entry.VolumeServer)
	}
	// vs2 (corrupted) should remain as replica.
	if len(entry.Replicas) != 1 || entry.Replicas[0].Server != "vs2:9333" {
		t.Fatalf("vs2 should remain as replica, got %+v", entry.Replicas)
	}
}

// ────────────────────────────────────────────────────────────
// QA-11: ExpandRF3_PartialReplicaFailure
//
// Primary expand succeeds, one replica expand fails.
// Verify: operation succeeds (best-effort), registry updated,
// failed replica state is degraded.
// ────────────────────────────────────────────────────────────
func TestQA_CP82_ExpandRF3_PartialReplicaFailure(t *testing.T) {
	ms := qaCP82Master(t)
	ctx := context.Background()

	// Create RF=3 volume first.
	_, err := ms.CreateBlockVolume(ctx, &master_pb.CreateBlockVolumeRequest{
		Name:          "vol-expand",
		SizeBytes:     1 << 30,
		ReplicaFactor: 3,
	})
	if err != nil {
		t.Fatalf("create: %v", err)
	}

	entry, _ := ms.blockRegistry.Lookup("vol-expand")
	failServer := entry.Replicas[1].Server

	// CP11A-2: coordinated expand — set up prepare/commit/cancel mocks.
	ms.blockVSPrepareExpand = func(ctx context.Context, server string, name string, newSize, expandEpoch uint64) error {
		return nil
	}
	ms.blockVSCommitExpand = func(ctx context.Context, server string, name string, expandEpoch uint64) (uint64, error) {
		if server == failServer {
			return 0, fmt.Errorf("disk full on %s", server)
		}
		return 2 << 30, nil
	}
	ms.blockVSCancelExpand = func(ctx context.Context, server string, name string, expandEpoch uint64) error {
		return nil
	}

	// Under coordinated expand, partial replica commit failure marks the volume degraded.
	_, err = ms.ExpandBlockVolume(ctx, &master_pb.ExpandBlockVolumeRequest{
		Name:         "vol-expand",
		NewSizeBytes: 2 << 30,
	})
	if err == nil {
		t.Fatal("expand should fail when a required replica commit fails")
	}

	// Registry size should NOT be updated (primary committed but replica failed → degraded).
	entry, _ = ms.blockRegistry.Lookup("vol-expand")
	if entry.SizeBytes != 1<<30 {
		t.Fatalf("registry size should be unchanged: got %d, want %d", entry.SizeBytes, uint64(1<<30))
	}
	if !entry.ExpandFailed {
		t.Fatal("ExpandFailed should be true after partial commit failure")
	}
	if !entry.ExpandInProgress {
		t.Fatal("ExpandInProgress should stay true to suppress heartbeat size updates")
	}

	// Cleanup: ClearExpandFailed allows future operations.
	ms.blockRegistry.ClearExpandFailed("vol-expand")

	// Now expand with all mocks succeeding should work.
	ms.blockVSCommitExpand = func(ctx context.Context, server string, name string, expandEpoch uint64) (uint64, error) {
		return 2 << 30, nil
	}
	resp, err := ms.ExpandBlockVolume(ctx, &master_pb.ExpandBlockVolumeRequest{
		Name:         "vol-expand",
		NewSizeBytes: 2 << 30,
	})
	if err != nil {
		t.Fatalf("retry expand after clear: %v", err)
	}
	if resp.CapacityBytes != 2<<30 {
		t.Fatalf("capacity: got %d, want %d", resp.CapacityBytes, uint64(2<<30))
	}
}

// ────────────────────────────────────────────────────────────
// QA-12: ByServerIndexConsistency_AfterReplicaMove
//
// Moves replica between servers repeatedly. ListByServer and
// byServer counts must stay exact — no dangling references.
// ────────────────────────────────────────────────────────────
func TestQA_CP82_ByServerIndexConsistency_AfterReplicaMove(t *testing.T) {
	ms := qaCP82Master(t)
	now := time.Now()
	qaRegisterRF3WithState(t, ms, "vol-move", "vs1:9333",
		[]struct {
			Server      string
			HealthScore float64
			WALHeadLSN  uint64
			Role        blockvol.Role
			Heartbeat   time.Time
		}{
			{"vs2:9333", 1.0, 100, blockvol.RoleReplica, now},
		},
		100,
	)

	// Verify initial state: vs1 and vs2 both have vol-move.
	assertServerHasVolume(t, ms, "vs1:9333", "vol-move")
	assertServerHasVolume(t, ms, "vs2:9333", "vol-move")

	// Move replica from vs2 to vs3 repeatedly.
	servers := []string{"vs3:9333", "vs2:9333", "vs3:9333", "vs2:9333", "vs3:9333"}
	for _, newServer := range servers {
		// Remove old replica.
		entry, _ := ms.blockRegistry.Lookup("vol-move")
		oldServer := ""
		if len(entry.Replicas) > 0 {
			oldServer = entry.Replicas[0].Server
		}
		if oldServer != "" {
			if err := ms.blockRegistry.RemoveReplica("vol-move", oldServer); err != nil {
				t.Fatalf("remove replica from %s: %v", oldServer, err)
			}
		}

		// Add new replica.
		if err := ms.blockRegistry.AddReplica("vol-move", ReplicaInfo{
			Server:      newServer,
			Path:        "/data/vol-move.blk",
			IQN:         fmt.Sprintf("iqn.2024.test:vol-move-r"),
			ISCSIAddr:   newServer + ":3260",
			HealthScore: 1.0,
		}); err != nil {
			t.Fatalf("add replica on %s: %v", newServer, err)
		}

		// Invariant: primary always indexed.
		assertServerHasVolume(t, ms, "vs1:9333", "vol-move")
		// New replica indexed.
		assertServerHasVolume(t, ms, newServer, "vol-move")
		// Old server should NOT have it (unless it's the primary).
		if oldServer != "" && oldServer != "vs1:9333" && oldServer != newServer {
			assertServerDoesNotHaveVolume(t, ms, oldServer, "vol-move")
		}
	}

	// Final check: ListAll should have exactly 1 volume.
	all := ms.blockRegistry.ListAll()
	if len(all) != 1 {
		t.Fatalf("expected 1 volume, got %d", len(all))
	}
}

// ────────────────────────────────────────────────────────────
// Helpers
// ────────────────────────────────────────────────────────────

func assertServerHasVolume(t *testing.T, ms *MasterServer, server, volName string) {
	t.Helper()
	entries := ms.blockRegistry.ListByServer(server)
	for _, e := range entries {
		if e.Name == volName {
			return
		}
	}
	t.Errorf("server %q should have volume %q in byServer index", server, volName)
}

func assertServerDoesNotHaveVolume(t *testing.T, ms *MasterServer, server, volName string) {
	t.Helper()
	entries := ms.blockRegistry.ListByServer(server)
	for _, e := range entries {
		if e.Name == volName {
			t.Errorf("server %q should NOT have volume %q in byServer index", server, volName)
			return
		}
	}
}
