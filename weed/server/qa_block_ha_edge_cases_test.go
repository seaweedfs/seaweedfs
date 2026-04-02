package weed_server

import (
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage/blockvol"
)

// ============================================================
// HA Edge Case Tests (from edge-cases-failover.md)
// ============================================================

// --- EC-6: WAL LSN blocks promotion when primary dies under load ---

func TestEC6_WALLagBlocksPromotion(t *testing.T) {
	r := NewBlockVolumeRegistry()
	r.MarkBlockCapable("primary:9333")
	r.MarkBlockCapable("replica:9333")

	r.Register(&BlockVolumeEntry{
		Name:           "ec6-vol",
		VolumeServer:   "primary:9333",
		Path:           "/data/ec6-vol.blk",
		SizeBytes:      1 << 30,
		Epoch:          1,
		Role:           blockvol.RoleToWire(blockvol.RolePrimary),
		Status:         StatusActive,
		LeaseTTL:       30 * time.Second,
		LastLeaseGrant: time.Now(),
		WALHeadLSN:     500,
		Replicas: []ReplicaInfo{
			{
				Server:        "replica:9333",
				Path:          "/data/ec6-vol.blk",
				Role:          blockvol.RoleToWire(blockvol.RoleReplica),
				WALHeadLSN:    350, // 150 behind — exceeds tolerance of 100
				HealthScore:   1.0,
				LastHeartbeat: time.Now(),
			},
		},
	})

	// Primary dies.
	r.UnmarkBlockCapable("primary:9333")

	// Auto-promote should FAIL.
	_, err := r.PromoteBestReplica("ec6-vol")

	if err == nil {
		t.Log("EC-6 NOT reproduced: promotion succeeded despite 150-entry lag")
		return
	}

	t.Logf("EC-6 REPRODUCED: %v", err)

	// Verify rejection is specifically "wal_lag".
	pf, _ := r.EvaluatePromotion("ec6-vol")
	hasWALLag := false
	for _, rej := range pf.Rejections {
		t.Logf("  rejected %s: %s", rej.Server, rej.Reason)
		if rej.Reason == "wal_lag" {
			hasWALLag = true
		}
	}
	if !hasWALLag {
		t.Fatal("expected wal_lag rejection")
	}

	t.Log("EC-6: volume stuck — replica has 70% of data but promotion blocked by stale primary LSN")
}

func TestEC6_ForcePromote_Bypasses(t *testing.T) {
	r := NewBlockVolumeRegistry()
	r.MarkBlockCapable("primary:9333")
	r.MarkBlockCapable("replica:9333")

	r.Register(&BlockVolumeEntry{
		Name:           "ec6-force",
		VolumeServer:   "primary:9333",
		Path:           "/data/ec6-force.blk",
		SizeBytes:      1 << 30,
		Epoch:          1,
		Role:           blockvol.RoleToWire(blockvol.RolePrimary),
		Status:         StatusActive,
		LeaseTTL:       30 * time.Second,
		LastLeaseGrant: time.Now(),
		WALHeadLSN:     500,
		Replicas: []ReplicaInfo{
			{
				Server:        "replica:9333",
				Path:          "/data/ec6-force.blk",
				Role:          blockvol.RoleToWire(blockvol.RoleReplica),
				WALHeadLSN:    350,
				HealthScore:   1.0,
				LastHeartbeat: time.Now(),
			},
		},
	})

	r.UnmarkBlockCapable("primary:9333")

	// Force promote bypasses wal_lag gate.
	newEpoch, _, _, _, err := r.ManualPromote("ec6-force", "", true)
	if err != nil {
		t.Fatalf("force promote should succeed: %v", err)
	}

	entry, _ := r.Lookup("ec6-force")
	if entry.VolumeServer != "replica:9333" {
		t.Fatalf("primary=%s, want replica:9333", entry.VolumeServer)
	}

	t.Logf("EC-6 workaround: force promote succeeded — epoch=%d, new primary=%s", newEpoch, entry.VolumeServer)
}

func TestEC6_WithinTolerance_Succeeds(t *testing.T) {
	r := NewBlockVolumeRegistry()
	r.MarkBlockCapable("primary:9333")
	r.MarkBlockCapable("replica:9333")

	r.Register(&BlockVolumeEntry{
		Name:           "ec6-ok",
		VolumeServer:   "primary:9333",
		Path:           "/data/ec6-ok.blk",
		SizeBytes:      1 << 30,
		Epoch:          1,
		Role:           blockvol.RoleToWire(blockvol.RolePrimary),
		Status:         StatusActive,
		LeaseTTL:       30 * time.Second,
		LastLeaseGrant: time.Now(),
		WALHeadLSN:     500,
		Replicas: []ReplicaInfo{
			{
				Server:        "replica:9333",
				Path:          "/data/ec6-ok.blk",
				Role:          blockvol.RoleToWire(blockvol.RoleReplica),
				WALHeadLSN:    450, // 50 behind — within tolerance
				HealthScore:   1.0,
				LastHeartbeat: time.Now(),
			},
		},
	})

	r.UnmarkBlockCapable("primary:9333")

	_, err := r.PromoteBestReplica("ec6-ok")
	if err != nil {
		t.Fatalf("within tolerance should succeed: %v", err)
	}

	t.Log("EC-6 within tolerance: lag=50 (< 100) → promotion succeeded")
}

// --- EC-5: Wrong primary after master restart ---

func TestEC5_FirstHeartbeatWins(t *testing.T) {
	r := NewBlockVolumeRegistry()
	r.MarkBlockCapable("replica:9333")
	r.MarkBlockCapable("primary:9333")

	// Replica heartbeats FIRST after master restart.
	r.UpdateFullHeartbeat("replica:9333", []*master_pb.BlockVolumeInfoMessage{
		{
			Path:       "/data/ec5-vol.blk",
			VolumeSize: 1 << 30,
			Epoch:      0, // no assignment yet
			Role:       0, // unknown
			WalHeadLsn: 100,
		},
	}, "")

	entry1, ok := r.Lookup("ec5-vol")
	if !ok {
		t.Fatal("volume should be auto-registered from first heartbeat")
	}
	t.Logf("after replica heartbeat: VolumeServer=%s epoch=%d", entry1.VolumeServer, entry1.Epoch)

	firstPrimary := entry1.VolumeServer

	// Real primary heartbeats SECOND with higher epoch and WAL.
	r.UpdateFullHeartbeat("primary:9333", []*master_pb.BlockVolumeInfoMessage{
		{
			Path:       "/data/ec5-vol.blk",
			VolumeSize: 1 << 30,
			Epoch:      1,
			Role:       blockvol.RoleToWire(blockvol.RolePrimary),
			WalHeadLsn: 200,
		},
	}, "")

	entry2, _ := r.Lookup("ec5-vol")
	t.Logf("after primary heartbeat: VolumeServer=%s epoch=%d WALHeadLSN=%d",
		entry2.VolumeServer, entry2.Epoch, entry2.WALHeadLSN)

	if entry2.VolumeServer == "replica:9333" && firstPrimary == "replica:9333" {
		t.Log("EC-5 REPRODUCED: first-heartbeat-wins race — replica is still primary")
		t.Log("Real primary (epoch=1, WAL=200) was ignored or reconciled incorrectly")
	} else if entry2.VolumeServer == "primary:9333" {
		t.Log("EC-5 mitigated: reconcileOnRestart corrected the primary via epoch/WAL comparison")
	} else {
		t.Logf("EC-5 unknown: VolumeServer=%s", entry2.VolumeServer)
	}
}
