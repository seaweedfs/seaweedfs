package weed_server

import (
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/storage/blockvol"
)

// ============================================================
// EC-6 Fix Verification
// ============================================================

// Gate still applies when primary is alive (prevents split-brain).
func TestEC6Fix_GateStillAppliesWhenPrimaryAlive(t *testing.T) {
	r := NewBlockVolumeRegistry()
	r.MarkBlockCapable("primary:9333")
	r.MarkBlockCapable("replica:9333")

	r.Register(&BlockVolumeEntry{
		Name:           "ec6-alive",
		VolumeServer:   "primary:9333",
		Path:           "/data/ec6-alive.blk",
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
				Path:          "/data/ec6-alive.blk",
				Role:          blockvol.RoleToWire(blockvol.RoleReplica),
				WALHeadLSN:    350, // 150 behind
				HealthScore:   1.0,
				LastHeartbeat: time.Now(),
			},
		},
	})

	// Primary still alive — DON'T unmark.
	// WAL lag gate should still apply → reject.
	pf, _ := r.EvaluatePromotion("ec6-alive")

	if pf.Promotable {
		t.Fatal("WAL lag gate should still reject when primary is ALIVE (prevents split-brain)")
	}

	hasWALLag := false
	for _, rej := range pf.Rejections {
		if rej.Reason == "wal_lag" {
			hasWALLag = true
		}
	}
	if !hasWALLag {
		t.Fatalf("expected wal_lag rejection when primary alive, got: %s", pf.Reason)
	}

	t.Log("EC-6 fix safe: WAL lag gate still applies when primary is alive")
}

// Gate skipped when primary is dead → promotion succeeds.
func TestEC6Fix_GateSkippedWhenPrimaryDead(t *testing.T) {
	r := NewBlockVolumeRegistry()
	r.MarkBlockCapable("primary:9333")
	r.MarkBlockCapable("replica:9333")

	r.Register(&BlockVolumeEntry{
		Name:           "ec6-dead",
		VolumeServer:   "primary:9333",
		Path:           "/data/ec6-dead.blk",
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
				Path:          "/data/ec6-dead.blk",
				Role:          blockvol.RoleToWire(blockvol.RoleReplica),
				WALHeadLSN:    350, // 150 behind — would fail old gate
				HealthScore:   1.0,
				LastHeartbeat: time.Now(),
			},
		},
	})

	// Primary dies.
	r.UnmarkBlockCapable("primary:9333")

	// Should succeed now (EC-6 fix: gate skipped for dead primary).
	newEpoch, err := r.PromoteBestReplica("ec6-dead")
	if err != nil {
		t.Fatalf("promotion should succeed when primary is dead: %v", err)
	}

	entry, _ := r.Lookup("ec6-dead")
	if entry.VolumeServer != "replica:9333" {
		t.Fatalf("primary=%s, want replica:9333", entry.VolumeServer)
	}

	t.Logf("EC-6 fix works: dead primary → WAL lag gate skipped → promoted to epoch %d", newEpoch)
}

// Large lag (1000+ entries behind) still promotes when primary dead.
func TestEC6Fix_LargeLag_StillPromotes(t *testing.T) {
	r := NewBlockVolumeRegistry()
	r.MarkBlockCapable("primary:9333")
	r.MarkBlockCapable("replica:9333")

	r.Register(&BlockVolumeEntry{
		Name:           "ec6-large",
		VolumeServer:   "primary:9333",
		Path:           "/data/ec6-large.blk",
		SizeBytes:      1 << 30,
		Epoch:          1,
		Role:           blockvol.RoleToWire(blockvol.RolePrimary),
		Status:         StatusActive,
		LeaseTTL:       30 * time.Second,
		LastLeaseGrant: time.Now(),
		WALHeadLSN:     10000, // very active primary
		Replicas: []ReplicaInfo{
			{
				Server:        "replica:9333",
				Path:          "/data/ec6-large.blk",
				Role:          blockvol.RoleToWire(blockvol.RoleReplica),
				WALHeadLSN:    5000, // 5000 entries behind
				HealthScore:   1.0,
				LastHeartbeat: time.Now(),
			},
		},
	})

	r.UnmarkBlockCapable("primary:9333")

	_, err := r.PromoteBestReplica("ec6-large")
	if err != nil {
		t.Fatalf("even 5000-entry lag should promote when primary dead: %v", err)
	}

	t.Log("EC-6 fix: 5000-entry lag + dead primary → still promoted (best available)")
}
