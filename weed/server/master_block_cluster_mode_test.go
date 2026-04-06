package weed_server

import (
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage/blockvol"
)

func TestT5_AllReplicasHealthy_Keepup(t *testing.T) {
	r := NewBlockVolumeRegistry()
	if err := r.Register(&BlockVolumeEntry{
		Name: "vol-crm-keepup", VolumeServer: "primary:8080",
		Path: "/data/vol-crm-keepup.blk", Status: StatusActive,
		Role: blockvol.RoleToWire(blockvol.RolePrimary), ReplicaFactor: 2,
		WALHeadLSN: 100,
		Replicas: []ReplicaInfo{{
			Server: "replica:8080", Path: "/data/vol-crm-keepup.blk",
			HealthScore: 1.0, WALHeadLSN: 100, Ready: true,
			Role: blockvol.RoleToWire(blockvol.RoleReplica), LastHeartbeat: time.Now(),
		}},
	}); err != nil {
		t.Fatalf("register: %v", err)
	}

	entry, _ := r.Lookup("vol-crm-keepup")
	if entry.ClusterReplicationMode != "keepup" {
		t.Fatalf("ClusterReplicationMode=%q, want %q", entry.ClusterReplicationMode, "keepup")
	}
}

func TestT5_ReplicaBehind_CatchingUp(t *testing.T) {
	r := NewBlockVolumeRegistry()
	if err := r.Register(&BlockVolumeEntry{
		Name: "vol-crm-catch", VolumeServer: "primary:8080",
		Path: "/data/vol-crm-catch.blk", Status: StatusActive,
		Role: blockvol.RoleToWire(blockvol.RolePrimary), ReplicaFactor: 2,
		WALHeadLSN: 100,
		Replicas: []ReplicaInfo{{
			Server: "replica:8080", Path: "/data/vol-crm-catch.blk",
			HealthScore: 1.0, WALHeadLSN: 90, Ready: true,
			Role: blockvol.RoleToWire(blockvol.RoleReplica), LastHeartbeat: time.Now(),
		}},
	}); err != nil {
		t.Fatalf("register: %v", err)
	}

	entry, _ := r.Lookup("vol-crm-catch")
	if entry.ClusterReplicationMode != "catching_up" {
		t.Fatalf("ClusterReplicationMode=%q, want %q", entry.ClusterReplicationMode, "catching_up")
	}
}

func TestT5_StaleHeartbeat_Degraded(t *testing.T) {
	r := NewBlockVolumeRegistry()
	if err := r.Register(&BlockVolumeEntry{
		Name: "vol-crm-degrade", VolumeServer: "primary:8080",
		Path: "/data/vol-crm-degrade.blk", Status: StatusActive,
		Role: blockvol.RoleToWire(blockvol.RolePrimary), ReplicaFactor: 2,
		WALHeadLSN: 100,
		Replicas: []ReplicaInfo{{
			Server: "replica:8080", Path: "/data/vol-crm-degrade.blk",
			HealthScore: 1.0, WALHeadLSN: 100, Ready: true,
			Role:          blockvol.RoleToWire(blockvol.RoleReplica),
			LastHeartbeat: time.Now().Add(-120 * time.Second), // stale
		}},
	}); err != nil {
		t.Fatalf("register: %v", err)
	}

	entry, _ := r.Lookup("vol-crm-degrade")
	if entry.ClusterReplicationMode != "degraded" {
		t.Fatalf("ClusterReplicationMode=%q, want %q", entry.ClusterReplicationMode, "degraded")
	}
}

func TestT5_UnrecoverableGap_NeedsRebuild(t *testing.T) {
	r := NewBlockVolumeRegistry()
	if err := r.Register(&BlockVolumeEntry{
		Name: "vol-crm-rebuild", VolumeServer: "primary:8080",
		Path: "/data/vol-crm-rebuild.blk", Status: StatusActive,
		Role: blockvol.RoleToWire(blockvol.RolePrimary), ReplicaFactor: 2,
		WALHeadLSN: 5000,
		Replicas: []ReplicaInfo{{
			Server: "replica:8080", Path: "/data/vol-crm-rebuild.blk",
			HealthScore: 1.0, WALHeadLSN: 100, Ready: true, // lag > 1000
			Role: blockvol.RoleToWire(blockvol.RoleReplica), LastHeartbeat: time.Now(),
		}},
	}); err != nil {
		t.Fatalf("register: %v", err)
	}

	entry, _ := r.Lookup("vol-crm-rebuild")
	if entry.ClusterReplicationMode != "needs_rebuild" {
		t.Fatalf("ClusterReplicationMode=%q, want %q", entry.ClusterReplicationMode, "needs_rebuild")
	}
}

func TestT5_RebuildingRole_NeedsRebuild(t *testing.T) {
	r := NewBlockVolumeRegistry()
	if err := r.Register(&BlockVolumeEntry{
		Name: "vol-crm-rebuilding", VolumeServer: "primary:8080",
		Path: "/data/vol-crm-rebuilding.blk", Status: StatusActive,
		Role: blockvol.RoleToWire(blockvol.RolePrimary), ReplicaFactor: 2,
		WALHeadLSN: 100,
		Replicas: []ReplicaInfo{{
			Server: "replica:8080", Path: "/data/vol-crm-rebuilding.blk",
			HealthScore: 1.0, WALHeadLSN: 100, Ready: true,
			Role: blockvol.RoleToWire(blockvol.RoleRebuilding), LastHeartbeat: time.Now(),
		}},
	}); err != nil {
		t.Fatalf("register: %v", err)
	}

	entry, _ := r.Lookup("vol-crm-rebuilding")
	if entry.ClusterReplicationMode != "needs_rebuild" {
		t.Fatalf("ClusterReplicationMode=%q, want %q", entry.ClusterReplicationMode, "needs_rebuild")
	}
}

func TestT5_NoReplicas_NoReplicasMode(t *testing.T) {
	r := NewBlockVolumeRegistry()
	if err := r.Register(&BlockVolumeEntry{
		Name: "vol-crm-none", VolumeServer: "primary:8080",
		Path: "/data/vol-crm-none.blk", Status: StatusActive,
		Role: blockvol.RoleToWire(blockvol.RolePrimary), ReplicaFactor: 1,
	}); err != nil {
		t.Fatalf("register: %v", err)
	}

	entry, _ := r.Lookup("vol-crm-none")
	if entry.ClusterReplicationMode != "no_replicas" {
		t.Fatalf("ClusterReplicationMode=%q, want %q", entry.ClusterReplicationMode, "no_replicas")
	}
}

func TestT5_ClusterReplicationModeDistinctFromEngineProjectionMode(t *testing.T) {
	r := NewBlockVolumeRegistry()
	if err := r.Register(&BlockVolumeEntry{
		Name: "vol-crm-distinct", VolumeServer: "primary:8080",
		Path: "/data/vol-crm-distinct.blk", Status: StatusActive,
		Role: blockvol.RoleToWire(blockvol.RolePrimary), ReplicaFactor: 2,
		WALHeadLSN: 100,
		// EngineProjectionMode says "publish_healthy" (VS-local).
		EngineProjectionMode:    "publish_healthy",
		HasEngineProjectionMode: true,
		Replicas: []ReplicaInfo{{
			Server: "replica:8080", Path: "/data/vol-crm-distinct.blk",
			HealthScore: 1.0, WALHeadLSN: 50, Ready: true, // behind → catching_up
			Role: blockvol.RoleToWire(blockvol.RoleReplica), LastHeartbeat: time.Now(),
		}},
	}); err != nil {
		t.Fatalf("register: %v", err)
	}

	entry, _ := r.Lookup("vol-crm-distinct")
	// EngineProjectionMode is VS-local: "publish_healthy"
	if entry.EngineProjectionMode != "publish_healthy" {
		t.Fatalf("EngineProjectionMode=%q, want %q", entry.EngineProjectionMode, "publish_healthy")
	}
	// ClusterReplicationMode is master-computed: "catching_up" because replica is behind.
	if entry.ClusterReplicationMode != "catching_up" {
		t.Fatalf("ClusterReplicationMode=%q, want %q", entry.ClusterReplicationMode, "catching_up")
	}
	// They must be different — proving the two are independent.
	if entry.EngineProjectionMode == entry.ClusterReplicationMode {
		t.Fatal("EngineProjectionMode and ClusterReplicationMode should differ in this scenario")
	}
}

func TestT5_HeartbeatUpdatesClusterReplicationMode(t *testing.T) {
	r := NewBlockVolumeRegistry()
	r.MarkBlockCapable("primary:8080")
	r.MarkBlockCapable("replica:8080")
	if err := r.Register(&BlockVolumeEntry{
		Name: "vol-crm-hb", VolumeServer: "primary:8080",
		Path: "/data/vol-crm-hb.blk", Status: StatusActive,
		Role: blockvol.RoleToWire(blockvol.RolePrimary), ReplicaFactor: 2,
		WALHeadLSN: 100,
		Replicas: []ReplicaInfo{{
			Server: "replica:8080", Path: "/data/vol-crm-hb-replica.blk",
			HealthScore: 1.0, WALHeadLSN: 100, Ready: true,
			Role: blockvol.RoleToWire(blockvol.RoleReplica), LastHeartbeat: time.Now(),
		}},
	}); err != nil {
		t.Fatalf("register: %v", err)
	}

	// Initial state: keepup.
	entry, _ := r.Lookup("vol-crm-hb")
	if entry.ClusterReplicationMode != "keepup" {
		t.Fatalf("initial ClusterReplicationMode=%q, want keepup", entry.ClusterReplicationMode)
	}

	// Primary heartbeat with higher WALHeadLSN → replica now behind.
	r.UpdateFullHeartbeat("primary:8080", []*master_pb.BlockVolumeInfoMessage{{
		Path:       "/data/vol-crm-hb.blk",
		Role:       blockvol.RoleToWire(blockvol.RolePrimary),
		WalHeadLsn: 200,
	}}, "")

	entry, _ = r.Lookup("vol-crm-hb")
	if entry.ClusterReplicationMode != "catching_up" {
		t.Fatalf("after primary advance: ClusterReplicationMode=%q, want catching_up", entry.ClusterReplicationMode)
	}
}

func TestT5_WorstReplicaDominates(t *testing.T) {
	r := NewBlockVolumeRegistry()
	if err := r.Register(&BlockVolumeEntry{
		Name: "vol-crm-worst", VolumeServer: "primary:8080",
		Path: "/data/vol-crm-worst.blk", Status: StatusActive,
		Role: blockvol.RoleToWire(blockvol.RolePrimary), ReplicaFactor: 3,
		WALHeadLSN: 100,
		Replicas: []ReplicaInfo{
			{
				Server: "replica1:8080", Path: "/data/vol-crm-worst.blk",
				HealthScore: 1.0, WALHeadLSN: 100, Ready: true, // keepup
				Role: blockvol.RoleToWire(blockvol.RoleReplica), LastHeartbeat: time.Now(),
			},
			{
				Server: "replica2:8080", Path: "/data/vol-crm-worst.blk",
				HealthScore: 1.0, WALHeadLSN: 100, Ready: true,
				Role: blockvol.RoleToWire(blockvol.RoleRebuilding), LastHeartbeat: time.Now(), // needs_rebuild
			},
		},
	}); err != nil {
		t.Fatalf("register: %v", err)
	}

	entry, _ := r.Lookup("vol-crm-worst")
	// One replica is keepup, one is needs_rebuild → worst dominates.
	if entry.ClusterReplicationMode != "needs_rebuild" {
		t.Fatalf("ClusterReplicationMode=%q, want needs_rebuild (worst dominates)", entry.ClusterReplicationMode)
	}
}
