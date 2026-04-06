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

func TestT5_RF1_NoReplicas(t *testing.T) {
	r := NewBlockVolumeRegistry()
	if err := r.Register(&BlockVolumeEntry{
		Name: "vol-crm-rf1", VolumeServer: "primary:8080",
		Path: "/data/vol-crm-rf1.blk", Status: StatusActive,
		Role: blockvol.RoleToWire(blockvol.RolePrimary), ReplicaFactor: 1,
	}); err != nil {
		t.Fatalf("register: %v", err)
	}

	entry, _ := r.Lookup("vol-crm-rf1")
	if entry.ClusterReplicationMode != "no_replicas" {
		t.Fatalf("ClusterReplicationMode=%q, want %q for RF=1", entry.ClusterReplicationMode, "no_replicas")
	}
}

func TestT5_RF2_MissingReplica_Degraded(t *testing.T) {
	r := NewBlockVolumeRegistry()
	if err := r.Register(&BlockVolumeEntry{
		Name: "vol-crm-missing", VolumeServer: "primary:8080",
		Path: "/data/vol-crm-missing.blk", Status: StatusActive,
		Role: blockvol.RoleToWire(blockvol.RolePrimary), ReplicaFactor: 2,
		// RF=2 but no replicas registered → degraded, not "no_replicas"
	}); err != nil {
		t.Fatalf("register: %v", err)
	}

	entry, _ := r.Lookup("vol-crm-missing")
	if entry.ClusterReplicationMode != "degraded" {
		t.Fatalf("ClusterReplicationMode=%q, want %q for RF=2 with missing replica", entry.ClusterReplicationMode, "degraded")
	}
}

func TestT5_TransportDegraded_Degraded(t *testing.T) {
	r := NewBlockVolumeRegistry()
	if err := r.Register(&BlockVolumeEntry{
		Name: "vol-crm-transport", VolumeServer: "primary:8080",
		Path: "/data/vol-crm-transport.blk", Status: StatusActive,
		Role: blockvol.RoleToWire(blockvol.RolePrimary), ReplicaFactor: 2,
		WALHeadLSN: 100, TransportDegraded: true,
		Replicas: []ReplicaInfo{{
			Server: "replica:8080", Path: "/data/vol-crm-transport.blk",
			HealthScore: 1.0, WALHeadLSN: 100, Ready: true,
			Role: blockvol.RoleToWire(blockvol.RoleReplica), LastHeartbeat: time.Now(),
		}},
	}); err != nil {
		t.Fatalf("register: %v", err)
	}

	entry, _ := r.Lookup("vol-crm-transport")
	if entry.ClusterReplicationMode != "degraded" {
		t.Fatalf("ClusterReplicationMode=%q, want %q for transport-degraded", entry.ClusterReplicationMode, "degraded")
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

func TestT5_APISurface_DistinctNaming(t *testing.T) {
	r := NewBlockVolumeRegistry()
	r.MarkBlockCapable("primary:8080")
	if err := r.Register(&BlockVolumeEntry{
		Name: "vol-crm-api", VolumeServer: "primary:8080",
		Path: "/data/vol-crm-api.blk", Status: StatusActive,
		Role: blockvol.RoleToWire(blockvol.RolePrimary), ReplicaFactor: 2,
		WALHeadLSN:              100,
		EngineProjectionMode:    "publish_healthy",
		HasEngineProjectionMode: true,
		Replicas: []ReplicaInfo{{
			Server: "replica:8080", Path: "/data/vol-crm-api.blk",
			HealthScore: 1.0, WALHeadLSN: 80, Ready: true,
			Role: blockvol.RoleToWire(blockvol.RoleReplica), LastHeartbeat: time.Now(),
		}},
	}); err != nil {
		t.Fatalf("register: %v", err)
	}

	entry, _ := r.Lookup("vol-crm-api")
	info := entryToVolumeInfo(&entry, true)

	// All three mode fields must be present and distinct.
	if info.VolumeMode == "" {
		t.Fatal("VolumeMode missing from API surface")
	}
	if info.EngineProjectionMode == "" {
		t.Fatal("EngineProjectionMode missing from API surface")
	}
	if info.ClusterReplicationMode == "" {
		t.Fatal("ClusterReplicationMode missing from API surface")
	}
	// EngineProjectionMode is VS-local (publish_healthy).
	// ClusterReplicationMode is master-computed (catching_up because replica behind).
	// They must differ in this scenario.
	if info.EngineProjectionMode == info.ClusterReplicationMode {
		t.Fatalf("EngineProjectionMode=%q should differ from ClusterReplicationMode=%q on API surface",
			info.EngineProjectionMode, info.ClusterReplicationMode)
	}
}

// P20-T5-C3: FailoverDiagnosticSnapshot carries both mode fields.
func TestT5_DiagnosticSnapshot_CarriesModes(t *testing.T) {
	ms := testMasterServerForFailover(t)
	registerVolumeWithReplica(t, ms, "vol-diag-modes", "vs1", "vs2", 1, 5*time.Second)

	// Set EngineProjectionMode + ClusterReplicationMode on the entry.
	if err := ms.blockRegistry.UpdateEntry("vol-diag-modes", func(e *BlockVolumeEntry) {
		e.EngineProjectionMode = "publish_healthy"
		e.HasEngineProjectionMode = true
		e.ClusterReplicationMode = "catching_up"
	}); err != nil {
		t.Fatalf("update entry: %v", err)
	}

	// Trigger a pending rebuild so the volume appears in failover diagnostic.
	ms.recordPendingRebuild("vs-dead", pendingRebuild{
		VolumeName: "vol-diag-modes",
		NewPrimary: "vs2",
		Epoch:      2,
	})

	diag := ms.FailoverDiagnosticSnapshot()
	var found *FailoverVolumeState
	for i := range diag.Volumes {
		if diag.Volumes[i].VolumeName == "vol-diag-modes" {
			found = &diag.Volumes[i]
			break
		}
	}
	if found == nil {
		t.Fatal("expected vol-diag-modes in failover diagnostic")
	}
	if found.ClusterReplicationMode != "catching_up" {
		t.Fatalf("diagnostic ClusterReplicationMode=%q, want catching_up", found.ClusterReplicationMode)
	}
	if found.EngineProjectionMode != "publish_healthy" {
		t.Fatalf("diagnostic EngineProjectionMode=%q, want publish_healthy", found.EngineProjectionMode)
	}
}

// P20-T3-C5: V2PromotionMode diagnostic reflects all three states.
func TestT3_V2PromotionMode_DiagnosticTriState(t *testing.T) {
	// State 1: disabled
	ms1 := testMasterServerForFailover(t)
	ms1.blockV2Promotion = false
	diag1 := ms1.FailoverDiagnosticSnapshot()
	if diag1.V2PromotionMode != "disabled" {
		t.Fatalf("state 1: V2PromotionMode=%q, want disabled", diag1.V2PromotionMode)
	}

	// State 2: placeholder_fail_closed
	ms2 := testMasterServerForFailover(t)
	ms2.blockV2Promotion = true
	ms2.blockVSQueryEvidence = ms2.defaultBlockVSQueryEvidence
	ms2.blockV2EvidenceTransport = false
	diag2 := ms2.FailoverDiagnosticSnapshot()
	if diag2.V2PromotionMode != "placeholder_fail_closed" {
		t.Fatalf("state 2: V2PromotionMode=%q, want placeholder_fail_closed", diag2.V2PromotionMode)
	}

	// State 3: transport_ready
	ms3 := testMasterServerForFailover(t)
	ms3.blockV2Promotion = true
	ms3.blockV2EvidenceTransport = true
	diag3 := ms3.FailoverDiagnosticSnapshot()
	if diag3.V2PromotionMode != "transport_ready" {
		t.Fatalf("state 3: V2PromotionMode=%q, want transport_ready", diag3.V2PromotionMode)
	}
}
