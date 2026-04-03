// Tests to reproduce the wrong_role auto-failover bug.
package weed_server

import (
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	blockvol "github.com/seaweedfs/seaweedfs/weed/storage/blockvol"
)

func lookupEntryT(t *testing.T, r *BlockVolumeRegistry, name string) *BlockVolumeEntry {
	t.Helper()
	e, ok := r.Lookup(name)
	if !ok {
		t.Fatalf("lookup %q: not found", name)
	}
	return &e
}

// TestAutoFailover_SamePath — simple case, same .blk path for both VS.
func TestAutoFailover_SamePath(t *testing.T) {
	ms := testMasterServerForFailover(t)
	registerVolumeWithReplica(t, ms, "vol1", "vs1", "vs2", 1, 30*time.Second)

	ms.blockRegistry.UpdateEntry("vol1", func(e *BlockVolumeEntry) {
		e.LastLeaseGrant = time.Now().Add(-1 * time.Minute)
	})
	ms.failoverBlockVolumes("vs1")

	entry := lookupEntryT(t, ms.blockRegistry, "vol1")
	if entry.VolumeServer != "vs2" {
		t.Fatalf("primary should be vs2, got %s", entry.VolumeServer)
	}

	ms.blockRegistry.MarkBlockCapable("vs1")
	ms.blockRegistry.UpdateFullHeartbeat("vs1", []*master_pb.BlockVolumeInfoMessage{
		{Path: entry.Path, VolumeSize: entry.SizeBytes, Epoch: 1, Role: blockvol.RoleToWire(blockvol.RolePrimary)},
	}, "")

	ms.blockRegistry.UpdateEntry("vol1", func(e *BlockVolumeEntry) {
		e.LastLeaseGrant = time.Now().Add(-1 * time.Minute)
	})
	ms.blockRegistry.UnmarkBlockCapable("vs2")
	ms.failoverBlockVolumes("vs2")

	entry = lookupEntryT(t, ms.blockRegistry, "vol1")
	if entry.VolumeServer != "vs1" {
		t.Fatalf("auto-failover FAILED (same path): primary=%s, want vs1", entry.VolumeServer)
	}
	t.Logf("same-path: OK, primary=%s epoch=%d", entry.VolumeServer, entry.Epoch)
}

// TestAutoFailover_DifferentPaths — the hardware case.
// VS-A and VS-B have different .blk file paths.
func TestAutoFailover_DifferentPaths(t *testing.T) {
	ms := testMasterServerForFailover(t)

	pathA := "/data/vs1/vol1.blk"
	pathB := "/data/vs2/vol1.blk"

	ms.blockRegistry.MarkBlockCapable("vs1")
	ms.blockRegistry.MarkBlockCapable("vs2")

	ms.blockRegistry.Register(&BlockVolumeEntry{
		Name:           "vol1",
		VolumeServer:   "vs1",
		Path:           pathA,
		IQN:            "iqn.2024.test:vol1",
		ISCSIAddr:      "vs1:3260",
		SizeBytes:      1 << 30,
		Epoch:          1,
		Role:           blockvol.RoleToWire(blockvol.RolePrimary),
		Status:         StatusActive,
		LeaseTTL:       30 * time.Second,
		LastLeaseGrant: time.Now().Add(-1 * time.Minute),
		ReplicaServer:  "vs2",
		ReplicaPath:    pathB,
		Replicas: []ReplicaInfo{
			{
				Server:        "vs2",
				Path:          pathB,
				ISCSIAddr:     "vs2:3260",
				HealthScore:   1.0,
				Role:          blockvol.RoleToWire(blockvol.RoleReplica),
				LastHeartbeat: time.Now(),
			},
		},
	})

	t.Logf("initial: primary=vs1 path=%s, replica=vs2 path=%s", pathA, pathB)

	// Promote VS-B (VS-A "dies").
	ms.failoverBlockVolumes("vs1")

	entry := lookupEntryT(t, ms.blockRegistry, "vol1")
	if entry.VolumeServer != "vs2" {
		t.Fatalf("promote failed: primary=%s", entry.VolumeServer)
	}
	t.Logf("after promote: primary=%s path=%s epoch=%d replicas=%d",
		entry.VolumeServer, entry.Path, entry.Epoch, len(entry.Replicas))

	// VS-A reconnects with its OWN path (different from entry.Path).
	ms.blockRegistry.MarkBlockCapable("vs1")
	ms.blockRegistry.UpdateFullHeartbeat("vs1", []*master_pb.BlockVolumeInfoMessage{
		{Path: pathA, VolumeSize: entry.SizeBytes, Epoch: 1, Role: blockvol.RoleToWire(blockvol.RolePrimary)},
	}, "")

	entry = lookupEntryT(t, ms.blockRegistry, "vol1")
	t.Logf("after VS-A reconnect: primary=%s replicas=%d", entry.VolumeServer, len(entry.Replicas))
	for i, ri := range entry.Replicas {
		t.Logf("  replica[%d]: server=%s path=%s role=%d", i, ri.Server, ri.Path, ri.Role)
	}

	// Check VS-A was added as replica with correct role.
	found := false
	for _, ri := range entry.Replicas {
		if ri.Server == "vs1" {
			found = true
			if blockvol.RoleFromWire(ri.Role) != blockvol.RoleReplica {
				t.Errorf("VS-A role=%d, want RoleReplica(%d)", ri.Role, blockvol.RoleToWire(blockvol.RoleReplica))
			}
		}
	}
	if !found {
		t.Error("VS-A not in replicas — reconcile missed it (different path problem)")
	}

	// Kill VS-B → auto-failover.
	ms.blockRegistry.UpdateEntry("vol1", func(e *BlockVolumeEntry) {
		e.LastLeaseGrant = time.Now().Add(-1 * time.Minute)
	})
	ms.blockRegistry.UnmarkBlockCapable("vs2")
	ms.failoverBlockVolumes("vs2")

	entry = lookupEntryT(t, ms.blockRegistry, "vol1")
	if entry.VolumeServer == "vs1" {
		t.Logf("different-paths: OK, primary=%s epoch=%d", entry.VolumeServer, entry.Epoch)
	} else {
		t.Fatalf("AUTO-FAILOVER FAILED (different paths): primary=%s, want vs1\n"+
			"VS-A path=%s, entry.Path=%s after promote", entry.VolumeServer, pathA, entry.Path)
	}
}
