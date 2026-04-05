// Reproduces the live cp11b3-manual-promote failure path:
// create RF=2 → kill primary ��� promote replica → restart killed VS → verify
// that the promoted primary eventually gets an assignment with replica addresses.
//
// This test simulates the exact heartbeat/assignment sequence observed in glog
// to find where replicas are dropped.
package weed_server

import (
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	blockvol "github.com/seaweedfs/seaweedfs/weed/storage/blockvol"
)

func TestPromote_LivePath_RestartedVSRejoinsAndPrimaryGetsReplica(t *testing.T) {
	ms := testMasterServerForFailover(t)

	pathVS1 := "/data/vs1/promote-test.blk"
	pathVS2 := "/data/vs2/promote-test.blk"

	ms.blockRegistry.MarkBlockCapable("vs1:18192")
	ms.blockRegistry.MarkBlockCapable("vs2:18193")

	// Step 1: Create RF=2 volume. VS1=primary, VS2=replica.
	ms.blockRegistry.Register(&BlockVolumeEntry{
		Name:           "promote-test",
		VolumeServer:   "vs1:18192",
		Path:           pathVS1,
		ISCSIAddr:      "vs1:3279",
		SizeBytes:      50 << 20,
		Epoch:          1,
		Role:           blockvol.RoleToWire(blockvol.RolePrimary),
		Status:         StatusActive,
		LeaseTTL:       30 * time.Second,
		LastLeaseGrant: time.Now(),
		ReplicaFactor:  2,
		ReplicaServer:  "vs2:18193",
		ReplicaPath:    pathVS2,
		Replicas: []ReplicaInfo{
			{
				Server:        "vs2:18193",
				Path:          pathVS2,
				ISCSIAddr:     "vs2:3280",
				HealthScore:   1.0,
				Role:          blockvol.RoleToWire(blockvol.RoleReplica),
				LastHeartbeat: time.Now(),
				DataAddr:      "vs2:14260",
				CtrlAddr:      "vs2:14261",
			},
		},
	})

	t.Log("Step 1: created RF=2, vs1=primary, vs2=replica")

	// Step 2: Kill VS1 (simulate disconnect).
	ms.blockRegistry.UnmarkBlockCapable("vs1:18192")

	// Step 3: Expire lease and promote VS2.
	ms.blockRegistry.UpdateEntry("promote-test", func(e *BlockVolumeEntry) {
		e.LastLeaseGrant = time.Now().Add(-1 * time.Minute)
	})
	ms.failoverBlockVolumes("vs1:18192")

	entry := lookupEntryT(t, ms.blockRegistry, "promote-test")
	if entry.VolumeServer != "vs2:18193" {
		t.Fatalf("promote failed: primary=%s", entry.VolumeServer)
	}
	t.Logf("Step 3: promoted vs2, epoch=%d, replicas=%d", entry.Epoch, len(entry.Replicas))

	// Drain the promote assignment.
	promoteAssignments := ms.blockAssignmentQueue.Peek("vs2:18193")
	for _, a := range promoteAssignments {
		ms.blockAssignmentQueue.Confirm("vs2:18193", a.Path, a.Epoch)
	}
	t.Logf("Step 3: drained %d promote assignments for vs2", len(promoteAssignments))

	// Step 4: VS1 restarts. First heartbeat: epoch=1, role=primary (stale).
	ms.blockRegistry.MarkBlockCapable("vs1:18192")
	result := ms.blockRegistry.UpdateFullHeartbeat("vs1:18192", []*master_pb.BlockVolumeInfoMessage{
		{
			Path:       pathVS1,
			VolumeSize: 50 << 20,
			Epoch:      1,
			Role:       blockvol.RoleToWire(blockvol.RolePrimary),
			// First heartbeat: no replica addresses yet.
		},
	}, "")

	entry = lookupEntryT(t, ms.blockRegistry, "promote-test")
	t.Logf("Step 4a: after vs1 first heartbeat: primary=%s replicas=%d", entry.VolumeServer, len(entry.Replicas))
	for i, ri := range entry.Replicas {
		t.Logf("  replica[%d]: server=%s data=%s ctrl=%s", i, ri.Server, ri.DataAddr, ri.CtrlAddr)
	}

	// Process any primary refresh from first heartbeat.
	for _, refresh := range result.PrimaryRefreshNeeded {
		ms.enqueuePrimaryRefresh(refresh)
		t.Logf("Step 4a: primary refresh triggered for %s", refresh.Name)
	}

	// Check if vs2 got a refresh assignment with replicas.
	vs2Assignments := ms.blockAssignmentQueue.Peek("vs2:18193")
	t.Logf("Step 4a: vs2 pending assignments: %d", len(vs2Assignments))
	for _, a := range vs2Assignments {
		t.Logf("  assignment: path=%s epoch=%d role=%d replicaDataAddr=%s replicaAddrs=%d",
			a.Path, a.Epoch, a.Role, a.ReplicaDataAddr, len(a.ReplicaAddrs))
	}

	// Step 5: VS1 second heartbeat: now with replica data/ctrl addresses.
	result2 := ms.blockRegistry.UpdateFullHeartbeat("vs1:18192", []*master_pb.BlockVolumeInfoMessage{
		{
			Path:            pathVS1,
			VolumeSize:      50 << 20,
			Epoch:           1,
			Role:            blockvol.RoleToWire(blockvol.RolePrimary),
			ReplicaDataAddr: "vs1:14262",
			ReplicaCtrlAddr: "vs1:14263",
		},
	}, "")

	entry = lookupEntryT(t, ms.blockRegistry, "promote-test")
	t.Logf("Step 5: after vs1 second heartbeat (with addrs): primary=%s replicas=%d", entry.VolumeServer, len(entry.Replicas))
	for i, ri := range entry.Replicas {
		t.Logf("  replica[%d]: server=%s data=%s ctrl=%s", i, ri.Server, ri.DataAddr, ri.CtrlAddr)
	}

	// Process any primary refresh from second heartbeat.
	for _, refresh := range result2.PrimaryRefreshNeeded {
		ms.enqueuePrimaryRefresh(refresh)
		t.Logf("Step 5: primary refresh triggered for %s", refresh.Name)
	}

	// Step 6: Final check — vs2 should now have a pending assignment with replica addresses.
	vs2Final := ms.blockAssignmentQueue.Peek("vs2:18193")
	t.Logf("Step 6: vs2 final pending assignments: %d", len(vs2Final))

	hasReplicaAddrs := false
	for _, a := range vs2Final {
		t.Logf("  assignment: path=%s epoch=%d role=%d replicaDataAddr=%s replicaAddrs=%d",
			a.Path, a.Epoch, a.Role, a.ReplicaDataAddr, len(a.ReplicaAddrs))
		if a.ReplicaDataAddr != "" || len(a.ReplicaAddrs) > 0 {
			hasReplicaAddrs = true
		}
	}

	if !hasReplicaAddrs {
		t.Fatalf("BUG: after promote + vs1 rejoin with addresses, vs2 STILL has no assignment with replica addresses.\n"+
			"This reproduces the live cp11b3-manual-promote failure: promoted primary never gets shipper configuration.")
	}
	t.Log("SUCCESS: promoted primary has pending assignment with replica addresses after rejoin")
}

func TestPromote_LivePath_FirstHeartbeatThenRecoverUsesDeterministicReplicaAddrs(t *testing.T) {
	ms := testMasterServerForFailover(t)

	pathVS1 := "/data/vs1/promote-test.blk"
	pathVS2 := "/data/vs2/promote-test.blk"

	ms.blockRegistry.MarkBlockCapable("vs1:18192")
	ms.blockRegistry.MarkBlockCapable("vs2:18193")

	ms.blockRegistry.Register(&BlockVolumeEntry{
		Name:           "promote-test",
		VolumeServer:   "vs1:18192",
		Path:           pathVS1,
		ISCSIAddr:      "192.168.1.184:3279",
		SizeBytes:      50 << 20,
		Epoch:          1,
		Role:           blockvol.RoleToWire(blockvol.RolePrimary),
		Status:         StatusActive,
		LeaseTTL:       30 * time.Second,
		LastLeaseGrant: time.Now().Add(-1 * time.Minute),
		ReplicaFactor:  2,
		ReplicaServer:  "vs2:18193",
		ReplicaPath:    pathVS2,
		Replicas: []ReplicaInfo{{
			Server:        "vs2:18193",
			Path:          pathVS2,
			ISCSIAddr:     "192.168.1.184:3280",
			HealthScore:   1.0,
			Role:          blockvol.RoleToWire(blockvol.RoleReplica),
			LastHeartbeat: time.Now(),
			DataAddr:      "192.168.1.184:4571",
			CtrlAddr:      "192.168.1.184:4572",
		}},
	})

	ms.blockRegistry.UnmarkBlockCapable("vs1:18192")
	ms.failoverBlockVolumes("vs1:18192")

	for _, a := range ms.blockAssignmentQueue.Peek("vs2:18193") {
		ms.blockAssignmentQueue.Confirm("vs2:18193", a.Path, a.Epoch)
	}

	// The restarted old primary first reports only a stale heartbeat with no
	// receiver addrs, matching the live manual-promote evidence.
	ms.blockRegistry.MarkBlockCapable("vs1:18192")
	ms.blockRegistry.UpdateFullHeartbeat("vs1:18192", []*master_pb.BlockVolumeInfoMessage{{
		Path:       pathVS1,
		VolumeSize: 50 << 20,
		Epoch:      1,
		Role:       blockvol.RoleToWire(blockvol.RolePrimary),
	}}, "")

	// Live path: after processing the heartbeat, the master runs reconnect
	// recovery immediately. The pending rebuild must already carry deterministic
	// replica addrs so recoverBlockVolumes can take the catch-up-first path
	// without waiting for a second heartbeat.
	ms.recoverBlockVolumes("vs1:18192")

	vs1Assignments := ms.blockAssignmentQueue.Peek("vs1:18192")
	foundReplicaAssign := false
	for _, a := range vs1Assignments {
		if a.Path != pathVS1 {
			continue
		}
		if blockvol.RoleFromWire(a.Role) == blockvol.RoleReplica && a.ReplicaDataAddr != "" && a.ReplicaCtrlAddr != "" {
			foundReplicaAssign = true
		}
		if blockvol.RoleFromWire(a.Role) == blockvol.RoleRebuilding {
			t.Fatalf("expected catch-up replica assignment, got rebuild assignment: %+v", a)
		}
	}
	if !foundReplicaAssign {
		t.Fatalf("restarted VS1 did not receive replica assignment with deterministic receiver addrs")
	}

	vs2Assignments := ms.blockAssignmentQueue.Peek("vs2:18193")
	foundPrimaryRefresh := false
	for _, a := range vs2Assignments {
		if a.Path != pathVS2 {
			continue
		}
		if blockvol.RoleFromWire(a.Role) == blockvol.RolePrimary && (a.ReplicaDataAddr != "" || len(a.ReplicaAddrs) > 0) {
			foundPrimaryRefresh = true
		}
	}
	if !foundPrimaryRefresh {
		t.Fatalf("promoted primary did not receive refreshed assignment with replica membership after reconnect recovery")
	}
}
