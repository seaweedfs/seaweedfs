package weed_server

import (
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/storage/blockvol"
)

// CP13-9: Mode normalization tests.
// Proves that computeVolumeMode returns the correct normalized mode
// for each lifecycle state of a block volume.

func TestCP13_9_VolumeMode_AllocatedOnly(t *testing.T) {
	e := &BlockVolumeEntry{
		Name:          "vol1",
		VolumeServer:  "vs1",
		ReplicaFactor: 1,
	}
	e.recomputeReplicaState()
	if e.VolumeMode != "allocated_only" {
		t.Fatalf("RF=1 no replicas: expected allocated_only, got %s", e.VolumeMode)
	}
}

func TestCP13_9_VolumeMode_BootstrapPending_NoReplicas(t *testing.T) {
	e := &BlockVolumeEntry{
		Name:          "vol1",
		VolumeServer:  "vs1",
		ReplicaFactor: 2,
		// No replicas registered yet.
	}
	e.recomputeReplicaState()
	if e.VolumeMode != "bootstrap_pending" {
		t.Fatalf("RF=2 no replicas: expected bootstrap_pending, got %s", e.VolumeMode)
	}
}

func TestCP13_9_VolumeMode_BootstrapPending_NotReady(t *testing.T) {
	e := &BlockVolumeEntry{
		Name:          "vol1",
		VolumeServer:  "vs1",
		ReplicaFactor: 2,
		Replicas: []ReplicaInfo{
			{Server: "vs2", Ready: false, LastHeartbeat: time.Now()},
		},
	}
	e.recomputeReplicaState()
	if e.VolumeMode != "bootstrap_pending" {
		t.Fatalf("RF=2 replica not ready: expected bootstrap_pending, got %s", e.VolumeMode)
	}
}

func TestCP13_9_VolumeMode_PublishHealthy(t *testing.T) {
	e := &BlockVolumeEntry{
		Name:          "vol1",
		VolumeServer:  "vs1",
		ReplicaFactor: 2,
		Replicas: []ReplicaInfo{
			{Server: "vs2", Ready: true, DataAddr: "vs2:14260", CtrlAddr: "vs2:14261", LastHeartbeat: time.Now()},
		},
	}
	e.recomputeReplicaState()
	if e.VolumeMode != "publish_healthy" {
		t.Fatalf("RF=2 replica ready, no degradation: expected publish_healthy, got %s", e.VolumeMode)
	}
}

func TestCP13_9_VolumeMode_Degraded(t *testing.T) {
	e := &BlockVolumeEntry{
		Name:              "vol1",
		VolumeServer:      "vs1",
		ReplicaFactor:     2,
		TransportDegraded: true,
		Replicas: []ReplicaInfo{
			{Server: "vs2", Ready: true, DataAddr: "vs2:14260", CtrlAddr: "vs2:14261", LastHeartbeat: time.Now()},
		},
	}
	e.recomputeReplicaState()
	if e.VolumeMode != "degraded" {
		t.Fatalf("RF=2 replica ready but transport degraded: expected degraded, got %s", e.VolumeMode)
	}
}

func TestCP13_9_VolumeMode_NeedsRebuild(t *testing.T) {
	e := &BlockVolumeEntry{
		Name:          "vol1",
		VolumeServer:  "vs1",
		ReplicaFactor: 2,
		Replicas: []ReplicaInfo{
			{Server: "vs2", Role: blockvol.RoleToWire(blockvol.RoleRebuilding), Ready: true, LastHeartbeat: time.Now()},
		},
	}
	e.recomputeReplicaState()
	if e.VolumeMode != "needs_rebuild" {
		t.Fatalf("RF=2 replica in Rebuilding: expected needs_rebuild, got %s", e.VolumeMode)
	}
}

func TestCP13_9_SurfaceConsistency(t *testing.T) {
	// Prove that VolumeMode, ReplicaDegraded, and ReplicaReady are consistent.
	e := &BlockVolumeEntry{
		Name:          "vol1",
		VolumeServer:  "vs1",
		ReplicaFactor: 2,
		Replicas: []ReplicaInfo{
			{Server: "vs2", Ready: true, DataAddr: "vs2:14260", CtrlAddr: "vs2:14261", LastHeartbeat: time.Now()},
		},
	}
	e.recomputeReplicaState()

	// publish_healthy: ReplicaReady=true, ReplicaDegraded=false
	if e.VolumeMode != "publish_healthy" || !e.ReplicaReady || e.ReplicaDegraded {
		t.Fatalf("surface mismatch: mode=%s ready=%v degraded=%v", e.VolumeMode, e.ReplicaReady, e.ReplicaDegraded)
	}

	// Transition to degraded.
	e.TransportDegraded = true
	e.recomputeReplicaState()
	if e.VolumeMode != "degraded" || !e.ReplicaDegraded {
		t.Fatalf("surface mismatch after degradation: mode=%s degraded=%v", e.VolumeMode, e.ReplicaDegraded)
	}

	// Transition to bootstrap_pending (remove ready).
	e.TransportDegraded = false
	e.Replicas[0].Ready = false
	e.recomputeReplicaState()
	if e.VolumeMode != "bootstrap_pending" || e.ReplicaReady {
		t.Fatalf("surface mismatch after unready: mode=%s ready=%v", e.VolumeMode, e.ReplicaReady)
	}

	t.Log("CP13-9: VolumeMode, ReplicaReady, and ReplicaDegraded are surface-consistent across transitions")
}
