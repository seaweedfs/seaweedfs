//go:build integration

package component

// CP13 Protocol Component Tests
//
// These test the Phase 13 sync replication protocol through the full
// weed master + volume server stack. No SSH, no kernel iSCSI — just
// real processes on localhost exercised through the HTTP/blockapi layer.
//
// Run: go test -tags integration -v -timeout 10m -run TestCP13 \
//        ./weed/storage/blockvol/test/component/
//
// Or with pre-built binary:
//   WEED_BINARY=/path/to/weed go test -tags integration ...

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/storage/blockvol/blockapi"
)

// ---------------------------------------------------------------------------
// Test 1: sync_all RF=2 volume creation and durability mode verification
// ---------------------------------------------------------------------------

func TestCP13_SyncAll_CreateVerifyMode(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	c := newCluster(t, weedBinary, 19510)
	c.addVolume(19511, 19513)
	c.addVolume(19512, 19514)
	c.start(ctx)
	c.waitBlockServers(ctx, 2, 60*time.Second)

	client := c.client()

	// Create RF=2 sync_all volume.
	info, err := client.CreateVolume(ctx, blockapi.CreateVolumeRequest{
		Name:           "sync-mode-test",
		SizeBytes:      50 << 20,
		ReplicaFactor:  2,
		DurabilityMode: "sync_all",
	})
	if err != nil {
		t.Fatalf("create: %v", err)
	}

	// Verify durability mode is stored and returned.
	if info.DurabilityMode != "sync_all" {
		t.Fatalf("durability_mode: got %q, want sync_all", info.DurabilityMode)
	}
	if info.ReplicaFactor != 2 {
		t.Fatalf("replica_factor: got %d, want 2", info.ReplicaFactor)
	}

	// Verify primary and replica are on different volume servers.
	if info.VolumeServer == "" {
		t.Fatal("volume_server is empty")
	}
	if len(info.Replicas) == 0 {
		t.Fatal("no replicas assigned for RF=2")
	}
	replicaServer := info.Replicas[0].Server
	if info.VolumeServer == replicaServer {
		t.Fatalf("primary and replica on same server: %s", info.VolumeServer)
	}

	t.Logf("PASS: sync_all RF=2 created: primary=%s replica=%s mode=%s",
		info.VolumeServer, replicaServer, info.DurabilityMode)

	// Lookup should return same info.
	looked, err := client.LookupVolume(ctx, "sync-mode-test")
	if err != nil {
		t.Fatalf("lookup: %v", err)
	}
	if looked.DurabilityMode != "sync_all" {
		t.Fatalf("lookup durability_mode: got %q, want sync_all", looked.DurabilityMode)
	}

	// Cleanup.
	client.DeleteVolume(ctx, "sync-mode-test")
}

// ---------------------------------------------------------------------------
// Test 2: best_effort volume survives replica death
// ---------------------------------------------------------------------------

func TestCP13_BestEffort_SurvivesReplicaDeath(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	c := newCluster(t, weedBinary, 19520)
	c.addVolume(19521, 19523)
	c.addVolume(19522, 19524)
	c.start(ctx)
	c.waitBlockServers(ctx, 2, 60*time.Second)

	client := c.client()

	// Create RF=2 best_effort volume.
	info, err := client.CreateVolume(ctx, blockapi.CreateVolumeRequest{
		Name:           "best-effort-test",
		SizeBytes:      50 << 20,
		ReplicaFactor:  2,
		DurabilityMode: "best_effort",
	})
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if info.DurabilityMode != "best_effort" {
		t.Fatalf("durability_mode: got %q, want best_effort", info.DurabilityMode)
	}

	// Identify which VS is the replica and kill it.
	primaryServer := info.VolumeServer
	replicaIdx := -1
	for i, vs := range c.volumes {
		addr := strings.TrimSpace(vs.addr(c))
		if addr != primaryServer {
			replicaIdx = i
			break
		}
	}
	if replicaIdx < 0 {
		t.Fatal("could not identify replica VS")
	}

	t.Logf("killing replica VS%d", replicaIdx)
	c.stopVolume(replicaIdx)

	// Wait for degradation to propagate through heartbeat.
	time.Sleep(10 * time.Second)

	// Lookup should still succeed — best_effort doesn't require replica.
	looked, err := client.LookupVolume(ctx, "best-effort-test")
	if err != nil {
		t.Fatalf("lookup after replica death: %v", err)
	}
	if looked.VolumeServer == "" {
		t.Fatal("volume has no primary after replica death")
	}

	t.Logf("PASS: best_effort volume still accessible after replica death: primary=%s degraded=%v",
		looked.VolumeServer, looked.ReplicaDegraded)

	client.DeleteVolume(ctx, "best-effort-test")
}

// ---------------------------------------------------------------------------
// Test 3: sync_all — kill primary → auto-failover → new primary at higher epoch
// ---------------------------------------------------------------------------

func TestCP13_SyncAll_FailoverPromotesReplica(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	c := newCluster(t, weedBinary, 19530)
	c.addVolume(19531, 19533)
	c.addVolume(19532, 19534)
	c.start(ctx)
	c.waitBlockServers(ctx, 2, 60*time.Second)

	client := c.client()

	info, err := client.CreateVolume(ctx, blockapi.CreateVolumeRequest{
		Name:           "failover-sync-test",
		SizeBytes:      50 << 20,
		ReplicaFactor:  2,
		DurabilityMode: "sync_all",
	})
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	initialPrimary := info.VolumeServer
	initialEpoch := info.Epoch
	t.Logf("initial: primary=%s epoch=%d", initialPrimary, initialEpoch)

	// Kill the primary VS.
	primaryIdx := -1
	for i, vs := range c.volumes {
		if vs.addr(c) == initialPrimary {
			primaryIdx = i
			break
		}
	}
	if primaryIdx < 0 {
		// Try matching by port.
		for i, vs := range c.volumes {
			if strings.Contains(initialPrimary, fmt.Sprintf("%d", vs.port)) {
				primaryIdx = i
				break
			}
		}
	}
	if primaryIdx < 0 {
		t.Fatalf("cannot find VS for primary %s", initialPrimary)
	}

	t.Logf("killing primary VS%d (%s)", primaryIdx, initialPrimary)
	c.stopVolume(primaryIdx)

	// Wait for auto-failover.
	promoted := c.waitPrimaryChange(ctx, "failover-sync-test", initialPrimary, 90*time.Second)

	if promoted.Epoch <= initialEpoch {
		t.Fatalf("epoch not incremented: got %d, want > %d", promoted.Epoch, initialEpoch)
	}
	if promoted.VolumeServer == initialPrimary {
		t.Fatal("primary didn't change after failover")
	}

	t.Logf("PASS: failover complete: new primary=%s epoch=%d (was %s epoch=%d)",
		promoted.VolumeServer, promoted.Epoch, initialPrimary, initialEpoch)

	client.DeleteVolume(ctx, "failover-sync-test")
}

// ---------------------------------------------------------------------------
// Test 4: sync_all — kill replica → restart → rejoin via catch-up
// ---------------------------------------------------------------------------

func TestCP13_SyncAll_ReplicaRestart_Rejoin(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()

	c := newCluster(t, weedBinary, 19540)
	c.addVolume(19541, 19543)
	c.addVolume(19542, 19544)
	c.start(ctx)
	c.waitBlockServers(ctx, 2, 60*time.Second)

	client := c.client()

	info, err := client.CreateVolume(ctx, blockapi.CreateVolumeRequest{
		Name:           "rejoin-test",
		SizeBytes:      50 << 20,
		ReplicaFactor:  2,
		DurabilityMode: "sync_all",
	})
	if err != nil {
		t.Fatalf("create: %v", err)
	}

	// Identify replica VS.
	primaryServer := info.VolumeServer
	replicaIdx := -1
	for i, vs := range c.volumes {
		if vs.addr(c) != primaryServer {
			replicaIdx = i
			break
		}
	}
	if replicaIdx < 0 {
		t.Fatal("cannot identify replica VS")
	}

	t.Logf("initial: primary=%s, killing replica VS%d", primaryServer, replicaIdx)
	c.stopVolume(replicaIdx)

	// Wait for degradation.
	time.Sleep(10 * time.Second)

	degraded, err := client.LookupVolume(ctx, "rejoin-test")
	if err != nil {
		t.Fatalf("lookup after kill: %v", err)
	}
	t.Logf("after kill: primary=%s degraded=%v", degraded.VolumeServer, degraded.ReplicaDegraded)

	// Restart the replica VS.
	t.Log("restarting replica VS")
	c.restartVolume(ctx, replicaIdx)

	// Wait for the replica to rejoin. Poll until degraded clears.
	deadline := time.After(90 * time.Second)
	ticker := time.NewTicker(3 * time.Second)
	defer ticker.Stop()

	rejoined := false
	for !rejoined {
		select {
		case <-deadline:
			t.Fatal("replica did not rejoin within 90s")
		case <-ctx.Done():
			t.Fatal("context cancelled")
		case <-ticker.C:
			info, err := client.LookupVolume(ctx, "rejoin-test")
			if err != nil {
				continue
			}
			if !info.ReplicaDegraded && len(info.Replicas) > 0 {
				t.Logf("replica rejoined: primary=%s replicas=%d degraded=%v",
					info.VolumeServer, len(info.Replicas), info.ReplicaDegraded)
				rejoined = true
			}
		}
	}

	t.Log("PASS: replica restarted and rejoined cluster")
	client.DeleteVolume(ctx, "rejoin-test")
}

// ---------------------------------------------------------------------------
// Test 5: Durability mode default — no mode specified = best_effort
// ---------------------------------------------------------------------------

func TestCP13_DurabilityModeDefault(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	c := newCluster(t, weedBinary, 19550)
	c.addVolume(19551, 19553)
	c.start(ctx)
	c.waitBlockServers(ctx, 1, 60*time.Second)

	client := c.client()

	info, err := client.CreateVolume(ctx, blockapi.CreateVolumeRequest{
		Name:      "default-mode-test",
		SizeBytes: 50 << 20,
	})
	if err != nil {
		t.Fatalf("create: %v", err)
	}

	if info.DurabilityMode != "best_effort" {
		t.Fatalf("default durability_mode: got %q, want best_effort", info.DurabilityMode)
	}

	t.Logf("PASS: default mode = %s", info.DurabilityMode)
	client.DeleteVolume(ctx, "default-mode-test")
}

// ---------------------------------------------------------------------------
// Test 6: sync_all RF=2 — replica addresses are canonical ip:port
// ---------------------------------------------------------------------------

func TestCP13_ReplicaAddressCanonical(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	c := newCluster(t, weedBinary, 19560)
	c.addVolume(19561, 19563)
	c.addVolume(19562, 19564)
	c.start(ctx)
	c.waitBlockServers(ctx, 2, 60*time.Second)

	client := c.client()

	info, err := client.CreateVolume(ctx, blockapi.CreateVolumeRequest{
		Name:           "addr-test",
		SizeBytes:      50 << 20,
		ReplicaFactor:  2,
		DurabilityMode: "sync_all",
	})
	if err != nil {
		t.Fatalf("create: %v", err)
	}

	// Replica data/ctrl addresses must be canonical ip:port.
	// They must NOT be ":port" or "0.0.0.0:port" or "[::]:port".
	for _, addr := range []struct{ name, val string }{
		{"replica_data_addr", info.ReplicaDataAddr},
		{"replica_ctrl_addr", info.ReplicaCtrlAddr},
	} {
		if addr.val == "" {
			t.Logf("WARNING: %s is empty — may not be populated in API response", addr.name)
			continue
		}
		if strings.HasPrefix(addr.val, ":") {
			t.Fatalf("%s = %q — missing IP, not routable cross-machine", addr.name, addr.val)
		}
		if strings.HasPrefix(addr.val, "0.0.0.0:") || strings.HasPrefix(addr.val, "[::]:") {
			t.Fatalf("%s = %q — wildcard, not routable", addr.name, addr.val)
		}
		t.Logf("%s = %s (canonical)", addr.name, addr.val)
	}

	t.Log("PASS: replica addresses are canonical ip:port")
	client.DeleteVolume(ctx, "addr-test")
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

// addr returns the volume server's address as the master would see it.
func (vs *volumeProc) addr(c *cluster) string {
	return fmt.Sprintf("%s:%d", c.ip, vs.port)
}
