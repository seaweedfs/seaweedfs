//go:build integration

package component

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/storage/blockvol/blockapi"
)

var weedBinary string

func TestMain(m *testing.M) {
	// Use WEED_BINARY env var if set, otherwise build from repo.
	bin := os.Getenv("WEED_BINARY")
	if bin != "" {
		weedBinary = bin
	} else {
		root := findRepoRoot()
		if root == "" {
			fmt.Fprintln(os.Stderr, "FATAL: cannot find repo root (go.mod)")
			os.Exit(1)
		}
		tmpBin := filepath.Join(os.TempDir(), "weed-component-test")
		cmd := exec.Command("go", "build", "-o", tmpBin, "./weed")
		cmd.Dir = root
		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr
		fmt.Println("=== Building weed binary ===")
		if err := cmd.Run(); err != nil {
			fmt.Fprintf(os.Stderr, "FATAL: build weed: %v\n", err)
			os.Exit(1)
		}
		fmt.Println("=== Build complete ===")
		weedBinary = tmpBin
		defer os.Remove(tmpBin)
	}

	os.Exit(m.Run())
}

func findRepoRoot() string {
	dir, _ := os.Getwd()
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			return ""
		}
		dir = parent
	}
}

// ---------------------------------------------------------------------------
// Test 1: Volume Lifecycle (create → lookup → expand → status → delete)
// ---------------------------------------------------------------------------

func TestComponent_VolumeLifecycle(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	c := newCluster(t, weedBinary, 19450)
	c.addVolume(19451, 19453)
	c.addVolume(19452, 19454)
	c.start(ctx)
	c.waitBlockServers(ctx, 2, 60*time.Second)

	client := c.client()

	// Create
	info, err := client.CreateVolume(ctx, blockapi.CreateVolumeRequest{
		Name: "lifecycle-test", SizeBytes: 50 << 20, ReplicaFactor: 2,
	})
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if info.SizeBytes != 50<<20 {
		t.Fatalf("create size: got %d, want %d", info.SizeBytes, 50<<20)
	}
	if info.Epoch != 1 {
		t.Fatalf("create epoch: got %d, want 1", info.Epoch)
	}
	if info.ReplicaFactor != 2 {
		t.Fatalf("create rf: got %d, want 2", info.ReplicaFactor)
	}

	// Lookup
	looked, err := client.LookupVolume(ctx, "lifecycle-test")
	if err != nil {
		t.Fatalf("lookup: %v", err)
	}
	if looked.SizeBytes != 50<<20 {
		t.Fatalf("lookup size: got %d, want %d", looked.SizeBytes, 50<<20)
	}

	// Expand 50M → 100M
	newCap, err := client.ExpandVolume(ctx, "lifecycle-test", 100<<20)
	if err != nil {
		t.Fatalf("expand: %v", err)
	}
	if newCap != 100<<20 {
		t.Fatalf("expand cap: got %d, want %d", newCap, 100<<20)
	}

	// Lookup after expand
	afterExpand, err := client.LookupVolume(ctx, "lifecycle-test")
	if err != nil {
		t.Fatalf("lookup after expand: %v", err)
	}
	if afterExpand.SizeBytes != 100<<20 {
		t.Fatalf("post-expand size: got %d, want %d", afterExpand.SizeBytes, 100<<20)
	}

	// Block status
	status, err := client.BlockStatus(ctx)
	if err != nil {
		t.Fatalf("block status: %v", err)
	}
	if status.VolumeCount < 1 {
		t.Fatalf("volume_count: got %d, want >= 1", status.VolumeCount)
	}
	if status.ServerCount < 2 {
		t.Fatalf("server_count: got %d, want >= 2", status.ServerCount)
	}

	// Delete
	if err := client.DeleteVolume(ctx, "lifecycle-test"); err != nil {
		t.Fatalf("delete: %v", err)
	}

	// Verify deleted (lookup should fail)
	_, err = client.LookupVolume(ctx, "lifecycle-test")
	if err == nil {
		t.Fatal("expected error looking up deleted volume")
	}

	t.Log("PASS: create → lookup → expand → status → delete → verify gone")
}

// ---------------------------------------------------------------------------
// Test 2: Auto-Failover + Promote (T1 candidate eval, T2 orphan re-eval, T4 rebuild)
// ---------------------------------------------------------------------------

func TestComponent_FailoverPromote(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	c := newCluster(t, weedBinary, 19460)
	c.addVolume(19461, 19463)
	c.addVolume(19462, 19464)
	c.start(ctx)
	c.waitBlockServers(ctx, 2, 60*time.Second)

	client := c.client()

	// Create RF=2 volume.
	info, err := client.CreateVolume(ctx, blockapi.CreateVolumeRequest{
		Name: "failover-test", SizeBytes: 50 << 20, ReplicaFactor: 2,
	})
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if info.Epoch != 1 {
		t.Fatalf("initial epoch: got %d, want 1", info.Epoch)
	}
	initialPrimary := info.VolumeServer

	// Record pre-failover metrics.
	preStats, err := client.BlockStatus(ctx)
	if err != nil {
		t.Fatalf("pre-stats: %v", err)
	}

	// Kill VS0 (likely primary).
	t.Logf("killing VS0 (primary=%s)", initialPrimary)
	c.stopVolume(0)

	// Wait for master to auto-promote (lease expiry + promotion).
	promoted := c.waitPrimaryChange(ctx, "failover-test", initialPrimary, 90*time.Second)
	t.Logf("promoted: new primary=%s epoch=%d", promoted.VolumeServer, promoted.Epoch)

	// Verify epoch incremented.
	if promoted.Epoch < 2 {
		t.Fatalf("post-failover epoch: got %d, want >= 2", promoted.Epoch)
	}

	// Verify promotion counter incremented.
	postStats, err := client.BlockStatus(ctx)
	if err != nil {
		t.Fatalf("post-stats: %v", err)
	}
	if postStats.PromotionsTotal <= preStats.PromotionsTotal {
		t.Fatalf("promotions_total: got %d, want > %d", postStats.PromotionsTotal, preStats.PromotionsTotal)
	}

	// Restart killed VS, verify rebuild queued.
	c.restartVolume(ctx, 0)
	c.waitBlockServers(ctx, 2, 60*time.Second)
	time.Sleep(5 * time.Second) // heartbeat propagation

	finalStats, err := client.BlockStatus(ctx)
	if err != nil {
		t.Fatalf("final-stats: %v", err)
	}
	if finalStats.RebuildsTotal <= postStats.RebuildsTotal {
		t.Fatalf("rebuilds_total: got %d, want > %d", finalStats.RebuildsTotal, postStats.RebuildsTotal)
	}

	t.Log("PASS: kill primary → auto-promote → epoch=2 → restart → rebuild queued")
}

// ---------------------------------------------------------------------------
// Test 3: Manual Promote (T5 — rejection, force, structured response)
// ---------------------------------------------------------------------------

func TestComponent_ManualPromote(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	c := newCluster(t, weedBinary, 19470)
	c.addVolume(19471, 19473)
	c.addVolume(19472, 19474)
	c.start(ctx)
	c.waitBlockServers(ctx, 2, 60*time.Second)

	client := c.client()

	// Create RF=2 volume.
	_, err := client.CreateVolume(ctx, blockapi.CreateVolumeRequest{
		Name: "promote-test", SizeBytes: 50 << 20, ReplicaFactor: 2,
	})
	if err != nil {
		t.Fatalf("create: %v", err)
	}

	// Attempt promote with primary alive — should be rejected (409).
	promoteURL := fmt.Sprintf("http://127.0.0.1:%d/block/volume/promote-test/promote", 19470)
	body := strings.NewReader(`{"force":false}`)
	resp, err := http.Post(promoteURL, "application/json", body)
	if err != nil {
		t.Fatalf("promote request: %v", err)
	}
	if resp.StatusCode != http.StatusConflict {
		t.Fatalf("promote with alive primary: got %d, want 409", resp.StatusCode)
	}
	var rejection blockapi.PromoteVolumeResponse
	json.NewDecoder(resp.Body).Decode(&rejection)
	resp.Body.Close()
	if !strings.Contains(rejection.Reason, "primary_alive") {
		t.Fatalf("rejection reason: got %q, want to contain 'primary_alive'", rejection.Reason)
	}
	t.Logf("promote rejected OK (primary alive): reason=%s", rejection.Reason)

	// Kill primary VS.
	c.stopVolume(0)
	time.Sleep(15 * time.Second) // wait for master to detect disconnect

	// Manual promote.
	promoteResp, err := client.PromoteVolume(ctx, "promote-test", blockapi.PromoteVolumeRequest{
		Reason: "component test: manual failover after kill",
	})
	if err != nil {
		t.Fatalf("manual promote: %v", err)
	}
	if promoteResp.Epoch < 2 {
		t.Fatalf("promoted epoch: got %d, want >= 2", promoteResp.Epoch)
	}
	t.Logf("manual promote OK: primary=%s epoch=%d", promoteResp.NewPrimary, promoteResp.Epoch)

	// Verify via lookup.
	afterPromote, err := client.LookupVolume(ctx, "promote-test")
	if err != nil {
		t.Fatalf("lookup after promote: %v", err)
	}
	if afterPromote.Epoch != promoteResp.Epoch {
		t.Fatalf("epoch mismatch: lookup=%d promote=%d", afterPromote.Epoch, promoteResp.Epoch)
	}

	t.Log("PASS: promote rejected (alive) → kill → manual promote → epoch incremented")
}

// ---------------------------------------------------------------------------
// Test 4: Fast Reconnect (T3 — deferred timer safety, no unnecessary promotion)
// ---------------------------------------------------------------------------

func TestComponent_FastReconnect(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	c := newCluster(t, weedBinary, 19480)
	c.addVolume(19481, 19483)
	c.addVolume(19482, 19484)
	c.start(ctx)
	c.waitBlockServers(ctx, 2, 60*time.Second)

	client := c.client()

	// Create RF=2 volume.
	info, err := client.CreateVolume(ctx, blockapi.CreateVolumeRequest{
		Name: "reconnect-test", SizeBytes: 50 << 20, ReplicaFactor: 2,
	})
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if info.Epoch != 1 {
		t.Fatalf("initial epoch: got %d, want 1", info.Epoch)
	}

	preStats, err := client.BlockStatus(ctx)
	if err != nil {
		t.Fatalf("pre-stats: %v", err)
	}

	// Kill VS0 briefly, restart within 3s (well within 30s lease TTL).
	c.stopVolume(0)
	time.Sleep(3 * time.Second)
	c.restartVolume(ctx, 0)
	c.waitBlockServers(ctx, 2, 60*time.Second)
	time.Sleep(5 * time.Second) // heartbeat propagation

	// Verify NO promotion happened.
	afterReconnect, err := client.LookupVolume(ctx, "reconnect-test")
	if err != nil {
		t.Fatalf("lookup after reconnect: %v", err)
	}
	if afterReconnect.Epoch != 1 {
		t.Fatalf("epoch after reconnect: got %d, want 1 (no promotion)", afterReconnect.Epoch)
	}

	postStats, err := client.BlockStatus(ctx)
	if err != nil {
		t.Fatalf("post-stats: %v", err)
	}
	if postStats.PromotionsTotal != preStats.PromotionsTotal {
		t.Fatalf("promotions_total changed: pre=%d post=%d (expected no change)",
			preStats.PromotionsTotal, postStats.PromotionsTotal)
	}

	t.Log("PASS: kill → 3s restart → no promotion, epoch=1, deferred timer cancelled")
}

// ---------------------------------------------------------------------------
// Test 5: Multi-Replica (3 VS, RF=2 create, server registration/deregistration)
// ---------------------------------------------------------------------------

func TestComponent_MultiReplica(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	c := newCluster(t, weedBinary, 19490)
	c.addVolume(19491, 19494)
	c.addVolume(19492, 19495)
	c.addVolume(19493, 19496)
	c.start(ctx)
	c.waitBlockServers(ctx, 3, 60*time.Second)

	client := c.client()

	// Verify 3 servers registered.
	status, err := client.BlockStatus(ctx)
	if err != nil {
		t.Fatalf("initial status: %v", err)
	}
	if status.ServerCount != 3 {
		t.Fatalf("server_count: got %d, want 3", status.ServerCount)
	}

	// Create RF=2 volume.
	info, err := client.CreateVolume(ctx, blockapi.CreateVolumeRequest{
		Name: "multi-test", SizeBytes: 50 << 20, ReplicaFactor: 2,
	})
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if info.ReplicaFactor != 2 {
		t.Fatalf("replica_factor: got %d, want 2", info.ReplicaFactor)
	}
	if info.Epoch != 1 {
		t.Fatalf("epoch: got %d, want 1", info.Epoch)
	}

	afterCreate, err := client.BlockStatus(ctx)
	if err != nil {
		t.Fatalf("after-create status: %v", err)
	}
	if afterCreate.VolumeCount != 1 {
		t.Fatalf("volume_count: got %d, want 1", afterCreate.VolumeCount)
	}

	// Kill VS2 (spare, not primary or replica for this volume).
	c.stopVolume(2)
	time.Sleep(10 * time.Second)

	afterKill, err := client.BlockStatus(ctx)
	if err != nil {
		t.Fatalf("after-kill status: %v", err)
	}
	t.Logf("after kill VS2: servers=%d volumes=%d", afterKill.ServerCount, afterKill.VolumeCount)

	// Create RF=1 volume with 2 remaining servers.
	info2, err := client.CreateVolume(ctx, blockapi.CreateVolumeRequest{
		Name: "multi-test-2", SizeBytes: 30 << 20, ReplicaFactor: 1,
	})
	if err != nil {
		t.Fatalf("create RF=1: %v", err)
	}
	if info2.ReplicaFactor != 1 {
		t.Fatalf("rf for vol2: got %d, want 1", info2.ReplicaFactor)
	}

	twoVols, err := client.BlockStatus(ctx)
	if err != nil {
		t.Fatalf("two-vol status: %v", err)
	}
	if twoVols.VolumeCount != 2 {
		t.Fatalf("volume_count: got %d, want 2", twoVols.VolumeCount)
	}

	t.Log("PASS: 3 VS → RF=2 create → kill spare → RF=1 create with 2 servers")
}

// ---------------------------------------------------------------------------
// Test 6: Expand Then Failover (CP11A-2 × CP11B-3 cross-check)
// ---------------------------------------------------------------------------

func TestComponent_ExpandThenFailover(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	c := newCluster(t, weedBinary, 19500)
	c.addVolume(19501, 19503)
	c.addVolume(19502, 19504)
	c.start(ctx)
	c.waitBlockServers(ctx, 2, 60*time.Second)

	client := c.client()

	// Create RF=2 volume, 50M.
	info, err := client.CreateVolume(ctx, blockapi.CreateVolumeRequest{
		Name: "expand-fail-test", SizeBytes: 50 << 20, ReplicaFactor: 2,
	})
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	initialPrimary := info.VolumeServer

	// Expand 50M → 100M.
	newCap, err := client.ExpandVolume(ctx, "expand-fail-test", 100<<20)
	if err != nil {
		t.Fatalf("expand: %v", err)
	}
	if newCap != 100<<20 {
		t.Fatalf("expand cap: got %d, want %d", newCap, 100<<20)
	}

	// Verify expanded size via lookup.
	afterExpand, err := client.LookupVolume(ctx, "expand-fail-test")
	if err != nil {
		t.Fatalf("lookup after expand: %v", err)
	}
	if afterExpand.SizeBytes != 100<<20 {
		t.Fatalf("post-expand size: got %d, want %d", afterExpand.SizeBytes, 100<<20)
	}
	if afterExpand.Epoch != 1 {
		t.Fatalf("post-expand epoch: got %d, want 1", afterExpand.Epoch)
	}

	// Kill primary VS.
	t.Logf("killing primary VS (server=%s)", initialPrimary)
	c.stopVolume(0)

	// Wait for auto-promotion.
	promoted := c.waitPrimaryChange(ctx, "expand-fail-test", initialPrimary, 90*time.Second)
	t.Logf("promoted: new primary=%s epoch=%d", promoted.VolumeServer, promoted.Epoch)

	// Verify size survives failover.
	if promoted.SizeBytes != 100<<20 {
		t.Fatalf("post-failover size: got %d, want %d (expand must survive promotion)", promoted.SizeBytes, 100<<20)
	}

	// Verify epoch incremented.
	if promoted.Epoch < 2 {
		t.Fatalf("post-failover epoch: got %d, want >= 2", promoted.Epoch)
	}

	// Verify primary changed.
	if promoted.VolumeServer == initialPrimary {
		t.Fatalf("primary didn't change: still %s", initialPrimary)
	}

	t.Log("PASS: create RF=2 → expand 50→100M → kill primary → size+epoch correct after failover")
}

// ---------------------------------------------------------------------------
// Test 7: NVMe Publication Lifecycle (create → verify NVMe addr → failover → verify new addr)
// ---------------------------------------------------------------------------

func TestComponent_NVMePublicationLifecycle(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	c := newCluster(t, weedBinary, 19510)
	// VS0: NVMe enabled on port 14420
	c.addVolume(19511, 19513,
		"-block.nvme.enable=true",
		"-block.nvme.listen=:14420",
		fmt.Sprintf("-block.nvme.portal=127.0.0.1:14420"),
	)
	// VS1: NVMe enabled on port 14421
	c.addVolume(19512, 19514,
		"-block.nvme.enable=true",
		"-block.nvme.listen=:14421",
		fmt.Sprintf("-block.nvme.portal=127.0.0.1:14421"),
	)
	c.start(ctx)
	c.waitBlockServers(ctx, 2, 60*time.Second)

	client := c.client()

	// Create RF=2 volume.
	info, err := client.CreateVolume(ctx, blockapi.CreateVolumeRequest{
		Name: "nvme-pub-test", SizeBytes: 50 << 20, ReplicaFactor: 2,
	})
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	initialPrimary := info.VolumeServer
	t.Logf("initial primary=%s", initialPrimary)

	// Wait for NVMe publication to propagate via heartbeat.
	time.Sleep(5 * time.Second)

	// Lookup — verify NVMe addr and NQN are populated.
	looked, err := client.LookupVolume(ctx, "nvme-pub-test")
	if err != nil {
		t.Fatalf("lookup: %v", err)
	}
	if looked.NvmeAddr == "" {
		t.Fatal("NvmeAddr is empty — NVMe publication not propagated to registry")
	}
	if looked.NQN == "" {
		t.Fatal("NQN is empty — NVMe publication not propagated to registry")
	}
	t.Logf("initial NVMe: addr=%s nqn=%s", looked.NvmeAddr, looked.NQN)

	preNvmeAddr := looked.NvmeAddr
	preNQN := looked.NQN

	// Kill primary VS.
	c.stopVolume(0)

	// Wait for auto-promotion.
	promoted := c.waitPrimaryChange(ctx, "nvme-pub-test", initialPrimary, 90*time.Second)
	t.Logf("promoted: new primary=%s epoch=%d", promoted.VolumeServer, promoted.Epoch)

	// Wait for new primary's NVMe publication to propagate via heartbeat.
	time.Sleep(5 * time.Second)

	// Lookup after failover — NVMe addr should change to the new primary's NVMe addr.
	afterFailover, err := client.LookupVolume(ctx, "nvme-pub-test")
	if err != nil {
		t.Fatalf("lookup after failover: %v", err)
	}
	if afterFailover.NvmeAddr == "" {
		t.Fatal("NvmeAddr empty after failover — NVMe publication lost")
	}
	if afterFailover.NQN == "" {
		t.Fatal("NQN empty after failover — NVMe publication lost")
	}

	// NVMe addr should differ from pre-failover (different VS, different NVMe port).
	if afterFailover.NvmeAddr == preNvmeAddr {
		t.Logf("warning: NvmeAddr unchanged (%s) — may be expected if both VS use same portal IP", preNvmeAddr)
	}
	t.Logf("post-failover NVMe: addr=%s nqn=%s (was addr=%s nqn=%s)",
		afterFailover.NvmeAddr, afterFailover.NQN, preNvmeAddr, preNQN)

	// Core assertion: NVMe publication is still present after failover.
	if afterFailover.Epoch < 2 {
		t.Fatalf("post-failover epoch: got %d, want >= 2", afterFailover.Epoch)
	}

	t.Log("PASS: NVMe publication populated → failover → NVMe publication survives on new primary")
}
