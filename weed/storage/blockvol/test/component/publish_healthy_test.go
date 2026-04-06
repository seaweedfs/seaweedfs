package component

// Component tests for the publish_healthy contract.
//
// Product contract (from phase-20-acceptance.md):
//   publish_healthy requires ALL of:
//     1. RoleApplied (assignment processed)
//     2. ShipperConfigured (replica transport wired)
//     3. ShipperConnected (transport contact established)
//     4. DurableLSN > 0 (at least one barrier-confirmed durable entry)
//
// These tests exercise the real blockvol engine through the full sequence
// and verify each gate independently.

import (
	"bytes"
	"path/filepath"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/storage/blockvol"
)

// TestPublishHealthy_WholeChain_FreshRF2 exercises the complete
// bootstrap-to-publish_healthy chain on a fresh RF=2 sync_all volume.
//
// This is the positive whole-chain case from the acceptance checklist:
// create → assign → configure shipper → write → SyncCache → verify
// that all 4 gates are satisfied and the mode would be publish_healthy.
func TestPublishHealthy_WholeChain_FreshRF2(t *testing.T) {
	primary, replica := createPublishPair(t)
	defer primary.Close()
	defer replica.Close()

	// Gate 1: Before assignment, role is not applied.
	status := primary.Status()
	if status.Role != blockvol.RolePrimary {
		t.Fatalf("expected RolePrimary after HandleAssignment, got %v", status.Role)
	}

	// Gate 2: Configure shipper (replica transport).
	if err := replica.StartReplicaReceiver(":0", ":0"); err != nil {
		t.Fatal(err)
	}
	recvAddr := replica.ReplicaReceiverAddr()
	primary.SetReplicaAddr(recvAddr.DataAddr, recvAddr.CtrlAddr)

	states := primary.ReplicaShipperStates()
	if len(states) == 0 {
		t.Fatal("no shipper states after SetReplicaAddr — gate 2 not satisfied")
	}
	t.Logf("gate 2 (ShipperConfigured): shipperStates=%+v", states)

	// Gate 3: Transport contact requires at least one shipped entry.
	if primary.PrimaryShipperConnected() {
		t.Fatal("PrimaryShipperConnected should be false before any write")
	}

	// Write — write-back admission only.
	block := bytes.Repeat([]byte{0xAA}, 4096)
	if err := primary.WriteLBA(0, block); err != nil {
		t.Fatalf("WriteLBA: %v", err)
	}

	// SyncCache — durability fence. For sync_all, triggers BarrierAll.
	syncErr := primary.SyncCache()
	t.Logf("SyncCache err=%v", syncErr)

	if syncErr != nil {
		t.Fatalf("SyncCache failed on fresh bootstrap happy path: %v\n"+
			"This means the durability fence did not confirm. "+
			"publish_healthy cannot be reached without this.", syncErr)
	}

	// Gate 3 check: transport contact after successful write+sync.
	connected := primary.PrimaryShipperConnected()
	t.Logf("gate 3 (ShipperConnected): PrimaryShipperConnected=%v", connected)
	if !connected {
		t.Fatal("gate 3 NOT satisfied: PrimaryShipperConnected=false after successful SyncCache — transport contact must be true after barrier success")
	}

	// Gate 4 check: replica has durable progress.
	replicaHead := replica.Status().WALHeadLSN
	t.Logf("gate 4 (DurableLSN>0): replica WALHeadLSN=%d", replicaHead)

	if replicaHead == 0 {
		t.Fatal("replica WALHeadLSN=0 after SyncCache success — durable boundary not established")
	}

	// Verify shipper reached in_sync (barrier confirmed).
	states = primary.ReplicaShipperStates()
	t.Logf("after SyncCache: shipperStates=%+v", states)

	if len(states) > 0 && states[0].State != "in_sync" {
		t.Logf("NOTE: shipper state=%s (expected in_sync after barrier success)", states[0].State)
	}
	if len(states) > 0 && states[0].FlushedLSN == 0 {
		t.Fatal("FlushedLSN=0 after SyncCache success — barrier did not report durable progress")
	}

	// All 4 gates satisfied:
	// 1. RoleApplied: HandleAssignment(1, RolePrimary, ...) done at create
	// 2. ShipperConfigured: SetReplicaAddr done
	// 3. ShipperConnected: Ship() dialed data channel during WriteLBA
	// 4. DurableLSN > 0: SyncCache/BarrierAll confirmed with FlushedLSN > 0
	t.Log("ALL 4 GATES SATISFIED — publish_healthy contract provable")
}

// TestPublishHealthy_Gate4_RequiresDurability proves that transport contact
// alone (gate 3) is not sufficient — DurableLSN > 0 (gate 4) requires
// a successful SyncCache, not just a successful WriteLBA.
func TestPublishHealthy_Gate4_RequiresDurability(t *testing.T) {
	primary, replica := createPublishPair(t)
	defer primary.Close()
	defer replica.Close()

	if err := replica.StartReplicaReceiver(":0", ":0"); err != nil {
		t.Fatal(err)
	}
	recvAddr := replica.ReplicaReceiverAddr()
	primary.SetReplicaAddr(recvAddr.DataAddr, recvAddr.CtrlAddr)

	// WriteLBA only — no SyncCache.
	block := bytes.Repeat([]byte{0xBB}, 4096)
	_ = primary.WriteLBA(0, block)

	// Give shipping time.
	time.Sleep(500 * time.Millisecond)

	// Transport contact may exist (gate 3), but barrier has not confirmed (gate 4).
	states := primary.ReplicaShipperStates()
	t.Logf("after WriteLBA only: states=%+v connected=%v",
		states, primary.PrimaryShipperConnected())

	// FlushedLSN must be 0 — no SyncCache/barrier was called.
	if len(states) > 0 && states[0].FlushedLSN > 0 {
		t.Fatalf("FlushedLSN=%d without SyncCache — barrier ran outside the declared fence",
			states[0].FlushedLSN)
	}
	t.Log("gate 4 NOT satisfied without SyncCache — correct: WriteLBA is not durability")
}

// TestPublishHealthy_Gate2_RequiresShipperConfig proves that assignment
// alone (gate 1) is not sufficient — ShipperConfigured requires
// SetReplicaAddr/SetReplicaAddrs.
func TestPublishHealthy_Gate2_RequiresShipperConfig(t *testing.T) {
	opts := blockvol.CreateOptions{
		VolumeSize:     4 * 1024 * 1024,
		BlockSize:      4096,
		WALSize:        1 * 1024 * 1024,
		DurabilityMode: blockvol.DurabilitySyncAll,
	}
	primary, err := blockvol.CreateBlockVol(filepath.Join(t.TempDir(), "primary.blk"), opts)
	if err != nil {
		t.Fatal(err)
	}
	defer primary.Close()

	if err := primary.HandleAssignment(1, blockvol.RolePrimary, 30*time.Second); err != nil {
		t.Fatal(err)
	}

	// Gate 1 satisfied (RoleApplied), but no shipper configured.
	states := primary.ReplicaShipperStates()
	if len(states) != 0 {
		t.Fatalf("expected no shipper states without SetReplicaAddr, got %+v", states)
	}

	// PrimaryShipperConnected must be false.
	if primary.PrimaryShipperConnected() {
		t.Fatal("PrimaryShipperConnected should be false without any replica configured")
	}

	t.Log("gate 2 NOT satisfied without SetReplicaAddr — correct")
}

// --- Helpers ---

func createPublishPair(t *testing.T) (primary, replica *blockvol.BlockVol) {
	t.Helper()
	opts := blockvol.CreateOptions{
		VolumeSize:     4 * 1024 * 1024,
		BlockSize:      4096,
		WALSize:        1 * 1024 * 1024,
		DurabilityMode: blockvol.DurabilitySyncAll,
	}
	p, err := blockvol.CreateBlockVol(filepath.Join(t.TempDir(), "primary.blk"), opts)
	if err != nil {
		t.Fatal(err)
	}
	if err := p.HandleAssignment(1, blockvol.RolePrimary, 30*time.Second); err != nil {
		p.Close()
		t.Fatal(err)
	}
	r, err := blockvol.CreateBlockVol(filepath.Join(t.TempDir(), "replica.blk"), opts)
	if err != nil {
		p.Close()
		t.Fatal(err)
	}
	if err := r.HandleAssignment(1, blockvol.RoleReplica, 30*time.Second); err != nil {
		p.Close()
		r.Close()
		t.Fatal(err)
	}
	return p, r
}
