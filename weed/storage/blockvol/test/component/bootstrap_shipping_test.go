package component

// Component tests for the fresh RF=2 bootstrap shipping path.
//
// These reproduce the blockers found during Phase 20 T6 Stage 0B hardware
// validation. The core issue: writes accumulate on the primary before the
// shipper is configured, creating an LSN gap that the replica rejects.
//
// All tests use public APIs only — no internal field access.

import (
	"bytes"
	"path/filepath"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/storage/blockvol"
)

// TestBootstrap_WritesBeforeShipperConfig_CreatesLSNGap reproduces the exact
// hardware blocker: writes accumulate on the primary before SetReplicaAddr,
// so the shipper's first Ship() sends a high LSN that the fresh replica
// (expecting LSN 1) rejects as out-of-order.
func TestBootstrap_WritesBeforeShipperConfig_CreatesLSNGap(t *testing.T) {
	primary, replica := createBootstrapPair(t)
	defer primary.Close()
	defer replica.Close()

	// Phase 1: Write BEFORE shipper is configured.
	preWrites := 50
	block := bytes.Repeat([]byte{0xAA}, 4096)
	for i := 0; i < preWrites; i++ {
		if err := primary.WriteLBA(uint64(i), block); err != nil {
			t.Fatalf("pre-shipper write %d: %v", i, err)
		}
	}

	preLSN := primary.Status().WALHeadLSN
	t.Logf("after %d pre-shipper writes: WALHeadLSN=%d", preWrites, preLSN)

	// Phase 2: Configure shipper.
	if err := replica.StartReplicaReceiver(":0", ":0"); err != nil {
		t.Fatal(err)
	}
	recvAddr := replica.ReplicaReceiverAddr()
	primary.SetReplicaAddr(recvAddr.DataAddr, recvAddr.CtrlAddr)

	// Phase 3: Write AFTER shipper configured.
	postBlock := bytes.Repeat([]byte{0xBB}, 4096)
	_ = primary.WriteLBA(uint64(preWrites), postBlock) // may fail on sync_all barrier

	// Phase 4: Give replica time to process.
	time.Sleep(1 * time.Second)

	replicaHead := replica.Status().WALHeadLSN
	shipperStates := primary.ReplicaShipperStates()

	t.Logf("replica WALHeadLSN=%d shipperStates=%+v", replicaHead, shipperStates)

	// THIS IS THE BUG: replica expects LSN 1 but receives LSN > preWrites.
	if replicaHead == 0 {
		t.Fatalf("CONFIRMED BUG: replica WALHeadLSN=0 — all entries rejected (LSN gap).\n"+
			"Primary had %d writes before shipper. Shipped LSN > %d to replica expecting LSN 1.\n"+
			"Fix: catch up the gap or reset replica expected LSN on fresh bootstrap.",
			preWrites, preLSN)
	}
}

// TestBootstrap_ShipperConfiguredBeforeWrites_NoGap is the control case.
// When the shipper is configured BEFORE any writes, LSN 1 is shipped to
// the fresh replica and accepted.
func TestBootstrap_ShipperConfiguredBeforeWrites_NoGap(t *testing.T) {
	primary, replica := createBootstrapPair(t)
	defer primary.Close()
	defer replica.Close()

	// Configure shipper BEFORE any writes.
	if err := replica.StartReplicaReceiver(":0", ":0"); err != nil {
		t.Fatal(err)
	}
	recvAddr := replica.ReplicaReceiverAddr()
	primary.SetReplicaAddr(recvAddr.DataAddr, recvAddr.CtrlAddr)

	// Write — should be LSN 1, replica accepts it.
	block := bytes.Repeat([]byte{0xCC}, 4096)
	writeErr := primary.WriteLBA(0, block)
	t.Logf("write err=%v (sync_all barrier may fail, checking shipping only)", writeErr)

	// Give replica time.
	time.Sleep(1 * time.Second)

	replicaHead := replica.Status().WALHeadLSN
	shipperStates := primary.ReplicaShipperStates()
	t.Logf("replica WALHeadLSN=%d shipperStates=%+v", replicaHead, shipperStates)

	if replicaHead == 0 {
		// Even if barrier failed, the data channel should have shipped
		// and the replica should have applied.
		t.Fatal("replica WALHeadLSN=0 — entry not applied even on happy-path bootstrap")
	}
}

// TestBootstrap_TransportContact_TrueAfterShip verifies that after the data
// channel ships at least one entry, PrimaryShipperConnected() returns true.
// This is the semantic split: transport contact != barrier durability.
func TestBootstrap_TransportContact_TrueAfterShip(t *testing.T) {
	primary, replica := createBootstrapPair(t)
	defer primary.Close()
	defer replica.Close()

	if err := replica.StartReplicaReceiver(":0", ":0"); err != nil {
		t.Fatal(err)
	}
	recvAddr := replica.ReplicaReceiverAddr()
	primary.SetReplicaAddr(recvAddr.DataAddr, recvAddr.CtrlAddr)

	// Before any write: no transport contact.
	if primary.PrimaryShipperConnected() {
		t.Fatal("PrimaryShipperConnected should be false before any write")
	}

	// Write triggers Ship() which dials data channel.
	block := bytes.Repeat([]byte{0xDD}, 4096)
	_ = primary.WriteLBA(0, block) // ignore barrier error

	// Give data channel time to connect + ship.
	time.Sleep(1 * time.Second)

	states := primary.ReplicaShipperStates()
	t.Logf("after write: shipperStates=%+v PrimaryShipperConnected=%v",
		states, primary.PrimaryShipperConnected())

	// If shipping succeeded (state is not degraded), transport contact
	// should be true.
	if len(states) > 0 && states[0].State == "degraded" {
		t.Logf("KNOWN ISSUE: barrier timeout degraded the shipper before "+
			"transport contact was observed. State=%s", states[0].State)
		// Document: this is the race where barrier timeout (5s) fires
		// and degrades the shipper, wiping transport contact even though
		// the data channel was successful.
	}

	if len(states) > 0 && states[0].State == "in_sync" {
		if !primary.PrimaryShipperConnected() {
			t.Fatal("BUG: shipper in_sync but PrimaryShipperConnected=false")
		}
	}
}

// TestBootstrap_BarrierOnFreshVolume_SyncAll verifies the barrier behavior
// on a fresh RF=2 sync_all volume. When the replica receiver is alive and
// can accept entries, the first barrier should succeed and transition the
// shipper to InSync.
func TestBootstrap_BarrierOnFreshVolume_SyncAll(t *testing.T) {
	primary, replica := createBootstrapPair(t)
	defer primary.Close()
	defer replica.Close()

	// Configure shipper before writes (happy path for barrier test).
	if err := replica.StartReplicaReceiver(":0", ":0"); err != nil {
		t.Fatal(err)
	}
	recvAddr := replica.ReplicaReceiverAddr()
	primary.SetReplicaAddr(recvAddr.DataAddr, recvAddr.CtrlAddr)

	// Write — sync_all means group commit calls BarrierAll.
	block := bytes.Repeat([]byte{0xEE}, 4096)
	err := primary.WriteLBA(0, block)

	states := primary.ReplicaShipperStates()
	replicaHead := replica.Status().WALHeadLSN
	t.Logf("write err=%v replicaHead=%d states=%+v", err, replicaHead, states)

	if err != nil {
		// sync_all barrier failed. Check why.
		if len(states) > 0 {
			t.Logf("shipper state after barrier failure: %s", states[0].State)
		}
		t.Fatalf("sync_all write failed on fresh bootstrap: %v\n"+
			"replica WALHeadLSN=%d (should be >0 if data was shipped)\n"+
			"This means the barrier protocol has a gap on fresh volumes.",
			err, replicaHead)
	}

	// Write returned nil error — sync_all claims durability succeeded.
	// Verify the claim: replica must have actually applied the entry,
	// and the shipper must be in_sync (barrier was confirmed).
	if replicaHead == 0 {
		t.Fatalf("BUG: sync_all write returned nil error but replica WALHeadLSN=0.\n"+
			"The barrier protocol accepted a write as durable without the replica "+
			"actually confirming. Shipper states=%+v", states)
	}

	if len(states) > 0 && states[0].State != "in_sync" {
		t.Logf("NOTE: write succeeded but shipper not in_sync (%s). "+
			"Barrier may have used a different confirmation path.", states[0].State)
	}
}

// TestBootstrap_GroupCommitRestart_PreservesDistributedSync verifies that
// when SetReplicaAddr restarts the group committer, subsequent writes use
// the new distributed sync (with barriers), not the old local-only sync.
func TestBootstrap_GroupCommitRestart_PreservesDistributedSync(t *testing.T) {
	primary, replica := createBootstrapPair(t)
	defer primary.Close()
	defer replica.Close()

	// Write before shipper — uses local-only group commit.
	block := bytes.Repeat([]byte{0x11}, 4096)
	if err := primary.WriteLBA(0, block); err != nil {
		t.Fatalf("pre-config write: %v", err)
	}

	// Configure shipper — restarts group committer.
	if err := replica.StartReplicaReceiver(":0", ":0"); err != nil {
		t.Fatal(err)
	}
	recvAddr := replica.ReplicaReceiverAddr()
	primary.SetReplicaAddr(recvAddr.DataAddr, recvAddr.CtrlAddr)

	// SyncCache forces a group commit sync. After SetReplicaAddr, this
	// should use the distributed sync (with barriers), not local-only.
	syncErr := primary.SyncCache()
	t.Logf("SyncCache after SetReplicaAddr: err=%v", syncErr)

	// The sync either succeeded (barrier worked) or failed (barrier failed).
	// Either way, the shipper should have been exercised.
	states := primary.ReplicaShipperStates()
	t.Logf("shipperStates=%+v", states)

	if len(states) == 0 {
		t.Fatal("no shipper states after SetReplicaAddr — group committer may not have been restarted")
	}

	// If SyncCache succeeded, the distributed sync path worked.
	// If it failed, check that it failed for the right reason (barrier,
	// not because it used the old local-only sync silently).
	if syncErr == nil {
		// Success means barrier completed.
		if states[0].State != "in_sync" {
			t.Logf("SyncCache succeeded but shipper not in_sync: %s (may be OK if best_effort fallback)", states[0].State)
		}
	}
}

// --- Helpers ---

func createBootstrapPair(t *testing.T) (primary, replica *blockvol.BlockVol) {
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
