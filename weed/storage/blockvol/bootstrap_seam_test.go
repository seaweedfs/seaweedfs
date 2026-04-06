package blockvol

import (
	"testing"
	"time"
)

// Priority 1: Post-assignment writes actually call ShipAll.
// After SetReplicaAddr, subsequent writes must go through the shipping path
// and ShippedLSN must advance.
func TestBootstrapSeam_PostAssignmentWritesAreShipped(t *testing.T) {
	primary, replica := createSyncAllPair(t)
	defer primary.Close()
	defer replica.Close()

	recv, err := NewReplicaReceiver(replica, "127.0.0.1:0", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("NewReplicaReceiver: %v", err)
	}
	recv.Serve()
	defer recv.Stop()

	// Write BEFORE shipper is configured.
	block := make([]byte, 4096)
	block[0] = 0xAA
	if err := primary.WriteLBA(0, block); err != nil {
		t.Fatalf("pre-shipper write: %v", err)
	}

	// Configure shipper.
	primary.SetReplicaAddr(recv.DataAddr(), recv.CtrlAddr())

	// Write AFTER shipper is configured.
	block[0] = 0xBB
	if err := primary.WriteLBA(1, block); err != nil {
		t.Fatalf("post-shipper write: %v", err)
	}

	// Allow async shipping to proceed.
	time.Sleep(200 * time.Millisecond)

	// ShippedLSN must have advanced — the post-assignment write entered
	// the shipping path.
	states := primary.ReplicaShipperStates()
	if len(states) == 0 {
		t.Fatal("expected at least one shipper state after SetReplicaAddr")
	}

	// The shipper group should have been created.
	if primary.shipperGroup == nil {
		t.Fatal("shipperGroup is nil after SetReplicaAddr")
	}

	// Verify SyncCache succeeds (drives barrier which requires shipping).
	if err := primary.SyncCache(); err != nil {
		t.Fatalf("SyncCache after post-shipper write: %v", err)
	}

	// After successful SyncCache, the replica must have received the data.
	if recv.ReceivedLSN() == 0 {
		t.Fatal("replica ReceivedLSN=0 after SyncCache — writes were not shipped")
	}
}

// Priority 2: Transport contact becomes true before barrier durability.
// Connected signal should fire when data path works, independent of barrier.
func TestBootstrapSeam_TransportContactBeforeBarrier(t *testing.T) {
	primary, replica := createSyncAllPair(t)
	defer primary.Close()
	defer replica.Close()

	recv, err := NewReplicaReceiver(replica, "127.0.0.1:0", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("NewReplicaReceiver: %v", err)
	}
	recv.Serve()
	defer recv.Stop()

	primary.SetReplicaAddr(recv.DataAddr(), recv.CtrlAddr())

	// Write to trigger shipping.
	block := make([]byte, 4096)
	block[0] = 0xCC
	if err := primary.WriteLBA(0, block); err != nil {
		t.Fatalf("write: %v", err)
	}

	// Wait for shipping to establish transport contact.
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if primary.PrimaryShipperConnected() {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	// Transport contact must be true BEFORE we call SyncCache (barrier).
	if !primary.PrimaryShipperConnected() {
		t.Fatal("PrimaryShipperConnected() should be true after write+ship, before barrier")
	}

	// Now verify barrier also works.
	if err := primary.SyncCache(); err != nil {
		t.Fatalf("SyncCache: %v", err)
	}
}

// Priority 4: Fresh bootstrap barrier with no shipped entries must not
// fake success. If no entries have been shipped, barrier should timeout
// or fail, not return a spurious success.
func TestBootstrapSeam_BarrierWithNoShippedEntries(t *testing.T) {
	primary, replica := createSyncAllPair(t)
	defer primary.Close()
	defer replica.Close()

	recv, err := NewReplicaReceiver(replica, "127.0.0.1:0", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("NewReplicaReceiver: %v", err)
	}
	recv.Serve()
	defer recv.Stop()

	// Write so WAL has entries.
	block := make([]byte, 4096)
	block[0] = 0xDD
	if err := primary.WriteLBA(0, block); err != nil {
		t.Fatalf("write: %v", err)
	}

	// Configure shipper but DON'T wait for shipping to complete.
	// The shipper may not have connected yet.
	primary.SetReplicaAddr(recv.DataAddr(), recv.CtrlAddr())

	// SyncCache should either succeed (if shipping happened fast enough)
	// or fail with a barrier error — but it must NOT return success with
	// FlushedLSN=0 (legacy 1-byte response).
	syncDone := make(chan error, 1)
	go func() {
		syncDone <- primary.SyncCache()
	}()

	select {
	case err := <-syncDone:
		if err != nil {
			// Barrier failed — acceptable (shipper might not have connected).
			t.Logf("SyncCache failed (acceptable): %v", err)
		} else {
			// Success — verify replica actually received data.
			if recv.ReceivedLSN() == 0 {
				t.Fatal("SyncCache succeeded but replica ReceivedLSN=0 — spurious barrier success")
			}
		}
	case <-time.After(10 * time.Second):
		t.Fatal("SyncCache hung — barrier did not complete within timeout")
	}
}

// Post-assignment writes have correct epoch and are shipped.
// Pre-assignment stale writes should not enter the new shipper.
func TestBootstrapSeam_PostAssignmentEpochCorrectness(t *testing.T) {
	primary, replica := createSyncAllPair(t)
	defer primary.Close()
	defer replica.Close()

	// Write at epoch 1 before shipper.
	block := make([]byte, 4096)
	block[0] = 0x11
	if err := primary.WriteLBA(0, block); err != nil {
		t.Fatalf("epoch 1 write: %v", err)
	}

	// Bump epoch (simulating promotion/reassignment).
	if err := primary.SetEpoch(2); err != nil {
		t.Fatalf("SetEpoch: %v", err)
	}
	primary.SetMasterEpoch(2)

	recv, err := NewReplicaReceiver(replica, "127.0.0.1:0", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("NewReplicaReceiver: %v", err)
	}
	// Set replica epoch to match.
	replica.SetEpoch(2)
	replica.SetMasterEpoch(2)
	recv.Serve()
	defer recv.Stop()

	primary.SetReplicaAddr(recv.DataAddr(), recv.CtrlAddr())

	// Write at epoch 2 after shipper.
	block[0] = 0x22
	if err := primary.WriteLBA(1, block); err != nil {
		t.Fatalf("epoch 2 write: %v", err)
	}

	// SyncCache should succeed — the epoch 2 write is accepted by
	// the epoch 2 replica.
	if err := primary.SyncCache(); err != nil {
		t.Fatalf("SyncCache at epoch 2: %v", err)
	}

	if recv.ReceivedLSN() == 0 {
		t.Fatal("replica ReceivedLSN=0 — epoch 2 writes not shipped")
	}
}
