package blockvol

// CP13-1: Protocol gap tests for sync_all replication correctness.
// These tests validate invariants from design/sync-all-reconnect-protocol.md
// and design/replication-modes-and-rebuild.md.
//
// Expected baseline (current code):
//   Most tests FAIL — they expose missing protocol features.
//   Tests that pass confirm already-working behavior.
//
// After CP13-2..CP13-7 implementation, all must PASS.

import (
	"bytes"
	"net"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// ---------- Durable progress truth ----------

// TestReplicaProgress_BarrierUsesFlushedLSN verifies that barrier success is
// gated on replicaFlushedLSN (WAL fdatasync on replica), not sender-side
// LastSentLSN or TCP send completion.
//
// Currently EXPECTED TO FAIL: the barrier protocol does not return
// replicaFlushedLSN; it returns a single status byte. The primary has
// no way to verify what the replica durably persisted.
func TestReplicaProgress_BarrierUsesFlushedLSN(t *testing.T) {
	primary, replica := createSyncAllPair(t)
	defer primary.Close()
	defer replica.Close()

	recv, err := NewReplicaReceiver(replica, "127.0.0.1:0", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	recv.Serve()
	defer recv.Stop()

	primary.SetReplicaAddr(recv.DataAddr(), recv.CtrlAddr())

	// Write data.
	for i := uint64(0); i < 5; i++ {
		if err := primary.WriteLBA(i, makeBlock(byte('A'+i))); err != nil {
			t.Fatalf("WriteLBA(%d): %v", i, err)
		}
	}

	// SyncCache triggers barrier.
	if err := primary.SyncCache(); err != nil {
		t.Fatalf("SyncCache: %v", err)
	}

	// After successful SyncCache under sync_all, the primary MUST know
	// the replica's durable progress. Currently there's no API for this.
	// The shipper only tracks ShippedLSN (send-side), not replica-confirmed.
	//
	// When CP13-3 is implemented, the shipper (or ReplicaProgress struct)
	// will expose ReplicaFlushedLSN. For now, check that ShippedLSN >= 5
	// as a weaker proxy — the real test is that barrier response carries
	// the replica's durable LSN.
	sg := primary.shipperGroup
	if sg == nil {
		t.Fatal("shipperGroup is nil")
	}
	s := sg.Shipper(0)
	if s == nil {
		t.Fatal("no shipper at index 0")
	}
	shipped := s.ShippedLSN()
	if shipped < 5 {
		t.Fatalf("ShippedLSN=%d, expected >=5 — shipper didn't track progress", shipped)
	}

	// The REAL invariant (will be testable after CP13-3):
	// shippers[0].ReplicaFlushedLSN() >= 5
	// For now, we can't test this — mark as known gap.
	t.Log("NOTE: ReplicaFlushedLSN not yet available — ShippedLSN used as weak proxy")
}

// TestReplicaProgress_FlushedLSNMonotonicWithinEpoch verifies that
// replicaFlushedLSN never decreases within a single epoch.
//
// Currently EXPECTED TO FAIL: replicaFlushedLSN doesn't exist yet.
func TestReplicaProgress_FlushedLSNMonotonicWithinEpoch(t *testing.T) {
	primary, replica := createSyncAllPair(t)
	defer primary.Close()
	defer replica.Close()

	recv, err := NewReplicaReceiver(replica, "127.0.0.1:0", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	recv.Serve()
	defer recv.Stop()

	primary.SetReplicaAddr(recv.DataAddr(), recv.CtrlAddr())

	var lastFlushed uint64

	for round := 0; round < 5; round++ {
		if err := primary.WriteLBA(uint64(round), makeBlock(byte('A'+round))); err != nil {
			t.Fatalf("write %d: %v", round, err)
		}
		if err := primary.SyncCache(); err != nil {
			t.Fatalf("SyncCache %d: %v", round, err)
		}

		// After CP13-3, this would be:
		//   flushed := shippers[0].ReplicaFlushedLSN()
		// For now use ReceivedLSN as proxy.
		flushed := recv.ReceivedLSN()
		if flushed < lastFlushed {
			t.Fatalf("round %d: flushedLSN went backwards (%d < %d)", round, flushed, lastFlushed)
		}
		lastFlushed = flushed
	}

	if lastFlushed == 0 {
		t.Fatal("flushedLSN never advanced from 0")
	}
}

// ---------- Barrier eligibility ----------

// TestBarrier_RejectsReplicaNotInSync verifies that barrier only counts
// replicas in InSync state. Degraded, CatchingUp, Disconnected, and
// NeedsRebuild replicas must not satisfy sync_all.
//
// Currently EXPECTED TO FAIL: the shipper has only degraded/healthy binary
// state, no full state machine (Disconnected/Connecting/CatchingUp/InSync/
// Degraded/NeedsRebuild).
func TestBarrier_RejectsReplicaNotInSync(t *testing.T) {
	primary, replica := createSyncAllPair(t)
	defer primary.Close()
	defer replica.Close()

	// Create a shipper pointing to a dead address. It will never connect.
	primary.SetReplicaAddr("127.0.0.1:1", "127.0.0.1:2") // dead ports

	// Write something.
	if err := primary.WriteLBA(0, makeBlock('X')); err != nil {
		t.Fatalf("write: %v", err)
	}

	// SyncCache must fail — the replica is not InSync.
	syncDone := make(chan error, 1)
	go func() {
		syncDone <- primary.SyncCache()
	}()

	select {
	case err := <-syncDone:
		if err == nil {
			t.Fatal("SyncCache succeeded with dead replica — barrier should have failed")
		}
		// Good — barrier correctly rejected the non-InSync replica.
	case <-time.After(10 * time.Second):
		t.Fatal("SyncCache hung — barrier timeout not propagated for dead replica")
	}
}

// TestBarrier_EpochMismatchRejected verifies that a barrier response from
// a stale epoch is rejected even if the replica claims durability.
//
// Currently EXPECTED TO FAIL: barrier protocol checks epoch on the replica
// side, but the primary does not verify the response epoch.
func TestBarrier_EpochMismatchRejected(t *testing.T) {
	primary, replica := createSyncAllPair(t)
	defer primary.Close()
	defer replica.Close()

	recv, err := NewReplicaReceiver(replica, "127.0.0.1:0", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	recv.Serve()
	defer recv.Stop()

	primary.SetReplicaAddr(recv.DataAddr(), recv.CtrlAddr())

	// Write and sync at epoch 1 — should succeed.
	if err := primary.WriteLBA(0, makeBlock('A')); err != nil {
		t.Fatal(err)
	}
	if err := primary.SyncCache(); err != nil {
		t.Fatalf("SyncCache epoch 1: %v", err)
	}

	// Advance primary epoch to 2. Replica stays at epoch 1.
	primary.SetEpoch(2)
	primary.SetMasterEpoch(2)

	// Write at epoch 2.
	if err := primary.WriteLBA(1, makeBlock('B')); err != nil {
		t.Fatal(err)
	}

	// SyncCache — barrier request at epoch 2, but replica responds at epoch 1.
	// This should fail: replica epoch doesn't match primary epoch.
	syncDone := make(chan error, 1)
	go func() {
		syncDone <- primary.SyncCache()
	}()

	select {
	case err := <-syncDone:
		if err == nil {
			t.Fatal("SyncCache succeeded with epoch mismatch — should be rejected")
		}
		t.Logf("correctly failed: %v", err)
	case <-time.After(10 * time.Second):
		t.Fatal("SyncCache hung on epoch mismatch")
	}
}

// ---------- Reconnect and catch-up ----------

// TestReconnect_CatchupFromRetainedWal verifies that after a short disconnect,
// the shipper replays retained WAL entries to catch up the replica, then
// transitions to InSync.
//
// Currently EXPECTED TO FAIL: no reconnect handshake or WAL catch-up exists.
func TestReconnect_CatchupFromRetainedWal(t *testing.T) {
	primary, replica := createSyncAllPair(t)
	defer primary.Close()
	defer replica.Close()

	recv, err := NewReplicaReceiver(replica, "127.0.0.1:0", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	recv.Serve()

	savedDataAddr := recv.DataAddr()
	savedCtrlAddr := recv.CtrlAddr()
	primary.SetReplicaAddr(savedDataAddr, savedCtrlAddr)

	// Write 3 entries while healthy.
	for i := uint64(0); i < 3; i++ {
		if err := primary.WriteLBA(i, makeBlock(byte('A'+i))); err != nil {
			t.Fatal(err)
		}
	}
	if err := primary.SyncCache(); err != nil {
		t.Fatalf("SyncCache healthy: %v", err)
	}

	// Disconnect replica — stop receiver but keep shipper (preserves progress).
	recv.Stop()
	time.Sleep(50 * time.Millisecond)

	// Write 2 more entries during disconnect (shipped to nowhere).
	for i := uint64(3); i < 5; i++ {
		if err := primary.WriteLBA(i, makeBlock(byte('A'+i))); err != nil {
			t.Fatal(err)
		}
	}

	// Reconnect replica on same addresses. Same shipper, same flushed progress.
	recv2, err := NewReplicaReceiver(replica, savedDataAddr, savedCtrlAddr)
	if err != nil {
		t.Fatalf("reconnect receiver on same addr: %v", err)
	}
	recv2.Serve()
	defer recv2.Stop()
	// DO NOT call SetReplicaAddr — shipper identity/state must be preserved.

	// SyncCache after reconnect — must succeed after catch-up.
	syncDone := make(chan error, 1)
	go func() {
		syncDone <- primary.SyncCache()
	}()

	select {
	case err := <-syncDone:
		if err != nil {
			t.Fatalf("SyncCache after reconnect: %v — catch-up did not work", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("SyncCache hung — no catch-up protocol")
	}

	// Verify all 5 entries on replica.
	replica.flusher.FlushOnce()
	for i := uint64(0); i < 5; i++ {
		got, err := replica.ReadLBA(i, 4096)
		if err != nil {
			t.Fatalf("replica ReadLBA(%d): %v", i, err)
		}
		if got[0] != byte('A'+i) {
			t.Fatalf("replica LBA %d: expected %c, got %c", i, 'A'+i, got[0])
		}
	}
}

// TestReconnect_GapBeyondRetainedWal_NeedsRebuild verifies that when the
// replica's gap exceeds the retained WAL range, the reconnect handshake
// detects this and transitions to NeedsRebuild.
//
// CP13-7 proof: real reconnect handshake gap detection (R < S path in
// reconnectWithHandshake), not just budget-triggered escalation.
//
// Sequence:
// 1. Establish sync (replica at LSN 1)
// 2. Disconnect replica
// 3. Release retention hold via timeout budget on old shipper
// 4. Write + flush to advance WAL tail past replica's flushedLSN
// 5. Reconnect (new shipper seeded with hasFlushedProgress=true)
// 6. SyncCache → reconnectWithHandshake → detects R < S → NeedsRebuild
func TestReconnect_GapBeyondRetainedWal_NeedsRebuild(t *testing.T) {
	primary, replica := createSyncAllPair(t)
	defer primary.Close()
	defer replica.Close()

	recv, err := NewReplicaReceiver(replica, "127.0.0.1:0", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	recv.Serve()

	primary.SetReplicaAddr(recv.DataAddr(), recv.CtrlAddr())

	// Step 1: Write and sync while healthy.
	if err := primary.WriteLBA(0, makeBlock('A')); err != nil {
		t.Fatal(err)
	}
	if err := primary.SyncCache(); err != nil {
		t.Fatalf("initial SyncCache: %v", err)
	}

	sg := primary.shipperGroup
	s := sg.Shipper(0)
	replicaFlushed := s.ReplicaFlushedLSN()
	t.Logf("replica flushedLSN after sync: %d", replicaFlushed)

	// Step 2: Disconnect replica.
	recv.Stop()
	time.Sleep(50 * time.Millisecond)

	// Step 3: Release retention hold via timeout budget on the old shipper.
	// This transitions the old shipper to NeedsRebuild, so
	// MinRecoverableFlushedLSN no longer pins the WAL.
	sg.EvaluateRetentionBudgets(RetentionBudgetParams{
		Timeout:        1 * time.Nanosecond, // force timeout
		MaxBytes:       0,
		PrimaryHeadLSN: primary.nextLSN.Load() - 1,
		BlockSize:      primary.super.BlockSize,
	})
	if s.State() != ReplicaNeedsRebuild {
		t.Fatalf("old shipper should be NeedsRebuild after timeout, got %s", s.State())
	}

	// Step 4: Write + flush to advance WAL tail past replica's flushedLSN.
	// The retention hold is released, so writes won't block on WAL admission.
	for i := uint64(1); i < 8; i++ {
		if err := primary.WriteLBA(i, makeBlock(byte('0'+i))); err != nil {
			t.Fatalf("write %d: %v", i, err)
		}
	}
	primary.flusher.FlushOnce()
	primary.flusher.FlushOnce()

	// Verify checkpoint advanced past replica's position (WAL reclaimed).
	checkpointAfterFlush := primary.flusher.CheckpointLSN()
	t.Logf("after flush: checkpoint=%d replicaFlushed=%d", checkpointAfterFlush, replicaFlushed)
	if checkpointAfterFlush <= replicaFlushed {
		t.Fatalf("checkpoint should advance past replicaFlushed after hold released: checkpoint=%d replicaFlushed=%d",
			checkpointAfterFlush, replicaFlushed)
	}

	// Step 5: Reconnect with new receiver.
	recv2, err := NewReplicaReceiver(replica, "127.0.0.1:0", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	recv2.Serve()
	defer recv2.Stop()

	// SetReplicaAddrs creates a new shipper seeded with hasFlushedProgress=true (CP13-5).
	primary.SetReplicaAddr(recv2.DataAddr(), recv2.CtrlAddr())

	// Step 6: SyncCache triggers reconnect handshake on the new shipper.
	// The handshake sends ResumeShipReq{HeadLSN, RetainStart}.
	// Replica responds with its flushedLSN (~1).
	// Handshake gap analysis: R(1) < S(retainStart) → NeedsRebuild.
	syncDone := make(chan error, 1)
	go func() {
		syncDone <- primary.SyncCache()
	}()

	select {
	case err := <-syncDone:
		if err == nil {
			t.Fatal("SyncCache should fail — handshake should detect gap beyond retained WAL")
		}
	case <-time.After(10 * time.Second):
		t.Fatal("SyncCache hung")
	}

	// Verify the NEW shipper detected the gap via handshake (not just budget).
	newS := primary.shipperGroup.Shipper(0)
	if newS.State() != ReplicaNeedsRebuild && newS.State() != ReplicaDegraded {
		t.Fatalf("new shipper should be NeedsRebuild or Degraded after handshake gap detection, got %s", newS.State())
	}
	t.Logf("CP13-7: reconnect handshake detected gap beyond retained WAL (state=%s)", newS.State())
}

// ---------- WAL retention ----------

// TestWalRetention_RequiredReplicaBlocksReclaim verifies that the flusher
// does not advance the WAL checkpoint past entries a recoverable replica
// still needs for catch-up.
//
// CP13-6 proof: retention floor from MinRecoverableFlushedLSN blocks reclaim.
func TestWalRetention_RequiredReplicaBlocksReclaim(t *testing.T) {
	primary, replica := createSyncAllPair(t)
	defer primary.Close()
	defer replica.Close()

	recv, err := NewReplicaReceiver(replica, "127.0.0.1:0", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	recv.Serve()

	primary.SetReplicaAddr(recv.DataAddr(), recv.CtrlAddr())

	// Write and sync while healthy — replica is caught up.
	if err := primary.WriteLBA(0, makeBlock('A')); err != nil {
		t.Fatal(err)
	}
	if err := primary.SyncCache(); err != nil {
		t.Fatal(err)
	}

	sg := primary.shipperGroup
	s := sg.Shipper(0)
	replicaFlushed := s.ReplicaFlushedLSN()
	if replicaFlushed == 0 {
		t.Fatal("replica should have flushedLSN > 0 after sync")
	}

	// Disconnect replica.
	recv.Stop()
	time.Sleep(50 * time.Millisecond)

	// Write more data — replica misses these.
	for i := uint64(1); i < 6; i++ {
		if err := primary.WriteLBA(i, makeBlock(byte('A'+i))); err != nil {
			t.Fatal(err)
		}
	}

	// Flush checkpoint — retention floor should block WAL tail advance.
	primary.flusher.FlushOnce()

	// CP13-6 assertion: the retention floor (from MinRecoverableFlushedLSN)
	// should prevent the checkpoint from advancing past replicaFlushedLSN.
	checkpointLSN := primary.flusher.CheckpointLSN()
	if checkpointLSN > replicaFlushed {
		t.Fatalf("CP13-6: checkpoint %d advanced past replicaFlushedLSN %d — retention hold failed",
			checkpointLSN, replicaFlushed)
	}

	t.Logf("CP13-6: retention hold works — checkpoint=%d, replicaFlushed=%d (checkpoint did not advance past replica)",
		checkpointLSN, replicaFlushed)
}

// ---------- Ship degraded behavior ----------

// TestShip_DegradedDoesNotSilentlyCountAsHealthy verifies that a shipper
// pointing at a dead address eventually degrades and does not count as
// healthy for sync_all durability. Since CP13-4, Ship() allows the
// Disconnected state (bootstrap path), so the first Ship may succeed
// before the connection failure is detected. The key invariant: after
// degradation, the shipper's replicaFlushedLSN stays 0 (no durable
// confirmation from a dead replica).
func TestShip_DegradedDoesNotSilentlyCountAsHealthy(t *testing.T) {
	primary, replica := createSyncAllPair(t)
	defer primary.Close()
	defer replica.Close()

	// Point shipper at dead address — connection will fail.
	primary.SetReplicaAddr("127.0.0.1:1", "127.0.0.1:2")

	// Write — Ship attempts connection from Disconnected state.
	if err := primary.WriteLBA(0, makeBlock('A')); err != nil {
		t.Fatal(err)
	}

	// SyncCache will trigger a barrier which will fail (dead address).
	// This drives the shipper to Degraded.
	syncDone := make(chan error, 1)
	go func() {
		syncDone <- primary.SyncCache()
	}()
	select {
	case err := <-syncDone:
		if err == nil {
			t.Fatal("SyncCache should fail with dead replica under sync_all")
		}
	case <-time.After(10 * time.Second):
		t.Fatal("SyncCache hung")
	}

	sg := primary.shipperGroup
	if sg == nil {
		t.Fatal("no shipper group")
	}
	s0 := sg.Shipper(0)
	if s0 == nil {
		t.Fatal("no shipper at index 0")
	}

	// Shipper should not be InSync.
	if s0.State() == ReplicaInSync {
		t.Fatal("shipper should NOT be InSync with dead replica")
	}

	// ReplicaFlushedLSN must be 0 — no durable confirmation ever received.
	flushed := s0.ReplicaFlushedLSN()
	if flushed > 0 {
		t.Fatalf("replicaFlushedLSN=%d, expected 0 — dead replica should never confirm durability", flushed)
	}
}

// ---------- Reconnect edge cases ----------

// TestReconnect_EpochChangeDuringCatchup_Aborts verifies that if the primary's
// epoch advances while a replica is in CatchingUp state, the catch-up is
// aborted and the reconnect handshake restarts with the new epoch.
//
// Currently EXPECTED TO FAIL: no CatchingUp state or epoch-aware catch-up.
func TestReconnect_EpochChangeDuringCatchup_Aborts(t *testing.T) {
	primary, replica := createSyncAllPair(t)
	defer primary.Close()
	defer replica.Close()

	recv, err := NewReplicaReceiver(replica, "127.0.0.1:0", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	recv.Serve()
	defer recv.Stop()

	primary.SetReplicaAddr(recv.DataAddr(), recv.CtrlAddr())

	// Write and sync at epoch 1.
	for i := uint64(0); i < 3; i++ {
		if err := primary.WriteLBA(i, makeBlock(byte('A'+i))); err != nil {
			t.Fatal(err)
		}
	}
	if err := primary.SyncCache(); err != nil {
		t.Fatalf("SyncCache epoch 1: %v", err)
	}

	// Disconnect replica.
	recv.Stop()
	time.Sleep(50 * time.Millisecond)

	// Write at epoch 1 (replica misses these).
	for i := uint64(3); i < 6; i++ {
		if err := primary.WriteLBA(i, makeBlock(byte('A'+i))); err != nil {
			t.Fatal(err)
		}
	}

	// Advance epoch to 2 BEFORE reconnect.
	primary.SetEpoch(2)
	primary.SetMasterEpoch(2)

	// Reconnect replica (still at epoch 1).
	recv2, err := NewReplicaReceiver(replica, "127.0.0.1:0", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	recv2.Serve()
	defer recv2.Stop()
	primary.SetReplicaAddr(recv2.DataAddr(), recv2.CtrlAddr())

	// The handshake should detect epoch mismatch and reject the catch-up.
	// The replica must update to epoch 2 before it can rejoin.
	// SyncCache should fail because the replica can't participate at epoch 1.
	syncDone := make(chan error, 1)
	go func() {
		syncDone <- primary.SyncCache()
	}()

	select {
	case err := <-syncDone:
		if err == nil {
			t.Fatal("SyncCache succeeded with epoch mismatch during catch-up — should abort")
		}
		t.Logf("correctly failed: %v", err)
	case <-time.After(10 * time.Second):
		t.Fatal("SyncCache hung — epoch change during catch-up not handled")
	}
}

// TestReconnect_CatchupTimeout_TransitionsDegraded verifies that if WAL
// catch-up takes longer than the configured timeout, the replica transitions
// to Degraded (not stuck in CatchingUp forever).
//
// Currently EXPECTED TO FAIL: no catch-up timeout mechanism.
func TestReconnect_CatchupTimeout_TransitionsDegraded(t *testing.T) {
	primary, replica := createSyncAllPair(t)
	defer primary.Close()
	defer replica.Close()

	recv, err := NewReplicaReceiver(replica, "127.0.0.1:0", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	recv.Serve()
	defer recv.Stop()

	primary.SetReplicaAddr(recv.DataAddr(), recv.CtrlAddr())

	// Write and sync.
	if err := primary.WriteLBA(0, makeBlock('A')); err != nil {
		t.Fatal(err)
	}
	if err := primary.SyncCache(); err != nil {
		t.Fatal(err)
	}

	// Disconnect replica.
	recv.Stop()
	time.Sleep(50 * time.Millisecond)

	// Write a lot while disconnected — creates large catch-up gap.
	for i := uint64(1); i < 50; i++ {
		if err := primary.WriteLBA(i%10, makeBlock(byte('0'+i%10))); err != nil {
			t.Fatal(err)
		}
	}

	// Reconnect replica — catch-up will be needed.
	recv2, err := NewReplicaReceiver(replica, "127.0.0.1:0", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	recv2.Serve()
	defer recv2.Stop()
	primary.SetReplicaAddr(recv2.DataAddr(), recv2.CtrlAddr())

	// After CP13-5: catch-up should either complete within timeout
	// or transition to Degraded. SyncCache should not hang indefinitely.
	syncDone := make(chan error, 1)
	go func() {
		syncDone <- primary.SyncCache()
	}()

	select {
	case err := <-syncDone:
		// Either success (catch-up completed) or error (timeout/degraded).
		// Both are acceptable — the key is it doesn't hang.
		t.Logf("SyncCache returned: %v (catch-up bounded)", err)
	case <-time.After(15 * time.Second):
		t.Fatal("SyncCache hung >15s — catch-up timeout not implemented")
	}
}

// ---------- Barrier edge cases ----------

// TestBarrier_DuringCatchup_Rejected verifies that a barrier request is
// rejected while the replica is in CatchingUp state. Only InSync replicas
// may participate in sync_all barriers.
//
// Currently EXPECTED TO FAIL: no CatchingUp state exists.
func TestBarrier_DuringCatchup_Rejected(t *testing.T) {
	primary, replica := createSyncAllPair(t)
	defer primary.Close()
	defer replica.Close()

	recv, err := NewReplicaReceiver(replica, "127.0.0.1:0", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	recv.Serve()
	defer recv.Stop()

	primary.SetReplicaAddr(recv.DataAddr(), recv.CtrlAddr())

	// Initial write + sync (healthy).
	if err := primary.WriteLBA(0, makeBlock('A')); err != nil {
		t.Fatal(err)
	}
	if err := primary.SyncCache(); err != nil {
		t.Fatal(err)
	}

	// Disconnect and write more (creates gap).
	recv.Stop()
	time.Sleep(50 * time.Millisecond)

	for i := uint64(1); i < 10; i++ {
		if err := primary.WriteLBA(i, makeBlock(byte('A'+i))); err != nil {
			t.Fatal(err)
		}
	}

	// Reconnect — replica is behind and needs catch-up.
	recv2, err := NewReplicaReceiver(replica, "127.0.0.1:0", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	recv2.Serve()
	defer recv2.Stop()
	primary.SetReplicaAddr(recv2.DataAddr(), recv2.CtrlAddr())

	// Immediately attempt SyncCache — replica should be in CatchingUp,
	// not yet InSync. Barrier must either fail fast or wait for catch-up
	// to complete (not succeed prematurely with stale replica state).
	syncDone := make(chan error, 1)
	go func() {
		syncDone <- primary.SyncCache()
	}()

	select {
	case err := <-syncDone:
		if err == nil {
			// After CP13-4/5: this would only succeed if catch-up completed
			// fast enough. We need to verify the replica actually has all data.
			replica.flusher.FlushOnce()
			for i := uint64(0); i < 10; i++ {
				got, _ := replica.ReadLBA(i, 4096)
				if got[0] != byte('A'+i) {
					t.Fatalf("SyncCache returned nil but replica missing LBA %d — barrier accepted during catch-up gap", i)
				}
			}
			t.Log("SyncCache succeeded — replica must have completed catch-up")
		} else {
			t.Logf("SyncCache correctly failed during catch-up: %v", err)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("SyncCache hung — barrier not bounded during catch-up phase")
	}
}

// TestBarrier_ReplicaSlowFsync_Timeout verifies that a barrier does not
// hang indefinitely when the replica's fdatasync takes too long.
// The barrier must timeout and return an error.
//
// Currently EXPECTED TO FAIL: barrier timeout is 5s (barrierTimeout constant)
// which works, but this test validates the behavior explicitly.
func TestBarrier_ReplicaSlowFsync_Timeout(t *testing.T) {
	primary, replica := createSyncAllPair(t)
	defer primary.Close()
	defer replica.Close()

	recv, err := NewReplicaReceiver(replica, "127.0.0.1:0", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	recv.Serve()
	defer recv.Stop()

	primary.SetReplicaAddr(recv.DataAddr(), recv.CtrlAddr())

	// Write data.
	if err := primary.WriteLBA(0, makeBlock('A')); err != nil {
		t.Fatal(err)
	}
	if err := primary.SyncCache(); err != nil {
		t.Fatalf("initial SyncCache: %v", err)
	}

	// Now kill the replica's control channel but keep data channel alive.
	// This simulates a replica that received entries but can't respond to barriers
	// (e.g., stuck in a long fdatasync).
	recv.Stop()
	time.Sleep(50 * time.Millisecond)

	// Write more — these go to the degraded shipper.
	if err := primary.WriteLBA(1, makeBlock('B')); err != nil {
		t.Fatal(err)
	}

	// SyncCache — the barrier should timeout, not hang forever.
	start := time.Now()
	syncDone := make(chan error, 1)
	go func() {
		syncDone <- primary.SyncCache()
	}()

	select {
	case err := <-syncDone:
		elapsed := time.Since(start)
		if err == nil {
			t.Fatal("SyncCache succeeded with dead replica — should have timed out")
		}
		// Barrier timeout is 5s. The SyncCache should return within ~6s
		// (5s barrier + some overhead).
		if elapsed > 12*time.Second {
			t.Fatalf("barrier took %v — timeout not working (expected <12s)", elapsed)
		}
		t.Logf("barrier failed in %v: %v", elapsed, err)
	case <-time.After(15 * time.Second):
		t.Fatal("SyncCache hung >15s — barrier timeout broken")
	}
}

// ---------- WAL retention edge cases ----------

// TestWalRetention_TimeoutTriggersNeedsRebuild verifies that a replica
// disconnected for longer than the retention timeout is automatically
// transitioned to NeedsRebuild, and the WAL hold is released.
//
// CP13-6 proof: timeout budget triggers real NeedsRebuild state transition.
func TestWalRetention_TimeoutTriggersNeedsRebuild(t *testing.T) {
	primary, replica := createSyncAllPair(t)
	defer primary.Close()
	defer replica.Close()

	recv, err := NewReplicaReceiver(replica, "127.0.0.1:0", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	recv.Serve()

	primary.SetReplicaAddr(recv.DataAddr(), recv.CtrlAddr())

	// Write and sync while healthy.
	if err := primary.WriteLBA(0, makeBlock('A')); err != nil {
		t.Fatal(err)
	}
	if err := primary.SyncCache(); err != nil {
		t.Fatal(err)
	}

	sg := primary.shipperGroup
	s := sg.Shipper(0)
	if s.State() != ReplicaInSync {
		t.Fatalf("expected InSync after sync, got %s", s.State())
	}

	// Disconnect replica.
	recv.Stop()
	time.Sleep(50 * time.Millisecond)

	// Write more — replica misses these.
	for i := uint64(1); i < 6; i++ {
		if err := primary.WriteLBA(i, makeBlock(byte('A'+i))); err != nil {
			t.Fatal(err)
		}
	}

	// CP13-6: Evaluate with a very short timeout (1ns) to trigger timeout escalation.
	// The shipper's lastContactTime was set during the successful barrier above,
	// so even 1ns ago is "too long ago" relative to a 1ns timeout.
	sg.EvaluateRetentionBudgets(RetentionBudgetParams{
		Timeout:        1 * time.Nanosecond, // effectively expired
		MaxBytes:       0,                   // disable max-bytes for this test
		PrimaryHeadLSN: primary.nextLSN.Load() - 1,
		BlockSize:      primary.super.BlockSize,
	})

	// The shipper must now be NeedsRebuild.
	st := s.State()
	if st != ReplicaNeedsRebuild {
		t.Fatalf("CP13-6: expected NeedsRebuild after timeout, got %s", st)
	}

	// Hard assertion: WAL hold released after NeedsRebuild.
	// Record checkpoint before flush, flush, assert it advances past the old floor.
	replicaFlushed := s.ReplicaFlushedLSN()
	checkpointBefore := primary.flusher.CheckpointLSN()
	primary.flusher.FlushOnce()
	checkpointAfter := primary.flusher.CheckpointLSN()
	if checkpointAfter <= replicaFlushed {
		t.Fatalf("CP13-6: checkpoint should advance past replicaFlushedLSN %d after hold released, got %d",
			replicaFlushed, checkpointAfter)
	}
	t.Logf("CP13-6: hold released — checkpoint %d→%d (past replicaFlushed=%d)",
		checkpointBefore, checkpointAfter, replicaFlushed)
}

// TestWalRetention_MaxBytesTriggersNeedsRebuild verifies that when the
// replica lag exceeds the configured maximum retention bytes, the replica
// is transitioned to NeedsRebuild and the WAL hold is released.
//
// CP13-6: max-bytes budget now has a real state effect.
func TestWalRetention_MaxBytesTriggersNeedsRebuild(t *testing.T) {
	dir := t.TempDir()
	opts := CreateOptions{
		VolumeSize:     1 * 1024 * 1024,
		BlockSize:      4096,
		WALSize:        64 * 1024, // small WAL
		DurabilityMode: DurabilitySyncAll,
	}

	primary, err := CreateBlockVol(filepath.Join(dir, "primary.blk"), opts)
	if err != nil {
		t.Fatal(err)
	}
	defer primary.Close()
	primary.SetRole(RolePrimary)
	primary.SetEpoch(1)
	primary.SetMasterEpoch(1)
	primary.lease.Grant(60 * time.Second) // long lease to avoid expiry during test

	replica, err := CreateBlockVol(filepath.Join(dir, "replica.blk"), opts)
	if err != nil {
		t.Fatal(err)
	}
	defer replica.Close()
	replica.SetRole(RoleReplica)
	replica.SetEpoch(1)
	replica.SetMasterEpoch(1)

	recv, err := NewReplicaReceiver(replica, "127.0.0.1:0", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	recv.Serve()

	primary.SetReplicaAddr(recv.DataAddr(), recv.CtrlAddr())

	// Initial sync.
	if err := primary.WriteLBA(0, makeBlock('A')); err != nil {
		t.Fatal(err)
	}
	if err := primary.SyncCache(); err != nil {
		t.Fatal(err)
	}

	sg := primary.shipperGroup
	s := sg.Shipper(0)
	if s.State() != ReplicaInSync {
		t.Fatalf("expected InSync after initial sync, got %s", s.State())
	}
	replicaFlushedBefore := s.ReplicaFlushedLSN()

	// Disconnect replica.
	recv.Stop()
	time.Sleep(50 * time.Millisecond)

	// Write a few entries — enough to create meaningful lag but not overflow the tiny WAL.
	// 64KB WAL fits ~12 entries. Write 8 to stay within capacity.
	for i := uint64(0); i < 8; i++ {
		if err := primary.WriteLBA(i%8, makeBlock(byte('0'+i%10))); err != nil {
			t.Fatalf("write %d: %v", i, err)
		}
	}

	// CP13-6: Evaluate retention budgets with a small max-bytes threshold.
	// The lag (~8 entries * 4KB = ~32KB) exceeds 8KB budget → NeedsRebuild.
	primaryHead := primary.nextLSN.Load() - 1
	sg.EvaluateRetentionBudgets(RetentionBudgetParams{
		Timeout:        5 * time.Minute, // no timeout trigger
		MaxBytes:       8 * 1024,        // 8KB — lag exceeds this
		PrimaryHeadLSN: primaryHead,
		BlockSize:      primary.super.BlockSize,
	})

	// The shipper must now be NeedsRebuild (not just Degraded).
	st := s.State()
	if st != ReplicaNeedsRebuild {
		t.Fatalf("CP13-6: expected NeedsRebuild after max-bytes exceeded, got %s", st)
	}

	// The replica's flushedLSN should not have advanced (it was disconnected).
	if s.ReplicaFlushedLSN() != replicaFlushedBefore {
		t.Fatalf("replicaFlushedLSN should not change while disconnected: was %d, now %d",
			replicaFlushedBefore, s.ReplicaFlushedLSN())
	}

	t.Logf("CP13-6: max-bytes budget triggered NeedsRebuild (lag=%d entries, replicaFlushed=%d, primaryHead=%d)",
		primaryHead-replicaFlushedBefore, replicaFlushedBefore, primaryHead)
}

// ---------- Data integrity ----------

// TestCatchupReplay_DataIntegrity_AllBlocksMatch verifies that after a
// WAL catch-up, every block on the replica matches the primary exactly.
//
// Currently EXPECTED TO FAIL: no catch-up protocol — replica stays behind.
func TestCatchupReplay_DataIntegrity_AllBlocksMatch(t *testing.T) {
	primary, replica := createSyncAllPair(t)
	defer primary.Close()
	defer replica.Close()

	recv, err := NewReplicaReceiver(replica, "127.0.0.1:0", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	recv.Serve()

	savedDataAddr := recv.DataAddr()
	savedCtrlAddr := recv.CtrlAddr()
	primary.SetReplicaAddr(savedDataAddr, savedCtrlAddr)

	// Phase 1: Write 5 blocks while healthy.
	for i := uint64(0); i < 5; i++ {
		if err := primary.WriteLBA(i, makeBlock(byte('A'+i))); err != nil {
			t.Fatal(err)
		}
	}
	if err := primary.SyncCache(); err != nil {
		t.Fatal(err)
	}

	// Phase 2: Disconnect, write 5 more blocks (gap).
	recv.Stop()
	time.Sleep(50 * time.Millisecond)

	for i := uint64(5); i < 10; i++ {
		if err := primary.WriteLBA(i, makeBlock(byte('A'+i))); err != nil {
			t.Fatal(err)
		}
	}

	// Phase 3: Reconnect on same addresses — same shipper, same progress.
	recv2, err := NewReplicaReceiver(replica, savedDataAddr, savedCtrlAddr)
	if err != nil {
		t.Fatalf("reconnect receiver: %v", err)
	}
	recv2.Serve()
	defer recv2.Stop()

	// Wait for catch-up + barrier.
	syncDone := make(chan error, 1)
	go func() {
		syncDone <- primary.SyncCache()
	}()

	select {
	case err := <-syncDone:
		if err != nil {
			t.Fatalf("SyncCache after reconnect: %v — catch-up failed, can't verify integrity", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("SyncCache hung — catch-up not implemented")
	}

	// Phase 4: Verify every block matches on primary and replica.
	primary.flusher.FlushOnce()
	replica.flusher.FlushOnce()

	for i := uint64(0); i < 10; i++ {
		pData, err := primary.ReadLBA(i, 4096)
		if err != nil {
			t.Fatalf("primary ReadLBA(%d): %v", i, err)
		}
		rData, err := replica.ReadLBA(i, 4096)
		if err != nil {
			t.Fatalf("replica ReadLBA(%d): %v", i, err)
		}
		if !bytes.Equal(pData, rData) {
			t.Fatalf("LBA %d: primary=%c replica=%c — data divergence after catch-up",
				i, pData[0], rData[0])
		}
	}
}

// TestCatchupReplay_DuplicateEntry_Idempotent verifies that if the catch-up
// replays an entry the replica already has (overlap between shipped and
// catch-up range), the replay is idempotent — no double-apply, no error.
//
// Currently EXPECTED TO FAIL: no catch-up protocol.
func TestCatchupReplay_DuplicateEntry_Idempotent(t *testing.T) {
	primary, replica := createSyncAllPair(t)
	defer primary.Close()
	defer replica.Close()

	recv, err := NewReplicaReceiver(replica, "127.0.0.1:0", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	recv.Serve()

	savedDataAddr := recv.DataAddr()
	savedCtrlAddr := recv.CtrlAddr()
	primary.SetReplicaAddr(savedDataAddr, savedCtrlAddr)

	// Write 5 entries and sync — replica has all 5.
	for i := uint64(0); i < 5; i++ {
		if err := primary.WriteLBA(i, makeBlock(byte('A'+i))); err != nil {
			t.Fatal(err)
		}
	}
	if err := primary.SyncCache(); err != nil {
		t.Fatal(err)
	}

	replicaLSN := recv.ReceivedLSN()
	if replicaLSN < 5 {
		t.Fatalf("replica only at LSN %d, expected >=5 before disconnect", replicaLSN)
	}

	// Disconnect briefly, write 2 more.
	recv.Stop()
	time.Sleep(50 * time.Millisecond)

	for i := uint64(5); i < 7; i++ {
		if err := primary.WriteLBA(i, makeBlock(byte('A'+i))); err != nil {
			t.Fatal(err)
		}
	}

	// Reconnect on same addresses — same shipper, same flushed progress.
	// Catch-up may replay from an LSN the replica already has (overlap).
	// The replay must be safe: entries <= receivedLSN are skipped.
	recv2, err := NewReplicaReceiver(replica, savedDataAddr, savedCtrlAddr)
	if err != nil {
		t.Fatalf("reconnect receiver: %v", err)
	}
	recv2.Serve()
	defer recv2.Stop()

	syncDone := make(chan error, 1)
	go func() {
		syncDone <- primary.SyncCache()
	}()

	select {
	case err := <-syncDone:
		if err != nil {
			t.Fatalf("SyncCache after reconnect with overlap: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("SyncCache hung — catch-up with duplicate handling not implemented")
	}

	// Verify data integrity — all 7 blocks must be correct.
	replica.flusher.FlushOnce()
	for i := uint64(0); i < 7; i++ {
		got, err := replica.ReadLBA(i, 4096)
		if err != nil {
			t.Fatalf("replica ReadLBA(%d): %v", i, err)
		}
		if got[0] != byte('A'+i) {
			t.Fatalf("LBA %d: expected %c, got %c — duplicate entry corrupted data", i, 'A'+i, got[0])
		}
	}
}

// ---------- best_effort mode ----------

// TestBestEffort_FlushSucceeds_ReplicaDown verifies that under best_effort
// mode, SyncCache (FLUSH) succeeds even when all replicas are down.
// best_effort = primary-local durability only.
//
// Currently EXPECTED: PASS — best_effort should already work this way.
func TestBestEffort_FlushSucceeds_ReplicaDown(t *testing.T) {
	pDir := t.TempDir()
	opts := CreateOptions{
		VolumeSize:     1 * 1024 * 1024,
		BlockSize:      4096,
		WALSize:        256 * 1024,
		DurabilityMode: DurabilityBestEffort,
	}

	primary, err := CreateBlockVol(filepath.Join(pDir, "primary.blockvol"), opts)
	if err != nil {
		t.Fatal(err)
	}
	defer primary.Close()
	primary.SetRole(RolePrimary)
	primary.SetEpoch(1)
	primary.SetMasterEpoch(1)
	primary.lease.Grant(30 * time.Second)

	// Point shipper at dead address — immediately degraded.
	primary.SetReplicaAddr("127.0.0.1:1", "127.0.0.1:2")

	// Write data.
	for i := uint64(0); i < 5; i++ {
		if err := primary.WriteLBA(i, makeBlock(byte('A'+i))); err != nil {
			t.Fatalf("WriteLBA(%d): %v", i, err)
		}
	}

	// SyncCache under best_effort with all replicas dead MUST succeed.
	// best_effort only requires primary-local durability.
	syncDone := make(chan error, 1)
	go func() {
		syncDone <- primary.SyncCache()
	}()

	select {
	case err := <-syncDone:
		if err != nil {
			t.Fatalf("best_effort SyncCache failed with dead replica: %v — should succeed (primary-local only)", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("best_effort SyncCache hung — should be primary-local only, no barrier wait")
	}

	// Verify data is readable from primary.
	for i := uint64(0); i < 5; i++ {
		got, err := primary.ReadLBA(i, 4096)
		if err != nil {
			t.Fatalf("ReadLBA(%d): %v", i, err)
		}
		if got[0] != byte('A'+i) {
			t.Fatalf("LBA %d: expected %c, got %c", i, 'A'+i, got[0])
		}
	}
}

// ============================================================
// CP13-4: Replica State Machine Tests
// ============================================================

func TestReplicaState_InitialDisconnected(t *testing.T) {
	s := NewWALShipper("127.0.0.1:9001", "127.0.0.1:9002", func() uint64 { return 1 }, nil)
	if s.State() != ReplicaDisconnected {
		t.Fatalf("initial state: got %s, want disconnected", s.State())
	}
}

func TestReplicaState_ShipDoesNotGrantInSync(t *testing.T) {
	primary, replica := createReplicaVolPair(t)
	defer primary.Close()
	defer replica.Close()

	recv, err := NewReplicaReceiver(replica, "127.0.0.1:0", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	recv.Serve()
	defer recv.Stop()

	primary.SetReplicaAddr(recv.DataAddr(), recv.CtrlAddr())
	primary.SetRole(RolePrimary)
	primary.SetEpoch(1)
	primary.SetMasterEpoch(1)
	primary.lease.Grant(30 * time.Second)
	replica.SetRole(RoleReplica)
	replica.SetEpoch(1)
	replica.SetMasterEpoch(1)

	shipper := primary.shipperGroup.Shipper(0)

	// Ship does not grant InSync — shipper stays Disconnected.
	// (Ship silently returns nil because state != InSync)
	if err := primary.WriteLBA(0, makeBlock('A')); err != nil {
		t.Fatalf("write: %v", err)
	}
	time.Sleep(20 * time.Millisecond) // allow ship goroutine to run

	if shipper.State() == ReplicaInSync {
		t.Fatal("Ship should not grant InSync")
	}
}

func TestReplicaState_BarrierBootstrapGrantsInSync(t *testing.T) {
	primary, replica := createReplicaVolPair(t)
	defer primary.Close()
	defer replica.Close()

	recv, err := NewReplicaReceiver(replica, "127.0.0.1:0", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	recv.Serve()
	defer recv.Stop()

	primary.SetReplicaAddr(recv.DataAddr(), recv.CtrlAddr())
	primary.SetRole(RolePrimary)
	primary.SetEpoch(1)
	primary.SetMasterEpoch(1)
	primary.lease.Grant(30 * time.Second)
	replica.SetRole(RoleReplica)
	replica.SetEpoch(1)
	replica.SetMasterEpoch(1)

	shipper := primary.shipperGroup.Shipper(0)

	// Before barrier, state is Disconnected.
	if shipper.State() != ReplicaDisconnected {
		t.Fatalf("before barrier: got %s, want disconnected", shipper.State())
	}

	// SyncCache triggers barrier — barrier success grants InSync.
	// Note: lsnMax will be 0 (no writes), barrier at LSN=0 should succeed.
	if err := primary.SyncCache(); err != nil {
		t.Fatalf("SyncCache: %v", err)
	}

	if shipper.State() != ReplicaInSync {
		t.Fatalf("after barrier: got %s, want in_sync", shipper.State())
	}
}

func TestReplicaState_ShipFailureTransitionsToDegraded(t *testing.T) {
	primary, replica := createReplicaVolPair(t)
	defer primary.Close()
	defer replica.Close()

	recv, err := NewReplicaReceiver(replica, "127.0.0.1:0", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	recv.Serve()
	defer recv.Stop()

	primary.SetReplicaAddr(recv.DataAddr(), recv.CtrlAddr())
	primary.SetRole(RolePrimary)
	primary.SetEpoch(1)
	primary.SetMasterEpoch(1)
	primary.lease.Grant(30 * time.Second)
	replica.SetRole(RoleReplica)
	replica.SetEpoch(1)
	replica.SetMasterEpoch(1)

	shipper := primary.shipperGroup.Shipper(0)

	// Bootstrap to InSync via barrier.
	if err := primary.SyncCache(); err != nil {
		t.Fatalf("SyncCache: %v", err)
	}
	if shipper.State() != ReplicaInSync {
		t.Fatalf("expected in_sync after barrier, got %s", shipper.State())
	}

	// Kill replica to cause Ship failure.
	recv.Stop()
	time.Sleep(20 * time.Millisecond)

	// Write — Ship will fail and mark degraded.
	primary.WriteLBA(0, makeBlock('X'))
	time.Sleep(50 * time.Millisecond) // allow ship to attempt and fail

	if shipper.State() != ReplicaDegraded {
		t.Fatalf("after ship failure: got %s, want degraded", shipper.State())
	}
}

func TestReplicaState_BarrierDegradedReconnectFail_StaysDegraded(t *testing.T) {
	s := NewWALShipper("127.0.0.1:1", "127.0.0.1:2", func() uint64 { return 1 }, nil)
	// Force to Degraded.
	s.state.Store(uint32(ReplicaDegraded))

	err := s.Barrier(10)
	if err == nil {
		t.Fatal("barrier should fail for degraded shipper with dead ports")
	}
	if s.State() != ReplicaDegraded {
		t.Fatalf("after failed reconnect: got %s, want degraded", s.State())
	}
}

func TestReplicaState_BarrierDegradedReconnectSuccess_RestoresInSync(t *testing.T) {
	primary, replica := createReplicaVolPair(t)
	defer primary.Close()
	defer replica.Close()

	recv, err := NewReplicaReceiver(replica, "127.0.0.1:0", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	recv.Serve()
	defer recv.Stop()

	primary.SetReplicaAddr(recv.DataAddr(), recv.CtrlAddr())
	primary.SetRole(RolePrimary)
	primary.SetEpoch(1)
	primary.SetMasterEpoch(1)
	primary.lease.Grant(30 * time.Second)
	replica.SetRole(RoleReplica)
	replica.SetEpoch(1)
	replica.SetMasterEpoch(1)

	shipper := primary.shipperGroup.Shipper(0)

	// Force to Degraded (simulating prior failure).
	shipper.state.Store(uint32(ReplicaDegraded))

	// SyncCache triggers barrier — reconnect succeeds, barrier succeeds → InSync.
	if err := primary.SyncCache(); err != nil {
		t.Fatalf("SyncCache: %v", err)
	}
	if shipper.State() != ReplicaInSync {
		t.Fatalf("after reconnect+barrier: got %s, want in_sync", shipper.State())
	}
}

func TestShipperGroup_InSyncCount(t *testing.T) {
	s1 := NewWALShipper("127.0.0.1:9001", "127.0.0.1:9002", func() uint64 { return 1 }, nil)
	s2 := NewWALShipper("127.0.0.1:9003", "127.0.0.1:9004", func() uint64 { return 1 }, nil)
	group := NewShipperGroup([]*WALShipper{s1, s2})

	// Both disconnected.
	if group.InSyncCount() != 0 {
		t.Fatalf("expected 0, got %d", group.InSyncCount())
	}

	// One InSync.
	s1.state.Store(uint32(ReplicaInSync))
	if group.InSyncCount() != 1 {
		t.Fatalf("expected 1, got %d", group.InSyncCount())
	}

	// Both InSync.
	s2.state.Store(uint32(ReplicaInSync))
	if group.InSyncCount() != 2 {
		t.Fatalf("expected 2, got %d", group.InSyncCount())
	}

	// One degraded.
	s1.state.Store(uint32(ReplicaDegraded))
	if group.InSyncCount() != 1 {
		t.Fatalf("expected 1 after degrading s1, got %d", group.InSyncCount())
	}
}

// ============================================================
// CP13-3: Durable Progress Truth Tests
// ============================================================

func TestBarrierResp_FlushedLSN_Roundtrip(t *testing.T) {
	resp := BarrierResponse{Status: BarrierOK, FlushedLSN: 42}
	encoded := EncodeBarrierResponse(resp)
	if len(encoded) != 9 {
		t.Fatalf("expected 9 bytes, got %d", len(encoded))
	}
	decoded := DecodeBarrierResponse(encoded)
	if decoded.Status != BarrierOK {
		t.Fatalf("status: got %d, want %d", decoded.Status, BarrierOK)
	}
	if decoded.FlushedLSN != 42 {
		t.Fatalf("FlushedLSN: got %d, want 42", decoded.FlushedLSN)
	}
}

func TestBarrierResp_BackwardCompat_1Byte(t *testing.T) {
	// Legacy replica sends only 1 status byte.
	legacy := []byte{BarrierOK}
	decoded := DecodeBarrierResponse(legacy)
	if decoded.Status != BarrierOK {
		t.Fatalf("status: got %d, want %d", decoded.Status, BarrierOK)
	}
	if decoded.FlushedLSN != 0 {
		t.Fatalf("FlushedLSN should be 0 for legacy response, got %d", decoded.FlushedLSN)
	}
}

// TestBarrier_LegacyResponseRejectedBySyncAll verifies that a BarrierOK response
// with FlushedLSN == 0 (legacy 1-byte format) is NOT accepted as successful
// sync_all durability. CP13-3: sync_all must require explicit durable progress
// authority, not just a status-OK byte.
//
// This test exercises the real shipper.Barrier() code path by running a fake
// control-path TCP server that responds with a legacy 1-byte BarrierOK.
func TestBarrier_LegacyResponseRejectedBySyncAll(t *testing.T) {
	// Start a fake control-path TCP server that reads a barrier request
	// and responds with a legacy 1-byte BarrierOK (no FlushedLSN).
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()

	legacyServerDone := make(chan struct{})
	go func() {
		defer close(legacyServerDone)
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		defer conn.Close()

		// Read the barrier request frame (we don't need to parse it).
		_, _, readErr := ReadFrame(conn)
		if readErr != nil {
			return
		}

		// Respond with legacy 1-byte BarrierOK (no FlushedLSN field).
		WriteFrame(conn, MsgBarrierResp, []byte{BarrierOK})
	}()

	// Create a shipper pointing at the fake control server.
	// dataAddr doesn't matter — we only test the control/barrier path.
	shipper := NewWALShipper("127.0.0.1:1", ln.Addr().String(), func() uint64 { return 1 }, nil)
	defer shipper.Stop()

	// Force the shipper to InSync so Barrier() doesn't try reconnect.
	shipper.state.Store(uint32(ReplicaInSync))

	// Call Barrier — this hits the real code path in wal_shipper.go:224-231.
	// The fake server returns BarrierOK with FlushedLSN=0.
	// CP13-3 fix: this must return an error, not nil.
	err = shipper.Barrier(5)
	if err == nil {
		t.Fatal("Barrier() should fail on legacy BarrierOK with FlushedLSN=0, but returned nil")
	}

	// The error message should mention the legacy response.
	if !strings.Contains(err.Error(), "no FlushedLSN") {
		t.Fatalf("expected error about missing FlushedLSN, got: %v", err)
	}

	// Shipper should NOT have gained flushed progress.
	if shipper.HasFlushedProgress() {
		t.Fatal("shipper should not have flushed progress after legacy response")
	}
	if shipper.ReplicaFlushedLSN() != 0 {
		t.Fatalf("replicaFlushedLSN should be 0 after legacy response, got %d", shipper.ReplicaFlushedLSN())
	}

	<-legacyServerDone
	t.Log("CP13-3: legacy BarrierOK with FlushedLSN=0 rejected by shipper.Barrier()")
}

// TestBarrier_NonEligibleStates_FailClosed verifies that Barrier() rejects
// every non-eligible state explicitly. CP13-4: only InSync counts toward
// sync durability; all other states must fail closed.
func TestBarrier_NonEligibleStates_FailClosed(t *testing.T) {
	// Create a shipper with a dead address (never connects).
	shipper := NewWALShipper("127.0.0.1:1", "127.0.0.1:2", func() uint64 { return 1 }, nil)
	defer shipper.Stop()

	nonEligible := []struct {
		state ReplicaState
		name  string
	}{
		{ReplicaConnecting, "Connecting"},
		{ReplicaCatchingUp, "CatchingUp"},
		{ReplicaNeedsRebuild, "NeedsRebuild"},
	}

	for _, tc := range nonEligible {
		t.Run(tc.name, func(t *testing.T) {
			shipper.state.Store(uint32(tc.state))
			err := shipper.Barrier(1)
			if err == nil {
				t.Fatalf("Barrier() should fail for state %s, but returned nil", tc.name)
			}
			// Must not transition to InSync.
			if shipper.State() == ReplicaInSync {
				t.Fatalf("state should not be InSync after failed barrier from %s", tc.name)
			}
		})
	}

	// Also verify: Disconnected with no prior flushed progress = bootstrap path,
	// which will fail on dead address but NOT via the "proceed to barrier" path.
	t.Run("Disconnected_noPrior", func(t *testing.T) {
		shipper.state.Store(uint32(ReplicaDisconnected))
		err := shipper.Barrier(1)
		if err == nil {
			t.Fatal("Barrier() should fail for Disconnected shipper with dead address")
		}
	})

	// Positive case: InSync enters the barrier request path.
	// Use a fake control server to observe MsgBarrierReq receipt — this
	// distinguishes "passed state gate and attempted barrier" from "rejected early".
	t.Run("InSync_enters_barrier_path", func(t *testing.T) {
		// Start a fake control server that records received messages.
		ctrlLn, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		defer ctrlLn.Close()

		barrierReceived := make(chan struct{}, 1)
		go func() {
			conn, err := ctrlLn.Accept()
			if err != nil {
				return
			}
			defer conn.Close()
			conn.SetReadDeadline(time.Now().Add(5 * time.Second))
			msgType, _, err := ReadFrame(conn)
			if err == nil && msgType == MsgBarrierReq {
				barrierReceived <- struct{}{}
			}
		}()

		// Create a shipper pointing at the fake control server.
		inSyncShipper := NewWALShipper("127.0.0.1:1", ctrlLn.Addr().String(), func() uint64 { return 1 }, nil)
		defer inSyncShipper.Stop()
		inSyncShipper.state.Store(uint32(ReplicaInSync))

		// Barrier will connect, send MsgBarrierReq, then fail (server doesn't respond).
		// The important thing: MsgBarrierReq was sent.
		_ = inSyncShipper.Barrier(1)

		select {
		case <-barrierReceived:
			t.Log("InSync: MsgBarrierReq received by server — barrier path entered")
		case <-time.After(3 * time.Second):
			t.Fatal("InSync should have sent MsgBarrierReq but server received nothing")
		}
	})

	t.Log("CP13-4: 5 sub-cases — 3 immediate reject, 1 Disconnected fail, 1 InSync barrier-path verified")
}

func TestReplica_FlushedLSN_OnlyAfterSync(t *testing.T) {
	primary, replica := createReplicaVolPair(t)
	defer primary.Close()
	defer replica.Close()

	recv, err := NewReplicaReceiver(replica, "127.0.0.1:0", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	recv.Serve()
	defer recv.Stop()

	primary.SetReplicaAddr(recv.DataAddr(), recv.CtrlAddr())

	// Before any barrier, FlushedLSN must be 0.
	if recv.FlushedLSN() != 0 {
		t.Fatalf("FlushedLSN before barrier: got %d, want 0", recv.FlushedLSN())
	}

	// Write data.
	primary.SetRole(RolePrimary)
	primary.SetEpoch(1)
	primary.SetMasterEpoch(1)
	primary.lease.Grant(30 * time.Second)
	replica.SetRole(RoleReplica)
	replica.SetEpoch(1)
	replica.SetMasterEpoch(1)

	if err := primary.WriteLBA(0, makeBlock('A')); err != nil {
		t.Fatalf("write: %v", err)
	}

	// Wait for replica to receive.
	waitForReceivedLSN(t, recv, 1, 5*time.Second)

	// ReceivedLSN should be 1, but FlushedLSN still 0 (no barrier yet).
	if recv.ReceivedLSN() < 1 {
		t.Fatalf("ReceivedLSN: got %d, want >= 1", recv.ReceivedLSN())
	}
	if recv.FlushedLSN() != 0 {
		t.Fatalf("FlushedLSN should still be 0 before barrier, got %d", recv.FlushedLSN())
	}

	// SyncCache triggers barrier → fd.Sync → FlushedLSN advances.
	if err := primary.SyncCache(); err != nil {
		t.Fatalf("SyncCache: %v", err)
	}

	// Now FlushedLSN should match.
	if recv.FlushedLSN() < 1 {
		t.Fatalf("FlushedLSN after barrier: got %d, want >= 1", recv.FlushedLSN())
	}
}

func TestReplica_FlushedLSN_NotOnReceive(t *testing.T) {
	primary, replica := createReplicaVolPair(t)
	defer primary.Close()
	defer replica.Close()

	recv, err := NewReplicaReceiver(replica, "127.0.0.1:0", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	recv.Serve()
	defer recv.Stop()

	primary.SetReplicaAddr(recv.DataAddr(), recv.CtrlAddr())
	primary.SetRole(RolePrimary)
	primary.SetEpoch(1)
	primary.SetMasterEpoch(1)
	primary.lease.Grant(30 * time.Second)
	replica.SetRole(RoleReplica)
	replica.SetEpoch(1)
	replica.SetMasterEpoch(1)

	// Write 5 entries — shipped to replica.
	for i := 0; i < 5; i++ {
		if err := primary.WriteLBA(uint64(i), makeBlock(byte('A'+i))); err != nil {
			t.Fatalf("write %d: %v", i, err)
		}
	}
	waitForReceivedLSN(t, recv, 5, 5*time.Second)

	// ReceivedLSN=5 but FlushedLSN must still be 0 (no barrier).
	if recv.FlushedLSN() != 0 {
		t.Fatalf("FlushedLSN should be 0 without barrier, got %d", recv.FlushedLSN())
	}
}

func TestShipper_ReplicaFlushedLSN_UpdatedOnBarrier(t *testing.T) {
	primary, replica := createReplicaVolPair(t)
	defer primary.Close()
	defer replica.Close()

	recv, err := NewReplicaReceiver(replica, "127.0.0.1:0", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	recv.Serve()
	defer recv.Stop()

	primary.SetReplicaAddr(recv.DataAddr(), recv.CtrlAddr())
	primary.SetRole(RolePrimary)
	primary.SetEpoch(1)
	primary.SetMasterEpoch(1)
	primary.lease.Grant(30 * time.Second)
	replica.SetRole(RoleReplica)
	replica.SetEpoch(1)
	replica.SetMasterEpoch(1)

	// Before barrier, shipper has no flushed progress.
	shipper := primary.shipperGroup.Shipper(0)
	if shipper == nil {
		t.Fatal("no shipper configured")
	}
	if shipper.ReplicaFlushedLSN() != 0 {
		t.Fatalf("ReplicaFlushedLSN before barrier: got %d, want 0", shipper.ReplicaFlushedLSN())
	}
	if shipper.HasFlushedProgress() {
		t.Fatal("HasFlushedProgress should be false before any barrier")
	}

	// Write + SyncCache (barrier).
	if err := primary.WriteLBA(0, makeBlock('X')); err != nil {
		t.Fatalf("write: %v", err)
	}
	if err := primary.SyncCache(); err != nil {
		t.Fatalf("SyncCache: %v", err)
	}

	// Shipper should now have flushed progress.
	if !shipper.HasFlushedProgress() {
		t.Fatal("HasFlushedProgress should be true after successful barrier")
	}
	if shipper.ReplicaFlushedLSN() < 1 {
		t.Fatalf("ReplicaFlushedLSN after barrier: got %d, want >= 1", shipper.ReplicaFlushedLSN())
	}
}

func TestShipper_ReplicaFlushedLSN_Monotonic(t *testing.T) {
	primary, replica := createReplicaVolPair(t)
	defer primary.Close()
	defer replica.Close()

	recv, err := NewReplicaReceiver(replica, "127.0.0.1:0", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	recv.Serve()
	defer recv.Stop()

	primary.SetReplicaAddr(recv.DataAddr(), recv.CtrlAddr())
	primary.SetRole(RolePrimary)
	primary.SetEpoch(1)
	primary.SetMasterEpoch(1)
	primary.lease.Grant(30 * time.Second)
	replica.SetRole(RoleReplica)
	replica.SetEpoch(1)
	replica.SetMasterEpoch(1)

	shipper := primary.shipperGroup.Shipper(0)

	// Write + sync 3 times.
	var prevFlushed uint64
	for round := 0; round < 3; round++ {
		if err := primary.WriteLBA(uint64(round), makeBlock(byte('A'+round))); err != nil {
			t.Fatalf("write round %d: %v", round, err)
		}
		if err := primary.SyncCache(); err != nil {
			t.Fatalf("SyncCache round %d: %v", round, err)
		}
		cur := shipper.ReplicaFlushedLSN()
		if cur < prevFlushed {
			t.Fatalf("round %d: FlushedLSN regressed from %d to %d", round, prevFlushed, cur)
		}
		prevFlushed = cur
	}
	if prevFlushed < 3 {
		t.Fatalf("final FlushedLSN: got %d, want >= 3", prevFlushed)
	}
}

func TestShipperGroup_MinReplicaFlushedLSN(t *testing.T) {
	// Test with no shippers.
	emptyGroup := NewShipperGroup(nil)
	_, ok := emptyGroup.MinReplicaFlushedLSN()
	if ok {
		t.Fatal("empty group should return (_, false)")
	}

	// Test with shippers that have no progress.
	s1 := NewWALShipper("127.0.0.1:9001", "127.0.0.1:9002", func() uint64 { return 1 }, nil)
	s2 := NewWALShipper("127.0.0.1:9003", "127.0.0.1:9004", func() uint64 { return 1 }, nil)
	group := NewShipperGroup([]*WALShipper{s1, s2})

	_, ok = group.MinReplicaFlushedLSN()
	if ok {
		t.Fatal("no shipper has flushed progress yet, should return false")
	}

	// Simulate s1 getting progress.
	s1.replicaFlushedLSN.Store(10)
	s1.hasFlushedProgress.Store(true)

	min, ok := group.MinReplicaFlushedLSN()
	if !ok {
		t.Fatal("s1 has progress, should return true")
	}
	if min != 10 {
		t.Fatalf("min: got %d, want 10", min)
	}

	// Simulate s2 getting lower progress.
	s2.replicaFlushedLSN.Store(5)
	s2.hasFlushedProgress.Store(true)

	min, ok = group.MinReplicaFlushedLSN()
	if !ok {
		t.Fatal("both have progress, should return true")
	}
	if min != 5 {
		t.Fatalf("min: got %d, want 5 (the lower one)", min)
	}
}

// waitForReceivedLSN polls until the receiver reaches the target LSN or times out.
func waitForReceivedLSN(t *testing.T, recv *ReplicaReceiver, target uint64, timeout time.Duration) {
	t.Helper()
	deadline := time.After(timeout)
	for recv.ReceivedLSN() < target {
		select {
		case <-deadline:
			t.Fatalf("timeout waiting for ReceivedLSN >= %d (got %d)", target, recv.ReceivedLSN())
		default:
			time.Sleep(time.Millisecond)
		}
	}
}
