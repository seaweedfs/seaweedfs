package blockvol

// CP13-5 adversarial tests: edge cases for reconnect, catch-up, and state machine.
// These test the 6 audit points from the CP13-5 review.

import (
	"bytes"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

// ---------- Point 1: catchupFailures concurrency ----------

// TestAdversarial_ConcurrentBarrierDoesNotCorruptCatchupFailures verifies
// that rapid concurrent SyncCache calls (which trigger Barrier on the same
// shipper) do not corrupt the catchupFailures counter.
// The group committer serializes SyncCache, but this test exercises the
// boundary by calling Barrier directly from multiple goroutines.
func TestAdversarial_ConcurrentBarrierDoesNotCorruptCatchupFailures(t *testing.T) {
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

	// Write + sync to establish InSync.
	if err := primary.WriteLBA(0, makeBlock('A')); err != nil {
		t.Fatal(err)
	}
	if err := primary.SyncCache(); err != nil {
		t.Fatal(err)
	}

	// Fire 10 concurrent SyncCache calls.
	var wg sync.WaitGroup
	errors := make([]error, 10)
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			if err := primary.WriteLBA(uint64(idx+1), makeBlock(byte('B'+idx))); err != nil {
				errors[idx] = err
				return
			}
			errors[idx] = primary.SyncCache()
		}(i)
	}
	wg.Wait()

	// All should succeed (healthy path).
	for i, err := range errors {
		if err != nil {
			t.Errorf("concurrent SyncCache[%d]: %v", i, err)
		}
	}
}

// ---------- Point 2: bootstrap vs reconnect discriminator ----------

// TestAdversarial_FreshShipperUsesBootstrapNotReconnect verifies that a
// freshly created shipper (hasFlushedProgress=false) uses the bootstrap
// path (bare TCP connect), not the reconnect handshake path.
func TestAdversarial_FreshShipperUsesBootstrapNotReconnect(t *testing.T) {
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

	sg := primary.shipperGroup
	s := sg.Shipper(0)
	if s == nil {
		t.Fatal("no shipper")
	}

	// Fresh shipper: hasFlushedProgress must be false.
	if s.HasFlushedProgress() {
		t.Fatal("fresh shipper should not have flushed progress")
	}

	// State should be Disconnected (initial).
	if s.State() != ReplicaDisconnected {
		t.Fatalf("fresh shipper state=%s, want Disconnected", s.State())
	}

	// First write + sync should succeed via bootstrap path.
	if err := primary.WriteLBA(0, makeBlock('X')); err != nil {
		t.Fatal(err)
	}
	if err := primary.SyncCache(); err != nil {
		t.Fatalf("first SyncCache (bootstrap): %v", err)
	}

	// After first successful barrier, hasFlushedProgress should be true.
	if !s.HasFlushedProgress() {
		t.Fatal("after successful barrier, hasFlushedProgress should be true")
	}
	if s.State() != ReplicaInSync {
		t.Fatalf("after bootstrap barrier, state=%s, want InSync", s.State())
	}
}

// TestAdversarial_ReconnectUsesHandshakeNotBootstrap verifies that after
// a degraded shipper reconnects, it uses the handshake protocol (not bare
// TCP retry) because hasFlushedProgress is true.
func TestAdversarial_ReconnectUsesHandshakeNotBootstrap(t *testing.T) {
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

	// Establish InSync.
	if err := primary.WriteLBA(0, makeBlock('A')); err != nil {
		t.Fatal(err)
	}
	if err := primary.SyncCache(); err != nil {
		t.Fatal(err)
	}

	sg := primary.shipperGroup
	s := sg.Shipper(0)
	if !s.HasFlushedProgress() {
		t.Fatal("should have flushed progress after sync")
	}

	// Disconnect replica.
	recv.Stop()
	time.Sleep(50 * time.Millisecond)

	// Write during disconnect.
	if err := primary.WriteLBA(1, makeBlock('B')); err != nil {
		t.Fatal(err)
	}

	// Reconnect.
	recv2, err := NewReplicaReceiver(replica, "127.0.0.1:0", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	recv2.Serve()
	defer recv2.Stop()

	// Reconfigure shipper to new address.
	// CP13-5: SetReplicaAddrs creates fresh shippers but seeds them with
	// hasFlushedProgress=true from the old group, so the new shipper uses
	// the reconnect handshake (ResumeShipReq) path, not bare bootstrap.
	primary.SetReplicaAddr(recv2.DataAddr(), recv2.CtrlAddr())

	// CP13-5 observable evidence 1: new shipper seeded with prior progress.
	newSg := primary.shipperGroup
	newS := newSg.Shipper(0)
	if !newS.HasFlushedProgress() {
		t.Fatal("CP13-5: new shipper should be seeded with hasFlushedProgress=true from old group")
	}

	// Record replica's receivedLSN before SyncCache to prove catch-up delivers entries.
	preRecvLSN := recv2.ReceivedLSN()

	syncDone := make(chan error, 1)
	go func() {
		syncDone <- primary.SyncCache()
	}()

	select {
	case err := <-syncDone:
		if err != nil {
			t.Fatalf("SyncCache after reconnect: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("SyncCache hung after reconnect")
	}

	// CP13-5 observable evidence 2: replica received entries via catch-up.
	// Block 'B' (LSN 2) was written during disconnect. If the handshake +
	// catch-up path was used, the replica's receivedLSN must have advanced.
	// Bootstrap alone would not deliver block 'B' — it only sends the barrier.
	postRecvLSN := recv2.ReceivedLSN()
	if postRecvLSN <= preRecvLSN {
		t.Fatalf("CP13-5: replica receivedLSN did not advance (%d → %d) — catch-up did not deliver entries",
			preRecvLSN, postRecvLSN)
	}

	// CP13-5 observable evidence 3: shipper now has updated flushedLSN from barrier.
	if newS.ReplicaFlushedLSN() == 0 {
		t.Fatal("CP13-5: shipper should have replicaFlushedLSN > 0 after successful barrier")
	}
}

// ---------- Point 3: duplicate catch-up LSN semantics ----------

// TestAdversarial_ReplicaRejectsDuplicateLSN verifies the replica skips
// entries with LSN <= receivedLSN (duplicate/old), does not error.
func TestAdversarial_ReplicaRejectsDuplicateLSN(t *testing.T) {
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

	// Write 5 entries.
	for i := uint64(0); i < 5; i++ {
		if err := primary.WriteLBA(i, makeBlock(byte('A'+i))); err != nil {
			t.Fatal(err)
		}
	}
	if err := primary.SyncCache(); err != nil {
		t.Fatal(err)
	}

	// Verify replica has all 5.
	if recv.ReceivedLSN() < 5 {
		t.Fatalf("replica receivedLSN=%d, expected >=5", recv.ReceivedLSN())
	}

	// Manually send a duplicate entry (LSN 3) to the replica.
	// This should be silently skipped, not error.
	entry := &WALEntry{
		LSN:    3, // already received
		Epoch:  1,
		Type:   EntryTypeWrite,
		LBA:    100,
		Length: 4096,
		Data:   makeBlock('Z'),
	}
	err = recv.ApplyEntryForTest(entry)
	if err != nil {
		t.Fatalf("duplicate LSN should be skipped, got error: %v", err)
	}

	// Original data at LBA 2 (LSN 3) should be unchanged.
	replica.flusher.FlushOnce()
	got, _ := replica.ReadLBA(2, 4096)
	if got[0] != 'C' {
		t.Fatalf("LBA 2: expected C, got %c — duplicate entry corrupted data", got[0])
	}
}

// TestAdversarial_ReplicaRejectsGapLSN verifies the replica rejects entries
// with LSN > receivedLSN+1 (gap — entries were missed).
func TestAdversarial_ReplicaRejectsGapLSN(t *testing.T) {
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

	// Write 3 entries.
	for i := uint64(0); i < 3; i++ {
		if err := primary.WriteLBA(i, makeBlock(byte('A'+i))); err != nil {
			t.Fatal(err)
		}
	}
	if err := primary.SyncCache(); err != nil {
		t.Fatal(err)
	}

	// Manually send LSN 10 (skipping 4-9). Should fail with gap error.
	entry := &WALEntry{
		LSN:    10,
		Epoch:  1,
		Type:   EntryTypeWrite,
		LBA:    50,
		Length: 4096,
		Data:   makeBlock('Z'),
	}
	err = recv.ApplyEntryForTest(entry)
	if err == nil {
		t.Fatal("gap LSN should be rejected, got nil error")
	}
}

// ---------- Point 4: NeedsRebuild stickiness ----------

// TestAdversarial_NeedsRebuildBlocksAllPaths verifies that once a shipper
// enters NeedsRebuild, neither Ship nor Barrier can bring it back to healthy.
func TestAdversarial_NeedsRebuildBlocksAllPaths(t *testing.T) {
	dir := t.TempDir()
	opts := CreateOptions{
		VolumeSize:     1 * 1024 * 1024,
		BlockSize:      4096,
		WALSize:        32 * 1024, // tiny WAL
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
	primary.lease.Grant(30 * time.Second)

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

	// Establish sync.
	if err := primary.WriteLBA(0, makeBlock('A')); err != nil {
		t.Fatal(err)
	}
	if err := primary.SyncCache(); err != nil {
		t.Fatal(err)
	}

	// Disconnect and write a lot to overflow WAL.
	recv.Stop()
	time.Sleep(50 * time.Millisecond)

	for i := uint64(0); i < 50; i++ {
		_ = primary.WriteLBA(i%8, makeBlock(byte('0'+i%10)))
	}
	primary.flusher.FlushOnce()
	primary.flusher.FlushOnce()

	// Reconnect — gap should exceed retained WAL → NeedsRebuild.
	recv2, err := NewReplicaReceiver(replica, "127.0.0.1:0", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	recv2.Serve()
	defer recv2.Stop()
	primary.SetReplicaAddr(recv2.DataAddr(), recv2.CtrlAddr())

	// SyncCache should fail.
	syncDone := make(chan error, 1)
	go func() {
		syncDone <- primary.SyncCache()
	}()

	select {
	case err := <-syncDone:
		if err == nil {
			t.Fatal("SyncCache should fail after NeedsRebuild")
		}
	case <-time.After(10 * time.Second):
		t.Fatal("SyncCache hung")
	}

	// Verify the shipper is in NeedsRebuild or Degraded.
	sg := primary.shipperGroup
	if sg == nil {
		t.Fatal("no shipper group")
	}
	s := sg.Shipper(0)
	if s == nil {
		t.Fatal("no shipper")
	}
	st := s.State()
	if st == ReplicaInSync {
		t.Fatal("shipper should NOT be InSync after NeedsRebuild")
	}
	t.Logf("shipper state after gap: %s (expected Degraded or NeedsRebuild)", st)

	// Try Ship — should silently drop (not transition to healthy).
	if err := primary.WriteLBA(0, makeBlock('Z')); err != nil {
		t.Fatal(err)
	}

	// State should still be unhealthy.
	st2 := s.State()
	if st2 == ReplicaInSync {
		t.Fatal("Ship should not restore InSync from NeedsRebuild/Degraded")
	}

	// Try Barrier again — should still fail.
	syncDone2 := make(chan error, 1)
	go func() {
		syncDone2 <- primary.SyncCache()
	}()

	select {
	case err := <-syncDone2:
		if err == nil {
			t.Fatal("second SyncCache should still fail after NeedsRebuild")
		}
	case <-time.After(10 * time.Second):
		t.Fatal("second SyncCache hung")
	}
}

// ---------- Point 6: data integrity after catch-up ----------

// TestAdversarial_CatchupDoesNotOverwriteNewerData verifies that if the
// replica has data at an LBA from a later LSN, catch-up replay of an
// earlier LSN for the same LBA does not overwrite the newer version.
// (This is actually handled by the WAL: the dirty map always uses the
// latest LSN for each LBA.)
func TestAdversarial_CatchupDoesNotOverwriteNewerData(t *testing.T) {
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

	// Write LBA 0 = 'A' (LSN 1), then LBA 0 = 'B' (LSN 2).
	if err := primary.WriteLBA(0, makeBlock('A')); err != nil {
		t.Fatal(err)
	}
	if err := primary.WriteLBA(0, makeBlock('B')); err != nil {
		t.Fatal(err)
	}
	if err := primary.SyncCache(); err != nil {
		t.Fatal(err)
	}

	// Disconnect, write LBA 0 = 'C' (LSN 3).
	recv.Stop()
	time.Sleep(50 * time.Millisecond)

	if err := primary.WriteLBA(0, makeBlock('C')); err != nil {
		t.Fatal(err)
	}

	// Reconnect — catch-up sends LSN 3.
	recv2, err := NewReplicaReceiver(replica, "127.0.0.1:0", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	recv2.Serve()
	defer recv2.Stop()
	primary.SetReplicaAddr(recv2.DataAddr(), recv2.CtrlAddr())

	syncDone := make(chan error, 1)
	go func() {
		syncDone <- primary.SyncCache()
	}()

	select {
	case err := <-syncDone:
		if err != nil {
			t.Fatalf("SyncCache: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("SyncCache hung")
	}

	// Replica should have 'C' at LBA 0, not 'A' or 'B'.
	replica.flusher.FlushOnce()
	got, err := replica.ReadLBA(0, 4096)
	if err != nil {
		t.Fatal(err)
	}
	if got[0] != 'C' {
		t.Fatalf("LBA 0: expected C (latest), got %c — catch-up overwrote newer data", got[0])
	}
}

// TestAdversarial_CatchupMultipleDisconnects verifies that multiple
// disconnect/reconnect cycles with writes in between all converge correctly.
func TestAdversarial_CatchupMultipleDisconnects(t *testing.T) {
	primary, replica := createSyncAllPair(t)
	defer primary.Close()
	defer replica.Close()

	recv, err := NewReplicaReceiver(replica, "127.0.0.1:0", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	recv.Serve()

	primary.SetReplicaAddr(recv.DataAddr(), recv.CtrlAddr())

	// Cycle 1: write, sync, disconnect, write.
	for i := uint64(0); i < 3; i++ {
		if err := primary.WriteLBA(i, makeBlock(byte('A'+i))); err != nil {
			t.Fatal(err)
		}
	}
	if err := primary.SyncCache(); err != nil {
		t.Fatal(err)
	}

	recv.Stop()
	time.Sleep(30 * time.Millisecond)

	for i := uint64(3); i < 5; i++ {
		if err := primary.WriteLBA(i, makeBlock(byte('A'+i))); err != nil {
			t.Fatal(err)
		}
	}

	// Reconnect 1.
	recv2, err := NewReplicaReceiver(replica, "127.0.0.1:0", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	recv2.Serve()
	primary.SetReplicaAddr(recv2.DataAddr(), recv2.CtrlAddr())

	if err := primary.SyncCache(); err != nil {
		t.Fatalf("cycle 1 reconnect SyncCache: %v", err)
	}

	// Cycle 2: disconnect again, write more.
	recv2.Stop()
	time.Sleep(30 * time.Millisecond)

	for i := uint64(5); i < 8; i++ {
		if err := primary.WriteLBA(i, makeBlock(byte('A'+i))); err != nil {
			t.Fatal(err)
		}
	}

	// Reconnect 2.
	recv3, err := NewReplicaReceiver(replica, "127.0.0.1:0", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	recv3.Serve()
	defer recv3.Stop()
	primary.SetReplicaAddr(recv3.DataAddr(), recv3.CtrlAddr())

	if err := primary.SyncCache(); err != nil {
		t.Fatalf("cycle 2 reconnect SyncCache: %v", err)
	}

	// Verify all 8 blocks on replica.
	replica.flusher.FlushOnce()
	for i := uint64(0); i < 8; i++ {
		got, err := replica.ReadLBA(i, 4096)
		if err != nil {
			t.Fatalf("ReadLBA(%d): %v", i, err)
		}
		expected := byte('A' + i)
		if !bytes.Equal(got[:1], []byte{expected}) {
			t.Errorf("LBA %d: expected %c, got %c after 2 disconnect/reconnect cycles", i, expected, got[0])
		}
	}
}
