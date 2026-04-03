package blockvol

// Tests for BUG-SYNC-ALL-FLUSH: three chained bugs that break sync_all mode.
// These tests are designed to FAIL on the current code and PASS after the fix.
//
// Bug 3: Replica addresses are :port not ip:port — cross-machine never connects.
// Bug 2: Reconnected shipper has LSN gap — replica rejects all entries.
// Bug 1: Shipper degrades permanently — no recovery path.

import (
	"bytes"
	"fmt"
	"net"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// --- Bug 3: Address resolution ---

// TestBug3_ReplicaAddr_MustBeIPPort_WildcardBind verifies that when
// ReplicaReceiver binds to ":0" (wildcard — the production default),
// DataAddr()/CtrlAddr() return canonical ip:port, not ":port" or wildcard.
//
// CP13-2 proof: this is a real proof, not witness coverage.
// The test verifies both the fallback path (no advertisedHost) and the
// explicit advertisedHost path (production: host extracted from serverID).
func TestBug3_ReplicaAddr_MustBeIPPort_WildcardBind(t *testing.T) {
	vol := createTestVol(t)
	defer vol.Close()

	assertCanonical := func(t *testing.T, name, addr string) {
		t.Helper()
		if strings.HasPrefix(addr, ":") {
			t.Fatalf("%s returned %q — missing IP", name, addr)
		}
		if strings.HasPrefix(addr, "0.0.0.0:") {
			t.Fatalf("%s returned %q — wildcard, not routable cross-machine", name, addr)
		}
		if strings.HasPrefix(addr, "[::]:") {
			t.Fatalf("%s returned %q — IPv6 wildcard, not routable cross-machine", name, addr)
		}
		host, port, err := net.SplitHostPort(addr)
		if err != nil {
			t.Fatalf("%s returned %q — not valid host:port: %v", name, addr, err)
		}
		if host == "" {
			t.Fatalf("%s returned %q — empty host", name, addr)
		}
		if port == "" || port == "0" {
			t.Fatalf("%s returned %q — invalid port", name, addr)
		}
	}

	// Case 1: wildcard bind with no advertisedHost (fallback to outbound IP).
	t.Run("fallback", func(t *testing.T) {
		recv, err := NewReplicaReceiver(vol, ":0", ":0")
		if err != nil {
			t.Fatal(err)
		}
		defer recv.Stop()
		assertCanonical(t, "DataAddr", recv.DataAddr())
		assertCanonical(t, "CtrlAddr", recv.CtrlAddr())
	})

	// Case 2: wildcard bind with explicit advertisedHost (production path).
	// CP13-2: this is the authoritative path — serverID host is forwarded
	// through StartReplicaReceiver → NewReplicaReceiver → canonicalizeListenerAddr.
	t.Run("advertisedHost", func(t *testing.T) {
		recv, err := NewReplicaReceiver(vol, ":0", ":0", "10.0.0.42")
		if err != nil {
			t.Fatal(err)
		}
		defer recv.Stop()
		assertCanonical(t, "DataAddr", recv.DataAddr())
		assertCanonical(t, "CtrlAddr", recv.CtrlAddr())
		// The advertised host must appear in the exported address.
		if !strings.HasPrefix(recv.DataAddr(), "10.0.0.42:") {
			t.Fatalf("DataAddr %q does not use advertisedHost 10.0.0.42", recv.DataAddr())
		}
		if !strings.HasPrefix(recv.CtrlAddr(), "10.0.0.42:") {
			t.Fatalf("CtrlAddr %q does not use advertisedHost 10.0.0.42", recv.CtrlAddr())
		}
	})

	// Case 3: StartReplicaReceiver (the public API) with advertisedHost.
	t.Run("StartReplicaReceiver_API", func(t *testing.T) {
		vol2 := createTestVol(t)
		defer vol2.Close()
		if err := vol2.StartReplicaReceiver(":0", ":0", "10.0.0.99"); err != nil {
			t.Fatal(err)
		}
		info := vol2.ReplicaReceiverAddr()
		if info == nil {
			t.Fatal("ReplicaReceiverAddr() returned nil")
		}
		assertCanonical(t, "DataAddr", info.DataAddr)
		assertCanonical(t, "CtrlAddr", info.CtrlAddr)
		if !strings.HasPrefix(info.DataAddr, "10.0.0.99:") {
			t.Fatalf("DataAddr %q does not use advertisedHost 10.0.0.99", info.DataAddr)
		}
		if !strings.HasPrefix(info.CtrlAddr, "10.0.0.99:") {
			t.Fatalf("CtrlAddr %q does not use advertisedHost 10.0.0.99", info.CtrlAddr)
		}
	})
}

// --- Bug 2: LSN gap after shipper degradation ---

// TestBug2_SyncAll_SyncCache_AfterDegradedShipperRecovers verifies the
// full round-trip: primary writes during degraded period → shipper reconnects
// → SyncCache (barrier) must succeed after catch-up, not fail permanently.
//
// This is the core bug: during degraded period, Ship() silently drops entries.
// After reconnection, the replica has a gap. Barrier hangs or fails because
// the replica never received the missing entries.
func TestBug2_SyncAll_SyncCache_AfterDegradedShipperRecovers(t *testing.T) {
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

	// Phase 1: Write + SyncCache while healthy. Must succeed.
	if err := primary.WriteLBA(0, makeBlock('A')); err != nil {
		t.Fatalf("write 1: %v", err)
	}
	if err := primary.SyncCache(); err != nil {
		t.Fatalf("SyncCache 1 (healthy): %v", err)
	}

	// Phase 2: Kill the replica's data connection to force shipper degradation.
	// Close the receiver, wait for shipper to detect failure.
	recv.Stop()
	time.Sleep(50 * time.Millisecond)

	// Write while degraded. Ship() silently drops these entries.
	if err := primary.WriteLBA(1, makeBlock('B')); err != nil {
		t.Fatalf("write 2 (degraded): %v", err)
	}
	if err := primary.WriteLBA(2, makeBlock('C')); err != nil {
		t.Fatalf("write 3 (degraded): %v", err)
	}

	// Phase 3: Restart the replica receiver on the SAME addresses.
	// We must NOT call SetReplicaAddr again — that creates a fresh shipper
	// and loses the flushed progress needed for reconnect handshake.
	savedDataAddr := recv.DataAddr()
	savedCtrlAddr := recv.CtrlAddr()
	recv2, err := NewReplicaReceiver(replica, savedDataAddr, savedCtrlAddr)
	if err != nil {
		// Address reuse failed (port still held) — use new ports and reconfigure.
		// This loses shipper state, so initialize the new receiver's receivedLSN.
		recv2, err = NewReplicaReceiver(replica, "127.0.0.1:0", "127.0.0.1:0")
		if err != nil {
			t.Fatalf("restart receiver: %v", err)
		}
		primary.SetReplicaAddr(recv2.DataAddr(), recv2.CtrlAddr())
	}
	recv2.Serve()
	defer recv2.Stop()

	// Phase 4: SyncCache after recovery. This is the critical test:
	// The shipper must catch up the replica on the missing LSNs before
	// the barrier can succeed. Without catch-up, the barrier hangs/fails.
	syncDone := make(chan error, 1)
	go func() {
		syncDone <- primary.SyncCache()
	}()

	select {
	case err := <-syncDone:
		if err != nil {
			t.Fatalf("SyncCache after recovery failed: %v — shipper did not catch up replica", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("SyncCache after recovery hung — shipper has no catch-up protocol")
	}

	// Phase 5: Verify replica has ALL the data.
	replica.flusher.FlushOnce()
	for lba := uint64(0); lba < 3; lba++ {
		got, err := replica.ReadLBA(lba, 4096)
		if err != nil {
			t.Fatalf("replica ReadLBA(%d): %v", lba, err)
		}
		expected := byte('A' + lba)
		if got[0] != expected {
			t.Fatalf("replica LBA %d: expected %c, got %c", lba, expected, got[0])
		}
	}
}

// --- Bug 1: Permanent degradation ---

// TestBug1_SyncAll_WriteDuringDegraded_SyncCacheMustFail verifies that
// under sync_all mode, SyncCache returns an error (not success) when the
// shipper is degraded and has not caught up. Writes may succeed locally,
// but durability confirmation must fail.
func TestBug1_SyncAll_WriteDuringDegraded_SyncCacheMustFail(t *testing.T) {
	primary, replica := createSyncAllPair(t)
	defer primary.Close()
	defer replica.Close()

	recv, err := NewReplicaReceiver(replica, "127.0.0.1:0", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	recv.Serve()

	primary.SetReplicaAddr(recv.DataAddr(), recv.CtrlAddr())

	// Initial healthy write.
	if err := primary.WriteLBA(0, makeBlock('A')); err != nil {
		t.Fatalf("write: %v", err)
	}
	if err := primary.SyncCache(); err != nil {
		t.Fatalf("SyncCache healthy: %v", err)
	}

	// Kill replica — force degradation.
	recv.Stop()
	time.Sleep(50 * time.Millisecond)

	// Write during degraded period.
	if err := primary.WriteLBA(1, makeBlock('B')); err != nil {
		t.Fatalf("write during degraded: %v", err)
	}

	// SyncCache under sync_all with degraded replica MUST return error.
	// The write is locally durable but the replica barrier fails.
	syncDone := make(chan error, 1)
	go func() {
		syncDone <- primary.SyncCache()
	}()

	select {
	case err := <-syncDone:
		if err == nil {
			t.Fatal("SyncCache returned nil under sync_all with degraded replica — durability violation")
		}
		// Expected: ErrDurabilityBarrierFailed or similar.
		t.Logf("SyncCache correctly failed: %v", err)
	case <-time.After(10 * time.Second):
		t.Fatal("SyncCache hung — barrier timeout not propagated")
	}
}

// --- Full chain: sync_all write + SyncCache round-trip ---

// TestSyncAll_FullRoundTrip_WriteAndFlush verifies the complete sync_all
// contract: write → ship → barrier → SyncCache returns nil only when
// replica confirms durability.
func TestSyncAll_FullRoundTrip_WriteAndFlush(t *testing.T) {
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

	// Write 10 blocks.
	for i := 0; i < 10; i++ {
		if err := primary.WriteLBA(uint64(i), makeBlock(byte('0'+i))); err != nil {
			t.Fatalf("write %d: %v", i, err)
		}
	}

	// SyncCache = barrier. Under sync_all, this must confirm replica durability.
	if err := primary.SyncCache(); err != nil {
		t.Fatalf("SyncCache: %v", err)
	}

	// Replica must have all 10 entries.
	if recv.ReceivedLSN() < 10 {
		t.Fatalf("replica receivedLSN=%d, expected >=10", recv.ReceivedLSN())
	}

	// Read back from replica to verify data integrity.
	replica.flusher.FlushOnce()
	for i := 0; i < 10; i++ {
		got, err := replica.ReadLBA(uint64(i), 4096)
		if err != nil {
			t.Fatalf("replica ReadLBA(%d): %v", i, err)
		}
		if got[0] != byte('0'+i) {
			t.Fatalf("replica LBA %d: expected %c, got %c", i, '0'+i, got[0])
		}
	}
}

// TestSyncAll_MultipleFlush_NoWritesBetween verifies that repeated
// SyncCache calls without new writes succeed (FLUSH without data).
// This is the mkfs pattern: write blocks → FLUSH → write superblock → FLUSH.
func TestSyncAll_MultipleFlush_NoWritesBetween(t *testing.T) {
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

	// Write + flush.
	if err := primary.WriteLBA(0, makeBlock('X')); err != nil {
		t.Fatalf("write: %v", err)
	}
	if err := primary.SyncCache(); err != nil {
		t.Fatalf("SyncCache 1: %v", err)
	}

	// Flush again without new writes — must succeed, not hang.
	syncDone := make(chan error, 1)
	go func() {
		syncDone <- primary.SyncCache()
	}()
	select {
	case err := <-syncDone:
		if err != nil {
			t.Fatalf("SyncCache 2 (no new writes): %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("SyncCache 2 hung — barrier on stale lsnMax not handled")
	}

	// Third flush.
	if err := primary.SyncCache(); err != nil {
		t.Fatalf("SyncCache 3: %v", err)
	}
}

// --- Helpers ---

func createSyncAllPair(t *testing.T) (primary *BlockVol, replica *BlockVol) {
	t.Helper()
	pDir := t.TempDir()
	rDir := t.TempDir()
	opts := CreateOptions{
		VolumeSize:     1 * 1024 * 1024,
		BlockSize:      4096,
		WALSize:        256 * 1024,
		DurabilityMode: DurabilitySyncAll,
	}
	p, err := CreateBlockVol(filepath.Join(pDir, "primary.blockvol"), opts)
	if err != nil {
		t.Fatalf("CreateBlockVol primary: %v", err)
	}
	p.SetRole(RolePrimary)
	p.SetEpoch(1)
	p.SetMasterEpoch(1)
	p.lease.Grant(30 * time.Second)

	r, err := CreateBlockVol(filepath.Join(rDir, "replica.blockvol"), opts)
	if err != nil {
		p.Close()
		t.Fatalf("CreateBlockVol replica: %v", err)
	}
	r.SetRole(RoleReplica)
	r.SetEpoch(1)
	r.SetMasterEpoch(1)

	return p, r
}

// Suppress unused import.
var _ = fmt.Sprintf
var _ = bytes.Equal
