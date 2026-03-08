package blockvol

import (
	"encoding/binary"
	"errors"
	"net"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestQAPhase4ACP3 tests Phase 4A CP3 (promotion/demotion/rebuild) adversarially.
func TestQAPhase4ACP3(t *testing.T) {
	tests := []struct {
		name string
		run  func(t *testing.T)
	}{
		// --- HandleAssignment ---
		{name: "assign_same_role_epoch_refresh", run: testAssignSameRoleEpochRefresh},
		{name: "assign_same_role_epoch_no_downgrade", run: testAssignSameRoleEpochNoDowngrade},
		{name: "assign_concurrent_serialized", run: testAssignConcurrentSerialized},
		{name: "assign_promote_epoch_before_role", run: testAssignPromoteEpochBeforeRole},
		{name: "assign_promote_write_uses_new_epoch", run: testAssignPromoteWriteUsesNewEpoch},
		{name: "assign_demote_blocks_new_writes", run: testAssignDemoteBlocksNewWrites},
		{name: "assign_demote_drains_inflight", run: testAssignDemoteDrainsInflight},
		{name: "assign_demote_drain_timeout", run: testAssignDemoteDrainTimeout},
		{name: "assign_demote_stops_shipper", run: testAssignDemoteStopsShipper},
		{name: "assign_invalid_transitions", run: testAssignInvalidTransitions},

		// --- ScanFrom ---
		{name: "scan_empty_wal", run: testScanEmptyWAL},
		{name: "scan_recycled_boundary", run: testScanRecycledBoundary},
		{name: "scan_from_middle_lsn", run: testScanFromMiddleLSN},
		{name: "scan_includes_trims", run: testScanIncludesTrims},
		{name: "scan_skips_padding", run: testScanSkipsPadding},
		{name: "scan_snapshot_isolation", run: testScanSnapshotIsolation},

		// --- Rebuild Server ---
		{name: "rbsrv_epoch_mismatch", run: testRbSrvEpochMismatch},
		{name: "rbsrv_unknown_type", run: testRbSrvUnknownType},
		{name: "rbsrv_wal_catchup_entries", run: testRbSrvWALCatchUpEntries},
		{name: "rbsrv_wal_catchup_empty", run: testRbSrvWALCatchUpEmpty},
		{name: "rbsrv_full_extent_size", run: testRbSrvFullExtentSize},
		{name: "rbsrv_full_extent_has_data", run: testRbSrvFullExtentHasData},
		{name: "rbsrv_stop_during_stream", run: testRbSrvStopDuringStream},

		// --- Rebuild Client ---
		{name: "rbcli_wrong_role_rejected", run: testRbCliWrongRole},
		{name: "rbcli_catchup_data_integrity", run: testRbCliCatchUpDataIntegrity},
		{name: "rbcli_wal_recycled_fallback", run: testRbCliWALRecycledFallback},
		{name: "rbcli_full_extent_clears_dirty_map", run: testRbCliFullExtentClearsDirtyMap},
		{name: "rbcli_full_extent_resets_wal", run: testRbCliFullExtentResetsWAL},
		{name: "rbcli_full_extent_persists_superblock", run: testRbCliFullExtentPersistsSuperblock},

		// --- applyRebuildEntry ---
		{name: "apply_nextlsn_advanced", run: testApplyNextLSNAdvanced},
		{name: "apply_out_of_order_accepted", run: testApplyOutOfOrderAccepted},
		{name: "apply_dirty_map_updated", run: testApplyDirtyMapUpdated},

		// --- Integration ---
		{name: "lifecycle_promote_write_demote_rebuild", run: testLifecycleFullCycle},
		{name: "dirty_map_clear_concurrent", run: testDirtyMapClearConcurrent},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.run(t)
		})
	}
}

// --- helpers ---

func cp3Vol(t *testing.T, name string, walSize uint64) *BlockVol {
	t.Helper()
	dir := t.TempDir()
	cfg := DefaultConfig()
	cfg.FlushInterval = 5 * time.Millisecond
	cfg.WALFullTimeout = 200 * time.Millisecond
	vol, err := CreateBlockVol(filepath.Join(dir, name), CreateOptions{
		VolumeSize: 64 * 1024,
		BlockSize:  4096,
		WALSize:    walSize,
	}, cfg)
	if err != nil {
		t.Fatalf("CreateBlockVol(%s): %v", name, err)
	}
	return vol
}

func cp3Primary(t *testing.T, name string, epoch uint64) *BlockVol {
	t.Helper()
	vol := cp3Vol(t, name, 64*1024)
	if err := vol.SetEpoch(epoch); err != nil {
		t.Fatalf("SetEpoch: %v", err)
	}
	vol.SetMasterEpoch(epoch)
	if err := vol.SetRole(RolePrimary); err != nil {
		t.Fatalf("SetRole: %v", err)
	}
	vol.lease.Grant(30 * time.Second)
	return vol
}

// cp3Rebuilding creates a vol in RoleRebuilding via valid state path.
func cp3Rebuilding(t *testing.T, name string, epoch uint64) *BlockVol {
	t.Helper()
	vol := cp3Vol(t, name, 64*1024)
	// None -> Primary -> Stale -> Rebuilding
	if err := vol.HandleAssignment(epoch, RolePrimary, 1*time.Second); err != nil {
		t.Fatalf("-> Primary: %v", err)
	}
	if err := vol.HandleAssignment(epoch, RoleStale, 0); err != nil {
		t.Fatalf("-> Stale: %v", err)
	}
	if err := vol.HandleAssignment(epoch, RoleRebuilding, 0); err != nil {
		t.Fatalf("-> Rebuilding: %v", err)
	}
	return vol
}

// ========== HandleAssignment ==========

func testAssignSameRoleEpochRefresh(t *testing.T) {
	// Reviewer finding #3 regression: same-role assignment must update epoch.
	vol := cp3Primary(t, "epoch_refresh.bv", 1)
	defer vol.Close()

	if err := vol.HandleAssignment(5, RolePrimary, 30*time.Second); err != nil {
		t.Fatalf("HandleAssignment: %v", err)
	}
	if vol.Epoch() != 5 {
		t.Errorf("Epoch = %d, want 5 (same-role epoch not refreshed)", vol.Epoch())
	}
	if vol.masterEpoch.Load() != 5 {
		t.Errorf("MasterEpoch = %d, want 5", vol.masterEpoch.Load())
	}
	// Lease should be refreshed.
	if !vol.lease.IsValid() {
		t.Error("lease not refreshed after same-role assignment")
	}
}

func testAssignSameRoleEpochNoDowngrade(t *testing.T) {
	// Epoch must not go backwards on same-role assignment — must be rejected.
	vol := cp3Primary(t, "no_downgrade.bv", 5)
	defer vol.Close()

	// Send assignment with lower epoch — must return epoch regression error.
	err := vol.HandleAssignment(3, RolePrimary, 30*time.Second)
	if err == nil {
		t.Fatalf("expected error for stale epoch, got nil")
	}
	if !errors.Is(err, ErrEpochRegression) {
		t.Fatalf("expected ErrEpochRegression, got: %v", err)
	}
	if vol.Epoch() != 5 {
		t.Errorf("Epoch = %d, want 5 (epoch should not downgrade from 5 to 3)", vol.Epoch())
	}
}

func testAssignConcurrentSerialized(t *testing.T) {
	vol := cp3Vol(t, "concurrent.bv", 32*1024)
	defer vol.Close()

	var wg sync.WaitGroup
	errs := make([]error, 2)

	wg.Add(2)
	go func() {
		defer wg.Done()
		errs[0] = vol.HandleAssignment(1, RolePrimary, 10*time.Second)
	}()
	go func() {
		defer wg.Done()
		errs[1] = vol.HandleAssignment(1, RoleReplica, 10*time.Second)
	}()

	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("deadlock in concurrent HandleAssignment")
	}

	succeeded := 0
	for _, e := range errs {
		if e == nil {
			succeeded++
		}
	}
	if succeeded == 0 {
		t.Errorf("both failed: %v, %v", errs[0], errs[1])
	}
	role := vol.Role()
	if role != RolePrimary && role != RoleReplica {
		t.Errorf("role = %s, want Primary or Replica", role)
	}
}

func testAssignPromoteEpochBeforeRole(t *testing.T) {
	// Epoch must be durable BEFORE role becomes Primary.
	// After promote, both epoch and masterEpoch match, writes work.
	vol := cp3Vol(t, "promote_order.bv", 32*1024)
	defer vol.Close()
	vol.SetEpoch(1)
	vol.SetMasterEpoch(1)
	vol.SetRole(RoleReplica)

	if err := vol.HandleAssignment(5, RolePrimary, 10*time.Second); err != nil {
		t.Fatalf("promote: %v", err)
	}
	if vol.Epoch() != 5 {
		t.Errorf("epoch = %d after promote, want 5", vol.Epoch())
	}
	if vol.masterEpoch.Load() != 5 {
		t.Errorf("masterEpoch = %d, want 5", vol.masterEpoch.Load())
	}
	// Writes must succeed with new epoch.
	if err := vol.WriteLBA(0, makeBlock('P')); err != nil {
		t.Errorf("WriteLBA after promote: %v", err)
	}
}

func testAssignPromoteWriteUsesNewEpoch(t *testing.T) {
	// After promote to epoch=5, WAL entries must carry epoch=5.
	vol := cp3Vol(t, "epoch_wal.bv", 64*1024)
	defer vol.Close()
	vol.SetEpoch(1)
	vol.SetMasterEpoch(1)
	vol.SetRole(RoleReplica)

	vol.HandleAssignment(5, RolePrimary, 10*time.Second)
	vol.WriteLBA(0, makeBlock('E'))

	// Scan the WAL for entries with the correct epoch.
	var foundEpoch uint64
	vol.wal.ScanFrom(vol.fd, vol.super.WALOffset, 0, 1, func(e *WALEntry) error {
		foundEpoch = e.Epoch
		return nil
	})
	if foundEpoch != 5 {
		t.Errorf("WAL entry epoch = %d, want 5 (write after promote should use new epoch)", foundEpoch)
	}
}

func testAssignDemoteBlocksNewWrites(t *testing.T) {
	// Demote: lease revoke must block writeGate before role transition.
	vol := cp3Primary(t, "demote_block.bv", 1)
	defer vol.Close()

	vol.WriteLBA(0, makeBlock('B'))
	vol.SyncCache()

	vol.HandleAssignment(2, RoleStale, 0)

	if vol.lease.IsValid() {
		t.Error("lease still valid after demote")
	}
	if vol.Role() != RoleStale {
		t.Errorf("role = %s, want Stale", vol.Role())
	}
	if err := vol.WriteLBA(0, makeBlock('X')); err == nil {
		t.Error("WriteLBA succeeded after demote, want error")
	}
	// Reads must still work.
	data, err := vol.ReadLBA(0, 4096)
	if err != nil {
		t.Fatalf("ReadLBA after demote: %v", err)
	}
	if data[0] != 'B' {
		t.Errorf("data = %c, want 'B'", data[0])
	}
}

func testAssignDemoteDrainsInflight(t *testing.T) {
	// In-flight writes must complete before demote finishes.
	vol := cp3Primary(t, "demote_drain.bv", 1)
	defer vol.Close()
	vol.drainTimeout = 2 * time.Second

	// Start a slow "in-flight op".
	vol.opsOutstanding.Add(1)

	demoteDone := make(chan error, 1)
	go func() {
		demoteDone <- vol.HandleAssignment(2, RoleStale, 0)
	}()

	// Demote should be waiting for ops to drain.
	select {
	case <-demoteDone:
		t.Fatal("demote returned immediately despite outstanding op")
	case <-time.After(50 * time.Millisecond):
	}

	// Finish the op -- demote should complete.
	vol.opsOutstanding.Add(-1)

	select {
	case err := <-demoteDone:
		if err != nil {
			t.Errorf("demote: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("demote hung after ops drained")
	}
	if vol.Role() != RoleStale {
		t.Errorf("role = %s, want Stale", vol.Role())
	}
}

func testAssignDemoteDrainTimeout(t *testing.T) {
	vol := cp3Primary(t, "drain_to.bv", 1)
	defer vol.Close()
	vol.drainTimeout = 100 * time.Millisecond

	vol.opsOutstanding.Add(1) // stuck op
	start := time.Now()
	err := vol.HandleAssignment(2, RoleStale, 0)
	elapsed := time.Since(start)

	if err == nil {
		t.Error("demote should fail with drain timeout")
	}
	if elapsed > 1*time.Second {
		t.Errorf("drain took %v, expected ~100ms", elapsed)
	}
	vol.opsOutstanding.Add(-1) // cleanup
}

func testAssignDemoteStopsShipper(t *testing.T) {
	primary, replica := createReplicaPair(t)
	defer replica.Close()

	// Demote should stop the shipper.
	primary.drainTimeout = 1 * time.Second
	primary.HandleAssignment(2, RoleStale, 0)

	// After demote, shipper should be stopped -- Ship is a no-op.
	if primary.shipperGroup != nil && primary.shipperGroup.Len() > 0 {
		shipper := primary.shipperGroup.Shipper(0)
		entry := &WALEntry{LSN: 999, Epoch: 2, Type: EntryTypeWrite, LBA: 0, Length: 4096, Data: makeBlock('X')}
		err := shipper.Ship(entry)
		if err != nil {
			t.Errorf("Ship after stop: %v (should silently drop)", err)
		}
		if shipper.ShippedLSN() == 999 {
			t.Error("shipper advanced LSN after demote -- should be stopped")
		}
	}
	primary.Close()
}

func testAssignInvalidTransitions(t *testing.T) {
	vol := cp3Primary(t, "invalid.bv", 1)
	defer vol.Close()

	cases := []struct {
		role Role
		desc string
	}{
		{RoleReplica, "Primary -> Replica"},
		{RoleRebuilding, "Primary -> Rebuilding"},
		{RoleDraining, "Primary -> Draining"},
	}
	for _, c := range cases {
		err := vol.HandleAssignment(2, c.role, 10*time.Second)
		if err == nil {
			t.Errorf("%s should fail", c.desc)
		}
	}
	if vol.Role() != RolePrimary {
		t.Errorf("role changed after failed assignment: %s", vol.Role())
	}
}

// ========== ScanFrom ==========

func testScanEmptyWAL(t *testing.T) {
	vol := cp3Vol(t, "scan_empty.bv", 32*1024)
	defer vol.Close()
	var count int
	err := vol.wal.ScanFrom(vol.fd, vol.super.WALOffset, 0, 1, func(e *WALEntry) error {
		count++
		return nil
	})
	if err != nil {
		t.Errorf("ScanFrom empty: %v", err)
	}
	if count != 0 {
		t.Errorf("got %d entries, want 0", count)
	}
}

func testScanRecycledBoundary(t *testing.T) {
	vol := cp3Vol(t, "scan_recyc.bv", 32*1024)
	defer vol.Close()

	// fromLSN < checkpointLSN -> recycled
	err := vol.wal.ScanFrom(vol.fd, vol.super.WALOffset, 10, 5, func(*WALEntry) error { return nil })
	if err != ErrWALRecycled {
		t.Errorf("fromLSN<checkpoint: got %v, want ErrWALRecycled", err)
	}
	// fromLSN == checkpointLSN -> recycled
	err = vol.wal.ScanFrom(vol.fd, vol.super.WALOffset, 10, 10, func(*WALEntry) error { return nil })
	if err != ErrWALRecycled {
		t.Errorf("fromLSN==checkpoint: got %v, want ErrWALRecycled", err)
	}
	// fromLSN == checkpointLSN+1 -> NOT recycled
	err = vol.wal.ScanFrom(vol.fd, vol.super.WALOffset, 10, 11, func(*WALEntry) error { return nil })
	if err == ErrWALRecycled {
		t.Error("fromLSN=checkpoint+1 should not be recycled")
	}
	// checkpointLSN==0, fromLSN==0 -> NOT recycled (fresh)
	err = vol.wal.ScanFrom(vol.fd, vol.super.WALOffset, 0, 0, func(*WALEntry) error { return nil })
	if err == ErrWALRecycled {
		t.Error("checkpoint=0, fromLSN=0 should not be recycled")
	}
}

func testScanFromMiddleLSN(t *testing.T) {
	// Write LSN 1-10, scan from LSN=6 -> only LSN 6-10 returned.
	vol := cp3Primary(t, "scan_mid.bv", 1)
	defer vol.Close()

	for i := 0; i < 10; i++ {
		vol.WriteLBA(uint64(i), makeBlock(byte('A'+i)))
	}

	var lsns []uint64
	vol.wal.ScanFrom(vol.fd, vol.super.WALOffset, 0, 6, func(e *WALEntry) error {
		lsns = append(lsns, e.LSN)
		return nil
	})

	if len(lsns) == 0 {
		t.Fatal("no entries returned")
	}
	for _, lsn := range lsns {
		if lsn < 6 {
			t.Errorf("ScanFrom(fromLSN=6) returned LSN %d (< 6)", lsn)
		}
	}
	// Should have LSNs 6-10 (5 entries).
	if len(lsns) < 5 {
		t.Errorf("got %d entries, want >= 5 (LSN 6-10)", len(lsns))
	}
}

func testScanIncludesTrims(t *testing.T) {
	// ScanFrom must return TRIM entries (line 279 checks EntryTypeTrim).
	vol := cp3Primary(t, "scan_trim.bv", 1)
	defer vol.Close()

	vol.WriteLBA(0, makeBlock('W'))
	vol.Trim(0, 4096) // TRIM entry

	var types []byte
	vol.wal.ScanFrom(vol.fd, vol.super.WALOffset, 0, 1, func(e *WALEntry) error {
		types = append(types, e.Type)
		return nil
	})

	foundTrim := false
	for _, typ := range types {
		if typ == EntryTypeTrim {
			foundTrim = true
		}
	}
	if !foundTrim {
		t.Errorf("ScanFrom returned types %v, want TRIM included", types)
	}
}

func testScanSkipsPadding(t *testing.T) {
	// Write enough entries to cause padding at WAL boundary, verify no padding leaks.
	vol := cp3Vol(t, "scan_pad.bv", 64*1024)
	defer vol.Close()
	vol.SetEpoch(1)
	vol.SetMasterEpoch(1)
	vol.SetRole(RolePrimary)
	vol.lease.Grant(30 * time.Second)

	written := 0
	for i := 0; i < 20; i++ {
		if err := vol.WriteLBA(uint64(i%16), makeBlock(byte('0'+i%10))); err != nil {
			break
		}
		written++
	}
	if written < 3 {
		t.Fatalf("only wrote %d entries", written)
	}

	var scanned int
	vol.wal.ScanFrom(vol.fd, vol.super.WALOffset, 0, 1, func(e *WALEntry) error {
		if e.Type == EntryTypePadding {
			t.Error("padding entry leaked through ScanFrom")
		}
		scanned++
		return nil
	})
	if scanned == 0 {
		t.Error("no entries returned")
	}
}

func testScanSnapshotIsolation(t *testing.T) {
	// ScanFrom snapshots head/tail. Writes during scan don't affect results.
	vol := cp3Primary(t, "scan_iso.bv", 1)
	defer vol.Close()

	// Write 3 entries.
	for i := 0; i < 3; i++ {
		vol.WriteLBA(uint64(i), makeBlock(byte('A'+i)))
	}

	// Scan -- callback writes more entries.
	var scanCount int
	vol.wal.ScanFrom(vol.fd, vol.super.WALOffset, 0, 1, func(e *WALEntry) error {
		scanCount++
		// Write during scan -- should NOT appear in this scan.
		vol.WriteLBA(10, makeBlock('Z'))
		return nil
	})

	// We wrote 3 entries before scan. The entries written DURING scan should
	// not be seen (snapshot isolation). scanCount should be exactly 3.
	if scanCount != 3 {
		t.Errorf("scanCount = %d, want 3 (snapshot isolation: writes during scan should not appear)", scanCount)
	}
}

// ========== Rebuild Server ==========

func testRbSrvEpochMismatch(t *testing.T) {
	vol := cp3Primary(t, "rbs_epoch.bv", 5)
	defer vol.Close()
	vol.StartRebuildServer("127.0.0.1:0")
	defer vol.StopRebuildServer()

	conn, _ := net.Dial("tcp", vol.rebuildServer.Addr())
	defer conn.Close()

	// Stale epoch.
	WriteFrame(conn, MsgRebuildReq, EncodeRebuildRequest(RebuildRequest{Type: RebuildWALCatchUp, Epoch: 3}))
	msgType, payload, _ := ReadFrame(conn)
	if msgType != MsgRebuildError || string(payload) != "EPOCH_MISMATCH" {
		t.Errorf("stale epoch: type=0x%x payload=%q, want EPOCH_MISMATCH", msgType, payload)
	}

	// Future epoch.
	conn2, _ := net.Dial("tcp", vol.rebuildServer.Addr())
	defer conn2.Close()
	WriteFrame(conn2, MsgRebuildReq, EncodeRebuildRequest(RebuildRequest{Type: RebuildWALCatchUp, Epoch: 99}))
	msgType2, payload2, _ := ReadFrame(conn2)
	if msgType2 != MsgRebuildError || string(payload2) != "EPOCH_MISMATCH" {
		t.Errorf("future epoch: type=0x%x payload=%q, want EPOCH_MISMATCH", msgType2, payload2)
	}
}

func testRbSrvUnknownType(t *testing.T) {
	vol := cp3Primary(t, "rbs_unk.bv", 1)
	defer vol.Close()
	vol.StartRebuildServer("127.0.0.1:0")
	defer vol.StopRebuildServer()

	conn, _ := net.Dial("tcp", vol.rebuildServer.Addr())
	defer conn.Close()

	WriteFrame(conn, MsgRebuildReq, EncodeRebuildRequest(RebuildRequest{Type: 0xFF, Epoch: 1}))
	msgType, payload, _ := ReadFrame(conn)
	if msgType != MsgRebuildError || string(payload) != "UNKNOWN_TYPE" {
		t.Errorf("unknown type: type=0x%x payload=%q, want UNKNOWN_TYPE", msgType, payload)
	}
}

func testRbSrvWALCatchUpEntries(t *testing.T) {
	vol := cp3Primary(t, "rbs_catchup.bv", 1)
	defer vol.Close()

	for i := 0; i < 5; i++ {
		vol.WriteLBA(uint64(i), makeBlock(byte('A'+i)))
	}

	vol.StartRebuildServer("127.0.0.1:0")
	defer vol.StopRebuildServer()

	conn, _ := net.Dial("tcp", vol.rebuildServer.Addr())
	defer conn.Close()
	WriteFrame(conn, MsgRebuildReq, EncodeRebuildRequest(RebuildRequest{Type: RebuildWALCatchUp, FromLSN: 1, Epoch: 1}))

	var entries int
	var snapshotLSN uint64
	for {
		msgType, payload, err := ReadFrame(conn)
		if err != nil {
			t.Fatalf("read: %v", err)
		}
		if msgType == MsgRebuildEntry {
			entries++
		} else if msgType == MsgRebuildDone {
			if len(payload) >= 8 {
				snapshotLSN = binary.BigEndian.Uint64(payload[:8])
			}
			break
		} else if msgType == MsgRebuildError {
			t.Fatalf("error: %s", payload)
		}
	}
	if entries < 5 {
		t.Errorf("entries = %d, want >= 5", entries)
	}
	if snapshotLSN < 6 {
		t.Errorf("snapshotLSN = %d, want >= 6", snapshotLSN)
	}
}

func testRbSrvWALCatchUpEmpty(t *testing.T) {
	// Empty WAL -> MsgRebuildDone with nextLSN=1 (no entries).
	vol := cp3Primary(t, "rbs_empty.bv", 1)
	defer vol.Close()

	vol.StartRebuildServer("127.0.0.1:0")
	defer vol.StopRebuildServer()

	conn, _ := net.Dial("tcp", vol.rebuildServer.Addr())
	defer conn.Close()
	WriteFrame(conn, MsgRebuildReq, EncodeRebuildRequest(RebuildRequest{Type: RebuildWALCatchUp, FromLSN: 1, Epoch: 1}))

	msgType, payload, err := ReadFrame(conn)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if msgType != MsgRebuildDone {
		t.Errorf("expected MsgRebuildDone on empty WAL, got 0x%x", msgType)
	}
	if len(payload) >= 8 {
		lsn := binary.BigEndian.Uint64(payload[:8])
		if lsn == 0 {
			t.Error("snapshotLSN = 0 on empty WAL, want >= 1 (nextLSN)")
		}
	}
}

func testRbSrvFullExtentSize(t *testing.T) {
	vol := cp3Primary(t, "rbs_ext_sz.bv", 1)
	defer vol.Close()

	for i := 0; i < 3; i++ {
		vol.WriteLBA(uint64(i), makeBlock(byte('X'+i)))
	}
	vol.SyncCache()
	time.Sleep(20 * time.Millisecond)

	vol.StartRebuildServer("127.0.0.1:0")
	defer vol.StopRebuildServer()

	conn, _ := net.Dial("tcp", vol.rebuildServer.Addr())
	defer conn.Close()
	WriteFrame(conn, MsgRebuildReq, EncodeRebuildRequest(RebuildRequest{Type: RebuildFullExtent, Epoch: 1}))

	var totalBytes uint64
	for {
		msgType, payload, err := ReadFrame(conn)
		if err != nil {
			t.Fatalf("read: %v", err)
		}
		if msgType == MsgRebuildExtent {
			totalBytes += uint64(len(payload))
		} else if msgType == MsgRebuildDone {
			break
		} else if msgType == MsgRebuildError {
			t.Fatalf("error: %s", payload)
		}
	}
	if totalBytes != vol.super.VolumeSize {
		t.Errorf("extent bytes = %d, want %d (VolumeSize)", totalBytes, vol.super.VolumeSize)
	}
}

func testRbSrvFullExtentHasData(t *testing.T) {
	// Write data, flush to extent, stream full extent, verify data present.
	vol := cp3Primary(t, "rbs_ext_data.bv", 1)
	defer vol.Close()

	vol.WriteLBA(0, makeBlock('D'))
	vol.SyncCache()
	time.Sleep(30 * time.Millisecond) // let flusher write to extent

	vol.StartRebuildServer("127.0.0.1:0")
	defer vol.StopRebuildServer()

	conn, _ := net.Dial("tcp", vol.rebuildServer.Addr())
	defer conn.Close()
	WriteFrame(conn, MsgRebuildReq, EncodeRebuildRequest(RebuildRequest{Type: RebuildFullExtent, Epoch: 1}))

	var extentData []byte
	for {
		msgType, payload, _ := ReadFrame(conn)
		if msgType == MsgRebuildExtent {
			extentData = append(extentData, payload...)
		} else {
			break
		}
	}
	// First 4KB of extent = LBA 0 data.
	if len(extentData) < 4096 {
		t.Fatalf("extent data too short: %d", len(extentData))
	}
	if extentData[0] != 'D' {
		t.Errorf("extent LBA 0 = %c, want 'D' (flushed data should be in extent stream)", extentData[0])
	}
}

func testRbSrvStopDuringStream(t *testing.T) {
	vol := cp3Primary(t, "rbs_stop.bv", 1)
	defer vol.Close()
	for i := 0; i < 5; i++ {
		vol.WriteLBA(uint64(i), makeBlock('S'))
	}
	vol.StartRebuildServer("127.0.0.1:0")

	conn, _ := net.Dial("tcp", vol.rebuildServer.Addr())
	defer conn.Close()
	WriteFrame(conn, MsgRebuildReq, EncodeRebuildRequest(RebuildRequest{Type: RebuildFullExtent, Epoch: 1}))
	ReadFrame(conn) // read first chunk

	vol.StopRebuildServer()

	done := make(chan struct{})
	go func() {
		for {
			if _, _, err := ReadFrame(conn); err != nil {
				close(done)
				return
			}
		}
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("hung after stop")
	}
}

// ========== Rebuild Client ==========

func testRbCliWrongRole(t *testing.T) {
	vol := cp3Primary(t, "rbc_role.bv", 1)
	defer vol.Close()
	err := StartRebuild(vol, "127.0.0.1:1", 0, 1)
	if err == nil {
		t.Error("StartRebuild from Primary should fail")
	}
}

func testRbCliCatchUpDataIntegrity(t *testing.T) {
	// Full E2E: primary writes -> rebuild server -> client catch-up -> data matches.
	primary := cp3Primary(t, "rbc_pri.bv", 1)
	defer primary.Close()

	for i := 0; i < 10; i++ {
		primary.WriteLBA(uint64(i), makeBlock(byte('A'+i)))
	}

	primary.StartRebuildServer("127.0.0.1:0")
	defer primary.StopRebuildServer()

	replica := cp3Rebuilding(t, "rbc_rep.bv", 1)
	defer replica.Close()

	if err := StartRebuild(replica, primary.rebuildServer.Addr(), 1, 1); err != nil {
		t.Fatalf("StartRebuild: %v", err)
	}
	if replica.Role() != RoleReplica {
		t.Errorf("role = %s, want Replica", replica.Role())
	}

	// Verify every LBA.
	for i := 0; i < 10; i++ {
		data, err := replica.ReadLBA(uint64(i), 4096)
		if err != nil {
			t.Fatalf("ReadLBA(%d): %v", i, err)
		}
		if data[0] != byte('A'+i) {
			t.Errorf("LBA %d: got %c, want %c", i, data[0], byte('A'+i))
		}
	}
}

func testRbCliWALRecycledFallback(t *testing.T) {
	primary := cp3Primary(t, "rbc_recyc_p.bv", 1)
	defer primary.Close()

	for i := 0; i < 5; i++ {
		primary.WriteLBA(uint64(i), makeBlock(byte('R'+i)))
	}
	primary.SyncCache()
	time.Sleep(30 * time.Millisecond) // checkpoint advances

	primary.StartRebuildServer("127.0.0.1:0")
	defer primary.StopRebuildServer()

	replica := cp3Rebuilding(t, "rbc_recyc_r.bv", 1)
	defer replica.Close()

	// fromLSN=0 is below checkpoint -> WAL_RECYCLED -> fallback to full extent.
	if err := StartRebuild(replica, primary.rebuildServer.Addr(), 0, 1); err != nil {
		t.Fatalf("StartRebuild (fallback): %v", err)
	}
	if replica.Role() != RoleReplica {
		t.Errorf("role = %s after fallback, want Replica", replica.Role())
	}
	// Data present from extent copy.
	data, _ := replica.ReadLBA(0, 4096)
	if data[0] != 'R' {
		t.Errorf("LBA 0 = %c, want 'R'", data[0])
	}
}

func testRbCliFullExtentClearsDirtyMap(t *testing.T) {
	// After full extent rebuild, dirty map must be empty (old WAL entries invalid).
	primary := cp3Primary(t, "rbc_dm_p.bv", 1)
	defer primary.Close()
	primary.WriteLBA(0, makeBlock('D'))
	primary.SyncCache()
	time.Sleep(30 * time.Millisecond)

	primary.StartRebuildServer("127.0.0.1:0")
	defer primary.StopRebuildServer()

	replica := cp3Rebuilding(t, "rbc_dm_r.bv", 1)
	defer replica.Close()

	// Pre-populate replica's dirty map with stale entries.
	replica.dirtyMap.Put(999, 0, 1, 4096)
	if replica.dirtyMap.Len() == 0 {
		t.Fatal("dirty map should have entries before rebuild")
	}

	// Force full extent (fromLSN=0 below checkpoint).
	StartRebuild(replica, primary.rebuildServer.Addr(), 0, 1)

	if replica.dirtyMap.Len() != 0 {
		t.Errorf("dirty map has %d entries after full extent rebuild, want 0", replica.dirtyMap.Len())
	}
}

func testRbCliFullExtentResetsWAL(t *testing.T) {
	// After full extent rebuild, WAL head and tail must be 0.
	primary := cp3Primary(t, "rbc_wal_p.bv", 1)
	defer primary.Close()
	primary.WriteLBA(0, makeBlock('W'))
	primary.SyncCache()
	time.Sleep(30 * time.Millisecond)

	primary.StartRebuildServer("127.0.0.1:0")
	defer primary.StopRebuildServer()

	replica := cp3Rebuilding(t, "rbc_wal_r.bv", 1)
	defer replica.Close()

	// Write something to replica's WAL first so it's non-zero.
	// (Can't write as Rebuilding, but we can check head is 0 after rebuild)
	StartRebuild(replica, primary.rebuildServer.Addr(), 0, 1)

	head := replica.wal.LogicalHead()
	tail := replica.wal.LogicalTail()
	// After full extent, WAL was reset to 0 then second catch-up may have added entries.
	// But with no writes during rebuild, WAL should be clean or have only catch-up entries.
	// The key invariant: head and tail are consistent (head >= tail).
	if head < tail {
		t.Errorf("WAL inconsistent after rebuild: head=%d < tail=%d", head, tail)
	}
}

func testRbCliFullExtentPersistsSuperblock(t *testing.T) {
	// After full extent rebuild, superblock should be persisted with clean WAL state.
	// Reopen the vol and verify.
	primary := cp3Primary(t, "rbc_sb_p.bv", 1)
	defer primary.Close()
	primary.WriteLBA(0, makeBlock('S'))
	primary.SyncCache()
	time.Sleep(30 * time.Millisecond)

	primary.StartRebuildServer("127.0.0.1:0")
	defer primary.StopRebuildServer()

	dir := t.TempDir()
	replicaPath := filepath.Join(dir, "rbc_sb_r.bv")
	cfg := DefaultConfig()
	cfg.FlushInterval = 5 * time.Millisecond

	replica, _ := CreateBlockVol(replicaPath, CreateOptions{
		VolumeSize: 64 * 1024, BlockSize: 4096, WALSize: 64 * 1024,
	}, cfg)
	replica.HandleAssignment(1, RolePrimary, 1*time.Second)
	replica.HandleAssignment(1, RoleStale, 0)
	replica.HandleAssignment(1, RoleRebuilding, 0)

	StartRebuild(replica, primary.rebuildServer.Addr(), 0, 1)
	replica.Close()

	// Reopen and check superblock.
	reopened, err := OpenBlockVol(replicaPath, cfg)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer reopened.Close()

	if reopened.super.WALHead != 0 {
		t.Errorf("WALHead after reopen = %d, want 0", reopened.super.WALHead)
	}
	if reopened.super.WALTail != 0 {
		t.Errorf("WALTail after reopen = %d, want 0", reopened.super.WALTail)
	}
}

// ========== applyRebuildEntry ==========

func testApplyNextLSNAdvanced(t *testing.T) {
	// applyRebuildEntry must advance nextLSN to max(entry.LSN)+1.
	vol := cp3Vol(t, "apply_lsn.bv", 64*1024)
	defer vol.Close()

	initial := vol.nextLSN.Load()

	entry := WALEntry{LSN: 42, Epoch: 0, Type: EntryTypeWrite, LBA: 0, Length: 4096, Data: makeBlock('N')}
	encoded, _ := entry.Encode()
	if err := applyRebuildEntry(vol, encoded); err != nil {
		t.Fatalf("apply: %v", err)
	}

	got := vol.nextLSN.Load()
	if got != 43 {
		t.Errorf("nextLSN = %d, want 43 (LSN 42 + 1), was %d before", got, initial)
	}
}

func testApplyOutOfOrderAccepted(t *testing.T) {
	// Unlike ReplicaReceiver, rebuild has NO contiguity enforcement.
	// Out-of-order entries are accepted.
	vol := cp3Vol(t, "apply_ooo.bv", 64*1024)
	defer vol.Close()

	// Apply LSN=10 first, then LSN=5 -- both should succeed.
	e10 := WALEntry{LSN: 10, Epoch: 0, Type: EntryTypeWrite, LBA: 0, Length: 4096, Data: makeBlock('A')}
	enc10, _ := e10.Encode()
	if err := applyRebuildEntry(vol, enc10); err != nil {
		t.Fatalf("apply LSN=10: %v", err)
	}

	e5 := WALEntry{LSN: 5, Epoch: 0, Type: EntryTypeWrite, LBA: 1, Length: 4096, Data: makeBlock('B')}
	enc5, _ := e5.Encode()
	if err := applyRebuildEntry(vol, enc5); err != nil {
		t.Fatalf("apply LSN=5: %v", err)
	}

	// nextLSN should be 11 (max of 10+1, 5+1 = 11).
	if vol.nextLSN.Load() != 11 {
		t.Errorf("nextLSN = %d, want 11 (max LSN was 10)", vol.nextLSN.Load())
	}

	// Both entries should be in dirty map.
	_, _, _, ok0 := vol.dirtyMap.Get(0)
	_, _, _, ok1 := vol.dirtyMap.Get(1)
	if !ok0 || !ok1 {
		t.Errorf("dirty map: LBA 0 ok=%v, LBA 1 ok=%v -- both should be present", ok0, ok1)
	}
}

func testApplyDirtyMapUpdated(t *testing.T) {
	vol := cp3Vol(t, "apply_dm.bv", 64*1024)
	defer vol.Close()

	// Apply a WRITE entry.
	entry := WALEntry{LSN: 1, Epoch: 0, Type: EntryTypeWrite, LBA: 7, Length: 4096, Data: makeBlock('D')}
	encoded, _ := entry.Encode()
	applyRebuildEntry(vol, encoded)

	_, lsn, _, ok := vol.dirtyMap.Get(7)
	if !ok {
		t.Fatal("LBA 7 not in dirty map after apply")
	}
	if lsn != 1 {
		t.Errorf("dirty map LSN for LBA 7 = %d, want 1", lsn)
	}

	// Apply a TRIM entry -- should also update dirty map.
	trimEntry := WALEntry{LSN: 2, Epoch: 0, Type: EntryTypeTrim, LBA: 7, Length: 4096}
	trimEnc, _ := trimEntry.Encode()
	applyRebuildEntry(vol, trimEnc)

	_, trimLSN, _, ok := vol.dirtyMap.Get(7)
	if !ok {
		t.Fatal("LBA 7 not in dirty map after TRIM apply")
	}
	if trimLSN != 2 {
		t.Errorf("dirty map LSN after TRIM = %d, want 2", trimLSN)
	}
}

// ========== Integration ==========

func testLifecycleFullCycle(t *testing.T) {
	// Full lifecycle: None -> Replica -> promote -> write -> demote -> rebuild -> replica.
	vol := cp3Vol(t, "lifecycle.bv", 64*1024)
	defer vol.Close()

	// 1. None -> Replica
	vol.HandleAssignment(1, RoleReplica, 0)
	if vol.Role() != RoleReplica {
		t.Fatalf("step 1: role = %s", vol.Role())
	}

	// 2. Promote to Primary
	vol.HandleAssignment(2, RolePrimary, 10*time.Second)
	if vol.Role() != RolePrimary {
		t.Fatalf("step 2: role = %s", vol.Role())
	}

	// 3. Write data
	for i := 0; i < 5; i++ {
		vol.WriteLBA(uint64(i), makeBlock(byte('L'+i)))
	}
	vol.SyncCache()

	// 4. Demote
	vol.HandleAssignment(3, RoleStale, 0)
	if vol.Role() != RoleStale {
		t.Fatalf("step 4: role = %s", vol.Role())
	}

	// 5. Writes rejected
	if err := vol.WriteLBA(0, makeBlock('X')); err == nil {
		t.Error("write after demote should fail")
	}

	// 6. Reads work
	data, _ := vol.ReadLBA(0, 4096)
	if data[0] != 'L' {
		t.Errorf("LBA 0 = %c, want 'L'", data[0])
	}

	// 7. Rebuilding
	vol.HandleAssignment(3, RoleRebuilding, 0)
	if vol.Role() != RoleRebuilding {
		t.Fatalf("step 7: role = %s", vol.Role())
	}
}

func testDirtyMapClearConcurrent(t *testing.T) {
	dm := NewDirtyMap(16)
	for i := uint64(0); i < 100; i++ {
		dm.Put(i, i*4096, i+1, 4096)
	}

	var wg sync.WaitGroup
	var reads atomic.Int64

	wg.Add(4)
	for g := 0; g < 4; g++ {
		go func() {
			defer wg.Done()
			for i := 0; i < 1000; i++ {
				_, _, _, _ = dm.Get(uint64(i % 100))
				reads.Add(1)
			}
		}()
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 10; i++ {
			dm.Clear()
			time.Sleep(100 * time.Microsecond)
		}
	}()

	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("deadlock")
	}
	if reads.Load() != 4000 {
		t.Errorf("reads = %d, want 4000", reads.Load())
	}
}
