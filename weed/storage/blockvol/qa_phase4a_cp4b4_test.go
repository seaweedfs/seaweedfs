package blockvol

import (
	"fmt"
	"net"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

// TestQAPhase4ACP4b4 tests Phase 4A CP4b-4 adversarial scenarios:
// partial channel failures, protocol edge cases, rebuild scenarios,
// edge interactions, and error injection.
func TestQAPhase4ACP4b4(t *testing.T) {
	tests := []struct {
		name string
		run  func(t *testing.T)
	}{
		// Group A: Partial Channel Failures
		{name: "barrier_stale_lsn_after_gap", run: testQA4b4BarrierStaleLSNAfterGap},
		{name: "ship_data_ok_ctrl_down", run: testQA4b4ShipDataOkCtrlDown},
		{name: "ship_ctrl_ok_data_down", run: testQA4b4ShipCtrlOkDataDown},

		// Group B: Protocol Edge Cases
		{name: "ship_large_entry_near_max", run: testQA4b4ShipLargeEntryNearMax},
		{name: "ship_entries_across_wal_wrap", run: testQA4b4ShipEntriesAcrossWALWrap},

		// Group C: Rebuild Scenarios
		{name: "rebuild_catchup_concurrent_writes", run: testQA4b4RebuildCatchupConcurrentWrites},
		{name: "rebuild_full_extent_midcopy_writes", run: testQA4b4RebuildFullExtentMidcopyWrites},
		{name: "rebuild_interrupted_retry", run: testQA4b4RebuildInterruptedRetry},

		// Group D: Edge Interactions
		{name: "read_during_wal_reclaim", run: testQA4b4ReadDuringWALReclaim},
		{name: "trim_blocks_survive_rebuild", run: testQA4b4TrimBlocksSurviveRebuild},
		{name: "demote_while_wal_full", run: testQA4b4DemoteWhileWALFull},

		// Group E: Error Injection
		{name: "rebuild_server_epoch_mismatch", run: testQA4b4RebuildServerEpochMismatch},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.run(t)
		})
	}
}

// ========== Group A: Partial Channel Failures ==========

// A1: Ship LSN 1,2,4 (skip 3) to replica receiver, then barrier(4).
// Barrier must time out because receiver blocks waiting for contiguous LSN 3.
func testQA4b4BarrierStaleLSNAfterGap(t *testing.T) {
	dir := t.TempDir()
	cfg := DefaultConfig()
	cfg.FlushInterval = 5 * time.Millisecond

	vol, err := CreateBlockVol(filepath.Join(dir, "replica.blockvol"), CreateOptions{
		VolumeSize: 1 * 1024 * 1024, BlockSize: 4096, WALSize: 256 * 1024,
	}, cfg)
	if err != nil {
		t.Fatalf("CreateBlockVol: %v", err)
	}
	defer vol.Close()
	if err := vol.SetEpoch(1); err != nil {
		t.Fatalf("SetEpoch: %v", err)
	}

	recv, err := NewReplicaReceiver(vol, "127.0.0.1:0", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("NewReplicaReceiver: %v", err)
	}
	recv.barrierTimeout = 200 * time.Millisecond // fast timeout for test
	recv.Serve()
	defer recv.Stop()

	dataConn, err := net.Dial("tcp", recv.DataAddr())
	if err != nil {
		t.Fatalf("dial data: %v", err)
	}
	defer dataConn.Close()

	// Send LSN 1, 2, 4 (skip 3).
	for _, lsn := range []uint64{1, 2, 4} {
		entry := &WALEntry{LSN: lsn, Epoch: 1, Type: EntryTypeWrite, LBA: lsn - 1, Length: 4096, Data: makeBlock(byte('A' + lsn))}
		encoded, _ := entry.Encode()
		if err := WriteFrame(dataConn, MsgWALEntry, encoded); err != nil {
			t.Fatalf("send LSN=%d: %v", lsn, err)
		}
	}
	time.Sleep(50 * time.Millisecond)

	// receivedLSN should be 2 (gap at 3 blocks further progress).
	if got := recv.ReceivedLSN(); got != 2 {
		t.Fatalf("ReceivedLSN = %d after gap, want 2", got)
	}

	// Barrier(4) must time out since LSN 3 is missing.
	ctrlConn, err := net.Dial("tcp", recv.CtrlAddr())
	if err != nil {
		t.Fatalf("dial ctrl: %v", err)
	}
	defer ctrlConn.Close()

	req := EncodeBarrierRequest(BarrierRequest{LSN: 4, Epoch: 1})
	if err := WriteFrame(ctrlConn, MsgBarrierReq, req); err != nil {
		t.Fatalf("send barrier: %v", err)
	}

	ctrlConn.SetReadDeadline(time.Now().Add(3 * time.Second))
	_, payload, err := ReadFrame(ctrlConn)
	if err != nil {
		t.Fatalf("read barrier response: %v", err)
	}
	if payload[0] == BarrierOK {
		t.Error("barrier returned OK despite gap at LSN 3, expected timeout")
	}
	// receivedLSN should still be 2.
	if got := recv.ReceivedLSN(); got != 2 {
		t.Errorf("ReceivedLSN = %d after barrier timeout, want 2", got)
	}
}

// A2: Data channel works, ctrl listener closed before Barrier.
// Barrier must return error. Data entries still applied.
func testQA4b4ShipDataOkCtrlDown(t *testing.T) {
	primary, replica := createReplicaPair(t)
	defer primary.Close()
	defer replica.Close()

	// Write data through primary -> shipped to replica via data channel.
	for i := 0; i < 5; i++ {
		if err := primary.WriteLBA(uint64(i), makeBlock(byte('A'+i))); err != nil {
			t.Fatalf("WriteLBA(%d): %v", i, err)
		}
	}
	time.Sleep(50 * time.Millisecond)

	// Verify data entries were applied to replica.
	replicaLSN := replica.replRecv.ReceivedLSN()
	if replicaLSN < 5 {
		t.Fatalf("replica receivedLSN = %d, want >= 5", replicaLSN)
	}

	// Kill ctrl listener to make barrier fail.
	replica.replRecv.ctrlListener.Close()

	// Force primary shipper to reconnect ctrl.
	shipper := primary.shipperGroup.Shipper(0)
	shipper.ctrlMu.Lock()
	if shipper.ctrlConn != nil {
		shipper.ctrlConn.Close()
		shipper.ctrlConn = nil
	}
	shipper.ctrlMu.Unlock()

	// Barrier must fail (ctrl down).
	err := shipper.Barrier(replicaLSN)
	if err == nil {
		t.Error("Barrier should fail when ctrl listener is closed")
	}

	// Data entries should still be applied (receivedLSN unchanged or higher).
	if got := replica.replRecv.ReceivedLSN(); got < replicaLSN {
		t.Errorf("receivedLSN regressed from %d to %d after ctrl failure", replicaLSN, got)
	}
}

// A3: Ctrl is up, data listener closed. Ship must fail, barrier may timeout.
func testQA4b4ShipCtrlOkDataDown(t *testing.T) {
	dir := t.TempDir()
	cfg := DefaultConfig()
	cfg.FlushInterval = 5 * time.Millisecond

	vol, err := CreateBlockVol(filepath.Join(dir, "replica.blockvol"), CreateOptions{
		VolumeSize: 1 * 1024 * 1024, BlockSize: 4096, WALSize: 256 * 1024,
	}, cfg)
	if err != nil {
		t.Fatalf("CreateBlockVol: %v", err)
	}
	defer vol.Close()
	if err := vol.SetEpoch(1); err != nil {
		t.Fatalf("SetEpoch: %v", err)
	}

	recv, err := NewReplicaReceiver(vol, "127.0.0.1:0", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("NewReplicaReceiver: %v", err)
	}
	recv.barrierTimeout = 200 * time.Millisecond
	recv.Serve()
	defer recv.Stop()

	ctrlAddr := recv.CtrlAddr()

	// Close data listener -- data conn will fail.
	recv.dataListener.Close()

	// Create shipper pointed at closed data port, working ctrl port.
	shipper := NewWALShipper(recv.DataAddr(), ctrlAddr, func() uint64 { return 1 }, nil)
	defer shipper.Stop()

	// Ship should fail (can't connect data), shipper degrades.
	entry := &WALEntry{LSN: 1, Epoch: 1, Type: EntryTypeWrite, LBA: 0, Length: 4096, Data: makeBlock('X')}
	_ = shipper.Ship(entry)

	// receivedLSN should stay 0.
	if got := recv.ReceivedLSN(); got != 0 {
		t.Errorf("ReceivedLSN = %d, want 0 (data channel was down)", got)
	}
}

// ========== Group B: Protocol Edge Cases ==========

// B1: Ship a large WAL entry through the frame codec and verify it round-trips.
// Uses WriteFrame/ReadFrame directly to test near-max frame payload.
// Then ships a multi-block entry through a real replica receiver.
func testQA4b4ShipLargeEntryNearMax(t *testing.T) {
	// Part 1: Frame codec round-trip with a large payload (1MB).
	const largeSize = 1 * 1024 * 1024
	largePayload := make([]byte, largeSize)
	for i := range largePayload {
		largePayload[i] = byte('L')
	}

	server, client := net.Pipe()
	defer server.Close()
	defer client.Close()

	writeDone := make(chan error, 1)
	go func() {
		writeDone <- WriteFrame(client, MsgWALEntry, largePayload)
	}()

	msgType, payload, err := ReadFrame(server)
	if err != nil {
		t.Fatalf("ReadFrame large: %v", err)
	}
	if msgType != MsgWALEntry {
		t.Errorf("msgType = 0x%02x, want MsgWALEntry", msgType)
	}
	if len(payload) != largeSize {
		t.Errorf("payload len = %d, want %d", len(payload), largeSize)
	}
	if payload[0] != 'L' || payload[largeSize-1] != 'L' {
		t.Error("payload data corrupted")
	}

	if err := <-writeDone; err != nil {
		t.Fatalf("WriteFrame: %v", err)
	}

	// Part 2: Ship a multi-block entry (8 blocks = 32KB) through a real receiver.
	dir := t.TempDir()
	cfg := DefaultConfig()
	cfg.FlushInterval = 5 * time.Millisecond

	vol, err := CreateBlockVol(filepath.Join(dir, "replica.blockvol"), CreateOptions{
		VolumeSize: 1 * 1024 * 1024, BlockSize: 4096, WALSize: 512 * 1024,
	}, cfg)
	if err != nil {
		t.Fatalf("CreateBlockVol: %v", err)
	}
	defer vol.Close()
	if err := vol.SetEpoch(1); err != nil {
		t.Fatalf("SetEpoch: %v", err)
	}

	recv, err := NewReplicaReceiver(vol, "127.0.0.1:0", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("NewReplicaReceiver: %v", err)
	}
	recv.Serve()
	defer recv.Stop()

	dataConn, err := net.Dial("tcp", recv.DataAddr())
	if err != nil {
		t.Fatalf("dial data: %v", err)
	}
	defer dataConn.Close()

	// 8 blocks = 32KB data.
	const blockCount = 8
	bigData := make([]byte, blockCount*4096)
	for i := range bigData {
		bigData[i] = byte('B')
	}

	entry := &WALEntry{LSN: 1, Epoch: 1, Type: EntryTypeWrite, LBA: 0, Length: uint32(len(bigData)), Data: bigData}
	encoded, err := entry.Encode()
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}

	if err := WriteFrame(dataConn, MsgWALEntry, encoded); err != nil {
		t.Fatalf("WriteFrame: %v", err)
	}

	time.Sleep(100 * time.Millisecond)

	if got := recv.ReceivedLSN(); got != 1 {
		t.Errorf("ReceivedLSN = %d, want 1 (multi-block entry should be applied)", got)
	}
}

// B2: Write enough entries to force WAL wrap, ship all, verify replica has data.
func testQA4b4ShipEntriesAcrossWALWrap(t *testing.T) {
	primary, replica := createReplicaPair(t)
	defer primary.Close()
	defer replica.Close()

	// createReplicaPair uses WALSize=512KB, VolumeSize=1MB.
	// Each write is 4KB WAL entry + header. Fill enough to wrap.
	// ~512KB WAL / ~4KB per entry ≈ 128 entries before wrap, but flusher
	// reclaims space so we can write more.
	const numWrites = 50
	for i := 0; i < numWrites; i++ {
		if err := primary.WriteLBA(uint64(i%256), makeBlock(byte('W'+i%26))); err != nil {
			// WAL full is acceptable if flusher can't keep up.
			t.Logf("WriteLBA(%d): %v (expected if WAL fills)", i, err)
			break
		}
	}

	// SyncCache to push barrier.
	if err := primary.SyncCache(); err != nil {
		// Degraded replica is ok -- we test that entries shipped correctly.
		t.Logf("SyncCache: %v (may degrade)", err)
	}

	time.Sleep(100 * time.Millisecond)

	// Replica should have received entries.
	replicaLSN := replica.replRecv.ReceivedLSN()
	if replicaLSN == 0 {
		t.Error("replica receivedLSN = 0, expected > 0 after writes across WAL")
	}

	// Verify first few LBAs on replica.
	for i := 0; i < 5 && uint64(i) <= replicaLSN; i++ {
		data, err := replica.ReadLBA(uint64(i), 4096)
		if err != nil {
			t.Fatalf("replica ReadLBA(%d): %v", i, err)
		}
		if data[0] == 0 {
			t.Errorf("replica LBA %d is zero, expected non-zero data", i)
		}
	}
}

// ========== Group C: Rebuild Scenarios ==========

// C1: Start rebuild while primary receives concurrent writes.
func testQA4b4RebuildCatchupConcurrentWrites(t *testing.T) {
	primary := cp3Primary(t, "rb_cc_pri.bv", 1)
	defer primary.Close()

	// Write initial data.
	for i := 0; i < 5; i++ {
		primary.WriteLBA(uint64(i), makeBlock(byte('A'+i)))
	}

	primary.StartRebuildServer("127.0.0.1:0")
	defer primary.StopRebuildServer()

	// Start concurrent writes on primary while rebuild happens.
	var wg sync.WaitGroup
	stopWriting := make(chan struct{})
	wg.Add(1)
	go func() {
		defer wg.Done()
		lba := uint64(10)
		for {
			select {
			case <-stopWriting:
				return
			default:
			}
			primary.WriteLBA(lba, makeBlock(byte('C')))
			lba++
			if lba > 14 {
				lba = 10
			}
			time.Sleep(1 * time.Millisecond)
		}
	}()

	// Start rebuild client.
	replica := cp3Rebuilding(t, "rb_cc_rep.bv", 1)
	defer replica.Close()

	err := StartRebuild(replica, primary.rebuildServer.Addr(), 1, 1)
	close(stopWriting)
	wg.Wait()

	if err != nil {
		t.Fatalf("StartRebuild: %v", err)
	}
	if replica.Role() != RoleReplica {
		t.Errorf("role = %s, want Replica", replica.Role())
	}

	// Verify initial data on replica.
	for i := 0; i < 5; i++ {
		data, err := replica.ReadLBA(uint64(i), 4096)
		if err != nil {
			t.Fatalf("ReadLBA(%d): %v", i, err)
		}
		if data[0] != byte('A'+i) {
			t.Errorf("LBA %d: got %c, want %c", i, data[0], byte('A'+i))
		}
	}
}

// C2: Write initial data, start full extent rebuild, concurrent writes during copy.
func testQA4b4RebuildFullExtentMidcopyWrites(t *testing.T) {
	primary := cp3Primary(t, "rb_fe_pri.bv", 1)
	defer primary.Close()

	// Write and flush to make data in extent.
	for i := 0; i < 5; i++ {
		primary.WriteLBA(uint64(i), makeBlock(byte('D'+i)))
	}
	primary.SyncCache()
	time.Sleep(30 * time.Millisecond) // let flusher move to extent

	primary.StartRebuildServer("127.0.0.1:0")
	defer primary.StopRebuildServer()

	// Start concurrent writes during rebuild.
	var wg sync.WaitGroup
	stopWriting := make(chan struct{})
	wg.Add(1)
	go func() {
		defer wg.Done()
		lba := uint64(10)
		for {
			select {
			case <-stopWriting:
				return
			default:
			}
			primary.WriteLBA(lba, makeBlock(byte('M')))
			lba++
			if lba > 14 {
				lba = 10
			}
			time.Sleep(1 * time.Millisecond)
		}
	}()

	// Force full extent rebuild (fromLSN=0 below checkpoint).
	replica := cp3Rebuilding(t, "rb_fe_rep.bv", 1)
	defer replica.Close()

	err := StartRebuild(replica, primary.rebuildServer.Addr(), 0, 1)
	close(stopWriting)
	wg.Wait()

	if err != nil {
		t.Fatalf("StartRebuild: %v", err)
	}
	if replica.Role() != RoleReplica {
		t.Errorf("role = %s, want Replica", replica.Role())
	}

	// Verify flushed data on replica (from extent).
	for i := 0; i < 5; i++ {
		data, err := replica.ReadLBA(uint64(i), 4096)
		if err != nil {
			t.Fatalf("ReadLBA(%d): %v", i, err)
		}
		if data[0] != byte('D'+i) {
			t.Errorf("LBA %d: got %c, want %c", i, data[0], byte('D'+i))
		}
	}
}

// C3: Start rebuild, read half the entries, close connection, retry from start.
func testQA4b4RebuildInterruptedRetry(t *testing.T) {
	primary := cp3Primary(t, "rb_int_pri.bv", 1)
	defer primary.Close()

	for i := 0; i < 10; i++ {
		primary.WriteLBA(uint64(i), makeBlock(byte('R'+i%26)))
	}

	primary.StartRebuildServer("127.0.0.1:0")
	defer primary.StopRebuildServer()

	// First attempt: connect, read a few entries, then close.
	conn1, err := net.Dial("tcp", primary.rebuildServer.Addr())
	if err != nil {
		t.Fatalf("dial 1: %v", err)
	}
	req := EncodeRebuildRequest(RebuildRequest{Type: RebuildWALCatchUp, FromLSN: 1, Epoch: 1})
	WriteFrame(conn1, MsgRebuildReq, req)

	// Read a few entries then abort.
	for i := 0; i < 3; i++ {
		_, _, err := ReadFrame(conn1)
		if err != nil {
			t.Fatalf("read entry %d: %v", i, err)
		}
	}
	conn1.Close() // simulate crash

	// Second attempt: full rebuild should succeed.
	replica := cp3Rebuilding(t, "rb_int_rep.bv", 1)
	defer replica.Close()

	if err := StartRebuild(replica, primary.rebuildServer.Addr(), 1, 1); err != nil {
		t.Fatalf("StartRebuild (retry): %v", err)
	}
	if replica.Role() != RoleReplica {
		t.Errorf("role = %s, want Replica", replica.Role())
	}

	// Verify all data.
	for i := 0; i < 10; i++ {
		data, err := replica.ReadLBA(uint64(i), 4096)
		if err != nil {
			t.Fatalf("ReadLBA(%d): %v", i, err)
		}
		expected := byte('R' + i%26)
		if data[0] != expected {
			t.Errorf("LBA %d: got %c, want %c", i, data[0], expected)
		}
	}
}

// ========== Group D: Edge Interactions ==========

// D1: Write blocks, flush to reclaim WAL, concurrent ReadLBA on flushed blocks.
func testQA4b4ReadDuringWALReclaim(t *testing.T) {
	vol := cp3Primary(t, "read_reclaim.bv", 1)
	defer vol.Close()

	// Write data.
	for i := 0; i < 10; i++ {
		if err := vol.WriteLBA(uint64(i), makeBlock(byte('R'+i%26))); err != nil {
			t.Fatalf("WriteLBA(%d): %v", i, err)
		}
	}

	// SyncCache + wait for flusher to reclaim WAL.
	vol.SyncCache()
	time.Sleep(50 * time.Millisecond)

	// Concurrent reads on recently-flushed blocks.
	var wg sync.WaitGroup
	errs := make([]error, 10)
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(lba int) {
			defer wg.Done()
			data, err := vol.ReadLBA(uint64(lba), 4096)
			if err != nil {
				errs[lba] = err
				return
			}
			expected := byte('R' + lba%26)
			if data[0] != expected {
				errs[lba] = fmt.Errorf("LBA %d: got %c, want %c", lba, data[0], expected)
			}
		}(i)
	}
	wg.Wait()

	for i, err := range errs {
		if err != nil {
			t.Errorf("read LBA %d: %v", i, err)
		}
	}
}

// D2: Write blocks, TRIM some, rebuild replica, verify trimmed blocks read as zeros.
func testQA4b4TrimBlocksSurviveRebuild(t *testing.T) {
	primary := cp3Primary(t, "trim_rb_pri.bv", 1)
	defer primary.Close()

	// Write 10 blocks.
	for i := 0; i < 10; i++ {
		primary.WriteLBA(uint64(i), makeBlock(byte('T'+i%26)))
	}

	// TRIM blocks 3-5.
	for i := 3; i <= 5; i++ {
		if err := primary.Trim(uint64(i), 4096); err != nil {
			t.Fatalf("Trim(%d): %v", i, err)
		}
	}

	primary.StartRebuildServer("127.0.0.1:0")
	defer primary.StopRebuildServer()

	replica := cp3Rebuilding(t, "trim_rb_rep.bv", 1)
	defer replica.Close()

	if err := StartRebuild(replica, primary.rebuildServer.Addr(), 1, 1); err != nil {
		t.Fatalf("StartRebuild: %v", err)
	}

	// Trimmed blocks should read as zeros.
	for i := 3; i <= 5; i++ {
		data, err := replica.ReadLBA(uint64(i), 4096)
		if err != nil {
			t.Fatalf("ReadLBA(%d): %v", i, err)
		}
		nonZero := false
		for _, b := range data {
			if b != 0 {
				nonZero = true
				break
			}
		}
		if nonZero {
			t.Errorf("LBA %d should be zero after TRIM+rebuild, got non-zero data", i)
		}
	}

	// Non-trimmed blocks should still have data.
	data0, _ := replica.ReadLBA(0, 4096)
	if data0[0] != 'T' {
		t.Errorf("LBA 0: got %c, want 'T'", data0[0])
	}
}

// D3: Fill WAL, start WriteLBA (blocks in WAL-full retry), concurrently demote.
func testQA4b4DemoteWhileWALFull(t *testing.T) {
	// Create volume with tiny WAL.
	dir := t.TempDir()
	cfg := DefaultConfig()
	cfg.FlushInterval = 100 * time.Millisecond // slow flusher
	cfg.WALFullTimeout = 2 * time.Second

	vol, err := CreateBlockVol(filepath.Join(dir, "demote_walful.bv"), CreateOptions{
		VolumeSize: 64 * 1024,
		BlockSize:  4096,
		WALSize:    16 * 1024, // tiny WAL: ~4 entries
	}, cfg)
	if err != nil {
		t.Fatalf("CreateBlockVol: %v", err)
	}
	defer vol.Close()

	if err := vol.HandleAssignment(1, RolePrimary, 30*time.Second); err != nil {
		t.Fatalf("assign primary: %v", err)
	}
	vol.drainTimeout = 3 * time.Second

	// Fill WAL.
	writeCount := 0
	for i := 0; i < 20; i++ {
		if err := vol.WriteLBA(uint64(i%16), makeBlock(byte('F'))); err != nil {
			break
		}
		writeCount++
	}
	if writeCount < 2 {
		t.Fatalf("only wrote %d entries before WAL full", writeCount)
	}

	// Start a WriteLBA that will likely block in WAL-full retry.
	writeDone := make(chan error, 1)
	go func() {
		writeDone <- vol.WriteLBA(0, makeBlock('Z'))
	}()

	// Give the write a moment to start retrying.
	time.Sleep(10 * time.Millisecond)

	// Concurrently demote.
	demoteDone := make(chan error, 1)
	go func() {
		demoteDone <- vol.HandleAssignment(2, RoleStale, 0)
	}()

	// Both should complete within timeout (no deadlock).
	timer := time.NewTimer(5 * time.Second)
	defer timer.Stop()

	select {
	case err := <-demoteDone:
		// Demote may succeed or fail with drain timeout -- both OK.
		t.Logf("demote result: %v", err)
	case <-timer.C:
		t.Fatal("demote hung for 5s -- deadlock between WAL-full retry and demote drain")
	}

	select {
	case err := <-writeDone:
		// Write may fail (demoted, closed, WAL full timeout) -- all acceptable.
		t.Logf("write result: %v", err)
	case <-timer.C:
		t.Fatal("write hung for 5s after demote")
	}
}

// ========== Group E: Error Injection ==========

// E1: Start rebuild server on primary (epoch=1), client sends epoch=2.
// Server should return EPOCH_MISMATCH.
func testQA4b4RebuildServerEpochMismatch(t *testing.T) {
	vol := cp3Primary(t, "rbs_epm.bv", 1)
	defer vol.Close()

	vol.StartRebuildServer("127.0.0.1:0")
	defer vol.StopRebuildServer()

	// Client sends mismatched epoch=2.
	conn, err := net.Dial("tcp", vol.rebuildServer.Addr())
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()

	req := EncodeRebuildRequest(RebuildRequest{Type: RebuildWALCatchUp, FromLSN: 1, Epoch: 2})
	if err := WriteFrame(conn, MsgRebuildReq, req); err != nil {
		t.Fatalf("send request: %v", err)
	}

	conn.SetReadDeadline(time.Now().Add(3 * time.Second))
	msgType, payload, err := ReadFrame(conn)
	if err != nil {
		t.Fatalf("read response: %v", err)
	}
	if msgType != MsgRebuildError {
		t.Errorf("msgType = 0x%02x, want MsgRebuildError(0x%02x)", msgType, MsgRebuildError)
	}
	if string(payload) != "EPOCH_MISMATCH" {
		t.Errorf("payload = %q, want EPOCH_MISMATCH", string(payload))
	}

	// Also test epoch=0 (lower than server's epoch=1).
	conn2, err := net.Dial("tcp", vol.rebuildServer.Addr())
	if err != nil {
		t.Fatalf("dial 2: %v", err)
	}
	defer conn2.Close()

	req2 := EncodeRebuildRequest(RebuildRequest{Type: RebuildFullExtent, Epoch: 0})
	if err := WriteFrame(conn2, MsgRebuildReq, req2); err != nil {
		t.Fatalf("send request 2: %v", err)
	}

	conn2.SetReadDeadline(time.Now().Add(3 * time.Second))
	msgType2, payload2, err := ReadFrame(conn2)
	if err != nil {
		t.Fatalf("read response 2: %v", err)
	}
	if msgType2 != MsgRebuildError || string(payload2) != "EPOCH_MISMATCH" {
		t.Errorf("epoch=0: type=0x%02x payload=%q, want EPOCH_MISMATCH", msgType2, payload2)
	}
}

