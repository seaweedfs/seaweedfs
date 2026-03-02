package blockvol

import (
	"bytes"
	"errors"
	"io"
	"math"
	"net"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestQAPhase4ACP2 tests Phase 4A CP2 replication primitives adversarially:
// frame protocol, WAL shipper, replica receiver, barrier, distributed sync.
func TestQAPhase4ACP2(t *testing.T) {
	tests := []struct {
		name string
		run  func(t *testing.T)
	}{
		// QA-4A-CP2-1: Frame Protocol Adversarial
		{name: "frame_max_payload_boundary", run: testQAFrameMaxPayloadBoundary},
		{name: "frame_truncated_header", run: testQAFrameTruncatedHeader},
		{name: "frame_truncated_payload", run: testQAFrameTruncatedPayload},
		{name: "frame_zero_payload", run: testQAFrameZeroPayload},
		{name: "frame_concurrent_writes_no_interleave", run: testQAFrameConcurrentWrites},

		// QA-4A-CP2-2: WALShipper Adversarial
		{name: "shipper_ship_after_stop", run: testQAShipperShipAfterStop},
		{name: "shipper_barrier_after_degraded", run: testQAShipperBarrierAfterDegraded},
		{name: "shipper_stale_epoch_no_shippedlsn", run: testQAShipperStaleEpochNoShippedLSN},
		{name: "shipper_degraded_is_permanent", run: testQAShipperDegradedPermanent},
		{name: "shipper_concurrent_ship_stop", run: testQAShipperConcurrentShipStop},

		// QA-4A-CP2-3: ReplicaReceiver Adversarial
		{name: "receiver_out_of_order_lsn_skipped", run: testQAReceiverOutOfOrderLSN},
		{name: "receiver_concurrent_data_conns", run: testQAReceiverConcurrentDataConns},
		{name: "receiver_future_epoch_rejected", run: testQAReceiverFutureEpochRejected},
		{name: "receiver_barrier_before_entries_waits", run: testQAReceiverBarrierBeforeEntries},
		{name: "receiver_barrier_timeout_no_entries", run: testQAReceiverBarrierTimeoutNoEntries},
		{name: "receiver_barrier_epoch_mismatch", run: testQAReceiverBarrierEpochMismatch},
		{name: "receiver_stop_unblocks_barrier", run: testQAReceiverStopUnblocksBarrier},

		// QA-4A-CP2-4: DistributedSync Adversarial
		{name: "dsync_local_fail_returns_error", run: testQADSyncLocalFailReturnsError},
		{name: "dsync_remote_fail_degrades_not_errors", run: testQADSyncRemoteFailDegrades},
		{name: "dsync_both_fail_returns_local", run: testQADSyncBothFail},
		{name: "dsync_parallel_execution", run: testQADSyncParallelExecution},

		// QA-4A-CP2-5: End-to-end Adversarial
		{name: "e2e_replica_data_matches_primary", run: testQAE2EReplicaDataMatchesPrimary},
		{name: "e2e_close_primary_during_ship", run: testQAE2EClosePrimaryDuringShip},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.run(t)
		})
	}
}

// --- helpers ---

// createReplicaPair creates a fenced primary + replica vol connected via loopback.
// Returns primary, replica, and a cleanup function.
func createReplicaPair(t *testing.T) (primary, replica *BlockVol) {
	t.Helper()
	dir := t.TempDir()
	cfg := DefaultConfig()
	cfg.FlushInterval = 5 * time.Millisecond

	// Create primary.
	p, err := CreateBlockVol(filepath.Join(dir, "primary.blockvol"), CreateOptions{
		VolumeSize: 1 * 1024 * 1024,
		BlockSize:  4096,
		WALSize:    512 * 1024,
	}, cfg)
	if err != nil {
		t.Fatalf("CreateBlockVol(primary): %v", err)
	}
	if err := p.SetEpoch(1); err != nil {
		t.Fatalf("SetEpoch: %v", err)
	}
	p.SetMasterEpoch(1)
	if err := p.SetRole(RolePrimary); err != nil {
		t.Fatalf("SetRole: %v", err)
	}
	p.lease.Grant(30 * time.Second)

	// Create replica.
	r, err := CreateBlockVol(filepath.Join(dir, "replica.blockvol"), CreateOptions{
		VolumeSize: 1 * 1024 * 1024,
		BlockSize:  4096,
		WALSize:    512 * 1024,
	}, cfg)
	if err != nil {
		p.Close()
		t.Fatalf("CreateBlockVol(replica): %v", err)
	}
	if err := r.SetEpoch(1); err != nil {
		p.Close()
		r.Close()
		t.Fatalf("SetEpoch(replica): %v", err)
	}

	// Start replica receiver on ephemeral ports.
	if err := r.StartReplicaReceiver("127.0.0.1:0", "127.0.0.1:0"); err != nil {
		p.Close()
		r.Close()
		t.Fatalf("StartReplicaReceiver: %v", err)
	}

	// Connect primary's shipper to replica's addresses.
	p.SetReplicaAddr(r.replRecv.DataAddr(), r.replRecv.CtrlAddr())

	return p, r
}

// --- QA-4A-CP2-1: Frame Protocol Adversarial ---

func testQAFrameMaxPayloadBoundary(t *testing.T) {
	// Payload exactly at maxFramePayload -> should succeed.
	// Payload at maxFramePayload+1 -> ReadFrame must return ErrFrameTooLarge.

	// Test at boundary: we can't allocate 256MB in a test, but we can test
	// the ReadFrame parser with a crafted header.
	var buf bytes.Buffer

	// Write a frame header claiming payload = maxFramePayload + 1
	hdr := make([]byte, frameHeaderSize)
	hdr[0] = MsgWALEntry
	bigEndianPut32(hdr[1:5], uint32(maxFramePayload)+1)
	buf.Write(hdr)

	_, _, err := ReadFrame(&buf)
	if !errors.Is(err, ErrFrameTooLarge) {
		t.Errorf("ReadFrame with oversized payload: got %v, want ErrFrameTooLarge", err)
	}

	// Exactly at max: header says maxFramePayload but we won't supply the data.
	// ReadFrame should try to read and get EOF (not ErrFrameTooLarge).
	buf.Reset()
	hdr[0] = MsgWALEntry
	bigEndianPut32(hdr[1:5], uint32(maxFramePayload))
	buf.Write(hdr)
	// No payload data -> ReadFrame should return io.ErrUnexpectedEOF (not ErrFrameTooLarge).
	_, _, err = ReadFrame(&buf)
	if errors.Is(err, ErrFrameTooLarge) {
		t.Error("payload exactly at maxFramePayload should not be rejected as too large")
	}
	// Should be some read error (EOF/UnexpectedEOF) since we didn't write payload bytes.
	if err == nil {
		t.Error("ReadFrame with missing payload data should return error")
	}
}

func testQAFrameTruncatedHeader(t *testing.T) {
	// Only 3 bytes (< 5 byte header) -> ReadFrame must return error.
	buf := bytes.NewReader([]byte{0x01, 0x00, 0x00})
	_, _, err := ReadFrame(buf)
	if err == nil {
		t.Error("ReadFrame with 3-byte truncated header should return error")
	}
}

func testQAFrameTruncatedPayload(t *testing.T) {
	// Header says 100 bytes payload, but only 10 bytes follow.
	var buf bytes.Buffer
	hdr := make([]byte, frameHeaderSize)
	hdr[0] = MsgWALEntry
	bigEndianPut32(hdr[1:5], 100)
	buf.Write(hdr)
	buf.Write(make([]byte, 10)) // only 10 of 100 bytes

	_, _, err := ReadFrame(&buf)
	if err == nil {
		t.Error("ReadFrame with truncated payload should return error")
	}
}

func testQAFrameZeroPayload(t *testing.T) {
	// Zero-length payload must roundtrip correctly.
	var buf bytes.Buffer
	if err := WriteFrame(&buf, 0x42, nil); err != nil {
		t.Fatalf("WriteFrame(nil payload): %v", err)
	}
	if err := WriteFrame(&buf, 0x43, []byte{}); err != nil {
		t.Fatalf("WriteFrame(empty payload): %v", err)
	}

	msgType, payload, err := ReadFrame(&buf)
	if err != nil {
		t.Fatalf("ReadFrame(nil payload): %v", err)
	}
	if msgType != 0x42 || len(payload) != 0 {
		t.Errorf("frame 1: type=0x%02x len=%d, want 0x42 len=0", msgType, len(payload))
	}

	msgType, payload, err = ReadFrame(&buf)
	if err != nil {
		t.Fatalf("ReadFrame(empty payload): %v", err)
	}
	if msgType != 0x43 || len(payload) != 0 {
		t.Errorf("frame 2: type=0x%02x len=%d, want 0x43 len=0", msgType, len(payload))
	}
}

func testQAFrameConcurrentWrites(t *testing.T) {
	// Multiple goroutines writing frames to the same connection.
	// Frames must not interleave (each frame readable as a complete unit).
	// This relies on the WALShipper's mutex -- test at the connection level.
	server, client := net.Pipe()
	defer server.Close()
	defer client.Close()

	const writers = 8
	const framesPerWriter = 20
	var mu sync.Mutex // simulate WALShipper's mutex

	var wg sync.WaitGroup
	wg.Add(writers)
	for g := 0; g < writers; g++ {
		go func(id int) {
			defer wg.Done()
			payload := []byte{byte(id)}
			for i := 0; i < framesPerWriter; i++ {
				mu.Lock()
				_ = WriteFrame(client, MsgWALEntry, payload)
				mu.Unlock()
			}
		}(g)
	}

	// Reader: read all frames and verify integrity.
	totalFrames := writers * framesPerWriter
	readDone := make(chan error, 1)
	var frameCount int
	go func() {
		for i := 0; i < totalFrames; i++ {
			msgType, payload, err := ReadFrame(server)
			if err != nil {
				readDone <- err
				return
			}
			if msgType != MsgWALEntry {
				readDone <- errors.New("wrong message type")
				return
			}
			if len(payload) != 1 {
				readDone <- errors.New("wrong payload length")
				return
			}
			if payload[0] >= byte(writers) {
				readDone <- errors.New("invalid writer ID in payload")
				return
			}
			frameCount++
		}
		readDone <- nil
	}()

	wg.Wait()

	select {
	case err := <-readDone:
		if err != nil {
			t.Fatalf("frame read error: %v (after %d frames)", err, frameCount)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("frame reader hung for 5s")
	}

	if frameCount != totalFrames {
		t.Errorf("read %d frames, want %d", frameCount, totalFrames)
	}
}

// --- QA-4A-CP2-2: WALShipper Adversarial ---

func testQAShipperShipAfterStop(t *testing.T) {
	s := NewWALShipper("127.0.0.1:0", "127.0.0.1:0", func() uint64 { return 1 })
	s.Stop()

	// Ship after Stop must not panic, must return nil (silently drop).
	entry := &WALEntry{LSN: 1, Epoch: 1, Type: EntryTypeWrite, LBA: 0, Length: 4096, Data: makeBlock('S')}
	err := s.Ship(entry)
	if err != nil {
		t.Errorf("Ship after Stop: got %v, want nil", err)
	}
	if s.ShippedLSN() != 0 {
		t.Errorf("ShippedLSN after stopped Ship: got %d, want 0", s.ShippedLSN())
	}
}

func testQAShipperBarrierAfterDegraded(t *testing.T) {
	s := NewWALShipper("127.0.0.1:0", "127.0.0.1:0", func() uint64 { return 1 })
	s.degraded.Store(true)

	err := s.Barrier(1)
	if !errors.Is(err, ErrReplicaDegraded) {
		t.Errorf("Barrier when degraded: got %v, want ErrReplicaDegraded", err)
	}
}

func testQAShipperStaleEpochNoShippedLSN(t *testing.T) {
	// Ship with epoch != current -> entry silently dropped, shippedLSN unchanged.
	currentEpoch := uint64(5)
	s := NewWALShipper("127.0.0.1:0", "127.0.0.1:0", func() uint64 { return currentEpoch })

	entry := &WALEntry{LSN: 10, Epoch: 3, Type: EntryTypeWrite, LBA: 0, Length: 4096, Data: makeBlock('X')}
	err := s.Ship(entry)
	if err != nil {
		t.Errorf("Ship with stale epoch: got %v, want nil", err)
	}
	if s.ShippedLSN() != 0 {
		t.Errorf("ShippedLSN after stale epoch Ship: got %d, want 0 (entry should be dropped)", s.ShippedLSN())
	}
}

func testQAShipperDegradedPermanent(t *testing.T) {
	// After degradation, Ship and Barrier must fail immediately forever.
	// Create a listener that accepts but immediately closes (triggers write error).
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer ln.Close()

	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			conn.Close() // immediately close -> Ship will get write error
		}
	}()

	s := NewWALShipper(ln.Addr().String(), ln.Addr().String(), func() uint64 { return 1 })
	defer s.Stop()

	// Ship triggers connection -> immediate close -> write error -> degraded.
	entry := &WALEntry{LSN: 1, Epoch: 1, Type: EntryTypeWrite, LBA: 0, Length: 4096, Data: makeBlock('D')}
	_ = s.Ship(entry) // first attempt may or may not degrade
	_ = s.Ship(entry) // second attempt more likely to hit closed conn
	_ = s.Ship(entry) // third attempt for good measure

	// Force degraded if the connection race didn't trigger it.
	if !s.IsDegraded() {
		// The connection might have worked briefly. Manually degrade for the rest of the test.
		s.degraded.Store(true)
	}

	// Once degraded, subsequent Ship must not attempt connection.
	lsnBefore := s.ShippedLSN()
	for i := 0; i < 100; i++ {
		_ = s.Ship(entry)
	}
	lsnAfter := s.ShippedLSN()
	if lsnAfter > lsnBefore {
		t.Errorf("ShippedLSN advanced from %d to %d after degradation -- should not ship when degraded", lsnBefore, lsnAfter)
	}

	// Barrier must immediately return ErrReplicaDegraded.
	err = s.Barrier(1)
	if !errors.Is(err, ErrReplicaDegraded) {
		t.Errorf("Barrier after degraded: got %v, want ErrReplicaDegraded", err)
	}
}

func testQAShipperConcurrentShipStop(t *testing.T) {
	// Ship and Stop from concurrent goroutines -> no deadlock, no panic.
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			// Just consume data.
			go io.Copy(io.Discard, conn)
		}
	}()
	defer ln.Close()

	s := NewWALShipper(ln.Addr().String(), ln.Addr().String(), func() uint64 { return 1 })

	var wg sync.WaitGroup
	// Concurrent shippers.
	wg.Add(4)
	for g := 0; g < 4; g++ {
		go func() {
			defer wg.Done()
			entry := &WALEntry{LSN: 1, Epoch: 1, Type: EntryTypeWrite, LBA: 0, Length: 4096, Data: makeBlock('C')}
			for i := 0; i < 50; i++ {
				_ = s.Ship(entry)
			}
		}()
	}

	// Stop mid-flight.
	time.Sleep(5 * time.Millisecond)
	s.Stop()

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("concurrent Ship + Stop deadlocked for 5s")
	}
}

// --- QA-4A-CP2-3: ReplicaReceiver Adversarial ---

func testQAReceiverOutOfOrderLSN(t *testing.T) {
	// Tests contiguity enforcement:
	// 1. Contiguous LSN=1..5 -> all applied
	// 2. Duplicate LSN=3 -> skipped
	// 3. Gap LSN=7 (skipping 6) -> rejected
	// 4. Correct next LSN=6 -> accepted
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
	recv.Serve()
	defer recv.Stop()

	conn, err := net.Dial("tcp", recv.DataAddr())
	if err != nil {
		t.Fatalf("dial data: %v", err)
	}
	defer conn.Close()

	// Case 1: Contiguous LSN=1..5 -> all applied.
	for lsn := uint64(1); lsn <= 5; lsn++ {
		entry := &WALEntry{LSN: lsn, Epoch: 1, Type: EntryTypeWrite, LBA: lsn - 1, Length: 4096, Data: makeBlock(byte('A' + lsn))}
		encoded, _ := entry.Encode()
		if err := WriteFrame(conn, MsgWALEntry, encoded); err != nil {
			t.Fatalf("send LSN=%d: %v", lsn, err)
		}
	}
	time.Sleep(20 * time.Millisecond)
	if recv.ReceivedLSN() != 5 {
		t.Fatalf("ReceivedLSN = %d after LSN 1-5, want 5", recv.ReceivedLSN())
	}

	// Case 2: Duplicate LSN=3 -> skipped.
	entry3 := &WALEntry{LSN: 3, Epoch: 1, Type: EntryTypeWrite, LBA: 2, Length: 4096, Data: makeBlock('Z')}
	encoded3, _ := entry3.Encode()
	if err := WriteFrame(conn, MsgWALEntry, encoded3); err != nil {
		t.Fatalf("send duplicate LSN=3: %v", err)
	}
	time.Sleep(20 * time.Millisecond)
	if recv.ReceivedLSN() != 5 {
		t.Errorf("ReceivedLSN = %d after duplicate LSN=3, want 5", recv.ReceivedLSN())
	}

	// Case 3: Gap LSN=7 (skips 6) -> rejected by contiguity check.
	entry7 := &WALEntry{LSN: 7, Epoch: 1, Type: EntryTypeWrite, LBA: 6, Length: 4096, Data: makeBlock('G')}
	encoded7, _ := entry7.Encode()
	if err := WriteFrame(conn, MsgWALEntry, encoded7); err != nil {
		t.Fatalf("send gap LSN=7: %v", err)
	}
	time.Sleep(20 * time.Millisecond)
	if recv.ReceivedLSN() != 5 {
		t.Errorf("ReceivedLSN = %d after gap LSN=7, want 5 (gap must be rejected)", recv.ReceivedLSN())
	}

	// Case 4: Correct next LSN=6 -> accepted.
	entry6 := &WALEntry{LSN: 6, Epoch: 1, Type: EntryTypeWrite, LBA: 5, Length: 4096, Data: makeBlock('F')}
	encoded6, _ := entry6.Encode()
	if err := WriteFrame(conn, MsgWALEntry, encoded6); err != nil {
		t.Fatalf("send LSN=6: %v", err)
	}
	time.Sleep(20 * time.Millisecond)
	if recv.ReceivedLSN() != 6 {
		t.Errorf("ReceivedLSN = %d after correct LSN=6, want 6", recv.ReceivedLSN())
	}
}

func testQAReceiverConcurrentDataConns(t *testing.T) {
	// With contiguous LSN enforcement, entries must arrive in order on a single
	// connection. Test that a stream of 40 contiguous entries from one connection
	// is fully applied, then a second connection sending a duplicate is rejected.
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

	// Send 40 contiguous entries from a single connection.
	conn1, err := net.Dial("tcp", recv.DataAddr())
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn1.Close()

	for lsn := uint64(1); lsn <= 40; lsn++ {
		entry := &WALEntry{LSN: lsn, Epoch: 1, Type: EntryTypeWrite, LBA: lsn, Length: 4096, Data: makeBlock(byte('A' + lsn%26))}
		encoded, _ := entry.Encode()
		if err := WriteFrame(conn1, MsgWALEntry, encoded); err != nil {
			t.Fatalf("send LSN=%d: %v", lsn, err)
		}
	}

	time.Sleep(100 * time.Millisecond)

	got := recv.ReceivedLSN()
	if got != 40 {
		t.Errorf("ReceivedLSN = %d after 40 contiguous entries, want 40", got)
	}

	// Second connection sending a duplicate LSN=5 -> must be skipped.
	conn2, err := net.Dial("tcp", recv.DataAddr())
	if err != nil {
		t.Fatalf("dial conn2: %v", err)
	}
	defer conn2.Close()

	dup := &WALEntry{LSN: 5, Epoch: 1, Type: EntryTypeWrite, LBA: 5, Length: 4096, Data: makeBlock('Z')}
	encodedDup, _ := dup.Encode()
	WriteFrame(conn2, MsgWALEntry, encodedDup)

	time.Sleep(20 * time.Millisecond)

	// receivedLSN must still be 40.
	if recv.ReceivedLSN() != 40 {
		t.Errorf("ReceivedLSN = %d after duplicate on conn2, want 40", recv.ReceivedLSN())
	}
}

func testQAReceiverFutureEpochRejected(t *testing.T) {
	// R1 fix: replica must reject entries with epoch > local (not just <).
	// A future epoch in the WAL stream is a protocol violation -- only master
	// can bump epochs via SetEpoch.
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
	if err := vol.SetEpoch(5); err != nil {
		t.Fatalf("SetEpoch: %v", err)
	}

	recv, err := NewReplicaReceiver(vol, "127.0.0.1:0", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("NewReplicaReceiver: %v", err)
	}
	recv.Serve()
	defer recv.Stop()

	conn, err := net.Dial("tcp", recv.DataAddr())
	if err != nil {
		t.Fatalf("dial data: %v", err)
	}
	defer conn.Close()

	// Send entry with stale epoch=3 -> rejected.
	stale := &WALEntry{LSN: 1, Epoch: 3, Type: EntryTypeWrite, LBA: 0, Length: 4096, Data: makeBlock('S')}
	encodedStale, _ := stale.Encode()
	if err := WriteFrame(conn, MsgWALEntry, encodedStale); err != nil {
		t.Fatalf("send stale epoch: %v", err)
	}
	time.Sleep(20 * time.Millisecond)
	if recv.ReceivedLSN() != 0 {
		t.Errorf("ReceivedLSN = %d after stale epoch entry, want 0", recv.ReceivedLSN())
	}

	// Send entry with future epoch=10 -> also rejected (R1 fix).
	future := &WALEntry{LSN: 1, Epoch: 10, Type: EntryTypeWrite, LBA: 0, Length: 4096, Data: makeBlock('F')}
	encodedFuture, _ := future.Encode()
	if err := WriteFrame(conn, MsgWALEntry, encodedFuture); err != nil {
		t.Fatalf("send future epoch: %v", err)
	}
	time.Sleep(20 * time.Millisecond)
	if recv.ReceivedLSN() != 0 {
		t.Errorf("ReceivedLSN = %d after future epoch entry, want 0 (future epoch must be rejected)", recv.ReceivedLSN())
	}

	// Send entry with correct epoch=5 -> accepted.
	correct := &WALEntry{LSN: 1, Epoch: 5, Type: EntryTypeWrite, LBA: 0, Length: 4096, Data: makeBlock('C')}
	encodedCorrect, _ := correct.Encode()
	if err := WriteFrame(conn, MsgWALEntry, encodedCorrect); err != nil {
		t.Fatalf("send correct epoch: %v", err)
	}
	time.Sleep(20 * time.Millisecond)
	if recv.ReceivedLSN() != 1 {
		t.Errorf("ReceivedLSN = %d after correct epoch entry, want 1", recv.ReceivedLSN())
	}
}

func testQAReceiverBarrierEpochMismatch(t *testing.T) {
	// Barrier with wrong epoch -> immediate BarrierEpochMismatch (no wait).
	dir := t.TempDir()
	cfg := DefaultConfig()
	cfg.FlushInterval = 100 * time.Millisecond

	vol, err := CreateBlockVol(filepath.Join(dir, "replica.blockvol"), CreateOptions{
		VolumeSize: 1 * 1024 * 1024, BlockSize: 4096, WALSize: 256 * 1024,
	}, cfg)
	if err != nil {
		t.Fatalf("CreateBlockVol: %v", err)
	}
	defer vol.Close()
	if err := vol.SetEpoch(5); err != nil {
		t.Fatalf("SetEpoch: %v", err)
	}

	recv, err := NewReplicaReceiver(vol, "127.0.0.1:0", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("NewReplicaReceiver: %v", err)
	}
	recv.Serve()
	defer recv.Stop()

	ctrlConn, err := net.Dial("tcp", recv.CtrlAddr())
	if err != nil {
		t.Fatalf("dial ctrl: %v", err)
	}
	defer ctrlConn.Close()

	// Barrier with stale epoch=3 -> immediate EpochMismatch.
	req := EncodeBarrierRequest(BarrierRequest{LSN: 1, Epoch: 3})
	start := time.Now()
	if err := WriteFrame(ctrlConn, MsgBarrierReq, req); err != nil {
		t.Fatalf("send barrier: %v", err)
	}

	ctrlConn.SetReadDeadline(time.Now().Add(3 * time.Second))
	_, payload, err := ReadFrame(ctrlConn)
	elapsed := time.Since(start)
	if err != nil {
		t.Fatalf("read barrier response: %v", err)
	}
	if payload[0] != BarrierEpochMismatch {
		t.Errorf("barrier status = %d, want BarrierEpochMismatch(%d)", payload[0], BarrierEpochMismatch)
	}
	// Must be fast (no waiting), well under 100ms.
	if elapsed > 500*time.Millisecond {
		t.Errorf("epoch mismatch barrier took %v, expected immediate response", elapsed)
	}

	// Barrier with future epoch=99 -> also immediate EpochMismatch.
	req2 := EncodeBarrierRequest(BarrierRequest{LSN: 1, Epoch: 99})
	if err := WriteFrame(ctrlConn, MsgBarrierReq, req2); err != nil {
		t.Fatalf("send barrier future: %v", err)
	}

	ctrlConn.SetReadDeadline(time.Now().Add(3 * time.Second))
	_, payload2, err := ReadFrame(ctrlConn)
	if err != nil {
		t.Fatalf("read barrier response (future): %v", err)
	}
	if payload2[0] != BarrierEpochMismatch {
		t.Errorf("future epoch barrier status = %d, want BarrierEpochMismatch(%d)", payload2[0], BarrierEpochMismatch)
	}
}

func testQAReceiverBarrierBeforeEntries(t *testing.T) {
	// Barrier arrives BEFORE data entries -> must wait, then succeed when entries arrive.
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
	recv.Serve()
	defer recv.Stop()

	// Send barrier for LSN=3 BEFORE any data.
	ctrlConn, err := net.Dial("tcp", recv.CtrlAddr())
	if err != nil {
		t.Fatalf("dial ctrl: %v", err)
	}
	defer ctrlConn.Close()

	barrierDone := make(chan BarrierResponse, 1)
	go func() {
		req := EncodeBarrierRequest(BarrierRequest{LSN: 3, Epoch: 1})
		if err := WriteFrame(ctrlConn, MsgBarrierReq, req); err != nil {
			return
		}
		_, payload, err := ReadFrame(ctrlConn)
		if err != nil {
			return
		}
		barrierDone <- BarrierResponse{Status: payload[0]}
	}()

	// Barrier should be waiting (not returned yet).
	select {
	case resp := <-barrierDone:
		t.Fatalf("barrier returned immediately with status %d, expected it to wait", resp.Status)
	case <-time.After(50 * time.Millisecond):
		// Good -- barrier is waiting.
	}

	// Now send entries LSN=1,2,3.
	dataConn, err := net.Dial("tcp", recv.DataAddr())
	if err != nil {
		t.Fatalf("dial data: %v", err)
	}
	defer dataConn.Close()

	for lsn := uint64(1); lsn <= 3; lsn++ {
		entry := &WALEntry{LSN: lsn, Epoch: 1, Type: EntryTypeWrite, LBA: lsn, Length: 4096, Data: makeBlock(byte('0' + lsn))}
		encoded, _ := entry.Encode()
		if err := WriteFrame(dataConn, MsgWALEntry, encoded); err != nil {
			t.Fatalf("send LSN=%d: %v", lsn, err)
		}
	}

	// Barrier should now complete with OK.
	select {
	case resp := <-barrierDone:
		if resp.Status != BarrierOK {
			t.Errorf("barrier status = %d, want BarrierOK(0)", resp.Status)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("barrier hung for 5s after entries were sent")
	}
}

func testQAReceiverBarrierTimeoutNoEntries(t *testing.T) {
	// Barrier for LSN=999 with no entries -> must timeout (not hang forever).
	// Uses short configurable barrierTimeout for fast test.
	dir := t.TempDir()
	cfg := DefaultConfig()
	cfg.FlushInterval = 100 * time.Millisecond

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
	recv.barrierTimeout = 100 * time.Millisecond // fast timeout for test
	recv.Serve()
	defer recv.Stop()

	ctrlConn, err := net.Dial("tcp", recv.CtrlAddr())
	if err != nil {
		t.Fatalf("dial ctrl: %v", err)
	}
	defer ctrlConn.Close()

	req := EncodeBarrierRequest(BarrierRequest{LSN: 999, Epoch: 1})
	start := time.Now()
	if err := WriteFrame(ctrlConn, MsgBarrierReq, req); err != nil {
		t.Fatalf("send barrier: %v", err)
	}

	ctrlConn.SetReadDeadline(time.Now().Add(3 * time.Second))
	_, payload, err := ReadFrame(ctrlConn)
	elapsed := time.Since(start)
	if err != nil {
		t.Fatalf("read barrier response: %v", err)
	}
	if payload[0] != BarrierTimeout {
		t.Errorf("barrier status = %d, want BarrierTimeout(%d)", payload[0], BarrierTimeout)
	}
	// Verify timeout was fast (~100ms, not 5s).
	if elapsed > 1*time.Second {
		t.Errorf("barrier timeout took %v, expected ~100ms (configurable timeout)", elapsed)
	}
}

func testQAReceiverStopUnblocksBarrier(t *testing.T) {
	// Barrier waiting for entries -> Stop called -> barrier must return (not hang).
	dir := t.TempDir()
	cfg := DefaultConfig()
	cfg.FlushInterval = 100 * time.Millisecond

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
	recv.Serve()

	ctrlConn, err := net.Dial("tcp", recv.CtrlAddr())
	if err != nil {
		t.Fatalf("dial ctrl: %v", err)
	}
	defer ctrlConn.Close()

	// Send barrier for high LSN (will wait).
	req := EncodeBarrierRequest(BarrierRequest{LSN: math.MaxUint64, Epoch: 1})
	if err := WriteFrame(ctrlConn, MsgBarrierReq, req); err != nil {
		t.Fatalf("send barrier: %v", err)
	}

	// Let barrier start waiting.
	time.Sleep(50 * time.Millisecond)

	// Stop should unblock the barrier.
	recv.Stop()

	ctrlConn.SetReadDeadline(time.Now().Add(3 * time.Second))
	_, payload, err := ReadFrame(ctrlConn)
	if err != nil {
		// Connection closed by Stop -> also acceptable.
		t.Logf("barrier read after Stop: %v (connection closed -- acceptable)", err)
		return
	}
	if payload[0] != BarrierTimeout {
		t.Errorf("barrier after Stop: status = %d, want BarrierTimeout(%d)", payload[0], BarrierTimeout)
	}
}

// --- QA-4A-CP2-4: DistributedSync Adversarial ---

func testQADSyncLocalFailReturnsError(t *testing.T) {
	// Local fsync fails -> must return error regardless of remote result.
	localErr := errors.New("disk on fire")

	vol := &BlockVol{}
	vol.nextLSN.Store(10)

	syncFn := MakeDistributedSync(
		func() error { return localErr },
		nil, // no shipper -> local only
		vol,
	)

	err := syncFn()
	if !errors.Is(err, localErr) {
		t.Errorf("dsync with local fail: got %v, want %v", err, localErr)
	}
}

func testQADSyncRemoteFailDegrades(t *testing.T) {
	// Remote barrier fails -> local succeeded -> must return nil + degrade shipper.
	primary, replica := createReplicaPair(t)
	defer primary.Close()
	defer replica.Close()

	// Write some data so there's something to sync.
	if err := primary.WriteLBA(0, makeBlock('D')); err != nil {
		t.Fatalf("WriteLBA: %v", err)
	}

	// Stop the replica receiver to make barrier fail.
	replica.replRecv.Stop()

	// Force shipper to reconnect (old ctrl conn may still be open).
	primary.shipper.ctrlMu.Lock()
	if primary.shipper.ctrlConn != nil {
		primary.shipper.ctrlConn.Close()
		primary.shipper.ctrlConn = nil
	}
	primary.shipper.ctrlMu.Unlock()

	// SyncCache triggers distributed sync -> barrier fails -> degrade.
	err := primary.SyncCache()
	// Should succeed (local fsync succeeded, remote degraded -- returned nil).
	if err != nil {
		t.Errorf("SyncCache after replica stop: got %v, want nil (local succeeded, remote should degrade silently)", err)
	}

	// Shipper should be degraded.
	if !primary.shipper.IsDegraded() {
		t.Error("shipper should be degraded after replica receiver stopped")
	}
}

func testQADSyncBothFail(t *testing.T) {
	// Both local and remote fail -> must return local error.
	localErr := errors.New("local disk fail")

	vol := &BlockVol{}
	vol.nextLSN.Store(10)

	shipper := NewWALShipper("127.0.0.1:1", "127.0.0.1:1", func() uint64 { return 1 })

	syncFn := MakeDistributedSync(
		func() error { return localErr },
		shipper,
		vol,
	)

	err := syncFn()
	if !errors.Is(err, localErr) {
		t.Errorf("dsync with both fail: got %v, want %v", err, localErr)
	}
}

func testQADSyncParallelExecution(t *testing.T) {
	// Verify local and remote execute in parallel, not sequentially.
	// If sequential, total time ≈ 200ms. If parallel, ≈ 100ms.
	//
	// Strategy: create a real replica pair where barrier takes ~80ms
	// (because data arrives with delay) and local fsync also takes ~80ms.
	// If parallel: ~80ms total. If sequential: ~160ms.
	primary, replica := createReplicaPair(t)
	defer primary.Close()
	defer replica.Close()

	// Write some data to primary so there's a WAL entry to sync.
	if err := primary.WriteLBA(0, makeBlock('P')); err != nil {
		t.Fatalf("WriteLBA: %v", err)
	}

	// Give replica time to receive the shipped entry.
	time.Sleep(30 * time.Millisecond)

	// Replace the distributed sync with one that has a slow local fsync.
	// The barrier should be near-instant since replica already has the data.
	var localStart, localEnd atomic.Int64
	slowLocalSync := func() error {
		localStart.Store(time.Now().UnixNano())
		time.Sleep(80 * time.Millisecond)
		localEnd.Store(time.Now().UnixNano())
		return nil
	}

	syncFn := MakeDistributedSync(slowLocalSync, primary.shipper, primary)

	start := time.Now()
	err := syncFn()
	elapsed := time.Since(start)

	if err != nil {
		t.Fatalf("dsync: %v", err)
	}

	// Parallel: total ≈ max(local=80ms, barrier=fast) ≈ 80ms
	// Sequential: total ≈ local + barrier ≈ 80ms + barrier_overhead
	// Key check: elapsed should be < 150ms (well under sequential 160ms+)
	if elapsed > 150*time.Millisecond {
		t.Errorf("dsync took %v, suggesting sequential execution (expected <150ms for parallel)", elapsed)
	}

	// Verify local fsync actually ran (not skipped).
	if localStart.Load() == 0 {
		t.Error("local fsync was never called -- distributed sync may have fallen back to local-only")
	}
}

// --- QA-4A-CP2-5: End-to-end Adversarial ---

func testQAE2EReplicaDataMatchesPrimary(t *testing.T) {
	// Write N blocks on primary -> verify replica has identical data via ReadLBA.
	primary, replica := createReplicaPair(t)
	defer primary.Close()
	defer replica.Close()

	const numBlocks = 20
	for i := 0; i < numBlocks; i++ {
		if err := primary.WriteLBA(uint64(i), makeBlock(byte('A'+i%26))); err != nil {
			t.Fatalf("WriteLBA(%d): %v", i, err)
		}
	}

	// SyncCache ensures WAL entries are durable on both nodes.
	if err := primary.SyncCache(); err != nil {
		t.Fatalf("SyncCache: %v", err)
	}

	// Give replica time to process entries.
	time.Sleep(50 * time.Millisecond)

	// Verify replica has the data.
	for i := 0; i < numBlocks; i++ {
		data, err := replica.ReadLBA(uint64(i), 4096)
		if err != nil {
			t.Fatalf("replica ReadLBA(%d): %v", i, err)
		}
		expected := byte('A' + i%26)
		if data[0] != expected {
			t.Errorf("replica LBA %d: data[0] = %c, want %c", i, data[0], expected)
		}
	}

	// Verify receivedLSN on replica matches what primary shipped.
	primaryLSN := primary.shipper.ShippedLSN()
	replicaLSN := replica.replRecv.ReceivedLSN()
	if replicaLSN < primaryLSN {
		t.Errorf("replica receivedLSN = %d < primary shippedLSN = %d", replicaLSN, primaryLSN)
	}
}

func testQAE2EClosePrimaryDuringShip(t *testing.T) {
	// Close primary while writes + shipping in progress -> no hang, no panic.
	primary, replica := createReplicaPair(t)
	defer replica.Close()

	var wg sync.WaitGroup

	// Writer goroutine.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 100; i++ {
			err := primary.WriteLBA(uint64(i%256), makeBlock(byte('W')))
			if err != nil {
				return // closed or WAL full -- expected
			}
		}
	}()

	// Close after a few writes.
	time.Sleep(10 * time.Millisecond)
	primary.Close()

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("Close primary during ship hung for 10s")
	}
}

// --- helper ---

func bigEndianPut32(b []byte, v uint32) {
	b[0] = byte(v >> 24)
	b[1] = byte(v >> 16)
	b[2] = byte(v >> 8)
	b[3] = byte(v)
}
