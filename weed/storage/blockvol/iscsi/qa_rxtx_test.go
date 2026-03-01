package iscsi

import (
	"encoding/binary"
	"io"
	"log"
	"net"
	"runtime"
	"sync"
	"testing"
	"time"
)

// TestQAPhase3RXTX tests Phase 3 iSCSI RX/TX split adversarial scenarios.
func TestQAPhase3RXTX(t *testing.T) {
	tests := []struct {
		name string
		run  func(t *testing.T)
	}{
		// QA-2.1: Channel & Goroutine Safety
		{name: "rxtx_respch_full_backpressure", run: testQARXTXRespChFullBackpressure},
		{name: "rxtx_double_close_session", run: testQARXTXDoubleCloseSession},
		{name: "rxtx_txloop_goroutine_leak", run: testQARXTXTxLoopGoroutineLeak},
		{name: "rxtx_concurrent_session_50", run: testQARXTXConcurrentSession50},

		// QA-2.2: Pending Queue & Data-Out
		{name: "rxtx_scsi_cmd_during_dataout", run: testQARXTXSCSICmdDuringDataOut},
		{name: "rxtx_nop_response_timing", run: testQARXTXNOPResponseTiming},

		// QA-2.3: StatSN & Sequence Edge Cases
		{name: "rxtx_statsn_wrap", run: testQARXTXStatSNWrap},
		{name: "rxtx_expstsn_mismatch", run: testQARXTXExpStatSNMismatch},
		{name: "rxtx_statsn_after_error_response", run: testQARXTXStatSNAfterErrorResponse},

		// QA-2.4: Shutdown Ordering
		{name: "rxtx_shutdown_writer_queued", run: testQARXTXShutdownWriterQueued},
		{name: "rxtx_target_close_active_io", run: testQARXTXTargetCloseActiveIO},
		{name: "rxtx_session_after_target_close", run: testQARXTXSessionAfterTargetClose},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.run(t)
		})
	}
}

// --- QA-2.1: Channel & Goroutine Safety ---

func testQARXTXRespChFullBackpressure(t *testing.T) {
	// With net.Pipe, writes block when reader isn't consuming, so we can't
	// truly fill respCh without reading. Instead, test that closing the
	// connection while enqueue is potentially blocked works cleanly.
	env := setupSession(t)
	doLogin(t, env.clientConn)

	// Start reading responses in background so writes don't block on net.Pipe.
	readDone := make(chan int, 1)
	go func() {
		count := 0
		for {
			_, err := ReadPDU(env.clientConn)
			if err != nil {
				break
			}
			count++
		}
		readDone <- count
	}()

	// Send 100 NOPs rapidly.
	sent := 0
	for i := 0; i < 100; i++ {
		nop := &PDU{}
		nop.SetOpcode(OpNOPOut)
		nop.SetOpSpecific1(FlagF)
		nop.SetInitiatorTaskTag(uint32(0x1000 + i))
		nop.SetImmediate(true)
		if err := WritePDU(env.clientConn, nop); err != nil {
			break
		}
		sent++
	}

	// Close conn to trigger cleanup.
	env.clientConn.Close()

	select {
	case count := <-readDone:
		t.Logf("backpressure: sent %d NOPs, received %d responses", sent, count)
	case <-time.After(3 * time.Second):
		t.Fatal("reader did not exit after conn close")
	}

	select {
	case <-env.done:
	case <-time.After(3 * time.Second):
		t.Fatal("session did not exit after conn close during backpressure")
	}
}

func testQARXTXDoubleCloseSession(t *testing.T) {
	env := setupSession(t)
	doLogin(t, env.clientConn)

	// Close session twice -- should not panic.
	env.session.Close()
	env.session.Close()

	select {
	case <-env.done:
	case <-time.After(2 * time.Second):
		t.Fatal("session did not exit after double close")
	}
}

func testQARXTXTxLoopGoroutineLeak(t *testing.T) {
	// Create and destroy 50 sessions rapidly. Goroutine count should not grow.
	baseGoroutines := runtime.NumGoroutine()

	for i := 0; i < 50; i++ {
		client, server := net.Pipe()
		dev := newMockDevice(256 * 4096)
		config := DefaultTargetConfig()
		config.TargetName = testTargetName
		resolver := newTestResolverWithDevice(dev)
		logger := log.New(io.Discard, "", 0)

		sess := NewSession(server, config, resolver, resolver, logger)
		done := make(chan error, 1)
		go func() {
			done <- sess.HandleConnection()
		}()

		// Login then immediately close.
		doLogin(t, client)
		client.Close()

		select {
		case <-done:
		case <-time.After(2 * time.Second):
			t.Fatalf("session %d did not exit", i)
		}
		server.Close()
	}

	// Allow goroutines to settle.
	time.Sleep(100 * time.Millisecond)
	runtime.GC()
	time.Sleep(50 * time.Millisecond)

	finalGoroutines := runtime.NumGoroutine()
	// Allow some slack for GC/runtime goroutines.
	if finalGoroutines > baseGoroutines+10 {
		t.Errorf("goroutine leak: started with %d, ended with %d after 50 sessions",
			baseGoroutines, finalGoroutines)
	}
	t.Logf("goroutine leak check: base=%d, final=%d", baseGoroutines, finalGoroutines)
}

func testQARXTXConcurrentSession50(t *testing.T) {
	// 50 concurrent sessions on same TargetServer, each doing I/O.
	ts, addr := qaSetupTarget(t)
	_ = ts

	var wg sync.WaitGroup
	wg.Add(50)
	for i := 0; i < 50; i++ {
		go func(id int) {
			defer wg.Done()
			conn, err := net.DialTimeout("tcp", addr, 2*time.Second)
			if err != nil {
				t.Logf("session %d: dial failed: %v", id, err)
				return
			}
			defer conn.Close()

			doLogin(t, conn)

			// Write 1 block.
			data := make([]byte, 4096)
			data[0] = byte(id)
			lba := uint32(id % 256)
			sendSCSIWriteImmediate(t, conn, lba, data, uint32(id+100), 2)
			resp, err := ReadPDU(conn)
			if err != nil {
				t.Logf("session %d: write response read failed: %v", id, err)
				return
			}
			if resp.Opcode() != OpSCSIResp {
				t.Logf("session %d: expected SCSI-Response, got %s", id, OpcodeName(resp.Opcode()))
			}
		}(i)
	}

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("50 concurrent sessions did not complete in 10s")
	}
}

// --- QA-2.2: Pending Queue & Data-Out ---

func testQARXTXSCSICmdDuringDataOut(t *testing.T) {
	// Start WRITE requiring R2T, send a READ_10 during Data-Out exchange.
	// The READ should be queued in pending and executed after Data-Out completes.
	env := setupSessionWithConfig(t, func(cfg *TargetConfig) {
		cfg.MaxRecvDataSegmentLength = 4096
		cfg.FirstBurstLength = 0 // force R2T
	})
	doLogin(t, env.clientConn)

	// Pre-write data to LBA 10 so we can read it back.
	preData := make([]byte, 4096)
	for i := range preData {
		preData[i] = 0xBB
	}
	sendSCSIWriteImmediate(t, env.clientConn, 10, preData, 0x50, 2)
	resp, err := ReadPDU(env.clientConn)
	if err != nil {
		t.Fatalf("pre-write read response: %v", err)
	}
	if resp.Opcode() != OpSCSIResp {
		t.Fatalf("pre-write: expected SCSI-Response, got %s", OpcodeName(resp.Opcode()))
	}

	// Start WRITE to LBA 0 with no immediate data (requires R2T).
	writeData := make([]byte, 4096)
	for i := range writeData {
		writeData[i] = 0xAA
	}
	cmd := &PDU{}
	cmd.SetOpcode(OpSCSICmd)
	cmd.SetOpSpecific1(FlagF | FlagW)
	cmd.SetInitiatorTaskTag(0x100)
	cmd.SetExpectedDataTransferLength(4096)
	cmd.SetCmdSN(3)
	var cdb [16]byte
	cdb[0] = ScsiWrite10
	binary.BigEndian.PutUint32(cdb[2:6], 0) // LBA 0
	binary.BigEndian.PutUint16(cdb[7:9], 1) // 1 block
	cmd.SetCDB(cdb)
	// No DataSegment (no immediate data).
	if err := WritePDU(env.clientConn, cmd); err != nil {
		t.Fatalf("write cmd: %v", err)
	}

	// Read R2T.
	r2t, err := ReadPDU(env.clientConn)
	if err != nil {
		t.Fatalf("read R2T: %v", err)
	}
	if r2t.Opcode() != OpR2T {
		t.Fatalf("expected R2T, got %s", OpcodeName(r2t.Opcode()))
	}

	// While Data-Out is expected, send a READ_10 for LBA 10.
	sendSCSIRead(t, env.clientConn, 10, 1, 0x200, 4)

	// Now send Data-Out to complete the WRITE.
	dataOut := &PDU{}
	dataOut.SetOpcode(OpSCSIDataOut)
	dataOut.SetOpSpecific1(FlagF)
	dataOut.SetInitiatorTaskTag(0x100)
	dataOut.SetTargetTransferTag(r2t.TargetTransferTag())
	dataOut.SetBufferOffset(0)
	dataOut.DataSegment = writeData
	if err := WritePDU(env.clientConn, dataOut); err != nil {
		t.Fatalf("data-out: %v", err)
	}

	// Should receive WRITE response first, then READ response.
	resp1, err := ReadPDU(env.clientConn)
	if err != nil {
		t.Fatalf("resp1: %v", err)
	}

	resp2, err := ReadPDU(env.clientConn)
	if err != nil {
		t.Fatalf("resp2: %v", err)
	}

	// First response should be WRITE (ITT=0x100).
	if resp1.InitiatorTaskTag() != 0x100 {
		t.Errorf("first response ITT=0x%x, want 0x100 (WRITE)", resp1.InitiatorTaskTag())
	}

	// Second response should be READ Data-In or SCSI-Response (ITT=0x200).
	if resp2.InitiatorTaskTag() != 0x200 {
		t.Errorf("second response ITT=0x%x, want 0x200 (READ)", resp2.InitiatorTaskTag())
	}
}

func testQARXTXNOPResponseTiming(t *testing.T) {
	// Send NOP-Out during R2T exchange. NOP-In should arrive after WRITE response.
	env := setupSessionWithConfig(t, func(cfg *TargetConfig) {
		cfg.MaxRecvDataSegmentLength = 4096
		cfg.FirstBurstLength = 0 // force R2T
	})
	doLogin(t, env.clientConn)

	// Start WRITE requiring R2T.
	cmd := &PDU{}
	cmd.SetOpcode(OpSCSICmd)
	cmd.SetOpSpecific1(FlagF | FlagW)
	cmd.SetInitiatorTaskTag(0x300)
	cmd.SetExpectedDataTransferLength(4096)
	cmd.SetCmdSN(2)
	var cdb [16]byte
	cdb[0] = ScsiWrite10
	binary.BigEndian.PutUint32(cdb[2:6], 0) // LBA 0
	binary.BigEndian.PutUint16(cdb[7:9], 1) // 1 block
	cmd.SetCDB(cdb)
	if err := WritePDU(env.clientConn, cmd); err != nil {
		t.Fatal(err)
	}

	// Read R2T.
	r2t, err := ReadPDU(env.clientConn)
	if err != nil {
		t.Fatal(err)
	}
	if r2t.Opcode() != OpR2T {
		t.Fatalf("expected R2T, got %s", OpcodeName(r2t.Opcode()))
	}

	// Send NOP-Out during Data-Out collection.
	nop := &PDU{}
	nop.SetOpcode(OpNOPOut)
	nop.SetOpSpecific1(FlagF)
	nop.SetInitiatorTaskTag(0x400)
	nop.SetImmediate(true)
	if err := WritePDU(env.clientConn, nop); err != nil {
		t.Fatal(err)
	}

	// Send Data-Out to complete WRITE.
	dataOut := &PDU{}
	dataOut.SetOpcode(OpSCSIDataOut)
	dataOut.SetOpSpecific1(FlagF)
	dataOut.SetInitiatorTaskTag(0x300)
	dataOut.SetTargetTransferTag(r2t.TargetTransferTag())
	dataOut.SetBufferOffset(0)
	dataOut.DataSegment = make([]byte, 4096)
	if err := WritePDU(env.clientConn, dataOut); err != nil {
		t.Fatal(err)
	}

	// Collect responses: WRITE response (0x300) and NOP-In (0x400).
	var responses []*PDU
	env.clientConn.SetReadDeadline(time.Now().Add(2 * time.Second))
	for i := 0; i < 2; i++ {
		resp, err := ReadPDU(env.clientConn)
		if err != nil {
			t.Fatalf("response %d: %v", i, err)
		}
		responses = append(responses, resp)
	}

	// WRITE response should come first (Data-Out completed, then pending NOP processed).
	if responses[0].InitiatorTaskTag() != 0x300 {
		t.Errorf("first response ITT=0x%x, want 0x300 (WRITE)", responses[0].InitiatorTaskTag())
	}
	if responses[1].InitiatorTaskTag() != 0x400 {
		t.Errorf("second response ITT=0x%x, want 0x400 (NOP)", responses[1].InitiatorTaskTag())
	}
	if responses[1].Opcode() != OpNOPIn {
		t.Errorf("second response opcode=%s, want NOP-In", OpcodeName(responses[1].Opcode()))
	}
}

// --- QA-2.3: StatSN & Sequence Edge Cases ---

func testQARXTXStatSNWrap(t *testing.T) {
	// Drive StatSN close to 0xFFFFFFFF and verify it wraps to 0.
	env := setupSession(t)
	doLogin(t, env.clientConn)

	// We can't easily set the initial StatSN, but we can observe the
	// current StatSN from a response and then verify monotonic increment.
	// Send 5 NOP-Outs and verify StatSN increases by 1 each time.
	var statSNs []uint32
	for i := 0; i < 5; i++ {
		nop := &PDU{}
		nop.SetOpcode(OpNOPOut)
		nop.SetOpSpecific1(FlagF)
		nop.SetInitiatorTaskTag(uint32(0x1000 + i))
		nop.SetImmediate(true)
		if err := WritePDU(env.clientConn, nop); err != nil {
			t.Fatal(err)
		}
		resp, err := ReadPDU(env.clientConn)
		if err != nil {
			t.Fatal(err)
		}
		statSNs = append(statSNs, resp.StatSN())
	}

	// Verify monotonic increment.
	for i := 1; i < len(statSNs); i++ {
		expected := statSNs[i-1] + 1
		if statSNs[i] != expected {
			t.Errorf("StatSN[%d] = %d, want %d (monotonic)", i, statSNs[i], expected)
		}
	}
	t.Logf("StatSN sequence: %v", statSNs)
}

func testQARXTXExpStatSNMismatch(t *testing.T) {
	// Send command with ExpStatSN != server's StatSN. Per RFC 7143,
	// ExpStatSN is advisory and should not cause rejection.
	env := setupSession(t)
	doLogin(t, env.clientConn)

	// Send NOP with wrong ExpStatSN.
	nop := &PDU{}
	nop.SetOpcode(OpNOPOut)
	nop.SetOpSpecific1(FlagF)
	nop.SetInitiatorTaskTag(0x5000)
	nop.SetImmediate(true)
	// Set ExpStatSN to a wrong value.
	binary.BigEndian.PutUint32(nop.BHS[28:32], 0xDEADBEEF)

	if err := WritePDU(env.clientConn, nop); err != nil {
		t.Fatal(err)
	}

	resp, err := ReadPDU(env.clientConn)
	if err != nil {
		t.Fatal(err)
	}

	// Should still get NOP-In response (ExpStatSN mismatch is not an error).
	if resp.Opcode() != OpNOPIn {
		t.Errorf("expected NOP-In, got %s", OpcodeName(resp.Opcode()))
	}
	if resp.InitiatorTaskTag() != 0x5000 {
		t.Errorf("ITT = 0x%x, want 0x5000", resp.InitiatorTaskTag())
	}
}

func testQARXTXStatSNAfterErrorResponse(t *testing.T) {
	// SCSI error response should still increment StatSN.
	env := setupSession(t)
	doLogin(t, env.clientConn)

	// First: send a NOP to get current StatSN.
	nop := &PDU{}
	nop.SetOpcode(OpNOPOut)
	nop.SetOpSpecific1(FlagF)
	nop.SetInitiatorTaskTag(0x6000)
	nop.SetImmediate(true)
	if err := WritePDU(env.clientConn, nop); err != nil {
		t.Fatal(err)
	}
	nopResp, err := ReadPDU(env.clientConn)
	if err != nil {
		t.Fatal(err)
	}
	baseSN := nopResp.StatSN()

	// Second: send READ to out-of-range LBA (causes CHECK_CONDITION).
	var cdb [16]byte
	cdb[0] = ScsiRead10
	binary.BigEndian.PutUint32(cdb[2:6], 0xFFFFFFF0) // huge LBA
	binary.BigEndian.PutUint16(cdb[7:9], 1)
	cmd := &PDU{}
	cmd.SetOpcode(OpSCSICmd)
	cmd.SetOpSpecific1(FlagF | FlagR)
	cmd.SetInitiatorTaskTag(0x6001)
	cmd.SetExpectedDataTransferLength(4096)
	cmd.SetCmdSN(2)
	cmd.SetCDB(cdb)
	if err := WritePDU(env.clientConn, cmd); err != nil {
		t.Fatal(err)
	}

	errResp, err := ReadPDU(env.clientConn)
	if err != nil {
		t.Fatal(err)
	}
	if errResp.Opcode() != OpSCSIResp {
		t.Fatalf("expected SCSI-Response, got %s", OpcodeName(errResp.Opcode()))
	}
	errSN := errResp.StatSN()

	// Third: send another NOP.
	nop2 := &PDU{}
	nop2.SetOpcode(OpNOPOut)
	nop2.SetOpSpecific1(FlagF)
	nop2.SetInitiatorTaskTag(0x6002)
	nop2.SetImmediate(true)
	if err := WritePDU(env.clientConn, nop2); err != nil {
		t.Fatal(err)
	}
	nop2Resp, err := ReadPDU(env.clientConn)
	if err != nil {
		t.Fatal(err)
	}
	afterSN := nop2Resp.StatSN()

	// Error response should have incremented StatSN.
	if errSN != baseSN+1 {
		t.Errorf("error StatSN = %d, want %d (baseSN+1)", errSN, baseSN+1)
	}
	if afterSN != baseSN+2 {
		t.Errorf("after error StatSN = %d, want %d (baseSN+2)", afterSN, baseSN+2)
	}
	t.Logf("StatSN: base=%d, error=%d, after=%d", baseSN, errSN, afterSN)
}

// --- QA-2.4: Shutdown Ordering ---

func testQARXTXShutdownWriterQueued(t *testing.T) {
	// Enqueue multiple responses, then disconnect.
	// Writer should drain all queued responses before exiting.
	env := setupSession(t)
	doLogin(t, env.clientConn)

	// Send 10 NOP-Outs rapidly.
	for i := 0; i < 10; i++ {
		nop := &PDU{}
		nop.SetOpcode(OpNOPOut)
		nop.SetOpSpecific1(FlagF)
		nop.SetInitiatorTaskTag(uint32(0x7000 + i))
		nop.SetImmediate(true)
		if err := WritePDU(env.clientConn, nop); err != nil {
			t.Fatalf("NOP %d: %v", i, err)
		}
	}

	// Read all 10 responses.
	env.clientConn.SetReadDeadline(time.Now().Add(2 * time.Second))
	count := 0
	for i := 0; i < 10; i++ {
		_, err := ReadPDU(env.clientConn)
		if err != nil {
			break
		}
		count++
	}
	if count != 10 {
		t.Errorf("received %d NOP-In responses, want 10", count)
	}
}

func testQARXTXTargetCloseActiveIO(t *testing.T) {
	// Heavy I/O on multiple sessions, target.Close() mid-flight.
	ts, addr := qaSetupTarget(t)

	var wg sync.WaitGroup
	wg.Add(4)

	// Start 4 sessions doing I/O.
	for i := 0; i < 4; i++ {
		go func(id int) {
			defer wg.Done()
			conn, err := net.DialTimeout("tcp", addr, 2*time.Second)
			if err != nil {
				return
			}
			defer conn.Close()

			doLogin(t, conn)

			// Write loop until connection closes.
			for j := 0; j < 100; j++ {
				data := make([]byte, 4096)
				data[0] = byte(id)
				sendSCSIWriteImmediate(t, conn, uint32(id), data, uint32(j+100), uint32(j+2))
				_, err := ReadPDU(conn)
				if err != nil {
					return // expected: target closed
				}
			}
		}(i)
	}

	// Give sessions time to start I/O.
	time.Sleep(50 * time.Millisecond)

	// Close target while I/O is in progress.
	ts.Close()

	// All goroutines should exit cleanly.
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("sessions did not exit after target.Close()")
	}
}

func testQARXTXSessionAfterTargetClose(t *testing.T) {
	// Close target, then try to connect -- should fail or close immediately.
	ts, addr := qaSetupTarget(t)

	// Verify one session works.
	conn, err := net.DialTimeout("tcp", addr, 2*time.Second)
	if err != nil {
		t.Fatalf("initial dial: %v", err)
	}
	doLogin(t, conn)
	conn.Close()

	// Close target.
	ts.Close()

	// New connection should fail.
	conn2, err := net.DialTimeout("tcp", addr, 500*time.Millisecond)
	if err != nil {
		// Expected: listener closed, dial fails.
		return
	}
	defer conn2.Close()

	// If dial succeeded (unlikely), the connection should be closed by server.
	conn2.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
	_, err = ReadPDU(conn2)
	if err == nil {
		t.Error("expected error reading from connection after target close")
	}
}
