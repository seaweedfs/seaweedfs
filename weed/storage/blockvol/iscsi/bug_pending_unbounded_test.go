package iscsi

import (
	"encoding/binary"
	"runtime"
	"testing"
	"time"
)

// TestBugPendingQueueUnbounded demonstrates that the pending queue
// in collectDataOut grows without bound.
//
// BUG: During Data-Out collection, any non-DataOut PDU is appended
// to s.pending (session.go line 428) with no limit. A misbehaving
// initiator can send thousands of PDUs during a Data-Out exchange,
// causing unbounded memory growth (OOM risk).
//
// FIX: Cap s.pending at a reasonable limit (e.g., 64 entries).
// Drop or reject excess PDUs with a REJECT response.
//
// This test FAILS until the bug is fixed.
func TestBugPendingQueueUnbounded(t *testing.T) {
	env := setupSessionWithConfig(t, func(cfg *TargetConfig) {
		cfg.MaxRecvDataSegmentLength = 4096
		cfg.FirstBurstLength = 0 // force R2T
	})
	doLogin(t, env.clientConn)

	// Start WRITE requiring R2T.
	cmd := &PDU{}
	cmd.SetOpcode(OpSCSICmd)
	cmd.SetOpSpecific1(FlagF | FlagW)
	cmd.SetInitiatorTaskTag(0xAAAA)
	cmd.SetExpectedDataTransferLength(4096)
	cmd.SetCmdSN(2)
	var cdb [16]byte
	cdb[0] = ScsiWrite10
	binary.BigEndian.PutUint32(cdb[2:6], 0)
	binary.BigEndian.PutUint16(cdb[7:9], 1)
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

	// Flood with 200 NOP-Out PDUs during Data-Out collection.
	// These all get queued in s.pending with no limit.
	memBefore := getAlloc()
	for i := 0; i < 200; i++ {
		nop := &PDU{}
		nop.SetOpcode(OpNOPOut)
		nop.SetOpSpecific1(FlagF)
		nop.SetInitiatorTaskTag(uint32(0xB000 + i))
		nop.SetImmediate(true)
		if err := WritePDU(env.clientConn, nop); err != nil {
			// If session closed or rejected, that's OK.
			t.Logf("NOP %d: write failed (session may have rejected): %v", i, err)
			break
		}
	}

	// Complete the Data-Out to let the session process the pending queue.
	dataOut := &PDU{}
	dataOut.SetOpcode(OpSCSIDataOut)
	dataOut.SetOpSpecific1(FlagF)
	dataOut.SetInitiatorTaskTag(0xAAAA)
	dataOut.SetTargetTransferTag(r2t.TargetTransferTag())
	dataOut.SetBufferOffset(0)
	dataOut.DataSegment = make([]byte, 4096)
	if err := WritePDU(env.clientConn, dataOut); err != nil {
		// Session may have closed if it correctly rejected the flood.
		t.Logf("Data-Out write: %v (may be expected if session rejected flood)", err)
	}

	// Read responses. Count how many NOP-In responses we get.
	env.clientConn.SetReadDeadline(time.Now().Add(2 * time.Second))
	nopResponses := 0
	for {
		resp, err := ReadPDU(env.clientConn)
		if err != nil {
			break
		}
		if resp.Opcode() == OpNOPIn {
			nopResponses++
		}
	}

	memAfter := getAlloc()
	t.Logf("pending flood: 200 NOPs sent, %d NOP-In responses, mem delta ~%d KB",
		nopResponses, (memAfter-memBefore)/1024)

	// BUG: All 200 NOPs were queued in pending and processed.
	// A well-behaved server should cap the pending queue.
	// With a cap of 64, at most 64 NOP responses should arrive.
	const maxAcceptable = 64
	if nopResponses > maxAcceptable {
		t.Fatalf("BUG: received %d NOP-In responses (all 200 queued in pending) -- "+
			"pending queue is unbounded, should be capped at %d", nopResponses, maxAcceptable)
	}
}

func getAlloc() uint64 {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	return m.Alloc
}
