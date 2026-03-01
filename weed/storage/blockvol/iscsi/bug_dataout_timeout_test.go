package iscsi

import (
	"encoding/binary"
	"testing"
	"time"
)


// TestBugCollectDataOutNoTimeout demonstrates that collectDataOut
// blocks forever if the initiator stops sending Data-Out PDUs.
//
// BUG: collectDataOut calls ReadPDU(s.conn) in a loop with no
// read deadline. If the initiator sends a WRITE requiring R2T,
// receives the R2T, but never sends Data-Out, the session is stuck
// forever (until TCP keepalive kills it, which could be minutes).
//
// FIX: Set a read deadline on s.conn before entering the Data-Out
// collection loop (e.g., 30 seconds), and return a timeout error
// if the deadline fires.
//
// This test FAILS until the bug is fixed.
func TestBugCollectDataOutNoTimeout(t *testing.T) {
	env := setupSessionWithConfig(t, func(cfg *TargetConfig) {
		cfg.MaxRecvDataSegmentLength = 4096
		cfg.FirstBurstLength = 0              // force R2T (no immediate data accepted)
		cfg.DataOutTimeout = 1 * time.Second  // short timeout for test
	})
	doLogin(t, env.clientConn)

	// Send WRITE command that requires R2T (no immediate data).
	cmd := &PDU{}
	cmd.SetOpcode(OpSCSICmd)
	cmd.SetOpSpecific1(FlagF | FlagW)
	cmd.SetInitiatorTaskTag(0xBEEF)
	cmd.SetExpectedDataTransferLength(4096)
	cmd.SetCmdSN(2)
	var cdb [16]byte
	cdb[0] = ScsiWrite10
	binary.BigEndian.PutUint32(cdb[2:6], 0) // LBA 0
	binary.BigEndian.PutUint16(cdb[7:9], 1) // 1 block
	cmd.SetCDB(cdb)
	// No DataSegment — forces R2T.

	if err := WritePDU(env.clientConn, cmd); err != nil {
		t.Fatalf("WritePDU: %v", err)
	}

	// Read R2T from server.
	r2t, err := ReadPDU(env.clientConn)
	if err != nil {
		t.Fatalf("ReadPDU (R2T): %v", err)
	}
	if r2t.Opcode() != OpR2T {
		t.Fatalf("expected R2T, got %s", OpcodeName(r2t.Opcode()))
	}

	// DO NOT send Data-Out. The session should time out and close.
	// Currently it blocks forever in collectDataOut → ReadPDU(s.conn).

	select {
	case err := <-env.done:
		// GOOD: session exited (timed out or errored out).
		t.Logf("session exited: %v", err)
	case <-time.After(5 * time.Second):
		env.clientConn.Close()
		t.Fatal("collectDataOut did not time out within 5s")
	}
}
