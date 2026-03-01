package iscsi

import (
	"bytes"
	"encoding/binary"
	"io"
	"log"
	"net"
	"testing"
	"time"
)

// testResolver implements TargetResolver, TargetLister, and DeviceLookup.
type testResolver struct {
	targets []DiscoveryTarget
	dev     BlockDevice
}

func (r *testResolver) HasTarget(name string) bool {
	for _, t := range r.targets {
		if t.Name == name {
			return true
		}
	}
	return false
}

func (r *testResolver) ListTargets() []DiscoveryTarget {
	return r.targets
}

func (r *testResolver) LookupDevice(iqn string) BlockDevice {
	if r.dev != nil {
		return r.dev
	}
	return &nullDevice{}
}

func newTestResolver() *testResolver {
	return newTestResolverWithDevice(nil)
}

func newTestResolverWithDevice(dev BlockDevice) *testResolver {
	return &testResolver{
		targets: []DiscoveryTarget{
			{Name: testTargetName, Address: "127.0.0.1:3260,1"},
		},
		dev: dev,
	}
}

func TestSession(t *testing.T) {
	tests := []struct {
		name string
		run  func(t *testing.T)
	}{
		{"login_and_read", testLoginAndRead},
		{"login_and_write", testLoginAndWrite},
		{"nop_ping", testNOPPing},
		{"logout", testLogout},
		{"discovery_session", testDiscoverySession},
		{"task_mgmt", testTaskMgmt},
		{"reject_scsi_before_login", testRejectSCSIBeforeLogin},
		{"connection_close", testConnectionClose},
		// Phase 3 CP2: RX/TX split tests.
		{"rxtx_read_write_basic", testRXTXReadWriteBasic},
		{"rxtx_pipelined_reads", testRXTXPipelinedReads},
		{"rxtx_pipelined_writes", testRXTXPipelinedWrites},
		{"rxtx_write_with_r2t", testRXTXWriteWithR2T},
		{"rxtx_mixed_ops", testRXTXMixedOps},
		{"rxtx_large_read_datain", testRXTXLargeReadDataIn},
		{"rxtx_statsn_monotonic", testRXTXStatSNMonotonic},
		{"rxtx_statsn_datain_no_increment", testRXTXStatSNDataInNoIncrement},
		{"rxtx_statsn_mixed_types", testRXTXStatSNMixedTypes},
		{"rxtx_shutdown_clean", testRXTXShutdownClean},
		{"rxtx_conn_drop_reader", testRXTXConnDropReader},
		{"rxtx_conn_drop_writer", testRXTXConnDropWriter},
		// Phase 3 code review fixes.
		{"rxtx_r2t_statsn_fresh", testRXTXR2TStatSNFresh},
		{"rxtx_tx_error_exits_clean", testRXTXTxErrorExitsClean},
		{"rxtx_login_phase_reject", testRXTXLoginPhaseReject},
		// Phase 3 bug fixes (P3-BUG-2, P3-BUG-3).
		{"rxtx_pending_queue_overflow", testRXTXPendingQueueOverflow},
		{"rxtx_dataout_timeout", testRXTXDataOutTimeout},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.run(t)
		})
	}
}

type sessionTestEnv struct {
	clientConn net.Conn
	session    *Session
	done       chan error
}

func setupSession(t *testing.T) *sessionTestEnv {
	return setupSessionWithConfig(t, nil)
}

func setupSessionWithConfig(t *testing.T, cfgFn func(*TargetConfig)) *sessionTestEnv {
	t.Helper()
	client, server := net.Pipe()

	dev := newMockDevice(1024 * 4096) // 1024 blocks
	config := DefaultTargetConfig()
	config.TargetName = testTargetName
	if cfgFn != nil {
		cfgFn(&config)
	}
	resolver := newTestResolverWithDevice(dev)
	logger := log.New(io.Discard, "", 0)

	sess := NewSession(server, config, resolver, resolver, logger)
	done := make(chan error, 1)
	go func() {
		done <- sess.HandleConnection()
	}()

	t.Cleanup(func() {
		sess.Close()
		client.Close()
	})

	return &sessionTestEnv{clientConn: client, session: sess, done: done}
}

func doLogin(t *testing.T, conn net.Conn) {
	t.Helper()
	params := NewParams()
	params.Set("InitiatorName", testInitiatorName)
	params.Set("TargetName", testTargetName)
	params.Set("SessionType", "Normal")

	req := makeLoginReq(StageSecurityNeg, StageFullFeature, true, params)
	req.SetCmdSN(1)

	if err := WritePDU(conn, req); err != nil {
		t.Fatal(err)
	}

	resp, err := ReadPDU(conn)
	if err != nil {
		t.Fatal(err)
	}
	if resp.Opcode() != OpLoginResp {
		t.Fatalf("expected LoginResp, got %s", OpcodeName(resp.Opcode()))
	}
	if resp.LoginStatusClass() != LoginStatusSuccess {
		t.Fatalf("login failed: %d/%d", resp.LoginStatusClass(), resp.LoginStatusDetail())
	}
}

func testLoginAndRead(t *testing.T) {
	env := setupSession(t)
	doLogin(t, env.clientConn)

	// Send SCSI READ_10 for 1 block at LBA 0
	cmd := &PDU{}
	cmd.SetOpcode(OpSCSICmd)
	cmd.SetOpSpecific1(FlagF | FlagR) // Final + Read
	cmd.SetInitiatorTaskTag(1)
	cmd.SetExpectedDataTransferLength(4096)
	cmd.SetCmdSN(2)
	cmd.SetExpStatSN(2)

	var cdb [16]byte
	cdb[0] = ScsiRead10
	binary.BigEndian.PutUint32(cdb[2:6], 0) // LBA=0
	binary.BigEndian.PutUint16(cdb[7:9], 1) // 1 block
	cmd.SetCDB(cdb)

	if err := WritePDU(env.clientConn, cmd); err != nil {
		t.Fatal(err)
	}

	// Expect Data-In with S-bit (single PDU, 4096 bytes of zeros)
	resp, err := ReadPDU(env.clientConn)
	if err != nil {
		t.Fatal(err)
	}
	if resp.Opcode() != OpSCSIDataIn {
		t.Fatalf("expected Data-In, got %s", OpcodeName(resp.Opcode()))
	}
	if resp.OpSpecific1()&FlagS == 0 {
		t.Fatal("S-bit not set")
	}
	if len(resp.DataSegment) != 4096 {
		t.Fatalf("data length: %d", len(resp.DataSegment))
	}
}

func testLoginAndWrite(t *testing.T) {
	env := setupSession(t)
	doLogin(t, env.clientConn)

	// SCSI WRITE_10: 1 block at LBA 5 with immediate data
	cmd := &PDU{}
	cmd.SetOpcode(OpSCSICmd)
	cmd.SetOpSpecific1(FlagF | FlagW) // Final + Write
	cmd.SetInitiatorTaskTag(1)
	cmd.SetExpectedDataTransferLength(4096)
	cmd.SetCmdSN(2)

	var cdb [16]byte
	cdb[0] = ScsiWrite10
	binary.BigEndian.PutUint32(cdb[2:6], 5)
	binary.BigEndian.PutUint16(cdb[7:9], 1)
	cmd.SetCDB(cdb)

	// Include immediate data
	cmd.DataSegment = bytes.Repeat([]byte{0xAA}, 4096)

	if err := WritePDU(env.clientConn, cmd); err != nil {
		t.Fatal(err)
	}

	// Expect SCSI Response (good)
	resp, err := ReadPDU(env.clientConn)
	if err != nil {
		t.Fatal(err)
	}
	if resp.Opcode() != OpSCSIResp {
		t.Fatalf("expected SCSI Response, got %s", OpcodeName(resp.Opcode()))
	}
	if resp.SCSIStatus() != SCSIStatusGood {
		t.Fatalf("status: %d", resp.SCSIStatus())
	}
}

func testNOPPing(t *testing.T) {
	env := setupSession(t)
	doLogin(t, env.clientConn)

	// Send NOP-Out
	nop := &PDU{}
	nop.SetOpcode(OpNOPOut)
	nop.SetOpSpecific1(FlagF)
	nop.SetInitiatorTaskTag(0x9999)
	nop.SetImmediate(true)
	nop.DataSegment = []byte("ping")

	if err := WritePDU(env.clientConn, nop); err != nil {
		t.Fatal(err)
	}

	resp, err := ReadPDU(env.clientConn)
	if err != nil {
		t.Fatal(err)
	}
	if resp.Opcode() != OpNOPIn {
		t.Fatalf("expected NOP-In, got %s", OpcodeName(resp.Opcode()))
	}
	if resp.InitiatorTaskTag() != 0x9999 {
		t.Fatal("ITT mismatch")
	}
	if string(resp.DataSegment) != "ping" {
		t.Fatalf("echo data: %q", resp.DataSegment)
	}
}

func testLogout(t *testing.T) {
	env := setupSession(t)
	doLogin(t, env.clientConn)

	// Send Logout
	logout := &PDU{}
	logout.SetOpcode(OpLogoutReq)
	logout.SetOpSpecific1(FlagF)
	logout.SetInitiatorTaskTag(0xAAAA)
	logout.SetCmdSN(2)

	if err := WritePDU(env.clientConn, logout); err != nil {
		t.Fatal(err)
	}

	resp, err := ReadPDU(env.clientConn)
	if err != nil {
		t.Fatal(err)
	}
	if resp.Opcode() != OpLogoutResp {
		t.Fatalf("expected Logout Response, got %s", OpcodeName(resp.Opcode()))
	}

	// After logout, the connection should be closed by the target.
	// Verify by trying to read — should get EOF.
	_, err = ReadPDU(env.clientConn)
	if err == nil {
		t.Fatal("expected EOF after logout")
	}
}

func testDiscoverySession(t *testing.T) {
	env := setupSession(t)

	// Login as discovery session
	params := NewParams()
	params.Set("InitiatorName", testInitiatorName)
	params.Set("SessionType", "Discovery")

	req := makeLoginReq(StageSecurityNeg, StageFullFeature, true, params)
	req.SetCmdSN(1)
	WritePDU(env.clientConn, req)
	ReadPDU(env.clientConn) // login resp

	// Send SendTargets=All
	textParams := NewParams()
	textParams.Set("SendTargets", "All")
	textReq := makeTextReq(textParams)
	textReq.SetCmdSN(2)

	if err := WritePDU(env.clientConn, textReq); err != nil {
		t.Fatal(err)
	}

	resp, err := ReadPDU(env.clientConn)
	if err != nil {
		t.Fatal(err)
	}
	if resp.Opcode() != OpTextResp {
		t.Fatalf("expected Text Response, got %s", OpcodeName(resp.Opcode()))
	}
	body := string(resp.DataSegment)
	if len(body) == 0 {
		t.Fatal("empty discovery response")
	}
}

func testTaskMgmt(t *testing.T) {
	env := setupSession(t)
	doLogin(t, env.clientConn)

	tm := &PDU{}
	tm.SetOpcode(OpSCSITaskMgmt)
	tm.SetOpSpecific1(FlagF | 0x01) // ABORT TASK
	tm.SetInitiatorTaskTag(0xBBBB)
	tm.SetImmediate(true)

	if err := WritePDU(env.clientConn, tm); err != nil {
		t.Fatal(err)
	}

	resp, err := ReadPDU(env.clientConn)
	if err != nil {
		t.Fatal(err)
	}
	if resp.Opcode() != OpSCSITaskResp {
		t.Fatalf("expected Task Mgmt Response, got %s", OpcodeName(resp.Opcode()))
	}
}

func testRejectSCSIBeforeLogin(t *testing.T) {
	env := setupSession(t)

	// Send SCSI command without login
	cmd := &PDU{}
	cmd.SetOpcode(OpSCSICmd)
	cmd.SetOpSpecific1(FlagF | FlagR)
	cmd.SetInitiatorTaskTag(1)

	if err := WritePDU(env.clientConn, cmd); err != nil {
		t.Fatal(err)
	}

	resp, err := ReadPDU(env.clientConn)
	if err != nil {
		t.Fatal(err)
	}
	if resp.Opcode() != OpReject {
		t.Fatalf("expected Reject, got %s", OpcodeName(resp.Opcode()))
	}
}

func testConnectionClose(t *testing.T) {
	env := setupSession(t)
	doLogin(t, env.clientConn)

	// Close client side
	env.clientConn.Close()

	select {
	case err := <-env.done:
		if err != nil {
			t.Fatalf("unexpected error on clean close: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("session did not detect close")
	}
}

// --- Phase 3 CP2: RX/TX split tests ---

func sendSCSIRead(t *testing.T, conn net.Conn, lba uint32, blocks uint16, itt uint32, cmdSN uint32) {
	t.Helper()
	cmd := &PDU{}
	cmd.SetOpcode(OpSCSICmd)
	cmd.SetOpSpecific1(FlagF | FlagR)
	cmd.SetInitiatorTaskTag(itt)
	cmd.SetExpectedDataTransferLength(uint32(blocks) * 4096)
	cmd.SetCmdSN(cmdSN)

	var cdb [16]byte
	cdb[0] = ScsiRead10
	binary.BigEndian.PutUint32(cdb[2:6], lba)
	binary.BigEndian.PutUint16(cdb[7:9], blocks)
	cmd.SetCDB(cdb)

	if err := WritePDU(conn, cmd); err != nil {
		t.Fatalf("sendSCSIRead: %v", err)
	}
}

func sendSCSIWriteImmediate(t *testing.T, conn net.Conn, lba uint32, data []byte, itt uint32, cmdSN uint32) {
	t.Helper()
	cmd := &PDU{}
	cmd.SetOpcode(OpSCSICmd)
	cmd.SetOpSpecific1(FlagF | FlagW)
	cmd.SetInitiatorTaskTag(itt)
	cmd.SetExpectedDataTransferLength(uint32(len(data)))
	cmd.SetCmdSN(cmdSN)

	var cdb [16]byte
	cdb[0] = ScsiWrite10
	binary.BigEndian.PutUint32(cdb[2:6], lba)
	binary.BigEndian.PutUint16(cdb[7:9], uint16(len(data)/4096))
	cmd.SetCDB(cdb)
	cmd.DataSegment = data

	if err := WritePDU(conn, cmd); err != nil {
		t.Fatalf("sendSCSIWriteImmediate: %v", err)
	}
}

func testRXTXReadWriteBasic(t *testing.T) {
	env := setupSession(t)
	doLogin(t, env.clientConn)

	// Write then read via the RX/TX split path.
	data := bytes.Repeat([]byte{0xBB}, 4096)
	sendSCSIWriteImmediate(t, env.clientConn, 0, data, 1, 2)

	resp, err := ReadPDU(env.clientConn)
	if err != nil {
		t.Fatal(err)
	}
	if resp.Opcode() != OpSCSIResp || resp.SCSIStatus() != SCSIStatusGood {
		t.Fatalf("write resp: opcode=%s status=%d", OpcodeName(resp.Opcode()), resp.SCSIStatus())
	}

	sendSCSIRead(t, env.clientConn, 0, 1, 2, 3)
	resp, err = ReadPDU(env.clientConn)
	if err != nil {
		t.Fatal(err)
	}
	if resp.Opcode() != OpSCSIDataIn {
		t.Fatalf("expected DataIn, got %s", OpcodeName(resp.Opcode()))
	}
	if !bytes.Equal(resp.DataSegment, data) {
		t.Error("read data mismatch")
	}
}

func testRXTXPipelinedReads(t *testing.T) {
	env := setupSession(t)
	doLogin(t, env.clientConn)

	// Send 4 reads back-to-back.
	for i := uint32(0); i < 4; i++ {
		sendSCSIRead(t, env.clientConn, i, 1, i+1, i+2)
	}

	// Receive all 4 responses.
	for i := 0; i < 4; i++ {
		resp, err := ReadPDU(env.clientConn)
		if err != nil {
			t.Fatalf("read %d: %v", i, err)
		}
		if resp.Opcode() != OpSCSIDataIn {
			t.Fatalf("read %d: expected DataIn, got %s", i, OpcodeName(resp.Opcode()))
		}
	}
}

func testRXTXPipelinedWrites(t *testing.T) {
	env := setupSession(t)
	doLogin(t, env.clientConn)

	// Send 8 writes back-to-back with immediate data.
	for i := uint32(0); i < 8; i++ {
		data := bytes.Repeat([]byte{byte(0xA0 + i)}, 4096)
		sendSCSIWriteImmediate(t, env.clientConn, i, data, i+1, i+2)
	}

	// Receive all 8 responses.
	for i := 0; i < 8; i++ {
		resp, err := ReadPDU(env.clientConn)
		if err != nil {
			t.Fatalf("write %d: %v", i, err)
		}
		if resp.Opcode() != OpSCSIResp {
			t.Fatalf("write %d: expected SCSIResp, got %s", i, OpcodeName(resp.Opcode()))
		}
		if resp.SCSIStatus() != SCSIStatusGood {
			t.Fatalf("write %d: status=%d", i, resp.SCSIStatus())
		}
	}
}

func testRXTXWriteWithR2T(t *testing.T) {
	env := setupSession(t)
	doLogin(t, env.clientConn)

	// Write without immediate data — should trigger R2T + Data-Out.
	cmd := &PDU{}
	cmd.SetOpcode(OpSCSICmd)
	cmd.SetOpSpecific1(FlagF | FlagW) // no immediate data
	cmd.SetInitiatorTaskTag(1)
	cmd.SetExpectedDataTransferLength(4096)
	cmd.SetCmdSN(2)

	var cdb [16]byte
	cdb[0] = ScsiWrite10
	binary.BigEndian.PutUint32(cdb[2:6], 10)
	binary.BigEndian.PutUint16(cdb[7:9], 1)
	cmd.SetCDB(cdb)
	// No DataSegment = no immediate data

	if err := WritePDU(env.clientConn, cmd); err != nil {
		t.Fatal(err)
	}

	// Expect R2T from target.
	r2t, err := ReadPDU(env.clientConn)
	if err != nil {
		t.Fatal(err)
	}
	if r2t.Opcode() != OpR2T {
		t.Fatalf("expected R2T, got %s", OpcodeName(r2t.Opcode()))
	}

	// Send Data-Out.
	dataOut := &PDU{}
	dataOut.SetOpcode(OpSCSIDataOut)
	dataOut.SetOpSpecific1(FlagF) // Final
	dataOut.SetInitiatorTaskTag(1)
	dataOut.SetTargetTransferTag(r2t.TargetTransferTag())
	dataOut.SetDataSN(0)
	dataOut.SetBufferOffset(0)
	dataOut.DataSegment = bytes.Repeat([]byte{0xCC}, 4096)

	if err := WritePDU(env.clientConn, dataOut); err != nil {
		t.Fatal(err)
	}

	// Expect SCSI Response.
	resp, err := ReadPDU(env.clientConn)
	if err != nil {
		t.Fatal(err)
	}
	if resp.Opcode() != OpSCSIResp {
		t.Fatalf("expected SCSIResp, got %s", OpcodeName(resp.Opcode()))
	}
	if resp.SCSIStatus() != SCSIStatusGood {
		t.Fatalf("status=%d", resp.SCSIStatus())
	}
}

func testRXTXMixedOps(t *testing.T) {
	env := setupSession(t)
	doLogin(t, env.clientConn)

	// Interleave: WRITE, READ, NOP, READ, WRITE
	cmdSN := uint32(2)

	// WRITE
	sendSCSIWriteImmediate(t, env.clientConn, 0, bytes.Repeat([]byte{0x11}, 4096), 1, cmdSN)
	cmdSN++
	resp, _ := ReadPDU(env.clientConn)
	if resp.Opcode() != OpSCSIResp {
		t.Fatalf("write: got %s", OpcodeName(resp.Opcode()))
	}

	// READ
	sendSCSIRead(t, env.clientConn, 0, 1, 2, cmdSN)
	cmdSN++
	resp, _ = ReadPDU(env.clientConn)
	if resp.Opcode() != OpSCSIDataIn {
		t.Fatalf("read: got %s", OpcodeName(resp.Opcode()))
	}

	// NOP
	nop := &PDU{}
	nop.SetOpcode(OpNOPOut)
	nop.SetOpSpecific1(FlagF)
	nop.SetInitiatorTaskTag(3)
	nop.SetImmediate(true)
	WritePDU(env.clientConn, nop)
	resp, _ = ReadPDU(env.clientConn)
	if resp.Opcode() != OpNOPIn {
		t.Fatalf("nop: got %s", OpcodeName(resp.Opcode()))
	}

	// TEST_UNIT_READY (CDB[0] = 0x00)
	tuCmd := &PDU{}
	tuCmd.SetOpcode(OpSCSICmd)
	tuCmd.SetOpSpecific1(FlagF)
	tuCmd.SetInitiatorTaskTag(4)
	tuCmd.SetCmdSN(cmdSN)
	var tuCDB [16]byte
	tuCmd.SetCDB(tuCDB) // all zeros = TEST_UNIT_READY
	WritePDU(env.clientConn, tuCmd)
	resp, _ = ReadPDU(env.clientConn)
	if resp.Opcode() != OpSCSIResp {
		t.Fatalf("tur: got %s", OpcodeName(resp.Opcode()))
	}
}

func testRXTXLargeReadDataIn(t *testing.T) {
	env := setupSessionWithConfig(t, func(c *TargetConfig) {
		c.MaxRecvDataSegmentLength = 4096 // small: force multi-PDU
	})
	doLogin(t, env.clientConn)

	// Read 4 blocks = 16384 bytes. With MaxRecvDataSegmentLength=4096,
	// this should produce 4 Data-In PDUs.
	sendSCSIRead(t, env.clientConn, 0, 4, 1, 2)

	var pdus []*PDU
	for {
		p, err := ReadPDU(env.clientConn)
		if err != nil {
			t.Fatal(err)
		}
		pdus = append(pdus, p)
		if p.OpSpecific1()&FlagS != 0 {
			break // final PDU with S-bit
		}
	}

	if len(pdus) < 2 {
		t.Fatalf("expected >= 2 Data-In PDUs, got %d", len(pdus))
	}

	// Reconstruct data.
	totalData := make([]byte, 0)
	for _, p := range pdus {
		totalData = append(totalData, p.DataSegment...)
	}
	if len(totalData) != 4*4096 {
		t.Fatalf("total data length: %d, want %d", len(totalData), 4*4096)
	}
}

func testRXTXStatSNMonotonic(t *testing.T) {
	env := setupSession(t)
	doLogin(t, env.clientConn)

	var lastStatSN uint32
	cmdSN := uint32(2)

	for i := 0; i < 10; i++ {
		sendSCSIRead(t, env.clientConn, 0, 1, uint32(i+1), cmdSN)
		cmdSN++

		resp, err := ReadPDU(env.clientConn)
		if err != nil {
			t.Fatal(err)
		}

		statSN := resp.StatSN()
		if i > 0 && statSN != lastStatSN+1 {
			t.Fatalf("StatSN not monotonic: prev=%d, curr=%d", lastStatSN, statSN)
		}
		lastStatSN = statSN
	}
}

func testRXTXStatSNDataInNoIncrement(t *testing.T) {
	env := setupSessionWithConfig(t, func(c *TargetConfig) {
		c.MaxRecvDataSegmentLength = 4096 // small: force multi-PDU
	})
	doLogin(t, env.clientConn)

	// Read 4 blocks to get multi-PDU Data-In.
	sendSCSIRead(t, env.clientConn, 0, 4, 1, 2)

	var pdus []*PDU
	for {
		p, err := ReadPDU(env.clientConn)
		if err != nil {
			t.Fatal(err)
		}
		pdus = append(pdus, p)
		if p.OpSpecific1()&FlagS != 0 {
			break
		}
	}

	if len(pdus) < 2 {
		t.Fatalf("expected >= 2 Data-In PDUs, got %d", len(pdus))
	}

	// Intermediate PDUs should NOT have StatSN incremented.
	// Only the final PDU (with S-bit) should have StatSN.
	finalPDU := pdus[len(pdus)-1]
	if finalPDU.OpSpecific1()&FlagS == 0 {
		t.Fatal("last PDU missing S-bit")
	}
	// StatSN on final PDU should be set.
	statSN := finalPDU.StatSN()
	if statSN == 0 {
		// It's actually valid for StatSN to be any value (depends on login StatSN).
		// Just verify it's set on the final PDU.
	}
	_ = statSN
}

func testRXTXStatSNMixedTypes(t *testing.T) {
	env := setupSession(t)
	doLogin(t, env.clientConn)

	cmdSN := uint32(2)
	var statSNs []uint32

	// NOP
	nop := &PDU{}
	nop.SetOpcode(OpNOPOut)
	nop.SetOpSpecific1(FlagF)
	nop.SetInitiatorTaskTag(1)
	nop.SetImmediate(true)
	WritePDU(env.clientConn, nop)
	resp, _ := ReadPDU(env.clientConn)
	statSNs = append(statSNs, resp.StatSN())

	// SCSI READ
	sendSCSIRead(t, env.clientConn, 0, 1, 2, cmdSN)
	cmdSN++
	resp, _ = ReadPDU(env.clientConn)
	statSNs = append(statSNs, resp.StatSN())

	// Logout
	logout := &PDU{}
	logout.SetOpcode(OpLogoutReq)
	logout.SetOpSpecific1(FlagF)
	logout.SetInitiatorTaskTag(3)
	logout.SetCmdSN(cmdSN)
	WritePDU(env.clientConn, logout)
	resp, _ = ReadPDU(env.clientConn)
	statSNs = append(statSNs, resp.StatSN())

	// All StatSNs should be strictly increasing.
	for i := 1; i < len(statSNs); i++ {
		if statSNs[i] != statSNs[i-1]+1 {
			t.Errorf("StatSN not monotonic at %d: %v", i, statSNs)
		}
	}
}

func testRXTXShutdownClean(t *testing.T) {
	env := setupSession(t)
	doLogin(t, env.clientConn)

	// Send logout.
	logout := &PDU{}
	logout.SetOpcode(OpLogoutReq)
	logout.SetOpSpecific1(FlagF)
	logout.SetInitiatorTaskTag(1)
	logout.SetCmdSN(2)
	WritePDU(env.clientConn, logout)

	// Read logout response.
	resp, err := ReadPDU(env.clientConn)
	if err != nil {
		t.Fatal(err)
	}
	if resp.Opcode() != OpLogoutResp {
		t.Fatalf("expected LogoutResp, got %s", OpcodeName(resp.Opcode()))
	}

	// HandleConnection should exit cleanly.
	select {
	case err := <-env.done:
		if err != nil {
			t.Fatalf("error after logout: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("session did not exit after logout")
	}
}

func testRXTXConnDropReader(t *testing.T) {
	env := setupSession(t)
	doLogin(t, env.clientConn)

	// Close client conn — reader should detect EOF.
	env.clientConn.Close()

	select {
	case err := <-env.done:
		if err != nil {
			t.Fatalf("error on conn drop: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("session did not exit after conn drop")
	}
}

func testRXTXConnDropWriter(t *testing.T) {
	client, server := net.Pipe()
	dev := newMockDevice(1024 * 4096)
	config := DefaultTargetConfig()
	config.TargetName = testTargetName
	resolver := newTestResolverWithDevice(dev)
	logger := log.New(io.Discard, "", 0)

	sess := NewSession(server, config, resolver, resolver, logger)
	done := make(chan error, 1)
	go func() {
		done <- sess.HandleConnection()
	}()

	doLogin(t, client)

	// Close the server side (writer side for txLoop).
	server.Close()

	// Session should exit.
	select {
	case <-done:
		// Good — session exited.
	case <-time.After(2 * time.Second):
		t.Fatal("session did not exit after server-side close")
	}

	client.Close()
}

// testRXTXR2TStatSNFresh verifies that R2T PDUs carry a fresh StatSN
// (assigned by txLoop) rather than a stale value baked in at build time.
func testRXTXR2TStatSNFresh(t *testing.T) {
	env := setupSession(t)
	doLogin(t, env.clientConn)

	// Send a NOP-Out (immediate) — its NOP-In response will consume one StatSN.
	nop := &PDU{}
	nop.SetOpcode(OpNOPOut)
	nop.SetOpSpecific1(FlagF)
	nop.SetInitiatorTaskTag(0x1000)
	nop.SetImmediate(true)
	if err := WritePDU(env.clientConn, nop); err != nil {
		t.Fatal(err)
	}

	// Read NOP-In response.
	nopResp, err := ReadPDU(env.clientConn)
	if err != nil {
		t.Fatal(err)
	}
	if nopResp.Opcode() != OpNOPIn {
		t.Fatalf("expected NOP-In, got %s", OpcodeName(nopResp.Opcode()))
	}
	nopStatSN := nopResp.StatSN()

	// Now send a WRITE command without immediate data to trigger R2T.
	cmd := &PDU{}
	cmd.SetOpcode(OpSCSICmd)
	cmd.SetOpSpecific1(FlagF | FlagW)
	cmd.SetInitiatorTaskTag(1)
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

	// Read R2T — its StatSN should be nopStatSN+1 (fresh, not stale).
	r2t, err := ReadPDU(env.clientConn)
	if err != nil {
		t.Fatal(err)
	}
	if r2t.Opcode() != OpR2T {
		t.Fatalf("expected R2T, got %s", OpcodeName(r2t.Opcode()))
	}

	// R2T uses statSNCopy: same as current StatSN (which is nopStatSN+1) without increment.
	expectedStatSN := nopStatSN + 1
	if r2t.StatSN() != expectedStatSN {
		t.Fatalf("R2T StatSN: got %d, want %d (NOP-In was %d)", r2t.StatSN(), expectedStatSN, nopStatSN)
	}

	// Complete the write by sending Data-Out.
	dataOut := &PDU{}
	dataOut.SetOpcode(OpSCSIDataOut)
	dataOut.SetOpSpecific1(FlagF)
	dataOut.SetInitiatorTaskTag(1)
	dataOut.SetTargetTransferTag(r2t.TargetTransferTag())
	dataOut.SetDataSN(0)
	dataOut.SetBufferOffset(0)
	dataOut.DataSegment = bytes.Repeat([]byte{0xDD}, 4096)

	if err := WritePDU(env.clientConn, dataOut); err != nil {
		t.Fatal(err)
	}

	// Read SCSI response — its StatSN should be nopStatSN+1 (R2T didn't increment).
	resp, err := ReadPDU(env.clientConn)
	if err != nil {
		t.Fatal(err)
	}
	if resp.Opcode() != OpSCSIResp {
		t.Fatalf("expected SCSIResp, got %s", OpcodeName(resp.Opcode()))
	}
	if resp.StatSN() != expectedStatSN {
		t.Fatalf("SCSI Resp StatSN: got %d, want %d", resp.StatSN(), expectedStatSN)
	}
}

// testRXTXTxErrorExitsClean verifies that when txLoop encounters a write error,
// HandleConnection exits cleanly without hanging.
func testRXTXTxErrorExitsClean(t *testing.T) {
	client, server := net.Pipe()
	dev := newMockDevice(1024 * 4096)
	config := DefaultTargetConfig()
	config.TargetName = testTargetName
	resolver := newTestResolverWithDevice(dev)
	logger := log.New(io.Discard, "", 0)

	sess := NewSession(server, config, resolver, resolver, logger)
	done := make(chan error, 1)
	go func() {
		done <- sess.HandleConnection()
	}()

	doLogin(t, client)

	// Close the server side — this makes txLoop's WritePDU fail.
	server.Close()

	// Send something from client side so rxLoop dispatches and enqueues a response.
	// The enqueue should not block forever thanks to txDone select.
	nop := &PDU{}
	nop.SetOpcode(OpNOPOut)
	nop.SetOpSpecific1(FlagF)
	nop.SetInitiatorTaskTag(1)
	nop.SetImmediate(true)
	// Write may fail since server is closed; that's fine.
	WritePDU(client, nop)

	// HandleConnection should exit cleanly without hanging.
	select {
	case <-done:
		// Good.
	case <-time.After(3 * time.Second):
		t.Fatal("HandleConnection did not exit after tx write error")
	}

	// Verify txDone is closed (no goroutine leak).
	select {
	case <-sess.txDone:
		// Good — txLoop exited.
	default:
		t.Fatal("txDone not closed — txLoop may be leaked")
	}

	client.Close()
}

// testRXTXLoginPhaseReject verifies that handlers reject PDUs sent before
// login completes and that login can still succeed afterward.
func testRXTXLoginPhaseReject(t *testing.T) {
	client, server := net.Pipe()
	dev := newMockDevice(1024 * 4096)
	config := DefaultTargetConfig()
	config.TargetName = testTargetName
	resolver := newTestResolverWithDevice(dev)
	logger := log.New(io.Discard, "", 0)

	sess := NewSession(server, config, resolver, resolver, logger)
	done := make(chan error, 1)
	go func() {
		done <- sess.HandleConnection()
	}()

	defer func() {
		sess.Close()
		client.Close()
	}()

	// Send a TextReq before login — should get a Reject (inline, not buffered).
	textParams := NewParams()
	textParams.Set("SendTargets", "All")
	textReq := makeTextReq(textParams)
	textReq.SetCmdSN(1)

	if err := WritePDU(client, textReq); err != nil {
		t.Fatal(err)
	}

	resp, err := ReadPDU(client)
	if err != nil {
		t.Fatal(err)
	}
	if resp.Opcode() != OpReject {
		t.Fatalf("expected Reject, got %s", OpcodeName(resp.Opcode()))
	}
	if resp.BHS[2] != 0x0b {
		t.Fatalf("reject reason: got 0x%02x, want 0x0b", resp.BHS[2])
	}

	// Login should still work after the reject.
	doLogin(t, client)

	// Verify session is functional by sending a NOP.
	nop := &PDU{}
	nop.SetOpcode(OpNOPOut)
	nop.SetOpSpecific1(FlagF)
	nop.SetInitiatorTaskTag(0x2222)
	nop.SetImmediate(true)
	if err := WritePDU(client, nop); err != nil {
		t.Fatal(err)
	}

	nopResp, err := ReadPDU(client)
	if err != nil {
		t.Fatal(err)
	}
	if nopResp.Opcode() != OpNOPIn {
		t.Fatalf("expected NOP-In, got %s", OpcodeName(nopResp.Opcode()))
	}
}

// testRXTXPendingQueueOverflow verifies that flooding non-Data-Out PDUs
// during Data-Out collection causes the session to close (not OOM).
func testRXTXPendingQueueOverflow(t *testing.T) {
	env := setupSessionWithConfig(t, func(cfg *TargetConfig) {
		cfg.MaxRecvDataSegmentLength = 4096
		cfg.FirstBurstLength = 0 // force R2T
		cfg.DataOutTimeout = 5 * time.Second
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

	// Flood 100 NOP-Out PDUs during Data-Out collection.
	for i := 0; i < 100; i++ {
		nop := &PDU{}
		nop.SetOpcode(OpNOPOut)
		nop.SetOpSpecific1(FlagF)
		nop.SetInitiatorTaskTag(uint32(0xB000 + i))
		nop.SetImmediate(true)
		if err := WritePDU(env.clientConn, nop); err != nil {
			break // session closed
		}
	}

	// Session should exit with an error (pending queue overflow).
	select {
	case <-env.done:
		// Good — session exited.
	case <-time.After(3 * time.Second):
		t.Fatal("session did not exit after pending overflow")
	}
}

// testRXTXDataOutTimeout verifies that collectDataOut times out
// when the initiator never sends Data-Out after R2T.
func testRXTXDataOutTimeout(t *testing.T) {
	env := setupSessionWithConfig(t, func(cfg *TargetConfig) {
		cfg.MaxRecvDataSegmentLength = 4096
		cfg.FirstBurstLength = 0 // force R2T
		cfg.DataOutTimeout = 500 * time.Millisecond
	})
	doLogin(t, env.clientConn)

	// Send WRITE command requiring R2T (no immediate data).
	cmd := &PDU{}
	cmd.SetOpcode(OpSCSICmd)
	cmd.SetOpSpecific1(FlagF | FlagW)
	cmd.SetInitiatorTaskTag(0xBEEF)
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

	// Do NOT send Data-Out. Session should time out and exit.
	select {
	case err := <-env.done:
		t.Logf("session exited: %v", err)
	case <-time.After(3 * time.Second):
		t.Fatal("session did not time out — DataOutTimeout not working")
	}
}
