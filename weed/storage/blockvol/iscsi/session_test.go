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
	t.Helper()
	client, server := net.Pipe()

	dev := newMockDevice(1024 * 4096) // 1024 blocks
	config := DefaultTargetConfig()
	config.TargetName = testTargetName
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
