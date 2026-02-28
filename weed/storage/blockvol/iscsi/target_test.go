package iscsi

import (
	"encoding/binary"
	"io"
	"log"
	"net"
	"strings"
	"testing"
	"time"
)

func TestTarget(t *testing.T) {
	tests := []struct {
		name string
		run  func(t *testing.T)
	}{
		{"listen_and_connect", testListenAndConnect},
		{"discovery_via_target", testDiscoveryViaTarget},
		{"login_read_write", testTargetLoginReadWrite},
		{"graceful_shutdown", testGracefulShutdown},
		{"add_remove_volume", testAddRemoveVolume},
		{"multiple_connections", testMultipleConnections},
		{"connect_no_volumes", testConnectNoVolumes},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.run(t)
		})
	}
}

func setupTarget(t *testing.T) (*TargetServer, string) {
	t.Helper()
	config := DefaultTargetConfig()
	config.TargetName = testTargetName
	logger := log.New(io.Discard, "", 0)
	ts := NewTargetServer("127.0.0.1:0", config, logger)

	dev := newMockDevice(256 * 4096) // 256 blocks = 1MB
	ts.AddVolume(testTargetName, dev)

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}

	go ts.Serve(ln)

	addr := ln.Addr().String()
	t.Cleanup(func() {
		ts.Close()
	})

	return ts, addr
}

func dialTarget(t *testing.T, addr string) net.Conn {
	t.Helper()
	conn, err := net.DialTimeout("tcp", addr, 2*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })
	return conn
}

func loginToTarget(t *testing.T, conn net.Conn) {
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
	if resp.LoginStatusClass() != LoginStatusSuccess {
		t.Fatalf("login failed: %d/%d", resp.LoginStatusClass(), resp.LoginStatusDetail())
	}
}

func testListenAndConnect(t *testing.T) {
	_, addr := setupTarget(t)

	conn := dialTarget(t, addr)
	loginToTarget(t, conn)
}

func testDiscoveryViaTarget(t *testing.T) {
	_, addr := setupTarget(t)

	conn := dialTarget(t, addr)

	// Login as discovery
	params := NewParams()
	params.Set("InitiatorName", testInitiatorName)
	params.Set("SessionType", "Discovery")
	req := makeLoginReq(StageSecurityNeg, StageFullFeature, true, params)
	req.SetCmdSN(1)
	WritePDU(conn, req)
	ReadPDU(conn)

	// SendTargets=All
	textParams := NewParams()
	textParams.Set("SendTargets", "All")
	textReq := makeTextReq(textParams)
	textReq.SetCmdSN(2)
	WritePDU(conn, textReq)

	resp, err := ReadPDU(conn)
	if err != nil {
		t.Fatal(err)
	}
	body := string(resp.DataSegment)
	if !strings.Contains(body, testTargetName) {
		t.Fatalf("discovery response missing target: %q", body)
	}
}

func testTargetLoginReadWrite(t *testing.T) {
	_, addr := setupTarget(t)
	conn := dialTarget(t, addr)
	loginToTarget(t, conn)

	// Write 1 block at LBA 0
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
	data := make([]byte, 4096)
	for i := range data {
		data[i] = 0xAB
	}
	cmd.DataSegment = data

	WritePDU(conn, cmd)
	resp, _ := ReadPDU(conn)
	if resp.SCSIStatus() != SCSIStatusGood {
		t.Fatalf("write failed: %d", resp.SCSIStatus())
	}

	// Read it back
	cmd2 := &PDU{}
	cmd2.SetOpcode(OpSCSICmd)
	cmd2.SetOpSpecific1(FlagF | FlagR)
	cmd2.SetInitiatorTaskTag(2)
	cmd2.SetExpectedDataTransferLength(4096)
	cmd2.SetCmdSN(3)
	var cdb2 [16]byte
	cdb2[0] = ScsiRead10
	binary.BigEndian.PutUint32(cdb2[2:6], 0)
	binary.BigEndian.PutUint16(cdb2[7:9], 1)
	cmd2.SetCDB(cdb2)

	WritePDU(conn, cmd2)
	resp2, _ := ReadPDU(conn)
	if resp2.Opcode() != OpSCSIDataIn {
		t.Fatalf("expected Data-In, got %s", OpcodeName(resp2.Opcode()))
	}
	if resp2.DataSegment[0] != 0xAB {
		t.Fatal("data mismatch")
	}
}

func testGracefulShutdown(t *testing.T) {
	ts, addr := setupTarget(t)

	conn := dialTarget(t, addr)
	loginToTarget(t, conn)

	// Close the target — should shut down cleanly
	ts.Close()

	// Connection should be dropped
	_, err := ReadPDU(conn)
	if err == nil {
		t.Fatal("expected error after shutdown")
	}
}

func testAddRemoveVolume(t *testing.T) {
	config := DefaultTargetConfig()
	logger := log.New(io.Discard, "", 0)
	ts := NewTargetServer("127.0.0.1:0", config, logger)

	ts.AddVolume("iqn.2024.com.test:v1", newMockDevice(1024*4096))
	ts.AddVolume("iqn.2024.com.test:v2", newMockDevice(1024*4096))

	if !ts.HasTarget("iqn.2024.com.test:v1") {
		t.Fatal("v1 should exist")
	}

	ts.RemoveVolume("iqn.2024.com.test:v1")
	if ts.HasTarget("iqn.2024.com.test:v1") {
		t.Fatal("v1 should be removed")
	}
	if !ts.HasTarget("iqn.2024.com.test:v2") {
		t.Fatal("v2 should still exist")
	}

	targets := ts.ListTargets()
	if len(targets) != 1 {
		t.Fatalf("expected 1 target, got %d", len(targets))
	}
}

func testMultipleConnections(t *testing.T) {
	_, addr := setupTarget(t)

	// Connect multiple clients
	for i := 0; i < 5; i++ {
		conn := dialTarget(t, addr)
		loginToTarget(t, conn)
	}
}

func testConnectNoVolumes(t *testing.T) {
	config := DefaultTargetConfig()
	logger := log.New(io.Discard, "", 0)
	ts := NewTargetServer("127.0.0.1:0", config, logger)
	// No volumes added

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	go ts.Serve(ln)
	t.Cleanup(func() { ts.Close() })

	conn := dialTarget(t, ln.Addr().String())

	// Login should work (discovery is fine without volumes)
	params := NewParams()
	params.Set("InitiatorName", testInitiatorName)
	params.Set("SessionType", "Discovery")
	req := makeLoginReq(StageSecurityNeg, StageFullFeature, true, params)
	req.SetCmdSN(1)
	WritePDU(conn, req)
	resp, _ := ReadPDU(conn)
	if resp.LoginStatusClass() != LoginStatusSuccess {
		t.Fatal("discovery login should succeed without volumes")
	}
}
