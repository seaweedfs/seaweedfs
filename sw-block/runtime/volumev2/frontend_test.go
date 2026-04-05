package volumev2

import (
	"bytes"
	"encoding/binary"
	"net"
	"path/filepath"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/sw-block/runtime/masterv2"
	"github.com/seaweedfs/seaweedfs/weed/storage/blockvol/iscsi"
)

func TestVolumeV2_ExportISCSI_StartsFrontendAndAcceptsLogin(t *testing.T) {
	master := masterv2.New(masterv2.Config{})
	node, err := New(Config{NodeID: "node-a"})
	if err != nil {
		t.Fatalf("new node: %v", err)
	}
	defer node.Close()
	session, err := NewInProcessSession(master)
	if err != nil {
		t.Fatalf("new session: %v", err)
	}
	orchestrator, err := NewOrchestrator(node, session)
	if err != nil {
		t.Fatalf("new orchestrator: %v", err)
	}

	path := filepath.Join(t.TempDir(), "frontend-vol.blk")
	if err := master.DeclarePrimary(masterv2.VolumeSpec{
		Name:          "frontend-vol",
		Path:          path,
		PrimaryNodeID: "node-a",
		CreateOptions: testCreateOptions(),
	}); err != nil {
		t.Fatalf("declare primary: %v", err)
	}
	if err := orchestrator.SyncOnce(); err != nil {
		t.Fatalf("sync 1: %v", err)
	}
	if err := orchestrator.SyncOnce(); err != nil {
		t.Fatalf("sync 2: %v", err)
	}

	export, err := node.ExportISCSI("frontend-vol", "127.0.0.1:0", "iqn.2026-04.com.seaweedfs:test.frontend-vol")
	if err != nil {
		t.Fatalf("export iscsi: %v", err)
	}
	defer export.Close()

	conn, err := net.DialTimeout("tcp", export.Address(), 2*time.Second)
	if err != nil {
		t.Fatalf("dial target: %v", err)
	}
	defer conn.Close()

	params := iscsi.NewParams()
	params.Set("InitiatorName", "iqn.2026-04.com.seaweedfs:initiator.test")
	params.Set("TargetName", export.IQN())
	params.Set("SessionType", "Normal")

	loginReq := &iscsi.PDU{}
	loginReq.SetOpcode(iscsi.OpLoginReq)
	loginReq.SetLoginStages(iscsi.StageSecurityNeg, iscsi.StageFullFeature)
	loginReq.SetLoginTransit(true)
	loginReq.SetISID([6]byte{0x00, 0x02, 0x3D, 0x00, 0x00, 0x01})
	loginReq.SetCmdSN(1)
	loginReq.DataSegment = params.Encode()

	if err := iscsi.WritePDU(conn, loginReq); err != nil {
		t.Fatalf("write login req: %v", err)
	}
	resp, err := iscsi.ReadPDU(conn)
	if err != nil {
		t.Fatalf("read login resp: %v", err)
	}
	if resp.LoginStatusClass() != iscsi.LoginStatusSuccess {
		t.Fatalf("login failed: %d/%d", resp.LoginStatusClass(), resp.LoginStatusDetail())
	}
}

func TestKernelDataPlaneClosure_ISCSIWriteReadVerify(t *testing.T) {
	master := masterv2.New(masterv2.Config{})
	node, err := New(Config{NodeID: "node-a"})
	if err != nil {
		t.Fatalf("new node: %v", err)
	}
	defer node.Close()
	session, err := NewInProcessSession(master)
	if err != nil {
		t.Fatalf("new session: %v", err)
	}
	orchestrator, err := NewOrchestrator(node, session)
	if err != nil {
		t.Fatalf("new orchestrator: %v", err)
	}

	path := filepath.Join(t.TempDir(), "data-plane-vol.blk")
	if err := master.DeclarePrimary(masterv2.VolumeSpec{
		Name:          "data-plane-vol",
		Path:          path,
		PrimaryNodeID: "node-a",
		CreateOptions: testCreateOptions(),
	}); err != nil {
		t.Fatalf("declare primary: %v", err)
	}
	if err := orchestrator.SyncOnce(); err != nil {
		t.Fatalf("sync 1: %v", err)
	}
	if err := orchestrator.SyncOnce(); err != nil {
		t.Fatalf("sync 2: %v", err)
	}

	export, err := node.ExportISCSI("data-plane-vol", "127.0.0.1:0", "iqn.2026-04.com.seaweedfs:test.data-plane-vol")
	if err != nil {
		t.Fatalf("export iscsi: %v", err)
	}
	defer export.Close()

	conn := mustLoginISCSI(t, export.Address(), export.IQN())
	defer conn.Close()

	writeData := make([]byte, 4096)
	for i := range writeData {
		writeData[i] = byte((i * 7) % 251)
	}

	var writeCDB [16]byte
	writeCDB[0] = iscsi.ScsiWrite10
	binary.BigEndian.PutUint32(writeCDB[2:6], 0)
	binary.BigEndian.PutUint16(writeCDB[7:9], 1)
	resp := sendSCSICmd(t, conn, writeCDB, 2, false, true, writeData, uint32(len(writeData)))
	if resp.SCSIStatus() != iscsi.SCSIStatusGood {
		t.Fatalf("iscsi write failed: status=%d", resp.SCSIStatus())
	}

	var syncCDB [16]byte
	syncCDB[0] = iscsi.ScsiSyncCache10
	resp = sendSCSICmd(t, conn, syncCDB, 3, false, false, nil, 0)
	if resp.SCSIStatus() != iscsi.SCSIStatusGood {
		t.Fatalf("iscsi sync cache failed: status=%d", resp.SCSIStatus())
	}

	var readCDB [16]byte
	readCDB[0] = iscsi.ScsiRead10
	binary.BigEndian.PutUint32(readCDB[2:6], 0)
	binary.BigEndian.PutUint16(readCDB[7:9], 1)
	resp = sendSCSICmd(t, conn, readCDB, 4, true, false, nil, uint32(len(writeData)))
	if resp.Opcode() != iscsi.OpSCSIDataIn {
		t.Fatalf("expected Data-In, got %s", iscsi.OpcodeName(resp.Opcode()))
	}
	if !bytes.Equal(resp.DataSegment, writeData) {
		t.Fatal("iscsi readback mismatch")
	}

	readBack, err := node.ReadLBA("data-plane-vol", 0, uint32(len(writeData)))
	if err != nil {
		t.Fatalf("backend read: %v", err)
	}
	if !bytes.Equal(readBack, writeData) {
		t.Fatal("backend readback mismatch")
	}
}

func mustLoginISCSI(t *testing.T, addr, iqn string) net.Conn {
	t.Helper()

	conn, err := net.DialTimeout("tcp", addr, 2*time.Second)
	if err != nil {
		t.Fatalf("dial target: %v", err)
	}

	params := iscsi.NewParams()
	params.Set("InitiatorName", "iqn.2026-04.com.seaweedfs:initiator.test")
	params.Set("TargetName", iqn)
	params.Set("SessionType", "Normal")

	loginReq := &iscsi.PDU{}
	loginReq.SetOpcode(iscsi.OpLoginReq)
	loginReq.SetLoginStages(iscsi.StageSecurityNeg, iscsi.StageFullFeature)
	loginReq.SetLoginTransit(true)
	loginReq.SetISID([6]byte{0x00, 0x02, 0x3D, 0x00, 0x00, 0x01})
	loginReq.SetCmdSN(1)
	loginReq.DataSegment = params.Encode()

	if err := iscsi.WritePDU(conn, loginReq); err != nil {
		conn.Close()
		t.Fatalf("write login req: %v", err)
	}
	resp, err := iscsi.ReadPDU(conn)
	if err != nil {
		conn.Close()
		t.Fatalf("read login resp: %v", err)
	}
	if resp.LoginStatusClass() != iscsi.LoginStatusSuccess {
		conn.Close()
		t.Fatalf("login failed: %d/%d", resp.LoginStatusClass(), resp.LoginStatusDetail())
	}
	return conn
}

func sendSCSICmd(t *testing.T, conn net.Conn, cdb [16]byte, cmdSN uint32, read bool, write bool, dataOut []byte, expLen uint32) *iscsi.PDU {
	t.Helper()

	cmd := &iscsi.PDU{}
	cmd.SetOpcode(iscsi.OpSCSICmd)
	flags := uint8(iscsi.FlagF)
	if read {
		flags |= iscsi.FlagR
	}
	if write {
		flags |= iscsi.FlagW
	}
	cmd.SetOpSpecific1(flags)
	cmd.SetInitiatorTaskTag(cmdSN)
	cmd.SetExpectedDataTransferLength(expLen)
	cmd.SetCmdSN(cmdSN)
	cmd.SetCDB(cdb)
	if dataOut != nil {
		cmd.DataSegment = dataOut
	}

	if err := iscsi.WritePDU(conn, cmd); err != nil {
		t.Fatalf("write scsi cmd: %v", err)
	}
	resp, err := iscsi.ReadPDU(conn)
	if err != nil {
		t.Fatalf("read scsi resp: %v", err)
	}
	return resp
}
