package iscsi_test

import (
	"bytes"
	"encoding/binary"
	"io"
	"log"
	"net"
	"path/filepath"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/storage/blockvol"
	"github.com/seaweedfs/seaweedfs/weed/storage/blockvol/iscsi"
)

// blockVolAdapter wraps a BlockVol to implement iscsi.BlockDevice.
type blockVolAdapter struct {
	vol *blockvol.BlockVol
}

func (a *blockVolAdapter) ReadAt(lba uint64, length uint32) ([]byte, error) {
	return a.vol.ReadLBA(lba, length)
}
func (a *blockVolAdapter) WriteAt(lba uint64, data []byte) error {
	return a.vol.WriteLBA(lba, data)
}
func (a *blockVolAdapter) Trim(lba uint64, length uint32) error {
	return a.vol.Trim(lba, length)
}
func (a *blockVolAdapter) SyncCache() error {
	return a.vol.SyncCache()
}
func (a *blockVolAdapter) BlockSize() uint32 {
	return a.vol.Info().BlockSize
}
func (a *blockVolAdapter) VolumeSize() uint64 {
	return a.vol.Info().VolumeSize
}
func (a *blockVolAdapter) IsHealthy() bool {
	return a.vol.Info().Healthy
}

const (
	intTargetName    = "iqn.2024.com.seaweedfs:integration"
	intInitiatorName = "iqn.2024.com.test:client"
)

func createTestVol(t *testing.T) *blockvol.BlockVol {
	t.Helper()
	path := filepath.Join(t.TempDir(), "test.blk")
	vol, err := blockvol.CreateBlockVol(path, blockvol.CreateOptions{
		VolumeSize: 1024 * 4096, // 1024 blocks = 4MB
		BlockSize:  4096,
		WALSize:    1024 * 1024, // 1MB WAL
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { vol.Close() })
	return vol
}

func setupIntegrationTarget(t *testing.T) (net.Conn, *iscsi.TargetServer) {
	t.Helper()
	vol := createTestVol(t)
	adapter := &blockVolAdapter{vol: vol}

	config := iscsi.DefaultTargetConfig()
	config.TargetName = intTargetName
	logger := log.New(io.Discard, "", 0)
	ts := iscsi.NewTargetServer("127.0.0.1:0", config, logger)
	ts.AddVolume(intTargetName, adapter)

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	go ts.Serve(ln)
	t.Cleanup(func() { ts.Close() })

	conn, err := net.DialTimeout("tcp", ln.Addr().String(), 2*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })

	// Login
	params := iscsi.NewParams()
	params.Set("InitiatorName", intInitiatorName)
	params.Set("TargetName", intTargetName)
	params.Set("SessionType", "Normal")

	loginReq := &iscsi.PDU{}
	loginReq.SetOpcode(iscsi.OpLoginReq)
	loginReq.SetLoginStages(iscsi.StageSecurityNeg, iscsi.StageFullFeature)
	loginReq.SetLoginTransit(true)
	loginReq.SetISID([6]byte{0x00, 0x02, 0x3D, 0x00, 0x00, 0x01})
	loginReq.SetCmdSN(1)
	loginReq.DataSegment = params.Encode()

	if err := iscsi.WritePDU(conn, loginReq); err != nil {
		t.Fatal(err)
	}
	resp, err := iscsi.ReadPDU(conn)
	if err != nil {
		t.Fatal(err)
	}
	if resp.LoginStatusClass() != iscsi.LoginStatusSuccess {
		t.Fatalf("login failed: %d/%d", resp.LoginStatusClass(), resp.LoginStatusDetail())
	}

	return conn, ts
}

func TestIntegration(t *testing.T) {
	tests := []struct {
		name string
		run  func(t *testing.T)
	}{
		{"write_read_verify", testWriteReadVerify},
		{"multi_block_write_read", testMultiBlockWriteRead},
		{"inquiry_capacity", testInquiryCapacity},
		{"sync_cache", testIntSyncCache},
		{"test_unit_ready", testIntTestUnitReady},
		{"concurrent_readers_writers", testConcurrentReadersWriters},
		{"write_at_boundary", testWriteAtBoundary},
		{"unmap_integration", testUnmapIntegration},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.run(t)
		})
	}
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
		t.Fatal(err)
	}

	resp, err := iscsi.ReadPDU(conn)
	if err != nil {
		t.Fatal(err)
	}
	return resp
}

func testWriteReadVerify(t *testing.T) {
	conn, _ := setupIntegrationTarget(t)

	// Write pattern to LBA 0
	writeData := make([]byte, 4096)
	for i := range writeData {
		writeData[i] = byte(i % 251) // prime modulus for pattern
	}

	var wCDB [16]byte
	wCDB[0] = iscsi.ScsiWrite10
	binary.BigEndian.PutUint32(wCDB[2:6], 0) // LBA 0
	binary.BigEndian.PutUint16(wCDB[7:9], 1) // 1 block

	resp := sendSCSICmd(t, conn, wCDB, 2, false, true, writeData, 4096)
	if resp.SCSIStatus() != iscsi.SCSIStatusGood {
		t.Fatalf("write failed: status %d", resp.SCSIStatus())
	}

	// Read it back
	var rCDB [16]byte
	rCDB[0] = iscsi.ScsiRead10
	binary.BigEndian.PutUint32(rCDB[2:6], 0)
	binary.BigEndian.PutUint16(rCDB[7:9], 1)

	resp2 := sendSCSICmd(t, conn, rCDB, 3, true, false, nil, 4096)
	if resp2.Opcode() != iscsi.OpSCSIDataIn {
		t.Fatalf("expected Data-In, got %s", iscsi.OpcodeName(resp2.Opcode()))
	}

	if !bytes.Equal(resp2.DataSegment, writeData) {
		t.Fatal("data integrity verification failed")
	}
}

func testMultiBlockWriteRead(t *testing.T) {
	conn, _ := setupIntegrationTarget(t)

	// Write 4 blocks at LBA 10
	blockCount := uint16(4)
	dataLen := int(blockCount) * 4096
	writeData := make([]byte, dataLen)
	for i := range writeData {
		writeData[i] = byte((i / 4096) + 1) // each block has different fill
	}

	var wCDB [16]byte
	wCDB[0] = iscsi.ScsiWrite10
	binary.BigEndian.PutUint32(wCDB[2:6], 10)
	binary.BigEndian.PutUint16(wCDB[7:9], blockCount)

	resp := sendSCSICmd(t, conn, wCDB, 2, false, true, writeData, uint32(dataLen))
	if resp.SCSIStatus() != iscsi.SCSIStatusGood {
		t.Fatalf("write failed: status %d", resp.SCSIStatus())
	}

	// Read back all 4 blocks
	var rCDB [16]byte
	rCDB[0] = iscsi.ScsiRead10
	binary.BigEndian.PutUint32(rCDB[2:6], 10)
	binary.BigEndian.PutUint16(rCDB[7:9], blockCount)

	resp2 := sendSCSICmd(t, conn, rCDB, 3, true, false, nil, uint32(dataLen))

	// May be split across multiple Data-In PDUs — reassemble
	var readData []byte
	readData = append(readData, resp2.DataSegment...)

	// If first PDU doesn't have S-bit, read more
	for resp2.OpSpecific1()&iscsi.FlagS == 0 {
		var err error
		resp2, err = iscsi.ReadPDU(conn)
		if err != nil {
			t.Fatal(err)
		}
		readData = append(readData, resp2.DataSegment...)
	}

	if !bytes.Equal(readData, writeData) {
		t.Fatal("multi-block data integrity failed")
	}
}

func testInquiryCapacity(t *testing.T) {
	conn, _ := setupIntegrationTarget(t)

	// TEST UNIT READY
	var tuCDB [16]byte
	tuCDB[0] = iscsi.ScsiTestUnitReady
	resp := sendSCSICmd(t, conn, tuCDB, 2, false, false, nil, 0)
	if resp.SCSIStatus() != iscsi.SCSIStatusGood {
		t.Fatal("test unit ready failed")
	}

	// INQUIRY
	var iqCDB [16]byte
	iqCDB[0] = iscsi.ScsiInquiry
	binary.BigEndian.PutUint16(iqCDB[3:5], 96)
	resp = sendSCSICmd(t, conn, iqCDB, 3, true, false, nil, 96)
	if resp.Opcode() != iscsi.OpSCSIDataIn {
		t.Fatal("expected Data-In for inquiry")
	}
	if resp.DataSegment[0] != 0x00 { // SBC device
		t.Fatal("not SBC device type")
	}

	// READ CAPACITY 10
	var rcCDB [16]byte
	rcCDB[0] = iscsi.ScsiReadCapacity10
	resp = sendSCSICmd(t, conn, rcCDB, 4, true, false, nil, 8)
	if resp.Opcode() != iscsi.OpSCSIDataIn {
		t.Fatal("expected Data-In for read capacity")
	}
	lastLBA := binary.BigEndian.Uint32(resp.DataSegment[0:4])
	blockSize := binary.BigEndian.Uint32(resp.DataSegment[4:8])
	if lastLBA != 1023 { // 1024 blocks, last = 1023
		t.Fatalf("last LBA: %d, expected 1023", lastLBA)
	}
	if blockSize != 4096 {
		t.Fatalf("block size: %d", blockSize)
	}
}

func testIntSyncCache(t *testing.T) {
	conn, _ := setupIntegrationTarget(t)

	var cdb [16]byte
	cdb[0] = iscsi.ScsiSyncCache10
	resp := sendSCSICmd(t, conn, cdb, 2, false, false, nil, 0)
	if resp.SCSIStatus() != iscsi.SCSIStatusGood {
		t.Fatalf("sync cache failed: %d", resp.SCSIStatus())
	}
}

func testIntTestUnitReady(t *testing.T) {
	conn, _ := setupIntegrationTarget(t)

	var cdb [16]byte
	cdb[0] = iscsi.ScsiTestUnitReady
	resp := sendSCSICmd(t, conn, cdb, 2, false, false, nil, 0)
	if resp.SCSIStatus() != iscsi.SCSIStatusGood {
		t.Fatal("TUR failed")
	}
}

func testConcurrentReadersWriters(t *testing.T) {
	conn, _ := setupIntegrationTarget(t)

	// Sequential writes to different LBAs, then sequential reads to verify
	cmdSN := uint32(2)
	for lba := uint32(0); lba < 10; lba++ {
		data := bytes.Repeat([]byte{byte(lba)}, 4096)
		var wCDB [16]byte
		wCDB[0] = iscsi.ScsiWrite10
		binary.BigEndian.PutUint32(wCDB[2:6], lba)
		binary.BigEndian.PutUint16(wCDB[7:9], 1)
		resp := sendSCSICmd(t, conn, wCDB, cmdSN, false, true, data, 4096)
		if resp.SCSIStatus() != iscsi.SCSIStatusGood {
			t.Fatalf("write LBA %d failed", lba)
		}
		cmdSN++
	}

	// Read back and verify
	for lba := uint32(0); lba < 10; lba++ {
		var rCDB [16]byte
		rCDB[0] = iscsi.ScsiRead10
		binary.BigEndian.PutUint32(rCDB[2:6], lba)
		binary.BigEndian.PutUint16(rCDB[7:9], 1)
		resp := sendSCSICmd(t, conn, rCDB, cmdSN, true, false, nil, 4096)
		if resp.Opcode() != iscsi.OpSCSIDataIn {
			t.Fatalf("LBA %d: expected Data-In", lba)
		}
		expected := bytes.Repeat([]byte{byte(lba)}, 4096)
		if !bytes.Equal(resp.DataSegment, expected) {
			t.Fatalf("LBA %d: data mismatch", lba)
		}
		cmdSN++
	}
}

func testWriteAtBoundary(t *testing.T) {
	conn, _ := setupIntegrationTarget(t)

	// Write at last valid LBA (1023)
	data := bytes.Repeat([]byte{0xFF}, 4096)
	var wCDB [16]byte
	wCDB[0] = iscsi.ScsiWrite10
	binary.BigEndian.PutUint32(wCDB[2:6], 1023)
	binary.BigEndian.PutUint16(wCDB[7:9], 1)
	resp := sendSCSICmd(t, conn, wCDB, 2, false, true, data, 4096)
	if resp.SCSIStatus() != iscsi.SCSIStatusGood {
		t.Fatal("boundary write failed")
	}

	// Write past end — should fail
	var wCDB2 [16]byte
	wCDB2[0] = iscsi.ScsiWrite10
	binary.BigEndian.PutUint32(wCDB2[2:6], 1024) // out of bounds
	binary.BigEndian.PutUint16(wCDB2[7:9], 1)
	resp2 := sendSCSICmd(t, conn, wCDB2, 3, false, true, data, 4096)
	if resp2.SCSIStatus() == iscsi.SCSIStatusGood {
		t.Fatal("OOB write should fail")
	}
}

func testUnmapIntegration(t *testing.T) {
	conn, _ := setupIntegrationTarget(t)

	// Write data at LBA 5
	writeData := bytes.Repeat([]byte{0xCC}, 4096)
	var wCDB [16]byte
	wCDB[0] = iscsi.ScsiWrite10
	binary.BigEndian.PutUint32(wCDB[2:6], 5)
	binary.BigEndian.PutUint16(wCDB[7:9], 1)
	sendSCSICmd(t, conn, wCDB, 2, false, true, writeData, 4096)

	// UNMAP LBA 5
	unmapData := make([]byte, 24)
	binary.BigEndian.PutUint16(unmapData[0:2], 22)
	binary.BigEndian.PutUint16(unmapData[2:4], 16)
	binary.BigEndian.PutUint64(unmapData[8:16], 5)
	binary.BigEndian.PutUint32(unmapData[16:20], 1)

	var uCDB [16]byte
	uCDB[0] = iscsi.ScsiUnmap
	resp := sendSCSICmd(t, conn, uCDB, 3, false, true, unmapData, uint32(len(unmapData)))
	if resp.SCSIStatus() != iscsi.SCSIStatusGood {
		t.Fatalf("unmap failed: %d", resp.SCSIStatus())
	}

	// Read back — should be zeros
	var rCDB [16]byte
	rCDB[0] = iscsi.ScsiRead10
	binary.BigEndian.PutUint32(rCDB[2:6], 5)
	binary.BigEndian.PutUint16(rCDB[7:9], 1)
	resp2 := sendSCSICmd(t, conn, rCDB, 4, true, false, nil, 4096)
	if resp2.Opcode() != iscsi.OpSCSIDataIn {
		t.Fatal("expected Data-In")
	}
	zeros := make([]byte, 4096)
	if !bytes.Equal(resp2.DataSegment, zeros) {
		t.Fatal("unmapped block should return zeros")
	}
}
