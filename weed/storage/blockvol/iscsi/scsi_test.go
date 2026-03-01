package iscsi

import (
	"encoding/binary"
	"errors"
	"testing"
)

// mockBlockDevice implements BlockDevice for testing.
type mockBlockDevice struct {
	blockSize  uint32
	volumeSize uint64
	healthy    bool
	blocks     map[uint64][]byte // LBA -> data
	syncErr    error
	readErr    error
	writeErr   error
	trimErr    error
}

func newMockDevice(volumeSize uint64) *mockBlockDevice {
	return &mockBlockDevice{
		blockSize:  4096,
		volumeSize: volumeSize,
		healthy:    true,
		blocks:     make(map[uint64][]byte),
	}
}

func (m *mockBlockDevice) ReadAt(lba uint64, length uint32) ([]byte, error) {
	if m.readErr != nil {
		return nil, m.readErr
	}
	blockCount := length / m.blockSize
	result := make([]byte, length)
	for i := uint32(0); i < blockCount; i++ {
		if data, ok := m.blocks[lba+uint64(i)]; ok {
			copy(result[i*m.blockSize:], data)
		}
		// Unwritten blocks return zeros (already zeroed)
	}
	return result, nil
}

func (m *mockBlockDevice) WriteAt(lba uint64, data []byte) error {
	if m.writeErr != nil {
		return m.writeErr
	}
	blockCount := uint32(len(data)) / m.blockSize
	for i := uint32(0); i < blockCount; i++ {
		block := make([]byte, m.blockSize)
		copy(block, data[i*m.blockSize:])
		m.blocks[lba+uint64(i)] = block
	}
	return nil
}

func (m *mockBlockDevice) Trim(lba uint64, length uint32) error {
	if m.trimErr != nil {
		return m.trimErr
	}
	blockCount := length / m.blockSize
	for i := uint32(0); i < blockCount; i++ {
		delete(m.blocks, lba+uint64(i))
	}
	return nil
}

func (m *mockBlockDevice) SyncCache() error  { return m.syncErr }
func (m *mockBlockDevice) BlockSize() uint32  { return m.blockSize }
func (m *mockBlockDevice) VolumeSize() uint64 { return m.volumeSize }
func (m *mockBlockDevice) IsHealthy() bool    { return m.healthy }

func TestSCSI(t *testing.T) {
	tests := []struct {
		name string
		run  func(t *testing.T)
	}{
		{"test_unit_ready_good", testTestUnitReadyGood},
		{"test_unit_ready_not_ready", testTestUnitReadyNotReady},
		{"inquiry_standard", testInquiryStandard},
		{"inquiry_vpd_supported_pages", testInquiryVPDSupportedPages},
		{"inquiry_vpd_serial", testInquiryVPDSerial},
		{"inquiry_vpd_device_id", testInquiryVPDDeviceID},
		{"inquiry_vpd_unknown_page", testInquiryVPDUnknownPage},
		{"inquiry_alloc_length", testInquiryAllocLength},
		{"read_capacity_10", testReadCapacity10},
		{"read_capacity_10_large", testReadCapacity10Large},
		{"read_capacity_16", testReadCapacity16},
		{"read_capacity_16_lbpme", testReadCapacity16LBPME},
		{"mode_sense_6", testModeSense6},
		{"report_luns", testReportLuns},
		{"unknown_opcode", testUnknownOpcode},
		{"read_10", testRead10},
		{"read_16", testRead16},
		{"write_10", testWrite10},
		{"write_16", testWrite16},
		{"read_write_roundtrip", testReadWriteRoundtrip},
		{"write_oob", testWriteOOB},
		{"read_oob", testReadOOB},
		{"zero_length_transfer", testZeroLengthTransfer},
		{"sync_cache", testSyncCache},
		{"sync_cache_error", testSyncCacheError},
		{"unmap_single", testUnmapSingle},
		{"unmap_multiple_descriptors", testUnmapMultipleDescriptors},
		{"unmap_short_param", testUnmapShortParam},
		{"build_sense_data", testBuildSenseData},
		{"read_error", testReadError},
		{"write_error", testWriteError},
		{"read_capacity_16_invalid_sa", testReadCapacity16InvalidSA},
		{"write_same_16_unmap", testWriteSame16Unmap},
		{"write_same_16_ndob", testWriteSame16NDOB},
		{"write_same_16_pattern", testWriteSame16Pattern},
		{"write_same_16_zeros", testWriteSame16Zeros},
		{"write_same_16_oob", testWriteSame16OOB},
		{"write_same_16_zero_blocks", testWriteSame16ZeroBlocks},
		{"mode_sense_10", testModeSense10},
		{"vpd_b0_block_limits", testVPDB0BlockLimits},
		{"vpd_b2_logical_block_prov", testVPDB2LogicalBlockProv},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.run(t)
		})
	}
}

func testTestUnitReadyGood(t *testing.T) {
	dev := newMockDevice(1024 * 1024)
	h := NewSCSIHandler(dev)
	var cdb [16]byte
	cdb[0] = ScsiTestUnitReady
	r := h.HandleCommand(cdb, nil)
	if r.Status != SCSIStatusGood {
		t.Fatalf("status: %d", r.Status)
	}
}

func testTestUnitReadyNotReady(t *testing.T) {
	dev := newMockDevice(1024 * 1024)
	dev.healthy = false
	h := NewSCSIHandler(dev)
	var cdb [16]byte
	cdb[0] = ScsiTestUnitReady
	r := h.HandleCommand(cdb, nil)
	if r.Status != SCSIStatusCheckCond {
		t.Fatalf("status: %d", r.Status)
	}
	if r.SenseKey != SenseNotReady {
		t.Fatalf("sense key: %d", r.SenseKey)
	}
}

func testInquiryStandard(t *testing.T) {
	dev := newMockDevice(1024 * 1024)
	h := NewSCSIHandler(dev)
	var cdb [16]byte
	cdb[0] = ScsiInquiry
	binary.BigEndian.PutUint16(cdb[3:5], 96)
	r := h.HandleCommand(cdb, nil)
	if r.Status != SCSIStatusGood {
		t.Fatalf("status: %d", r.Status)
	}
	if len(r.Data) != 96 {
		t.Fatalf("data length: %d", len(r.Data))
	}
	// Check peripheral device type
	if r.Data[0] != 0x00 {
		t.Fatal("not SBC device type")
	}
	// Check vendor
	vendor := string(r.Data[8:16])
	if vendor != "SeaweedF" {
		t.Fatalf("vendor: %q", vendor)
	}
	// Check CmdQue
	if r.Data[7]&0x02 == 0 {
		t.Fatal("CmdQue not set")
	}
}

func testInquiryVPDSupportedPages(t *testing.T) {
	dev := newMockDevice(1024 * 1024)
	h := NewSCSIHandler(dev)
	var cdb [16]byte
	cdb[0] = ScsiInquiry
	cdb[1] = 0x01 // EVPD
	cdb[2] = 0x00 // Supported pages
	binary.BigEndian.PutUint16(cdb[3:5], 255)
	r := h.HandleCommand(cdb, nil)
	if r.Status != SCSIStatusGood {
		t.Fatalf("status: %d", r.Status)
	}
	if r.Data[1] != 0x00 {
		t.Fatal("wrong page code")
	}
	// Should list pages 0x00, 0x80, 0x83, 0xB0, 0xB2
	want := []byte{0x00, 0x80, 0x83, 0xb0, 0xb2}
	if len(r.Data) < 4+len(want) {
		t.Fatalf("supported pages too short: %v", r.Data)
	}
	for i, p := range want {
		if r.Data[4+i] != p {
			t.Fatalf("page[%d]: got %02x, want %02x (data=%v)", i, r.Data[4+i], p, r.Data)
		}
	}
}

func testInquiryVPDSerial(t *testing.T) {
	dev := newMockDevice(1024 * 1024)
	h := NewSCSIHandler(dev)
	var cdb [16]byte
	cdb[0] = ScsiInquiry
	cdb[1] = 0x01
	cdb[2] = 0x80
	binary.BigEndian.PutUint16(cdb[3:5], 255)
	r := h.HandleCommand(cdb, nil)
	if r.Status != SCSIStatusGood {
		t.Fatalf("status: %d", r.Status)
	}
	if r.Data[1] != 0x80 {
		t.Fatal("wrong page code")
	}
}

func testInquiryVPDDeviceID(t *testing.T) {
	dev := newMockDevice(1024 * 1024)
	h := NewSCSIHandler(dev)
	var cdb [16]byte
	cdb[0] = ScsiInquiry
	cdb[1] = 0x01
	cdb[2] = 0x83
	binary.BigEndian.PutUint16(cdb[3:5], 255)
	r := h.HandleCommand(cdb, nil)
	if r.Status != SCSIStatusGood {
		t.Fatalf("status: %d", r.Status)
	}
	if r.Data[1] != 0x83 {
		t.Fatal("wrong page code")
	}
}

func testInquiryVPDUnknownPage(t *testing.T) {
	dev := newMockDevice(1024 * 1024)
	h := NewSCSIHandler(dev)
	var cdb [16]byte
	cdb[0] = ScsiInquiry
	cdb[1] = 0x01
	cdb[2] = 0xFF // unknown page
	binary.BigEndian.PutUint16(cdb[3:5], 255)
	r := h.HandleCommand(cdb, nil)
	if r.Status != SCSIStatusCheckCond {
		t.Fatal("expected CHECK_CONDITION")
	}
	if r.SenseKey != SenseIllegalRequest {
		t.Fatal("expected ILLEGAL_REQUEST")
	}
}

func testInquiryAllocLength(t *testing.T) {
	dev := newMockDevice(1024 * 1024)
	h := NewSCSIHandler(dev)
	var cdb [16]byte
	cdb[0] = ScsiInquiry
	binary.BigEndian.PutUint16(cdb[3:5], 10) // small alloc
	r := h.HandleCommand(cdb, nil)
	if r.Status != SCSIStatusGood {
		t.Fatal("should succeed")
	}
	if len(r.Data) != 10 {
		t.Fatalf("truncation: expected 10, got %d", len(r.Data))
	}
}

func testReadCapacity10(t *testing.T) {
	dev := newMockDevice(100 * 4096) // 100 blocks
	h := NewSCSIHandler(dev)
	var cdb [16]byte
	cdb[0] = ScsiReadCapacity10
	r := h.HandleCommand(cdb, nil)
	if r.Status != SCSIStatusGood {
		t.Fatal("status not good")
	}
	if len(r.Data) != 8 {
		t.Fatalf("data length: %d", len(r.Data))
	}
	lastLBA := binary.BigEndian.Uint32(r.Data[0:4])
	blockSize := binary.BigEndian.Uint32(r.Data[4:8])
	if lastLBA != 99 {
		t.Fatalf("last LBA: %d, expected 99", lastLBA)
	}
	if blockSize != 4096 {
		t.Fatalf("block size: %d", blockSize)
	}
}

func testReadCapacity10Large(t *testing.T) {
	// Volume with >2^32 blocks should return 0xFFFFFFFF
	dev := newMockDevice(uint64(0x100000001) * 4096) // 2^32+1 blocks
	h := NewSCSIHandler(dev)
	var cdb [16]byte
	cdb[0] = ScsiReadCapacity10
	r := h.HandleCommand(cdb, nil)
	lastLBA := binary.BigEndian.Uint32(r.Data[0:4])
	if lastLBA != 0xFFFFFFFF {
		t.Fatalf("should return 0xFFFFFFFF for >2TB, got %d", lastLBA)
	}
}

func testReadCapacity16(t *testing.T) {
	dev := newMockDevice(3 * 1024 * 1024 * 1024 * 1024) // 3 TB
	h := NewSCSIHandler(dev)
	var cdb [16]byte
	cdb[0] = ScsiReadCapacity16
	cdb[1] = ScsiSAReadCapacity16
	binary.BigEndian.PutUint32(cdb[10:14], 32)
	r := h.HandleCommand(cdb, nil)
	if r.Status != SCSIStatusGood {
		t.Fatal("status not good")
	}
	lastLBA := binary.BigEndian.Uint64(r.Data[0:8])
	expectedBlocks := uint64(3*1024*1024*1024*1024) / 4096
	if lastLBA != expectedBlocks-1 {
		t.Fatalf("last LBA: %d, expected %d", lastLBA, expectedBlocks-1)
	}
}

func testReadCapacity16LBPME(t *testing.T) {
	dev := newMockDevice(100 * 4096)
	h := NewSCSIHandler(dev)
	var cdb [16]byte
	cdb[0] = ScsiReadCapacity16
	cdb[1] = ScsiSAReadCapacity16
	binary.BigEndian.PutUint32(cdb[10:14], 32)
	r := h.HandleCommand(cdb, nil)
	// LBPME bit should be set (byte 14, bit 7)
	if r.Data[14]&0x80 == 0 {
		t.Fatal("LBPME bit not set")
	}
}

func testModeSense6(t *testing.T) {
	dev := newMockDevice(1024 * 1024)
	h := NewSCSIHandler(dev)
	var cdb [16]byte
	cdb[0] = ScsiModeSense6
	cdb[4] = 255
	r := h.HandleCommand(cdb, nil)
	if r.Status != SCSIStatusGood {
		t.Fatal("status not good")
	}
	if len(r.Data) != 4 {
		t.Fatalf("mode sense data: %d bytes", len(r.Data))
	}
	// No write protect
	if r.Data[2]&0x80 != 0 {
		t.Fatal("write protect set")
	}
}

func testReportLuns(t *testing.T) {
	dev := newMockDevice(1024 * 1024)
	h := NewSCSIHandler(dev)
	var cdb [16]byte
	cdb[0] = ScsiReportLuns
	binary.BigEndian.PutUint32(cdb[6:10], 256)
	r := h.HandleCommand(cdb, nil)
	if r.Status != SCSIStatusGood {
		t.Fatal("status not good")
	}
	lunListLen := binary.BigEndian.Uint32(r.Data[0:4])
	if lunListLen != 8 {
		t.Fatalf("LUN list length: %d (expected 8 for 1 LUN)", lunListLen)
	}
}

func testUnknownOpcode(t *testing.T) {
	dev := newMockDevice(1024 * 1024)
	h := NewSCSIHandler(dev)
	var cdb [16]byte
	cdb[0] = 0xFF
	r := h.HandleCommand(cdb, nil)
	if r.Status != SCSIStatusCheckCond {
		t.Fatal("expected CHECK_CONDITION")
	}
	if r.SenseKey != SenseIllegalRequest {
		t.Fatal("expected ILLEGAL_REQUEST")
	}
}

func testRead10(t *testing.T) {
	dev := newMockDevice(100 * 4096)
	h := NewSCSIHandler(dev)

	// Write some data first
	data := make([]byte, 4096)
	for i := range data {
		data[i] = 0xAB
	}
	dev.blocks[5] = data

	var cdb [16]byte
	cdb[0] = ScsiRead10
	binary.BigEndian.PutUint32(cdb[2:6], 5) // LBA=5
	binary.BigEndian.PutUint16(cdb[7:9], 1) // 1 block
	r := h.HandleCommand(cdb, nil)
	if r.Status != SCSIStatusGood {
		t.Fatal("read failed")
	}
	if len(r.Data) != 4096 {
		t.Fatalf("data length: %d", len(r.Data))
	}
	if r.Data[0] != 0xAB {
		t.Fatal("data mismatch")
	}
}

func testRead16(t *testing.T) {
	dev := newMockDevice(100 * 4096)
	h := NewSCSIHandler(dev)

	data := make([]byte, 4096)
	data[0] = 0xCD
	dev.blocks[10] = data

	var cdb [16]byte
	cdb[0] = ScsiRead16
	binary.BigEndian.PutUint64(cdb[2:10], 10)
	binary.BigEndian.PutUint32(cdb[10:14], 1)
	r := h.HandleCommand(cdb, nil)
	if r.Status != SCSIStatusGood {
		t.Fatal("read16 failed")
	}
	if r.Data[0] != 0xCD {
		t.Fatal("data mismatch")
	}
}

func testWrite10(t *testing.T) {
	dev := newMockDevice(100 * 4096)
	h := NewSCSIHandler(dev)

	dataOut := make([]byte, 4096)
	dataOut[0] = 0xEF

	var cdb [16]byte
	cdb[0] = ScsiWrite10
	binary.BigEndian.PutUint32(cdb[2:6], 7)
	binary.BigEndian.PutUint16(cdb[7:9], 1)
	r := h.HandleCommand(cdb, dataOut)
	if r.Status != SCSIStatusGood {
		t.Fatal("write failed")
	}
	if dev.blocks[7][0] != 0xEF {
		t.Fatal("data not written")
	}
}

func testWrite16(t *testing.T) {
	dev := newMockDevice(100 * 4096)
	h := NewSCSIHandler(dev)

	dataOut := make([]byte, 8192)
	dataOut[0] = 0x11
	dataOut[4096] = 0x22

	var cdb [16]byte
	cdb[0] = ScsiWrite16
	binary.BigEndian.PutUint64(cdb[2:10], 50)
	binary.BigEndian.PutUint32(cdb[10:14], 2)
	r := h.HandleCommand(cdb, dataOut)
	if r.Status != SCSIStatusGood {
		t.Fatal("write16 failed")
	}
	if dev.blocks[50][0] != 0x11 {
		t.Fatal("block 50 wrong")
	}
	if dev.blocks[51][0] != 0x22 {
		t.Fatal("block 51 wrong")
	}
}

func testReadWriteRoundtrip(t *testing.T) {
	dev := newMockDevice(100 * 4096)
	h := NewSCSIHandler(dev)

	// Write
	dataOut := make([]byte, 4096)
	for i := range dataOut {
		dataOut[i] = byte(i % 256)
	}
	var wcdb [16]byte
	wcdb[0] = ScsiWrite10
	binary.BigEndian.PutUint32(wcdb[2:6], 0)
	binary.BigEndian.PutUint16(wcdb[7:9], 1)
	h.HandleCommand(wcdb, dataOut)

	// Read back
	var rcdb [16]byte
	rcdb[0] = ScsiRead10
	binary.BigEndian.PutUint32(rcdb[2:6], 0)
	binary.BigEndian.PutUint16(rcdb[7:9], 1)
	r := h.HandleCommand(rcdb, nil)
	if r.Status != SCSIStatusGood {
		t.Fatal("read failed")
	}
	for i := 0; i < 4096; i++ {
		if r.Data[i] != byte(i%256) {
			t.Fatalf("byte %d: got %d, want %d", i, r.Data[i], i%256)
		}
	}
}

func testWriteOOB(t *testing.T) {
	dev := newMockDevice(10 * 4096) // 10 blocks
	h := NewSCSIHandler(dev)

	var cdb [16]byte
	cdb[0] = ScsiWrite10
	binary.BigEndian.PutUint32(cdb[2:6], 9)
	binary.BigEndian.PutUint16(cdb[7:9], 2) // LBA 9 + 2 blocks > 10
	r := h.HandleCommand(cdb, make([]byte, 8192))
	if r.Status != SCSIStatusCheckCond {
		t.Fatal("should fail for OOB")
	}
}

func testReadOOB(t *testing.T) {
	dev := newMockDevice(10 * 4096)
	h := NewSCSIHandler(dev)

	var cdb [16]byte
	cdb[0] = ScsiRead10
	binary.BigEndian.PutUint32(cdb[2:6], 10)  // LBA 10 == total blocks
	binary.BigEndian.PutUint16(cdb[7:9], 1)
	r := h.HandleCommand(cdb, nil)
	if r.Status != SCSIStatusCheckCond {
		t.Fatal("should fail for OOB")
	}
}

func testZeroLengthTransfer(t *testing.T) {
	dev := newMockDevice(100 * 4096)
	h := NewSCSIHandler(dev)

	var cdb [16]byte
	cdb[0] = ScsiRead10
	binary.BigEndian.PutUint32(cdb[2:6], 0)
	binary.BigEndian.PutUint16(cdb[7:9], 0) // 0 blocks
	r := h.HandleCommand(cdb, nil)
	if r.Status != SCSIStatusGood {
		t.Fatal("zero-length read should succeed")
	}
}

func testSyncCache(t *testing.T) {
	dev := newMockDevice(100 * 4096)
	h := NewSCSIHandler(dev)
	var cdb [16]byte
	cdb[0] = ScsiSyncCache10
	r := h.HandleCommand(cdb, nil)
	if r.Status != SCSIStatusGood {
		t.Fatal("sync cache failed")
	}
}

func testSyncCacheError(t *testing.T) {
	dev := newMockDevice(100 * 4096)
	dev.syncErr = errors.New("disk error")
	h := NewSCSIHandler(dev)
	var cdb [16]byte
	cdb[0] = ScsiSyncCache10
	r := h.HandleCommand(cdb, nil)
	if r.Status != SCSIStatusCheckCond {
		t.Fatal("should fail")
	}
}

func testUnmapSingle(t *testing.T) {
	dev := newMockDevice(100 * 4096)
	h := NewSCSIHandler(dev)

	// Write data at LBA 5
	dev.blocks[5] = make([]byte, 4096)
	dev.blocks[5][0] = 0xFF

	// UNMAP parameter list
	unmapData := make([]byte, 24) // 8 header + 16 descriptor
	binary.BigEndian.PutUint16(unmapData[0:2], 22)  // data length
	binary.BigEndian.PutUint16(unmapData[2:4], 16)   // block desc length
	binary.BigEndian.PutUint64(unmapData[8:16], 5)   // LBA
	binary.BigEndian.PutUint32(unmapData[16:20], 1)  // num blocks

	var cdb [16]byte
	cdb[0] = ScsiUnmap
	r := h.HandleCommand(cdb, unmapData)
	if r.Status != SCSIStatusGood {
		t.Fatal("unmap failed")
	}
	if _, ok := dev.blocks[5]; ok {
		t.Fatal("block 5 should be trimmed")
	}
}

func testUnmapMultipleDescriptors(t *testing.T) {
	dev := newMockDevice(100 * 4096)
	h := NewSCSIHandler(dev)

	dev.blocks[3] = make([]byte, 4096)
	dev.blocks[7] = make([]byte, 4096)

	// 2 descriptors
	unmapData := make([]byte, 40) // 8 header + 2*16 descriptors
	binary.BigEndian.PutUint16(unmapData[0:2], 38)
	binary.BigEndian.PutUint16(unmapData[2:4], 32)
	// Descriptor 1: LBA=3, count=1
	binary.BigEndian.PutUint64(unmapData[8:16], 3)
	binary.BigEndian.PutUint32(unmapData[16:20], 1)
	// Descriptor 2: LBA=7, count=1
	binary.BigEndian.PutUint64(unmapData[24:32], 7)
	binary.BigEndian.PutUint32(unmapData[32:36], 1)

	var cdb [16]byte
	cdb[0] = ScsiUnmap
	r := h.HandleCommand(cdb, unmapData)
	if r.Status != SCSIStatusGood {
		t.Fatal("unmap failed")
	}
	if _, ok := dev.blocks[3]; ok {
		t.Fatal("block 3 should be trimmed")
	}
	if _, ok := dev.blocks[7]; ok {
		t.Fatal("block 7 should be trimmed")
	}
}

func testUnmapShortParam(t *testing.T) {
	dev := newMockDevice(100 * 4096)
	h := NewSCSIHandler(dev)
	var cdb [16]byte
	cdb[0] = ScsiUnmap
	r := h.HandleCommand(cdb, []byte{1, 2, 3}) // too short
	if r.Status != SCSIStatusCheckCond {
		t.Fatal("should fail for short unmap params")
	}
}

func testBuildSenseData(t *testing.T) {
	data := BuildSenseData(SenseIllegalRequest, ASCInvalidOpcode, ASCQLuk)
	if len(data) != 18 {
		t.Fatalf("length: %d", len(data))
	}
	if data[0] != 0x70 {
		t.Fatal("response code wrong")
	}
	if data[2] != SenseIllegalRequest {
		t.Fatal("sense key wrong")
	}
	if data[12] != ASCInvalidOpcode {
		t.Fatal("ASC wrong")
	}
}

func testReadError(t *testing.T) {
	dev := newMockDevice(100 * 4096)
	dev.readErr = errors.New("io error")
	h := NewSCSIHandler(dev)

	var cdb [16]byte
	cdb[0] = ScsiRead10
	binary.BigEndian.PutUint32(cdb[2:6], 0)
	binary.BigEndian.PutUint16(cdb[7:9], 1)
	r := h.HandleCommand(cdb, nil)
	if r.Status != SCSIStatusCheckCond {
		t.Fatal("should fail")
	}
	if r.SenseKey != SenseMediumError {
		t.Fatal("should be MEDIUM_ERROR")
	}
}

func testWriteError(t *testing.T) {
	dev := newMockDevice(100 * 4096)
	dev.writeErr = errors.New("io error")
	h := NewSCSIHandler(dev)

	var cdb [16]byte
	cdb[0] = ScsiWrite10
	binary.BigEndian.PutUint32(cdb[2:6], 0)
	binary.BigEndian.PutUint16(cdb[7:9], 1)
	r := h.HandleCommand(cdb, make([]byte, 4096))
	if r.Status != SCSIStatusCheckCond {
		t.Fatal("should fail")
	}
}

// --- WRITE SAME(16) tests ---

func testWriteSame16Unmap(t *testing.T) {
	dev := newMockDevice(100 * 4096)
	h := NewSCSIHandler(dev)

	// Write data at LBA 10-14
	for i := uint64(10); i < 15; i++ {
		block := make([]byte, 4096)
		block[0] = byte(i)
		dev.blocks[i] = block
	}

	// WRITE SAME(16) with UNMAP bit: should trim LBA 10-14
	var cdb [16]byte
	cdb[0] = ScsiWriteSame16
	cdb[1] = 0x08 // UNMAP bit
	binary.BigEndian.PutUint64(cdb[2:10], 10)
	binary.BigEndian.PutUint32(cdb[10:14], 5)

	r := h.HandleCommand(cdb, nil)
	if r.Status != SCSIStatusGood {
		t.Fatalf("status: %d", r.Status)
	}
	for i := uint64(10); i < 15; i++ {
		if _, ok := dev.blocks[i]; ok {
			t.Fatalf("block %d should be trimmed", i)
		}
	}
}

func testWriteSame16NDOB(t *testing.T) {
	dev := newMockDevice(100 * 4096)
	h := NewSCSIHandler(dev)

	dev.blocks[20] = make([]byte, 4096)
	dev.blocks[20][0] = 0xFF

	// WRITE SAME(16) with NDOB bit (no data-out buffer): zero the range
	var cdb [16]byte
	cdb[0] = ScsiWriteSame16
	cdb[1] = 0x01 // NDOB bit
	binary.BigEndian.PutUint64(cdb[2:10], 20)
	binary.BigEndian.PutUint32(cdb[10:14], 1)

	r := h.HandleCommand(cdb, nil)
	if r.Status != SCSIStatusGood {
		t.Fatalf("status: %d", r.Status)
	}
	if _, ok := dev.blocks[20]; ok {
		t.Fatal("block 20 should be trimmed")
	}
}

func testWriteSame16Pattern(t *testing.T) {
	dev := newMockDevice(100 * 4096)
	h := NewSCSIHandler(dev)

	// Write a non-zero pattern across 3 blocks
	pattern := make([]byte, 4096)
	pattern[0] = 0xAB
	pattern[4095] = 0xCD

	var cdb [16]byte
	cdb[0] = ScsiWriteSame16
	// No UNMAP, no NDOB — normal write same
	binary.BigEndian.PutUint64(cdb[2:10], 30)
	binary.BigEndian.PutUint32(cdb[10:14], 3)

	r := h.HandleCommand(cdb, pattern)
	if r.Status != SCSIStatusGood {
		t.Fatalf("status: %d", r.Status)
	}
	for i := uint64(30); i < 33; i++ {
		if dev.blocks[i][0] != 0xAB {
			t.Fatalf("block %d[0]: got %02x, want AB", i, dev.blocks[i][0])
		}
		if dev.blocks[i][4095] != 0xCD {
			t.Fatalf("block %d[4095]: got %02x, want CD", i, dev.blocks[i][4095])
		}
	}
}

func testWriteSame16Zeros(t *testing.T) {
	dev := newMockDevice(100 * 4096)
	h := NewSCSIHandler(dev)

	// Write data at LBA 40
	dev.blocks[40] = make([]byte, 4096)
	dev.blocks[40][0] = 0xFF

	// WRITE SAME with all-zero pattern — should use Trim for efficiency
	pattern := make([]byte, 4096)
	var cdb [16]byte
	cdb[0] = ScsiWriteSame16
	binary.BigEndian.PutUint64(cdb[2:10], 40)
	binary.BigEndian.PutUint32(cdb[10:14], 1)

	r := h.HandleCommand(cdb, pattern)
	if r.Status != SCSIStatusGood {
		t.Fatalf("status: %d", r.Status)
	}
	if _, ok := dev.blocks[40]; ok {
		t.Fatal("block 40 should be trimmed (zero pattern)")
	}
}

func testWriteSame16OOB(t *testing.T) {
	dev := newMockDevice(10 * 4096) // 10 blocks
	h := NewSCSIHandler(dev)

	var cdb [16]byte
	cdb[0] = ScsiWriteSame16
	cdb[1] = 0x08 // UNMAP
	binary.BigEndian.PutUint64(cdb[2:10], 8)
	binary.BigEndian.PutUint32(cdb[10:14], 5) // 8+5 > 10

	r := h.HandleCommand(cdb, nil)
	if r.Status != SCSIStatusCheckCond {
		t.Fatal("should fail for OOB")
	}
	if r.SenseASC != ASCLBAOutOfRange {
		t.Fatalf("expected LBA_OUT_OF_RANGE, got ASC=%02x", r.SenseASC)
	}
}

func testWriteSame16ZeroBlocks(t *testing.T) {
	dev := newMockDevice(100 * 4096)
	h := NewSCSIHandler(dev)

	var cdb [16]byte
	cdb[0] = ScsiWriteSame16
	binary.BigEndian.PutUint64(cdb[2:10], 0)
	binary.BigEndian.PutUint32(cdb[10:14], 0) // 0 blocks = no-op

	r := h.HandleCommand(cdb, nil)
	if r.Status != SCSIStatusGood {
		t.Fatal("zero-block write same should succeed")
	}
}

func testModeSense10(t *testing.T) {
	dev := newMockDevice(1024 * 1024)
	h := NewSCSIHandler(dev)
	var cdb [16]byte
	cdb[0] = ScsiModeSense10
	binary.BigEndian.PutUint16(cdb[7:9], 255) // alloc length
	r := h.HandleCommand(cdb, nil)
	if r.Status != SCSIStatusGood {
		t.Fatalf("status: %d", r.Status)
	}
	if len(r.Data) != 8 {
		t.Fatalf("mode sense(10) data: %d bytes, want 8", len(r.Data))
	}
	// Mode data length field (bytes 0-1) = 6
	mdl := binary.BigEndian.Uint16(r.Data[0:2])
	if mdl != 6 {
		t.Fatalf("mode data length: %d, want 6", mdl)
	}
	// No write protect (byte 3, bit 7)
	if r.Data[3]&0x80 != 0 {
		t.Fatal("write protect set")
	}
	// Block descriptor length = 0 (bytes 6-7)
	bdl := binary.BigEndian.Uint16(r.Data[6:8])
	if bdl != 0 {
		t.Fatalf("block descriptor length: %d, want 0", bdl)
	}
}

func testVPDB0BlockLimits(t *testing.T) {
	dev := newMockDevice(100 * 4096) // 100 blocks
	h := NewSCSIHandler(dev)
	var cdb [16]byte
	cdb[0] = ScsiInquiry
	cdb[1] = 0x01 // EVPD
	cdb[2] = 0xb0 // Block Limits
	binary.BigEndian.PutUint16(cdb[3:5], 255)
	r := h.HandleCommand(cdb, nil)
	if r.Status != SCSIStatusGood {
		t.Fatalf("status: %d", r.Status)
	}
	if r.Data[1] != 0xb0 {
		t.Fatalf("page code: %02x, want B0", r.Data[1])
	}
	// Page length (bytes 2-3) = 60
	pgLen := binary.BigEndian.Uint16(r.Data[2:4])
	if pgLen != 60 {
		t.Fatalf("page length: %d, want 60", pgLen)
	}
	// Max WRITE SAME length (bytes 40-47) should be 100 (total blocks)
	maxWS := binary.BigEndian.Uint64(r.Data[40:48])
	if maxWS != 100 {
		t.Fatalf("max WRITE SAME length: %d, want 100", maxWS)
	}
	// Max UNMAP LBA count (bytes 24-27) should be 100
	maxUnmap := binary.BigEndian.Uint32(r.Data[24:28])
	if maxUnmap != 100 {
		t.Fatalf("max UNMAP LBA count: %d, want 100", maxUnmap)
	}
}

func testVPDB2LogicalBlockProv(t *testing.T) {
	dev := newMockDevice(100 * 4096)
	h := NewSCSIHandler(dev)
	var cdb [16]byte
	cdb[0] = ScsiInquiry
	cdb[1] = 0x01 // EVPD
	cdb[2] = 0xb2 // Logical Block Provisioning
	binary.BigEndian.PutUint16(cdb[3:5], 255)
	r := h.HandleCommand(cdb, nil)
	if r.Status != SCSIStatusGood {
		t.Fatalf("status: %d", r.Status)
	}
	if r.Data[1] != 0xb2 {
		t.Fatalf("page code: %02x, want B2", r.Data[1])
	}
	// Provisioning type (byte 5, bits 2:0) = 0x02 (thin)
	if r.Data[5]&0x07 != 0x02 {
		t.Fatalf("provisioning type: %02x, want 02", r.Data[5]&0x07)
	}
	// LBPU=1 (byte 6, bit 7), LBPWS=1 (byte 6, bit 6)
	if r.Data[6]&0xC0 != 0xC0 {
		t.Fatalf("LBPU/LBPWS flags: %02x, want C0", r.Data[6]&0xC0)
	}
}

func testReadCapacity16InvalidSA(t *testing.T) {
	dev := newMockDevice(100 * 4096)
	h := NewSCSIHandler(dev)
	var cdb [16]byte
	cdb[0] = ScsiReadCapacity16
	cdb[1] = 0x05 // wrong service action
	r := h.HandleCommand(cdb, nil)
	if r.Status != SCSIStatusCheckCond {
		t.Fatal("should fail for wrong SA")
	}
}
