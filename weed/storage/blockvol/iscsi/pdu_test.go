package iscsi

import (
	"bytes"
	"encoding/binary"
	"io"
	"strings"
	"testing"
)

func TestPDU(t *testing.T) {
	tests := []struct {
		name string
		run  func(t *testing.T)
	}{
		{"roundtrip_bhs_only", testRoundtripBHSOnly},
		{"roundtrip_with_data", testRoundtripWithData},
		{"roundtrip_with_ahs_and_data", testRoundtripWithAHSAndData},
		{"data_padding", testDataPadding},
		{"opcode_accessors", testOpcodeAccessors},
		{"immediate_flag", testImmediateFlag},
		{"field32_accessors", testField32Accessors},
		{"lun_accessor", testLUNAccessor},
		{"isid_accessor", testISIDAccessor},
		{"cdb_accessor", testCDBAccessor},
		{"login_stage_accessors", testLoginStageAccessors},
		{"login_transit_continue", testLoginTransitContinue},
		{"login_status", testLoginStatus},
		{"scsi_response_status", testSCSIResponseStatus},
		{"data_segment_length_3byte", testDataSegmentLength3Byte},
		{"tsih_accessor", testTSIHAccessor},
		{"cmdsn_expstatsn", testCmdSNExpStatSN},
		{"statsn_expcmdsn_maxcmdsn", testStatSNExpCmdSNMaxCmdSN},
		{"datasn_bufferoffset", testDataSNBufferOffset},
		{"r2t_fields", testR2TFields},
		{"residual_count", testResidualCount},
		{"expected_data_transfer_length", testExpectedDataTransferLength},
		{"target_transfer_tag", testTargetTransferTag},
		{"opcode_name", testOpcodeName},
		{"read_truncated_bhs", testReadTruncatedBHS},
		{"read_truncated_data", testReadTruncatedData},
		{"read_truncated_ahs", testReadTruncatedAHS},
		{"read_oversized_data", testReadOversizedData},
		{"read_eof", testReadEOF},
		{"write_invalid_ahs_length", testWriteInvalidAHSLength},
		{"roundtrip_all_opcodes", testRoundtripAllOpcodes},
		{"data_segment_exact_4byte_boundary", testDataSegmentExact4ByteBoundary},
		{"zero_length_data_segment", testZeroLengthDataSegment},
		{"max_3byte_data_length", testMax3ByteDataLength},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.run(t)
		})
	}
}

func testRoundtripBHSOnly(t *testing.T) {
	p := &PDU{}
	p.SetOpcode(OpSCSICmd)
	p.SetInitiatorTaskTag(0xDEADBEEF)
	p.SetCmdSN(42)

	var buf bytes.Buffer
	if err := WritePDU(&buf, p); err != nil {
		t.Fatal(err)
	}
	if buf.Len() != BHSLength {
		t.Fatalf("expected %d bytes, got %d", BHSLength, buf.Len())
	}

	p2, err := ReadPDU(&buf)
	if err != nil {
		t.Fatal(err)
	}
	if p2.Opcode() != OpSCSICmd {
		t.Fatalf("opcode mismatch: got 0x%02x", p2.Opcode())
	}
	if p2.InitiatorTaskTag() != 0xDEADBEEF {
		t.Fatalf("ITT mismatch: got 0x%x", p2.InitiatorTaskTag())
	}
	if p2.CmdSN() != 42 {
		t.Fatalf("CmdSN mismatch: got %d", p2.CmdSN())
	}
}

func testRoundtripWithData(t *testing.T) {
	p := &PDU{}
	p.SetOpcode(OpSCSIDataIn)
	p.DataSegment = []byte("hello, iSCSI world!")

	var buf bytes.Buffer
	if err := WritePDU(&buf, p); err != nil {
		t.Fatal(err)
	}

	// BHS(48) + data(19) + padding(1) = 68
	expectedLen := 48 + pad4(uint32(len(p.DataSegment)))
	if uint32(buf.Len()) != expectedLen {
		t.Fatalf("wire length: expected %d, got %d", expectedLen, buf.Len())
	}

	p2, err := ReadPDU(&buf)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(p2.DataSegment, []byte("hello, iSCSI world!")) {
		t.Fatalf("data mismatch: %q", p2.DataSegment)
	}
}

func testRoundtripWithAHSAndData(t *testing.T) {
	p := &PDU{}
	p.SetOpcode(OpSCSICmd)
	p.AHS = make([]byte, 8) // 2 words
	p.AHS[0] = 0xAA
	p.AHS[7] = 0xBB
	p.DataSegment = []byte{1, 2, 3, 4, 5}

	var buf bytes.Buffer
	if err := WritePDU(&buf, p); err != nil {
		t.Fatal(err)
	}

	p2, err := ReadPDU(&buf)
	if err != nil {
		t.Fatal(err)
	}
	if len(p2.AHS) != 8 {
		t.Fatalf("AHS length: expected 8, got %d", len(p2.AHS))
	}
	if p2.AHS[0] != 0xAA || p2.AHS[7] != 0xBB {
		t.Fatal("AHS content mismatch")
	}
	if !bytes.Equal(p2.DataSegment, []byte{1, 2, 3, 4, 5}) {
		t.Fatal("data segment mismatch")
	}
}

func testDataPadding(t *testing.T) {
	for _, dataLen := range []int{1, 2, 3, 4, 5, 7, 8, 100, 1023} {
		p := &PDU{}
		p.SetOpcode(OpSCSIDataIn)
		p.DataSegment = bytes.Repeat([]byte{0xFF}, dataLen)

		var buf bytes.Buffer
		if err := WritePDU(&buf, p); err != nil {
			t.Fatalf("dataLen=%d: write: %v", dataLen, err)
		}

		expectedWire := BHSLength + int(pad4(uint32(dataLen)))
		if buf.Len() != expectedWire {
			t.Fatalf("dataLen=%d: wire=%d, expected=%d", dataLen, buf.Len(), expectedWire)
		}

		p2, err := ReadPDU(&buf)
		if err != nil {
			t.Fatalf("dataLen=%d: read: %v", dataLen, err)
		}
		if len(p2.DataSegment) != dataLen {
			t.Fatalf("dataLen=%d: got %d", dataLen, len(p2.DataSegment))
		}
	}
}

func testOpcodeAccessors(t *testing.T) {
	p := &PDU{}
	// Set immediate first, then opcode — verify no interference
	p.SetImmediate(true)
	p.SetOpcode(OpLoginReq)
	if p.Opcode() != OpLoginReq {
		t.Fatalf("opcode: got 0x%02x, want 0x%02x", p.Opcode(), OpLoginReq)
	}
	if !p.Immediate() {
		t.Fatal("immediate flag lost")
	}

	// Change opcode, verify immediate preserved
	p.SetOpcode(OpTextReq)
	if p.Opcode() != OpTextReq {
		t.Fatalf("opcode: got 0x%02x, want 0x%02x", p.Opcode(), OpTextReq)
	}
	if !p.Immediate() {
		t.Fatal("immediate flag lost after opcode change")
	}
}

func testImmediateFlag(t *testing.T) {
	p := &PDU{}
	if p.Immediate() {
		t.Fatal("should start false")
	}
	p.SetImmediate(true)
	if !p.Immediate() {
		t.Fatal("should be true")
	}
	p.SetImmediate(false)
	if p.Immediate() {
		t.Fatal("should be false again")
	}
}

func testField32Accessors(t *testing.T) {
	p := &PDU{}
	p.SetField32(20, 0x12345678)
	if got := p.Field32(20); got != 0x12345678 {
		t.Fatalf("got 0x%08x", got)
	}
}

func testLUNAccessor(t *testing.T) {
	p := &PDU{}
	p.SetLUN(0x0001000000000000) // LUN 1 in SAM encoding
	if p.LUN() != 0x0001000000000000 {
		t.Fatalf("LUN mismatch: 0x%016x", p.LUN())
	}
}

func testISIDAccessor(t *testing.T) {
	p := &PDU{}
	isid := [6]byte{0x00, 0x02, 0x3D, 0x00, 0x00, 0x01}
	p.SetISID(isid)
	got := p.ISID()
	if got != isid {
		t.Fatalf("ISID mismatch: %v", got)
	}
}

func testCDBAccessor(t *testing.T) {
	p := &PDU{}
	var cdb [16]byte
	cdb[0] = 0x28 // READ_10
	cdb[1] = 0x00
	binary.BigEndian.PutUint32(cdb[2:6], 100) // LBA
	binary.BigEndian.PutUint16(cdb[7:9], 8)   // transfer length
	p.SetCDB(cdb)
	got := p.CDB()
	if got != cdb {
		t.Fatal("CDB mismatch")
	}
}

func testLoginStageAccessors(t *testing.T) {
	p := &PDU{}
	p.SetLoginStages(StageSecurityNeg, StageLoginOp)
	if p.LoginCSG() != StageSecurityNeg {
		t.Fatalf("CSG: got %d", p.LoginCSG())
	}
	if p.LoginNSG() != StageLoginOp {
		t.Fatalf("NSG: got %d", p.LoginNSG())
	}

	// Change to LoginOp -> FullFeature
	p.SetLoginStages(StageLoginOp, StageFullFeature)
	if p.LoginCSG() != StageLoginOp {
		t.Fatalf("CSG: got %d", p.LoginCSG())
	}
	if p.LoginNSG() != StageFullFeature {
		t.Fatalf("NSG: got %d", p.LoginNSG())
	}
}

func testLoginTransitContinue(t *testing.T) {
	p := &PDU{}
	if p.LoginTransit() {
		t.Fatal("Transit should start false")
	}
	if p.LoginContinue() {
		t.Fatal("Continue should start false")
	}

	p.SetLoginTransit(true)
	p.SetLoginStages(StageLoginOp, StageFullFeature)
	if !p.LoginTransit() {
		t.Fatal("Transit should be true")
	}
	// Verify stages preserved
	if p.LoginCSG() != StageLoginOp {
		t.Fatal("CSG lost after setting transit")
	}
}

func testLoginStatus(t *testing.T) {
	p := &PDU{}
	p.SetLoginStatus(0x02, 0x01) // Initiator error, authentication failure
	if p.LoginStatusClass() != 0x02 {
		t.Fatalf("class: got %d", p.LoginStatusClass())
	}
	if p.LoginStatusDetail() != 0x01 {
		t.Fatalf("detail: got %d", p.LoginStatusDetail())
	}
}

func testSCSIResponseStatus(t *testing.T) {
	p := &PDU{}
	p.SetSCSIResponse(ISCSIRespCompleted)
	p.SetSCSIStatus(SCSIStatusGood)
	if p.SCSIResponse() != ISCSIRespCompleted {
		t.Fatal("response mismatch")
	}
	if p.SCSIStatus() != SCSIStatusGood {
		t.Fatal("status mismatch")
	}
}

func testDataSegmentLength3Byte(t *testing.T) {
	p := &PDU{}
	// Test various sizes including >64KB (needs all 3 bytes)
	for _, size := range []uint32{0, 1, 255, 256, 65535, 65536, 1<<24 - 1} {
		p.SetDataSegmentLength(size)
		if got := p.DataSegmentLength(); got != size {
			t.Fatalf("size %d: got %d", size, got)
		}
	}
}

func testTSIHAccessor(t *testing.T) {
	p := &PDU{}
	p.SetTSIH(0x1234)
	if p.TSIH() != 0x1234 {
		t.Fatalf("TSIH: got 0x%04x", p.TSIH())
	}
}

func testCmdSNExpStatSN(t *testing.T) {
	p := &PDU{}
	p.SetCmdSN(100)
	p.SetExpStatSN(200)
	if p.CmdSN() != 100 {
		t.Fatal("CmdSN mismatch")
	}
	if p.ExpStatSN() != 200 {
		t.Fatal("ExpStatSN mismatch")
	}
}

func testStatSNExpCmdSNMaxCmdSN(t *testing.T) {
	p := &PDU{}
	p.SetStatSN(10)
	p.SetExpCmdSN(20)
	p.SetMaxCmdSN(30)
	if p.StatSN() != 10 {
		t.Fatal("StatSN mismatch")
	}
	if p.ExpCmdSN() != 20 {
		t.Fatal("ExpCmdSN mismatch")
	}
	if p.MaxCmdSN() != 30 {
		t.Fatal("MaxCmdSN mismatch")
	}
}

func testDataSNBufferOffset(t *testing.T) {
	p := &PDU{}
	p.SetDataSN(5)
	p.SetBufferOffset(8192)
	if p.DataSN() != 5 {
		t.Fatal("DataSN mismatch")
	}
	if p.BufferOffset() != 8192 {
		t.Fatal("BufferOffset mismatch")
	}
}

func testR2TFields(t *testing.T) {
	p := &PDU{}
	p.SetR2TSN(3)
	p.SetDesiredDataLength(65536)
	if p.R2TSN() != 3 {
		t.Fatal("R2TSN mismatch")
	}
	if p.DesiredDataLength() != 65536 {
		t.Fatal("DesiredDataLength mismatch")
	}
}

func testResidualCount(t *testing.T) {
	p := &PDU{}
	p.SetResidualCount(512)
	if p.ResidualCount() != 512 {
		t.Fatal("ResidualCount mismatch")
	}
}

func testExpectedDataTransferLength(t *testing.T) {
	p := &PDU{}
	p.SetExpectedDataTransferLength(1048576)
	if p.ExpectedDataTransferLength() != 1048576 {
		t.Fatal("mismatch")
	}
}

func testTargetTransferTag(t *testing.T) {
	p := &PDU{}
	p.SetTargetTransferTag(0xFFFFFFFF)
	if p.TargetTransferTag() != 0xFFFFFFFF {
		t.Fatal("mismatch")
	}
}

func testOpcodeName(t *testing.T) {
	if OpcodeName(OpSCSICmd) != "SCSI-Command" {
		t.Fatal("wrong name for SCSI-Command")
	}
	if OpcodeName(OpLoginReq) != "Login-Request" {
		t.Fatal("wrong name for Login-Request")
	}
	if !strings.HasPrefix(OpcodeName(0xFF), "Unknown") {
		t.Fatal("unknown opcode should have Unknown prefix")
	}
}

func testReadTruncatedBHS(t *testing.T) {
	// Only 20 bytes — not enough for BHS
	buf := bytes.NewReader(make([]byte, 20))
	_, err := ReadPDU(buf)
	if err == nil {
		t.Fatal("expected error")
	}
}

func testReadTruncatedData(t *testing.T) {
	// Valid BHS claiming 100 bytes of data, but only 10 bytes follow
	var bhs [BHSLength]byte
	bhs[5] = 0
	bhs[6] = 0
	bhs[7] = 100 // DataSegmentLength = 100
	buf := make([]byte, BHSLength+10)
	copy(buf, bhs[:])

	_, err := ReadPDU(bytes.NewReader(buf))
	if err == nil {
		t.Fatal("expected truncation error")
	}
}

func testReadTruncatedAHS(t *testing.T) {
	// BHS claiming 2 words of AHS but only 4 bytes follow
	var bhs [BHSLength]byte
	bhs[4] = 2 // TotalAHSLength = 2 words = 8 bytes
	buf := make([]byte, BHSLength+4)
	copy(buf, bhs[:])

	_, err := ReadPDU(bytes.NewReader(buf))
	if err == nil {
		t.Fatal("expected truncation error")
	}
}

func testReadOversizedData(t *testing.T) {
	// BHS claiming MaxDataSegmentLength+1 bytes
	var bhs [BHSLength]byte
	oversized := uint32(MaxDataSegmentLength + 1)
	bhs[5] = byte(oversized >> 16)
	bhs[6] = byte(oversized >> 8)
	bhs[7] = byte(oversized)

	_, err := ReadPDU(bytes.NewReader(bhs[:]))
	if err == nil {
		t.Fatal("expected oversized error")
	}
	if !strings.Contains(err.Error(), "exceeds maximum") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func testReadEOF(t *testing.T) {
	_, err := ReadPDU(bytes.NewReader(nil))
	if err != io.EOF {
		t.Fatalf("expected io.EOF, got %v", err)
	}
}

func testWriteInvalidAHSLength(t *testing.T) {
	p := &PDU{}
	p.AHS = make([]byte, 5) // not multiple of 4
	var buf bytes.Buffer
	err := WritePDU(&buf, p)
	if err != ErrInvalidAHSLength {
		t.Fatalf("expected ErrInvalidAHSLength, got %v", err)
	}
}

func testRoundtripAllOpcodes(t *testing.T) {
	opcodes := []uint8{
		OpNOPOut, OpSCSICmd, OpSCSITaskMgmt, OpLoginReq, OpTextReq,
		OpSCSIDataOut, OpLogoutReq, OpSNACKReq,
		OpNOPIn, OpSCSIResp, OpSCSITaskResp, OpLoginResp, OpTextResp,
		OpSCSIDataIn, OpLogoutResp, OpR2T, OpAsyncMsg, OpReject,
	}
	for _, op := range opcodes {
		p := &PDU{}
		p.SetOpcode(op)
		var buf bytes.Buffer
		if err := WritePDU(&buf, p); err != nil {
			t.Fatalf("op=0x%02x: write: %v", op, err)
		}
		p2, err := ReadPDU(&buf)
		if err != nil {
			t.Fatalf("op=0x%02x: read: %v", op, err)
		}
		if p2.Opcode() != op {
			t.Fatalf("op=0x%02x: got 0x%02x", op, p2.Opcode())
		}
	}
}

func testDataSegmentExact4ByteBoundary(t *testing.T) {
	for _, size := range []int{4, 8, 12, 16, 256, 4096} {
		p := &PDU{}
		p.DataSegment = bytes.Repeat([]byte{0xAB}, size)
		var buf bytes.Buffer
		if err := WritePDU(&buf, p); err != nil {
			t.Fatalf("size=%d: %v", size, err)
		}
		// No padding needed
		if buf.Len() != BHSLength+size {
			t.Fatalf("size=%d: wire=%d, expected=%d", size, buf.Len(), BHSLength+size)
		}
		p2, err := ReadPDU(&buf)
		if err != nil {
			t.Fatalf("size=%d: read: %v", size, err)
		}
		if len(p2.DataSegment) != size {
			t.Fatalf("size=%d: got %d", size, len(p2.DataSegment))
		}
	}
}

func testZeroLengthDataSegment(t *testing.T) {
	p := &PDU{}
	p.SetOpcode(OpNOPOut)
	// DataSegment is nil

	var buf bytes.Buffer
	if err := WritePDU(&buf, p); err != nil {
		t.Fatal(err)
	}
	if buf.Len() != BHSLength {
		t.Fatalf("expected %d, got %d", BHSLength, buf.Len())
	}

	p2, err := ReadPDU(&buf)
	if err != nil {
		t.Fatal(err)
	}
	if len(p2.DataSegment) != 0 {
		t.Fatalf("expected empty data segment, got %d bytes", len(p2.DataSegment))
	}
}

func testMax3ByteDataLength(t *testing.T) {
	p := &PDU{}
	maxLen := uint32(1<<24 - 1) // 16MB - 1
	p.SetDataSegmentLength(maxLen)
	if got := p.DataSegmentLength(); got != maxLen {
		t.Fatalf("expected %d, got %d", maxLen, got)
	}
}
