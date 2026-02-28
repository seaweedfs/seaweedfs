package iscsi

import (
	"bytes"
	"testing"
)

func TestDataIO(t *testing.T) {
	tests := []struct {
		name string
		run  func(t *testing.T)
	}{
		{"datain_single_pdu", testDataInSinglePDU},
		{"datain_multi_pdu", testDataInMultiPDU},
		{"datain_exact_boundary", testDataInExactBoundary},
		{"datain_zero_length", testDataInZeroLength},
		{"datain_datasn_ordering", testDataInDataSNOrdering},
		{"datain_fbit_sbit", testDataInFbitSbit},
		{"dataout_single_pdu", testDataOutSinglePDU},
		{"dataout_multi_pdu", testDataOutMultiPDU},
		{"dataout_immediate_data", testDataOutImmediateData},
		{"dataout_immediate_plus_r2t", testDataOutImmediatePlusR2T},
		{"dataout_wrong_datasn", testDataOutWrongDataSN},
		{"dataout_overflow", testDataOutOverflow},
		{"r2t_build", testR2TBuild},
		{"scsi_response_good", testSCSIResponseGood},
		{"scsi_response_check_condition", testSCSIResponseCheckCondition},
		{"datain_statsn_increment", testDataInStatSNIncrement},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.run(t)
		})
	}
}

func testDataInSinglePDU(t *testing.T) {
	w := &bytes.Buffer{}
	dw := NewDataInWriter(8192)
	data := bytes.Repeat([]byte{0xAA}, 4096)
	statSN := uint32(1)

	n, err := dw.WriteDataIn(w, data, 0x100, 1, 10, &statSN)
	if err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("expected 1 PDU, got %d", n)
	}
	if statSN != 2 {
		t.Fatalf("StatSN should be 2, got %d", statSN)
	}

	pdu, err := ReadPDU(w)
	if err != nil {
		t.Fatal(err)
	}
	if pdu.Opcode() != OpSCSIDataIn {
		t.Fatal("wrong opcode")
	}
	if pdu.OpSpecific1()&FlagF == 0 {
		t.Fatal("F-bit not set")
	}
	if pdu.OpSpecific1()&FlagS == 0 {
		t.Fatal("S-bit not set on final PDU")
	}
	if len(pdu.DataSegment) != 4096 {
		t.Fatalf("data length: %d", len(pdu.DataSegment))
	}
}

func testDataInMultiPDU(t *testing.T) {
	w := &bytes.Buffer{}
	dw := NewDataInWriter(1024) // small segment
	data := bytes.Repeat([]byte{0xBB}, 3000)
	statSN := uint32(1)

	n, err := dw.WriteDataIn(w, data, 0x200, 1, 10, &statSN)
	if err != nil {
		t.Fatal(err)
	}
	// 3000 / 1024 = 3 PDUs (1024 + 1024 + 952)
	if n != 3 {
		t.Fatalf("expected 3 PDUs, got %d", n)
	}

	// Read them back
	var reassembled []byte
	for i := 0; i < 3; i++ {
		pdu, err := ReadPDU(w)
		if err != nil {
			t.Fatalf("PDU %d: %v", i, err)
		}
		if pdu.DataSN() != uint32(i) {
			t.Fatalf("PDU %d: DataSN=%d", i, pdu.DataSN())
		}
		reassembled = append(reassembled, pdu.DataSegment...)

		if i < 2 {
			if pdu.OpSpecific1()&FlagF != 0 {
				t.Fatalf("PDU %d should not have F-bit", i)
			}
		} else {
			if pdu.OpSpecific1()&FlagF == 0 {
				t.Fatal("last PDU should have F-bit")
			}
			if pdu.OpSpecific1()&FlagS == 0 {
				t.Fatal("last PDU should have S-bit")
			}
		}
	}

	if !bytes.Equal(reassembled, data) {
		t.Fatal("reassembled data mismatch")
	}
}

func testDataInExactBoundary(t *testing.T) {
	w := &bytes.Buffer{}
	dw := NewDataInWriter(1024)
	data := bytes.Repeat([]byte{0xCC}, 2048) // exact 2 PDUs
	statSN := uint32(1)

	n, err := dw.WriteDataIn(w, data, 0x300, 1, 10, &statSN)
	if err != nil {
		t.Fatal(err)
	}
	if n != 2 {
		t.Fatalf("expected 2 PDUs, got %d", n)
	}
}

func testDataInZeroLength(t *testing.T) {
	w := &bytes.Buffer{}
	dw := NewDataInWriter(8192)
	statSN := uint32(5)

	n, err := dw.WriteDataIn(w, nil, 0x400, 1, 10, &statSN)
	if err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("expected 1 PDU for zero-length, got %d", n)
	}
	if statSN != 6 {
		t.Fatal("StatSN should still increment")
	}
}

func testDataInDataSNOrdering(t *testing.T) {
	w := &bytes.Buffer{}
	dw := NewDataInWriter(512)
	data := bytes.Repeat([]byte{0xDD}, 2048) // 4 PDUs
	statSN := uint32(1)

	dw.WriteDataIn(w, data, 0x500, 1, 10, &statSN)

	for i := 0; i < 4; i++ {
		pdu, _ := ReadPDU(w)
		if pdu.DataSN() != uint32(i) {
			t.Fatalf("PDU %d: DataSN=%d", i, pdu.DataSN())
		}
		expectedOffset := uint32(i) * 512
		if pdu.BufferOffset() != expectedOffset {
			t.Fatalf("PDU %d: offset=%d, expected %d", i, pdu.BufferOffset(), expectedOffset)
		}
	}
}

func testDataInFbitSbit(t *testing.T) {
	w := &bytes.Buffer{}
	dw := NewDataInWriter(1000)
	data := bytes.Repeat([]byte{0xEE}, 2500)
	statSN := uint32(1)

	dw.WriteDataIn(w, data, 0x600, 1, 10, &statSN)

	for i := 0; i < 3; i++ {
		pdu, _ := ReadPDU(w)
		flags := pdu.OpSpecific1()
		if i < 2 {
			if flags&FlagF != 0 || flags&FlagS != 0 {
				t.Fatalf("PDU %d: should have no F/S bits", i)
			}
		} else {
			if flags&FlagF == 0 || flags&FlagS == 0 {
				t.Fatal("last PDU must have F+S bits")
			}
		}
	}
}

func testDataOutSinglePDU(t *testing.T) {
	c := NewDataOutCollector(4096)

	pdu := &PDU{}
	pdu.SetOpcode(OpSCSIDataOut)
	pdu.SetOpSpecific1(FlagF)
	pdu.SetDataSN(0)
	pdu.SetBufferOffset(0)
	pdu.DataSegment = bytes.Repeat([]byte{0x11}, 4096)

	if err := c.AddDataOut(pdu); err != nil {
		t.Fatal(err)
	}
	if !c.Done() {
		t.Fatal("should be done")
	}
	if c.Remaining() != 0 {
		t.Fatal("remaining should be 0")
	}
}

func testDataOutMultiPDU(t *testing.T) {
	c := NewDataOutCollector(8192)

	for i := 0; i < 2; i++ {
		pdu := &PDU{}
		pdu.SetOpcode(OpSCSIDataOut)
		pdu.SetDataSN(uint32(i))
		pdu.SetBufferOffset(uint32(i) * 4096)
		pdu.DataSegment = bytes.Repeat([]byte{byte(i + 1)}, 4096)
		if i == 1 {
			pdu.SetOpSpecific1(FlagF)
		}
		if err := c.AddDataOut(pdu); err != nil {
			t.Fatalf("PDU %d: %v", i, err)
		}
	}

	if !c.Done() {
		t.Fatal("should be done")
	}
	data := c.Data()
	if data[0] != 0x01 || data[4096] != 0x02 {
		t.Fatal("data assembly wrong")
	}
}

func testDataOutImmediateData(t *testing.T) {
	c := NewDataOutCollector(4096)
	err := c.AddImmediateData(bytes.Repeat([]byte{0xFF}, 4096))
	if err != nil {
		t.Fatal(err)
	}
	if !c.Done() {
		t.Fatal("should be done with immediate data")
	}
}

func testDataOutImmediatePlusR2T(t *testing.T) {
	c := NewDataOutCollector(8192)

	// Immediate: first 4096
	err := c.AddImmediateData(bytes.Repeat([]byte{0xAA}, 4096))
	if err != nil {
		t.Fatal(err)
	}
	if c.Done() {
		t.Fatal("should not be done yet")
	}
	if c.Remaining() != 4096 {
		t.Fatalf("remaining: %d", c.Remaining())
	}

	// R2T-solicited Data-Out: next 4096
	pdu := &PDU{}
	pdu.SetOpcode(OpSCSIDataOut)
	pdu.SetOpSpecific1(FlagF)
	pdu.SetDataSN(0)
	pdu.SetBufferOffset(4096)
	pdu.DataSegment = bytes.Repeat([]byte{0xBB}, 4096)
	if err := c.AddDataOut(pdu); err != nil {
		t.Fatal(err)
	}
	if !c.Done() {
		t.Fatal("should be done")
	}

	data := c.Data()
	if data[0] != 0xAA || data[4096] != 0xBB {
		t.Fatal("assembly wrong")
	}
}

func testDataOutWrongDataSN(t *testing.T) {
	c := NewDataOutCollector(8192)

	pdu := &PDU{}
	pdu.SetOpcode(OpSCSIDataOut)
	pdu.SetDataSN(1) // should be 0
	pdu.SetBufferOffset(0)
	pdu.DataSegment = make([]byte, 4096)

	err := c.AddDataOut(pdu)
	if err != ErrDataSNOrder {
		t.Fatalf("expected ErrDataSNOrder, got %v", err)
	}
}

func testDataOutOverflow(t *testing.T) {
	c := NewDataOutCollector(4096)

	pdu := &PDU{}
	pdu.SetOpcode(OpSCSIDataOut)
	pdu.SetDataSN(0)
	pdu.SetBufferOffset(0)
	pdu.DataSegment = make([]byte, 8192) // more than expected

	err := c.AddDataOut(pdu)
	if err != ErrDataOverflow {
		t.Fatalf("expected ErrDataOverflow, got %v", err)
	}
}

func testR2TBuild(t *testing.T) {
	pdu := BuildR2T(0x100, 0x200, 0, 4096, 4096, 5, 10, 20)
	if pdu.Opcode() != OpR2T {
		t.Fatal("wrong opcode")
	}
	if pdu.InitiatorTaskTag() != 0x100 {
		t.Fatal("ITT wrong")
	}
	if pdu.TargetTransferTag() != 0x200 {
		t.Fatal("TTT wrong")
	}
	if pdu.R2TSN() != 0 {
		t.Fatal("R2TSN wrong")
	}
	if pdu.BufferOffset() != 4096 {
		t.Fatal("offset wrong")
	}
	if pdu.DesiredDataLength() != 4096 {
		t.Fatal("desired length wrong")
	}
	if pdu.StatSN() != 5 {
		t.Fatal("StatSN wrong")
	}
}

func testSCSIResponseGood(t *testing.T) {
	w := &bytes.Buffer{}
	statSN := uint32(10)
	result := SCSIResult{Status: SCSIStatusGood}
	err := SendSCSIResponse(w, result, 0x300, &statSN, 5, 15)
	if err != nil {
		t.Fatal(err)
	}
	if statSN != 11 {
		t.Fatal("StatSN not incremented")
	}

	pdu, err := ReadPDU(w)
	if err != nil {
		t.Fatal(err)
	}
	if pdu.Opcode() != OpSCSIResp {
		t.Fatal("wrong opcode")
	}
	if pdu.SCSIStatus() != SCSIStatusGood {
		t.Fatal("status wrong")
	}
	if len(pdu.DataSegment) != 0 {
		t.Fatal("no data expected for good status")
	}
}

func testSCSIResponseCheckCondition(t *testing.T) {
	w := &bytes.Buffer{}
	statSN := uint32(20)
	result := SCSIResult{
		Status:    SCSIStatusCheckCond,
		SenseKey:  SenseIllegalRequest,
		SenseASC:  ASCInvalidOpcode,
		SenseASCQ: ASCQLuk,
	}
	err := SendSCSIResponse(w, result, 0x400, &statSN, 5, 15)
	if err != nil {
		t.Fatal(err)
	}

	pdu, err := ReadPDU(w)
	if err != nil {
		t.Fatal(err)
	}
	if pdu.SCSIStatus() != SCSIStatusCheckCond {
		t.Fatal("status wrong")
	}
	// Data segment should contain sense data with 2-byte length prefix
	if len(pdu.DataSegment) < 20 { // 2 + 18
		t.Fatalf("sense data too short: %d", len(pdu.DataSegment))
	}
	senseLen := int(pdu.DataSegment[0])<<8 | int(pdu.DataSegment[1])
	if senseLen != 18 {
		t.Fatalf("sense length: %d", senseLen)
	}
}

func testDataInStatSNIncrement(t *testing.T) {
	w := &bytes.Buffer{}
	dw := NewDataInWriter(1024)
	data := bytes.Repeat([]byte{0x00}, 3072) // 3 PDUs
	statSN := uint32(100)

	dw.WriteDataIn(w, data, 0x700, 1, 10, &statSN)
	// Only the final PDU has S-bit, so StatSN increments once
	if statSN != 101 {
		t.Fatalf("StatSN should be 101, got %d", statSN)
	}
}
