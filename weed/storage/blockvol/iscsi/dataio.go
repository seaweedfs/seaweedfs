package iscsi

import (
	"errors"
	"io"
)

var (
	ErrDataSNOrder      = errors.New("iscsi: Data-Out DataSN out of order")
	ErrDataOffsetOrder  = errors.New("iscsi: Data-Out buffer offset out of order")
	ErrDataOverflow     = errors.New("iscsi: data exceeds expected transfer length")
	ErrDataIncomplete   = errors.New("iscsi: data transfer incomplete")
)

// DataInWriter splits a read response into multiple Data-In PDUs, respecting
// MaxRecvDataSegmentLength. The final PDU carries the S-bit (status) and F-bit.
type DataInWriter struct {
	maxSegLen uint32 // negotiated MaxRecvDataSegmentLength
}

// NewDataInWriter creates a writer with the given max segment length.
func NewDataInWriter(maxSegLen uint32) *DataInWriter {
	if maxSegLen == 0 {
		maxSegLen = 8192 // sensible default
	}
	return &DataInWriter{maxSegLen: maxSegLen}
}

// WriteDataIn splits data into Data-In PDUs and writes them to w.
// itt is the initiator task tag, ttt is the target transfer tag.
// statSN is the current StatSN (incremented when S-bit is set).
// Returns the number of PDUs written.
func (d *DataInWriter) WriteDataIn(w io.Writer, data []byte, itt uint32, expCmdSN, maxCmdSN uint32, statSN *uint32) (int, error) {
	totalLen := uint32(len(data))
	if totalLen == 0 {
		// Zero-length read — send single Data-In with S-bit, no data
		pdu := &PDU{}
		pdu.SetOpcode(OpSCSIDataIn)
		pdu.SetOpSpecific1(FlagF | FlagS) // Final + Status
		pdu.SetInitiatorTaskTag(itt)
		pdu.SetTargetTransferTag(0xFFFFFFFF)
		pdu.SetStatSN(*statSN)
		*statSN++
		pdu.SetExpCmdSN(expCmdSN)
		pdu.SetMaxCmdSN(maxCmdSN)
		pdu.SetDataSN(0)
		pdu.SetSCSIStatus(SCSIStatusGood)
		if err := WritePDU(w, pdu); err != nil {
			return 0, err
		}
		return 1, nil
	}

	var offset uint32
	var dataSN uint32
	count := 0

	for offset < totalLen {
		segLen := d.maxSegLen
		if offset+segLen > totalLen {
			segLen = totalLen - offset
		}
		isFinal := (offset + segLen) >= totalLen

		pdu := &PDU{}
		pdu.SetOpcode(OpSCSIDataIn)
		pdu.SetInitiatorTaskTag(itt)
		pdu.SetTargetTransferTag(0xFFFFFFFF)
		pdu.SetExpCmdSN(expCmdSN)
		pdu.SetMaxCmdSN(maxCmdSN)
		pdu.SetDataSN(dataSN)
		pdu.SetBufferOffset(offset)

		pdu.DataSegment = data[offset : offset+segLen]

		if isFinal {
			pdu.SetOpSpecific1(FlagF | FlagS) // Final + Status
			pdu.SetStatSN(*statSN)
			*statSN++
			pdu.SetSCSIStatus(SCSIStatusGood)
		} else {
			pdu.SetOpSpecific1(0) // no flags
		}

		if err := WritePDU(w, pdu); err != nil {
			return count, err
		}
		count++
		dataSN++
		offset += segLen
	}

	return count, nil
}

// DataOutCollector collects Data-Out PDUs for a single write command,
// assembling the full data buffer from potentially multiple PDUs.
// It handles both immediate data and R2T-solicited data.
type DataOutCollector struct {
	expectedLen uint32
	buf         []byte
	received    uint32
	nextDataSN  uint32
	done        bool
}

// NewDataOutCollector creates a collector expecting the given total transfer length.
func NewDataOutCollector(expectedLen uint32) *DataOutCollector {
	return &DataOutCollector{
		expectedLen: expectedLen,
		buf:         make([]byte, expectedLen),
	}
}

// AddImmediateData adds the data from the SCSI Command PDU (immediate data).
func (c *DataOutCollector) AddImmediateData(data []byte) error {
	if uint32(len(data)) > c.expectedLen {
		return ErrDataOverflow
	}
	copy(c.buf, data)
	c.received += uint32(len(data))
	if c.received >= c.expectedLen {
		c.done = true
	}
	return nil
}

// AddDataOut processes a Data-Out PDU and adds its data to the buffer.
func (c *DataOutCollector) AddDataOut(pdu *PDU) error {
	dataSN := pdu.DataSN()
	if dataSN != c.nextDataSN {
		return ErrDataSNOrder
	}
	c.nextDataSN++

	offset := pdu.BufferOffset()
	data := pdu.DataSegment
	end := offset + uint32(len(data))

	if end > c.expectedLen {
		return ErrDataOverflow
	}

	// Validate buffer offset matches received position (DataPDUInOrder/DataSequenceInOrder)
	if offset != c.received {
		return ErrDataOffsetOrder
	}

	copy(c.buf[offset:], data)
	c.received += uint32(len(data))

	// Check F-bit
	if pdu.OpSpecific1()&FlagF != 0 {
		c.done = true
	}

	return nil
}

// Done returns true if all expected data has been received.
func (c *DataOutCollector) Done() bool { return c.done }

// Data returns the assembled data buffer.
func (c *DataOutCollector) Data() []byte { return c.buf }

// Remaining returns how many bytes are still needed.
func (c *DataOutCollector) Remaining() uint32 {
	if c.received >= c.expectedLen {
		return 0
	}
	return c.expectedLen - c.received
}

// BuildR2T creates an R2T PDU requesting more data from the initiator.
func BuildR2T(itt, ttt uint32, r2tSN uint32, bufferOffset, desiredLen uint32, statSN, expCmdSN, maxCmdSN uint32) *PDU {
	pdu := &PDU{}
	pdu.SetOpcode(OpR2T)
	pdu.SetOpSpecific1(FlagF) // always Final for R2T
	pdu.SetInitiatorTaskTag(itt)
	pdu.SetTargetTransferTag(ttt)
	pdu.SetStatSN(statSN)
	pdu.SetExpCmdSN(expCmdSN)
	pdu.SetMaxCmdSN(maxCmdSN)
	pdu.SetR2TSN(r2tSN)
	pdu.SetBufferOffset(bufferOffset)
	pdu.SetDesiredDataLength(desiredLen)
	return pdu
}

// SendSCSIResponse sends a SCSI Response PDU with optional sense data.
func SendSCSIResponse(w io.Writer, result SCSIResult, itt uint32, statSN *uint32, expCmdSN, maxCmdSN uint32) error {
	pdu := &PDU{}
	pdu.SetOpcode(OpSCSIResp)
	pdu.SetOpSpecific1(FlagF) // Final
	pdu.SetSCSIResponse(ISCSIRespCompleted)
	pdu.SetSCSIStatus(result.Status)
	pdu.SetInitiatorTaskTag(itt)
	pdu.SetStatSN(*statSN)
	*statSN++
	pdu.SetExpCmdSN(expCmdSN)
	pdu.SetMaxCmdSN(maxCmdSN)

	if result.Status == SCSIStatusCheckCond {
		senseData := BuildSenseData(result.SenseKey, result.SenseASC, result.SenseASCQ)
		// Sense data is wrapped in a 2-byte length prefix
		pdu.DataSegment = make([]byte, 2+len(senseData))
		pdu.DataSegment[0] = byte(len(senseData) >> 8)
		pdu.DataSegment[1] = byte(len(senseData))
		copy(pdu.DataSegment[2:], senseData)
	}

	return WritePDU(w, pdu)
}
