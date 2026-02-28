package iscsi

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
)

// BHS (Basic Header Segment) is 48 bytes, big-endian.
// RFC 7143, Section 12.1
const BHSLength = 48

// Opcode constants — initiator opcodes (RFC 7143, Section 12.1.1)
const (
	OpNOPOut        uint8 = 0x00
	OpSCSICmd       uint8 = 0x01
	OpSCSITaskMgmt  uint8 = 0x02
	OpLoginReq      uint8 = 0x03
	OpTextReq       uint8 = 0x04
	OpSCSIDataOut   uint8 = 0x05
	OpLogoutReq     uint8 = 0x06
	OpSNACKReq      uint8 = 0x0c
)

// Target opcodes (RFC 7143, Section 12.1.1)
const (
	OpNOPIn        uint8 = 0x20
	OpSCSIResp     uint8 = 0x21
	OpSCSITaskResp uint8 = 0x22
	OpLoginResp    uint8 = 0x23
	OpTextResp     uint8 = 0x24
	OpSCSIDataIn   uint8 = 0x25
	OpLogoutResp   uint8 = 0x26
	OpR2T          uint8 = 0x31
	OpAsyncMsg     uint8 = 0x32
	OpReject       uint8 = 0x3f
)

// BHS flag masks
const (
	opcMask    = 0x3f // lower 6 bits of byte 0
	FlagI      = 0x40 // Immediate delivery bit (byte 0)
	FlagF      = 0x80 // Final bit (byte 1)
	FlagR      = 0x40 // Read bit (byte 1, SCSI command)
	FlagW      = 0x20 // Write bit (byte 1, SCSI command)
	FlagC      = 0x40 // Continue bit (byte 1, login)
	FlagT      = 0x80 // Transit bit (byte 1, login)
	FlagS      = 0x01 // Status bit (byte 1, Data-In)
	FlagU      = 0x02 // Underflow bit (byte 1, SCSI Response)
	FlagO      = 0x04 // Overflow bit (byte 1, SCSI Response)
	FlagBiU    = 0x08 // Bidi underflow (byte 1, SCSI Response)
	FlagBiO    = 0x10 // Bidi overflow (byte 1, SCSI Response)
	FlagA      = 0x40 // Acknowledge bit (byte 1, Data-Out)
)

// Login stage constants (CSG/NSG, 2-bit fields in byte 1 of login PDU)
const (
	StageSecurityNeg uint8 = 0 // Security Negotiation
	StageLoginOp     uint8 = 1 // Login Operational Negotiation
	StageFullFeature uint8 = 3 // Full Feature Phase
)

// SCSI status codes
const (
	SCSIStatusGood          uint8 = 0x00
	SCSIStatusCheckCond     uint8 = 0x02
	SCSIStatusBusy          uint8 = 0x08
	SCSIStatusResvConflict  uint8 = 0x18
)

// iSCSI response codes
const (
	ISCSIRespCompleted uint8 = 0x00
)

// MaxDataSegmentLength limits the maximum data segment we'll accept.
// The iSCSI data segment length is a 3-byte field (max 16MB-1).
// We cap at 8MB which is well above typical MaxRecvDataSegmentLength values.
const MaxDataSegmentLength = 8 * 1024 * 1024 // 8 MB

var (
	ErrPDUTruncated     = errors.New("iscsi: PDU truncated")
	ErrPDUTooLarge      = errors.New("iscsi: data segment exceeds maximum length")
	ErrInvalidAHSLength = errors.New("iscsi: invalid AHS length (not multiple of 4)")
	ErrUnknownOpcode    = errors.New("iscsi: unknown opcode")
)

// PDU represents a full iSCSI Protocol Data Unit.
type PDU struct {
	BHS           [BHSLength]byte
	AHS           []byte // Additional Header Segment (multiple of 4 bytes)
	DataSegment   []byte // Data segment (padded to 4-byte boundary on wire)
}

// --- BHS field accessors ---

// Opcode returns the opcode (lower 6 bits of byte 0).
func (p *PDU) Opcode() uint8 { return p.BHS[0] & opcMask }

// SetOpcode sets the opcode (lower 6 bits of byte 0).
func (p *PDU) SetOpcode(op uint8) {
	p.BHS[0] = (p.BHS[0] & ^uint8(opcMask)) | (op & opcMask)
}

// Immediate returns true if the immediate delivery bit is set.
func (p *PDU) Immediate() bool { return p.BHS[0]&FlagI != 0 }

// SetImmediate sets the immediate delivery bit.
func (p *PDU) SetImmediate(v bool) {
	if v {
		p.BHS[0] |= FlagI
	} else {
		p.BHS[0] &^= FlagI
	}
}

// OpSpecific1 returns byte 1 (opcode-specific flags).
func (p *PDU) OpSpecific1() uint8 { return p.BHS[1] }

// SetOpSpecific1 sets byte 1.
func (p *PDU) SetOpSpecific1(v uint8) { p.BHS[1] = v }

// TotalAHSLength returns the total AHS length in 4-byte words (byte 4).
func (p *PDU) TotalAHSLength() uint8 { return p.BHS[4] }

// DataSegmentLength returns the 3-byte data segment length (bytes 5-7).
func (p *PDU) DataSegmentLength() uint32 {
	return uint32(p.BHS[5])<<16 | uint32(p.BHS[6])<<8 | uint32(p.BHS[7])
}

// SetDataSegmentLength sets the 3-byte data segment length field.
func (p *PDU) SetDataSegmentLength(n uint32) {
	p.BHS[5] = byte(n >> 16)
	p.BHS[6] = byte(n >> 8)
	p.BHS[7] = byte(n)
}

// LUN returns the 8-byte LUN field (bytes 8-15).
func (p *PDU) LUN() uint64 { return binary.BigEndian.Uint64(p.BHS[8:16]) }

// SetLUN sets the LUN field.
func (p *PDU) SetLUN(lun uint64) { binary.BigEndian.PutUint64(p.BHS[8:16], lun) }

// InitiatorTaskTag returns bytes 16-19.
func (p *PDU) InitiatorTaskTag() uint32 { return binary.BigEndian.Uint32(p.BHS[16:20]) }

// SetInitiatorTaskTag sets bytes 16-19.
func (p *PDU) SetInitiatorTaskTag(tag uint32) { binary.BigEndian.PutUint32(p.BHS[16:20], tag) }

// Field32 reads a generic 4-byte field at the given BHS offset.
func (p *PDU) Field32(offset int) uint32 { return binary.BigEndian.Uint32(p.BHS[offset : offset+4]) }

// SetField32 writes a generic 4-byte field at the given BHS offset.
func (p *PDU) SetField32(offset int, v uint32) {
	binary.BigEndian.PutUint32(p.BHS[offset:offset+4], v)
}

// --- Common BHS offsets for named fields ---

// TSIH returns the Target Session Identifying Handle (bytes 14-15, login PDU).
func (p *PDU) TSIH() uint16 { return binary.BigEndian.Uint16(p.BHS[14:16]) }

// SetTSIH sets the TSIH field.
func (p *PDU) SetTSIH(v uint16) { binary.BigEndian.PutUint16(p.BHS[14:16], v) }

// CmdSN returns bytes 24-27.
func (p *PDU) CmdSN() uint32 { return binary.BigEndian.Uint32(p.BHS[24:28]) }

// SetCmdSN sets bytes 24-27.
func (p *PDU) SetCmdSN(v uint32) { binary.BigEndian.PutUint32(p.BHS[24:28], v) }

// ExpStatSN returns bytes 28-31.
func (p *PDU) ExpStatSN() uint32 { return binary.BigEndian.Uint32(p.BHS[28:32]) }

// SetExpStatSN sets bytes 28-31.
func (p *PDU) SetExpStatSN(v uint32) { binary.BigEndian.PutUint32(p.BHS[28:32], v) }

// StatSN returns bytes 24-27 (target response PDUs).
func (p *PDU) StatSN() uint32 { return binary.BigEndian.Uint32(p.BHS[24:28]) }

// SetStatSN sets bytes 24-27.
func (p *PDU) SetStatSN(v uint32) { binary.BigEndian.PutUint32(p.BHS[24:28], v) }

// ExpCmdSN returns bytes 28-31 (target response PDUs).
func (p *PDU) ExpCmdSN() uint32 { return binary.BigEndian.Uint32(p.BHS[28:32]) }

// SetExpCmdSN sets bytes 28-31.
func (p *PDU) SetExpCmdSN(v uint32) { binary.BigEndian.PutUint32(p.BHS[28:32], v) }

// MaxCmdSN returns bytes 32-35 (target response PDUs).
func (p *PDU) MaxCmdSN() uint32 { return binary.BigEndian.Uint32(p.BHS[32:36]) }

// SetMaxCmdSN sets bytes 32-35.
func (p *PDU) SetMaxCmdSN(v uint32) { binary.BigEndian.PutUint32(p.BHS[32:36], v) }

// DataSN returns bytes 36-39 (Data-In / Data-Out PDUs).
func (p *PDU) DataSN() uint32 { return binary.BigEndian.Uint32(p.BHS[36:40]) }

// SetDataSN sets bytes 36-39.
func (p *PDU) SetDataSN(v uint32) { binary.BigEndian.PutUint32(p.BHS[36:40], v) }

// BufferOffset returns bytes 40-43 (Data-In / Data-Out PDUs).
func (p *PDU) BufferOffset() uint32 { return binary.BigEndian.Uint32(p.BHS[40:44]) }

// SetBufferOffset sets bytes 40-43.
func (p *PDU) SetBufferOffset(v uint32) { binary.BigEndian.PutUint32(p.BHS[40:44], v) }

// R2TSN returns bytes 36-39 (R2T PDUs).
func (p *PDU) R2TSN() uint32 { return binary.BigEndian.Uint32(p.BHS[36:40]) }

// SetR2TSN sets bytes 36-39.
func (p *PDU) SetR2TSN(v uint32) { binary.BigEndian.PutUint32(p.BHS[36:40], v) }

// DesiredDataLength returns bytes 44-47 (R2T PDUs).
func (p *PDU) DesiredDataLength() uint32 { return binary.BigEndian.Uint32(p.BHS[44:48]) }

// SetDesiredDataLength sets bytes 44-47.
func (p *PDU) SetDesiredDataLength(v uint32) { binary.BigEndian.PutUint32(p.BHS[44:48], v) }

// ExpectedDataTransferLength returns bytes 20-23 (SCSI Command PDUs).
func (p *PDU) ExpectedDataTransferLength() uint32 { return binary.BigEndian.Uint32(p.BHS[20:24]) }

// SetExpectedDataTransferLength sets bytes 20-23.
func (p *PDU) SetExpectedDataTransferLength(v uint32) {
	binary.BigEndian.PutUint32(p.BHS[20:24], v)
}

// TargetTransferTag returns bytes 20-23 (target PDUs: R2T, Data-In, etc.).
func (p *PDU) TargetTransferTag() uint32 { return binary.BigEndian.Uint32(p.BHS[20:24]) }

// SetTargetTransferTag sets bytes 20-23.
func (p *PDU) SetTargetTransferTag(v uint32) { binary.BigEndian.PutUint32(p.BHS[20:24], v) }

// ISID returns the 6-byte Initiator Session ID (bytes 8-13, login PDU).
func (p *PDU) ISID() [6]byte {
	var id [6]byte
	copy(id[:], p.BHS[8:14])
	return id
}

// SetISID sets the 6-byte ISID field.
func (p *PDU) SetISID(id [6]byte) { copy(p.BHS[8:14], id[:]) }

// CDB returns the 16-byte SCSI CDB from the BHS (bytes 32-47, SCSI Command PDU).
func (p *PDU) CDB() [16]byte {
	var cdb [16]byte
	copy(cdb[:], p.BHS[32:48])
	return cdb
}

// SetCDB sets the 16-byte SCSI CDB in the BHS.
func (p *PDU) SetCDB(cdb [16]byte) { copy(p.BHS[32:48], cdb[:]) }

// ResidualCount returns bytes 44-47 (SCSI Response PDUs).
func (p *PDU) ResidualCount() uint32 { return binary.BigEndian.Uint32(p.BHS[44:48]) }

// SetResidualCount sets bytes 44-47.
func (p *PDU) SetResidualCount(v uint32) { binary.BigEndian.PutUint32(p.BHS[44:48], v) }

// --- Wire I/O ---

// pad4 rounds n up to the next multiple of 4.
func pad4(n uint32) uint32 { return (n + 3) &^ 3 }

// ReadPDU reads a complete PDU from r.
func ReadPDU(r io.Reader) (*PDU, error) {
	p := &PDU{}

	// Read BHS (48 bytes)
	if _, err := io.ReadFull(r, p.BHS[:]); err != nil {
		if err == io.ErrUnexpectedEOF {
			return nil, ErrPDUTruncated
		}
		return nil, err
	}

	// AHS
	ahsLen := uint32(p.TotalAHSLength()) * 4
	if ahsLen > 0 {
		p.AHS = make([]byte, ahsLen)
		if _, err := io.ReadFull(r, p.AHS); err != nil {
			if err == io.ErrUnexpectedEOF {
				return nil, ErrPDUTruncated
			}
			return nil, err
		}
	}

	// Data segment
	dsLen := p.DataSegmentLength()
	if dsLen > MaxDataSegmentLength {
		return nil, fmt.Errorf("%w: %d bytes", ErrPDUTooLarge, dsLen)
	}

	if dsLen > 0 {
		paddedLen := pad4(dsLen)
		buf := make([]byte, paddedLen)
		if _, err := io.ReadFull(r, buf); err != nil {
			if err == io.ErrUnexpectedEOF {
				return nil, ErrPDUTruncated
			}
			return nil, err
		}
		p.DataSegment = buf[:dsLen] // strip padding
	}

	return p, nil
}

// WritePDU writes a complete PDU to w, with proper padding.
func WritePDU(w io.Writer, p *PDU) error {
	// Update header lengths from actual AHS/DataSegment
	if len(p.AHS) > 0 {
		if len(p.AHS)%4 != 0 {
			return ErrInvalidAHSLength
		}
		p.BHS[4] = uint8(len(p.AHS) / 4)
	} else {
		p.BHS[4] = 0
	}
	p.SetDataSegmentLength(uint32(len(p.DataSegment)))

	// Write BHS
	if _, err := w.Write(p.BHS[:]); err != nil {
		return err
	}

	// Write AHS
	if len(p.AHS) > 0 {
		if _, err := w.Write(p.AHS); err != nil {
			return err
		}
	}

	// Write data segment with padding
	if len(p.DataSegment) > 0 {
		if _, err := w.Write(p.DataSegment); err != nil {
			return err
		}
		padLen := pad4(uint32(len(p.DataSegment))) - uint32(len(p.DataSegment))
		if padLen > 0 {
			var pad [3]byte
			if _, err := w.Write(pad[:padLen]); err != nil {
				return err
			}
		}
	}

	return nil
}

// OpcodeName returns a human-readable name for the given opcode.
func OpcodeName(op uint8) string {
	switch op {
	case OpNOPOut:
		return "NOP-Out"
	case OpSCSICmd:
		return "SCSI-Command"
	case OpSCSITaskMgmt:
		return "SCSI-Task-Mgmt"
	case OpLoginReq:
		return "Login-Request"
	case OpTextReq:
		return "Text-Request"
	case OpSCSIDataOut:
		return "SCSI-Data-Out"
	case OpLogoutReq:
		return "Logout-Request"
	case OpSNACKReq:
		return "SNACK-Request"
	case OpNOPIn:
		return "NOP-In"
	case OpSCSIResp:
		return "SCSI-Response"
	case OpSCSITaskResp:
		return "SCSI-Task-Mgmt-Response"
	case OpLoginResp:
		return "Login-Response"
	case OpTextResp:
		return "Text-Response"
	case OpSCSIDataIn:
		return "SCSI-Data-In"
	case OpLogoutResp:
		return "Logout-Response"
	case OpR2T:
		return "R2T"
	case OpAsyncMsg:
		return "Async-Message"
	case OpReject:
		return "Reject"
	default:
		return fmt.Sprintf("Unknown(0x%02x)", op)
	}
}

// Login PDU helpers

// LoginCSG returns the Current Stage from byte 1 of a login PDU (bits 3-2).
func (p *PDU) LoginCSG() uint8 { return (p.BHS[1] >> 2) & 0x03 }

// LoginNSG returns the Next Stage from byte 1 of a login PDU (bits 1-0).
func (p *PDU) LoginNSG() uint8 { return p.BHS[1] & 0x03 }

// SetLoginStages sets the CSG and NSG fields in byte 1, preserving T and C flags.
func (p *PDU) SetLoginStages(csg, nsg uint8) {
	p.BHS[1] = (p.BHS[1] & 0xF0) | ((csg & 0x03) << 2) | (nsg & 0x03)
}

// LoginTransit returns true if the Transit bit is set (byte 1, bit 7).
func (p *PDU) LoginTransit() bool { return p.BHS[1]&FlagT != 0 }

// SetLoginTransit sets or clears the Transit bit.
func (p *PDU) SetLoginTransit(v bool) {
	if v {
		p.BHS[1] |= FlagT
	} else {
		p.BHS[1] &^= FlagT
	}
}

// LoginContinue returns true if the Continue bit is set.
func (p *PDU) LoginContinue() bool { return p.BHS[1]&FlagC != 0 }

// LoginStatusClass returns the status class (byte 36) of a login response.
func (p *PDU) LoginStatusClass() uint8 { return p.BHS[36] }

// LoginStatusDetail returns the status detail (byte 37) of a login response.
func (p *PDU) LoginStatusDetail() uint8 { return p.BHS[37] }

// SetLoginStatus sets the status class and detail in a login response.
func (p *PDU) SetLoginStatus(class, detail uint8) {
	p.BHS[36] = class
	p.BHS[37] = detail
}

// SCSIResponse returns the iSCSI response byte (byte 2) of a SCSI Response PDU.
func (p *PDU) SCSIResponse() uint8 { return p.BHS[2] }

// SetSCSIResponse sets byte 2.
func (p *PDU) SetSCSIResponse(v uint8) { p.BHS[2] = v }

// SCSIStatus returns the SCSI status byte (byte 3) of a SCSI Response PDU.
func (p *PDU) SCSIStatus() uint8 { return p.BHS[3] }

// SetSCSIStatus sets byte 3.
func (p *PDU) SetSCSIStatus(v uint8) { p.BHS[3] = v }
