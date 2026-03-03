package iscsi

import "encoding/binary"

// ALUA asymmetric access state constants (SPC-5).
const (
	ALUAActiveOptimized uint8 = 0x00
	ALUAActiveNonOpt    uint8 = 0x01
	ALUAStandby         uint8 = 0x02
	ALUAUnavailable     uint8 = 0x0E
	ALUATransitioning   uint8 = 0x0F
)

// ALUAProvider is optionally implemented by BlockDevice to expose ALUA state.
// SCSIHandler probes for this via type assertion.
type ALUAProvider interface {
	ALUAState() uint8    // Current asymmetric access state
	TPGroupID() uint16   // Target Port Group ID (1-65535)
	DeviceNAA() [8]byte  // NAA-6 device identifier (from volume UUID)
}

// checkStandbyReject checks if the device is in a non-writable ALUA state
// (Standby, Unavailable, or Transitioning). Returns a SCSIResult pointer
// with NOT READY sense if writes should be rejected, nil otherwise.
func (h *SCSIHandler) checkStandbyReject() *SCSIResult {
	alua, ok := h.dev.(ALUAProvider)
	if !ok {
		return nil
	}
	state := alua.ALUAState()
	if state == ALUAStandby || state == ALUAUnavailable || state == ALUATransitioning {
		r := SCSIResult{
			Status:    SCSIStatusCheckCond,
			SenseKey:  SenseNotReady,
			SenseASC:  0x04, // LOGICAL UNIT NOT READY
			SenseASCQ: 0x0B, // TARGET PORT IN STANDBY STATE
		}
		return &r
	}
	return nil
}

// reportTargetPortGroups handles REPORT TARGET PORT GROUPS (service action 0x0A
// under MAINTENANCE IN 0xA3). Returns a single TPG descriptor for the local
// target port group. dm-multipath queries each path separately and merges.
func (h *SCSIHandler) reportTargetPortGroups(cdb [16]byte) SCSIResult {
	alua, ok := h.dev.(ALUAProvider)
	if !ok {
		return illegalRequest(ASCInvalidOpcode, ASCQLuk)
	}

	allocLen := binary.BigEndian.Uint32(cdb[6:10])

	state := alua.ALUAState()
	tpgID := alua.TPGroupID()

	// Response: 4-byte header + 8-byte TPG descriptor + 4-byte target port descriptor = 16 bytes.
	data := make([]byte, 16)

	// Header: return data length (excludes the 4-byte header itself).
	binary.BigEndian.PutUint32(data[0:4], 12)

	// TPG descriptor (8 bytes at offset 4).
	data[4] = state & 0x0F // Byte 0: asymmetric access state (lower 4 bits)
	// Byte 1: supported states flags.
	// T_SUP=1, O_SUP=1, S_SUP=1, U_SUP=1 (we use Transitioning for Draining/Rebuilding).
	data[5] = 0x0F // bits: 0000_1111 = T_SUP | O_SUP | S_SUP | U_SUP
	data[6] = 0x00 // reserved
	data[7] = 0x00 // status code: 0 = no status available
	binary.BigEndian.PutUint16(data[8:10], tpgID)
	data[10] = 0x00 // reserved
	data[11] = 0x01 // target port count = 1

	// Target port descriptor (4 bytes at offset 12).
	data[12] = 0x00 // reserved
	data[13] = 0x00 // reserved
	binary.BigEndian.PutUint16(data[14:16], 1) // relative target port identifier = 1

	if allocLen > 0 && allocLen < uint32(len(data)) {
		data = data[:allocLen]
	}
	return SCSIResult{Status: SCSIStatusGood, Data: data}
}
