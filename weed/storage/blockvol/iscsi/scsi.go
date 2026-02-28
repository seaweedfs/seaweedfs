package iscsi

import (
	"encoding/binary"
)

// SCSI opcode constants (SPC-5 / SBC-4)
const (
	ScsiTestUnitReady  uint8 = 0x00
	ScsiInquiry        uint8 = 0x12
	ScsiModeSense6     uint8 = 0x1a
	ScsiReadCapacity10 uint8 = 0x25
	ScsiRead10         uint8 = 0x28
	ScsiWrite10        uint8 = 0x2a
	ScsiSyncCache10    uint8 = 0x35
	ScsiUnmap          uint8 = 0x42
	ScsiReportLuns     uint8 = 0xa0
	ScsiRead16         uint8 = 0x88
	ScsiWrite16        uint8 = 0x8a
	ScsiReadCapacity16 uint8 = 0x9e // SERVICE ACTION IN (16), SA=0x10
	ScsiSyncCache16    uint8 = 0x91
)

// Service action for READ CAPACITY (16)
const ScsiSAReadCapacity16 uint8 = 0x10

// SCSI sense keys
const (
	SenseNoSense        uint8 = 0x00
	SenseNotReady       uint8 = 0x02
	SenseMediumError    uint8 = 0x03
	SenseHardwareError  uint8 = 0x04
	SenseIllegalRequest uint8 = 0x05
	SenseAbortedCommand uint8 = 0x0b
)

// ASC/ASCQ pairs
const (
	ASCInvalidOpcode       uint8 = 0x20
	ASCQLuk                uint8 = 0x00
	ASCInvalidFieldInCDB   uint8 = 0x24
	ASCLBAOutOfRange       uint8 = 0x21
	ASCNotReady            uint8 = 0x04
	ASCQNotReady           uint8 = 0x03 // manual intervention required
)

// BlockDevice is the interface that the SCSI command handler uses to
// interact with the underlying storage. This maps onto BlockVol.
type BlockDevice interface {
	ReadAt(lba uint64, length uint32) ([]byte, error)
	WriteAt(lba uint64, data []byte) error
	Trim(lba uint64, length uint32) error
	SyncCache() error
	BlockSize() uint32
	VolumeSize() uint64 // total size in bytes
	IsHealthy() bool
}

// SCSIHandler processes SCSI commands from iSCSI PDUs.
type SCSIHandler struct {
	dev      BlockDevice
	vendorID string // 8 bytes for INQUIRY
	prodID   string // 16 bytes for INQUIRY
	serial   string // for VPD page 0x80
}

// NewSCSIHandler creates a SCSI command handler for the given block device.
func NewSCSIHandler(dev BlockDevice) *SCSIHandler {
	return &SCSIHandler{
		dev:      dev,
		vendorID: "SeaweedF",
		prodID:  "BlockVol        ",
		serial:   "SWF00001",
	}
}

// SCSIResult holds the result of a SCSI command execution.
type SCSIResult struct {
	Status    uint8  // SCSI status
	Data      []byte // Response data (for Data-In)
	SenseKey  uint8  // Sense key (if CHECK_CONDITION)
	SenseASC  uint8  // Additional sense code
	SenseASCQ uint8  // Additional sense code qualifier
}

// HandleCommand dispatches a SCSI CDB to the appropriate handler.
// dataOut contains any data sent by the initiator (for WRITE commands).
func (h *SCSIHandler) HandleCommand(cdb [16]byte, dataOut []byte) SCSIResult {
	opcode := cdb[0]

	switch opcode {
	case ScsiTestUnitReady:
		return h.testUnitReady()
	case ScsiInquiry:
		return h.inquiry(cdb)
	case ScsiModeSense6:
		return h.modeSense6(cdb)
	case ScsiReadCapacity10:
		return h.readCapacity10()
	case ScsiReadCapacity16:
		sa := cdb[1] & 0x1f
		if sa == ScsiSAReadCapacity16 {
			return h.readCapacity16(cdb)
		}
		return illegalRequest(ASCInvalidOpcode, ASCQLuk)
	case ScsiReportLuns:
		return h.reportLuns(cdb)
	case ScsiRead10:
		return h.read10(cdb)
	case ScsiRead16:
		return h.read16(cdb)
	case ScsiWrite10:
		return h.write10(cdb, dataOut)
	case ScsiWrite16:
		return h.write16(cdb, dataOut)
	case ScsiSyncCache10:
		return h.syncCache()
	case ScsiSyncCache16:
		return h.syncCache()
	case ScsiUnmap:
		return h.unmap(cdb, dataOut)
	default:
		return illegalRequest(ASCInvalidOpcode, ASCQLuk)
	}
}

// --- Metadata commands ---

func (h *SCSIHandler) testUnitReady() SCSIResult {
	if !h.dev.IsHealthy() {
		return SCSIResult{
			Status:    SCSIStatusCheckCond,
			SenseKey:  SenseNotReady,
			SenseASC:  ASCNotReady,
			SenseASCQ: ASCQNotReady,
		}
	}
	return SCSIResult{Status: SCSIStatusGood}
}

func (h *SCSIHandler) inquiry(cdb [16]byte) SCSIResult {
	evpd := cdb[1] & 0x01
	pageCode := cdb[2]
	allocLen := binary.BigEndian.Uint16(cdb[3:5])
	if allocLen == 0 {
		allocLen = 36
	}

	if evpd != 0 {
		return h.inquiryVPD(pageCode, allocLen)
	}

	// Standard INQUIRY response (SPC-5, Section 6.6.1)
	data := make([]byte, 96)
	data[0] = 0x00 // Peripheral device type: SBC (direct access block device)
	data[1] = 0x00 // RMB=0 (not removable)
	data[2] = 0x06 // SPC-4 version
	data[3] = 0x02 // Response data format = 2 (SPC-2+)
	data[4] = 91   // Additional length (96-5)
	data[5] = 0x00 // SCCS, ACC, TPGS, 3PC
	data[6] = 0x00 // Obsolete, EncServ, VS, MultiP
	data[7] = 0x02 // CmdQue=1 (supports command queuing)

	// Vendor ID (bytes 8-15, 8 chars, space padded)
	copy(data[8:16], padRight(h.vendorID, 8))
	// Product ID (bytes 16-31, 16 chars, space padded)
	copy(data[16:32], padRight(h.prodID, 16))
	// Product revision (bytes 32-35, 4 chars)
	copy(data[32:36], "0001")

	if int(allocLen) < len(data) {
		data = data[:allocLen]
	}
	return SCSIResult{Status: SCSIStatusGood, Data: data}
}

func (h *SCSIHandler) inquiryVPD(pageCode uint8, allocLen uint16) SCSIResult {
	switch pageCode {
	case 0x00: // Supported VPD pages
		data := []byte{
			0x00,       // device type
			0x00,       // page code
			0x00, 0x03, // page length
			0x00, // supported pages: 0x00
			0x80, //                  0x80 (serial)
			0x83, //                  0x83 (device identification)
		}
		if int(allocLen) < len(data) {
			data = data[:allocLen]
		}
		return SCSIResult{Status: SCSIStatusGood, Data: data}

	case 0x80: // Unit serial number
		serial := padRight(h.serial, 8)
		data := make([]byte, 4+len(serial))
		data[0] = 0x00 // device type
		data[1] = 0x80 // page code
		binary.BigEndian.PutUint16(data[2:4], uint16(len(serial)))
		copy(data[4:], serial)
		if int(allocLen) < len(data) {
			data = data[:allocLen]
		}
		return SCSIResult{Status: SCSIStatusGood, Data: data}

	case 0x83: // Device identification
		// NAA identifier (8 bytes)
		naaID := []byte{
			0x01,                               // code set: binary
			0x03,                               // identifier type: NAA
			0x00,                               // reserved
			0x08,                               // identifier length
			0x60, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, // NAA-6 fake
		}
		data := make([]byte, 4+len(naaID))
		data[0] = 0x00 // device type
		data[1] = 0x83 // page code
		binary.BigEndian.PutUint16(data[2:4], uint16(len(naaID)))
		copy(data[4:], naaID)
		if int(allocLen) < len(data) {
			data = data[:allocLen]
		}
		return SCSIResult{Status: SCSIStatusGood, Data: data}

	default:
		return illegalRequest(ASCInvalidFieldInCDB, ASCQLuk)
	}
}

func (h *SCSIHandler) readCapacity10() SCSIResult {
	blockSize := h.dev.BlockSize()
	totalBlocks := h.dev.VolumeSize() / uint64(blockSize)

	data := make([]byte, 8)
	// If >2TB (blocks > 0xFFFFFFFF), return 0xFFFFFFFF to signal use READ_CAPACITY_16
	if totalBlocks > 0xFFFFFFFF {
		binary.BigEndian.PutUint32(data[0:4], 0xFFFFFFFF)
	} else {
		binary.BigEndian.PutUint32(data[0:4], uint32(totalBlocks-1)) // last LBA
	}
	binary.BigEndian.PutUint32(data[4:8], blockSize)

	return SCSIResult{Status: SCSIStatusGood, Data: data}
}

func (h *SCSIHandler) readCapacity16(cdb [16]byte) SCSIResult {
	allocLen := binary.BigEndian.Uint32(cdb[10:14])
	if allocLen < 32 {
		allocLen = 32
	}

	blockSize := h.dev.BlockSize()
	totalBlocks := h.dev.VolumeSize() / uint64(blockSize)

	data := make([]byte, 32)
	binary.BigEndian.PutUint64(data[0:8], totalBlocks-1)  // last LBA
	binary.BigEndian.PutUint32(data[8:12], blockSize)      // block length
	// data[12]: LBPME (logical block provisioning management enabled) = 1 for UNMAP support
	data[14] = 0x80 // LBPME bit

	if allocLen < uint32(len(data)) {
		data = data[:allocLen]
	}
	return SCSIResult{Status: SCSIStatusGood, Data: data}
}

func (h *SCSIHandler) modeSense6(cdb [16]byte) SCSIResult {
	// Minimal MODE SENSE(6) response — no mode pages
	allocLen := cdb[4]
	if allocLen == 0 {
		allocLen = 4
	}

	data := make([]byte, 4)
	data[0] = 3    // Mode data length (3 bytes follow)
	data[1] = 0x00 // Medium type: default
	data[2] = 0x00 // Device-specific parameter (no write protect)
	data[3] = 0x00 // Block descriptor length = 0

	if int(allocLen) < len(data) {
		data = data[:allocLen]
	}
	return SCSIResult{Status: SCSIStatusGood, Data: data}
}

func (h *SCSIHandler) reportLuns(cdb [16]byte) SCSIResult {
	allocLen := binary.BigEndian.Uint32(cdb[6:10])
	if allocLen < 16 {
		allocLen = 16
	}

	// Report a single LUN (LUN 0)
	data := make([]byte, 16)
	binary.BigEndian.PutUint32(data[0:4], 8) // LUN list length (8 bytes, 1 LUN)
	// data[4:7] reserved
	// data[8:16] = LUN 0 (all zeros)

	if allocLen < uint32(len(data)) {
		data = data[:allocLen]
	}
	return SCSIResult{Status: SCSIStatusGood, Data: data}
}

// --- Data commands (Task 2.6) ---

func (h *SCSIHandler) read10(cdb [16]byte) SCSIResult {
	lba := uint64(binary.BigEndian.Uint32(cdb[2:6]))
	transferLen := uint32(binary.BigEndian.Uint16(cdb[7:9]))
	return h.doRead(lba, transferLen)
}

func (h *SCSIHandler) read16(cdb [16]byte) SCSIResult {
	lba := binary.BigEndian.Uint64(cdb[2:10])
	transferLen := binary.BigEndian.Uint32(cdb[10:14])
	return h.doRead(lba, transferLen)
}

func (h *SCSIHandler) write10(cdb [16]byte, dataOut []byte) SCSIResult {
	lba := uint64(binary.BigEndian.Uint32(cdb[2:6]))
	transferLen := uint32(binary.BigEndian.Uint16(cdb[7:9]))
	return h.doWrite(lba, transferLen, dataOut)
}

func (h *SCSIHandler) write16(cdb [16]byte, dataOut []byte) SCSIResult {
	lba := binary.BigEndian.Uint64(cdb[2:10])
	transferLen := binary.BigEndian.Uint32(cdb[10:14])
	return h.doWrite(lba, transferLen, dataOut)
}

func (h *SCSIHandler) doRead(lba uint64, transferLen uint32) SCSIResult {
	if transferLen == 0 {
		return SCSIResult{Status: SCSIStatusGood}
	}

	blockSize := h.dev.BlockSize()
	totalBlocks := h.dev.VolumeSize() / uint64(blockSize)

	if lba+uint64(transferLen) > totalBlocks {
		return illegalRequest(ASCLBAOutOfRange, ASCQLuk)
	}

	byteLen := transferLen * blockSize
	data, err := h.dev.ReadAt(lba, byteLen)
	if err != nil {
		return SCSIResult{
			Status:    SCSIStatusCheckCond,
			SenseKey:  SenseMediumError,
			SenseASC:  0x11, // Unrecovered read error
			SenseASCQ: 0x00,
		}
	}

	return SCSIResult{Status: SCSIStatusGood, Data: data}
}

func (h *SCSIHandler) doWrite(lba uint64, transferLen uint32, dataOut []byte) SCSIResult {
	if transferLen == 0 {
		return SCSIResult{Status: SCSIStatusGood}
	}

	blockSize := h.dev.BlockSize()
	totalBlocks := h.dev.VolumeSize() / uint64(blockSize)

	if lba+uint64(transferLen) > totalBlocks {
		return illegalRequest(ASCLBAOutOfRange, ASCQLuk)
	}

	expectedBytes := transferLen * blockSize
	if uint32(len(dataOut)) < expectedBytes {
		return illegalRequest(ASCInvalidFieldInCDB, ASCQLuk)
	}

	if err := h.dev.WriteAt(lba, dataOut[:expectedBytes]); err != nil {
		return SCSIResult{
			Status:    SCSIStatusCheckCond,
			SenseKey:  SenseMediumError,
			SenseASC:  0x0C, // Write error
			SenseASCQ: 0x00,
		}
	}

	return SCSIResult{Status: SCSIStatusGood}
}

func (h *SCSIHandler) syncCache() SCSIResult {
	if err := h.dev.SyncCache(); err != nil {
		return SCSIResult{
			Status:    SCSIStatusCheckCond,
			SenseKey:  SenseHardwareError,
			SenseASC:  0x00,
			SenseASCQ: 0x00,
		}
	}
	return SCSIResult{Status: SCSIStatusGood}
}

func (h *SCSIHandler) unmap(cdb [16]byte, dataOut []byte) SCSIResult {
	if len(dataOut) < 8 {
		return illegalRequest(ASCInvalidFieldInCDB, ASCQLuk)
	}

	// UNMAP parameter list header (8 bytes)
	// descLen := binary.BigEndian.Uint16(dataOut[0:2]) // data length (unused)
	blockDescLen := binary.BigEndian.Uint16(dataOut[2:4])

	if int(blockDescLen)+8 > len(dataOut) {
		return illegalRequest(ASCInvalidFieldInCDB, ASCQLuk)
	}
	if blockDescLen%16 != 0 {
		return illegalRequest(ASCInvalidFieldInCDB, ASCQLuk)
	}

	// Each UNMAP block descriptor is 16 bytes
	descData := dataOut[8 : 8+blockDescLen]
	for len(descData) >= 16 {
		lba := binary.BigEndian.Uint64(descData[0:8])
		numBlocks := binary.BigEndian.Uint32(descData[8:12])
		// descData[12:16] reserved

		if numBlocks > 0 {
			blockSize := h.dev.BlockSize()
			if err := h.dev.Trim(lba, numBlocks*blockSize); err != nil {
				return SCSIResult{
					Status:    SCSIStatusCheckCond,
					SenseKey:  SenseMediumError,
					SenseASC:  0x0C,
					SenseASCQ: 0x00,
				}
			}
		}
		descData = descData[16:]
	}

	return SCSIResult{Status: SCSIStatusGood}
}

// BuildSenseData constructs a fixed-format sense data buffer (18 bytes).
func BuildSenseData(key, asc, ascq uint8) []byte {
	data := make([]byte, 18)
	data[0] = 0x70       // Response code: current errors, fixed format
	data[2] = key & 0x0f // Sense key
	data[7] = 10         // Additional sense length
	data[12] = asc       // ASC
	data[13] = ascq      // ASCQ
	return data
}

func illegalRequest(asc, ascq uint8) SCSIResult {
	return SCSIResult{
		Status:    SCSIStatusCheckCond,
		SenseKey:  SenseIllegalRequest,
		SenseASC:  asc,
		SenseASCQ: ascq,
	}
}

func padRight(s string, n int) string {
	if len(s) >= n {
		return s[:n]
	}
	b := make([]byte, n)
	copy(b, s)
	for i := len(s); i < n; i++ {
		b[i] = ' '
	}
	return string(b)
}
