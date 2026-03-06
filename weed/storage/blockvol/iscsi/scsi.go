package iscsi

import (
	"encoding/binary"
	"errors"

	"github.com/seaweedfs/seaweedfs/weed/storage/blockvol/blockerr"
)

// SCSI opcode constants (SPC-5 / SBC-4)
const (
	ScsiTestUnitReady      uint8 = 0x00
	ScsiRequestSense       uint8 = 0x03
	ScsiInquiry            uint8 = 0x12
	ScsiModeSelect6        uint8 = 0x15
	ScsiModeSense6         uint8 = 0x1a
	ScsiStartStopUnit      uint8 = 0x1b
	ScsiReadCapacity10     uint8 = 0x25
	ScsiRead10             uint8 = 0x28
	ScsiWrite10            uint8 = 0x2a
	ScsiSyncCache10        uint8 = 0x35
	ScsiUnmap              uint8 = 0x42
	ScsiModeSelect10       uint8 = 0x55
	ScsiModeSense10        uint8 = 0x5a
	ScsiPersistReserveIn   uint8 = 0x5e
	ScsiPersistReserveOut  uint8 = 0x5f
	ScsiRead16             uint8 = 0x88
	ScsiWrite16            uint8 = 0x8a
	ScsiSyncCache16        uint8 = 0x91
	ScsiWriteSame16        uint8 = 0x93
	ScsiServiceActionIn16  uint8 = 0x9e // READ CAPACITY(16), etc.
	ScsiReportLuns         uint8 = 0xa0
	ScsiMaintenanceIn      uint8 = 0xa3 // REPORT SUPPORTED OPCODES, REPORT TARGET PORT GROUPS, etc.
	ScsiMaintenanceOut     uint8 = 0xa4 // SET TARGET PORT GROUPS, etc.
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
	case ScsiRequestSense:
		return h.requestSense(cdb)
	case ScsiInquiry:
		return h.inquiry(cdb)
	case ScsiModeSelect6:
		return h.modeSelect6(cdb, dataOut)
	case ScsiModeSense6:
		return h.modeSense6(cdb)
	case ScsiStartStopUnit:
		return h.startStopUnit(cdb)
	case ScsiReadCapacity10:
		return h.readCapacity10()
	case ScsiRead10:
		return h.read10(cdb)
	case ScsiWrite10:
		if r := h.checkStandbyReject(); r != nil {
			return *r
		}
		return h.write10(cdb, dataOut)
	case ScsiSyncCache10:
		if r := h.checkStandbyReject(); r != nil {
			return *r
		}
		return h.syncCache()
	case ScsiUnmap:
		if r := h.checkStandbyReject(); r != nil {
			return *r
		}
		return h.unmap(cdb, dataOut)
	case ScsiModeSelect10:
		return h.modeSelect10(cdb, dataOut)
	case ScsiModeSense10:
		return h.modeSense10(cdb)
	case ScsiPersistReserveIn:
		return h.persistReserveIn(cdb)
	case ScsiPersistReserveOut:
		return h.persistReserveOut(cdb, dataOut)
	case ScsiRead16:
		return h.read16(cdb)
	case ScsiWrite16:
		if r := h.checkStandbyReject(); r != nil {
			return *r
		}
		return h.write16(cdb, dataOut)
	case ScsiSyncCache16:
		if r := h.checkStandbyReject(); r != nil {
			return *r
		}
		return h.syncCache()
	case ScsiWriteSame16:
		if r := h.checkStandbyReject(); r != nil {
			return *r
		}
		return h.writeSame16(cdb, dataOut)
	case ScsiServiceActionIn16:
		sa := cdb[1] & 0x1f
		if sa == ScsiSAReadCapacity16 {
			return h.readCapacity16(cdb)
		}
		return illegalRequest(ASCInvalidOpcode, ASCQLuk)
	case ScsiReportLuns:
		return h.reportLuns(cdb)
	case ScsiMaintenanceIn:
		return h.maintenanceIn(cdb)
	case ScsiMaintenanceOut:
		return illegalRequest(ASCInvalidOpcode, ASCQLuk)
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

func (h *SCSIHandler) requestSense(cdb [16]byte) SCSIResult {
	allocLen := cdb[4]
	if allocLen == 0 {
		allocLen = 18
	}

	// Return fixed-format sense data with NO SENSE (no pending error).
	data := BuildSenseData(SenseNoSense, 0x00, 0x00)
	if int(allocLen) < len(data) {
		data = data[:allocLen]
	}
	return SCSIResult{Status: SCSIStatusGood, Data: data}
}

// startStopUnit handles START STOP UNIT (0x1B).
// Windows sends this during disk init. We accept and ignore.
func (h *SCSIHandler) startStopUnit(cdb [16]byte) SCSIResult {
	return SCSIResult{Status: SCSIStatusGood}
}

// modeSelect6 handles MODE SELECT(6) (0x15).
// Windows sends this to set caching mode pages. We accept and ignore the data.
func (h *SCSIHandler) modeSelect6(cdb [16]byte, dataOut []byte) SCSIResult {
	return SCSIResult{Status: SCSIStatusGood}
}

// modeSelect10 handles MODE SELECT(10) (0x55).
// Same as modeSelect6 but with 10-byte CDB.
func (h *SCSIHandler) modeSelect10(cdb [16]byte, dataOut []byte) SCSIResult {
	return SCSIResult{Status: SCSIStatusGood}
}

// persistReserveIn handles PERSISTENT RESERVE IN (0x5E).
// Windows Cluster and MPIO use this. We don't support reservations,
// so return ILLEGAL_REQUEST with a specific ASC that tells Windows
// "not supported" rather than "broken device".
func (h *SCSIHandler) persistReserveIn(cdb [16]byte) SCSIResult {
	return illegalRequest(ASCInvalidOpcode, ASCQLuk)
}

// persistReserveOut handles PERSISTENT RESERVE OUT (0x5F).
func (h *SCSIHandler) persistReserveOut(cdb [16]byte, dataOut []byte) SCSIResult {
	return illegalRequest(ASCInvalidOpcode, ASCQLuk)
}

// maintenanceIn handles MAINTENANCE IN (0xA3).
// Service action 0x0A = REPORT TARGET PORT GROUPS (ALUA).
// Service action 0x0C = REPORT SUPPORTED OPERATION CODES.
// Windows sends 0x0C to discover which commands we support.
func (h *SCSIHandler) maintenanceIn(cdb [16]byte) SCSIResult {
	sa := cdb[1] & 0x1f
	switch sa {
	case 0x0a:
		return h.reportTargetPortGroups(cdb)
	case 0x0c:
		// REPORT SUPPORTED OPERATION CODES -- return empty (not supported).
		// This tells Windows to probe commands individually.
		return illegalRequest(ASCInvalidOpcode, ASCQLuk)
	default:
		return illegalRequest(ASCInvalidOpcode, ASCQLuk)
	}
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
	// If ALUA is supported, set TPGS = implicit (bits 5:4 = 01).
	if _, ok := h.dev.(ALUAProvider); ok {
		data[5] |= 0x10 // TPGS = 01 (implicit ALUA)
	}
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
			0x00, 0x05, // page length
			0x00, // supported pages: 0x00
			0x80, //                  0x80 (serial)
			0x83, //                  0x83 (device identification)
			0xb0, //                  0xB0 (block limits)
			0xb2, //                  0xB2 (logical block provisioning)
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
		return h.inquiryVPD83(allocLen)

	case 0xb0: // Block Limits (SBC-4, Section 6.6.4)
		return h.inquiryVPDB0(allocLen)

	case 0xb2: // Logical Block Provisioning (SBC-4, Section 6.6.6)
		return h.inquiryVPDB2(allocLen)

	default:
		return illegalRequest(ASCInvalidFieldInCDB, ASCQLuk)
	}
}

// inquiryVPDB0 returns the Block Limits VPD page (0xB0).
// Tells the kernel our maximum WRITE SAME, UNMAP, and transfer lengths.
func (h *SCSIHandler) inquiryVPDB0(allocLen uint16) SCSIResult {
	blockSize := h.dev.BlockSize()
	totalBlocks := h.dev.VolumeSize() / uint64(blockSize)

	// Cap WRITE SAME to the whole volume (no practical limit).
	maxWS := totalBlocks
	if maxWS > 0xFFFFFFFF {
		maxWS = 0xFFFFFFFF
	}

	data := make([]byte, 64)
	data[0] = 0x00 // device type
	data[1] = 0xb0 // page code
	binary.BigEndian.PutUint16(data[2:4], 0x003c) // page length = 60

	// Byte 5: WSNZ (Write Same No Zero) = 0 -- we accept zero-length WRITE SAME
	// Bytes 6-7: Maximum compare and write length = 0 (not supported)

	// Bytes 8-9: Optimal transfer length granularity = 1 block
	binary.BigEndian.PutUint16(data[6:8], 1)

	// Bytes 8-11: Maximum transfer length = 0 (no limit from our side)
	// Bytes 12-15: Optimal transfer length = 128 blocks (512KB)
	binary.BigEndian.PutUint32(data[12:16], 128)

	// Bytes 20-23: Maximum prefetch length = 0
	// Bytes 24-27: Maximum UNMAP LBA count
	binary.BigEndian.PutUint32(data[24:28], uint32(maxWS))
	// Bytes 28-31: Maximum UNMAP block descriptor count
	binary.BigEndian.PutUint32(data[28:32], 256)
	// Bytes 32-35: Optimal UNMAP granularity = 1 block
	binary.BigEndian.PutUint32(data[32:36], 1)
	// Bytes 36-39: UNMAP granularity alignment (bit 31 = UGAVALID)
	// Not set -- no alignment requirement.

	// Bytes 40-47: Maximum WRITE SAME length (uint64)
	binary.BigEndian.PutUint64(data[40:48], maxWS)

	if int(allocLen) < len(data) {
		data = data[:allocLen]
	}
	return SCSIResult{Status: SCSIStatusGood, Data: data}
}

// inquiryVPDB2 returns the Logical Block Provisioning VPD page (0xB2).
// Tells the kernel what thin provisioning features we support.
func (h *SCSIHandler) inquiryVPDB2(allocLen uint16) SCSIResult {
	data := make([]byte, 8)
	data[0] = 0x00 // device type
	data[1] = 0xb2 // page code
	binary.BigEndian.PutUint16(data[2:4], 0x0004) // page length = 4

	// Byte 5: Provisioning type and flags
	// Bits 7: Threshold exponent = 0
	data[4] = 0x00

	// Byte 5 (offset 5 in page):
	// Bit 7: DP (descriptor present) = 0
	// Bits 2-0: Provisioning type = 0x02 (thin provisioned)
	data[5] = 0x02

	// Byte 6: Provisioning group descriptor flags
	// Bit 7: LBPU (Logical Block Provisioning UNMAP) = 1 -- we support UNMAP
	// Bit 6: LBPWS (LBP Write Same) = 1 -- we support WRITE SAME with UNMAP bit
	// Bit 5: LBPWS10 = 0 -- we don't support WRITE SAME(10)
	data[6] = 0xC0 // LBPU=1, LBPWS=1

	if int(allocLen) < len(data) {
		data = data[:allocLen]
	}
	return SCSIResult{Status: SCSIStatusGood, Data: data}
}

// inquiryVPD83 returns the Device Identification VPD page (0x83).
// When ALUAProvider is available, returns three descriptors needed by dm-multipath:
//  1. NAA-6 identifier (logical unit identity, UUID-derived)
//  2. Target Port Group designator (TPG identity)
//  3. Relative Target Port designator (target port identity)
//
// Without ALUAProvider, returns only the NAA descriptor with a hardcoded value.
func (h *SCSIHandler) inquiryVPD83(allocLen uint16) SCSIResult {
	alua, hasALUA := h.dev.(ALUAProvider)

	// Descriptor 1: NAA-6 identifier (always present).
	var naaBytes [8]byte
	if hasALUA {
		naaBytes = alua.DeviceNAA()
	} else {
		naaBytes = [8]byte{0x60, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07}
	}
	naaDesc := []byte{
		0x01, // code set: binary
		0x03, // PIV=0, association=00 (logical unit), type=3 (NAA)
		0x00, // reserved
		0x08, // identifier length
	}
	naaDesc = append(naaDesc, naaBytes[:]...)

	descs := make([]byte, 0, 32)
	descs = append(descs, naaDesc...)

	if hasALUA {
		tpgID := alua.TPGroupID()

		// Descriptor 2: Target Port Group designator (type 0x05).
		tpgDesc := []byte{
			0x01, // code set: binary
			0x15, // PIV=1, association=01 (target port), type=5 (target port group)
			0x00, // reserved
			0x04, // identifier length
			0x00, 0x00, // reserved
			byte(tpgID >> 8), byte(tpgID), // TPG ID big-endian
		}
		descs = append(descs, tpgDesc...)

		// Descriptor 3: Relative Target Port designator (type 0x04).
		rtpDesc := []byte{
			0x01, // code set: binary
			0x14, // PIV=1, association=01 (target port), type=4 (relative target port)
			0x00, // reserved
			0x04, // identifier length
			0x00, 0x00, // reserved
			0x00, 0x01, // relative target port identifier = 1
		}
		descs = append(descs, rtpDesc...)
	}

	data := make([]byte, 4+len(descs))
	data[0] = 0x00 // device type
	data[1] = 0x83 // page code
	binary.BigEndian.PutUint16(data[2:4], uint16(len(descs)))
	copy(data[4:], descs)

	if int(allocLen) < len(data) {
		data = data[:allocLen]
	}
	return SCSIResult{Status: SCSIStatusGood, Data: data}
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
	allocLen := cdb[4]
	if allocLen == 0 {
		allocLen = 4
	}
	pageCode := cdb[2] & 0x3f

	// Build mode page data based on requested page.
	pages := h.buildModePages(pageCode)

	// MODE SENSE(6) header = 4 bytes + pages
	data := make([]byte, 4+len(pages))
	data[0] = byte(3 + len(pages)) // Mode data length (everything after byte 0)
	data[1] = 0x00                 // Medium type: default
	data[2] = 0x00                 // Device-specific parameter (no write protect)
	data[3] = 0x00                 // Block descriptor length = 0
	copy(data[4:], pages)

	if int(allocLen) < len(data) {
		data = data[:allocLen]
	}
	return SCSIResult{Status: SCSIStatusGood, Data: data}
}

func (h *SCSIHandler) modeSense10(cdb [16]byte) SCSIResult {
	allocLen := binary.BigEndian.Uint16(cdb[7:9])
	if allocLen == 0 {
		allocLen = 8
	}
	pageCode := cdb[2] & 0x3f

	// Build mode page data based on requested page.
	pages := h.buildModePages(pageCode)

	// MODE SENSE(10) header = 8 bytes + pages
	data := make([]byte, 8+len(pages))
	binary.BigEndian.PutUint16(data[0:2], uint16(6+len(pages))) // Mode data length
	data[2] = 0x00 // Medium type: default
	data[3] = 0x00 // Device-specific parameter (no write protect)
	data[4] = 0x00 // Reserved (LONGLBA=0)
	data[5] = 0x00 // Reserved
	binary.BigEndian.PutUint16(data[6:8], 0) // Block descriptor length = 0
	copy(data[8:], pages)

	if int(allocLen) < len(data) {
		data = data[:allocLen]
	}
	return SCSIResult{Status: SCSIStatusGood, Data: data}
}

// buildModePages returns the mode page data for the requested page code.
// Page 0x3F = return all pages.
func (h *SCSIHandler) buildModePages(pageCode uint8) []byte {
	switch pageCode {
	case 0x08: // Caching mode page
		return h.modePage08()
	case 0x0a: // Control mode page
		return h.modePage0A()
	case 0x3f: // All pages
		var pages []byte
		pages = append(pages, h.modePage08()...)
		pages = append(pages, h.modePage0A()...)
		return pages
	default:
		return nil
	}
}

// modePage08 returns the Caching mode page (SBC-4, Section 7.5.5).
// Tells Windows we support write caching.
func (h *SCSIHandler) modePage08() []byte {
	page := make([]byte, 20)
	page[0] = 0x08 // Page code
	page[1] = 18   // Page length (20 - 2)
	page[2] = 0x04 // WCE=1 (Write Cache Enable), RCD=0 (Read Cache Disable)
	return page
}

// modePage0A returns the Control mode page (SPC-5, Section 8.4.8).
func (h *SCSIHandler) modePage0A() []byte {
	page := make([]byte, 12)
	page[0] = 0x0a // Page code
	page[1] = 10   // Page length (12 - 2)
	return page
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
		return mapBlockVolError(err)
	}

	return SCSIResult{Status: SCSIStatusGood}
}

func (h *SCSIHandler) syncCache() SCSIResult {
	if err := h.dev.SyncCache(); err != nil {
		return mapBlockVolError(err)
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
				return mapBlockVolError(err)
			}
		}
		descData = descData[16:]
	}

	return SCSIResult{Status: SCSIStatusGood}
}

// writeSame16 handles WRITE SAME(16) -- SBC-4, Section 5.42.
// If UNMAP bit is set, the range is trimmed (zeroed). Otherwise the single
// logical block from dataOut is written repeatedly across the range.
// NDOB (No Data-Out Buffer) means zero the range with no data payload.
func (h *SCSIHandler) writeSame16(cdb [16]byte, dataOut []byte) SCSIResult {
	lba := binary.BigEndian.Uint64(cdb[2:10])
	numBlocks := binary.BigEndian.Uint32(cdb[10:14])
	unmap := cdb[1]&0x08 != 0
	ndob := cdb[1]&0x01 != 0

	if numBlocks == 0 {
		return SCSIResult{Status: SCSIStatusGood}
	}

	blockSize := h.dev.BlockSize()
	totalBlocks := h.dev.VolumeSize() / uint64(blockSize)

	if lba+uint64(numBlocks) > totalBlocks {
		return illegalRequest(ASCLBAOutOfRange, ASCQLuk)
	}

	// UNMAP or NDOB: zero/trim the range.
	if unmap || ndob {
		if err := h.dev.Trim(lba, numBlocks*blockSize); err != nil {
			return mapBlockVolError(err)
		}
		return SCSIResult{Status: SCSIStatusGood}
	}

	// Normal WRITE SAME: replicate the single block across the range.
	if uint32(len(dataOut)) < blockSize {
		return illegalRequest(ASCInvalidFieldInCDB, ASCQLuk)
	}
	pattern := dataOut[:blockSize]

	// Check if the pattern is all zeros -- use Trim for efficiency.
	allZero := true
	for _, b := range pattern {
		if b != 0 {
			allZero = false
			break
		}
	}
	if allZero {
		if err := h.dev.Trim(lba, numBlocks*blockSize); err != nil {
			return mapBlockVolError(err)
		}
		return SCSIResult{Status: SCSIStatusGood}
	}

	// Non-zero pattern: write each block individually.
	for i := uint32(0); i < numBlocks; i++ {
		if err := h.dev.WriteAt(lba+uint64(i), pattern); err != nil {
			return mapBlockVolError(err)
		}
	}
	return SCSIResult{Status: SCSIStatusGood}
}

// isDurabilityError checks if an error originates from the durability layer.
// Uses errors.Is with shared sentinels from blockerr (avoids import cycle
// since blockerr has no dependencies on blockvol or iscsi).
func isDurabilityError(err error) bool {
	return errors.Is(err, blockerr.ErrDurabilityBarrierFailed) ||
		errors.Is(err, blockerr.ErrDurabilityQuorumLost)
}

// mapBlockVolError maps a blockvol write/sync error to a SCSI sense result.
// Durability errors (barrier failed, quorum lost) are mapped to
// HARDWARE_ERROR + ASC 0x08 (Logical Unit Communication Failure) to
// distinguish them from local I/O errors (MEDIUM_ERROR + ASC 0x0C).
func mapBlockVolError(err error) SCSIResult {
	if isDurabilityError(err) {
		return SCSIResult{
			Status:    SCSIStatusCheckCond,
			SenseKey:  SenseHardwareError,
			SenseASC:  0x08, // Logical Unit Communication Failure
			SenseASCQ: 0x00,
		}
	}
	return SCSIResult{
		Status:    SCSIStatusCheckCond,
		SenseKey:  SenseMediumError,
		SenseASC:  0x0C, // Write error
		SenseASCQ: 0x00,
	}
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
