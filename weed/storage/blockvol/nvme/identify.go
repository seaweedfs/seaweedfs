package nvme

import (
	"encoding/binary"
	"math/bits"
)

const identifySize = 4096

// handleIdentify dispatches Identify commands by CNS type.
func (c *Controller) handleIdentify(req *Request) error {
	cns := uint8(req.capsule.D10 & 0xFF)

	switch cns {
	case cnsIdentifyController:
		return c.identifyController(req)
	case cnsIdentifyNamespace:
		return c.identifyNamespace(req)
	case cnsActiveNSList:
		return c.identifyActiveNSList(req)
	case cnsNSDescriptorList:
		return c.identifyNSDescriptors(req)
	default:
		req.resp.Status = uint16(StatusInvalidField)
		return c.sendResponse(req)
	}
}

// identifyController returns the 4096-byte Identify Controller data structure.
func (c *Controller) identifyController(req *Request) error {
	buf := make([]byte, identifySize)

	sub := c.subsystem
	if sub == nil {
		req.resp.Status = uint16(StatusInvalidField)
		return c.sendResponse(req)
	}

	// VID (PCI Vendor ID) - use 0 for software target
	// SSVID - 0

	// Serial Number (offset 4, 20 bytes, space-padded ASCII)
	copyPadded(buf[4:24], "SWF00001")

	// Model Number (offset 24, 40 bytes, space-padded ASCII)
	copyPadded(buf[24:64], "SeaweedFS BlockVol")

	// Firmware Revision (offset 64, 8 bytes, space-padded ASCII)
	copyPadded(buf[64:72], "0001")

	// RAB (Recommended Arbitration Burst) - offset 72
	buf[72] = 6

	// IEEE OUI - offset 73-75 (3 bytes, 0 for software)

	// CMIC (Controller Multi-Path I/O Capabilities) - offset 76
	// bit 3: ANA reporting supported
	buf[76] = 0x08

	// MDTS (Maximum Data Transfer Size) - offset 77
	// 2^MDTS * 4096 = max transfer. MDTS=3 → 32KB
	buf[77] = 3

	// CNTLID (Controller ID) - offset 78-79
	binary.LittleEndian.PutUint16(buf[78:], c.cntlID)

	// Version - offset 80-83
	binary.LittleEndian.PutUint32(buf[80:], nvmeVersion14)

	// OACS (Optional Admin Command Support) - offset 256-257
	// 0 = no optional admin commands
	binary.LittleEndian.PutUint16(buf[256:], 0)

	// ACRTD (Abort Command Limit) - offset 258
	buf[258] = 3

	// AERTL (Async Event Request Limit) - offset 259
	buf[259] = 3

	// FRMW (Firmware Updates) - offset 260
	buf[260] = 0x02 // slot 1 read-only

	// LPA (Log Page Attributes) - offset 261
	buf[261] = 0

	// ELPE (Error Log Page Entries) - offset 262
	buf[262] = 0 // 1 entry (0-based)

	// KAS (Keep Alive Support) - offset 320-321
	// Granularity in 100ms units. Non-zero is mandatory for fabrics controllers.
	binary.LittleEndian.PutUint16(buf[320:], 10) // 1 second granularity

	// ANACAP (ANA Capabilities) - offset 341
	// bit 3: reports Optimized state
	buf[341] = 0x08

	// ANAGRPMAX (Max ANA Group ID) - offset 344-347
	binary.LittleEndian.PutUint32(buf[344:], 1)

	// NANAGRPID (Number of ANA Group IDs) - offset 348-351
	binary.LittleEndian.PutUint32(buf[348:], 1)

	// SQES (Submission Queue Entry Size) - offset 512
	// min=6 (2^6=64 bytes), max=6
	buf[512] = 0x66

	// CQES (Completion Queue Entry Size) - offset 513
	// min=4 (2^4=16 bytes), max=4
	buf[513] = 0x44

	// MAXCMD - offset 514-515
	binary.LittleEndian.PutUint16(buf[514:], 64)

	// NN (Number of Namespaces) - offset 516-519
	binary.LittleEndian.PutUint32(buf[516:], 1)

	// ONCS (Optional NVM Command Support) - offset 520-521
	// bit 3: WriteZeros, bit 2: DatasetMgmt (Trim)
	binary.LittleEndian.PutUint16(buf[520:], 0x0C)

	// VWC (Volatile Write Cache) - offset 525
	// bit 0: volatile write cache present → Flush required
	buf[525] = 0x01

	// SGLS (SGL Support) - offset 536-539
	// bit 0: SGLs supported (required for NVMe/TCP)
	binary.LittleEndian.PutUint32(buf[536:], 0x01)

	// MNAN (Maximum Number of Allowed Namespaces) - offset 540-543
	// Must be non-zero for NVMe 1.4+ controllers; kernel validates this.
	binary.LittleEndian.PutUint32(buf[540:], 1)

	// SubNQN (Subsystem NQN) - offset 768, 256 bytes, NUL-terminated
	// Must NOT be space-padded — kernel uses strcmp() to match against Connect NQN.
	copy(buf[768:1024], sub.NQN) // buf is already zeroed → NUL-terminated

	// IOCCSZ (I/O Queue Command Capsule Supported Size) - offset 1792-1795
	// In 16-byte units: 64/16 = 4
	binary.LittleEndian.PutUint32(buf[1792:], 4)

	// IORCSZ (I/O Queue Response Capsule Supported Size) - offset 1796-1799
	// In 16-byte units: 16/16 = 1
	binary.LittleEndian.PutUint32(buf[1796:], 1)

	// ICDOFF (In Capsule Data Offset) - offset 1800-1801
	// 0 means inline data immediately follows SQE in capsule
	binary.LittleEndian.PutUint16(buf[1800:], 0)

	// FCATT (Fabrics Controller Attributes) - offset 1802
	// bit 0: 0 = I/O controller (not discovery)
	buf[1802] = 0

	// MSDBD (Maximum SGL Data Block Descriptors) - offset 1803
	buf[1803] = 1

	// OFCS (Optional Fabric Commands Supported) - offset 1804-1805
	// bit 0: Disconnect command supported
	binary.LittleEndian.PutUint16(buf[1804:], 0x01)

	req.c2hData = buf
	return c.sendC2HDataAndResponse(req)
}

// identifyNamespace returns the 4096-byte Identify Namespace data for NSID=1.
func (c *Controller) identifyNamespace(req *Request) error {
	sub := c.subsystem
	if sub == nil {
		req.resp.Status = uint16(StatusInvalidField)
		return c.sendResponse(req)
	}

	dev := sub.Dev
	blockSize := dev.BlockSize()
	nsze := dev.VolumeSize() / uint64(blockSize)

	buf := make([]byte, identifySize)

	// NSZE (Namespace Size in blocks) - offset 0-7
	binary.LittleEndian.PutUint64(buf[0:], nsze)

	// NCAP (Namespace Capacity) - offset 8-15
	binary.LittleEndian.PutUint64(buf[8:], nsze)

	// NUSE (Namespace Utilization) - offset 16-23
	binary.LittleEndian.PutUint64(buf[16:], nsze)

	// NSFEAT (Namespace Features) - offset 24
	// bit 0: thin provisioning (supports Trim)
	buf[24] = 0x01

	// NLBAF (Number of LBA Formats minus 1) - offset 25
	buf[25] = 0 // one format

	// FLBAS (Formatted LBA Size) - offset 26
	// bits 3:0 = LBA format index (0)
	buf[26] = 0

	// MC (Metadata Capabilities) - offset 27
	buf[27] = 0

	// DLFEAT (Deallocate Logical Block Features) - offset 28
	// bit 2: Deallocated blocks return zeros on read
	buf[28] = 0x04

	// NGUID (Namespace Globally Unique Identifier) - offset 104-119 (16 bytes)
	copy(buf[104:120], sub.NGUID[:])

	// LBAF[0] (LBA Format 0) - offset 128-131
	// bits 23:16 = LBADS (log2 of block size)
	lbads := uint8(bits.TrailingZeros32(blockSize))
	binary.LittleEndian.PutUint32(buf[128:], uint32(lbads)<<16)

	// ANAGRPID (ANA Group Identifier) - offset 92-95
	binary.LittleEndian.PutUint32(buf[92:], 1)

	req.c2hData = buf
	return c.sendC2HDataAndResponse(req)
}

// identifyActiveNSList returns the list of active namespace IDs (just NSID=1).
func (c *Controller) identifyActiveNSList(req *Request) error {
	buf := make([]byte, identifySize)
	// Single namespace: NSID=1
	binary.LittleEndian.PutUint32(buf[0:], 1)

	req.c2hData = buf
	return c.sendC2HDataAndResponse(req)
}

// identifyNSDescriptors returns namespace descriptor list for NSID=1.
func (c *Controller) identifyNSDescriptors(req *Request) error {
	sub := c.subsystem
	if sub == nil {
		req.resp.Status = uint16(StatusInvalidField)
		return c.sendResponse(req)
	}

	buf := make([]byte, identifySize)
	off := 0

	// NGUID descriptor (type=0x02, length=16)
	buf[off] = 0x02 // NIDT: NGUID
	off++
	buf[off] = 16 // NIDL: 16 bytes
	off++
	off += 2 // reserved
	copy(buf[off:off+16], sub.NGUID[:])

	req.c2hData = buf
	return c.sendC2HDataAndResponse(req)
}

// copyPadded copies src into dst, padding remaining bytes with spaces.
func copyPadded(dst []byte, src string) {
	n := copy(dst, src)
	for i := n; i < len(dst); i++ {
		dst[i] = ' '
	}
}
