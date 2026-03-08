// Package nvme implements an NVMe/TCP target for SeaweedFS BlockVol.
//
// This package provides a functionally correct NVMe-oF over TCP transport
// that shares the same BlockVol engine, fencing, replication, and failover
// as the iSCSI target.
package nvme

import (
	"encoding/binary"
	"fmt"
)

// ---------- PDU type codes ----------

const (
	pduICReq       uint8 = 0x0 // Initialization Connection Request
	pduICResp      uint8 = 0x1 // Initialization Connection Response
	pduH2CTermReq  uint8 = 0x2 // Host-to-Controller Termination Request
	pduC2HTermReq  uint8 = 0x3 // Controller-to-Host Termination Request
	pduCapsuleCmd  uint8 = 0x4 // NVMe Capsule Command
	pduCapsuleResp uint8 = 0x5 // NVMe Capsule Response
	pduC2HData     uint8 = 0x7 // Controller-to-Host Data Transfer
	pduR2T         uint8 = 0x9 // Ready-to-Transfer
)

// ---------- Admin command opcodes ----------

const (
	adminFlush        uint8 = 0x00 // NVM Flush (admin context unused here)
	adminGetLogPage   uint8 = 0x02
	adminIdentify     uint8 = 0x06
	adminAbort        uint8 = 0x08
	adminSetFeatures  uint8 = 0x09
	adminGetFeatures  uint8 = 0x0A
	adminAsyncEvent   uint8 = 0x0C
	adminKeepAlive    uint8 = 0x18
	adminFabric       uint8 = 0x7F // Fabric-specific commands
)

// ---------- IO command opcodes ----------

const (
	ioFlush      uint8 = 0x00
	ioWrite      uint8 = 0x01
	ioRead       uint8 = 0x02
	ioWriteZeros uint8 = 0x08
)

// ---------- Fabric command types (FCType) ----------

const (
	fcPropertySet uint8 = 0x00
	fcConnect     uint8 = 0x01
	fcPropertyGet uint8 = 0x04
	fcDisconnect  uint8 = 0x08
)

// ---------- Feature identifiers ----------

const (
	fidNumberOfQueues   uint8 = 0x07
	fidAsyncEventConfig uint8 = 0x0B
	fidKeepAliveTimer   uint8 = 0x0F
)

// ---------- Identify CNS types ----------

const (
	cnsIdentifyNamespace  uint8 = 0x00
	cnsIdentifyController uint8 = 0x01
	cnsActiveNSList       uint8 = 0x02
	cnsNSDescriptorList   uint8 = 0x03
)

// ---------- Log page identifiers ----------

const (
	logPageError uint8 = 0x01
	logPageSMART uint8 = 0x02
	logPageANA   uint8 = 0x0C
)

// ---------- Property register offsets ----------

const (
	propCAP  uint32 = 0x00 // Controller Capabilities
	propVS   uint32 = 0x08 // Version
	propCC   uint32 = 0x14 // Controller Configuration
	propCSTS uint32 = 0x1C // Controller Status
)

// ---------- ANA states ----------

const (
	anaOptimized     uint8 = 0x01
	anaNonOptimized  uint8 = 0x02
	anaInaccessible  uint8 = 0x03
	anaPersistentLoss uint8 = 0x04
	anaChange        uint8 = 0x0F
)

// ---------- Misc constants ----------

const (
	commonHeaderSize = 8
	maxHeaderSize    = 128
	maxH2CDataLen    = 0x8000 // 32 KB

	capsuleCmdSize  = 64 // CapsuleCommand specific header size (after CommonHeader)
	capsuleRespSize = 16 // CapsuleResponse specific header size
	c2hDataHdrSize  = 16 // C2HDataHeader specific header size
	icBodySize      = 120 // ICReq/ICResp body size (after CommonHeader)
	connectDataSize = 1024

	// Total header lengths including CommonHeader
	capsuleCmdHdrLen  = commonHeaderSize + capsuleCmdSize  // 72
	capsuleRespHdrLen = commonHeaderSize + capsuleRespSize // 24
	c2hDataHdrLen     = commonHeaderSize + c2hDataHdrSize  // 24
	icHdrLen          = commonHeaderSize + icBodySize       // 128

	commandBitDeallocate = 1 << 25

	nvmeVersion14 uint32 = 0x00010400 // NVMe 1.4

	// C2HData flags
	c2hFlagLast uint8 = 0x04
)

// ---------- CommonHeader (8 bytes) ----------

// CommonHeader is the 8-byte preamble of every NVMe/TCP PDU.
type CommonHeader struct {
	Type         uint8
	Flags        uint8
	HeaderLength uint8
	DataOffset   uint8
	DataLength   uint32
}

func (h *CommonHeader) Marshal(buf []byte) {
	buf[0] = h.Type
	buf[1] = h.Flags
	buf[2] = h.HeaderLength
	buf[3] = h.DataOffset
	binary.LittleEndian.PutUint32(buf[4:], h.DataLength)
}

func (h *CommonHeader) Unmarshal(buf []byte) {
	h.Type = buf[0]
	h.Flags = buf[1]
	h.HeaderLength = buf[2]
	h.DataOffset = buf[3]
	h.DataLength = binary.LittleEndian.Uint32(buf[4:])
}

func (h *CommonHeader) String() string {
	return fmt.Sprintf("PDU{type=0x%x hlen=%d doff=%d dlen=%d}",
		h.Type, h.HeaderLength, h.DataOffset, h.DataLength)
}

// ---------- PDU interface ----------

// PDU is the interface for all NVMe/TCP PDU-specific headers.
type PDU interface {
	Marshal([]byte)
	Unmarshal([]byte)
}

// ---------- ICRequest (120-byte body) ----------

// ICRequest is the host-to-controller initialization request.
type ICRequest struct {
	PDUFormatVersion uint16
	PDUDataAlignment uint8
	PDUDataDigest    uint8
	PDUMaxR2T        uint32
	// remaining 112 bytes reserved
}

func (r *ICRequest) Marshal(buf []byte) {
	// zero out the full 120-byte body
	for i := range buf[:icBodySize] {
		buf[i] = 0
	}
	binary.LittleEndian.PutUint16(buf[0:], r.PDUFormatVersion)
	buf[2] = r.PDUDataAlignment
	buf[3] = r.PDUDataDigest
	binary.LittleEndian.PutUint32(buf[4:], r.PDUMaxR2T)
}

func (r *ICRequest) Unmarshal(buf []byte) {
	r.PDUFormatVersion = binary.LittleEndian.Uint16(buf[0:])
	r.PDUDataAlignment = buf[2]
	r.PDUDataDigest = buf[3]
	r.PDUMaxR2T = binary.LittleEndian.Uint32(buf[4:])
}

// ---------- ICResponse (120-byte body) ----------

// ICResponse is the controller-to-host initialization response.
type ICResponse struct {
	PDUFormatVersion uint16
	PDUDataAlignment uint8
	PDUDataDigest    uint8
	MaxH2CDataLength uint32
	// remaining 112 bytes reserved
}

func (r *ICResponse) Marshal(buf []byte) {
	for i := range buf[:icBodySize] {
		buf[i] = 0
	}
	binary.LittleEndian.PutUint16(buf[0:], r.PDUFormatVersion)
	buf[2] = r.PDUDataAlignment
	buf[3] = r.PDUDataDigest
	binary.LittleEndian.PutUint32(buf[4:], r.MaxH2CDataLength)
}

func (r *ICResponse) Unmarshal(buf []byte) {
	r.PDUFormatVersion = binary.LittleEndian.Uint16(buf[0:])
	r.PDUDataAlignment = buf[2]
	r.PDUDataDigest = buf[3]
	r.MaxH2CDataLength = binary.LittleEndian.Uint32(buf[4:])
}

// ---------- CapsuleCommand (64-byte specific header) ----------

// CapsuleCommand is the 64-byte NVMe command capsule.
type CapsuleCommand struct {
	OpCode uint8
	PRP    uint8
	CID    uint16
	FCType uint8    // Fabric command type (only for OpCode=0x7F)
	NSID   uint32   // Namespace ID (bytes 4-7 of NVMe SQE after opcode/flags/CID)
	DPTR   [16]byte // Data pointer
	D10    uint32
	D11    uint32
	D12    uint32
	D13    uint32
	D14    uint32
	D15    uint32
}

// Lba returns the starting LBA from D10:D11 (Read/Write commands).
func (c *CapsuleCommand) Lba() uint64 {
	return uint64(c.D11)<<32 | uint64(c.D10)
}

// LbaLength returns the number of logical blocks (0-based in D12, actual = D12&0xFFFF + 1).
func (c *CapsuleCommand) LbaLength() uint32 {
	return c.D12&0xFFFF + 1
}

func (c *CapsuleCommand) Marshal(buf []byte) {
	for i := range buf[:capsuleCmdSize] {
		buf[i] = 0
	}
	buf[0] = c.OpCode
	buf[1] = c.PRP
	binary.LittleEndian.PutUint16(buf[2:], c.CID)
	// Bytes 4-7: NSID for normal commands, FCType at byte 4 for Fabric (0x7F).
	// They share the same offset per NVMe spec.
	if c.OpCode == adminFabric {
		buf[4] = c.FCType
	} else {
		binary.LittleEndian.PutUint32(buf[4:], c.NSID)
	}
	copy(buf[24:40], c.DPTR[:])
	binary.LittleEndian.PutUint32(buf[40:], c.D10)
	binary.LittleEndian.PutUint32(buf[44:], c.D11)
	binary.LittleEndian.PutUint32(buf[48:], c.D12)
	binary.LittleEndian.PutUint32(buf[52:], c.D13)
	binary.LittleEndian.PutUint32(buf[56:], c.D14)
	binary.LittleEndian.PutUint32(buf[60:], c.D15)
}

func (c *CapsuleCommand) Unmarshal(buf []byte) {
	c.OpCode = buf[0]
	c.PRP = buf[1]
	c.CID = binary.LittleEndian.Uint16(buf[2:])
	c.FCType = buf[4]
	c.NSID = binary.LittleEndian.Uint32(buf[4:])
	copy(c.DPTR[:], buf[24:40])
	c.D10 = binary.LittleEndian.Uint32(buf[40:])
	c.D11 = binary.LittleEndian.Uint32(buf[44:])
	c.D12 = binary.LittleEndian.Uint32(buf[48:])
	c.D13 = binary.LittleEndian.Uint32(buf[52:])
	c.D14 = binary.LittleEndian.Uint32(buf[56:])
	c.D15 = binary.LittleEndian.Uint32(buf[60:])
}

func (c *CapsuleCommand) String() string {
	return fmt.Sprintf("CapsuleCmd{op=0x%02x cid=%d nsid=%d}", c.OpCode, c.CID, c.NSID)
}

// ---------- CapsuleResponse (16-byte specific header) ----------

// CapsuleResponse is the NVMe completion queue entry (16 bytes).
type CapsuleResponse struct {
	DW0    uint32 // Command-specific DWord 0 (also FabricResponse bytes 0-3)
	DW1    uint32 // Command-specific DWord 1 (also FabricResponse bytes 4-7)
	SQHD   uint16 // Submission Queue Head Pointer
	QueueID uint16
	CID    uint16
	Status uint16 // Status field: DNR(15) | More(14) | SCT(13:9) | SC(8:1) | P(0)
}

func (r *CapsuleResponse) Marshal(buf []byte) {
	binary.LittleEndian.PutUint32(buf[0:], r.DW0)
	binary.LittleEndian.PutUint32(buf[4:], r.DW1)
	binary.LittleEndian.PutUint16(buf[8:], r.SQHD)
	binary.LittleEndian.PutUint16(buf[10:], r.QueueID)
	binary.LittleEndian.PutUint16(buf[12:], r.CID)
	binary.LittleEndian.PutUint16(buf[14:], r.Status)
}

func (r *CapsuleResponse) Unmarshal(buf []byte) {
	r.DW0 = binary.LittleEndian.Uint32(buf[0:])
	r.DW1 = binary.LittleEndian.Uint32(buf[4:])
	r.SQHD = binary.LittleEndian.Uint16(buf[8:])
	r.QueueID = binary.LittleEndian.Uint16(buf[10:])
	r.CID = binary.LittleEndian.Uint16(buf[12:])
	r.Status = binary.LittleEndian.Uint16(buf[14:])
}

func (r *CapsuleResponse) String() string {
	return fmt.Sprintf("CapsuleResp{sqhd=%d qid=%d cid=%d status=0x%04x}",
		r.SQHD, r.QueueID, r.CID, r.Status)
}

// ---------- C2HDataHeader (16-byte specific header) ----------

// C2HDataHeader is the controller-to-host data transfer header.
type C2HDataHeader struct {
	CCCID uint16 // Command Capsule CID
	_     uint16 // reserved
	DATAO uint32 // Data offset within the total transfer
	DATAL uint32 // Data length in this PDU
	_pad  uint32 // reserved
}

func (h *C2HDataHeader) Marshal(buf []byte) {
	for i := range buf[:c2hDataHdrSize] {
		buf[i] = 0
	}
	binary.LittleEndian.PutUint16(buf[0:], h.CCCID)
	binary.LittleEndian.PutUint32(buf[4:], h.DATAO)
	binary.LittleEndian.PutUint32(buf[8:], h.DATAL)
}

func (h *C2HDataHeader) Unmarshal(buf []byte) {
	h.CCCID = binary.LittleEndian.Uint16(buf[0:])
	h.DATAO = binary.LittleEndian.Uint32(buf[4:])
	h.DATAL = binary.LittleEndian.Uint32(buf[8:])
}

// ---------- ConnectData (1024 bytes, payload of Fabric Connect) ----------

// ConnectData is the 1024-byte payload sent with a Fabric Connect command.
type ConnectData struct {
	HostID [16]byte // Host UUID
	CNTLID uint16   // Requested controller ID (0xFFFF = new)
	SubNQN string   // Subsystem NQN
	HostNQN string  // Host NQN
}

func (d *ConnectData) Marshal(buf []byte) {
	for i := range buf[:connectDataSize] {
		buf[i] = 0
	}
	copy(buf[0:16], d.HostID[:])
	binary.LittleEndian.PutUint16(buf[16:], d.CNTLID)
	copyNQN(buf[256:512], d.SubNQN)
	copyNQN(buf[512:768], d.HostNQN)
}

func (d *ConnectData) Unmarshal(buf []byte) {
	copy(d.HostID[:], buf[0:16])
	d.CNTLID = binary.LittleEndian.Uint16(buf[16:])
	d.SubNQN = extractNQN(buf[256:512])
	d.HostNQN = extractNQN(buf[512:768])
}

// copyNQN writes a NUL-terminated string into a fixed-size buffer.
func copyNQN(dst []byte, s string) {
	n := copy(dst, s)
	if n < len(dst) {
		dst[n] = 0
	}
}

// extractNQN reads a NUL-terminated string from a fixed-size buffer.
func extractNQN(buf []byte) string {
	for i, b := range buf {
		if b == 0 {
			return string(buf[:i])
		}
	}
	return string(buf)
}

// ---------- Status word encoding ----------

// StatusWord encodes NVMe status: DNR(15) | More(14) | SCT(13:9) | SC(8:1) | P(0)
//
//	StatusWord = (DNR << 15) | (SCT << 9) | (SC << 1)
type StatusWord uint16

// MakeStatus constructs a status word from SCT, SC, and DNR flag.
func MakeStatus(sct, sc uint8, dnr bool) StatusWord {
	w := uint16(sct)<<9 | uint16(sc)<<1
	if dnr {
		w |= 1 << 15
	}
	return StatusWord(w)
}

// StatusSuccess is the zero-value success status.
const StatusSuccess StatusWord = 0

// Pre-defined status words used in the NVMe target.
var (
	StatusInvalidOpcode   = MakeStatus(0, 0x01, true)  // Generic: Invalid Command Opcode
	StatusInvalidField    = MakeStatus(0, 0x02, true)  // Generic: Invalid Field in Command
	StatusInternalError   = MakeStatus(0, 0x06, false) // Generic: Internal Error (retryable)
	StatusInternalErrorDNR = MakeStatus(0, 0x06, true)  // Generic: Internal Error (permanent)
	StatusNSNotReady      = MakeStatus(0, 0x82, false) // Generic: Namespace Not Ready (retryable)
	StatusNSNotReadyDNR   = MakeStatus(0, 0x82, true)  // Generic: Namespace Not Ready (permanent)
	StatusLBAOutOfRange   = MakeStatus(0, 0x80, true)  // Generic: LBA Out of Range
	StatusMediaWriteFault = MakeStatus(2, 0x80, false) // Media: Write Fault
	StatusMediaReadError  = MakeStatus(2, 0x81, false) // Media: Uncorrectable Read Error
)

func (s StatusWord) SCT() uint8  { return uint8((s >> 9) & 0x07) }
func (s StatusWord) SC() uint8   { return uint8((s >> 1) & 0xFF) }
func (s StatusWord) DNR() bool   { return s&(1<<15) != 0 }
func (s StatusWord) IsError() bool { return s != StatusSuccess }

func (s StatusWord) String() string {
	if s == StatusSuccess {
		return "Success"
	}
	return fmt.Sprintf("Status{sct=%d sc=0x%02x dnr=%v}", s.SCT(), s.SC(), s.DNR())
}
