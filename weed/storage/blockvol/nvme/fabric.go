package nvme

import (
	"encoding/binary"
)

// handleFabricCommand dispatches Fabric-specific commands by FCType.
// Returns errDisconnect on Disconnect to signal the rxLoop to exit gracefully.
func (c *Controller) handleFabricCommand(req *Request) error {
	switch req.capsule.FCType {
	case fcConnect:
		c.handleConnect(req)
		return nil
	case fcPropertyGet:
		c.handlePropertyGet(req)
		return nil
	case fcPropertySet:
		c.handlePropertySet(req)
		return nil
	case fcDisconnect:
		return c.handleDisconnect(req)
	default:
		req.resp.Status = uint16(StatusInvalidField)
		c.enqueueResponse(&response{resp: req.resp})
		return nil
	}
}

// handleConnect processes a Fabric Connect command.
func (c *Controller) handleConnect(req *Request) {
	capsule := &req.capsule

	// Parse QueueID, QueueSize, KATO, CATTR from capsule dwords.
	// Connect command layout (CDW10-CDW12):
	//   CDW10[15:0]=RECFM, CDW10[31:16]=QID
	//   CDW11[15:0]=SQSIZE, CDW11[23:16]=CATTR
	//   CDW12=KATO
	queueID := uint16(capsule.D10 >> 16)
	queueSize := uint16(capsule.D11&0xFFFF) + 1 // SQSIZE is 0-based
	cattr := uint8(capsule.D11 >> 16)
	kato := capsule.D12

	// Parse ConnectData from payload
	if len(req.payload) < connectDataSize {
		req.resp.Status = uint16(StatusInvalidField)
		c.enqueueResponse(&response{resp: req.resp})
		return
	}
	var cd ConnectData
	cd.Unmarshal(req.payload)

	if queueID == 0 {
		// Admin queue connect
		sub := c.server.findSubsystem(cd.SubNQN)
		if sub == nil {
			req.resp.Status = uint16(StatusInvalidField)
			c.enqueueResponse(&response{resp: req.resp})
			return
		}

		c.subsystem = sub
		c.subNQN = cd.SubNQN
		c.queueID = 0
		c.queueSize = queueSize
		c.cntlID = c.server.allocCNTLID()
		c.katoMs = kato
		c.flowCtlOff = (cattr & 0x04) != 0
		c.state = stateAdminReady
		c.isAdmin = true

		// Register admin session so IO queue connections can find us.
		c.server.registerAdmin(&adminSession{
			cntlID:    c.cntlID,
			subsystem: sub,
			subNQN:    cd.SubNQN,
			hostNQN:   cd.HostNQN,
			regCAP:    c.regCAP,
			regCC:     c.regCC,
			regCSTS:   c.regCSTS,
			regVS:     c.regVS,
			katoMs:    kato,
		})

		// Return CNTLID in DW0
		req.resp.DW0 = uint32(c.cntlID)
		c.enqueueResponse(&response{resp: req.resp})
		return
	}

	// IO queue connect — look up admin session from server registry.
	// IO queues arrive on separate TCP connections with fresh Controllers,
	// so we must find the admin session by CNTLID from the server.
	admin := c.server.lookupAdmin(cd.CNTLID)
	if admin == nil {
		req.resp.Status = uint16(StatusInvalidField)
		c.enqueueResponse(&response{resp: req.resp})
		return
	}

	// Validate SubNQN and HostNQN match the admin session.
	if cd.SubNQN != admin.subNQN {
		req.resp.Status = uint16(StatusInvalidField)
		c.enqueueResponse(&response{resp: req.resp})
		return
	}
	if cd.HostNQN != admin.hostNQN {
		req.resp.Status = uint16(StatusInvalidField)
		c.enqueueResponse(&response{resp: req.resp})
		return
	}

	c.cntlID = cd.CNTLID
	c.subsystem = admin.subsystem
	c.subNQN = admin.subNQN
	c.queueID = queueID
	c.queueSize = queueSize
	c.flowCtlOff = (cattr & 0x04) != 0
	c.state = stateIOActive

	req.resp.DW0 = uint32(c.cntlID)
	c.enqueueResponse(&response{resp: req.resp})
}

// handlePropertyGet returns a controller register value.
func (c *Controller) handlePropertyGet(req *Request) {
	// Per NVMe-oF spec: CDW10 bits 2:0 = ATTRIB (size), CDW11 = OFST (offset)
	size8 := (req.capsule.D10 & 1) != 0
	offset := req.capsule.D11

	var val uint64
	switch offset {
	case propCAP:
		val = c.regCAP
	case propVS:
		val = uint64(c.regVS)
	case propCC:
		val = uint64(c.regCC)
	case propCSTS:
		val = uint64(c.regCSTS)
	default:
		req.resp.Status = uint16(StatusInvalidField)
		c.enqueueResponse(&response{resp: req.resp})
		return
	}

	if size8 {
		// 8-byte value in DW0+DW1
		req.resp.DW0 = uint32(val)
		req.resp.DW1 = uint32(val >> 32)
	} else {
		req.resp.DW0 = uint32(val)
	}
	c.enqueueResponse(&response{resp: req.resp})
}

// handlePropertySet handles controller register writes.
func (c *Controller) handlePropertySet(req *Request) {
	// Per NVMe-oF spec: CDW10 = ATTRIB (size), CDW11 = OFST (offset), CDW12-CDW13 = VALUE
	offset := req.capsule.D11
	value := uint64(req.capsule.D12) | uint64(req.capsule.D13)<<32

	switch offset {
	case propCC:
		c.regCC = uint32(value)
		// Check CC.EN (bit 0)
		if c.regCC&1 != 0 {
			c.regCSTS |= 1 // Set CSTS.RDY
			c.state = stateCtrlReady
			if c.katoMs > 0 {
				c.startKATO()
			}
		} else {
			c.regCSTS &^= 1 // Clear CSTS.RDY
		}
	default:
		// Ignore writes to other registers
	}
	c.enqueueResponse(&response{resp: req.resp})
}

// handleDisconnect processes a Fabric Disconnect.
// Enqueues the response and returns errDisconnect to signal rxLoop exit.
func (c *Controller) handleDisconnect(req *Request) error {
	c.enqueueResponse(&response{resp: req.resp})
	return errDisconnect
}

// ---------- Subsystem ----------

// Subsystem represents an NVMe subsystem backed by a BlockDevice.
type Subsystem struct {
	NQN  string
	Dev  BlockDevice
	NGUID [16]byte // Namespace GUID
}

// BlockDevice is the interface for the underlying storage.
// This is the same as iscsi.BlockDevice.
type BlockDevice interface {
	ReadAt(lba uint64, length uint32) ([]byte, error)
	WriteAt(lba uint64, data []byte) error
	Trim(lba uint64, length uint32) error
	SyncCache() error
	BlockSize() uint32
	VolumeSize() uint64
	IsHealthy() bool
}

// ANAProvider extends BlockDevice with ANA state reporting.
type ANAProvider interface {
	ANAState() uint8
	ANAGroupID() uint16
	DeviceNGUID() [16]byte
}

// allocCNTLID allocates a new controller ID from the server.
func (s *Server) allocCNTLID() uint16 {
	return uint16(s.nextCNTLID.Add(1))
}

// findSubsystem looks up a subsystem by NQN.
func (s *Server) findSubsystem(nqn string) *Subsystem {
	s.mu.RLock()
	defer s.mu.RUnlock()
	sub, ok := s.subsystems[nqn]
	if !ok {
		return nil
	}
	return sub
}

// ---------- ConnectData field access helpers ----------

// connectQueueID extracts the QueueID from a Connect capsule D10.
func connectQueueID(capsule *CapsuleCommand) uint16 {
	return uint16(capsule.D10 >> 16)
}

// connectQueueSize extracts the QueueSize from a Connect capsule D11 (0-based → +1).
func connectQueueSize(capsule *CapsuleCommand) uint16 {
	return uint16(capsule.D11&0xFFFF) + 1
}

// connectKATO extracts the KeepAlive timeout from a Connect capsule D12.
func connectKATO(capsule *CapsuleCommand) uint32 {
	return capsule.D12
}

// propertySetValue extracts the value from a PropertySet capsule (CDW12-CDW13).
func propertySetValue(capsule *CapsuleCommand) uint64 {
	return uint64(capsule.D12) | uint64(capsule.D13)<<32
}

// propertyGetSize8 returns true if the PropertyGet requests an 8-byte value.
func propertyGetSize8(capsule *CapsuleCommand) bool {
	return (capsule.D10 & 1) != 0
}

// propertyGetOffset returns the register offset for PropertyGet.
func propertyGetOffset(capsule *CapsuleCommand) uint32 {
	return capsule.D11
}

// ---------- ConnectData marshal helpers for tests ----------

func marshalConnectData(cd *ConnectData) []byte {
	buf := make([]byte, connectDataSize)
	cd.Marshal(buf)
	return buf
}

func makeConnectCapsule(queueID, queueSize uint16, kato uint32, fcType uint8) CapsuleCommand {
	return CapsuleCommand{
		OpCode: adminFabric,
		FCType: fcType,
		D10:    uint32(queueID) << 16,
		D11:    uint32(queueSize - 1), // 0-based
		D12:    kato,
	}
}

// makePropertyGetCapsule creates a PropertyGet capsule for the given register offset.
// Per NVMe-oF spec: CDW10 = ATTRIB (size), CDW11 = OFST (offset).
func makePropertyGetCapsule(offset uint32, size8 bool) CapsuleCommand {
	c := CapsuleCommand{
		OpCode: adminFabric,
		FCType: fcPropertyGet,
		D11:    offset,
	}
	if size8 {
		c.D10 = 1
	}
	return c
}

// makePropertySetCapsule creates a PropertySet capsule.
// Per NVMe-oF spec: CDW10 = ATTRIB (size), CDW11 = OFST (offset), CDW12-13 = VALUE.
func makePropertySetCapsule(offset uint32, value uint64) CapsuleCommand {
	return CapsuleCommand{
		OpCode: adminFabric,
		FCType: fcPropertySet,
		D11:    offset,
		D12:    uint32(value),
		D13:    uint32(value >> 32),
	}
}

// putCNTLID stores the controller ID in ConnectData at offset 16.
func putCNTLID(buf []byte, cntlid uint16) {
	binary.LittleEndian.PutUint16(buf[16:], cntlid)
}
