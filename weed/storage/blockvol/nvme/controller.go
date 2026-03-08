package nvme

import (
	"fmt"
	"io"
	"log"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

// controllerState tracks the lifecycle of an NVMe controller session.
type controllerState int

const (
	stateConnected  controllerState = iota // TCP connected, no IC yet
	stateICComplete                        // IC exchange done
	stateAdminReady                        // Admin queue connected
	stateCtrlReady                         // CC.EN=1, CSTS.RDY=1
	stateIOActive                          // IO queues active
	stateClosed                            // Shut down
)

// Request represents an in-flight NVMe command being processed.
type Request struct {
	capsule CapsuleCommand
	payload []byte // inline data from host (Write commands)
	resp    CapsuleResponse
	c2hData []byte  // data to send to host (Read commands)
	status  StatusWord
}

// Controller handles one NVMe/TCP connection (one queue per connection).
type Controller struct {
	mu sync.Mutex

	// Session identity
	conn   net.Conn
	in     *Reader
	out    *Writer
	state  controllerState
	closed atomic.Bool

	// Queue state (one queue per TCP connection)
	queueID    uint16
	queueSize  uint16
	sqhd       uint16 // Submission Queue Head pointer
	flowCtlOff bool   // CATTR bit2: SQ flow control disabled

	// Controller identity
	cntlID uint16
	subNQN string

	// Controller registers
	regCAP  uint64 // Controller Capabilities
	regCC   uint32 // Controller Configuration (set by host via PropertySet)
	regCSTS uint32 // Controller Status (RDY bit)
	regVS   uint32 // Version

	// KeepAlive
	katoMs    uint32
	katoTimer *time.Timer
	katoMu    sync.Mutex

	// Async completion (IO queues)
	waiting     chan *Request // pre-allocated request pool
	completions chan *Request // completed requests to send

	// Backend
	subsystem *Subsystem
	server    *Server

	// Features
	maxIOQueues   uint16
	grantedQueues uint16
	isAdmin       bool // true if this controller owns admin queue (QID=0)

	// Lifecycle
	wg     sync.WaitGroup
	closeOnce sync.Once
}

// newController creates a controller for the given connection.
func newController(conn net.Conn, server *Server) *Controller {
	c := &Controller{
		conn:   conn,
		in:     NewReader(conn),
		out:    NewWriter(conn),
		state:  stateConnected,
		server: server,
		regVS:  nvmeVersion14,
		// CAP register: MQES=63 (bits 15:0), CQR=1 (bit 16), TO=30 (bits 31:24, *500ms=15s), CSS bit37=1 (NVM command set)
		regCAP: uint64(63) | (1 << 16) | (uint64(30) << 24) | (1 << 37),
		maxIOQueues: server.cfg.MaxIOQueues,
	}
	return c
}

// Serve is the main event loop for this controller connection.
func (c *Controller) Serve() error {
	defer c.shutdown()

	// IC handshake timeout
	if err := c.conn.SetReadDeadline(time.Now().Add(10 * time.Second)); err != nil {
		return err
	}

	for {
		if c.closed.Load() {
			return nil
		}

		hdr, err := c.in.Dequeue()
		if err != nil {
			if err == io.EOF || c.closed.Load() {
				return nil
			}
			return fmt.Errorf("read header: %w", err)
		}

		switch hdr.Type {
		case pduICReq:
			if err := c.handleIC(); err != nil {
				return fmt.Errorf("IC handshake: %w", err)
			}
			// Clear read deadline after successful IC
			if err := c.conn.SetReadDeadline(time.Time{}); err != nil {
				return err
			}

		case pduCapsuleCmd:
			if err := c.handleCapsule(); err != nil {
				return fmt.Errorf("capsule: %w", err)
			}

		case pduH2CTermReq:
			return nil // host terminated

		default:
			return fmt.Errorf("unexpected PDU type: 0x%x", hdr.Type)
		}
	}
}

// handleIC processes the IC handshake.
func (c *Controller) handleIC() error {
	var req ICRequest
	if err := c.in.Receive(&req); err != nil {
		return err
	}

	resp := ICResponse{
		PDUFormatVersion: 0,
		MaxH2CDataLength: maxH2CDataLen,
	}
	if err := c.out.SendHeaderOnly(pduICResp, &resp, icBodySize); err != nil {
		return err
	}

	c.state = stateICComplete
	return nil
}

// handleCapsule dispatches a CapsuleCmd PDU.
func (c *Controller) handleCapsule() error {
	// Reject capsule commands before IC handshake is complete.
	if c.state < stateICComplete {
		return fmt.Errorf("capsule command before IC handshake")
	}

	var capsule CapsuleCommand
	if err := c.in.Receive(&capsule); err != nil {
		return err
	}

	// Read optional inline data
	var payload []byte
	if dataLen := c.in.Length(); dataLen > 0 {
		payload = make([]byte, dataLen)
		if err := c.in.ReceiveData(payload); err != nil {
			return err
		}
	}

	// Advance SQHD
	c.sqhd++
	if c.sqhd >= c.queueSize && c.queueSize > 0 {
		c.sqhd = 0
	}

	req := &Request{
		capsule: capsule,
		payload: payload,
	}
	req.resp.CID = capsule.CID
	req.resp.QueueID = c.queueID
	// SQHD is set in sendResponse/sendC2HDataAndResponse using the
	// latest c.flowCtlOff value, so Connect responses correctly get
	// SQHD=0xFFFF when the host requests flowCtlOff via CATTR.
	req.resp.Status = uint16(StatusSuccess)

	if c.queueID == 0 {
		return c.dispatchAdmin(req)
	}
	return c.dispatchIO(req)
}

// dispatchAdmin handles admin queue commands synchronously.
func (c *Controller) dispatchAdmin(req *Request) error {
	capsule := &req.capsule

	if capsule.OpCode == adminFabric {
		return c.handleFabricCommand(req)
	}

	switch capsule.OpCode {
	case adminIdentify:
		return c.handleIdentify(req)
	case adminSetFeatures:
		return c.handleSetFeatures(req)
	case adminGetFeatures:
		return c.handleGetFeatures(req)
	case adminGetLogPage:
		return c.handleGetLogPage(req)
	case adminKeepAlive:
		return c.handleKeepAlive(req)
	case adminAsyncEvent:
		// Stub: just succeed (don't deliver events in CP10-1)
		return c.sendResponse(req)
	default:
		req.resp.Status = uint16(StatusInvalidOpcode)
		return c.sendResponse(req)
	}
}

// dispatchIO handles IO queue commands.
func (c *Controller) dispatchIO(req *Request) error {
	capsule := &req.capsule

	switch capsule.OpCode {
	case ioRead:
		return c.handleRead(req)
	case ioWrite:
		return c.handleWrite(req)
	case ioFlush:
		return c.handleFlush(req)
	case ioWriteZeros:
		return c.handleWriteZeros(req)
	default:
		req.resp.Status = uint16(StatusInvalidOpcode)
		return c.sendResponse(req)
	}
}

// sendC2HDataAndResponse sends C2HData PDUs followed by a CapsuleResp.
func (c *Controller) sendC2HDataAndResponse(req *Request) error {
	if len(req.c2hData) > 0 {
		data := req.c2hData
		offset := uint32(0)
		chunkSize := uint32(maxH2CDataLen)

		for offset < uint32(len(data)) {
			end := offset + chunkSize
			if end > uint32(len(data)) {
				end = uint32(len(data))
			}
			chunk := data[offset:end]

			hdr := C2HDataHeader{
				CCCID: req.capsule.CID,
				DATAO: offset,
				DATAL: uint32(len(chunk)),
			}

			flags := uint8(0)
			if end >= uint32(len(data)) {
				flags = c2hFlagLast
			}

			if err := c.out.SendWithData(pduC2HData, flags, &hdr, c2hDataHdrSize, chunk); err != nil {
				return err
			}
			offset = end
		}
	}

	return c.sendResponse(req)
}

// sendResponse sends a CapsuleResp PDU.
// SQHD is set here (not in handleCapsule) so that flowCtlOff changes
// made during command dispatch (e.g. Fabric Connect) take effect
// on the same response.
func (c *Controller) sendResponse(req *Request) error {
	if c.flowCtlOff {
		req.resp.SQHD = 0xFFFF
	} else {
		req.resp.SQHD = c.sqhd
	}
	c.resetKATO()
	return c.out.SendHeaderOnly(pduCapsuleResp, &req.resp, capsuleRespSize)
}

// ---------- KATO management ----------

func (c *Controller) startKATO() {
	c.katoMu.Lock()
	defer c.katoMu.Unlock()
	if c.katoMs == 0 {
		return
	}
	d := time.Duration(c.katoMs) * time.Millisecond
	// Add 50% margin per spec recommendation
	d = d + d/2
	c.katoTimer = time.AfterFunc(d, func() {
		log.Printf("nvme: KATO expired for cntlid=%d, closing connection", c.cntlID)
		c.conn.Close()
	})
}

func (c *Controller) resetKATO() {
	c.katoMu.Lock()
	defer c.katoMu.Unlock()
	if c.katoTimer != nil {
		c.katoTimer.Reset(time.Duration(c.katoMs)*time.Millisecond + time.Duration(c.katoMs)*time.Millisecond/2)
	}
}

func (c *Controller) stopKATO() {
	c.katoMu.Lock()
	defer c.katoMu.Unlock()
	if c.katoTimer != nil {
		c.katoTimer.Stop()
		c.katoTimer = nil
	}
}

// ---------- Lifecycle ----------

func (c *Controller) shutdown() {
	c.closeOnce.Do(func() {
		c.closed.Store(true)
		c.stopKATO()
		c.state = stateClosed
		c.conn.Close()
		if c.server != nil {
			if c.isAdmin && c.cntlID != 0 {
				c.server.unregisterAdmin(c.cntlID)
			}
			c.server.removeSession(c)
		}
	})
}
