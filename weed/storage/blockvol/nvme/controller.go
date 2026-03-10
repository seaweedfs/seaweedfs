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
	isAdmin       bool   // true if this controller owns admin queue (QID=0)
	maxDataLen    uint32 // C2H/H2C data chunk size (from Config)

	// Command interleaving: capsules received during R2T H2CData collection.
	// Drained by Serve() before reading the next PDU from the wire.
	pendingCapsules []*Request

	// Lifecycle
	wg     sync.WaitGroup
	closeOnce sync.Once
}

// newController creates a controller for the given connection.
func newController(conn net.Conn, server *Server) *Controller {
	maxData := server.cfg.MaxH2CDataLength
	if maxData == 0 {
		maxData = maxH2CDataLen // fallback to 32KB default
	}
	c := &Controller{
		conn:   conn,
		in:     NewReader(conn),
		out:    NewWriterSize(conn, int(maxData)+maxHeaderSize),
		state:  stateConnected,
		server: server,
		regVS:  nvmeVersion14,
		// CAP register: MQES=63 (bits 15:0), CQR=1 (bit 16), TO=30 (bits 31:24, *500ms=15s), CSS bit37=1 (NVM command set)
		regCAP:      uint64(63) | (1 << 16) | (uint64(30) << 24) | (1 << 37),
		maxIOQueues: server.cfg.MaxIOQueues,
		maxDataLen:  maxData,
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

		// Drain capsules that arrived during a prior R2T data collection.
		for len(c.pendingCapsules) > 0 {
			req := c.pendingCapsules[0]
			c.pendingCapsules = c.pendingCapsules[1:]
			if err := c.dispatchPending(req); err != nil {
				return fmt.Errorf("pending capsule: %w", err)
			}
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

		case pduH2CData:
			// H2CData PDUs are only expected after R2T, handled inline
			// by recvH2CData. If we see one here, it's unexpected.
			return fmt.Errorf("unexpected H2CData PDU outside R2T flow")

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
		MaxH2CDataLength: c.maxDataLen,
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
		payload = getBuffer(int(dataLen))
		if err := c.in.ReceiveData(payload); err != nil {
			putBuffer(payload)
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

// dispatchPending processes a capsule that was buffered during R2T data
// collection. The capsule and payload are already fully read — only
// SQHD advance and command dispatch remain.
func (c *Controller) dispatchPending(req *Request) error {
	c.sqhd++
	if c.sqhd >= c.queueSize && c.queueSize > 0 {
		c.sqhd = 0
	}
	if c.queueID == 0 {
		return c.dispatchAdmin(req)
	}
	return c.dispatchIO(req)
}

// dispatchAdmin handles admin queue commands synchronously.
func (c *Controller) dispatchAdmin(req *Request) error {
	defer func() {
		if req.payload != nil {
			putBuffer(req.payload)
			req.payload = nil
		}
	}()
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
	defer func() {
		if req.payload != nil {
			putBuffer(req.payload)
			req.payload = nil
		}
	}()
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
// All chunks and the final response are batched in the bufio buffer,
// then flushed to the wire in a single FlushBuf() call.
func (c *Controller) sendC2HDataAndResponse(req *Request) error {
	if len(req.c2hData) > 0 {
		data := req.c2hData
		offset := uint32(0)
		chunkSize := c.maxDataLen

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

			if err := c.out.writeHeaderAndData(pduC2HData, flags, &hdr, c2hDataHdrSize, chunk); err != nil {
				return err
			}
			offset = end
		}
	}

	// Write CapsuleResp to bufio buffer
	if c.flowCtlOff {
		req.resp.SQHD = 0xFFFF
	} else {
		req.resp.SQHD = c.sqhd
	}
	c.resetKATO()
	if err := c.out.writeHeaderAndData(pduCapsuleResp, 0, &req.resp, capsuleRespSize, nil); err != nil {
		return err
	}

	// Single flush: all C2H chunks + CapsuleResp in one syscall
	return c.out.FlushBuf()
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

// ---------- R2T / H2C Data ----------

// sendR2T sends a Ready-to-Transfer PDU requesting data from the host.
func (c *Controller) sendR2T(cid uint16, tag uint16, offset, length uint32) error {
	r2t := R2THeader{
		CCCID: cid,
		TAG:   tag,
		DATAO: offset,
		DATAL: length,
	}
	return c.out.SendHeaderOnly(pduR2T, &r2t, r2tHdrSize)
}

// recvH2CData reads H2CData PDU(s) from the wire and returns the accumulated data.
// Reads exactly `totalBytes` of data, potentially across multiple H2C PDUs.
//
// At QD>1 the host may interleave CapsuleCmd PDUs on the same connection
// before the H2CData for a prior R2T arrives. Such capsules are fully read
// and buffered in c.pendingCapsules for dispatch after the current command
// completes (NVMe/TCP spec §3.5 — command pipelining).
func (c *Controller) recvH2CData(totalBytes uint32) ([]byte, error) {
	buf := getBuffer(int(totalBytes))
	received := uint32(0)

	for received < totalBytes {
		hdr, err := c.in.Dequeue()
		if err != nil {
			putBuffer(buf)
			return nil, fmt.Errorf("recvH2CData: read header: %w", err)
		}

		// Interleaved CapsuleCmd: buffer it for later dispatch.
		if hdr.Type == pduCapsuleCmd {
			if err := c.bufferInterleaved(); err != nil {
				putBuffer(buf)
				return nil, fmt.Errorf("recvH2CData: buffer interleaved capsule: %w", err)
			}
			continue
		}

		if hdr.Type != pduH2CData {
			putBuffer(buf)
			return nil, fmt.Errorf("recvH2CData: expected H2CData (0x6), got 0x%x", hdr.Type)
		}

		var h2c H2CDataHeader
		if err := c.in.Receive(&h2c); err != nil {
			putBuffer(buf)
			return nil, fmt.Errorf("recvH2CData: receive header: %w", err)
		}

		dataLen := c.in.Length()
		if dataLen == 0 {
			putBuffer(buf)
			return nil, fmt.Errorf("recvH2CData: H2CData PDU has no payload")
		}
		if h2c.DATAO+dataLen > totalBytes {
			putBuffer(buf)
			return nil, fmt.Errorf("recvH2CData: data exceeds expected size (%d+%d > %d)",
				h2c.DATAO, dataLen, totalBytes)
		}

		if err := c.in.ReceiveData(buf[h2c.DATAO : h2c.DATAO+dataLen]); err != nil {
			putBuffer(buf)
			return nil, fmt.Errorf("recvH2CData: receive data: %w", err)
		}
		received += dataLen
	}

	return buf, nil
}

// bufferInterleaved reads a complete CapsuleCmd (header + optional inline
// data) that arrived during R2T data collection and appends it to
// c.pendingCapsules. Called from recvH2CData when hdr.Type == pduCapsuleCmd.
func (c *Controller) bufferInterleaved() error {
	var capsule CapsuleCommand
	if err := c.in.Receive(&capsule); err != nil {
		return err
	}

	var payload []byte
	if dataLen := c.in.Length(); dataLen > 0 {
		payload = getBuffer(int(dataLen))
		if err := c.in.ReceiveData(payload); err != nil {
			putBuffer(payload)
			return err
		}
	}

	req := &Request{
		capsule: capsule,
		payload: payload,
	}
	req.resp.CID = capsule.CID
	req.resp.QueueID = c.queueID
	req.resp.Status = uint16(StatusSuccess)

	c.pendingCapsules = append(c.pendingCapsules, req)
	return nil
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
