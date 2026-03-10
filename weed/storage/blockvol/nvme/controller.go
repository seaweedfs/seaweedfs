package nvme

import (
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

// errDisconnect is a sentinel returned by handleDisconnect to signal
// the rxLoop to exit gracefully after enqueuing the disconnect response.
var errDisconnect = errors.New("disconnect")

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

// response represents a pending response to be written by the txLoop.
// Either a standard CapsuleResp (with optional C2H data), or an R2T PDU.
type response struct {
	// Standard response fields
	resp    CapsuleResponse
	c2hData []byte // non-nil for read responses

	// R2T variant (when r2t is non-nil, resp/c2hData are ignored)
	r2t     *R2THeader
	r2tDone chan struct{} // closed after R2T is flushed to wire
}

// Controller handles one NVMe/TCP connection (one queue per connection).
//
// After the IC handshake, the connection is split into two goroutines:
//   - rxLoop: reads PDUs from the wire, dispatches commands
//   - txLoop: drains the response channel, writes responses to the wire
//
// IO commands are dispatched to goroutines for concurrent processing.
// Admin commands run synchronously in the rxLoop (infrequent, state-changing).
// All responses flow through respCh — only txLoop writes to c.out.
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
	rxSQHD     uint16        // Submission Queue Head (updated by rxLoop only)
	sqhdVal    atomic.Uint32 // Latest SQHD for txLoop (lower 16 bits)
	flowCtlOff bool          // CATTR bit2: SQ flow control disabled

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

	// RX/TX split
	respCh chan *response // responses from handlers → txLoop
	done   chan struct{}  // closed when connection is shutting down
	cmdWg  sync.WaitGroup // tracks in-flight IO command goroutines

	// Backend
	subsystem *Subsystem
	server    *Server

	// Features
	maxIOQueues   uint16
	grantedQueues uint16
	isAdmin       bool   // true if this controller owns admin queue (QID=0)
	maxDataLen    uint32 // C2H/H2C data chunk size (from Config)

	// Command interleaving: capsules received during R2T H2CData collection.
	// Drained by rxLoop before reading the next PDU from the wire.
	pendingCapsules []*Request

	// Lifecycle
	wg        sync.WaitGroup
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
		in:     NewReaderSize(conn, int(maxData)+maxHeaderSize),
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
//
// Phase 1: IC handshake (synchronous, direct c.out access).
// Phase 2: Start txLoop goroutine, enter rxLoop.
func (c *Controller) Serve() error {
	defer c.shutdown()

	// Phase 1: IC handshake with timeout (synchronous).
	if err := c.conn.SetReadDeadline(time.Now().Add(10 * time.Second)); err != nil {
		return err
	}
	hdr, err := c.in.Dequeue()
	if err != nil {
		return fmt.Errorf("IC handshake: %w", err)
	}
	if hdr.Type != pduICReq {
		return fmt.Errorf("expected ICReq (0x%x), got 0x%x", pduICReq, hdr.Type)
	}
	if err := c.handleIC(); err != nil {
		return fmt.Errorf("IC handshake: %w", err)
	}
	if err := c.conn.SetReadDeadline(time.Time{}); err != nil {
		return err
	}

	// Phase 2: Start TX loop, enter RX loop.
	c.respCh = make(chan *response, 128)
	c.done = make(chan struct{})

	txErrCh := make(chan error, 1)
	go func() {
		txErrCh <- c.txLoop()
	}()

	rxErr := c.rxLoop()

	// Wait for in-flight IO command goroutines to finish and enqueue responses.
	c.cmdWg.Wait()

	// Signal txLoop to drain remaining responses and exit.
	close(c.done)
	txErr := <-txErrCh

	// errDisconnect is a graceful exit, not an error.
	if errors.Is(rxErr, errDisconnect) {
		rxErr = nil
	}
	if rxErr != nil {
		return rxErr
	}
	return txErr
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

// rxLoop reads PDUs from the wire and dispatches commands.
// Admin commands (QID=0) run synchronously. IO commands dispatch to goroutines.
func (c *Controller) rxLoop() error {
	for {
		if c.closed.Load() {
			return nil
		}

		// Drain capsules that arrived during a prior R2T data collection.
		for len(c.pendingCapsules) > 0 {
			req := c.pendingCapsules[0]
			c.pendingCapsules = c.pendingCapsules[1:]
			c.advanceSQHD()
			if err := c.dispatchFromRx(req); err != nil {
				return err
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
		case pduCapsuleCmd:
			if c.state < stateICComplete {
				return fmt.Errorf("capsule command before IC handshake")
			}
			req, err := c.parseCapsule()
			if err != nil {
				return fmt.Errorf("capsule: %w", err)
			}
			c.advanceSQHD()
			if err := c.dispatchFromRx(req); err != nil {
				return err
			}

		case pduH2CData:
			return fmt.Errorf("unexpected H2CData PDU outside R2T flow")

		case pduH2CTermReq:
			return nil

		default:
			return fmt.Errorf("unexpected PDU type: 0x%x", hdr.Type)
		}
	}
}

// txLoop drains the response channel and writes responses to the wire.
// Only txLoop touches c.out after the IC handshake.
func (c *Controller) txLoop() error {
	var firstErr error
	for {
		select {
		case resp := <-c.respCh:
			if firstErr != nil {
				completeWaiters(resp)
				continue // discard — connection already failed
			}
			if err := c.writeResponse(resp); err != nil {
				firstErr = err
				c.conn.Close() // force rxLoop EOF
			}
		case <-c.done:
			// Drain remaining responses.
			for {
				select {
				case resp := <-c.respCh:
					if firstErr == nil {
						if err := c.writeResponse(resp); err != nil {
							firstErr = err
						}
					} else {
						completeWaiters(resp)
					}
				default:
					return firstErr
				}
			}
		}
	}
}

// parseCapsule reads a CapsuleCmd PDU (specific header + optional inline data).
func (c *Controller) parseCapsule() (*Request, error) {
	var capsule CapsuleCommand
	if err := c.in.Receive(&capsule); err != nil {
		return nil, err
	}

	var payload []byte
	if dataLen := c.in.Length(); dataLen > 0 {
		payload = getBuffer(int(dataLen))
		if err := c.in.ReceiveData(payload); err != nil {
			putBuffer(payload)
			return nil, err
		}
	}

	req := &Request{
		capsule: capsule,
		payload: payload,
	}
	req.resp.CID = capsule.CID
	req.resp.QueueID = c.queueID
	req.resp.Status = uint16(StatusSuccess)
	return req, nil
}

// advanceSQHD increments the submission queue head pointer (rxLoop only).
func (c *Controller) advanceSQHD() {
	sqhd := c.rxSQHD + 1
	if sqhd >= c.queueSize && c.queueSize > 0 {
		sqhd = 0
	}
	c.rxSQHD = sqhd
	c.sqhdVal.Store(uint32(sqhd))
}

// dispatchFromRx routes a parsed capsule to the appropriate handler.
// Admin queue: synchronous in rxLoop. IO queue: concurrent goroutine.
func (c *Controller) dispatchFromRx(req *Request) error {
	if c.queueID == 0 {
		return c.dispatchAdmin(req)
	}

	// For Write commands without inline data, collect R2T data in rxLoop
	// before dispatching to the goroutine.
	if req.capsule.OpCode == ioWrite && len(req.payload) == 0 {
		if err := c.collectR2TData(req); err != nil {
			return err
		}
	}

	c.cmdWg.Add(1)
	go func() {
		defer c.cmdWg.Done()
		c.dispatchIO(req)
	}()
	return nil
}

// collectR2TData handles the R2T/H2C flow in the rxLoop for Write commands
// that don't carry inline data. Sends R2T through the txLoop, then reads
// H2C Data PDUs inline. Sets req.payload with the collected data.
func (c *Controller) collectR2TData(req *Request) error {
	sub := c.subsystem
	if sub == nil {
		// Let handleWrite deal with the nil subsystem.
		return nil
	}

	nlb := req.capsule.LbaLength()
	expectedBytes := uint32(nlb) * sub.Dev.BlockSize()

	// Send R2T through txLoop and wait for it to be flushed.
	r2tDone := make(chan struct{})
	select {
	case c.respCh <- &response{
		r2t: &R2THeader{
			CCCID: req.capsule.CID,
			TAG:   0,
			DATAO: 0,
			DATAL: expectedBytes,
		},
		r2tDone: r2tDone,
	}:
	case <-c.done:
		return nil
	}

	select {
	case <-r2tDone:
	case <-c.done:
		return nil
	}

	// Read H2C Data PDUs from the wire.
	data, err := c.recvH2CData(expectedBytes)
	if err != nil {
		return err
	}
	req.payload = data
	return nil
}

// dispatchAdmin handles admin queue commands synchronously in the rxLoop.
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
		c.handleIdentify(req)
	case adminSetFeatures:
		c.handleSetFeatures(req)
	case adminGetFeatures:
		c.handleGetFeatures(req)
	case adminGetLogPage:
		c.handleGetLogPage(req)
	case adminKeepAlive:
		c.handleKeepAlive(req)
	case adminAsyncEvent:
		c.enqueueResponse(&response{resp: req.resp})
	default:
		req.resp.Status = uint16(StatusInvalidOpcode)
		c.enqueueResponse(&response{resp: req.resp})
	}
	return nil
}

// dispatchIO handles IO queue commands (runs in a goroutine).
func (c *Controller) dispatchIO(req *Request) {
	defer func() {
		if req.payload != nil {
			putBuffer(req.payload)
			req.payload = nil
		}
	}()
	capsule := &req.capsule

	switch capsule.OpCode {
	case ioRead:
		c.handleRead(req)
	case ioWrite:
		c.handleWrite(req)
	case ioFlush:
		c.handleFlush(req)
	case ioWriteZeros:
		c.handleWriteZeros(req)
	default:
		req.resp.Status = uint16(StatusInvalidOpcode)
		c.enqueueResponse(&response{resp: req.resp})
	}
}

// completeWaiters closes any synchronization channels on a discarded response.
// Must be called when txLoop drops a response after a write error, so that
// senders blocked on r2tDone (e.g. collectR2TData) are unblocked.
func completeWaiters(resp *response) {
	if resp.r2tDone != nil {
		close(resp.r2tDone)
	}
}

// enqueueResponse sends a response to the txLoop for writing.
// Safe to call from any goroutine. If the connection is shutting down,
// the response is silently discarded.
func (c *Controller) enqueueResponse(resp *response) {
	select {
	case c.respCh <- resp:
	case <-c.done:
	}
}

// writeResponse writes a single response to the wire (txLoop only).
func (c *Controller) writeResponse(resp *response) error {
	// R2T variant: write R2T header and signal caller.
	if resp.r2t != nil {
		err := c.out.SendHeaderOnly(pduR2T, resp.r2t, r2tHdrSize)
		if resp.r2tDone != nil {
			close(resp.r2tDone)
		}
		return err
	}

	// Standard response: set SQHD from latest value.
	if c.flowCtlOff {
		resp.resp.SQHD = 0xFFFF
	} else {
		resp.resp.SQHD = uint16(c.sqhdVal.Load())
	}
	c.resetKATO()

	if len(resp.c2hData) > 0 {
		return c.writeC2HAndResp(resp)
	}
	return c.out.SendHeaderOnly(pduCapsuleResp, &resp.resp, capsuleRespSize)
}

// writeC2HAndResp writes C2H data chunks followed by CapsuleResp, batched.
func (c *Controller) writeC2HAndResp(resp *response) error {
	data := resp.c2hData
	offset := uint32(0)
	chunkSize := c.maxDataLen

	for offset < uint32(len(data)) {
		end := offset + chunkSize
		if end > uint32(len(data)) {
			end = uint32(len(data))
		}
		chunk := data[offset:end]

		hdr := C2HDataHeader{
			CCCID: resp.resp.CID,
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

	if err := c.out.writeHeaderAndData(pduCapsuleResp, 0, &resp.resp, capsuleRespSize, nil); err != nil {
		return err
	}
	return c.out.FlushBuf()
}

// ---------- R2T / H2C Data ----------

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

		// Release pooled buffers from interleaved capsules that were never dispatched.
		for _, req := range c.pendingCapsules {
			if req.payload != nil {
				putBuffer(req.payload)
				req.payload = nil
			}
		}
		c.pendingCapsules = nil

		if c.server != nil {
			if c.isAdmin && c.cntlID != 0 {
				c.server.unregisterAdmin(c.cntlID)
			}
			c.server.removeSession(c)
		}
	})
}
