package iscsi

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

var (
	ErrSessionClosed    = errors.New("iscsi: session closed")
	ErrCmdSNOutOfWindow = errors.New("iscsi: CmdSN out of window")
)

// maxPendingQueue limits the number of non-Data-Out PDUs that can be
// queued during a Data-Out collection phase. 2× the CmdSN window (32).
const maxPendingQueue = 64

// SessionState tracks the lifecycle of an iSCSI session.
type SessionState int

const (
	SessionLogin    SessionState = iota // login phase
	SessionLoggedIn                     // full feature phase
	SessionLogout                       // logout requested
	SessionClosed                       // terminated
)

// Session manages a single iSCSI session (one initiator connection).
type Session struct {
	mu sync.Mutex

	state    SessionState
	conn     net.Conn
	scsi     *SCSIHandler
	config   TargetConfig
	resolver TargetResolver
	devices  DeviceLookup

	// Sequence numbers
	expCmdSN atomic.Uint32 // expected CmdSN from initiator
	maxCmdSN atomic.Uint32 // max CmdSN we allow
	statSN   uint32        // target status sequence number (txLoop only after login)

	// Login state
	negotiator *LoginNegotiator
	loginDone  bool

	// Negotiated session parameters
	negImmediateData bool
	negInitialR2T    bool

	// Data sequencing
	dataInWriter *DataInWriter

	// PDU queue for commands received during Data-Out collection.
	pending []*PDU

	// TX goroutine channel for response PDUs.
	// Buffered; txLoop reads and writes to conn.
	respCh chan *PDU
	txDone chan struct{}

	// Shutdown
	closed   atomic.Bool
	closeErr error

	// Logging
	logger *log.Logger
}

// NewSession creates a new iSCSI session on the given connection.
func NewSession(conn net.Conn, config TargetConfig, resolver TargetResolver, devices DeviceLookup, logger *log.Logger) *Session {
	if logger == nil {
		logger = log.Default()
	}
	s := &Session{
		state:      SessionLogin,
		conn:       conn,
		config:     config,
		resolver:   resolver,
		devices:    devices,
		negotiator: NewLoginNegotiator(config),
		respCh:     make(chan *PDU, 64),
		txDone:     make(chan struct{}),
		logger:     logger,
	}
	s.expCmdSN.Store(1)
	s.maxCmdSN.Store(32) // window of 32 commands
	return s
}

// HandleConnection processes PDUs until the connection is closed or an error occurs.
// Login phase runs inline (single goroutine). After login completes, a txLoop
// goroutine is started for pipelined response writing.
func (s *Session) HandleConnection() error {
	defer s.close()

	// Login phase: handle login PDUs inline (no txLoop yet).
	if err := s.loginPhase(); err != nil {
		return err
	}

	if !s.loginDone {
		// Connection closed during login phase.
		return nil
	}

	// Start TX goroutine for full-feature phase.
	go s.txLoop()

	// RX loop: read PDUs and dispatch serially.
	err := s.rxLoop()

	// Shutdown: close respCh so txLoop exits, then wait for it.
	close(s.respCh)
	<-s.txDone

	return err
}

// loginPhase handles login PDUs inline until login is complete or connection closes.
func (s *Session) loginPhase() error {
	for !s.closed.Load() && !s.loginDone {
		pdu, err := ReadPDU(s.conn)
		if err != nil {
			if s.closed.Load() || errors.Is(err, io.EOF) || errors.Is(err, net.ErrClosed) {
				return nil
			}
			return fmt.Errorf("read PDU: %w", err)
		}
		if err := s.dispatch(pdu); err != nil {
			if s.closed.Load() {
				return nil
			}
			return fmt.Errorf("dispatch %s: %w", OpcodeName(pdu.Opcode()), err)
		}
	}
	return nil
}

// rxLoop reads PDUs from the connection and dispatches them serially.
// Response PDUs are enqueued on respCh by handlers.
func (s *Session) rxLoop() error {
	for !s.closed.Load() {
		pdu, err := s.nextPDU()
		if err != nil {
			if s.closed.Load() || errors.Is(err, io.EOF) || errors.Is(err, net.ErrClosed) {
				return nil
			}
			return fmt.Errorf("read PDU: %w", err)
		}
		if err := s.dispatch(pdu); err != nil {
			if s.closed.Load() {
				return nil
			}
			return fmt.Errorf("dispatch %s: %w", OpcodeName(pdu.Opcode()), err)
		}
	}
	return nil
}

// txLoop reads response PDUs from respCh, assigns StatSN, and writes to conn.
// Runs as a goroutine during full-feature phase.
func (s *Session) txLoop() {
	defer close(s.txDone)
	for pdu := range s.respCh {
		if pdu == nil {
			continue
		}
		// Assign StatSN based on PDU type.
		switch s.pduStatSNMode(pdu) {
		case statSNAssign:
			s.mu.Lock()
			pdu.SetStatSN(s.statSN)
			s.statSN++
			pdu.SetExpCmdSN(s.expCmdSN.Load())
			pdu.SetMaxCmdSN(s.maxCmdSN.Load())
			s.mu.Unlock()
		case statSNCopy:
			s.mu.Lock()
			pdu.SetStatSN(s.statSN)
			pdu.SetExpCmdSN(s.expCmdSN.Load())
			pdu.SetMaxCmdSN(s.maxCmdSN.Load())
			s.mu.Unlock()
		}
		if err := WritePDU(s.conn, pdu); err != nil {
			if !s.closed.Load() {
				s.logger.Printf("txLoop write error: %v", err)
			}
			// Close connection so rxLoop exits, which lets HandleConnection
			// close respCh, which lets the drain loop below finish.
			s.closed.Store(true)
			s.conn.Close()
			for range s.respCh {
			}
			return
		}
	}
}

// statSNMode controls how txLoop handles StatSN for a PDU.
type statSNMode int

const (
	statSNNone   statSNMode = iota // don't touch (intermediate Data-In)
	statSNAssign                   // assign current StatSN, increment
	statSNCopy                     // assign current StatSN, do NOT increment (R2T)
)

// pduStatSNMode returns the StatSN handling mode for the given PDU.
func (s *Session) pduStatSNMode(pdu *PDU) statSNMode {
	op := pdu.Opcode()
	switch op {
	case OpSCSIDataIn:
		// Only the final Data-In PDU (with S-bit) gets StatSN.
		if pdu.OpSpecific1()&FlagS != 0 {
			return statSNAssign
		}
		return statSNNone
	case OpR2T:
		// R2T carries StatSN but does NOT increment it (RFC 7143).
		return statSNCopy
	default:
		return statSNAssign
	}
}

// enqueue sends a response PDU to the TX goroutine.
func (s *Session) enqueue(pdu *PDU) {
	select {
	case s.respCh <- pdu:
	case <-s.txDone:
		// TX loop exited, discard response.
	}
}

// nextPDU returns the next PDU to process, draining the pending queue
// (populated during Data-Out collection) before reading from the connection.
func (s *Session) nextPDU() (*PDU, error) {
	if len(s.pending) > 0 {
		pdu := s.pending[0]
		s.pending = s.pending[1:]
		return pdu, nil
	}
	return ReadPDU(s.conn)
}

// Close terminates the session.
func (s *Session) Close() error {
	s.closed.Store(true)
	return s.conn.Close()
}

func (s *Session) close() {
	s.closed.Store(true)
	s.mu.Lock()
	s.state = SessionClosed
	s.mu.Unlock()
	s.conn.Close()
}

func (s *Session) dispatch(pdu *PDU) error {
	op := pdu.Opcode()

	switch op {
	case OpLoginReq:
		return s.handleLogin(pdu)
	case OpTextReq:
		return s.handleText(pdu)
	case OpSCSICmd:
		return s.handleSCSICmd(pdu)
	case OpSCSIDataOut:
		// Handled inline during write command processing
		return nil
	case OpNOPOut:
		return s.handleNOPOut(pdu)
	case OpLogoutReq:
		return s.handleLogout(pdu)
	case OpSCSITaskMgmt:
		return s.handleTaskMgmt(pdu)
	default:
		s.logger.Printf("unhandled opcode: %s", OpcodeName(op))
		return s.sendReject(pdu, 0x04) // command not supported
	}
}

func (s *Session) handleLogin(pdu *PDU) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	resp := s.negotiator.HandleLoginPDU(pdu, s.resolver)

	// During login phase, StatSN is assigned inline (no txLoop yet).
	resp.SetStatSN(s.statSN)
	s.statSN++
	resp.SetExpCmdSN(s.expCmdSN.Load())
	resp.SetMaxCmdSN(s.maxCmdSN.Load())

	if err := WritePDU(s.conn, resp); err != nil {
		return err
	}

	if s.negotiator.Done() {
		s.loginDone = true
		s.state = SessionLoggedIn
		result := s.negotiator.Result()
		s.dataInWriter = NewDataInWriter(uint32(result.MaxRecvDataSegLen))
		s.negImmediateData = result.ImmediateData
		s.negInitialR2T = result.InitialR2T

		if s.devices != nil && result.TargetName != "" {
			dev := s.devices.LookupDevice(result.TargetName)
			s.scsi = NewSCSIHandler(dev)
		}

		s.logger.Printf("login complete: initiator=%s target=%s session=%s",
			result.InitiatorName, result.TargetName, result.SessionType)
	}

	return nil
}

func (s *Session) handleText(pdu *PDU) error {
	if !s.loginDone {
		return s.sendReject(pdu, 0x0b) // protocol error
	}

	var targets []DiscoveryTarget
	if lister, ok := s.resolver.(TargetLister); ok {
		targets = lister.ListTargets()
	}

	resp := HandleTextRequest(pdu, targets)
	// ExpCmdSN/MaxCmdSN are set by txLoop via pduNeedsStatSN.
	s.enqueue(resp)
	return nil
}

func (s *Session) handleSCSICmd(pdu *PDU) error {
	if !s.loginDone {
		return s.sendReject(pdu, 0x0b) // protocol error
	}

	if s.scsi == nil {
		return s.sendReject(pdu, 0x04) // command not supported
	}

	cdb := pdu.CDB()
	itt := pdu.InitiatorTaskTag()
	flags := pdu.OpSpecific1()

	// CmdSN validation for non-immediate commands
	if !pdu.Immediate() {
		cmdSN := pdu.CmdSN()
		expCmdSN := s.expCmdSN.Load()
		maxCmdSN := s.maxCmdSN.Load()
		if !cmdSNInWindow(cmdSN, expCmdSN, maxCmdSN) {
			s.logger.Printf("CmdSN %d out of window [%d, %d], dropping", cmdSN, expCmdSN, maxCmdSN)
			return nil
		}
		s.advanceCmdSN()
	}

	isWrite := flags&FlagW != 0
	isRead := flags&FlagR != 0
	expectedLen := pdu.ExpectedDataTransferLength()

	// Handle write commands — collect data
	var dataOut []byte
	if isWrite && expectedLen > 0 {
		collector := NewDataOutCollector(expectedLen)

		if len(pdu.DataSegment) > 0 {
			if !s.negImmediateData {
				return s.sendCheckCondition(itt, SenseIllegalRequest, ASCInvalidFieldInCDB, ASCQLuk)
			}
			if err := collector.AddImmediateData(pdu.DataSegment); err != nil {
				return s.sendCheckCondition(itt, SenseIllegalRequest, ASCInvalidFieldInCDB, ASCQLuk)
			}
		}

		if !collector.Done() {
			if err := s.collectDataOut(collector, itt); err != nil {
				return err
			}
		}

		dataOut = collector.Data()
	}

	// Execute SCSI command
	result := s.scsi.HandleCommand(cdb, dataOut)

	if isRead && result.Status == SCSIStatusGood && len(result.Data) > 0 {
		// Build Data-In PDUs and enqueue them all.
		pdus := s.dataInWriter.BuildDataInPDUs(result.Data, itt, s.expCmdSN.Load(), s.maxCmdSN.Load())
		for _, p := range pdus {
			s.enqueue(p)
		}
		return nil
	}

	// Build SCSI Response PDU and enqueue.
	resp := BuildSCSIResponse(result, itt, s.expCmdSN.Load(), s.maxCmdSN.Load())
	s.enqueue(resp)
	return nil
}

func (s *Session) collectDataOut(collector *DataOutCollector, itt uint32) error {
	var r2tSN uint32
	ttt := itt

	// Clear any read deadline on exit (success or error).
	defer s.conn.SetReadDeadline(time.Time{})

	for !collector.Done() {
		// Build R2T and enqueue (txLoop assigns StatSN before writing).
		r2t := BuildR2T(itt, ttt, r2tSN, s.totalReceived(collector), collector.Remaining(),
			s.expCmdSN.Load(), s.maxCmdSN.Load())

		s.enqueue(r2t)
		r2tSN++

		// Set read deadline for Data-Out collection.
		if s.config.DataOutTimeout > 0 {
			s.conn.SetReadDeadline(time.Now().Add(s.config.DataOutTimeout))
		}

		// Read Data-Out PDUs until F-bit.
		for {
			doPDU, err := ReadPDU(s.conn)
			if err != nil {
				return err
			}
			if doPDU.Opcode() != OpSCSIDataOut {
				if len(s.pending) >= maxPendingQueue {
					return fmt.Errorf("pending queue overflow (%d PDUs)", maxPendingQueue)
				}
				s.pending = append(s.pending, doPDU)
				continue
			}
			if err := collector.AddDataOut(doPDU); err != nil {
				return err
			}
			if doPDU.OpSpecific1()&FlagF != 0 {
				break
			}
		}
	}
	return nil
}

func (s *Session) totalReceived(c *DataOutCollector) uint32 {
	return c.expectedLen - c.Remaining()
}

func (s *Session) handleNOPOut(pdu *PDU) error {
	if !s.loginDone {
		return s.sendReject(pdu, 0x0b) // protocol error
	}

	resp := &PDU{}
	resp.SetOpcode(OpNOPIn)
	resp.SetOpSpecific1(FlagF)
	resp.SetInitiatorTaskTag(pdu.InitiatorTaskTag())
	resp.SetTargetTransferTag(0xFFFFFFFF)

	if len(pdu.DataSegment) > 0 {
		resp.DataSegment = pdu.DataSegment
	}

	s.enqueue(resp)
	return nil
}

func (s *Session) handleLogout(pdu *PDU) error {
	if !s.loginDone {
		return s.sendReject(pdu, 0x0b) // protocol error
	}

	resp := &PDU{}
	resp.SetOpcode(OpLogoutResp)
	resp.SetOpSpecific1(FlagF)
	resp.SetInitiatorTaskTag(pdu.InitiatorTaskTag())
	resp.BHS[2] = 0x00 // response: connection/session closed successfully

	s.enqueue(resp)

	// Give txLoop a moment to write the response before closing.
	// We signal close after enqueue so the logout response is sent.
	s.mu.Lock()
	s.state = SessionLogout
	s.mu.Unlock()
	s.closed.Store(true)
	return nil
}

func (s *Session) handleTaskMgmt(pdu *PDU) error {
	if !s.loginDone {
		return s.sendReject(pdu, 0x0b) // protocol error
	}

	resp := &PDU{}
	resp.SetOpcode(OpSCSITaskResp)
	resp.SetOpSpecific1(FlagF)
	resp.SetInitiatorTaskTag(pdu.InitiatorTaskTag())
	resp.BHS[2] = 0x00 // function complete

	s.enqueue(resp)
	return nil
}

func (s *Session) advanceCmdSN() {
	s.expCmdSN.Add(1)
	s.maxCmdSN.Add(1)
}

// cmdSNInWindow checks if cmdSN is within [expCmdSN, maxCmdSN] using
// serial number arithmetic (RFC 7143 section 4.2.2.1). Handles uint32 wrap.
func cmdSNInWindow(cmdSN, expCmdSN, maxCmdSN uint32) bool {
	return serialLE(expCmdSN, cmdSN) && serialLE(cmdSN, maxCmdSN)
}

func serialLE(a, b uint32) bool {
	return a == b || int32(b-a) > 0
}

func (s *Session) sendReject(origPDU *PDU, reason uint8) error {
	resp := &PDU{}
	resp.SetOpcode(OpReject)
	resp.SetOpSpecific1(FlagF)
	resp.BHS[2] = reason
	resp.SetInitiatorTaskTag(0xFFFFFFFF)
	resp.DataSegment = origPDU.BHS[:]

	if s.loginDone {
		s.enqueue(resp)
	} else {
		// During login phase, write inline (no txLoop).
		s.mu.Lock()
		resp.SetStatSN(s.statSN)
		s.statSN++
		resp.SetExpCmdSN(s.expCmdSN.Load())
		resp.SetMaxCmdSN(s.maxCmdSN.Load())
		s.mu.Unlock()
		return WritePDU(s.conn, resp)
	}
	return nil
}

func (s *Session) sendCheckCondition(itt uint32, senseKey, asc, ascq uint8) error {
	result := SCSIResult{
		Status:    SCSIStatusCheckCond,
		SenseKey:  senseKey,
		SenseASC:  asc,
		SenseASCQ: ascq,
	}
	resp := BuildSCSIResponse(result, itt, s.expCmdSN.Load(), s.maxCmdSN.Load())
	s.enqueue(resp)
	return nil
}

// TargetLister is an optional interface that TargetResolver can implement
// to support SendTargets discovery.
type TargetLister interface {
	ListTargets() []DiscoveryTarget
}

// DeviceLookup resolves a target IQN to a BlockDevice.
type DeviceLookup interface {
	LookupDevice(iqn string) BlockDevice
}

// State returns the current session state.
func (s *Session) State() SessionState {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.state
}
