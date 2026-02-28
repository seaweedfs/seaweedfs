package iscsi

import (
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"sync"
	"sync/atomic"
)

var (
	ErrSessionClosed = errors.New("iscsi: session closed")
	ErrCmdSNOutOfWindow = errors.New("iscsi: CmdSN out of window")
)

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
	statSN   uint32        // target status sequence number

	// Login state
	negotiator *LoginNegotiator
	loginDone  bool

	// Negotiated session parameters
	negImmediateData bool
	negInitialR2T    bool

	// Data sequencing
	dataInWriter *DataInWriter

	// PDU queue for commands received during Data-Out collection.
	// The initiator pipelines commands; we may read a SCSI Command
	// while waiting for Data-Out PDUs.
	pending []*PDU

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
		logger:     logger,
	}
	s.expCmdSN.Store(1)
	s.maxCmdSN.Store(32) // window of 32 commands
	return s
}

// HandleConnection processes PDUs until the connection is closed or an error occurs.
func (s *Session) HandleConnection() error {
	defer s.close()

	for !s.closed.Load() {
		pdu, err := s.nextPDU()
		if err != nil {
			if s.closed.Load() {
				return nil
			}
			if errors.Is(err, io.EOF) || errors.Is(err, net.ErrClosed) {
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

	// Set sequence numbers
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

		// Bind SCSI handler to the device for the target the initiator logged into.
		// Discovery sessions have no TargetName — s.scsi stays nil, which is
		// checked in handleSCSICmd.
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
	s.mu.Lock()
	defer s.mu.Unlock()

	// Gather discovery targets from resolver if it supports listing
	var targets []DiscoveryTarget
	if lister, ok := s.resolver.(TargetLister); ok {
		targets = lister.ListTargets()
	}

	resp := HandleTextRequest(pdu, targets)
	resp.SetStatSN(s.statSN)
	s.statSN++
	resp.SetExpCmdSN(s.expCmdSN.Load())
	resp.SetMaxCmdSN(s.maxCmdSN.Load())

	return WritePDU(s.conn, resp)
}

func (s *Session) handleSCSICmd(pdu *PDU) error {
	if !s.loginDone {
		return s.sendReject(pdu, 0x0b) // protocol error
	}

	// Discovery sessions have no SCSI handler — reject commands
	if s.scsi == nil {
		return s.sendReject(pdu, 0x04) // command not supported
	}

	cdb := pdu.CDB()
	itt := pdu.InitiatorTaskTag()
	flags := pdu.OpSpecific1()

	// CmdSN validation for non-immediate commands (RFC 7143 section 4.2.2.1)
	if !pdu.Immediate() {
		cmdSN := pdu.CmdSN()
		expCmdSN := s.expCmdSN.Load()
		maxCmdSN := s.maxCmdSN.Load()
		// CmdSN is within window if ExpCmdSN <= CmdSN <= MaxCmdSN (serial arithmetic)
		if !cmdSNInWindow(cmdSN, expCmdSN, maxCmdSN) {
			s.logger.Printf("CmdSN %d out of window [%d, %d], dropping", cmdSN, expCmdSN, maxCmdSN)
			return nil // silently drop per RFC 7143
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

		// Immediate data — enforce negotiated ImmediateData flag
		if len(pdu.DataSegment) > 0 {
			if !s.negImmediateData {
				// ImmediateData=No but initiator sent data — reject
				return s.sendCheckCondition(itt, SenseIllegalRequest, ASCInvalidFieldInCDB, ASCQLuk)
			}
			if err := collector.AddImmediateData(pdu.DataSegment); err != nil {
				return s.sendCheckCondition(itt, SenseIllegalRequest, ASCInvalidFieldInCDB, ASCQLuk)
			}
		}

		// If more data needed, send R2T and collect Data-Out PDUs
		if !collector.Done() {
			if err := s.collectDataOut(collector, itt); err != nil {
				return err
			}
		}

		dataOut = collector.Data()
	}

	// Execute SCSI command
	result := s.scsi.HandleCommand(cdb, dataOut)

	// Send response
	s.mu.Lock()
	expCmdSN := s.expCmdSN.Load()
	maxCmdSN := s.maxCmdSN.Load()
	s.mu.Unlock()

	if isRead && result.Status == SCSIStatusGood && len(result.Data) > 0 {
		// Send Data-In PDUs
		s.mu.Lock()
		_, err := s.dataInWriter.WriteDataIn(s.conn, result.Data, itt, expCmdSN, maxCmdSN, &s.statSN)
		s.mu.Unlock()
		return err
	}

	// Send SCSI Response
	s.mu.Lock()
	err := SendSCSIResponse(s.conn, result, itt, &s.statSN, expCmdSN, maxCmdSN)
	s.mu.Unlock()
	return err
}

func (s *Session) collectDataOut(collector *DataOutCollector, itt uint32) error {
	var r2tSN uint32
	ttt := itt // use ITT as TTT for simplicity

	for !collector.Done() {
		// Send R2T
		s.mu.Lock()
		r2t := BuildR2T(itt, ttt, r2tSN, s.totalReceived(collector), collector.Remaining(),
			s.statSN, s.expCmdSN.Load(), s.maxCmdSN.Load())
		s.mu.Unlock()

		if err := WritePDU(s.conn, r2t); err != nil {
			return err
		}
		r2tSN++

		// Read Data-Out PDUs until F-bit.
		// The initiator may pipeline other commands; queue them for later.
		for {
			doPDU, err := ReadPDU(s.conn)
			if err != nil {
				return err
			}
			if doPDU.Opcode() != OpSCSIDataOut {
				// Not our Data-Out — queue for later dispatch
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
	resp := &PDU{}
	resp.SetOpcode(OpNOPIn)
	resp.SetOpSpecific1(FlagF)
	resp.SetInitiatorTaskTag(pdu.InitiatorTaskTag())
	resp.SetTargetTransferTag(0xFFFFFFFF)

	s.mu.Lock()
	resp.SetStatSN(s.statSN)
	s.statSN++
	resp.SetExpCmdSN(s.expCmdSN.Load())
	resp.SetMaxCmdSN(s.maxCmdSN.Load())
	s.mu.Unlock()

	// Echo back data if present
	if len(pdu.DataSegment) > 0 {
		resp.DataSegment = pdu.DataSegment
	}

	return WritePDU(s.conn, resp)
}

func (s *Session) handleLogout(pdu *PDU) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.state = SessionLogout

	resp := &PDU{}
	resp.SetOpcode(OpLogoutResp)
	resp.SetOpSpecific1(FlagF)
	resp.SetInitiatorTaskTag(pdu.InitiatorTaskTag())
	resp.BHS[2] = 0x00 // response: connection/session closed successfully
	resp.SetStatSN(s.statSN)
	s.statSN++
	resp.SetExpCmdSN(s.expCmdSN.Load())
	resp.SetMaxCmdSN(s.maxCmdSN.Load())

	if err := WritePDU(s.conn, resp); err != nil {
		return err
	}

	// Signal HandleConnection to exit
	s.closed.Store(true)
	s.conn.Close()
	return nil
}

func (s *Session) handleTaskMgmt(pdu *PDU) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Simplified: always respond with "function complete"
	resp := &PDU{}
	resp.SetOpcode(OpSCSITaskResp)
	resp.SetOpSpecific1(FlagF)
	resp.SetInitiatorTaskTag(pdu.InitiatorTaskTag())
	resp.BHS[2] = 0x00 // function complete
	resp.SetStatSN(s.statSN)
	s.statSN++
	resp.SetExpCmdSN(s.expCmdSN.Load())
	resp.SetMaxCmdSN(s.maxCmdSN.Load())

	return WritePDU(s.conn, resp)
}

func (s *Session) advanceCmdSN() {
	s.expCmdSN.Add(1)
	s.maxCmdSN.Add(1)
}

// cmdSNInWindow checks if cmdSN is within [expCmdSN, maxCmdSN] using
// serial number arithmetic (RFC 7143 section 4.2.2.1). Handles uint32 wrap.
func cmdSNInWindow(cmdSN, expCmdSN, maxCmdSN uint32) bool {
	// Serial comparison: a <= b means (b - a) < 2^31
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

	s.mu.Lock()
	resp.SetStatSN(s.statSN)
	s.statSN++
	resp.SetExpCmdSN(s.expCmdSN.Load())
	resp.SetMaxCmdSN(s.maxCmdSN.Load())
	s.mu.Unlock()

	// Include the rejected BHS in the data segment
	resp.DataSegment = origPDU.BHS[:]

	return WritePDU(s.conn, resp)
}

func (s *Session) sendCheckCondition(itt uint32, senseKey, asc, ascq uint8) error {
	result := SCSIResult{
		Status:    SCSIStatusCheckCond,
		SenseKey:  senseKey,
		SenseASC:  asc,
		SenseASCQ: ascq,
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return SendSCSIResponse(s.conn, result, itt, &s.statSN, s.expCmdSN.Load(), s.maxCmdSN.Load())
}

// TargetLister is an optional interface that TargetResolver can implement
// to support SendTargets discovery.
type TargetLister interface {
	ListTargets() []DiscoveryTarget
}

// DeviceLookup resolves a target IQN to a BlockDevice.
// Used after login to bind the session to the correct volume.
type DeviceLookup interface {
	LookupDevice(iqn string) BlockDevice
}

// State returns the current session state.
func (s *Session) State() SessionState {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.state
}
