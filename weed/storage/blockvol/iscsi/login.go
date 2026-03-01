package iscsi

import (
	"errors"
	"strconv"
	"time"
)

// Login status classes (RFC 7143, Section 11.13.5)
const (
	LoginStatusSuccess       uint8 = 0x00
	LoginStatusRedirect      uint8 = 0x01
	LoginStatusInitiatorErr  uint8 = 0x02
	LoginStatusTargetErr     uint8 = 0x03
)

// Login status details
const (
	LoginDetailSuccess           uint8 = 0x00
	LoginDetailTargetMoved       uint8 = 0x01 // redirect: permanently moved
	LoginDetailTargetMovedTemp   uint8 = 0x02 // redirect: temporarily moved
	LoginDetailInitiatorError    uint8 = 0x00 // initiator error (miscellaneous)
	LoginDetailAuthFailure       uint8 = 0x01 // authentication failure
	LoginDetailAuthorizationFail uint8 = 0x02 // authorization failure
	LoginDetailNotFound          uint8 = 0x03 // target not found
	LoginDetailTargetRemoved     uint8 = 0x04 // target removed
	LoginDetailUnsupported       uint8 = 0x05 // unsupported version
	LoginDetailTooManyConns      uint8 = 0x06 // too many connections
	LoginDetailMissingParam      uint8 = 0x07 // missing parameter
	LoginDetailNoSessionSlot     uint8 = 0x08 // no session slot
	LoginDetailNoTCPConn         uint8 = 0x09 // no TCP connection available
	LoginDetailNoSession         uint8 = 0x0a // no existing session
	LoginDetailTargetError       uint8 = 0x00 // target error (miscellaneous)
	LoginDetailServiceUnavail    uint8 = 0x01 // service unavailable
	LoginDetailOutOfResources    uint8 = 0x02 // out of resources
)

var (
	ErrLoginInvalidStage    = errors.New("iscsi: invalid login stage transition")
	ErrLoginMissingParam    = errors.New("iscsi: missing required login parameter")
	ErrLoginInvalidISID     = errors.New("iscsi: invalid ISID")
	ErrLoginTargetNotFound  = errors.New("iscsi: target not found")
	ErrLoginSessionExists   = errors.New("iscsi: session already exists for this ISID")
	ErrLoginInvalidRequest  = errors.New("iscsi: invalid login request")
)

// LoginPhase tracks the current phase of login negotiation.
type LoginPhase int

const (
	LoginPhaseStart    LoginPhase = iota // before first PDU
	LoginPhaseSecurity                   // CSG=SecurityNeg
	LoginPhaseOperational                // CSG=LoginOp
	LoginPhaseDone                       // transition to FFP complete
)

// TargetConfig holds the target-side negotiation defaults.
type TargetConfig struct {
	TargetName              string
	TargetAlias             string
	MaxRecvDataSegmentLength int
	MaxBurstLength          int
	FirstBurstLength        int
	MaxConnections          int
	MaxOutstandingR2T       int
	DefaultTime2Wait        int
	DefaultTime2Retain      int
	DataPDUInOrder          bool
	DataSequenceInOrder     bool
	InitialR2T              bool
	ImmediateData           bool
	ErrorRecoveryLevel      int
	DataOutTimeout          time.Duration // read deadline for Data-Out collection (default 30s)
}

// DefaultTargetConfig returns sensible defaults for a target.
func DefaultTargetConfig() TargetConfig {
	return TargetConfig{
		MaxRecvDataSegmentLength: 262144, // 256KB
		MaxBurstLength:           262144,
		FirstBurstLength:         65536,
		MaxConnections:           1,
		MaxOutstandingR2T:        1,
		DefaultTime2Wait:         2,
		DefaultTime2Retain:       0,
		DataPDUInOrder:           true,
		DataSequenceInOrder:      true,
		InitialR2T:               true,
		ImmediateData:            true,
		ErrorRecoveryLevel:       0,
		DataOutTimeout:           30 * time.Second,
	}
}

// LoginNegotiator handles the target side of login negotiation.
type LoginNegotiator struct {
	config   TargetConfig
	phase    LoginPhase
	isid     [6]byte
	tsih     uint16
	targetOK bool // target name validated

	// Negotiated values (updated during negotiation)
	NegMaxRecvDataSegLen int
	NegMaxBurstLength    int
	NegFirstBurstLength  int
	NegInitialR2T        bool
	NegImmediateData     bool

	// Initiator/target info captured during login
	InitiatorName string
	TargetName    string
	SessionType   string // "Normal" or "Discovery"
}

// NewLoginNegotiator creates a negotiator for a new login sequence.
func NewLoginNegotiator(config TargetConfig) *LoginNegotiator {
	return &LoginNegotiator{
		config:               config,
		phase:                LoginPhaseStart,
		NegMaxRecvDataSegLen: config.MaxRecvDataSegmentLength,
		NegMaxBurstLength:    config.MaxBurstLength,
		NegFirstBurstLength:  config.FirstBurstLength,
		NegInitialR2T:        config.InitialR2T,
		NegImmediateData:     config.ImmediateData,
	}
}

// HandleLoginPDU processes one login request PDU and returns the response PDU.
// It manages stage transitions and parameter negotiation.
func (ln *LoginNegotiator) HandleLoginPDU(req *PDU, resolver TargetResolver) *PDU {
	resp := &PDU{}
	resp.SetOpcode(OpLoginResp)
	resp.SetInitiatorTaskTag(req.InitiatorTaskTag())
	resp.SetISID(req.ISID())
	resp.SetTSIH(req.TSIH())

	// Validate opcode
	if req.Opcode() != OpLoginReq {
		setLoginReject(resp, LoginStatusInitiatorErr, LoginDetailInitiatorError)
		return resp
	}

	csg := req.LoginCSG()
	nsg := req.LoginNSG()
	transit := req.LoginTransit()

	// Parse text parameters from data segment
	params, err := ParseParams(req.DataSegment)
	if err != nil {
		setLoginReject(resp, LoginStatusInitiatorErr, LoginDetailInitiatorError)
		return resp
	}

	// Process based on current stage
	respParams := NewParams()

	switch csg {
	case StageSecurityNeg:
		if ln.phase != LoginPhaseStart && ln.phase != LoginPhaseSecurity {
			setLoginReject(resp, LoginStatusInitiatorErr, LoginDetailInitiatorError)
			return resp
		}
		ln.phase = LoginPhaseSecurity

		// Capture initiator name
		if name, ok := params.Get("InitiatorName"); ok {
			ln.InitiatorName = name
		} else if ln.InitiatorName == "" {
			setLoginReject(resp, LoginStatusInitiatorErr, LoginDetailMissingParam)
			return resp
		}

		// Session type
		if st, ok := params.Get("SessionType"); ok {
			ln.SessionType = st
		}
		if ln.SessionType == "" {
			ln.SessionType = "Normal"
		}

		// Target name (required for Normal sessions)
		if tn, ok := params.Get("TargetName"); ok {
			if ln.SessionType == "Normal" {
				if resolver == nil || !resolver.HasTarget(tn) {
					setLoginReject(resp, LoginStatusInitiatorErr, LoginDetailNotFound)
					return resp
				}
			}
			ln.TargetName = tn
			ln.targetOK = true
		} else if ln.SessionType == "Normal" && !ln.targetOK {
			setLoginReject(resp, LoginStatusInitiatorErr, LoginDetailMissingParam)
			return resp
		}

		// ISID
		ln.isid = req.ISID()

		// We don't implement CHAP — declare AuthMethod=None
		respParams.Set("AuthMethod", "None")

		if transit {
			if nsg == StageLoginOp {
				ln.phase = LoginPhaseOperational
			} else if nsg == StageFullFeature {
				ln.phase = LoginPhaseDone
			}
		}

	case StageLoginOp:
		if ln.phase != LoginPhaseOperational && ln.phase != LoginPhaseSecurity {
			// Allow direct jump to LoginOp if security was skipped
			if ln.phase == LoginPhaseStart {
				// Need InitiatorName at minimum
				if name, ok := params.Get("InitiatorName"); ok {
					ln.InitiatorName = name
				} else if ln.InitiatorName == "" {
					setLoginReject(resp, LoginStatusInitiatorErr, LoginDetailMissingParam)
					return resp
				}
				// Session type
				if st, ok := params.Get("SessionType"); ok {
					ln.SessionType = st
				}
				if ln.SessionType == "" {
					ln.SessionType = "Normal"
				}
				// Target name (required for Normal sessions)
				if tn, ok := params.Get("TargetName"); ok {
					if ln.SessionType == "Normal" {
						if resolver == nil || !resolver.HasTarget(tn) {
							setLoginReject(resp, LoginStatusInitiatorErr, LoginDetailNotFound)
							return resp
						}
					}
					ln.TargetName = tn
					ln.targetOK = true
				} else if ln.SessionType == "Normal" && !ln.targetOK {
					setLoginReject(resp, LoginStatusInitiatorErr, LoginDetailMissingParam)
					return resp
				}
			} else {
				setLoginReject(resp, LoginStatusInitiatorErr, LoginDetailInitiatorError)
				return resp
			}
		}
		ln.phase = LoginPhaseOperational

		// Negotiate operational parameters
		ln.negotiateParams(params, respParams)

		if transit && nsg == StageFullFeature {
			ln.phase = LoginPhaseDone
		}

	default:
		setLoginReject(resp, LoginStatusInitiatorErr, LoginDetailInitiatorError)
		return resp
	}

	// Build response
	resp.SetLoginStages(csg, nsg)
	if transit {
		resp.SetLoginTransit(true)
	}
	resp.SetLoginStatus(LoginStatusSuccess, LoginDetailSuccess)

	// Assign TSIH on first successful login
	if ln.tsih == 0 {
		ln.tsih = 1 // simplified: single session
	}
	resp.SetTSIH(ln.tsih)

	// Encode response params
	if respParams.Len() > 0 {
		resp.DataSegment = respParams.Encode()
	}

	return resp
}

// Done returns true if login negotiation is complete.
func (ln *LoginNegotiator) Done() bool {
	return ln.phase == LoginPhaseDone
}

// Phase returns the current login phase.
func (ln *LoginNegotiator) Phase() LoginPhase {
	return ln.phase
}

// negotiateParams processes operational parameter negotiation.
func (ln *LoginNegotiator) negotiateParams(req *Params, resp *Params) {
	req.Each(func(key, value string) {
		switch key {
		case "MaxRecvDataSegmentLength":
			if v, err := NegotiateNumber(value, ln.config.MaxRecvDataSegmentLength, 512, 16777215); err == nil {
				ln.NegMaxRecvDataSegLen = v
				resp.Set(key, strconv.Itoa(ln.config.MaxRecvDataSegmentLength))
			}
		case "MaxBurstLength":
			if v, err := NegotiateNumber(value, ln.config.MaxBurstLength, 512, 16777215); err == nil {
				ln.NegMaxBurstLength = v
				resp.Set(key, strconv.Itoa(v))
			}
		case "FirstBurstLength":
			if v, err := NegotiateNumber(value, ln.config.FirstBurstLength, 512, 16777215); err == nil {
				ln.NegFirstBurstLength = v
				resp.Set(key, strconv.Itoa(v))
			}
		case "InitialR2T":
			if v, err := NegotiateBool(value, ln.config.InitialR2T); err == nil {
				// InitialR2T uses OR semantics: result is Yes if either side says Yes
				if value == "Yes" || ln.config.InitialR2T {
					ln.NegInitialR2T = true
				} else {
					ln.NegInitialR2T = v
				}
				resp.Set(key, BoolStr(ln.NegInitialR2T))
			}
		case "ImmediateData":
			if v, err := NegotiateBool(value, ln.config.ImmediateData); err == nil {
				ln.NegImmediateData = v
				resp.Set(key, BoolStr(v))
			}
		case "MaxConnections":
			resp.Set(key, strconv.Itoa(ln.config.MaxConnections))
		case "DataPDUInOrder":
			resp.Set(key, BoolStr(ln.config.DataPDUInOrder))
		case "DataSequenceInOrder":
			resp.Set(key, BoolStr(ln.config.DataSequenceInOrder))
		case "DefaultTime2Wait":
			resp.Set(key, strconv.Itoa(ln.config.DefaultTime2Wait))
		case "DefaultTime2Retain":
			resp.Set(key, strconv.Itoa(ln.config.DefaultTime2Retain))
		case "MaxOutstandingR2T":
			resp.Set(key, strconv.Itoa(ln.config.MaxOutstandingR2T))
		case "ErrorRecoveryLevel":
			resp.Set(key, strconv.Itoa(ln.config.ErrorRecoveryLevel))
		case "HeaderDigest":
			resp.Set(key, "None")
		case "DataDigest":
			resp.Set(key, "None")
		case "TargetName", "InitiatorName", "SessionType", "AuthMethod":
			// Already handled or declarative -- skip
		case "TargetAlias", "InitiatorAlias":
			// Informational -- skip
		default:
			// Unknown keys: respond with NotUnderstood
			resp.Set(key, "NotUnderstood")
		}
	})

	// Always declare our TargetAlias if configured
	if ln.config.TargetAlias != "" {
		resp.Set("TargetAlias", ln.config.TargetAlias)
	}
}

// TargetResolver allows the login state machine to check if a target exists.
type TargetResolver interface {
	HasTarget(name string) bool
}

func setLoginReject(resp *PDU, class, detail uint8) {
	resp.SetLoginStatus(class, detail)
	resp.SetLoginTransit(false)
}

// LoginResult contains the outcome of a completed login negotiation.
type LoginResult struct {
	InitiatorName        string
	TargetName           string
	SessionType          string
	ISID                 [6]byte
	TSIH                 uint16
	MaxRecvDataSegLen    int
	MaxBurstLength       int
	FirstBurstLength     int
	InitialR2T           bool
	ImmediateData        bool
}

// Result returns the negotiation outcome. Only valid after Done() returns true.
func (ln *LoginNegotiator) Result() LoginResult {
	return LoginResult{
		InitiatorName:     ln.InitiatorName,
		TargetName:        ln.TargetName,
		SessionType:       ln.SessionType,
		ISID:              ln.isid,
		TSIH:              ln.tsih,
		MaxRecvDataSegLen: ln.NegMaxRecvDataSegLen,
		MaxBurstLength:    ln.NegMaxBurstLength,
		FirstBurstLength:  ln.NegFirstBurstLength,
		InitialR2T:        ln.NegInitialR2T,
		ImmediateData:     ln.NegImmediateData,
	}
}

// BuildRedirectResponse creates a login response that redirects the initiator
// to a different target address.
func BuildRedirectResponse(req *PDU, addr string, permanent bool) *PDU {
	resp := &PDU{}
	resp.SetOpcode(OpLoginResp)
	resp.SetInitiatorTaskTag(req.InitiatorTaskTag())
	resp.SetISID(req.ISID())

	detail := LoginDetailTargetMovedTemp
	if permanent {
		detail = LoginDetailTargetMoved
	}
	resp.SetLoginStatus(LoginStatusRedirect, detail)

	params := NewParams()
	params.Set("TargetAddress", addr)
	resp.DataSegment = params.Encode()

	return resp
}
