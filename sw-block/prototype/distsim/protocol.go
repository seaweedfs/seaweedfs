package distsim

type ProtocolVersion string

const (
	ProtocolV1  ProtocolVersion = "v1"
	ProtocolV15 ProtocolVersion = "v1_5"
	ProtocolV2  ProtocolVersion = "v2"
)

type ProtocolPolicy struct {
	Version ProtocolVersion
}

func (p ProtocolPolicy) CanAttemptCatchup(addressStable bool) bool {
	switch p.Version {
	case ProtocolV1:
		return false
	case ProtocolV15:
		return addressStable
	case ProtocolV2:
		return true
	default:
		return false
	}
}

func (p ProtocolPolicy) BriefDisconnectAction(addressStable, recoverable bool) string {
	switch p.Version {
	case ProtocolV1:
		return "degrade_or_rebuild"
	case ProtocolV15:
		if addressStable && recoverable {
			return "catchup_if_history_survives"
		}
		return "stall_or_control_plane_recovery"
	case ProtocolV2:
		if recoverable {
			return "reserved_catchup"
		}
		return "explicit_rebuild"
	default:
		return "unknown"
	}
}

func (p ProtocolPolicy) TailChasingAction(converged bool) string {
	switch p.Version {
	case ProtocolV1:
		if converged {
			return "unexpected_catchup"
		}
		return "degrade"
	case ProtocolV15:
		if converged {
			return "catchup"
		}
		return "stall_or_rebuild"
	case ProtocolV2:
		if converged {
			return "catchup"
		}
		return "abort_to_rebuild"
	default:
		return "unknown"
	}
}

func (p ProtocolPolicy) RestartRejoinAction(addressStable bool) string {
	switch p.Version {
	case ProtocolV1:
		return "control_plane_only"
	case ProtocolV15:
		if addressStable {
			return "background_reconnect_or_control_plane"
		}
		return "control_plane_only"
	case ProtocolV2:
		if addressStable {
			return "direct_reconnect_or_control_plane"
		}
		return "explicit_reassignment_or_rebuild"
	default:
		return "unknown"
	}
}

func (p ProtocolPolicy) ChangedAddressRestartAction(recoverable bool) string {
	switch p.Version {
	case ProtocolV1:
		return "control_plane_only"
	case ProtocolV15:
		return "control_plane_only"
	case ProtocolV2:
		if recoverable {
			return "explicit_reassignment_then_catchup"
		}
		return "explicit_reassignment_or_rebuild"
	default:
		return "unknown"
	}
}
