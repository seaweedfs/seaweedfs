package engine

import (
	"time"

	"github.com/seaweedfs/seaweedfs/weed/s3api/s3lifecycle"
)

// RuleMode mirrors the durable s3_lifecycle_pb.LifecycleState.RuleMode enum;
// the worker maps between them when it reads/writes durable state.
type RuleMode int

const (
	ModeUnspecified RuleMode = iota
	ModeEventDriven
	ModeScanAtDate
	ModeScanOnly
	ModeDisabled
	ModePendingBootstrap
)

func (m RuleMode) String() string {
	switch m {
	case ModeEventDriven:
		return "event_driven"
	case ModeScanAtDate:
		return "scan_at_date"
	case ModeScanOnly:
		return "scan_only"
	case ModeDisabled:
		return "disabled"
	case ModePendingBootstrap:
		return "pending_bootstrap"
	default:
		return "unspecified"
	}
}

// decideMode: disabled rule -> DISABLED; EXPIRATION_DATE -> SCAN_AT_DATE;
// reader-driven kind whose horizon exceeds retention -> SCAN_ONLY; else
// EVENT_DRIVEN. metaLogRetention=0 means unbounded (default), gate doesn't
// trip.
func decideMode(rule *s3lifecycle.Rule, kind s3lifecycle.ActionKind, metaLogRetention, bootstrapLookbackMin time.Duration) RuleMode {
	if rule == nil || rule.Status != s3lifecycle.StatusEnabled {
		return ModeDisabled
	}
	if kind == s3lifecycle.ActionKindExpirationDate {
		return ModeScanAtDate
	}
	if metaLogRetention > 0 {
		horizon := s3lifecycle.EventLogHorizon(rule, kind)
		if horizon > 0 && metaLogRetention < horizon+bootstrapLookbackMin {
			return ModeScanOnly
		}
	}
	return ModeEventDriven
}
