package engine

import (
	"time"

	"github.com/seaweedfs/seaweedfs/weed/s3api/s3lifecycle"
)

// RuleMode mirrors the durable s3_lifecycle_pb.LifecycleState.RuleMode enum.
// Defined here as a separate Go type so engine code doesn't depend on the
// generated proto package directly. Mappings to/from the proto value are
// handled by the worker when it reads/writes the durable state file.
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

// decideMode picks the scheduling mode for one (rule, kind) compiled action.
// The decision is the same predicate the tick-time mode decision uses (and
// is also called by the safety-scan-tick code path):
//
//   - status disabled                                                         -> DISABLED
//   - kind == EXPIRATION_DATE                                                 -> SCAN_AT_DATE
//   - reader-driven kind, retention < eventLogHorizon + bootstrapLookbackMin  -> SCAN_ONLY
//   - otherwise                                                               -> EVENT_DRIVEN
//
// MetaLogRetention=0 is treated as "unbounded" — the gate never trips at
// the default SeaweedFS deployment, where the meta log isn't pruned.
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
