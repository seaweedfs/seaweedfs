package s3lifecycle

// ActionKey is the engine-wide identity of one compiled lifecycle action.
// Bucket is part of the key because two buckets may carry rules with
// identical RuleHash; without scoping they'd collide in any keyed map.
// The on-disk path /etc/s3/lifecycle/<bucket>/<rule_hash>/<action_kind>/
// mirrors this shape.
type ActionKey struct {
	Bucket     string
	RuleHash   [8]byte
	ActionKind ActionKind
}

// ActionKind values mirror the wire-form enum in s3_lifecycle.proto.
type ActionKind int

const (
	ActionKindUnspecified ActionKind = iota
	ActionKindExpirationDays
	ActionKindExpirationDate
	ActionKindNoncurrentDays
	ActionKindNewerNoncurrent
	ActionKindAbortMPU
	ActionKindExpiredDeleteMarker
)

// String returns the leaf-directory name used in
// /etc/s3/lifecycle/<bucket>/<rule_hash>/<action_kind>/.
func (k ActionKind) String() string {
	switch k {
	case ActionKindExpirationDays:
		return "expiration_days"
	case ActionKindExpirationDate:
		return "expiration_date"
	case ActionKindNoncurrentDays:
		return "noncurrent_days"
	case ActionKindNewerNoncurrent:
		return "newer_noncurrent"
	case ActionKindAbortMPU:
		return "abort_mpu"
	case ActionKindExpiredDeleteMarker:
		return "expired_delete_marker"
	default:
		return "unspecified"
	}
}

// RuleActionKinds returns the compiled actions a single XML rule expands to,
// in deterministic order. NewerNoncurrentVersions paired with NoncurrentDays
// is subsumed into NONCURRENT_DAYS; only stand-alone NewerNoncurrent
// produces a NEWER_NONCURRENT action.
func RuleActionKinds(rule *Rule) []ActionKind {
	if rule == nil {
		return nil
	}
	var kinds []ActionKind
	if rule.ExpirationDays > 0 {
		kinds = append(kinds, ActionKindExpirationDays)
	}
	if !rule.ExpirationDate.IsZero() {
		kinds = append(kinds, ActionKindExpirationDate)
	}
	if rule.ExpiredObjectDeleteMarker {
		kinds = append(kinds, ActionKindExpiredDeleteMarker)
	}
	if rule.NoncurrentVersionExpirationDays > 0 {
		kinds = append(kinds, ActionKindNoncurrentDays)
	} else if rule.NewerNoncurrentVersions > 0 {
		kinds = append(kinds, ActionKindNewerNoncurrent)
	}
	if rule.AbortMPUDaysAfterInitiation > 0 {
		kinds = append(kinds, ActionKindAbortMPU)
	}
	return kinds
}
