package s3lifecycle

// ActionKey is the engine-wide identity of one compiled lifecycle action.
// One XML <Rule> with N populated action sub-elements expands into N
// ActionKeys sharing the same RuleHash but differing in ActionKind. Every
// per-action data structure — engine indexes, target modes, newly-completed
// sets, bootstrap completion, drain locks, metrics, blocker / retry-budget
// records — is keyed by ActionKey, not RuleHash alone, so sibling actions
// of the same rule are scheduled and degraded independently.
//
// Bucket is part of the key because two buckets may carry rules whose XML
// (and therefore RuleHash) is identical; without scoping, those collide in
// any map keyed by ActionKey. The on-disk storage layout
// /etc/s3/lifecycle/<bucket>/<rule_hash>/<action_kind>/ is naturally
// bucket-scoped; ActionKey mirrors that.
type ActionKey struct {
	Bucket     string
	RuleHash   [8]byte
	ActionKind ActionKind
}

// ActionKind identifies a single compiled lifecycle action under one XML
// rule. A single XML <Rule> may declare multiple action sub-elements in
// parallel, each yielding a separate compiled action with its own delay
// group, mode, pending stream, and durable state directory.
//
// The values here mirror the wire-form ActionKind enum in
// weed/pb/s3_lifecycle.proto (offset by the UNSPECIFIED sentinel at 0).
type ActionKind int

const (
	ActionKindUnspecified         ActionKind = iota // matches proto ACTION_KIND_UNSPECIFIED
	ActionKindExpirationDays                        // Expiration.Days
	ActionKindExpirationDate                        // Expiration.Date
	ActionKindNoncurrentDays                        // NoncurrentVersionExpiration.NoncurrentDays (with optional NewerNoncurrent retention)
	ActionKindNewerNoncurrent                       // NoncurrentVersionExpiration.NewerNoncurrentVersions (count-only, no NoncurrentDays)
	ActionKindAbortMPU                              // AbortIncompleteMultipartUpload.DaysAfterInitiation
	ActionKindExpiredDeleteMarker                   // Expiration.ExpiredObjectDeleteMarker
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

// RuleActionKinds returns the compiled actions a single XML rule expands to.
// Empty when no action sub-element is populated. Order is deterministic so
// callers can hash / iterate stably:
//
//	EXPIRATION_DAYS, EXPIRATION_DATE, EXPIRED_DELETE_MARKER,
//	NONCURRENT_DAYS, NEWER_NONCURRENT, ABORT_MPU
//
// Note: NewerNoncurrentVersions is paired with NoncurrentDays into a single
// NONCURRENT_DAYS action when both are set; only when NewerNoncurrent is set
// alone (no day threshold) does it produce a NEWER_NONCURRENT action.
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
