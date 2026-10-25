package s3lifecycle

import "time"

// Rule is the flat representation built from the XML-parsed s3api.Rule via
// s3api.LifecycleToCanonical.
type Rule struct {
	ID     string
	Status string // "Enabled" | "Disabled"

	Prefix string

	ExpirationDays            int
	ExpirationDate            time.Time
	ExpiredObjectDeleteMarker bool

	NoncurrentVersionExpirationDays int
	NewerNoncurrentVersions         int

	AbortMPUDaysAfterInitiation int

	FilterTags map[string]string

	// Zero is "not set"; can't represent an explicit
	// <ObjectSizeGreaterThan>0</...> exclusion of empty objects.
	FilterSizeGreaterThan int64
	FilterSizeLessThan    int64
}

type ObjectInfo struct {
	Key            string
	ModTime        time.Time
	Size           int64
	IsLatest       bool
	IsDeleteMarker bool
	NumVersions    int

	SuccessorModTime time.Time

	// NoncurrentIndex: 0-based among non-current versions, newest first.
	// nil = current or not yet computed; count-based retention returns
	// ActionNone rather than guess. Pointer so valid 0 doesn't collide
	// with zero-value "uninitialised".
	NoncurrentIndex *int

	Tags map[string]string

	// IsMPUInit: in-flight upload under .uploads/<id>/; ModTime is init time.
	IsMPUInit bool
}

const (
	StatusEnabled  = "Enabled"
	StatusDisabled = "Disabled"
)

// SmallDelay is the lookback for predicate-change events and the event-log
// horizon for count / immediate kinds.
const SmallDelay = time.Minute

type Action int

const (
	ActionNone Action = iota
	ActionDeleteObject
	ActionDeleteVersion
	ActionExpireDeleteMarker
	ActionAbortMultipartUpload
)

type EvalResult struct {
	Action Action
	RuleID string
}
