package s3lifecycle

import "time"

// Rule is the flat, evaluator-friendly representation of an S3 lifecycle
// rule. Callers convert from the XML-parsed s3api.Rule via
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

	// Zero is treated as "not set." Note that this can't represent an
	// explicit <ObjectSizeGreaterThan>0</...> (excludes 0-byte objects);
	// if a deployment ever needs that, switch to *int64.
	FilterSizeGreaterThan int64
	FilterSizeLessThan    int64
}

// ObjectInfo is the live-entry shape the evaluator consults. Callers build
// it from filer entry attributes and Extended metadata.
type ObjectInfo struct {
	Key            string
	ModTime        time.Time
	Size           int64
	IsLatest       bool
	IsDeleteMarker bool
	NumVersions    int

	// SuccessorModTime is when the version that replaced this one was
	// created. Zero for IsLatest entries.
	SuccessorModTime time.Time

	// NoncurrentIndex: 0-based index among non-current versions, newest
	// first. nil = current version or index not yet computed; the count-
	// based retention path returns ActionNone in that case rather than
	// guessing. Pointer-not-int so the valid index 0 can't collide with
	// the zero-value uninitialised case.
	NoncurrentIndex *int

	Tags map[string]string

	// IsMPUInit signals an in-flight multipart-upload init under
	// <bucket>/.uploads/<uploadId>/. ModTime then carries the init time.
	IsMPUInit bool
}

const (
	StatusEnabled  = "Enabled"
	StatusDisabled = "Disabled"
)

// SmallDelay is the lookback for predicate-change events and the event-log
// horizon for count / immediate kinds (NewerNoncurrent, ExpiredDeleteMarker).
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
