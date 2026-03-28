package s3lifecycle

import "time"

// Rule is a flattened, evaluator-friendly representation of an S3 lifecycle rule.
// Callers convert from the XML-parsed s3api.Rule (which has nested structs with
// set-flags for conditional XML marshaling) to this type.
type Rule struct {
	ID     string
	Status string // "Enabled" or "Disabled"

	// Prefix filter (from Rule.Prefix or Rule.Filter.Prefix or Rule.Filter.And.Prefix).
	Prefix string

	// Expiration for current versions.
	ExpirationDays            int
	ExpirationDate            time.Time
	ExpiredObjectDeleteMarker bool

	// Expiration for non-current versions.
	NoncurrentVersionExpirationDays int
	NewerNoncurrentVersions         int

	// Abort incomplete multipart uploads.
	AbortMPUDaysAfterInitiation int

	// Tag filter (from Rule.Filter.Tag or Rule.Filter.And.Tags).
	FilterTags map[string]string

	// Size filters.
	FilterSizeGreaterThan int64
	FilterSizeLessThan    int64
}

// ObjectInfo is the metadata about an object that the evaluator uses to
// determine which lifecycle action applies. Callers build this from filer
// entry attributes and extended metadata.
type ObjectInfo struct {
	// Key is the object key relative to the bucket root.
	Key string

	// ModTime is the object's modification time (entry.Attributes.Mtime).
	ModTime time.Time

	// Size is the object size in bytes (entry.Attributes.FileSize).
	Size int64

	// IsLatest is true if this is the current version of the object.
	IsLatest bool

	// IsDeleteMarker is true if this entry is an S3 delete marker.
	IsDeleteMarker bool

	// NumVersions is the total number of versions for this object key,
	// including delete markers. Used for ExpiredObjectDeleteMarker evaluation.
	NumVersions int

	// SuccessorModTime is the creation time of the version that replaced
	// this one (making it non-current). Derived from the successor's version
	// ID timestamp. Zero value for the latest version.
	SuccessorModTime time.Time

	// Tags are the object's user-defined tags, extracted from the entry's
	// Extended metadata (keys prefixed with "X-Amz-Tagging-").
	Tags map[string]string
}

// Action represents the lifecycle action to take on an object.
type Action int

const (
	// ActionNone means no lifecycle rule applies.
	ActionNone Action = iota
	// ActionDeleteObject deletes the current version of the object.
	ActionDeleteObject
	// ActionDeleteVersion deletes a specific non-current version.
	ActionDeleteVersion
	// ActionExpireDeleteMarker removes a delete marker that is the sole remaining version.
	ActionExpireDeleteMarker
	// ActionAbortMultipartUpload aborts an incomplete multipart upload.
	ActionAbortMultipartUpload
)

// EvalResult is the output of lifecycle rule evaluation.
type EvalResult struct {
	// Action is the lifecycle action to take.
	Action Action
	// RuleID is the ID of the rule that triggered this action.
	RuleID string
}
