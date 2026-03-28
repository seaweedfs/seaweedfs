package s3lifecycle

import (
	"math"
	"strconv"
	"time"
)

// versionIdFormatThreshold distinguishes old vs new format version IDs.
// New format (inverted timestamps) produces values above this threshold;
// old format (raw timestamps) produces values below it.
const versionIdFormatThreshold = 0x4000000000000000

// GetVersionTimestamp extracts the actual timestamp from a SeaweedFS version ID,
// handling both old (raw nanosecond) and new (inverted nanosecond) formats.
// Returns zero time if the version ID is invalid or "null".
func GetVersionTimestamp(versionId string) time.Time {
	ns := getVersionTimestampNanos(versionId)
	if ns == 0 {
		return time.Time{}
	}
	return time.Unix(0, ns)
}

// getVersionTimestampNanos extracts the raw nanosecond timestamp from a version ID.
func getVersionTimestampNanos(versionId string) int64 {
	if len(versionId) < 16 || versionId == "null" {
		return 0
	}
	timestampPart, err := strconv.ParseUint(versionId[:16], 16, 64)
	if err != nil {
		return 0
	}
	if timestampPart > math.MaxInt64 {
		return 0
	}
	if timestampPart > versionIdFormatThreshold {
		// New format: inverted timestamp, convert back.
		return int64(math.MaxInt64 - timestampPart)
	}
	return int64(timestampPart)
}

// isNewFormatVersionId returns true if the version ID uses inverted timestamps.
func isNewFormatVersionId(versionId string) bool {
	if len(versionId) < 16 || versionId == "null" {
		return false
	}
	timestampPart, err := strconv.ParseUint(versionId[:16], 16, 64)
	if err != nil {
		return false
	}
	return timestampPart > versionIdFormatThreshold && timestampPart <= math.MaxInt64
}

// CompareVersionIds compares two version IDs for sorting (newest first).
// Returns negative if a is newer, positive if b is newer, 0 if equal.
// Handles both old and new format version IDs and uses full lexicographic
// comparison (not just timestamps) to break ties from the random suffix.
func CompareVersionIds(a, b string) int {
	if a == b {
		return 0
	}
	if a == "null" {
		return 1
	}
	if b == "null" {
		return -1
	}

	aIsNew := isNewFormatVersionId(a)
	bIsNew := isNewFormatVersionId(b)

	if aIsNew == bIsNew {
		if aIsNew {
			// New format: smaller hex = newer (inverted timestamps).
			if a < b {
				return -1
			}
			return 1
		}
		// Old format: smaller hex = older.
		if a < b {
			return 1
		}
		return -1
	}

	// Mixed formats: compare by actual timestamp.
	aTime := getVersionTimestampNanos(a)
	bTime := getVersionTimestampNanos(b)
	if aTime > bTime {
		return -1
	}
	if aTime < bTime {
		return 1
	}
	return 0
}
