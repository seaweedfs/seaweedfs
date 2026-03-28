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
	if timestampPart > versionIdFormatThreshold {
		// New format: inverted timestamp, convert back.
		return int64(math.MaxInt64 - timestampPart)
	}
	if timestampPart > math.MaxInt64 {
		return 0
	}
	return int64(timestampPart)
}
