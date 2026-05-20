package s3lifecycle

import (
	"strconv"
)

// versionIdFormatThreshold distinguishes old vs new format version IDs.
// New format (inverted timestamps) produces values above this threshold;
// old format (raw timestamps) produces values below it.
const versionIdFormatThreshold = 0x4000000000000000

// CompareVersionIds returns negative if a is newer than b, positive if b
// is newer, 0 if equal. Mirrors compareVersionIds in s3api_version_id.go
// (kept duplicated to avoid the s3api -> s3lifecycle import cycle). Used
// as a tiebreak when version mtimes collide at second resolution.
func CompareVersionIds(a, b string) int {
	if a == b {
		return 0
	}
	if a == "null" {
		return 1 // null sorts last
	}
	if b == "null" {
		return -1
	}
	aIsNew := isNewFormatVersionId(a)
	bIsNew := isNewFormatVersionId(b)
	if aIsNew == bIsNew {
		// Same format. New format (inverted timestamps) sorts smaller=newer
		// lexicographically; old format sorts smaller=older.
		if aIsNew {
			if a < b {
				return -1
			}
			return 1
		}
		if a < b {
			return 1
		}
		return -1
	}
	at := getVersionTimestamp(a)
	bt := getVersionTimestamp(b)
	if at > bt {
		return -1
	}
	if at < bt {
		return 1
	}
	return 0
}

func isNewFormatVersionId(versionId string) bool {
	if len(versionId) < 16 || versionId == "null" {
		return false
	}
	t, err := strconv.ParseUint(versionId[:16], 16, 64)
	if err != nil {
		return false
	}
	return t > versionIdFormatThreshold
}

func getVersionTimestamp(versionId string) int64 {
	if len(versionId) < 16 || versionId == "null" {
		return 0
	}
	t, err := strconv.ParseUint(versionId[:16], 16, 64)
	if err != nil {
		return 0
	}
	if t > versionIdFormatThreshold {
		return int64(^uint64(0)>>1) - int64(t)
	}
	return int64(t)
}
