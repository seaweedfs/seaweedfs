package s3_objectlock

import (
	"strconv"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
)

// ObjectLockState is an ExpectedExtended condition that makes the filer refuse
// a change if the entry's Object Lock state changed after it was checked.
func ObjectLockState(entry *filer_pb.Entry) map[string][]byte {
	state := make(map[string][]byte, 3)
	for _, key := range []string{s3_constants.ExtLegalHoldKey, s3_constants.ExtObjectLockModeKey, s3_constants.ExtRetentionUntilDateKey} {
		state[key] = entry.Extended[key]
	}
	return state
}

// DefaultRetention returns the retention that a bucket's Object Lock
// configuration gives a new object.
func DefaultRetention(bucket *filer_pb.Entry, now time.Time) (mode string, retainUntil time.Time, ok bool) {
	if bucket == nil {
		return "", time.Time{}, false
	}
	enabled := string(bucket.Extended[s3_constants.ExtObjectLockEnabledKey])
	mode = string(bucket.Extended[s3_constants.ExtObjectLockDefaultModeKey])
	if enabled != s3_constants.ObjectLockEnabled && enabled != "true" || mode == "" {
		return "", time.Time{}, false
	}
	if days, _ := strconv.Atoi(string(bucket.Extended[s3_constants.ExtObjectLockDefaultDaysKey])); days > 0 {
		return mode, now.AddDate(0, 0, days), true
	}
	if years, _ := strconv.Atoi(string(bucket.Extended[s3_constants.ExtObjectLockDefaultYearsKey])); years > 0 {
		return mode, now.AddDate(years, 0, 0), true
	}
	return "", time.Time{}, false
}
