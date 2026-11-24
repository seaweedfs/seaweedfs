package s3_objectlock

import (
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
