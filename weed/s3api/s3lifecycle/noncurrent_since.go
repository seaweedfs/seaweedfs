package s3lifecycle

import (
	"strconv"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
)

// SuccessorFromEntryStamp returns the explicit noncurrent-since timestamp
// written by the S3 PUT handler at demotion time
// (s3_constants.ExtNoncurrentSinceNsKey). Returns zero time if the stamp
// is missing or unparseable — the caller falls back to derived values
// (sibling mtime or entry mtime) for legacy entries written before the
// stamp existed.
//
// The lifecycle engine prefers this stamp over the legacy
// "next-newer sibling mtime" derivation because it records the exact
// moment a version was demoted, immune to later mtime edits on the
// sibling. The router and bootstrap walker both read it through this
// helper so the parsing rules — non-positive, unparseable, missing —
// stay in one place.
func SuccessorFromEntryStamp(entry *filer_pb.Entry) time.Time {
	if entry == nil {
		return time.Time{}
	}
	raw, ok := entry.Extended[s3_constants.ExtNoncurrentSinceNsKey]
	if !ok || len(raw) == 0 {
		return time.Time{}
	}
	ns, err := strconv.ParseInt(string(raw), 10, 64)
	if err != nil || ns <= 0 {
		return time.Time{}
	}
	return time.Unix(0, ns)
}
