package s3api

import (
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3err"
)

// ConditionalHeaderResult holds the result of conditional header checking
type ConditionalHeaderResult struct {
	ErrorCode s3err.ErrorCode
	ETag      string          // ETag of the object (for 304 responses)
	Entry     *filer_pb.Entry // Entry fetched during conditional check (nil if not fetched or object doesn't exist)
}
