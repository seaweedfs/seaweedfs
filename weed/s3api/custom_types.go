package s3api

import "github.com/seaweedfs/seaweedfs/weed/s3api/s3err"

const s3TimeFormat = "2006-01-02T15:04:05.999Z07:00"

// ConditionalHeaderResult holds the result of conditional header checking
type ConditionalHeaderResult struct {
	ErrorCode s3err.ErrorCode
	ETag      string // ETag of the object (for 304 responses)
}
