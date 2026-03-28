package s3api

import (
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3err"
)

// ErrorHandlers provide common error handling patterns for S3 API operations

// handleMultipartError logs an error and returns the standard multipart error format
func handleMultipartError(operation string, err error, errorCode s3err.ErrorCode) (interface{}, s3err.ErrorCode) {
	glog.Errorf("Failed to %s: %v", operation, err)
	return nil, errorCode
}

// handleMultipartInternalError is a convenience wrapper for internal errors in multipart operations
func handleMultipartInternalError(operation string, err error) (interface{}, s3err.ErrorCode) {
	return handleMultipartError(operation, err, s3err.ErrInternalError)
}
