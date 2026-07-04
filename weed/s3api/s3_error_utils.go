package s3api

import (
	"errors"
	"strings"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3err"
)

// isFilerListNotFound reports whether a filer list error is a not-found.
// Unlike lookups (normalized in filer_pb.LookupEntry), list errors cross gRPC
// as raw status errors, so the sentinel survives only as text. Callers must
// pass errors from paths without user-controlled segments that could spoof it.
func isFilerListNotFound(err error) bool {
	return errors.Is(err, filer_pb.ErrNotFound) || strings.Contains(err.Error(), filer_pb.ErrNotFound.Error())
}

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
