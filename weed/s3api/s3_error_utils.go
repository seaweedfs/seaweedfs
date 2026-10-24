package s3api

import (
	"errors"
	"strings"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3err"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"google.golang.org/grpc/codes"
)

// isFilerNotFound reports whether a filer error is a not-found.
// Unlike lookups (normalized in filer_pb.LookupEntry), list and cache errors
// cross gRPC as raw status errors, so the sentinel survives as codes.NotFound
// or only as text. The text is matched last and only on what the filer itself
// said, since callers wrap these errors with a path the client chose.
func isFilerNotFound(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, filer_pb.ErrNotFound) {
		return true
	}
	if st, ok := util.ServerStatus(err); ok {
		return st.Code() == codes.NotFound || strings.Contains(st.Message(), filer_pb.ErrNotFound.Error())
	}
	return strings.Contains(err.Error(), filer_pb.ErrNotFound.Error())
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
