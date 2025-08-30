package s3api

import (
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3err"
)

// ErrorHandlers provide common error handling patterns for S3 API operations

// handlePutToFilerError logs an error and returns the standard putToFiler error format
func handlePutToFilerError(operation string, err error, errorCode s3err.ErrorCode) (string, s3err.ErrorCode, string) {
	glog.Errorf("Failed to %s: %v", operation, err)
	return "", errorCode, ""
}

// handlePutToFilerInternalError is a convenience wrapper for internal errors in putToFiler
func handlePutToFilerInternalError(operation string, err error) (string, s3err.ErrorCode, string) {
	return handlePutToFilerError(operation, err, s3err.ErrInternalError)
}

// handleMultipartError logs an error and returns the standard multipart error format
func handleMultipartError(operation string, err error, errorCode s3err.ErrorCode) (interface{}, s3err.ErrorCode) {
	glog.Errorf("Failed to %s: %v", operation, err)
	return nil, errorCode
}

// handleMultipartInternalError is a convenience wrapper for internal errors in multipart operations
func handleMultipartInternalError(operation string, err error) (interface{}, s3err.ErrorCode) {
	return handleMultipartError(operation, err, s3err.ErrInternalError)
}

// logErrorAndReturn logs an error with operation context and returns the specified error code
func logErrorAndReturn(operation string, err error, errorCode s3err.ErrorCode) s3err.ErrorCode {
	glog.Errorf("Failed to %s: %v", operation, err)
	return errorCode
}

// logInternalError is a convenience wrapper for internal error logging
func logInternalError(operation string, err error) s3err.ErrorCode {
	return logErrorAndReturn(operation, err, s3err.ErrInternalError)
}

// SSE-specific error handlers

// handleSSEError handles common SSE-related errors with appropriate context
func handleSSEError(sseType string, operation string, err error, errorCode s3err.ErrorCode) (string, s3err.ErrorCode, string) {
	glog.Errorf("Failed to %s for %s: %v", operation, sseType, err)
	return "", errorCode, ""
}

// handleSSEInternalError is a convenience wrapper for SSE internal errors
func handleSSEInternalError(sseType string, operation string, err error) (string, s3err.ErrorCode, string) {
	return handleSSEError(sseType, operation, err, s3err.ErrInternalError)
}
