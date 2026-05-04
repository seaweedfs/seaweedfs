package mount

import (
	"strings"
	"syscall"

	"github.com/seaweedfs/go-fuse/v2/fuse"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func grpcErrorToFuseStatus(err error) fuse.Status {
	if err == nil {
		return fuse.OK
	}

	// Unpack error for inspection
	if s, ok := status.FromError(err); ok {
		switch s.Code() {
		case codes.OK:
			return fuse.OK
		case codes.Canceled, codes.DeadlineExceeded:
			return fuse.Status(syscall.ETIMEDOUT)
		case codes.Unavailable:
			return fuse.Status(syscall.EAGAIN)
		case codes.ResourceExhausted:
			return fuse.Status(syscall.EAGAIN) // Or syscall.ENOSPC
		case codes.PermissionDenied:
			return fuse.Status(syscall.EACCES)
		case codes.Unauthenticated:
			return fuse.Status(syscall.EPERM)
		case codes.NotFound:
			return fuse.ENOENT
		case codes.AlreadyExists:
			return fuse.Status(syscall.EEXIST)
		case codes.InvalidArgument:
			return fuse.EINVAL
		}
	}

	// String matching for errors that don't have proper gRPC codes but are known
	errStr := err.Error()
	if strings.Contains(errStr, "transport") {
		return fuse.Status(syscall.EAGAIN)
	}
	// Add other string matches if necessary

	return fuse.EIO
}

// isRetryableFilerError reports whether a filer RPC error looks transient
// enough to retry. It takes a conservative whitelist approach: only errors
// that clearly describe a permanent application-level failure
// (NotFound/AlreadyExists/InvalidArgument/PermissionDenied/Unauthenticated/
// FailedPrecondition) short-circuit the retry loop. Everything else —
// transport errors, Canceled/Unavailable/ResourceExhausted, or errors with no
// gRPC status — is treated as potentially transient and retried.
func isRetryableFilerError(err error) bool {
	if err == nil {
		return false
	}
	if s, ok := status.FromError(err); ok {
		switch s.Code() {
		case codes.NotFound,
			codes.AlreadyExists,
			codes.InvalidArgument,
			codes.PermissionDenied,
			codes.Unauthenticated,
			codes.FailedPrecondition:
			return false
		}
	}
	return true
}
