package mount

import (
	"errors"
	"fmt"
	"syscall"
	"testing"
	"time"

	"github.com/seaweedfs/go-fuse/v2/fuse"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Regression: wfs_save.go used to wrap the gRPC error with fmt.Errorf(... %v ...)
// which stringified the status and made status.FromError fall through to the
// default EIO mapping. Wrapping with %w must preserve the code so that
// codes.Canceled from a closing filer connection surfaces as ETIMEDOUT (a
// retryable hint for FUSE callers) rather than EIO.
func TestGrpcErrorToFuseStatusUnwrapsCanceledThroughFmtErrorf(t *testing.T) {
	grpcErr := status.Error(codes.Canceled, "grpc: the client connection is closing")

	wrapped := fmt.Errorf("UpdateEntry dir /some/path: %w", grpcErr)

	got := grpcErrorToFuseStatus(wrapped)
	want := fuse.Status(syscall.ETIMEDOUT)
	if got != want {
		t.Fatalf("grpcErrorToFuseStatus(canceled wrapped with %%w) = %v, want %v", got, want)
	}
}

// Guard against regressing the wrap verb: %v loses the gRPC status and the
// classifier must fall through to EIO. This test documents that behavior so
// anyone reverting the %w change sees the intent.
func TestGrpcErrorToFuseStatusDropsCanceledThroughPercentV(t *testing.T) {
	grpcErr := status.Error(codes.Canceled, "grpc: the client connection is closing")

	wrapped := fmt.Errorf("UpdateEntry dir /some/path: %v", grpcErr)

	got := grpcErrorToFuseStatus(wrapped)
	if got != fuse.EIO {
		t.Fatalf("grpcErrorToFuseStatus(canceled wrapped with %%v) = %v, want EIO (regression guard)", got)
	}
}

func TestIsRetryableFilerError(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want bool
	}{
		{"nil", nil, false},
		{"canceled", status.Error(codes.Canceled, "grpc: the client connection is closing"), true},
		{"unavailable", status.Error(codes.Unavailable, "connection refused"), true},
		{"deadline_exceeded", status.Error(codes.DeadlineExceeded, "deadline exceeded"), true},
		{"resource_exhausted", status.Error(codes.ResourceExhausted, "too many concurrent requests"), true},
		{"internal", status.Error(codes.Internal, "server crashed"), true},
		{"not_found", status.Error(codes.NotFound, "entry missing"), false},
		{"already_exists", status.Error(codes.AlreadyExists, "duplicate"), false},
		{"invalid_argument", status.Error(codes.InvalidArgument, "bad request"), false},
		{"permission_denied", status.Error(codes.PermissionDenied, "no access"), false},
		{"unauthenticated", status.Error(codes.Unauthenticated, "missing creds"), false},
		{"failed_precondition", status.Error(codes.FailedPrecondition, "not empty"), false},
		{"plain_error_retries", errors.New("random network glitch"), true},
		{"wrapped_canceled_still_retries", fmt.Errorf("ctx: %w", status.Error(codes.Canceled, "closing")), true},
		{"wrapped_not_found_still_skipped", fmt.Errorf("ctx: %w", status.Error(codes.NotFound, "gone")), false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := isRetryableFilerError(tc.err); got != tc.want {
				t.Fatalf("isRetryableFilerError(%v) = %v, want %v", tc.err, got, tc.want)
			}
		})
	}
}

// retryMetadataFlushIf must short-circuit on non-retryable errors so that
// synchronous FUSE ops (chmod/utimes/xattr) don't hang for ~7s on ENOENT/
// EACCES/EINVAL.
func TestRetryMetadataFlushIfShortCircuitsOnPermanentError(t *testing.T) {
	originalSleep := metadataFlushSleep
	t.Cleanup(func() {
		metadataFlushSleep = originalSleep
	})
	metadataFlushSleep = func(_ time.Duration) {
		t.Fatal("sleep should not be called when shouldRetry returns false")
	}

	attempts := 0
	permanent := status.Error(codes.NotFound, "entry missing")
	err := retryMetadataFlushIf(func() error {
		attempts++
		return permanent
	}, isRetryableFilerError, nil)

	if attempts != 1 {
		t.Fatalf("attempts = %d, want 1 (permanent error should short-circuit)", attempts)
	}
	if !errors.Is(err, permanent) {
		t.Fatalf("err = %v, want permanent sentinel", err)
	}
}

// Transient errors must keep retrying up to the attempt cap even when a
// predicate is supplied.
func TestRetryMetadataFlushIfRetriesTransientErrors(t *testing.T) {
	originalSleep := metadataFlushSleep
	t.Cleanup(func() {
		metadataFlushSleep = originalSleep
	})
	metadataFlushSleep = func(_ time.Duration) {}

	attempts := 0
	transient := status.Error(codes.Canceled, "grpc: the client connection is closing")
	err := retryMetadataFlushIf(func() error {
		attempts++
		return transient
	}, isRetryableFilerError, nil)

	if attempts != metadataFlushRetries+1 {
		t.Fatalf("attempts = %d, want %d", attempts, metadataFlushRetries+1)
	}
	if !errors.Is(err, transient) {
		t.Fatalf("err = %v, want transient sentinel", err)
	}
}
