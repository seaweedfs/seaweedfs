package mount

import (
	"context"
	"errors"
	"fmt"
	"syscall"
	"testing"
	"time"

	"github.com/seaweedfs/go-fuse/v2/fuse"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
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

// A full cluster must surface as ENOSPC so cp/rsync abort with "No space left
// on device" instead of a generic I/O error. The master's message reaches the
// mount stringified inside AssignVolumeResponse.Error, in one of two shapes:
// the load-shedding gRPC status text or the topology's plain error text.
func TestWriteErrorToFuseStatus(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want fuse.Status
	}{
		{
			"assign shed during growth",
			errors.New("upload data: filerGrpcAddress assign volume: assign volume failure count:1: assign volume: rpc error: code = ResourceExhausted desc = no writable volumes for replication:000 collection:, volume growth in progress"),
			fuse.Status(syscall.ENOSPC),
		},
		{
			"no free volumes left",
			errors.New("flush data: upload data: assign volume: No writable volumes available for collection: replication:000 ttl: and no free volumes left for {replication:000}"),
			fuse.Status(syscall.ENOSPC),
		},
		{
			"unrelated upload failure",
			errors.New("upload data: put http://volume:8080/3,0144: connection reset by peer"),
			fuse.EIO,
		},
		{
			"bucket over quota",
			fmt.Errorf("flush data: upload data: filerGrpcAddress assign volume: assign volume failure count:1: %w",
				filer_pb.AssignVolumeResponseError(&filer_pb.AssignVolumeResponse{
					Error:     "assign volume: read only: /buckets/b/ (e.g. bucket over quota)",
					ErrorCode: filer_pb.FilerError_READ_ONLY,
				})),
			fuse.Status(syscall.EDQUOT),
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := writeErrorToFuseStatus(tc.err); got != tc.want {
				t.Fatalf("writeErrorToFuseStatus(%v) = %v, want %v", tc.err, got, tc.want)
			}
		})
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
		{"create_condition_failed", fmt.Errorf("fh flush create /f: CreateEntry /f: %w", filer_pb.ErrPreconditionFailed), false},
		{"create_read_only", fmt.Errorf("fh flush create /f: CreateEntry /f: %w", filer_pb.ErrReadOnly), false},
		{"create_read_only_from_older_filer", fmt.Errorf("fh flush create /f: CreateEntry /f: %w", filer_pb.LegacyReadOnlyError("read only: /buckets/b/ (e.g. bucket over quota)")), false},
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
	metadataFlushSleep = func(_ context.Context, _ time.Duration) {
		t.Fatal("sleep should not be called when shouldRetry returns false")
	}

	attempts := 0
	permanent := status.Error(codes.NotFound, "entry missing")
	err := retryMetadataFlushIf(context.Background(), func() error {
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

// readOnlyFlushCreateErrors are the shapes a close() of a new file sees when
// its bucket is read-only, from a filer that codes the verdict and from one
// that only sends text, wrapped the way flushMetadataToFiler wraps them.
func readOnlyFlushCreateErrors() map[string]error {
	const path = "/buckets/b/dir/.git/index.lock"
	wrap := func(cause error) error {
		return &contentPublicationError{cause: fmt.Errorf("fh flush create %s: %w", path, cause)}
	}
	return map[string]error{
		"coded":  wrap(fmt.Errorf("CreateEntry /buckets/b/dir/index.lock: %w", filer_pb.ErrReadOnly)),
		"legacy": wrap(fmt.Errorf("CreateEntry /buckets/b/dir/index.lock: %w", filer_pb.LegacyReadOnlyError("read only: /buckets/b/ (e.g. bucket over quota)"))),
	}
}

// A close() on a read-only bucket used to retry its create four times (7 s)
// and then return EIO. It must fail on the first answer, as EDQUOT.
func TestRetryMetadataFlushIfStopsOnReadOnly(t *testing.T) {
	originalSleep := metadataFlushSleep
	t.Cleanup(func() {
		metadataFlushSleep = originalSleep
	})
	metadataFlushSleep = func(_ context.Context, _ time.Duration) {
		t.Fatal("sleep should not be called for a read-only verdict")
	}

	for name, readOnly := range readOnlyFlushCreateErrors() {
		t.Run(name, func(t *testing.T) {
			attempts := 0
			err := retryMetadataFlushIf(context.Background(), func() error {
				attempts++
				return readOnly
			}, isRetryableFilerError, nil)
			if attempts != 1 {
				t.Fatalf("attempts = %d, want 1", attempts)
			}
			if got, want := grpcErrorToFuseStatus(err), fuse.Status(syscall.EDQUOT); got != want {
				t.Fatalf("grpcErrorToFuseStatus(%v) = %v, want %v", err, got, want)
			}
		})
	}
}

// mkdir, symlink and link name the read-only verdict and keep EIO for the rest.
func TestEntryCreateErrorToFuseStatus(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want fuse.Status
	}{
		{"read_only", fmt.Errorf("CreateEntry /buckets/b/d: %w", filer_pb.ErrReadOnly), fuse.Status(syscall.EDQUOT)},
		{"read_only_from_older_filer", fmt.Errorf("CreateEntry /buckets/b/d: %w", filer_pb.LegacyReadOnlyError("read only: /buckets/b/ (e.g. bucket over quota)")), fuse.Status(syscall.EDQUOT)},
		{"other", &streamMutateError{msg: "store unavailable", errno: syscall.EIO}, fuse.EIO},
		{"unavailable", status.Error(codes.Unavailable, "connection refused"), fuse.EIO},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := entryCreateErrorToFuseStatus(tc.err); got != tc.want {
				t.Fatalf("entryCreateErrorToFuseStatus(%v) = %v, want %v", tc.err, got, tc.want)
			}
		})
	}
}

// Transient errors must keep retrying up to the attempt cap even when a
// predicate is supplied.
func TestRetryMetadataFlushIfRetriesTransientErrors(t *testing.T) {
	originalSleep := metadataFlushSleep
	t.Cleanup(func() {
		metadataFlushSleep = originalSleep
	})
	metadataFlushSleep = func(_ context.Context, _ time.Duration) {}

	attempts := 0
	transient := status.Error(codes.Canceled, "grpc: the client connection is closing")
	err := retryMetadataFlushIf(context.Background(), func() error {
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

// withFilerClient must not retry a filer verdict, even when the path in its
// message contains a word the transient-text match looks for.
func TestIsRetryableFilerCall(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want bool
	}{
		{"read only, path names transport", fmt.Errorf("CreateEntry /buckets/b/transport/f: %w", filer_pb.ErrReadOnly), false},
		{"older filer read only", fmt.Errorf("CreateEntry /buckets/b/transport/f: %w", filer_pb.LegacyReadOnlyError("read only: /buckets/b/ (e.g. bucket over quota)")), false},
		{"assign refused by rule", fmt.Errorf("assign volume failure path:\"/buckets/b/transport/f\": %w", filer_pb.AssignVolumeResponseError(&filer_pb.AssignVolumeResponse{
			Error:     "assign volume: read only: /buckets/b/ (e.g. bucket over quota)",
			ErrorCode: filer_pb.FilerError_READ_ONLY,
		})), false},
		{"filer unavailable", status.Error(codes.Unavailable, "connection refused"), true},
		{"transport failure text", errors.New("assign volume: transport is closing"), true},
		{"not transient", errors.New("mismatching cookie"), false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := isRetryableFilerCall(tc.err); got != tc.want {
				t.Fatalf("isRetryableFilerCall(%v) = %v, want %v", tc.err, got, tc.want)
			}
		})
	}
}
