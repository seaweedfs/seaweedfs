package mount

import (
	"fmt"
	"syscall"
	"testing"

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
