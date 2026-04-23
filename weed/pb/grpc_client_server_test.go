package pb

import (
	"fmt"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// TestShouldInvalidateConnection_MarshalErrorIsPerRequest ensures that a
// client-side proto marshal failure does NOT cause the shared cached
// ClientConn to be torn down. Tearing it down would cancel every other
// in-flight RPC (seaweedfs#9139: one file with invalid-UTF-8 bytes triggered
// an avalanche of "connection is closing" errors on unrelated operations).
func TestShouldInvalidateConnection_MarshalErrorIsPerRequest(t *testing.T) {
	// Reproduces the exact error gRPC returns when a string field in the
	// outgoing request contains invalid UTF-8 bytes.
	marshalErr := status.Error(codes.Internal,
		"grpc: error while marshaling: string field contains invalid UTF-8")
	if shouldInvalidateConnection(marshalErr) {
		t.Fatalf("client-side marshal error must not invalidate the shared connection")
	}

	// Same error wrapped with fmt.Errorf (common when callers add context).
	wrapped := fmt.Errorf("upload data: %w", marshalErr)
	if shouldInvalidateConnection(wrapped) {
		t.Fatalf("wrapped marshal error must not invalidate the shared connection")
	}
}

// TestShouldInvalidateConnection_GenuineInternalStillInvalidates ensures the
// marshal-error carve-out does not swallow real server-side Internal errors,
// which previously caused — and should continue to cause — connection
// invalidation.
func TestShouldInvalidateConnection_GenuineInternalStillInvalidates(t *testing.T) {
	serverInternal := status.Error(codes.Internal, "stream terminated by RST_STREAM with code 2")
	if !shouldInvalidateConnection(serverInternal) {
		t.Fatalf("genuine server-side Internal must still invalidate the connection")
	}
}

// TestShouldInvalidateConnection_TransportErrorsStillInvalidate is a
// regression guard for the string-matching fallback path (e.g. a raw
// "connection refused" from net.Dial that never acquired a gRPC status).
func TestShouldInvalidateConnection_TransportErrorsStillInvalidate(t *testing.T) {
	for _, msg := range []string{
		"rpc error: code = Unavailable desc = transport is closing",
		"dial tcp: connection refused",
		"read: connection reset by peer",
	} {
		if !shouldInvalidateConnection(fmt.Errorf("%s", msg)) {
			t.Fatalf("transport error %q must still invalidate", msg)
		}
	}
}
