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

// TestIsClientSideMarshalError_RequiresGrpcStatus ensures the carve-out is
// type-based (via errors.As on the grpc status interface), not a naive
// string match against arbitrary errors that happen to mention marshaling.
// A plain errors.New(...) with the same prefix must NOT be treated as a
// per-request marshal error — we have no evidence the connection is healthy.
func TestIsClientSideMarshalError_RequiresGrpcStatus(t *testing.T) {
	impostor := fmt.Errorf("grpc: error while marshaling: synthetic non-status error")
	if isClientSideMarshalError(impostor) {
		t.Fatalf("plain error must not match the marshal-error carve-out")
	}
}

// TestResolveLocalGrpcSocket_RemotePortCollision is a regression test for
// issue #9254. A `weed server` process registers a Unix socket for its
// in-process volume server on host A. A standalone `weed volume` on host B
// happens to use the same gRPC port. Dials from the master to host B must
// continue out over TCP — they must NOT be hijacked into host A's local
// socket on the basis of port match alone.
func TestResolveLocalGrpcSocket_RemotePortCollision(t *testing.T) {
	// Snapshot and restore global state so the test does not leak into others.
	localGrpcSocketsLock.Lock()
	prevSockets := localGrpcSockets
	prevHosts := localGrpcHosts
	localGrpcSockets = make(map[int]string)
	localGrpcHosts = make(map[int]map[string]struct{})
	localGrpcSocketsLock.Unlock()
	t.Cleanup(func() {
		localGrpcSocketsLock.Lock()
		localGrpcSockets = prevSockets
		localGrpcHosts = prevHosts
		localGrpcSocketsLock.Unlock()
	})

	const localHost = "10.0.0.2"
	const remoteHost = "10.0.0.3"
	const collidingPort = 17334
	const socketPath = "/tmp/seaweedfs-volume-grpc-17334.sock"

	RegisterLocalGrpcSocket(localHost, collidingPort, socketPath)

	cases := []struct {
		name    string
		address string
		want    string
	}{
		{"local advertised host routes to socket", localHost + ":17334", socketPath},
		{"loopback v4 routes to socket", "127.0.0.1:17334", socketPath},
		{"localhost routes to socket", "localhost:17334", socketPath},
		{"loopback v6 routes to socket", "[::1]:17334", socketPath},
		{"empty host (bare port) routes to socket", ":17334", socketPath},
		{"remote host with same port stays on TCP", remoteHost + ":17334", ""},
		{"unrelated host with same port stays on TCP", "192.168.1.5:17334", ""},
		{"unregistered port stays on TCP", localHost + ":17335", ""},
		{"malformed address stays on TCP", "not-a-host-port", ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := resolveLocalGrpcSocket(tc.address); got != tc.want {
				t.Fatalf("resolveLocalGrpcSocket(%q) = %q, want %q", tc.address, got, tc.want)
			}
		})
	}
}
