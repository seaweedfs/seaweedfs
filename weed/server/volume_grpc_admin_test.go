package weed_server

import (
	"context"
	"net"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"

	"github.com/seaweedfs/seaweedfs/weed/security"
)

// fakeAddr is a net.Addr that is not *net.TCPAddr. We use it to fake a
// gRPC peer for the in-process / bufconn / unix-socket case.
type fakeAddr struct{ s string }

func (a fakeAddr) Network() string { return "fake" }
func (a fakeAddr) String() string  { return a.s }

// TestCheckGrpcAdminAuthNonTCPPeerTrusted pins the in-process trust contract.
// When `weed server` runs master+volume+filer in the same process, the master
// dials the volume server via an in-process / bufconn / unix-socket transport.
// The peer Addr is not a *net.TCPAddr (often surfaces as "@"), so we cannot
// extract an IP — but the caller is in the same OS process, so we trust it
// unconditionally. Without this, an empty-whitelist fail-closed guard would
// break every embedded-cluster deployment.
func TestCheckGrpcAdminAuthNonTCPPeerTrusted(t *testing.T) {
	vs := &VolumeServer{guard: security.NewGuard(nil, "", 0, "", 0)}
	cases := []net.Addr{
		fakeAddr{s: "@"},
		fakeAddr{s: ""},
		fakeAddr{s: "bufconn"},
		&net.UnixAddr{Name: "/tmp/weed.sock", Net: "unix"},
	}
	for _, addr := range cases {
		ctx := peer.NewContext(context.Background(), &peer.Peer{Addr: addr})
		if err := vs.checkGrpcAdminAuth(ctx); err != nil {
			t.Errorf("non-TCP peer %T(%q) should be trusted, got %v", addr, addr.String(), err)
		}
	}
}

// TestCheckGrpcAdminAuthTCPLoopbackTrusted covers the common single-host
// deployment where a separate `weed` process on the same host dials over
// real TCP. With no whitelist configured, loopback peers are still allowed
// (delegated to Guard.IsAdminAuthorized).
func TestCheckGrpcAdminAuthTCPLoopbackTrusted(t *testing.T) {
	vs := &VolumeServer{guard: security.NewGuard(nil, "", 0, "", 0)}
	for _, ip := range []string{"127.0.0.1", "127.0.0.5", "::1"} {
		ctx := peer.NewContext(context.Background(), &peer.Peer{
			Addr: &net.TCPAddr{IP: net.ParseIP(ip), Port: 50000},
		})
		if err := vs.checkGrpcAdminAuth(ctx); err != nil {
			t.Errorf("loopback TCP peer %s should be trusted, got %v", ip, err)
		}
	}
}

// TestCheckGrpcAdminAuthTCPOffHostDenied is the fail-closed half of the
// contract: an off-host TCP peer with no matching whitelist entry is denied
// with PermissionDenied.
func TestCheckGrpcAdminAuthTCPOffHostDenied(t *testing.T) {
	vs := &VolumeServer{guard: security.NewGuard(nil, "", 0, "", 0)}
	ctx := peer.NewContext(context.Background(), &peer.Peer{
		Addr: &net.TCPAddr{IP: net.ParseIP("10.0.0.5"), Port: 50000},
	})
	err := vs.checkGrpcAdminAuth(ctx)
	if err == nil {
		t.Fatalf("off-host TCP peer with empty whitelist should be denied")
	}
	if got, want := status.Code(err), codes.PermissionDenied; got != want {
		t.Errorf("status code = %v want %v", got, want)
	}
}

// TestCheckGrpcAdminAuthTCPWhitelistedAllowed verifies that an off-host TCP
// peer present in the configured whitelist is allowed through.
func TestCheckGrpcAdminAuthTCPWhitelistedAllowed(t *testing.T) {
	vs := &VolumeServer{guard: security.NewGuard([]string{"10.0.0.5"}, "", 0, "", 0)}
	ctx := peer.NewContext(context.Background(), &peer.Peer{
		Addr: &net.TCPAddr{IP: net.ParseIP("10.0.0.5"), Port: 50000},
	})
	if err := vs.checkGrpcAdminAuth(ctx); err != nil {
		t.Errorf("whitelisted TCP peer should be allowed, got %v", err)
	}
}

// TestCheckGrpcAdminAuthNoPeerInfoDenied confirms we still deny when gRPC
// gives us no peer info at all (should not happen with real transports but
// we cannot prove a positive identity, so we refuse).
func TestCheckGrpcAdminAuthNoPeerInfoDenied(t *testing.T) {
	vs := &VolumeServer{guard: security.NewGuard(nil, "", 0, "", 0)}
	err := vs.checkGrpcAdminAuth(context.Background())
	if err == nil {
		t.Fatalf("missing peer info should be denied")
	}
	if got, want := status.Code(err), codes.PermissionDenied; got != want {
		t.Errorf("status code = %v want %v", got, want)
	}
}

// TestCheckGrpcAdminAuthNoGuardIsNoop preserves the developmental / embedded
// path: with no Guard wired up at all, the gate is a no-op regardless of
// peer information.
func TestCheckGrpcAdminAuthNoGuardIsNoop(t *testing.T) {
	vs := &VolumeServer{guard: nil}
	ctx := peer.NewContext(context.Background(), &peer.Peer{
		Addr: &net.TCPAddr{IP: net.ParseIP("10.0.0.5"), Port: 50000},
	})
	if err := vs.checkGrpcAdminAuth(ctx); err != nil {
		t.Errorf("no guard should be a no-op, got %v", err)
	}
}
