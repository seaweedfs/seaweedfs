package security

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"net"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
)

func peerCtx(addr net.Addr) context.Context {
	return peer.NewContext(context.Background(), &peer.Peer{Addr: addr})
}

func bearerCtx(addr net.Addr, signingKey SigningKey) context.Context {
	ctx := peerCtx(addr)
	token := GenJwtForFilerAdmin(signingKey, 0)
	return metadata.NewIncomingContext(ctx, metadata.Pairs("authorization", "Bearer "+string(token)))
}

func TestCheckGrpcAdminPeer(t *testing.T) {
	key := SigningKey("test-signing-key-0123456789abcdef")
	loopback := &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 5000}
	remote := &net.TCPAddr{IP: net.ParseIP("203.0.113.9"), Port: 5000}

	for name, tc := range map[string]struct {
		ctx        context.Context
		signingKey SigningKey
		wantCode   codes.Code
	}{
		"no peer info, no key":          {context.Background(), nil, codes.Unauthenticated},
		"loopback, no key":             {peerCtx(loopback), nil, codes.OK},
		"remote, no key":               {peerCtx(remote), nil, codes.Unauthenticated},
		"remote + valid token":         {bearerCtx(remote, key), key, codes.OK},
		"remote + no metadata, key set": {peerCtx(remote), key, codes.Unauthenticated},
		"remote + bad token":           {metadata.NewIncomingContext(peerCtx(remote), metadata.Pairs("authorization", "Bearer wrong")), key, codes.Unauthenticated},
		"unix socket, no key":          {peerCtx(&net.UnixAddr{Name: "/tmp/s.sock"}), nil, codes.OK},
		"mtls verified peer": {peer.NewContext(context.Background(), &peer.Peer{
			Addr:     remote,
			AuthInfo: credentials.TLSInfo{State: tls.ConnectionState{VerifiedChains: [][]*x509.Certificate{{}}}},
		}), nil, codes.OK},
	} {
		err := CheckGrpcAdminPeer(tc.ctx, tc.signingKey)
		got := codes.OK
		if err != nil {
			got = status.Code(err)
		}
		if got != tc.wantCode {
			t.Errorf("%s: got %v want %v (err=%v)", name, got, tc.wantCode, err)
		}
	}
}

func TestGrpcAdminPeerCredentials(t *testing.T) {
	if md := GrpcAdminPeerCredentials(nil, 0); len(md) != 0 {
		t.Errorf("expected nil metadata without signing key, got %v", md)
	}
	md := GrpcAdminPeerCredentials(SigningKey("key"), 60)
	if len(md.Get("authorization")) != 1 {
		t.Fatalf("expected one authorization entry, got %v", md)
	}
}
