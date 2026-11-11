package security

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"net"
	"slices"
	"testing"
	"time"

	jwt "github.com/golang-jwt/jwt/v5"
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

func TestCheckGrpcAdminToken(t *testing.T) {
	key := SigningKey("test-signing-key-0123456789abcdef")
	fresh := "Bearer " + string(GenJwtForFilerAdmin(key, 60))
	stale := "Bearer " + string(GenJwtForFilerAdmin(SigningKey("another-key"), 60))
	expired, signErr := jwt.NewWithClaims(jwt.SigningMethodHS256, SeaweedFilerAdminClaims{
		RegisteredClaims: jwt.RegisteredClaims{ExpiresAt: jwt.NewNumericDate(time.Now().Add(-time.Hour))},
	}).SignedString([]byte(key))
	if signErr != nil {
		t.Fatalf("sign expired token: %v", signErr)
	}
	session, signErr := jwt.NewWithClaims(jwt.SigningMethodHS256, SeaweedFilerAdminClaims{
		SessionId:        "session",
		RegisteredClaims: jwt.RegisteredClaims{ExpiresAt: jwt.NewNumericDate(time.Now().Add(time.Hour))},
	}).SignedString([]byte(key))
	if signErr != nil {
		t.Fatalf("sign session token: %v", signErr)
	}
	scoped := func(claims SeaweedFilerClaims) string {
		claims.ExpiresAt = jwt.NewNumericDate(time.Now().Add(time.Hour))
		signed, err := jwt.NewWithClaims(jwt.SigningMethodHS256, claims).SignedString([]byte(key))
		if err != nil {
			t.Fatalf("sign scoped token: %v", err)
		}
		return "Bearer " + signed
	}
	pathScoped := scoped(SeaweedFilerClaims{AllowedPrefixes: []string{"/tenants/t1"}})
	methodScoped := scoped(SeaweedFilerClaims{AllowedMethods: []string{"GET"}})
	unscoped := scoped(SeaweedFilerClaims{})

	for name, tc := range map[string]struct {
		values  []string
		wantErr string
	}{
		"stale then fresh":    {[]string{stale, fresh}, ""},
		"fresh then garbage":  {[]string{fresh, "garbage"}, ""},
		"expired then fresh":  {[]string{"Bearer " + expired, fresh}, ""},
		"only expired":        {[]string{"Bearer " + expired}, "invalid admin token"},
		"only stale":          {[]string{stale}, "invalid admin token"},
		"same-key session":    {[]string{"Bearer " + session}, "invalid admin token"},
		"path-scoped":         {[]string{pathScoped}, "invalid admin token"},
		"method-scoped":       {[]string{methodScoped}, "invalid admin token"},
		"scoped then fresh":   {[]string{pathScoped, fresh}, ""},
		"unscoped filer":      {[]string{unscoped}, ""},
		"basic then stale":    {[]string{"Basic dXNlcjpwYXNz", stale}, "invalid admin token"},
		"no bearer value":     {[]string{"Basic dXNlcjpwYXNz", "garbage"}, "authorization header must use Bearer scheme"},
		"lowercase scheme ok": {[]string{"bearer " + fresh[len("Bearer "):]}, ""},
		"fresh past the cap":  {append(slices.Repeat([]string{stale}, maxGrpcAdminAuthValues), fresh), "invalid admin token"},
	} {
		ctx := metadata.NewIncomingContext(context.Background(), metadata.MD{"authorization": tc.values})
		err := CheckGrpcAdminToken(ctx, key)
		if tc.wantErr == "" {
			if err != nil {
				t.Errorf("%s: unexpected error %v", name, err)
			}
			continue
		}
		if status.Code(err) != codes.Unauthenticated || status.Convert(err).Message() != tc.wantErr {
			t.Errorf("%s: got %v, want Unauthenticated %q", name, err, tc.wantErr)
		}
	}
}
