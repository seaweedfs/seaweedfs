package security

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"slices"
	"testing"
	"time"

	jwt "github.com/golang-jwt/jwt/v5"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
)

// mtlsPeerCtx is the context of a caller whose client certificate the server
// verified, the peer CheckGrpcMetadataSubscription trusts without a token.
func mtlsPeerCtx() context.Context {
	return peer.NewContext(context.Background(), &peer.Peer{
		AuthInfo: credentials.TLSInfo{State: tls.ConnectionState{VerifiedChains: [][]*x509.Certificate{{}}}},
	})
}

func mtlsBearerCtx(token string) context.Context {
	return metadata.NewIncomingContext(mtlsPeerCtx(), metadata.Pairs("authorization", "Bearer "+token))
}

// CheckGrpcMetadataSubscriptionToken validates the bearer token alone, without
// the mTLS shortcut, so grpc_require_token can refuse an mTLS peer that carries
// no valid token; and CheckGrpcMetadataSubscription applies a subscription
// token's scope even to an mTLS peer, rather than treating it as unrestricted.
func TestMetadataSubscriptionMtlsAndTokenOnly(t *testing.T) {
	subKey := SigningKey("subscribe-key")
	adminKey := SigningKey("admin-key")
	scoped := string(GenJwtForMetadataSubscription(subKey, 60, []string{"/buckets/a"}))
	admin := string(GenJwtForFilerAdmin(adminKey, 60))
	foreign := string(GenJwtForMetadataSubscription(SigningKey("other"), 60, []string{"/buckets/a"}))
	if scoped == "" || admin == "" {
		t.Fatal("failed to mint tokens")
	}
	method := MethodSubscribeMetadata

	// Token-only check: mTLS never substitutes for a valid token.
	t.Run("token-only ignores mTLS", func(t *testing.T) {
		// mTLS peer with no token -> refused (this is what keeps require mode
		// fail-closed when the interceptor falls back to this check).
		if _, err := CheckGrpcMetadataSubscriptionToken(mtlsPeerCtx(), subKey, adminKey, method); status.Code(err) != codes.Unauthenticated {
			t.Fatalf("mTLS peer, no token: code = %v, want Unauthenticated", status.Code(err))
		}
		// mTLS peer with an invalid (foreign-key) token -> refused.
		if _, err := CheckGrpcMetadataSubscriptionToken(mtlsBearerCtx(foreign), subKey, adminKey, method); status.Code(err) != codes.Unauthenticated {
			t.Fatalf("mTLS peer, foreign token: code = %v, want Unauthenticated", status.Code(err))
		}
		// mTLS peer with a valid scoped token -> accepted AND scoped.
		scope, err := CheckGrpcMetadataSubscriptionToken(mtlsBearerCtx(scoped), subKey, adminKey, method)
		if err != nil {
			t.Fatalf("mTLS peer, scoped token: %v", err)
		}
		if scope == nil || !slices.Equal(scope.Prefixes, []string{"/buckets/a"}) {
			t.Fatalf("scope = %+v, want /buckets/a", scope)
		}
		// Admin token -> unrestricted (nil scope), no error.
		if scope, err := CheckGrpcMetadataSubscriptionToken(mtlsBearerCtx(admin), subKey, adminKey, method); err != nil || scope != nil {
			t.Fatalf("mTLS peer, admin token: scope=%+v err=%v, want nil,nil", scope, err)
		}
		// Plain (no mTLS) scoped token -> accepted AND scoped.
		if scope, err := CheckGrpcMetadataSubscriptionToken(incomingBearer(scoped), subKey, adminKey, method); err != nil || scope == nil {
			t.Fatalf("plain scoped token: scope=%+v err=%v, want a scope", scope, err)
		}
	})

	// Full check: the mTLS shortcut still trusts a tokenless peer, but a present
	// scoped token's scope is applied even under mTLS (no longer unrestricted).
	t.Run("full check honors a scoped token over the mTLS shortcut", func(t *testing.T) {
		// mTLS peer, no token -> unrestricted (pre-existing trust model preserved).
		if scope, err := CheckGrpcMetadataSubscription(mtlsPeerCtx(), subKey, adminKey, method); err != nil || scope != nil {
			t.Fatalf("mTLS peer, no token: scope=%+v err=%v, want nil,nil", scope, err)
		}
		// mTLS peer, scoped token -> BOUNDED to the token's scope (the fix).
		scope, err := CheckGrpcMetadataSubscription(mtlsBearerCtx(scoped), subKey, adminKey, method)
		if err != nil {
			t.Fatalf("mTLS peer, scoped token: %v", err)
		}
		if scope == nil || !slices.Equal(scope.Prefixes, []string{"/buckets/a"}) {
			t.Fatalf("mTLS peer, scoped token: scope = %+v, want it bounded to /buckets/a", scope)
		}
		// mTLS peer, admin token -> unrestricted.
		if scope, err := CheckGrpcMetadataSubscription(mtlsBearerCtx(admin), subKey, adminKey, method); err != nil || scope != nil {
			t.Fatalf("mTLS peer, admin token: scope=%+v err=%v, want nil,nil", scope, err)
		}
		// No mTLS, no token, subscribe key set -> refused.
		if _, err := CheckGrpcMetadataSubscription(context.Background(), subKey, adminKey, method); status.Code(err) != codes.Unauthenticated {
			t.Fatalf("no mTLS, no token: code = %v, want Unauthenticated", status.Code(err))
		}
	})
}

func incomingBearer(token string) context.Context {
	return metadata.NewIncomingContext(context.Background(), metadata.Pairs("authorization", "Bearer "+token))
}

func TestCheckGrpcMetadataSubscription(t *testing.T) {
	subKey := SigningKey("subscribe-key")
	adminKey := SigningKey("admin-key")
	scoped := string(GenJwtForMetadataSubscription(subKey, 60, []string{"/buckets/uploads", "/buckets/logs/"}))
	if scoped == "" {
		t.Fatal("no scoped token")
	}
	signWith := func(method jwt.SigningMethod, key SigningKey, claims jwt.Claims) string {
		s, err := jwt.NewWithClaims(method, claims).SignedString([]byte(key))
		if err != nil {
			t.Fatal(err)
		}
		return s
	}
	sign := func(key SigningKey, claims jwt.Claims) string { return signWith(jwt.SigningMethodHS256, key, claims) }
	exp := jwt.RegisteredClaims{ExpiresAt: jwt.NewNumericDate(time.Now().Add(time.Minute))}
	subscribeOnly := []string{MethodSubscribeMetadata}
	wantScope := []string{"/buckets/uploads", "/buckets/logs"}

	tests := []struct {
		name      string
		ctx       context.Context
		subKey    SigningKey
		method    string
		wantScope []string
		wantCode  codes.Code
	}{
		{"feature off keeps the call open", context.Background(), nil, MethodSubscribeMetadata, nil, codes.OK},
		{"scoped token", incomingBearer(scoped), subKey, MethodSubscribeMetadata, wantScope, codes.OK},
		{"scoped token, local call", incomingBearer(scoped), subKey, MethodSubscribeLocalMetadata, wantScope, codes.OK},
		{"admin token is unrestricted", incomingBearer(string(GenJwtForFilerAdmin(adminKey, 60))), subKey, MethodSubscribeMetadata, nil, codes.OK},
		{"no token", context.Background(), subKey, MethodSubscribeMetadata, nil, codes.Unauthenticated},
		{"scoped token signed with another key", incomingBearer(string(GenJwtForMetadataSubscription(SigningKey("other"), 60, []string{"/a"}))), subKey, MethodSubscribeMetadata, nil, codes.Unauthenticated},
		{"scoped token signed with the admin key", incomingBearer(string(GenJwtForMetadataSubscription(adminKey, 60, []string{"/a"}))), subKey, MethodSubscribeMetadata, nil, codes.Unauthenticated},
		{"admin token signed with the subscribe key", incomingBearer(string(GenJwtForFilerAdmin(subKey, 60))), subKey, MethodSubscribeMetadata, nil, codes.Unauthenticated},
		{"token without prefixes", incomingBearer(sign(subKey, SeaweedFilerClaims{AllowedMethods: subscribeOnly, RegisteredClaims: exp})), subKey, MethodSubscribeMetadata, nil, codes.Unauthenticated},
		{"token without methods", incomingBearer(sign(subKey, SeaweedFilerClaims{AllowedPrefixes: []string{"/a"}, RegisteredClaims: exp})), subKey, MethodSubscribeMetadata, nil, codes.Unauthenticated},
		{"token for a write method too", incomingBearer(sign(subKey, SeaweedFilerClaims{AllowedPrefixes: []string{"/a"}, AllowedMethods: []string{MethodSubscribeMetadata, "PUT"}, RegisteredClaims: exp})), subKey, MethodSubscribeMetadata, nil, codes.Unauthenticated},
		{"token for the other method only", incomingBearer(sign(subKey, SeaweedFilerClaims{AllowedPrefixes: []string{"/a"}, AllowedMethods: []string{MethodSubscribeLocalMetadata}, RegisteredClaims: exp})), subKey, MethodSubscribeMetadata, nil, codes.Unauthenticated},
		{"relative prefix", incomingBearer(sign(subKey, SeaweedFilerClaims{AllowedPrefixes: []string{"/a", "b"}, AllowedMethods: subscribeOnly, RegisteredClaims: exp})), subKey, MethodSubscribeMetadata, nil, codes.Unauthenticated},
		{"session token", incomingBearer(sign(subKey, SeaweedFilerClaims{SessionId: "s", AllowedPrefixes: []string{"/a"}, AllowedMethods: subscribeOnly, RegisteredClaims: exp})), subKey, MethodSubscribeMetadata, nil, codes.Unauthenticated},
		{"token without expiry", incomingBearer(sign(subKey, SeaweedFilerClaims{AllowedPrefixes: []string{"/a"}, AllowedMethods: subscribeOnly})), subKey, MethodSubscribeMetadata, nil, codes.Unauthenticated},
		{"HS384 token", incomingBearer(signWith(jwt.SigningMethodHS384, subKey, SeaweedFilerClaims{AllowedPrefixes: []string{"/a"}, AllowedMethods: subscribeOnly, RegisteredClaims: exp})), subKey, MethodSubscribeMetadata, nil, codes.Unauthenticated},
		{"HS512 token", incomingBearer(signWith(jwt.SigningMethodHS512, subKey, SeaweedFilerClaims{AllowedPrefixes: []string{"/a"}, AllowedMethods: subscribeOnly, RegisteredClaims: exp})), subKey, MethodSubscribeMetadata, nil, codes.Unauthenticated},
		{"expired token", incomingBearer(sign(subKey, SeaweedFilerClaims{AllowedPrefixes: []string{"/a"}, AllowedMethods: subscribeOnly, RegisteredClaims: jwt.RegisteredClaims{ExpiresAt: jwt.NewNumericDate(time.Now().Add(-time.Hour))}})), subKey, MethodSubscribeMetadata, nil, codes.Unauthenticated},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			scope, err := CheckGrpcMetadataSubscription(tt.ctx, tt.subKey, adminKey, tt.method)
			if status.Code(err) != tt.wantCode {
				t.Fatalf("code = %v (%v), want %v", status.Code(err), err, tt.wantCode)
			}
			if (scope == nil) != (tt.wantScope == nil) || scope != nil && !slices.Equal(scope.Prefixes, tt.wantScope) {
				t.Fatalf("scope = %+v, want %v", scope, tt.wantScope)
			}
			if scope != nil && time.Until(scope.ExpiresAt) <= 0 {
				t.Fatalf("scope expiry %v is not in the future", scope.ExpiresAt)
			}
		})
	}
}

func TestGenJwtForMetadataSubscriptionRefusesUnusableScope(t *testing.T) {
	key := SigningKey("k")
	for name, tok := range map[string]EncodedJwt{
		"no expiry":       GenJwtForMetadataSubscription(key, 0, []string{"/a"}),
		"no prefix":       GenJwtForMetadataSubscription(key, 60, nil),
		"relative prefix": GenJwtForMetadataSubscription(key, 60, []string{"/a", "b"}),
		"no key":          GenJwtForMetadataSubscription(nil, 60, []string{"/a"}),
	} {
		if tok != "" {
			t.Errorf("%s: minted %q", name, tok)
		}
	}
}

func TestScopedSubscriptionTokenIsNotAdmin(t *testing.T) {
	key := SigningKey("same-key")
	if IsFilerAdminToken(key, GenJwtForMetadataSubscription(key, 60, []string{"/a"})) {
		t.Fatal("a scoped subscription token must not count as an admin token")
	}
}

func TestMetadataPathInScope(t *testing.T) {
	prefixes := []string{"/buckets/a"}
	for p, want := range map[string]bool{
		"/buckets/a":        true,
		"/buckets/a/x/y":    true,
		"/buckets/ab":       false,
		"/buckets":          false,
		"/buckets/a/../b/x": false,
		"/buckets/a/../a/x": true,
		"/":                 false,
	} {
		if got := MetadataPathInScope(p, prefixes); got != want {
			t.Errorf("MetadataPathInScope(%q) = %v, want %v", p, got, want)
		}
	}
	if !MetadataPathInScope("/anything", []string{"/"}) {
		t.Error("the root prefix holds every path")
	}
}

func TestWithMetadataSubscriptionCredentials(t *testing.T) {
	v := util.GetViper()
	defer v.Set("jwt.filer_signing.key", "")
	defer v.Set("jwt.filer_signing.subscribe.key", "")

	v.Set("jwt.filer_signing.key", "admin-key")
	if md, _ := metadata.FromOutgoingContext(WithMetadataSubscriptionCredentials(context.Background(), nil)); len(md.Get("authorization")) != 0 {
		t.Fatal("no admin token without the subscribe key")
	}

	v.Set("jwt.filer_signing.subscribe.key", "subscribe-key")
	md, _ := metadata.FromOutgoingContext(WithMetadataSubscriptionCredentials(context.Background(), nil))
	in := metadata.NewIncomingContext(context.Background(), md)
	scope, err := CheckGrpcMetadataSubscription(in, SigningKey("subscribe-key"), SigningKey("admin-key"), MethodSubscribeLocalMetadata)
	if err != nil || scope != nil {
		t.Fatalf("scope=%v err=%v, want unrestricted", scope, err)
	}

	clusterKey := SigningKey("cluster-b-key")
	md, _ = metadata.FromOutgoingContext(WithMetadataSubscriptionCredentials(context.Background(), func(bool) EncodedJwt {
		return GenJwtForFilerServer(clusterKey, 10)
	}))
	if len(md.Get("authorization")) != 1 {
		t.Fatalf("want only the per-cluster token, got %v", md.Get("authorization"))
	}
	scope, err = CheckGrpcMetadataSubscription(metadata.NewIncomingContext(context.Background(), md), SigningKey("subscribe-b"), clusterKey, MethodSubscribeMetadata)
	if err != nil || scope != nil {
		t.Fatalf("per-cluster token: scope=%v err=%v, want unrestricted", scope, err)
	}
}
