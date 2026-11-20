package security

import (
	"context"
	"slices"
	"testing"
	"time"

	jwt "github.com/golang-jwt/jwt/v5"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

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
			scope, err := CheckGrpcMetadataSubscription(tt.ctx, tt.subKey, adminKey, nil, tt.method)
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
	scope, err := CheckGrpcMetadataSubscription(in, SigningKey("subscribe-key"), SigningKey("admin-key"), nil, MethodSubscribeLocalMetadata)
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
	scope, err = CheckGrpcMetadataSubscription(metadata.NewIncomingContext(context.Background(), md), SigningKey("subscribe-b"), clusterKey, nil, MethodSubscribeMetadata)
	if err != nil || scope != nil {
		t.Fatalf("per-cluster token: scope=%v err=%v, want unrestricted", scope, err)
	}
}
