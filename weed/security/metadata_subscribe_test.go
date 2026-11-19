package security

import (
	"context"
	"strings"
	"testing"
	"time"

	jwt "github.com/golang-jwt/jwt/v5"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

var (
	testReadKey  = SigningKey("filer-read-key")
	testAdminKey = SigningKey("filer-write-key")
)

func signSubscribeClaims(t *testing.T, key SigningKey, claims SeaweedFilerClaims) EncodedJwt {
	t.Helper()
	s, err := jwt.NewWithClaims(jwt.SigningMethodHS256, claims).SignedString([]byte(key))
	if err != nil {
		t.Fatal(err)
	}
	return EncodedJwt(s)
}

func subscribeClaims(prefixes, methods []string, expiresIn time.Duration) SeaweedFilerClaims {
	return SeaweedFilerClaims{
		AllowedPrefixes:  prefixes,
		AllowedMethods:   methods,
		RegisteredClaims: jwt.RegisteredClaims{ExpiresAt: jwt.NewNumericDate(time.Now().Add(expiresIn))},
	}
}

func ctxWithBearer(tokens ...EncodedJwt) context.Context {
	md := metadata.MD{}
	for _, tok := range tokens {
		md.Append("authorization", "Bearer "+string(tok))
	}
	return metadata.NewIncomingContext(context.Background(), md)
}

func TestGenJwtForMetadataSubscriptionRoundTrip(t *testing.T) {
	tok := GenJwtForMetadataSubscription(testReadKey, 60, []string{"/buckets/uploads"})
	if tok == "" {
		t.Fatal("expected a token")
	}
	for _, method := range []string{MethodSubscribeMetadata, MethodSubscribeLocalMetadata} {
		scope, err := CheckGrpcMetadataSubscription(ctxWithBearer(tok), testReadKey, testAdminKey, method)
		if err != nil || len(scope) != 1 || scope[0] != "/buckets/uploads" {
			t.Errorf("%s: scope %v, err %v", method, scope, err)
		}
	}
	if IsFilerAdminToken(testReadKey, tok) || IsFilerAdminToken(testAdminKey, tok) {
		t.Error("a subscription token must never count as an admin token")
	}
}

func TestGenJwtForMetadataSubscriptionRefusesBadInput(t *testing.T) {
	cases := map[string]struct {
		key      SigningKey
		expires  int
		prefixes []string
	}{
		"no key":            {nil, 60, []string{"/a"}},
		"no expiry":         {testReadKey, 0, []string{"/a"}},
		"no prefixes":       {testReadKey, 60, nil},
		"relative prefix":   {testReadKey, 60, []string{"a"}},
		"dot-dot prefix":    {testReadKey, 60, []string{"/a/../b"}},
		"trailing slash":    {testReadKey, 60, []string{"/a/"}},
		"empty prefix":      {testReadKey, 60, []string{""}},
		"one bad of two":    {testReadKey, 60, []string{"/a", "/b/./c"}},
		"double slash":      {testReadKey, 60, []string{"//a"}},
		"negative lifetime": {testReadKey, -1, []string{"/a"}},
	}
	for name, c := range cases {
		if tok := GenJwtForMetadataSubscription(c.key, c.expires, c.prefixes); tok != "" {
			t.Errorf("%s: minted %q", name, tok)
		}
	}
}

func TestCheckGrpcMetadataSubscriptionRefuses(t *testing.T) {
	both := []string{MethodSubscribeMetadata, MethodSubscribeLocalMetadata}
	valid := signSubscribeClaims(t, testReadKey, subscribeClaims([]string{"/a"}, both, time.Minute))
	parts := strings.Split(string(valid), ".")
	tampered := EncodedJwt(parts[0] + "." + parts[1] + "." + strings.Repeat("A", len(parts[2])))

	// A payload changed to widen the scope keeps the original signature.
	widened := signSubscribeClaims(t, testReadKey, subscribeClaims([]string{"/"}, both, time.Minute))
	wp := strings.Split(string(widened), ".")
	forged := EncodedJwt(parts[0] + "." + wp[1] + "." + parts[2])

	sid := subscribeClaims([]string{"/a"}, both, time.Minute)
	sid.SessionId = "session"

	noExp := subscribeClaims([]string{"/a"}, both, 0)
	noExp.ExpiresAt = nil

	notYet := subscribeClaims([]string{"/a"}, both, time.Hour)
	notYet.NotBefore = jwt.NewNumericDate(time.Now().Add(time.Hour))

	none, err := jwt.NewWithClaims(jwt.SigningMethodNone, subscribeClaims([]string{"/a"}, both, time.Minute)).SignedString(jwt.UnsafeAllowNoneSignatureType)
	if err != nil {
		t.Fatal(err)
	}

	cases := map[string]context.Context{
		"no metadata":           context.Background(),
		"no authorization":      metadata.NewIncomingContext(context.Background(), metadata.MD{}),
		"not bearer":            metadata.NewIncomingContext(context.Background(), metadata.Pairs("authorization", "Basic "+string(valid))),
		"wrong signature":       ctxWithBearer(tampered),
		"forged payload":        ctxWithBearer(forged),
		"signed with write key": ctxWithBearer(signSubscribeClaims(t, testAdminKey, subscribeClaims([]string{"/a"}, both, time.Minute))),
		"signed with other key": ctxWithBearer(signSubscribeClaims(t, SigningKey("other"), subscribeClaims([]string{"/a"}, both, time.Minute))),
		"alg none":              ctxWithBearer(EncodedJwt(none)),
		"expired":               ctxWithBearer(signSubscribeClaims(t, testReadKey, subscribeClaims([]string{"/a"}, both, -time.Minute))),
		"no expiry":             ctxWithBearer(signSubscribeClaims(t, testReadKey, noExp)),
		"not yet valid":         ctxWithBearer(signSubscribeClaims(t, testReadKey, notYet)),
		"session token":         ctxWithBearer(signSubscribeClaims(t, testReadKey, sid)),
		"plain read token":      ctxWithBearer(GenJwtForFilerServer(testReadKey, 60)),
		"no prefixes":           ctxWithBearer(signSubscribeClaims(t, testReadKey, subscribeClaims(nil, both, time.Minute))),
		"no methods":            ctxWithBearer(signSubscribeClaims(t, testReadKey, subscribeClaims([]string{"/a"}, nil, time.Minute))),
		"relative prefix":       ctxWithBearer(signSubscribeClaims(t, testReadKey, subscribeClaims([]string{"a"}, both, time.Minute))),
		"dot-dot prefix":        ctxWithBearer(signSubscribeClaims(t, testReadKey, subscribeClaims([]string{"/a/.."}, both, time.Minute))),
		"dot-dot inside":        ctxWithBearer(signSubscribeClaims(t, testReadKey, subscribeClaims([]string{"/a/../etc"}, both, time.Minute))),
		"HTTP read method too":  ctxWithBearer(signSubscribeClaims(t, testReadKey, subscribeClaims([]string{"/a"}, []string{MethodSubscribeMetadata, "GET"}, time.Minute))),
		"write method":          ctxWithBearer(signSubscribeClaims(t, testReadKey, subscribeClaims([]string{"/a"}, []string{"CreateEntry"}, time.Minute))),
		"other call only":       ctxWithBearer(signSubscribeClaims(t, testReadKey, subscribeClaims([]string{"/a"}, []string{MethodSubscribeLocalMetadata}, time.Minute))),
		"unrelated key":         ctxWithBearer(GenJwtForFilerServer(SigningKey("unrelated"), 60)),
	}
	for name, ctx := range cases {
		scope, err := CheckGrpcMetadataSubscription(ctx, testReadKey, testAdminKey, MethodSubscribeMetadata)
		if status.Code(err) != codes.Unauthenticated {
			t.Errorf("%s: scope %v, err %v; want Unauthenticated", name, scope, err)
		}
	}

	// With no read key, no subscription token can be valid.
	if _, err := CheckGrpcMetadataSubscription(ctxWithBearer(valid), nil, testAdminKey, MethodSubscribeMetadata); status.Code(err) != codes.Unauthenticated {
		t.Errorf("no read key: err %v", err)
	}
}

func TestCheckGrpcMetadataSubscriptionAdmin(t *testing.T) {
	admin := GenJwtForFilerAdmin(testAdminKey, 60)
	scope, err := CheckGrpcMetadataSubscription(ctxWithBearer(admin), testReadKey, testAdminKey, MethodSubscribeMetadata)
	if err != nil || scope != nil {
		t.Errorf("admin token: scope %v, err %v; want unrestricted", scope, err)
	}
	// An admin-shaped token signed with the read key is not an admin token.
	readAdmin := GenJwtForFilerAdmin(testReadKey, 60)
	if _, err := CheckGrpcMetadataSubscription(ctxWithBearer(readAdmin), testReadKey, testAdminKey, MethodSubscribeMetadata); err == nil {
		t.Error("an unscoped token signed with the read key must not be unrestricted")
	}
	// A valid token after a stale one still counts.
	scoped := GenJwtForMetadataSubscription(testReadKey, 60, []string{"/a"})
	stale := signSubscribeClaims(t, testReadKey, subscribeClaims([]string{"/"}, []string{MethodSubscribeMetadata}, -time.Minute))
	scope, err = CheckGrpcMetadataSubscription(ctxWithBearer(stale, scoped), testReadKey, testAdminKey, MethodSubscribeMetadata)
	if err != nil || len(scope) != 1 || scope[0] != "/a" {
		t.Errorf("stale then scoped: scope %v, err %v", scope, err)
	}
}

func TestMetadataPathInScope(t *testing.T) {
	prefixes := []string{"/buckets/a"}
	cases := map[string]bool{
		"/buckets/a":            true,
		"/buckets/a/x":          true,
		"/buckets/a/x/y.txt":    true,
		"/buckets/a/":           true,
		"buckets/a/x":           true, // made absolute
		"/buckets/ab":           false,
		"/buckets/ab/x":         false,
		"/buckets/a-private/x":  false,
		"/buckets":              false,
		"/":                     false,
		"":                      false,
		"/buckets/a/../b/x":     false,
		"/buckets/a/x/../../b":  false,
		"/buckets/a/..":         false,
		"/buckets/a/../a/x":     true,
		"/buckets/./a/x":        true,
		"//buckets//a//x":       true,
		"/buckets/a%2F..%2Fb/x": false, // a name starting "a%2F", a sibling of a
		"/buckets/a%2fx":        false,
		"/buckets/a/%2e%2e/x":   true, // a literal "%2e%2e" directory inside a
		"/buckets/a/%2E%2E%2Fb": true,
		"/buckets/%61/x":        false, // %61 is not decoded to a
		"/buckets/a\x00/x":      false,
	}
	for p, want := range cases {
		if got := MetadataPathInScope(p, prefixes); got != want {
			t.Errorf("MetadataPathInScope(%q) = %v, want %v", p, got, want)
		}
	}
	if !MetadataPathInScope("/anything", []string{"/"}) {
		t.Error("the root prefix holds every path")
	}
	if MetadataPathInScope("/c/x", []string{"/a", "/b"}) || !MetadataPathInScope("/b/x", []string{"/a", "/b"}) {
		t.Error("several prefixes are a union")
	}
}
