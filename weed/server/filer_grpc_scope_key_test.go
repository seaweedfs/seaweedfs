package weed_server

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/security"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

func TestGrpcScopeSigningKey(t *testing.T) {
	cases := []struct {
		name, grpcKey, filerKey, want string
	}{
		{"grpc_key wins", "g", "f", "g"},
		{"falls back to key", "", "f", "f"},
		{"grpc_key alone", "g", "", "g"},
		{"neither", "", "", ""},
	}
	for _, c := range cases {
		if got := string(grpcScopeSigningKey(c.grpcKey, security.SigningKey(c.filerKey))); got != c.want {
			t.Errorf("%s: got %q want %q", c.name, got, c.want)
		}
	}
}

// With only grpc_key configured, the filesystem gRPC service enforces scoped
// tokens signed with it, refuses tokens signed with another key, and (outside
// require mode) still serves a call without a token; the HTTP guard, built
// from jwt.filer_signing.key, stays open for writes as before.
func TestGrpcKeyAloneEnforcesScopeAndLeavesHttpOpen(t *testing.T) {
	const grpcKey = "grpc-only-key"
	v := util.GetViper()
	v.Set("jwt.filer_signing.grpc_key", grpcKey)
	t.Cleanup(func() { v.Set("jwt.filer_signing.grpc_key", "") })
	// The filer as NewFilerServer wires it, with jwt.filer_signing.key unset.
	fs := &FilerServer{filerGuard: security.NewGuard([]string{}, "", 10, "", 60)}
	fs.loadGrpcScopeConfig(v)
	grpcScope := newFilerGrpcScope(scopeConfig{
		signingKey: func() security.SigningKey { return grpcScopeSigningKey(*fs.grpcKey.Load(), fs.filerGuard.SigningKey()) },
		require:    fs.grpcRequireToken.Load,
	}, nil)
	call := func(token string) (*scope, error) {
		ctx := context.Background()
		if token != "" {
			ctx = metadata.NewIncomingContext(ctx, metadata.Pairs("authorization", "Bearer "+token))
		}
		return grpcScope.resolve(ctx)
	}
	scoped := string(security.GenJwtForFilerScope(security.SigningKey(grpcKey), time.Minute, "lease-1", []string{"/buckets/t/u"}, nil))
	sc, err := call(scoped)
	if err != nil || sc == nil || sc.id != "lease-1" {
		t.Fatalf("token signed with grpc_key is a scope: %v %v", sc, err)
	}
	other := string(security.GenJwtForFilerScope(security.SigningKey("other-key"), time.Minute, "lease-1", []string{"/buckets/t/u"}, nil))
	if _, err := call(other); status.Code(err) != codes.Unauthenticated {
		t.Fatalf("token signed with another key is refused: %v", err)
	}
	if sc, err := call(""); err != nil || sc != nil {
		t.Fatalf("no token outside require mode is served unscoped: %v %v", sc, err)
	}
	write := httptest.NewRequest(http.MethodPost, "/buckets/t/u/file.txt", nil)
	if !fs.maybeCheckJwtAuthorization(write, true) {
		t.Fatal("grpc_key must not make the HTTP API demand a JWT on writes")
	}
}
