package weed_server

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/security"
	"google.golang.org/grpc/metadata"
)

func TestWithCallerAuthContextCopiesOnlyIncomingAuthorization(t *testing.T) {
	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs(
		"authorization", "Bearer caller-token",
		"x-unrelated", "must-not-forward",
	))
	ctx = metadata.AppendToOutgoingContext(ctx, "x-amz-request-id", "request-id")

	forwarded := withCallerAuthContext(ctx)
	outgoing, ok := metadata.FromOutgoingContext(forwarded)
	if !ok {
		t.Fatal("caller context lost outgoing metadata")
	}
	if got := outgoing.Get("authorization"); len(got) != 1 || got[0] != "Bearer caller-token" {
		t.Fatalf("authorization = %v, want the caller bearer only", got)
	}
	if got := outgoing.Get("x-amz-request-id"); len(got) != 1 || got[0] != "request-id" {
		t.Fatalf("request id = %v, want the existing outgoing metadata preserved", got)
	}
	if got := outgoing.Get("x-unrelated"); len(got) != 0 {
		t.Fatalf("incoming unrelated metadata leaked to the peer: %v", got)
	}
}

func TestWithCallerAuthContextReplacesExistingAuthorization(t *testing.T) {
	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs("authorization", "Bearer caller-token"))
	ctx = metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer stale-token")

	outgoing, ok := metadata.FromOutgoingContext(withCallerAuthContext(ctx))
	if !ok {
		t.Fatal("caller context lost outgoing metadata")
	}
	if got := outgoing.Get("authorization"); len(got) != 1 || got[0] != "Bearer caller-token" {
		t.Fatalf("authorization = %v, want exactly the caller bearer", got)
	}
}

func TestWithCallerAuthContextRemovesStaleAuthorizationWhenCallerIsTokenless(t *testing.T) {
	ctx := metadata.AppendToOutgoingContext(context.Background(), "authorization", "Bearer stale-token")
	outgoing, ok := metadata.FromOutgoingContext(withCallerAuthContext(ctx))
	if !ok || len(outgoing.Get("authorization")) != 0 {
		t.Fatalf("tokenless caller retained stale authorization: %v", outgoing)
	}
}

func TestWithHTTPBearerContext(t *testing.T) {
	ctx := withHTTPBearerContext(context.Background(), security.EncodedJwt("http-token"))
	outgoing, ok := metadata.FromOutgoingContext(ctx)
	if !ok {
		t.Fatal("HTTP bearer context has no outgoing metadata")
	}
	if got := outgoing.Get("authorization"); len(got) != 1 || got[0] != "Bearer http-token" {
		t.Fatalf("authorization = %v, want the HTTP bearer", got)
	}

	ctx = metadata.AppendToOutgoingContext(context.Background(), "authorization", "Bearer stale-token")
	if outgoing, ok := metadata.FromOutgoingContext(withHTTPBearerContext(ctx, "")); !ok || len(outgoing.Get("authorization")) != 0 {
		t.Fatalf("empty HTTP bearer retained stale authorization: %v", outgoing)
	}
}

func TestWithHTTPForwardAuthContextFollowsRequireMode(t *testing.T) {
	r := httptest.NewRequest(http.MethodPut, "http://filer.test/file?op=append", nil)
	r.Header.Set("Authorization", "Bearer http-token")
	base, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	base = metadata.AppendToOutgoingContext(base, "x-request-id", "request-id", "authorization", "Bearer stale-token")

	for _, tc := range []struct {
		name         string
		requireToken bool
		wantAuth     string
	}{
		{name: "compatibility mode", wantAuth: ""},
		{name: "require mode", requireToken: true, wantAuth: "Bearer http-token"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fs := &FilerServer{}
			fs.grpcScopeSettings.Store(&grpcScopeSettings{requireToken: tc.requireToken})

			forwarded := fs.withHTTPForwardAuthContext(base, r)
			deadline, ok := forwarded.Deadline()
			if !ok {
				t.Fatal("forwarded context lost its deadline")
			}
			wantDeadline, _ := base.Deadline()
			if !deadline.Equal(wantDeadline) {
				t.Fatalf("deadline = %v, want %v", deadline, wantDeadline)
			}
			outgoing, ok := metadata.FromOutgoingContext(forwarded)
			if !ok {
				t.Fatal("forwarded context lost outgoing metadata")
			}
			gotAuth := outgoing.Get("authorization")
			if tc.wantAuth == "" {
				if len(gotAuth) != 0 {
					t.Fatalf("authorization = %v, want no values", gotAuth)
				}
			} else if len(gotAuth) != 1 || gotAuth[0] != tc.wantAuth {
				t.Fatalf("authorization = %v, want exactly %q", gotAuth, tc.wantAuth)
			}
			if got := outgoing.Get("x-request-id"); len(got) != 1 || got[0] != "request-id" {
				t.Fatalf("request id = %v, want request-id", got)
			}
		})
	}
}
