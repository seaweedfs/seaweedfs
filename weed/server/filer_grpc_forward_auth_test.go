package weed_server

import (
	"context"
	"testing"

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
