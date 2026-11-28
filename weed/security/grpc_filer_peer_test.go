package security

import (
	"context"
	"crypto/rand"
	"testing"
	"time"

	"google.golang.org/grpc/metadata"
)

func TestFilerPeerCredentialsPerCall(t *testing.T) {
	key := make([]byte, 32)
	if _, err := rand.Read(key); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	original := metadata.Pairs("authorization", "stale", "authorization", "other", "trace-id", "test-trace")
	ctx = metadata.NewOutgoingContext(ctx, original)
	ctx = metadata.NewIncomingContext(ctx, metadata.Pairs("authorization", "caller-token"))
	before := time.Now()
	peerCtx := WithFilerPeerCredentials(ctx, key)
	md, _ := metadata.FromOutgoingContext(peerCtx)
	if got := md.Get("authorization"); len(got) != 1 {
		t.Fatalf("authorization count=%d, want one", len(got))
	}
	if md.Get("trace-id")[0] != "test-trace" || len(original.Get("authorization")) != 2 {
		t.Fatal("peer credentials changed unrelated metadata or mutated the caller's metadata")
	}
	if err := CheckGrpcAdminToken(metadata.NewIncomingContext(context.Background(), md), key); err != nil {
		t.Fatalf("peer token was not accepted as admin: %v", err)
	}
	claims := &SeaweedFilerClaims{}
	raw := EncodedJwt(md.Get("authorization")[0][len("Bearer "):])
	if _, err := DecodeJwt(key, raw, claims); err != nil {
		t.Fatalf("decode peer token: %v", err)
	}
	if claims.ExpiresAt == nil || claims.ExpiresAt.Time.Before(before.Add(9*time.Second)) || claims.ExpiresAt.Time.After(time.Now().Add(10*time.Second)) {
		t.Fatal("peer token does not have the fixed ten-second lifetime")
	}
	deadline, ok := peerCtx.Deadline()
	wantDeadline, _ := ctx.Deadline()
	if !ok || !deadline.Equal(wantDeadline) {
		t.Fatal("peer metadata changed the deadline")
	}
	cleared, _ := metadata.FromOutgoingContext(WithFilerPeerCredentials(peerCtx, nil))
	if len(cleared.Get("authorization")) != 0 || cleared.Get("trace-id")[0] != "test-trace" {
		t.Fatal("key removal did not remove only the authorization")
	}
	cancel()
	if peerCtx.Err() != context.Canceled {
		t.Fatal("peer metadata detached cancellation")
	}
}
