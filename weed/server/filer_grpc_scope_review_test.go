package weed_server

// Regression tests for the scoped filer gRPC service: a forwarded (is_moved)
// delete is refused, a rename into the scope does not leak the source
// directory, the interceptors ignore non-filesystem methods (IAM, reflection),
// and allowed_methods is enforced on gRPC.

import (
	"context"
	"testing"
	"time"

	jwt "github.com/golang-jwt/jwt/v5"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/security"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// A scoped delete that carries is_moved is a forwarded transaction, which a
// scoped client may not originate; create, update and PosixLock already refuse
// it, and delete must too.
func TestScopeDeleteRefusesIsMoved(t *testing.T) {
	h := newScopeHarness(t)
	c := h.alice()

	before := len(h.fake.received("DeleteEntry"))
	_, err := c.DeleteEntry(ctxBG(), &filer_pb.DeleteEntryRequest{Directory: aliceWS, Name: "a", IsMoved: true})
	if status.Code(err) != codes.PermissionDenied {
		t.Fatalf("a forwarded (is_moved) delete must be refused, got %v", err)
	}
	if len(h.fake.received("DeleteEntry")) != before {
		t.Fatal("a refused delete must not reach the filer")
	}

	expectForwarded(t, h, "DeleteEntry", "a plain delete in scope", func() error {
		_, e := c.DeleteEntry(ctxBG(), &filer_pb.DeleteEntryRequest{Directory: aliceWS, Name: "a"})
		return e
	})
}

// A rename from a path the holder cannot see into one it can is delivered as a
// plain create in the visible directory: the event must not carry the source
// directory (resp.Directory) or the source entry (OldEntry).
func TestScopeRenameInEventHidesSourceDirectory(t *testing.T) {
	grants, _ := scopeGrantsFromClaims([]string{aliceWS}, nil)
	sc := &scope{grants: grants}

	in := &filer_pb.SubscribeMetadataResponse{
		Directory: bobWS, // the rename source, outside the scope
		EventNotification: &filer_pb.EventNotification{
			OldEntry:      entryWith("x"),
			NewEntry:      entryWith("x"),
			NewParentPath: aliceWS, // the destination, inside the scope
		},
	}
	out := sc.filterEvent(in)
	if out == nil {
		t.Fatal("a rename into the scope must be delivered")
	}
	if got := out.GetDirectory(); got != aliceWS {
		t.Errorf("event directory = %q, want the visible destination %q (the source must not leak)", got, aliceWS)
	}
	ev := out.GetEventNotification()
	if ev.GetNewParentPath() != "" {
		t.Errorf("NewParentPath = %q, want cleared", ev.GetNewParentPath())
	}
	if ev.GetOldEntry() != nil {
		t.Error("the out-of-scope source entry (OldEntry) must be cleared")
	}
	if ev.GetNewEntry() == nil {
		t.Error("the destination entry (NewEntry) must be kept")
	}
}

// The interceptors chain onto the whole filer gRPC server, which also serves
// the IAM service and reflection. A call outside /filer_pb.SeaweedFiler/ must
// pass through untouched even when a token would not verify under the scope
// key; a filesystem call with the same token is still refused.
func TestScopeInterceptorsIgnoreNonFilerMethods(t *testing.T) {
	h := newScopeHarness(t)
	// A bearer token that does not verify under the scope key: enough to make
	// resolve refuse a filesystem call.
	badCtx := metadata.NewIncomingContext(context.Background(),
		metadata.Pairs("authorization", "Bearer not-a-valid-token"))

	called := false
	unaryHandler := func(ctx context.Context, req any) (any, error) { called = true; return "ok", nil }
	resp, err := h.scope.unary(badCtx, nil,
		&grpc.UnaryServerInfo{FullMethod: "/iam_pb.SeaweedIdentityAccessManagement/GetS3Config"}, unaryHandler)
	if err != nil || resp != "ok" || !called {
		t.Fatalf("a non-filer unary call must pass through: resp=%v err=%v called=%v", resp, err, called)
	}

	called = false
	if _, err := h.scope.unary(badCtx, nil,
		&grpc.UnaryServerInfo{FullMethod: "/filer_pb.SeaweedFiler/CreateEntry"}, unaryHandler); status.Code(err) != codes.Unauthenticated || called {
		t.Fatalf("a filer unary call with an invalid token must be refused, not pass through: err=%v called=%v", err, called)
	}

	called = false
	streamHandler := func(srv any, ss grpc.ServerStream) error { called = true; return nil }
	if err := h.scope.stream(nil, &nonFilerStream{ctx: badCtx},
		&grpc.StreamServerInfo{FullMethod: "/grpc.reflection.v1.ServerReflection/ServerReflectionInfo"}, streamHandler); err != nil || !called {
		t.Fatalf("a non-filer stream must pass through: err=%v called=%v", err, called)
	}
}

type nonFilerStream struct{ ctx context.Context }

func (s *nonFilerStream) SetHeader(metadata.MD) error  { return nil }
func (s *nonFilerStream) SendHeader(metadata.MD) error { return nil }
func (s *nonFilerStream) SetTrailer(metadata.MD)       {}
func (s *nonFilerStream) Context() context.Context     { return s.ctx }
func (s *nonFilerStream) SendMsg(any) error            { return nil }
func (s *nonFilerStream) RecvMsg(any) error            { return nil }

// methodScopedToken mints a token carrying allowed_methods (and prefixes).
func methodScopedToken(t *testing.T, id string, prefixes, methods []string) string {
	t.Helper()
	claims := security.SeaweedFilerClaims{
		AllowedPrefixes: prefixes,
		AllowedMethods:  methods,
		RegisteredClaims: jwt.RegisteredClaims{
			ID:        id,
			ExpiresAt: jwt.NewNumericDate(time.Now().Add(time.Hour)),
		},
	}
	s, err := jwt.NewWithClaims(jwt.SigningMethodHS256, claims).SignedString([]byte(scopeTestKey))
	if err != nil {
		t.Fatalf("sign method token: %v", err)
	}
	return s
}

// A token restricted to the subscription methods (as the metadata aggregator
// mints) may open a subscription inside its prefixes but may not mutate an
// entry there; a token without allowed_methods is unrestricted by this rule.
func TestScopeMethodRestrictionEnforced(t *testing.T) {
	h := newScopeHarness(t)

	subOnly := methodScopedToken(t, "sub", []string{aliceWS},
		[]string{security.MethodSubscribeMetadata, security.MethodSubscribeLocalMetadata})
	c := h.client(subOnly)

	before := len(h.fake.received("CreateEntry"))
	_, err := c.CreateEntry(ctxBG(), &filer_pb.CreateEntryRequest{Directory: aliceWS, Entry: &filer_pb.Entry{Name: "f"}})
	if status.Code(err) != codes.PermissionDenied {
		t.Fatalf("a subscription-only token must not create entries, got %v", err)
	}
	if len(h.fake.received("CreateEntry")) != before {
		t.Fatal("a refused create must not reach the filer")
	}

	// The same token may open a subscription inside its prefix: the call
	// reaches the filer rather than being refused by the method gate. Use a
	// bounded context and assert the handler saw it, so no Recv blocks.
	if _, err := c.SubscribeMetadata(scopeCtx(t), &filer_pb.SubscribeMetadataRequest{PathPrefix: aliceWS + "/"}); err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	waitFor(t, "the subscription-only token to reach the filer", func() bool {
		return len(h.fake.received("SubscribeMetadata")) == 1
	})

	// A token with prefixes but no allowed_methods keeps full method access.
	expectForwarded(t, h, "CreateEntry", "a token without allowed_methods creates in scope", func() error {
		_, e := h.alice().CreateEntry(ctxBG(), &filer_pb.CreateEntryRequest{Directory: aliceWS, Entry: &filer_pb.Entry{Name: "g"}})
		return e
	})
}
