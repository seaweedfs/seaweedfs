package weed_server

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

func TestScopeAllowedMethodsRestrictCallsBeforeDispatch(t *testing.T) {
	h := newScopeHarness(t)
	token := methodScopedToken(t, "alice", []string{aliceWS}, []string{"LookupDirectoryEntry"})
	c := h.client(token)

	expectScopeForwarded(t, h, "LookupDirectoryEntry", "the listed method", func() error {
		_, err := c.LookupDirectoryEntry(scopeCtx(t), &filer_pb.LookupDirectoryEntryRequest{Directory: aliceWS, Name: "file"})
		return err
	})
	expectScopeDenied(t, h, "CreateEntry", "an unlisted method", func() error {
		_, err := c.CreateEntry(scopeCtx(t), &filer_pb.CreateEntryRequest{Directory: aliceWS, Entry: entryWith("file")})
		return err
	})
}

func TestScopeMethodOnlyTokenAllowsListedMethodWithoutPathGrant(t *testing.T) {
	h := newScopeHarness(t)
	c := h.client(methodScopedToken(t, "alice", nil, []string{"Ping"}))

	expectScopeForwarded(t, h, "Ping", "method-only Ping", func() error {
		_, err := c.Ping(scopeCtx(t), &filer_pb.PingRequest{})
		return err
	})
	expectScopeDenied(t, h, "LookupDirectoryEntry", "method-only lookup without a path grant", func() error {
		_, err := c.LookupDirectoryEntry(scopeCtx(t), &filer_pb.LookupDirectoryEntryRequest{Directory: aliceWS, Name: "file"})
		return err
	})
}

func TestScopeRejectsInvalidScopedClaimsBeforeDispatch(t *testing.T) {
	h := newScopeHarness(t)
	future := jwt.NewNumericDate(time.Now().Add(time.Hour))
	cases := []struct {
		name     string
		token    string
		wantCode codes.Code
	}{
		{
			name: "missing jti",
			token: signedScopeClaims(t, scopeTestKey, security.SeaweedFilerClaims{
				AllowedPrefixes:  []string{aliceWS},
				RegisteredClaims: jwt.RegisteredClaims{ExpiresAt: future},
			}),
			wantCode: codes.PermissionDenied,
		},
		{
			name: "missing expiry",
			token: signedScopeClaims(t, scopeTestKey, security.SeaweedFilerClaims{
				AllowedPrefixes:  []string{aliceWS},
				RegisteredClaims: jwt.RegisteredClaims{ID: "lease-1"},
			}),
			wantCode: codes.PermissionDenied,
		},
		{
			name: "expired",
			token: signedScopeClaims(t, scopeTestKey, security.SeaweedFilerClaims{
				AllowedPrefixes:  []string{aliceWS},
				RegisteredClaims: jwt.RegisteredClaims{ID: "lease-1", ExpiresAt: jwt.NewNumericDate(time.Now().Add(-time.Minute))},
			}),
			wantCode: codes.Unauthenticated,
		},
		{
			name: "wrong signature",
			token: signedScopeClaims(t, "wrong-scope-key", security.SeaweedFilerClaims{
				AllowedPrefixes:  []string{aliceWS},
				RegisteredClaims: jwt.RegisteredClaims{ID: "lease-1", ExpiresAt: future},
			}),
			wantCode: codes.Unauthenticated,
		},
		{
			name: "STS session",
			token: signedScopeClaims(t, scopeTestKey, security.SeaweedFilerClaims{
				AllowedPrefixes: []string{aliceWS},
				SessionId:       "session-1",
				RegisteredClaims: jwt.RegisteredClaims{
					ID:        "lease-1",
					ExpiresAt: future,
				},
			}),
			wantCode: codes.Unauthenticated,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			before := len(h.fake.received("LookupDirectoryEntry"))
			_, err := h.client(tc.token).LookupDirectoryEntry(scopeCtx(t), &filer_pb.LookupDirectoryEntryRequest{Directory: aliceWS, Name: "file"})
			expectScopeCode(t, tc.name, err, tc.wantCode)
			if got := len(h.fake.received("LookupDirectoryEntry")); got != before {
				t.Fatalf("invalid token reached handler: %d calls before=%d", got, before)
			}
		})
	}
}

func TestScopeAssignVolumeAllowedAndMountRegisterRefused(t *testing.T) {
	h := newScopeHarness(t)
	c := h.client(scopedToken("alice", time.Hour, []string{aliceWS}, nil))

	expectScopeForwarded(t, h, "AssignVolume", "AssignVolume for an in-scope path", func() error {
		_, err := c.AssignVolume(scopeCtx(t), &filer_pb.AssignVolumeRequest{Path: aliceWS + "/new"})
		return err
	})
	expectScopeDenied(t, h, "MountRegister", "MountRegister", func() error {
		_, err := c.MountRegister(scopeCtx(t), &filer_pb.MountRegisterRequest{PeerAddr: "127.0.0.1:1"})
		return err
	})

	if _, err := h.client(unscopedToken()).MountRegister(scopeCtx(t), &filer_pb.MountRegisterRequest{PeerAddr: "127.0.0.1:1"}); err != nil {
		t.Fatalf("unscoped MountRegister: %v", err)
	}
	if got := len(h.fake.received("MountRegister")); got != 1 {
		t.Fatalf("unscoped MountRegister reached the handler %d times, want 1", got)
	}
}

func TestScopeSubscriptionMethodsRequireMethodGrant(t *testing.T) {
	h := newScopeHarness(t)
	c := h.client(methodScopedToken(t, "alice", []string{aliceWS}, []string{"ListEntries"}))

	for _, tc := range []struct {
		name   string
		method string
		call   func() error
	}{
		{
			name:   "SubscribeMetadata",
			method: "SubscribeMetadata",
			call: func() error {
				stream, err := c.SubscribeMetadata(scopeCtx(t), &filer_pb.SubscribeMetadataRequest{PathPrefix: aliceWS})
				if err == nil {
					_, err = stream.Recv()
				}
				return err
			},
		},
		{
			name:   "SubscribeLocalMetadata",
			method: "SubscribeLocalMetadata",
			call: func() error {
				stream, err := c.SubscribeLocalMetadata(scopeCtx(t), &filer_pb.SubscribeMetadataRequest{PathPrefix: aliceWS})
				if err == nil {
					_, err = stream.Recv()
				}
				return err
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			expectScopeDenied(t, h, tc.method, tc.name, tc.call)
		})
	}
}

func TestScopeUnknownMethodAliasIsRefusedEvenForKnownRequestType(t *testing.T) {
	h := newScopeHarness(t)
	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs("authorization", "Bearer "+scopedToken("alice", time.Hour, []string{aliceWS}, nil)))
	called := false
	_, err := h.scope.unary(ctx, &filer_pb.LookupDirectoryEntryRequest{Directory: aliceWS, Name: "file"}, &grpc.UnaryServerInfo{FullMethod: scopeFilerMethodPrefix + "LookupDirectoryEntryAlias"}, func(context.Context, any) (any, error) {
		called = true
		return &filer_pb.LookupDirectoryEntryResponse{}, nil
	})
	if status.Code(err) != codes.PermissionDenied || called {
		t.Fatalf("unknown method alias got err=%v called=%v, want PermissionDenied before dispatch", err, called)
	}
}

func TestScopeInterceptorsPassNonFilerMethodsThrough(t *testing.T) {
	h := newScopeHarness(t)
	called := false
	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs("authorization", "Bearer not-a-filer-token"))
	resp, err := h.scope.unary(ctx, nil, &grpc.UnaryServerInfo{FullMethod: "/iam_pb.SeaweedIdentityAccessManagement/GetS3Config"}, func(context.Context, any) (any, error) {
		called = true
		return "ok", nil
	})
	if err != nil || resp != "ok" || !called {
		t.Fatalf("non-filer method was not passed through: response=%v error=%v called=%v", resp, err, called)
	}
}
