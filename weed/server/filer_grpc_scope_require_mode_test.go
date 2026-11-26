package weed_server

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/security"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"github.com/spf13/viper"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
)

func tokenlessVerifiedPeerContext() context.Context {
	return peer.NewContext(context.Background(), &peer.Peer{
		AuthInfo: credentials.TLSInfo{State: tls.ConnectionState{VerifiedChains: [][]*x509.Certificate{{}}}},
	})
}

func TestFilerGrpcRequireTokenConfigDefaultsOffAndReloads(t *testing.T) {
	fs := &FilerServer{filerGuard: security.NewGuard(nil, "http-key", 10, "", 0, "", "")}
	config := util.NewViperProxy(viper.New())
	config.Set("jwt.filer_signing.grpc_key", scopeTestKey)

	fs.loadGrpcScopeConfig(config)
	if got := fs.grpcScopeSettings.Load().requireToken; got {
		t.Fatal("grpc_require_token defaulted on")
	}

	config.Set("jwt.filer_signing.grpc_require_token", true)
	fs.loadGrpcScopeConfig(config)
	if got := fs.grpcScopeSettings.Load().requireToken; !got {
		t.Fatal("grpc_require_token reload did not enable require-mode")
	}
}

func TestFilerGrpcRequireTokenRefusesTokenlessUnaryAndStreamingCalls(t *testing.T) {
	h := newScopeHarness(t)
	h.scope.cfg.requireToken = func() bool { return true }
	c := h.client("")

	beforeUnary := len(h.fake.received("LookupDirectoryEntry"))
	_, err := c.LookupDirectoryEntry(scopeCtx(t), &filer_pb.LookupDirectoryEntryRequest{Directory: "/", Name: "file"})
	if status.Code(err) != codes.Unauthenticated {
		t.Fatalf("tokenless unary error=%v, want Unauthenticated", err)
	}
	if got := len(h.fake.received("LookupDirectoryEntry")); got != beforeUnary {
		t.Fatalf("tokenless unary reached handler: %d calls before=%d", got, beforeUnary)
	}

	beforeStream := len(h.fake.received("ListEntries"))
	stream, err := c.ListEntries(scopeCtx(t), &filer_pb.ListEntriesRequest{Directory: "/"})
	if err == nil {
		_, err = stream.Recv()
	}
	if status.Code(err) != codes.Unauthenticated {
		t.Fatalf("tokenless stream error=%v, want Unauthenticated", err)
	}
	if got := len(h.fake.received("ListEntries")); got != beforeStream {
		t.Fatalf("tokenless stream reached handler: %d calls before=%d", got, beforeStream)
	}
}

func TestFilerGrpcRequireTokenRefusesTokenlessVerifiedMTLSUnaryAndStreaming(t *testing.T) {
	scope := newFilerGrpcScope(scopeConfig{
		verificationKeys: func() (security.SigningKey, security.SigningKey) {
			return security.SigningKey(scopeTestKey), nil
		},
		requireToken: func() bool { return true },
	}, newScopeFakeFiler())
	ctx := tokenlessVerifiedPeerContext()

	calledUnary := false
	_, err := scope.unary(ctx, &filer_pb.PingRequest{}, &grpc.UnaryServerInfo{
		FullMethod: filer_pb.SeaweedFiler_Ping_FullMethodName,
	}, func(context.Context, any) (any, error) {
		calledUnary = true
		return &filer_pb.PingResponse{}, nil
	})
	if status.Code(err) != codes.Unauthenticated || calledUnary {
		t.Fatalf("tokenless verified mTLS unary error=%v called=%v, want Unauthenticated before dispatch", err, calledUnary)
	}

	calledStream := false
	err = scope.stream(nil, &scopeExpiryServerStream{ctx: ctx}, &grpc.StreamServerInfo{
		FullMethod: filer_pb.SeaweedFiler_ListEntries_FullMethodName,
	}, func(any, grpc.ServerStream) error {
		calledStream = true
		return nil
	})
	if status.Code(err) != codes.Unauthenticated || calledStream {
		t.Fatalf("tokenless verified mTLS stream error=%v called=%v, want Unauthenticated before dispatch", err, calledStream)
	}
}

func TestFilerGrpcRequireTokenFailsClosedWithoutVerificationKey(t *testing.T) {
	scope := newFilerGrpcScope(scopeConfig{
		verificationKeys: func() (security.SigningKey, security.SigningKey) { return nil, nil },
		requireToken:     func() bool { return true },
	}, newScopeFakeFiler())
	called := false
	_, err := scope.unary(context.Background(), &filer_pb.PingRequest{}, &grpc.UnaryServerInfo{
		FullMethod: filer_pb.SeaweedFiler_Ping_FullMethodName,
	}, func(context.Context, any) (any, error) {
		called = true
		return &filer_pb.PingResponse{}, nil
	})
	if status.Code(err) != codes.Unauthenticated || called {
		t.Fatalf("missing-key require-mode error=%v called=%v, want Unauthenticated before dispatch", err, called)
	}
}

func TestFilerGrpcRequireTokenDoesNotBypassTokenlessMTLSSubscription(t *testing.T) {
	scope := subscriptionScopeServer(true)
	scope.cfg.requireToken = func() bool { return true }
	called := false
	err := callSubscriptionStream(t, scope, security.MethodSubscribeMetadata, tokenlessVerifiedPeerContext(), func() {
		called = true
	})
	if status.Code(err) != codes.Unauthenticated || called {
		t.Fatalf("tokenless verified mTLS subscription error=%v called=%v, want Unauthenticated before dispatch", err, called)
	}
}

func TestFilerGrpcRequireTokenPreservesDedicatedSubscriptionTokenFallback(t *testing.T) {
	scope := subscriptionScopeServer(true)
	scope.cfg.requireToken = func() bool { return true }
	token := string(security.GenJwtForMetadataSubscription(security.SigningKey(subscriptionTokenKey), 60, []string{aliceWS}))
	for _, method := range []string{security.MethodSubscribeMetadata, security.MethodSubscribeLocalMetadata} {
		for _, transport := range []struct {
			name string
			ctx  func(string) context.Context
		}{
			{"plain", subscriptionStreamContext},
			{"mtls", verifiedSubscriptionContext},
		} {
			for _, bearer := range []struct {
				name, token string
				allowed     bool
			}{
				{"valid", token, true},
				{"invalid", "invalid", false},
				{"missing", "", false},
			} {
				t.Run(method+"/"+transport.name+"/"+bearer.name, func(t *testing.T) {
					ctx := transport.ctx(bearer.token)
					called := false
					err := scope.stream(nil, &scopeExpiryServerStream{ctx: ctx}, &grpc.StreamServerInfo{
						FullMethod: scopeFilerMethodPrefix + method,
					}, func(_ any, stream grpc.ServerStream) error {
						called = true
						if stream.Context() != ctx {
							t.Fatal("subscription admission replaced the handler's transport context")
						}
						return nil
					})
					if bearer.allowed {
						if err != nil || !called {
							t.Fatalf("valid subscription bearer error=%v called=%v, want dispatch", err, called)
						}
					} else if status.Code(err) != codes.Unauthenticated || called {
						t.Fatalf("invalid or missing subscription bearer error=%v called=%v, want rejection", err, called)
					}
				})
			}
		}
	}
}

func TestFilerGrpcRequireTokenRejectsInvalidBearerWithPermissiveSubscriptionVerifier(t *testing.T) {
	scope := subscriptionScopeServerWithKeys(true, security.SigningKey(subscriptionFilerKey), nil, nil)
	scope.cfg.requireToken = func() bool { return true }
	called := false
	err := callSubscriptionStream(t, scope, security.MethodSubscribeMetadata, subscriptionStreamContext("invalid"), func() {
		called = true
	})
	if status.Code(err) != codes.Unauthenticated || called {
		t.Fatalf("invalid bearer with permissive subscription verifier error=%v called=%v, want Unauthenticated before dispatch", err, called)
	}
}

func TestFilerGrpcRequireTokenSubscriptionKeepsTokenScopeOverMtls(t *testing.T) {
	v := util.GetViper()
	v.Set("jwt.filer_signing.subscribe.key", subscriptionTokenKey)
	t.Cleanup(func() { v.Set("jwt.filer_signing.subscribe.key", "") })
	fs := &FilerServer{}
	fs.grpcScopeSettings.Store(&grpcScopeSettings{requireToken: true})
	token := string(security.GenJwtForMetadataSubscription(security.SigningKey(subscriptionTokenKey), 60, []string{aliceWS}))
	for _, method := range []string{security.MethodSubscribeMetadata, security.MethodSubscribeLocalMetadata} {
		scope, err := fs.metadataSubscriptionScope(verifiedSubscriptionContext(token), method)
		if err != nil || scope == nil || len(scope.Prefixes) != 1 || scope.Prefixes[0] != aliceWS {
			t.Fatalf("%s over mTLS in require-mode: scope=%+v err=%v, want the token's prefixes", method, scope, err)
		}
		if scope, err := fs.metadataSubscriptionScope(verifiedSubscriptionContext("not-a-subscription-token"), method); err != nil || scope != nil {
			t.Fatalf("%s over mTLS with another bearer: scope=%+v err=%v, want the mTLS full stream", method, scope, err)
		}
	}
}
