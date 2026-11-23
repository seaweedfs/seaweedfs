package weed_server

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"testing"
	"time"

	jwt "github.com/golang-jwt/jwt/v5"
	"github.com/seaweedfs/seaweedfs/weed/security"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
)

const (
	subscriptionFilerKey = "subscription-filer-key"
	subscriptionTokenKey = "subscription-token-key"
)

func subscriptionScopeServer(withSubscriptionVerifier bool) *filerGrpcScope {
	cfg := scopeConfig{
		verificationKeys: func() (security.SigningKey, security.SigningKey) {
			return security.SigningKey(subscriptionFilerKey), nil
		},
	}
	if withSubscriptionVerifier {
		cfg.subscriptionScope = func(ctx context.Context, method string) (*security.MetadataSubscriptionScope, error) {
			return security.CheckGrpcMetadataSubscription(
				ctx,
				security.SigningKey(subscriptionTokenKey),
				security.SigningKey(subscriptionFilerKey),
				nil,
				method,
			)
		}
	}
	return newFilerGrpcScope(cfg, newScopeFakeFiler())
}

func subscriptionStreamContext(token string) context.Context {
	return metadata.NewIncomingContext(context.Background(), metadata.Pairs("authorization", "Bearer "+token))
}

func subscriptionStreamContextValues(values ...string) context.Context {
	md := metadata.MD{}
	for _, value := range values {
		md.Append("authorization", value)
	}
	return metadata.NewIncomingContext(context.Background(), md)
}

func verifiedSubscriptionContext(token string) context.Context {
	ctx := peer.NewContext(context.Background(), &peer.Peer{
		AuthInfo: credentials.TLSInfo{State: tls.ConnectionState{VerifiedChains: [][]*x509.Certificate{{}}}},
	})
	return metadata.NewIncomingContext(ctx, metadata.Pairs("authorization", "Bearer "+token))
}

func callSubscriptionStream(t *testing.T, scope *filerGrpcScope, method string, ctx context.Context, handler func()) error {
	t.Helper()
	stream := &scopeExpiryServerStream{ctx: ctx}
	return scope.stream(nil, stream, &grpc.StreamServerInfo{
		FullMethod: scopeFilerMethodPrefix + method,
	}, func(any, grpc.ServerStream) error {
		handler()
		return nil
	})
}

func TestScopePreservesDedicatedSubscriptionTokensForBothMethods(t *testing.T) {
	token := string(security.GenJwtForMetadataSubscription(
		security.SigningKey(subscriptionTokenKey),
		60,
		[]string{aliceWS},
	))
	if token == "" {
		t.Fatal("failed to mint dedicated subscription token")
	}
	scope := subscriptionScopeServer(true)
	for _, method := range []string{security.MethodSubscribeMetadata, security.MethodSubscribeLocalMetadata} {
		t.Run(method, func(t *testing.T) {
			called := false
			err := callSubscriptionStream(t, scope, method, subscriptionStreamContext(token), func() { called = true })
			if err != nil || !called {
				t.Fatalf("dedicated subscription token err=%v called=%v, want handler dispatch", err, called)
			}
		})
	}
}

func TestScopeDedicatedSubscriptionTokenSurvivesVerifiedPeerFallback(t *testing.T) {
	validToken := string(security.GenJwtForMetadataSubscription(security.SigningKey(subscriptionTokenKey), 60, []string{aliceWS}))
	if validToken == "" {
		t.Fatal("failed to mint dedicated subscription token")
	}
	wrongToken := string(security.GenJwtForMetadataSubscription(security.SigningKey("wrong-subscription-key"), 60, []string{aliceWS}))
	if wrongToken == "" {
		t.Fatal("failed to mint wrong-key subscription token")
	}

	t.Run("valid token retains original verified peer", func(t *testing.T) {
		scope := subscriptionScopeServer(true)
		stream := &scopeExpiryServerStream{ctx: verifiedSubscriptionContext(validToken)}
		called := false
		var handlerContext context.Context
		err := scope.stream(nil, stream, &grpc.StreamServerInfo{
			FullMethod: scopeFilerMethodPrefix + security.MethodSubscribeMetadata,
		}, func(_ any, handlerStream grpc.ServerStream) error {
			called = true
			handlerContext = handlerStream.Context()
			return nil
		})
		if err != nil || !called {
			t.Fatalf("mTLS dedicated subscription err=%v called=%v, want handler dispatch", err, called)
		}
		peerInfo, ok := peer.FromContext(handlerContext)
		if !ok || peerInfo == nil {
			t.Fatal("handler lost the original peer context")
		}
		if _, ok := peerInfo.AuthInfo.(credentials.TLSInfo); !ok {
			t.Fatalf("handler peer auth info = %T, want verified TLSInfo", peerInfo.AuthInfo)
		}
	})

	t.Run("invalid token is still denied", func(t *testing.T) {
		scope := subscriptionScopeServer(true)
		stream := &scopeExpiryServerStream{ctx: verifiedSubscriptionContext(wrongToken)}
		called := false
		err := scope.stream(nil, stream, &grpc.StreamServerInfo{
			FullMethod: scopeFilerMethodPrefix + security.MethodSubscribeMetadata,
		}, func(any, grpc.ServerStream) error {
			called = true
			return nil
		})
		if status.Code(err) != codes.Unauthenticated || called {
			t.Fatalf("mTLS invalid dedicated subscription err=%v called=%v, want Unauthenticated before dispatch", err, called)
		}
	})
}

func TestScopeRejectsGeneralFilerScopesOnSubscriptionsWithOrWithoutVerifier(t *testing.T) {
	token := string(security.GenJwtForFilerScope(
		security.SigningKey(subscriptionFilerKey),
		time.Minute,
		"general-scope",
		[]string{aliceWS},
		nil,
	))
	if token == "" {
		t.Fatal("failed to mint general filer scope token")
	}
	for _, withVerifier := range []bool{false, true} {
		t.Run(map[bool]string{false: "subscription-auth-disabled", true: "subscription-auth-enabled"}[withVerifier], func(t *testing.T) {
			scope := subscriptionScopeServer(withVerifier)
			for _, method := range []string{security.MethodSubscribeMetadata, security.MethodSubscribeLocalMetadata} {
				t.Run(method, func(t *testing.T) {
					called := false
					err := callSubscriptionStream(t, scope, method, subscriptionStreamContext(token), func() { called = true })
					if status.Code(err) != codes.PermissionDenied || called {
						t.Fatalf("general scoped subscription err=%v called=%v, want PermissionDenied before dispatch", err, called)
					}
				})
			}
		})
	}
}

func TestScopeRejectsMalformedAndInvalidDedicatedSubscriptionTokens(t *testing.T) {
	validToken := string(security.GenJwtForMetadataSubscription(security.SigningKey(subscriptionTokenKey), 60, []string{aliceWS}))
	if validToken == "" {
		t.Fatal("failed to mint valid dedicated subscription token")
	}
	expiredClaims := security.SeaweedFilerClaims{
		AllowedPrefixes: []string{aliceWS},
		AllowedMethods:  []string{security.MethodSubscribeMetadata, security.MethodSubscribeLocalMetadata},
		RegisteredClaims: jwt.RegisteredClaims{
			ExpiresAt: jwt.NewNumericDate(time.Now().Add(-time.Minute)),
		},
	}
	expiredToken, err := jwt.NewWithClaims(jwt.SigningMethodHS256, expiredClaims).SignedString([]byte(subscriptionTokenKey))
	if err != nil {
		t.Fatalf("failed to mint expired dedicated subscription token: %v", err)
	}
	cases := []struct {
		name  string
		token string
		ctx   context.Context
	}{
		{
			name: "malformed authorization",
			ctx:  metadata.NewIncomingContext(context.Background(), metadata.Pairs("authorization", "Basic opaque")),
		},
		{
			name: "duplicate valid authorization",
			ctx:  subscriptionStreamContextValues("Bearer "+validToken, "Bearer "+validToken),
		},
		{
			name:  "wrong signing key",
			token: string(security.GenJwtForMetadataSubscription(security.SigningKey("wrong-subscription-key"), 60, []string{aliceWS})),
		},
		{
			name:  "expired",
			token: expiredToken,
		},
	}
	scope := subscriptionScopeServer(true)
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := tc.ctx
			if ctx == nil {
				if tc.token == "" {
					t.Fatal("test case has neither context nor token")
				}
				ctx = subscriptionStreamContext(tc.token)
			}
			called := false
			err := callSubscriptionStream(t, scope, security.MethodSubscribeMetadata, ctx, func() { called = true })
			if status.Code(err) != codes.Unauthenticated || called {
				t.Fatalf("invalid dedicated subscription err=%v called=%v, want Unauthenticated before dispatch", err, called)
			}
		})
	}
}
