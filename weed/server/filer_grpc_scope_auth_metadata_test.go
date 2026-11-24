package weed_server

import (
	"context"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/security"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

func authMetadataContext(values ...string) context.Context {
	if len(values) == 0 {
		return context.Background()
	}
	md := metadata.MD{}
	for _, value := range values {
		md.Append("authorization", value)
	}
	return metadata.NewIncomingContext(context.Background(), md)
}

func TestScopeRejectsMalformedOrDuplicateAuthorizationWithSigningKey(t *testing.T) {
	scope := newFilerGrpcScope(scopeConfig{
		signingKey: func() security.SigningKey { return security.SigningKey(scopeTestKey) },
	}, newScopeFakeFiler())
	cases := []struct {
		name string
		ctx  context.Context
	}{
		{name: "malformed scheme", ctx: authMetadataContext("Basic opaque")},
		{name: "bare bearer", ctx: authMetadataContext("Bearer")},
		{name: "empty bearer", ctx: authMetadataContext("Bearer ")},
		{name: "duplicate valid", ctx: authMetadataContext("Bearer "+unscopedToken(), "Bearer "+unscopedToken())},
		{name: "malformed plus valid", ctx: authMetadataContext("Basic opaque", "Bearer "+unscopedToken())},
	}
	for _, tc := range cases {
		t.Run(tc.name+" unary", func(t *testing.T) {
			called := false
			_, err := scope.unary(tc.ctx, &filer_pb.PingRequest{}, &grpc.UnaryServerInfo{
				FullMethod: filer_pb.SeaweedFiler_Ping_FullMethodName,
			}, func(context.Context, any) (any, error) {
				called = true
				return &filer_pb.PingResponse{}, nil
			})
			if status.Code(err) != codes.Unauthenticated || called {
				t.Fatalf("unary err=%v called=%v, want Unauthenticated before dispatch", err, called)
			}
		})
		t.Run(tc.name+" stream", func(t *testing.T) {
			called := false
			stream := &scopeExpiryServerStream{ctx: tc.ctx}
			err := scope.stream(nil, stream, &grpc.StreamServerInfo{
				FullMethod: filer_pb.SeaweedFiler_ListEntries_FullMethodName,
			}, func(any, grpc.ServerStream) error {
				called = true
				return nil
			})
			if status.Code(err) != codes.Unauthenticated || called {
				t.Fatalf("stream err=%v called=%v, want Unauthenticated before dispatch", err, called)
			}
		})
	}
}

func TestScopeAcceptsAbsentAuthorizationWithSigningKey(t *testing.T) {
	scope := newFilerGrpcScope(scopeConfig{
		signingKey: func() security.SigningKey { return security.SigningKey(scopeTestKey) },
	}, newScopeFakeFiler())

	calledUnary := false
	if _, err := scope.unary(context.Background(), &filer_pb.PingRequest{}, &grpc.UnaryServerInfo{
		FullMethod: filer_pb.SeaweedFiler_Ping_FullMethodName,
	}, func(context.Context, any) (any, error) {
		calledUnary = true
		return &filer_pb.PingResponse{}, nil
	}); err != nil || !calledUnary {
		t.Fatalf("absent unary authorization err=%v called=%v, want passthrough", err, calledUnary)
	}

	calledStream := false
	err := scope.stream(nil, &scopeExpiryServerStream{ctx: context.Background()}, &grpc.StreamServerInfo{
		FullMethod: filer_pb.SeaweedFiler_ListEntries_FullMethodName,
	}, func(any, grpc.ServerStream) error {
		calledStream = true
		return nil
	})
	if err != nil || !calledStream {
		t.Fatalf("absent stream authorization err=%v called=%v, want passthrough", err, calledStream)
	}
}

func TestScopeKeylessSigningBypassesAuthorizationParsing(t *testing.T) {
	scope := newFilerGrpcScope(scopeConfig{
		signingKey: func() security.SigningKey { return nil },
	}, newScopeFakeFiler())
	cases := []struct {
		name string
		ctx  context.Context
	}{
		{name: "absent", ctx: context.Background()},
		{name: "malformed", ctx: authMetadataContext("Basic opaque")},
		{name: "bare bearer", ctx: authMetadataContext("Bearer")},
		{name: "empty bearer", ctx: authMetadataContext("Bearer ")},
		{name: "duplicate", ctx: authMetadataContext("Bearer one", "Bearer two")},
	}
	for _, tc := range cases {
		t.Run(tc.name+" unary", func(t *testing.T) {
			called := false
			_, err := scope.unary(tc.ctx, &filer_pb.PingRequest{}, &grpc.UnaryServerInfo{
				FullMethod: filer_pb.SeaweedFiler_Ping_FullMethodName,
			}, func(context.Context, any) (any, error) {
				called = true
				return &filer_pb.PingResponse{}, nil
			})
			if err != nil || !called {
				t.Fatalf("keyless unary err=%v called=%v, want passthrough", err, called)
			}
		})
		t.Run(tc.name+" stream", func(t *testing.T) {
			called := false
			err := scope.stream(nil, &scopeExpiryServerStream{ctx: tc.ctx}, &grpc.StreamServerInfo{
				FullMethod: filer_pb.SeaweedFiler_ListEntries_FullMethodName,
			}, func(any, grpc.ServerStream) error {
				called = true
				return nil
			})
			if err != nil || !called {
				t.Fatalf("keyless stream err=%v called=%v, want passthrough", err, called)
			}
		})
	}
}
