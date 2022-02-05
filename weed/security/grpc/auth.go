package grpc

import (
	"context"

	grpc_auth "github.com/grpc-ecosystem/go-grpc-middleware/auth"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/util"
)

const (
	authConfigSection = "grpc.auth"
	authMechanismKey  = "mech"

	// Client authenticates by carrying an :authorization header in the metadata.
	// e.g., authorization: "bearer <token>"
	authMechanismBearer = "bearer"

	headerAuthorize = "authorization"
)

type makeServerAuthFunc func(config *util.ViperProxy) grpc_auth.AuthFunc

var mechToServerMakeAuthFunc = map[string]makeServerAuthFunc{
	authMechanismBearer: makeServerBearerAuthFunc,
}

type makeClientAuthFunc func(config *util.ViperProxy) grpc.UnaryClientInterceptor

var mechToClientMakeAuthFunc = map[string]makeClientAuthFunc{
	authMechanismBearer: makeClientBearerAuthFunc,
}

func LoadServerAuthMiddleware(config *util.ViperProxy) grpc.ServerOption {
	if config == nil {
		return nil
	}

	mech := getAuthOption(config, authMechanismKey)
	if len(mech) == 0 {
		return nil
	}

	makeFunc, ok := mechToServerMakeAuthFunc[mech]
	if !ok {
		glog.V(1).Infof("invalid grpc auth mechanism: %v", mech)
		return nil
	}

	authFunc := makeFunc(config)
	if authFunc == nil {
		return nil
	}

	return grpc.UnaryInterceptor(grpc_auth.UnaryServerInterceptor(authFunc))
}

func LoadClientAuthInterceptor(config *util.ViperProxy) grpc.DialOption {
	if config == nil {
		return nil
	}

	mech := getAuthOption(config, authMechanismKey)
	if len(mech) == 0 {
		return nil
	}

	makeFunc, ok := mechToClientMakeAuthFunc[mech]
	if !ok {
		glog.V(1).Infof("invalid grpc auth mechanism: %v", mech)
		return nil
	}

	authFunc := makeFunc(config)
	if authFunc == nil {
		return nil
	}

	return grpc.WithUnaryInterceptor(authFunc)
}

func getAuthOption(config *util.ViperProxy, option string) string {
	return config.GetString(authConfigSection + "." + option)
}

func makeServerBearerAuthFunc(config *util.ViperProxy) grpc_auth.AuthFunc {
	expectToken := getAuthOption(config, "token")
	if len(expectToken) == 0 {
		return nil
	}

	return func(ctx context.Context) (context.Context, error) {
		token, err := grpc_auth.AuthFromMD(ctx, authMechanismBearer)
		if err != nil {
			return nil, err
		}

		if token != expectToken {
			return nil, status.Error(codes.Unauthenticated, "invalid auth token")
		}

		return ctx, nil
	}
}

func makeClientBearerAuthFunc(config *util.ViperProxy) grpc.UnaryClientInterceptor {
	expectToken := getAuthOption(config, "token")
	if len(expectToken) == 0 {
		return nil
	}

	return func(ctx context.Context, method string, req, reply interface{},
		cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {

		token := authMechanismBearer + " " + expectToken
		ctx = metadata.AppendToOutgoingContext(ctx, headerAuthorize, token)

		err := invoker(ctx, method, req, reply, cc, opts...)
		return err
	}
}
