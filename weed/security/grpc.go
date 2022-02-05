package security

import (
	"google.golang.org/grpc"

	grpc_sec "github.com/chrislusf/seaweedfs/weed/security/grpc"
	"github.com/chrislusf/seaweedfs/weed/util"
)

func LoadGrpcServerOptions(config *util.ViperProxy, component string) []grpc.ServerOption {
	var options []grpc.ServerOption

	creds, allowedCommonNames := grpc_sec.LoadServerTLS(config, component)
	options = append(options, creds, allowedCommonNames)

	authMiddleware := grpc_sec.LoadServerAuthMiddleware(config)
	options = append(options, authMiddleware)

	return options
}

func LoadGrpcClientOptions(config *util.ViperProxy, component string) []grpc.DialOption {
	var options []grpc.DialOption

	tlsOption := grpc_sec.LoadClientTLS(config, component)
	options = append(options, tlsOption)

	authInterceptor := grpc_sec.LoadClientAuthInterceptor(config)
	options = append(options, authInterceptor)

	return options
}
