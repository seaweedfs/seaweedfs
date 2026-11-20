package pb

import (
	"context"
	"sync/atomic"

	"google.golang.org/grpc"
)

// clientUnaryInterceptors run around every unary call on a connection opened
// by GrpcDial, in the order they were added.
var clientUnaryInterceptors atomic.Pointer[[]grpc.UnaryClientInterceptor]

// AddClientUnaryInterceptor makes interceptor run around every unary call on
// connections opened by GrpcDial, those already open included.
func AddClientUnaryInterceptor(interceptor grpc.UnaryClientInterceptor) {
	for {
		current := clientUnaryInterceptors.Load()
		var next []grpc.UnaryClientInterceptor
		if current != nil {
			next = append(next, *current...)
		}
		next = append(next, interceptor)
		if clientUnaryInterceptors.CompareAndSwap(current, &next) {
			return
		}
	}
}

// interceptClientUnary is installed on every connection, so an interceptor
// added later still applies to it. With none added it only calls through.
func interceptClientUnary(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
	chain := clientUnaryInterceptors.Load()
	if chain == nil {
		return invoker(ctx, method, req, reply, cc, opts...)
	}
	return chainClientUnary(*chain, invoker)(ctx, method, req, reply, cc, opts...)
}

func chainClientUnary(chain []grpc.UnaryClientInterceptor, invoker grpc.UnaryInvoker) grpc.UnaryInvoker {
	if len(chain) == 0 {
		return invoker
	}
	return func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, opts ...grpc.CallOption) error {
		return chain[0](ctx, method, req, reply, cc, chainClientUnary(chain[1:], invoker), opts...)
	}
}
