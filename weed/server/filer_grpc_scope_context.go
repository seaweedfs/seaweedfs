package weed_server

import "context"

// scopeKeepOnlyContextKey marks a handler call admitted for a scoped token.
// It is deliberately unexported so callers cannot manufacture the marker
// through a public context contract; the gRPC scope interceptor is the only
// producer.
type scopeKeepOnlyContextKey struct{}

func withScopeKeepOnly(ctx context.Context) context.Context {
	return context.WithValue(ctx, scopeKeepOnlyContextKey{}, true)
}

func scopeKeepOnly(ctx context.Context) bool {
	keepOnly, _ := ctx.Value(scopeKeepOnlyContextKey{}).(bool)
	return keepOnly
}
