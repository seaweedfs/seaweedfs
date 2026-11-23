package weed_server

import "context"

type scopeWriteAuthorityKey struct{}

// Only the authenticated interceptor supplies this authority. Proof maps stay
// on each request, so concurrent stream messages never share mutable proofs.
type scopeWriteAuthority struct {
	s  *filerGrpcScope
	sc *scope
}

func (s *filerGrpcScope) withWriteAuthority(ctx context.Context, sc *scope) context.Context {
	return context.WithValue(withScopeKeepOnly(ctx), scopeWriteAuthorityKey{}, &scopeWriteAuthority{s: s, sc: sc})
}

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
