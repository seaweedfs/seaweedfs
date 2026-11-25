package weed_server

import (
	"context"

	"github.com/seaweedfs/seaweedfs/weed/security"
	"google.golang.org/grpc/metadata"
)

// withCallerAuthContext carries the caller's bearer to the next filer hop.
// It deliberately copies only authorization; filer-owned calls use
// peerContext instead and therefore never inherit a caller's authority.
func withCallerAuthContext(ctx context.Context) context.Context {
	incoming, _ := metadata.FromIncomingContext(ctx)
	return withAuthorizationValues(ctx, incoming.Get("authorization"))
}

// withHTTPBearerContext adapts the bearer already authenticated on an HTTP
// request to the metadata form used by the outgoing filer RPC. An absent
// bearer remains absent, preserving tokenless compatibility when require-mode
// is disabled and allowing require-mode to reject it at the receiver.
func withHTTPBearerContext(ctx context.Context, token security.EncodedJwt) context.Context {
	if token == "" {
		return withAuthorizationValues(ctx, nil)
	}
	return withAuthorizationValues(ctx, []string{security.BearerPrefix + string(token)})
}

func withAuthorizationValues(ctx context.Context, values []string) context.Context {
	outgoing, ok := metadata.FromOutgoingContext(ctx)
	if len(values) == 0 {
		if !ok || len(outgoing.Get("authorization")) == 0 {
			return ctx
		}
		outgoing = outgoing.Copy()
		delete(outgoing, "authorization")
		return metadata.NewOutgoingContext(ctx, outgoing)
	}
	if outgoing == nil {
		outgoing = metadata.MD{}
	} else {
		outgoing = outgoing.Copy()
	}
	// A context may already carry a metadata value from a caller or an earlier
	// forwarding helper. Replace it so the next hop sees exactly the caller's
	// original authorization values, never a duplicate or an admin token.
	delete(outgoing, "authorization")
	outgoing.Set("authorization", values...)
	return metadata.NewOutgoingContext(ctx, outgoing)
}
