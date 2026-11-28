package security

import (
	"context"
	"strings"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/tap"
)

const grpcHealthServicePrefix = "/grpc.health.v1.Health/"

// healthCheckOnlyTap confines clients whose certificate common name is in
// grpc.health_check_commonNames to the gRPC health service.
//
// A monitoring probe (blackbox_exporter, grpc_health_probe) has to finish the
// mTLS handshake to reach the health service, and before this the only way in
// was a name on allowed_commonNames, which opens every RPC on the port. The
// probe's certificate is then a cluster credential. With this list the probe
// still proves a CA-signed certificate, so it still checks the server's
// certificate chain and expiry, but it can call nothing else.
//
// The check runs as a tap handle so it covers unary and streaming RPCs with
// one server option, before the request is decoded. A name on both lists
// stays confined: the health check list is the narrower, deliberate grant.
func healthCheckOnlyTap(healthCheckNames map[string]bool) tap.ServerInHandle {
	return func(ctx context.Context, info *tap.Info) (context.Context, error) {
		if strings.HasPrefix(info.FullMethodName, grpcHealthServicePrefix) {
			return ctx, nil
		}
		p, ok := peer.FromContext(ctx)
		if !ok {
			return ctx, nil
		}
		tlsInfo, ok := p.AuthInfo.(credentials.TLSInfo)
		if !ok || len(tlsInfo.State.PeerCertificates) == 0 {
			return ctx, nil
		}
		if commonName := tlsInfo.State.PeerCertificates[0].Subject.CommonName; healthCheckNames[commonName] {
			return ctx, status.Errorf(codes.PermissionDenied, "certificate %q may only call the gRPC health service", commonName)
		}
		return ctx, nil
	}
}
