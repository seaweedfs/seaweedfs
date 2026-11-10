package security

import (
	"context"
	"net"
	"strings"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
)

// CheckGrpcAdminPeer authenticates the peer of an admin-plane gRPC handler:
// a client certificate verified by the server's TLS config, or a Bearer token
// signed by signingKey (jwt.filer_signing.key). With no key configured remote
// callers cannot be told apart, so only local clients (unix socket, loopback,
// or an address assigned to this host) are allowed.
func CheckGrpcAdminPeer(ctx context.Context, signingKey SigningKey) error {
	if isMtlsVerifiedPeer(ctx) {
		return nil
	}
	if len(signingKey) == 0 {
		return CheckLocalGrpcPeer(ctx)
	}
	return CheckGrpcAdminToken(ctx, signingKey)
}

// maxGrpcAdminAuthValues bounds the JWT parses one request can trigger; a
// caller sends its own token plus at most one forwarded from its context.
const maxGrpcAdminAuthValues = 4

// CheckGrpcAdminToken accepts the call when one of the first
// maxGrpcAdminAuthValues authorization values is a Bearer admin token signed
// by signingKey, so a fresh token still counts when the caller's context also
// forwards an older one.
func CheckGrpcAdminToken(ctx context.Context, signingKey SigningKey) error {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return status.Error(codes.Unauthenticated, "missing metadata")
	}
	authHeaders := md.Get("authorization")
	if len(authHeaders) == 0 {
		return status.Error(codes.Unauthenticated, "missing authorization metadata")
	}
	if len(authHeaders) > maxGrpcAdminAuthValues {
		authHeaders = authHeaders[:maxGrpcAdminAuthValues]
	}
	err := status.Error(codes.Unauthenticated, "authorization header must use Bearer scheme")
	for _, raw := range authHeaders {
		parts := strings.Fields(raw)
		if len(parts) != 2 || !strings.EqualFold(parts[0], "Bearer") || parts[1] == "" {
			continue
		}
		if IsFilerAdminToken(signingKey, EncodedJwt(parts[1])) {
			return nil
		}
		err = status.Error(codes.Unauthenticated, "invalid admin token")
	}
	return err
}

// IsFilerAdminToken reports whether token is an admin token signed by
// signingKey. An STS session token, or a filer token scoped to paths or
// methods, signed with the same key does not count.
func IsFilerAdminToken(signingKey SigningKey, token EncodedJwt) bool {
	if len(signingKey) == 0 || token == "" {
		return false
	}
	parsed, err := DecodeJwt(signingKey, token, &SeaweedFilerAdminClaims{})
	if err != nil || parsed == nil || !parsed.Valid {
		return false
	}
	claims, ok := parsed.Claims.(*SeaweedFilerAdminClaims)
	return ok && claims.SessionId == "" && len(claims.AllowedPrefixes) == 0 && len(claims.AllowedMethods) == 0
}

// GrpcAdminPeerCredentials returns outgoing metadata carrying a Bearer token
// for admin-plane gRPC handlers, or nil when no signing key is configured.
func GrpcAdminPeerCredentials(signingKey SigningKey, expiresAfterSec int) metadata.MD {
	token := GenJwtForFilerAdmin(signingKey, expiresAfterSec)
	if token == "" {
		return nil
	}
	return metadata.Pairs("authorization", "Bearer "+string(token))
}

func isMtlsVerifiedPeer(ctx context.Context) bool {
	pr, ok := peer.FromContext(ctx)
	if !ok {
		return false
	}
	tlsInfo, ok := pr.AuthInfo.(credentials.TLSInfo)
	return ok && len(tlsInfo.State.VerifiedChains) > 0
}

// CheckLocalGrpcPeer admits only clients on this host: unix sockets, loopback,
// or an address assigned to a local interface.
func CheckLocalGrpcPeer(ctx context.Context) error {
	if ctx == nil {
		return status.Error(codes.Unauthenticated, "admin gRPC calls require jwt.filer_signing.key or a local client")
	}
	pr, ok := peer.FromContext(ctx)
	if !ok {
		return status.Error(codes.Unauthenticated, "admin gRPC calls require jwt.filer_signing.key or a local client")
	}
	if _, isUnix := pr.Addr.(*net.UnixAddr); isUnix {
		return nil
	}
	var ip net.IP
	if tcpAddr, ok := pr.Addr.(*net.TCPAddr); ok {
		ip = tcpAddr.IP
	} else if h, _, err := net.SplitHostPort(pr.Addr.String()); err == nil {
		ip = net.ParseIP(h)
	} else {
		ip = net.ParseIP(pr.Addr.String())
	}
	if ip != nil && (ip.IsLoopback() || isLocalAddress(ip)) {
		return nil
	}
	glog.V(1).Infof("rejected unauthenticated admin gRPC call from %s: no jwt.filer_signing.key configured", pr.Addr)
	return status.Error(codes.Unauthenticated, "admin gRPC calls require jwt.filer_signing.key or a local client")
}

// isLocalAddress enumerates interfaces per call: only reachable for
// non-loopback TCP peers on the no-key admin path, which is low-volume —
// and a cached set would keep trusting an address after it is removed from
// the host and reassigned.
func isLocalAddress(ip net.IP) bool {
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		return false
	}
	for _, a := range addrs {
		if ipNet, ok := a.(*net.IPNet); ok && ipNet.IP.Equal(ip) {
			return true
		}
	}
	return false
}
