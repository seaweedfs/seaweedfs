package security

import (
	"context"
	"path"
	"strings"
	"time"

	jwt "github.com/golang-jwt/jwt/v5"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// The filer gRPC methods a metadata subscription token may name in
// allowed_methods.
const (
	MethodSubscribeMetadata      = "SubscribeMetadata"
	MethodSubscribeLocalMetadata = "SubscribeLocalMetadata"
)

// GenJwtForMetadataSubscription mints a read-only token for the filer's
// SubscribeMetadata and SubscribeLocalMetadata calls, limited to events under
// the given path prefixes. It is signed with jwt.filer_signing.read.key.
//
// allowed_methods names only the two gRPC calls, so the filer's HTTP API,
// which matches allowed_methods against the HTTP method, refuses the token,
// and its prefixes and methods mean it is never an admin token (see
// IsFilerAdminToken). It returns an empty string without a key, without an
// expiry, or when a prefix is not an absolute, clean path.
func GenJwtForMetadataSubscription(readSigningKey SigningKey, expiresAfterSec int, prefixes []string) EncodedJwt {
	if len(readSigningKey) == 0 || expiresAfterSec <= 0 || !validMetadataPrefixes(prefixes) {
		return ""
	}
	claims := SeaweedFilerClaims{
		AllowedPrefixes: prefixes,
		AllowedMethods:  []string{MethodSubscribeMetadata, MethodSubscribeLocalMetadata},
		RegisteredClaims: jwt.RegisteredClaims{
			ExpiresAt: jwt.NewNumericDate(time.Now().Add(time.Second * time.Duration(expiresAfterSec))),
		},
	}
	encoded, err := jwt.NewWithClaims(jwt.SigningMethodHS256, claims).SignedString([]byte(readSigningKey))
	if err != nil {
		glog.V(0).Infof("Failed to sign metadata subscription claims: %v", err)
		return ""
	}
	return EncodedJwt(encoded)
}

// validMetadataPrefixes reports whether prefixes is non-empty and every
// prefix is an absolute path already in clean form, so "/a/../b", "/a/" and
// "a" are refused rather than reinterpreted.
func validMetadataPrefixes(prefixes []string) bool {
	if len(prefixes) == 0 {
		return false
	}
	for _, p := range prefixes {
		if !strings.HasPrefix(p, "/") || path.Clean(p) != p {
			return false
		}
	}
	return true
}

// MetadataPathInScope reports whether fullPath is one of the prefixes or below
// one of them, by whole path segments: "/buckets/a" holds "/buckets/a/x" and
// not "/buckets/ab". The path is cleaned first, so "/buckets/a/../b" is
// "/buckets/b". Paths are compared literally: percent signs are not decoded.
func MetadataPathInScope(fullPath string, prefixes []string) bool {
	fullPath = path.Clean("/" + fullPath)
	for _, p := range prefixes {
		if p == "/" || fullPath == p || strings.HasPrefix(fullPath, p+"/") {
			return true
		}
	}
	return false
}

// metadataSubscriptionScope returns the prefixes of a valid subscription token
// for method signed by readSigningKey. A token without an expiry, with a
// session id, without prefixes, with a prefix that is not an absolute clean
// path, or naming any method other than the two subscribe calls is refused.
func metadataSubscriptionScope(readSigningKey SigningKey, token EncodedJwt, method string) ([]string, bool) {
	if len(readSigningKey) == 0 || token == "" {
		return nil, false
	}
	parsed, err := decodeJwt(readSigningKey, token, &SeaweedFilerClaims{}, jwt.WithExpirationRequired())
	if err != nil || parsed == nil || !parsed.Valid {
		return nil, false
	}
	claims, ok := parsed.Claims.(*SeaweedFilerClaims)
	if !ok || claims.SessionId != "" || len(claims.AllowedMethods) == 0 || !validMetadataPrefixes(claims.AllowedPrefixes) {
		return nil, false
	}
	allowsMethod := false
	for _, m := range claims.AllowedMethods {
		switch m {
		case MethodSubscribeMetadata, MethodSubscribeLocalMetadata:
		default:
			return nil, false
		}
		if m == method {
			allowsMethod = true
		}
	}
	if !allowsMethod {
		return nil, false
	}
	return claims.AllowedPrefixes, true
}

// CheckGrpcMetadataSubscription authorizes a metadata subscription call when
// the filer requires it (jwt.filer_signing.read.require_for_metadata_subscription).
//
// The caller must be an mTLS-verified peer or present an admin token signed
// by adminKey (jwt.filer_signing.key); both are unrestricted and get a nil
// scope. Otherwise it must present a subscription token signed by
// readSigningKey, and the returned prefixes are the only paths the events it
// is sent may name.
func CheckGrpcMetadataSubscription(ctx context.Context, readSigningKey, adminKey SigningKey, method string) (scope []string, err error) {
	if isMtlsVerifiedPeer(ctx) {
		return nil, nil
	}
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return nil, status.Error(codes.Unauthenticated, "missing metadata")
	}
	authHeaders := md.Get("authorization")
	if len(authHeaders) > maxGrpcAdminAuthValues {
		authHeaders = authHeaders[:maxGrpcAdminAuthValues]
	}
	for _, raw := range authHeaders {
		parts := strings.Fields(raw)
		if len(parts) != 2 || !strings.EqualFold(parts[0], "Bearer") || parts[1] == "" {
			continue
		}
		token := EncodedJwt(parts[1])
		if IsFilerAdminToken(adminKey, token) {
			return nil, nil
		}
		if prefixes, ok := metadataSubscriptionScope(readSigningKey, token, method); ok {
			return prefixes, nil
		}
	}
	return nil, status.Error(codes.Unauthenticated, "metadata subscription requires mTLS, an admin token, or a subscription token signed with jwt.filer_signing.read.key")
}
