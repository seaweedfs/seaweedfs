package security

import (
	"context"
	"path"
	"strings"
	"time"

	jwt "github.com/golang-jwt/jwt/v5"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// The filer gRPC methods a scoped subscription token may name in allowed_methods.
const (
	MethodSubscribeMetadata      = "SubscribeMetadata"
	MethodSubscribeLocalMetadata = "SubscribeLocalMetadata"
)

// GenJwtForMetadataSubscription mints a read-only token for the filer's
// metadata subscription calls, limited to events under prefixes. It carries
// allowed_prefixes and allowed_methods, so it is never an admin token.
// Returns an empty string without a key, an expiry, or an absolute prefix.
func GenJwtForMetadataSubscription(signingKey SigningKey, expiresAfterSec int, prefixes []string) EncodedJwt {
	prefixes, ok := cleanMetadataPrefixes(prefixes)
	if len(signingKey) == 0 || expiresAfterSec <= 0 || !ok {
		return ""
	}
	claims := SeaweedFilerClaims{
		AllowedPrefixes: prefixes,
		AllowedMethods:  []string{MethodSubscribeMetadata, MethodSubscribeLocalMetadata},
		RegisteredClaims: jwt.RegisteredClaims{
			ExpiresAt: jwt.NewNumericDate(time.Now().Add(time.Second * time.Duration(expiresAfterSec))),
		},
	}
	encoded, err := jwt.NewWithClaims(jwt.SigningMethodHS256, claims).SignedString([]byte(signingKey))
	if err != nil {
		glog.V(0).Infof("Failed to sign metadata subscription claims: %v", err)
		return ""
	}
	return EncodedJwt(encoded)
}

// cleanMetadataPrefixes cleans prefixes; ok is false when the list is empty
// or any prefix is not absolute.
func cleanMetadataPrefixes(prefixes []string) (cleaned []string, ok bool) {
	for _, p := range prefixes {
		if !strings.HasPrefix(p, "/") {
			return nil, false
		}
		cleaned = append(cleaned, path.Clean(p))
	}
	return cleaned, len(cleaned) > 0
}

// MetadataPathInScope reports whether fullPath is one of the prefixes or below
// one, by whole path segments: "/buckets/a" holds "/buckets/a/x", not "/buckets/ab".
func MetadataPathInScope(fullPath string, prefixes []string) bool {
	fullPath = path.Clean("/" + fullPath)
	for _, p := range prefixes {
		if p == "/" || fullPath == p || strings.HasPrefix(fullPath, p+"/") {
			return true
		}
	}
	return false
}

// MetadataSubscriptionScope limits a subscription to events under Prefixes
// until ExpiresAt.
type MetadataSubscriptionScope struct {
	Prefixes  []string
	ExpiresAt time.Time
}

func metadataSubscriptionScope(signingKey SigningKey, token EncodedJwt, method string) (*MetadataSubscriptionScope, bool) {
	parsed, err := decodeJwt(signingKey, nil, token, &SeaweedFilerClaims{}, jwt.WithValidMethods([]string{jwt.SigningMethodHS256.Alg()}))
	if err != nil || parsed == nil || !parsed.Valid {
		return nil, false
	}
	claims, ok := parsed.Claims.(*SeaweedFilerClaims)
	if !ok || claims.SessionId != "" || claims.ExpiresAt == nil {
		return nil, false
	}
	allowsMethod := false
	for _, m := range claims.AllowedMethods {
		if m != MethodSubscribeMetadata && m != MethodSubscribeLocalMetadata {
			return nil, false
		}
		allowsMethod = allowsMethod || m == method
	}
	if !allowsMethod {
		return nil, false
	}
	prefixes, ok := cleanMetadataPrefixes(claims.AllowedPrefixes)
	if !ok {
		return nil, false
	}
	return &MetadataSubscriptionScope{Prefixes: prefixes, ExpiresAt: claims.ExpiresAt.Time}, true
}

// CheckGrpcMetadataSubscription authorizes a metadata subscription call.
// Without subscribeKey every caller is unrestricted (nil scope). With it, an
// mTLS-verified peer or an admin token signed by adminKey or previousAdminKey
// is unrestricted, and a token signed by subscribeKey is limited to the
// returned scope.
func CheckGrpcMetadataSubscription(ctx context.Context, subscribeKey, adminKey, previousAdminKey SigningKey, method string) (scope *MetadataSubscriptionScope, err error) {
	if len(subscribeKey) == 0 || isMtlsVerifiedPeer(ctx) {
		return nil, nil
	}
	md, _ := metadata.FromIncomingContext(ctx)
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
		if IsFilerAdminToken(adminKey, token, previousAdminKey) {
			return nil, nil
		}
		if scope, ok := metadataSubscriptionScope(subscribeKey, token, method); ok {
			return scope, nil
		}
	}
	return nil, status.Error(codes.Unauthenticated, "metadata subscription requires mTLS, an admin token, or a subscription token for this method")
}

// WithMetadataSubscriptionCredentials adds an admin token to a subscription
// call: from jwtProvider when the caller has per-cluster keys, else when this
// process has both jwt.filer_signing.key and jwt.filer_signing.subscribe.key,
// so internal subscribers keep full access.
func WithMetadataSubscriptionCredentials(ctx context.Context, jwtProvider FilerJwtProvider) context.Context {
	var token EncodedJwt
	if jwtProvider != nil {
		token = jwtProvider(true)
	} else if v := util.GetViper(); v.GetString("jwt.filer_signing.subscribe.key") != "" {
		token = GenJwtForFilerAdmin(SigningKey(v.GetString("jwt.filer_signing.key")), v.GetInt("jwt.filer_signing.expires_after_seconds"))
	}
	if token == "" {
		return ctx
	}
	return metadata.AppendToOutgoingContext(ctx, "authorization", BearerPrefix+string(token))
}
