package credential

import (
	"context"
	"strings"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/security"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"google.golang.org/grpc/metadata"
)

func TestWithIamCacheAdminAuth_NoKey_NoOp(t *testing.T) {
	util.GetViper().Set("jwt.filer_signing.key", "")
	ctx := withIamCacheAdminAuth(context.Background())
	md, ok := metadata.FromOutgoingContext(ctx)
	if ok && len(md.Get("authorization")) > 0 {
		t.Fatalf("expected no authorization metadata without key, got %v", md.Get("authorization"))
	}
}

func TestWithIamCacheAdminAuth_WithKey_AttachesBearer(t *testing.T) {
	const k = "propagation-test-signing-key"
	util.GetViper().Set("jwt.filer_signing.key", k)
	defer util.GetViper().Set("jwt.filer_signing.key", "")
	util.GetViper().Set("jwt.filer_signing.expires_after_seconds", 60)

	ctx := withIamCacheAdminAuth(context.Background())
	md, ok := metadata.FromOutgoingContext(ctx)
	if !ok {
		t.Fatal("expected outgoing metadata to be set")
	}
	headers := md.Get("authorization")
	if len(headers) != 1 {
		t.Fatalf("expected one authorization header, got %v", headers)
	}
	if !strings.HasPrefix(headers[0], security.BearerPrefix) {
		t.Fatalf("expected Bearer scheme, got %q", headers[0])
	}
	token := strings.TrimPrefix(headers[0], security.BearerPrefix)
	parsed, err := security.DecodeJwt(security.SigningKey(k), security.EncodedJwt(token), &security.SeaweedFilerAdminClaims{})
	if err != nil || parsed == nil || !parsed.Valid {
		t.Fatalf("attached token failed signature validation: %v", err)
	}
}
