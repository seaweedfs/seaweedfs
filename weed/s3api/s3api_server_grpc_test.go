package s3api

import (
	"context"
	"testing"
	"time"

	jwt "github.com/golang-jwt/jwt/v5"
	"github.com/seaweedfs/seaweedfs/weed/pb/iam_pb"
	"github.com/seaweedfs/seaweedfs/weed/security"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

const testS3IamCacheSigningKey = "s3-iam-cache-test-key-do-not-use-in-prod"

func newTestS3IamCacheServer(t *testing.T, signingKey string) *S3ApiServer {
	t.Helper()
	s3a := newTestS3ApiServerWithMemoryIAM(t, nil)
	s3a.filerGuard = security.NewGuard(nil, signingKey, 10, "", 60)
	return s3a
}

func s3IamCacheBearerCtx(token string) context.Context {
	md := metadata.New(map[string]string{"authorization": "Bearer " + token})
	return metadata.NewIncomingContext(context.Background(), md)
}

func TestS3IamCache_NoMetadata_Unauthenticated(t *testing.T) {
	s := newTestS3IamCacheServer(t, testS3IamCacheSigningKey)
	_, err := s.PutIdentity(context.Background(), &iam_pb.PutIdentityRequest{
		Identity: &iam_pb.Identity{Name: "pwn", Actions: []string{"Admin"}},
	})
	if got, want := status.Code(err), codes.Unauthenticated; got != want {
		t.Fatalf("PutIdentity without metadata: got code %v, want %v (err=%v)", got, want, err)
	}
}

func TestS3IamCache_MissingAuthorizationHeader_Unauthenticated(t *testing.T) {
	s := newTestS3IamCacheServer(t, testS3IamCacheSigningKey)
	ctx := metadata.NewIncomingContext(context.Background(), metadata.New(map[string]string{"other": "value"}))
	_, err := s.PutIdentity(ctx, &iam_pb.PutIdentityRequest{
		Identity: &iam_pb.Identity{Name: "pwn", Actions: []string{"Admin"}},
	})
	if got, want := status.Code(err), codes.Unauthenticated; got != want {
		t.Fatalf("PutIdentity with no authorization header: got code %v, want %v (err=%v)", got, want, err)
	}
}

func TestS3IamCache_NonBearerAuthorization_Unauthenticated(t *testing.T) {
	s := newTestS3IamCacheServer(t, testS3IamCacheSigningKey)
	md := metadata.New(map[string]string{"authorization": "Basic dXNlcjpwYXNz"})
	ctx := metadata.NewIncomingContext(context.Background(), md)
	_, err := s.PutIdentity(ctx, &iam_pb.PutIdentityRequest{
		Identity: &iam_pb.Identity{Name: "pwn", Actions: []string{"Admin"}},
	})
	if got, want := status.Code(err), codes.Unauthenticated; got != want {
		t.Fatalf("PutIdentity with non-Bearer scheme: got code %v, want %v (err=%v)", got, want, err)
	}
}

func TestS3IamCache_InvalidToken_Unauthenticated(t *testing.T) {
	s := newTestS3IamCacheServer(t, testS3IamCacheSigningKey)
	bad := security.GenJwtForFilerAdmin(security.SigningKey("a-different-key"), 60)
	if bad == "" {
		t.Fatal("GenJwtForFilerAdmin returned empty")
	}
	_, err := s.PutIdentity(s3IamCacheBearerCtx(string(bad)), &iam_pb.PutIdentityRequest{
		Identity: &iam_pb.Identity{Name: "pwn", Actions: []string{"Admin"}},
	})
	if got, want := status.Code(err), codes.Unauthenticated; got != want {
		t.Fatalf("PutIdentity with mis-signed token: got code %v, want %v (err=%v)", got, want, err)
	}
}

func TestS3IamCache_GarbageToken_Unauthenticated(t *testing.T) {
	s := newTestS3IamCacheServer(t, testS3IamCacheSigningKey)
	_, err := s.PutIdentity(s3IamCacheBearerCtx("not.a.jwt"), &iam_pb.PutIdentityRequest{
		Identity: &iam_pb.Identity{Name: "pwn", Actions: []string{"Admin"}},
	})
	if got, want := status.Code(err), codes.Unauthenticated; got != want {
		t.Fatalf("PutIdentity with garbage token: got code %v, want %v (err=%v)", got, want, err)
	}
}

func TestS3IamCache_ExpiredToken_Unauthenticated(t *testing.T) {
	s := newTestS3IamCacheServer(t, testS3IamCacheSigningKey)
	claims := security.SeaweedFilerAdminClaims{
		RegisteredClaims: jwt.RegisteredClaims{
			ExpiresAt: jwt.NewNumericDate(time.Now().Add(-time.Hour)),
		},
	}
	tok := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	encoded, err := tok.SignedString([]byte(testS3IamCacheSigningKey))
	if err != nil {
		t.Fatalf("SignedString: %v", err)
	}
	_, err = s.PutIdentity(s3IamCacheBearerCtx(encoded), &iam_pb.PutIdentityRequest{
		Identity: &iam_pb.Identity{Name: "pwn", Actions: []string{"Admin"}},
	})
	if got, want := status.Code(err), codes.Unauthenticated; got != want {
		t.Fatalf("PutIdentity with expired token: got code %v, want %v (err=%v)", got, want, err)
	}
}

func TestS3IamCache_ValidToken_WritesIdentity(t *testing.T) {
	s := newTestS3IamCacheServer(t, testS3IamCacheSigningKey)
	good := security.GenJwtForFilerAdmin(security.SigningKey(testS3IamCacheSigningKey), 60)
	if good == "" {
		t.Fatal("GenJwtForFilerAdmin returned empty")
	}
	_, err := s.PutIdentity(s3IamCacheBearerCtx(string(good)), &iam_pb.PutIdentityRequest{
		Identity: &iam_pb.Identity{
			Name:    "pushed",
			Actions: []string{"Admin"},
			Credentials: []*iam_pb.Credential{
				{AccessKey: "AKIAPUSHED", SecretKey: "s3cr3t"},
			},
		},
	})
	if err != nil {
		t.Fatalf("PutIdentity with valid token: unexpected error %v", err)
	}
	s.iam.m.RLock()
	id := s.iam.accessKeyIdent["AKIAPUSHED"]
	s.iam.m.RUnlock()
	if id == nil {
		t.Fatalf("expected pushed identity to land in accessKeyIdent map")
	}
	if !id.isAdmin() {
		t.Fatalf("expected pushed identity to be admin, actions=%v", id.Actions)
	}
}

func TestS3IamCache_NoSigningKey_Unauthenticated_Allowed(t *testing.T) {
	s := newTestS3IamCacheServer(t, "")
	_, err := s.PutIdentity(context.Background(), &iam_pb.PutIdentityRequest{
		Identity: &iam_pb.Identity{Name: "pushed", Actions: []string{"Read"}},
	})
	if err != nil {
		t.Fatalf("PutIdentity without key: unexpected error %v", err)
	}
	good := security.GenJwtForFilerAdmin(security.SigningKey(testS3IamCacheSigningKey), 60)
	if _, err := s.PutIdentity(s3IamCacheBearerCtx(string(good)), &iam_pb.PutIdentityRequest{
		Identity: &iam_pb.Identity{Name: "pushed2", Actions: []string{"Read"}},
	}); err != nil {
		t.Fatalf("PutIdentity with stray token but no server key: unexpected error %v", err)
	}
}

func TestS3IamCache_RemoveIdentity_RequiresAuth(t *testing.T) {
	s := newTestS3IamCacheServer(t, testS3IamCacheSigningKey)
	_, err := s.RemoveIdentity(context.Background(), &iam_pb.RemoveIdentityRequest{Username: "anyone"})
	if got, want := status.Code(err), codes.Unauthenticated; got != want {
		t.Fatalf("RemoveIdentity without token: got code %v, want %v (err=%v)", got, want, err)
	}
}

func TestS3IamCache_PutPolicy_RequiresAuth(t *testing.T) {
	s := newTestS3IamCacheServer(t, testS3IamCacheSigningKey)
	_, err := s.PutPolicy(context.Background(), &iam_pb.PutPolicyRequest{Name: "p", Content: "{}"})
	if got, want := status.Code(err), codes.Unauthenticated; got != want {
		t.Fatalf("PutPolicy without token: got code %v, want %v (err=%v)", got, want, err)
	}
}

func TestS3IamCache_DeletePolicy_RequiresAuth(t *testing.T) {
	s := newTestS3IamCacheServer(t, testS3IamCacheSigningKey)
	_, err := s.DeletePolicy(context.Background(), &iam_pb.DeletePolicyRequest{Name: "p"})
	if got, want := status.Code(err), codes.Unauthenticated; got != want {
		t.Fatalf("DeletePolicy without token: got code %v, want %v (err=%v)", got, want, err)
	}
}

func TestS3IamCache_GetPolicy_RequiresAuth(t *testing.T) {
	s := newTestS3IamCacheServer(t, testS3IamCacheSigningKey)
	_, err := s.GetPolicy(context.Background(), &iam_pb.GetPolicyRequest{Name: "p"})
	if got, want := status.Code(err), codes.Unauthenticated; got != want {
		t.Fatalf("GetPolicy without token: got code %v, want %v (err=%v)", got, want, err)
	}
}

func TestS3IamCache_ListPolicies_RequiresAuth(t *testing.T) {
	s := newTestS3IamCacheServer(t, testS3IamCacheSigningKey)
	_, err := s.ListPolicies(context.Background(), &iam_pb.ListPoliciesRequest{})
	if got, want := status.Code(err), codes.Unauthenticated; got != want {
		t.Fatalf("ListPolicies without token: got code %v, want %v (err=%v)", got, want, err)
	}
}

func TestS3IamCache_PutGroup_RequiresAuth(t *testing.T) {
	s := newTestS3IamCacheServer(t, testS3IamCacheSigningKey)
	_, err := s.PutGroup(context.Background(), &iam_pb.PutGroupRequest{Group: &iam_pb.Group{Name: "g"}})
	if got, want := status.Code(err), codes.Unauthenticated; got != want {
		t.Fatalf("PutGroup without token: got code %v, want %v (err=%v)", got, want, err)
	}
}

func TestS3IamCache_RemoveGroup_RequiresAuth(t *testing.T) {
	s := newTestS3IamCacheServer(t, testS3IamCacheSigningKey)
	_, err := s.RemoveGroup(context.Background(), &iam_pb.RemoveGroupRequest{GroupName: "g"})
	if got, want := status.Code(err), codes.Unauthenticated; got != want {
		t.Fatalf("RemoveGroup without token: got code %v, want %v (err=%v)", got, want, err)
	}
}
