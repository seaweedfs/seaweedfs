package weed_server

import (
	"context"
	"testing"
	"time"

	jwt "github.com/golang-jwt/jwt/v5"
	"github.com/seaweedfs/seaweedfs/weed/credential"
	_ "github.com/seaweedfs/seaweedfs/weed/credential/memory"
	"github.com/seaweedfs/seaweedfs/weed/pb/iam_pb"
	"github.com/seaweedfs/seaweedfs/weed/security"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

const testIamSigningKey = "iam-admin-test-key-do-not-use-in-prod"

func newTestIamGrpcServer(t *testing.T) *IamGrpcServer {
	t.Helper()
	cm, err := credential.NewCredentialManager(credential.StoreTypeMemory, nil, "")
	if err != nil {
		t.Fatalf("NewCredentialManager: %v", err)
	}
	return NewIamGrpcServer(cm, security.SigningKey(testIamSigningKey))
}

func ctxWithBearer(token string) context.Context {
	md := metadata.New(map[string]string{"authorization": "Bearer " + token})
	return metadata.NewIncomingContext(context.Background(), md)
}

func TestIamGrpc_NoMetadata_Unauthenticated(t *testing.T) {
	s := newTestIamGrpcServer(t)
	_, err := s.ListUsers(context.Background(), &iam_pb.ListUsersRequest{})
	if got, want := status.Code(err), codes.Unauthenticated; got != want {
		t.Fatalf("ListUsers without metadata: got code %v, want %v (err=%v)", got, want, err)
	}
}

func TestIamGrpc_MissingAuthorizationHeader_Unauthenticated(t *testing.T) {
	s := newTestIamGrpcServer(t)
	ctx := metadata.NewIncomingContext(context.Background(), metadata.New(map[string]string{"other": "value"}))
	_, err := s.ListUsers(ctx, &iam_pb.ListUsersRequest{})
	if got, want := status.Code(err), codes.Unauthenticated; got != want {
		t.Fatalf("ListUsers with no authorization header: got code %v, want %v (err=%v)", got, want, err)
	}
}

func TestIamGrpc_NonBearerAuthorization_Unauthenticated(t *testing.T) {
	s := newTestIamGrpcServer(t)
	md := metadata.New(map[string]string{"authorization": "Basic dXNlcjpwYXNz"})
	ctx := metadata.NewIncomingContext(context.Background(), md)
	_, err := s.ListUsers(ctx, &iam_pb.ListUsersRequest{})
	if got, want := status.Code(err), codes.Unauthenticated; got != want {
		t.Fatalf("ListUsers with non-Bearer scheme: got code %v, want %v (err=%v)", got, want, err)
	}
}

func TestIamGrpc_InvalidToken_Unauthenticated(t *testing.T) {
	s := newTestIamGrpcServer(t)
	// Token signed with the wrong key.
	bad := security.GenJwtForFilerAdmin(security.SigningKey("a-different-key"), 60)
	if bad == "" {
		t.Fatal("GenJwtForFilerAdmin returned empty")
	}
	_, err := s.ListUsers(ctxWithBearer(string(bad)), &iam_pb.ListUsersRequest{})
	if got, want := status.Code(err), codes.Unauthenticated; got != want {
		t.Fatalf("ListUsers with mis-signed token: got code %v, want %v (err=%v)", got, want, err)
	}
}

func TestIamGrpc_GarbageToken_Unauthenticated(t *testing.T) {
	s := newTestIamGrpcServer(t)
	_, err := s.ListUsers(ctxWithBearer("not.a.jwt"), &iam_pb.ListUsersRequest{})
	if got, want := status.Code(err), codes.Unauthenticated; got != want {
		t.Fatalf("ListUsers with garbage token: got code %v, want %v (err=%v)", got, want, err)
	}
}

func TestIamGrpc_ExpiredToken_Unauthenticated(t *testing.T) {
	s := newTestIamGrpcServer(t)
	// Mint a token that's already expired (exp in the past).
	claims := security.SeaweedFilerAdminClaims{
		RegisteredClaims: jwt.RegisteredClaims{
			ExpiresAt: jwt.NewNumericDate(time.Now().Add(-time.Hour)),
		},
	}
	tok := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	encoded, err := tok.SignedString([]byte(testIamSigningKey))
	if err != nil {
		t.Fatalf("SignedString: %v", err)
	}
	_, err = s.ListUsers(ctxWithBearer(encoded), &iam_pb.ListUsersRequest{})
	if got, want := status.Code(err), codes.Unauthenticated; got != want {
		t.Fatalf("ListUsers with expired token: got code %v, want %v (err=%v)", got, want, err)
	}
}

func TestIamGrpc_ValidToken_ReachesHandler(t *testing.T) {
	s := newTestIamGrpcServer(t)
	good := security.GenJwtForFilerAdmin(security.SigningKey(testIamSigningKey), 60)
	if good == "" {
		t.Fatal("GenJwtForFilerAdmin returned empty")
	}
	resp, err := s.ListUsers(ctxWithBearer(string(good)), &iam_pb.ListUsersRequest{})
	if err != nil {
		t.Fatalf("ListUsers with valid token: unexpected error %v", err)
	}
	if resp == nil {
		t.Fatal("ListUsers with valid token: nil response")
	}
	// Memory store starts empty; the handler ran past the auth gate.
	if len(resp.Usernames) != 0 {
		t.Fatalf("ListUsers: expected empty user list from fresh memory store, got %v", resp.Usernames)
	}
}

func TestIamGrpc_NoSigningKey_Unauthenticated_Allowed(t *testing.T) {
	// Auth is opt-in: when the server is built without a signing key, every
	// RPC is accepted regardless of (or in the absence of) metadata so the
	// admin UI works against a filer that has no jwt.filer_signing.key set.
	cm, err := credential.NewCredentialManager(credential.StoreTypeMemory, nil, "")
	if err != nil {
		t.Fatalf("NewCredentialManager: %v", err)
	}
	s := NewIamGrpcServer(cm, nil)
	resp, err := s.ListUsers(context.Background(), &iam_pb.ListUsersRequest{})
	if err != nil {
		t.Fatalf("ListUsers without key: unexpected error %v", err)
	}
	if resp == nil {
		t.Fatal("ListUsers without key: nil response")
	}
	if len(resp.Usernames) != 0 {
		t.Fatalf("ListUsers: expected empty user list from fresh memory store, got %v", resp.Usernames)
	}

	// A token sent by a client that does configure a key is also accepted —
	// the server just ignores it rather than rejecting on signature mismatch.
	good := security.GenJwtForFilerAdmin(security.SigningKey(testIamSigningKey), 60)
	if _, err := s.ListUsers(ctxWithBearer(string(good)), &iam_pb.ListUsersRequest{}); err != nil {
		t.Fatalf("ListUsers with stray token but no server key: unexpected error %v", err)
	}
}

func TestIamGrpc_CreateUser_RequiresAuth(t *testing.T) {
	// Spot-check a write RPC too — auth must run before any work.
	s := newTestIamGrpcServer(t)
	_, err := s.CreateUser(context.Background(), &iam_pb.CreateUserRequest{
		Identity: &iam_pb.Identity{Name: "admin"},
	})
	if got, want := status.Code(err), codes.Unauthenticated; got != want {
		t.Fatalf("CreateUser without token: got code %v, want %v (err=%v)", got, want, err)
	}
}
