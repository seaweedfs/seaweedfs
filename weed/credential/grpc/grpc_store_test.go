package grpc

import (
	"context"
	"fmt"
	"net"
	"strings"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/credential"
	_ "github.com/seaweedfs/seaweedfs/weed/credential/memory"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/iam_pb"
	"github.com/seaweedfs/seaweedfs/weed/security"
	weed_server "github.com/seaweedfs/seaweedfs/weed/server"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

// TestIamGrpcStore_AdminBearerToken pins the contract that broke in 4.24:
// after PR #9442 the filer's IAM gRPC service requires an admin-signed
// Bearer token on every RPC, and IamGrpcStore is the only production
// client that talks to it (from the admin server). If the store doesn't
// mint and attach a token the admin UI Users/Groups pages fail with
// Unauthenticated; that's what issues #9495/#9496 reported.
func TestIamGrpcStore_AdminBearerToken(t *testing.T) {
	const goodKey = "iam-admin-itest-key"
	const wrongKey = "iam-admin-itest-key-different"

	// Real IamGrpcServer backed by an in-memory credential manager so we
	// exercise the full handler (auth check + business logic), not a stub.
	cm, err := credential.NewCredentialManager(credential.StoreTypeMemory, nil, "")
	if err != nil {
		t.Fatalf("NewCredentialManager: %v", err)
	}
	defer cm.Shutdown()

	iamSrv := weed_server.NewIamGrpcServer(cm, security.SigningKey(goodKey))

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	grpcSrv := grpc.NewServer()
	iam_pb.RegisterSeaweedIdentityAccessManagementServer(grpcSrv, iamSrv)
	go func() { _ = grpcSrv.Serve(lis) }()
	t.Cleanup(grpcSrv.Stop)

	// pb.ServerAddress encodes the grpc port in the trailing ".N" segment.
	// host:0 is the unused HTTP port; the listener's actual port is the
	// gRPC one that ToGrpcAddress() unpacks for dialing.
	_, portStr, err := net.SplitHostPort(lis.Addr().String())
	if err != nil {
		t.Fatalf("split listener addr: %v", err)
	}
	serverAddr := pb.ServerAddress(fmt.Sprintf("127.0.0.1:0.%s", portStr))

	newStore := func(key string) *IamGrpcStore {
		s := &IamGrpcStore{}
		s.SetFilerAddressFunc(func() pb.ServerAddress { return serverAddr }, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if key != "" {
			s.SetAdminSigning(security.SigningKey(key), 0)
		}
		return s
	}

	ctx := context.Background()

	t.Run("matching_key_succeeds", func(t *testing.T) {
		store := newStore(goodKey)
		if _, err := store.ListUsers(ctx); err != nil {
			t.Fatalf("ListUsers with matching key: %v", err)
		}
		if err := store.CreateUser(ctx, &iam_pb.Identity{Name: "alice"}); err != nil {
			t.Fatalf("CreateUser with matching key: %v", err)
		}
		got, err := store.GetUser(ctx, "alice")
		if err != nil {
			t.Fatalf("GetUser with matching key: %v", err)
		}
		if got == nil || got.Name != "alice" {
			t.Fatalf("GetUser returned %+v, want name=alice", got)
		}
	})

	t.Run("wrong_key_unauthenticated", func(t *testing.T) {
		store := newStore(wrongKey)
		_, err := store.ListUsers(ctx)
		if got := status.Code(err); got != codes.Unauthenticated {
			t.Fatalf("ListUsers with wrong key: got code %v err=%v, want Unauthenticated", got, err)
		}
	})

	t.Run("no_key_unauthenticated", func(t *testing.T) {
		store := newStore("")
		_, err := store.ListUsers(ctx)
		// Server reports "missing authorization metadata" when no token is
		// attached. Either way the gRPC status code is Unauthenticated.
		if got := status.Code(err); got != codes.Unauthenticated {
			t.Fatalf("ListUsers with no key: got code %v err=%v, want Unauthenticated", got, err)
		}
		if err != nil && !strings.Contains(err.Error(), "authorization") {
			t.Fatalf("ListUsers with no key: error message %q does not mention authorization", err)
		}
	})
}
