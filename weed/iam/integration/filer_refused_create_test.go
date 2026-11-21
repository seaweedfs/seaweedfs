package integration

import (
	"context"
	"net"
	"strconv"
	"sync/atomic"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// refusingFiler refuses every create the way a filer does: the RPC succeeds and
// its response names the failure, here a read-only path.
type refusingFiler struct {
	filer_pb.UnimplementedSeaweedFilerServer
	creates atomic.Int32
}

func (f *refusingFiler) CreateEntry(context.Context, *filer_pb.CreateEntryRequest) (*filer_pb.CreateEntryResponse, error) {
	f.creates.Add(1)
	return &filer_pb.CreateEntryResponse{
		Error:     "read only: /etc/iam/ (e.g. bucket over quota)",
		ErrorCode: filer_pb.FilerError_READ_ONLY,
	}, nil
}

func (f *refusingFiler) ListEntries(*filer_pb.ListEntriesRequest, filer_pb.SeaweedFiler_ListEntriesServer) error {
	return nil
}

func startRefusingFiler(t *testing.T) (*refusingFiler, func() string) {
	filer := &refusingFiler{}
	return filer, startTestFiler(t, filer)
}

func startTestFiler(t *testing.T, filer filer_pb.SeaweedFilerServer) func() string {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	server := pb.NewGrpcServer()
	filer_pb.RegisterSeaweedFilerServer(server, filer)
	go func() { _ = server.Serve(lis) }()
	t.Cleanup(func() {
		server.Stop()
		_ = lis.Close()
	})
	host, port, err := net.SplitHostPort(lis.Addr().String())
	require.NoError(t, err)
	grpcPort, err := strconv.Atoi(port)
	require.NoError(t, err)
	address := string(pb.NewServerAddress(host, 1, grpcPort))
	return func() string { return address }
}

// The IAM stores must not take a refused create for a stored record. A session
// revocation the filer never wrote would leave the session usable.
func TestFilerStoresFailWhenTheFilerRefusesTheCreate(t *testing.T) {
	ctx := context.Background()
	dial := grpc.WithTransportCredentials(insecure.NewCredentials())
	filer, address := startRefusingFiler(t)
	const arn = "arn:aws:iam::123456789012:oidc-provider/issuer.example.com"

	revocations := NewFilerSessionRevocationStore(nil, address)
	revocations.grpcDialOption = dial
	audit := NewFilerAuditSink(nil, address)
	audit.grpcDialOption = dial
	providers := NewFilerOIDCProviderStore(nil, address)
	providers.grpcDialOption = dial
	roles, err := NewFilerRoleStore(nil, address)
	require.NoError(t, err)
	roles.grpcDialOption = dial

	writes := []struct {
		name  string
		write func() error
	}{
		{"session revocation", func() error {
			return revocations.Revoke(ctx, "", &RevocationEntry{JTI: "session-1"})
		}},
		{"audit event", func() error {
			return audit.Emit(ctx, &OIDCProviderAuditEvent{Type: OIDCAuditEventCreated, ARN: arn})
		}},
		{"OIDC provider", func() error {
			return providers.StoreProvider(ctx, "", &OIDCProviderRecord{ARN: arn})
		}},
		{"role", func() error {
			return roles.StoreRole(ctx, "", "app", &RoleDefinition{RoleName: "app"})
		}},
	}
	for _, tc := range writes {
		t.Run(tc.name, func(t *testing.T) {
			before := filer.creates.Load()
			err := tc.write()
			require.Equal(t, before+1, filer.creates.Load(), "the write must reach the filer")
			require.ErrorContains(t, err, "read only")
		})
	}
}
