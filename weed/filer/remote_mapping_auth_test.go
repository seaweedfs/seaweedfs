package filer

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"net"
	"strings"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/remote_pb"
	"github.com/seaweedfs/seaweedfs/weed/security"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

type remoteMappingAuthFiler struct {
	filer_pb.UnimplementedSeaweedFilerServer
	key     security.SigningKey
	content []byte
}

func (s *remoteMappingAuthFiler) LookupDirectoryEntry(_ context.Context, _ *filer_pb.LookupDirectoryEntryRequest) (*filer_pb.LookupDirectoryEntryResponse, error) {
	return &filer_pb.LookupDirectoryEntryResponse{Entry: &filer_pb.Entry{Content: s.content}}, nil
}

func remoteMappingAuthKey(t *testing.T) security.SigningKey {
	t.Helper()
	buf := make([]byte, 32)
	if _, err := rand.Read(buf); err != nil {
		t.Fatal(err)
	}
	return security.SigningKey(hex.EncodeToString(buf))
}

func startRemoteMappingAuthFiler(t *testing.T, filer *remoteMappingAuthFiler) (pb.ServerAddress, grpc.DialOption) {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server := grpc.NewServer(grpc.UnaryInterceptor(func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		md, _ := metadata.FromIncomingContext(ctx)
		values := md.Get("authorization")
		if len(values) != 1 || !strings.HasPrefix(values[0], security.BearerPrefix) ||
			!security.IsFilerAdminToken(filer.key, security.EncodedJwt(strings.TrimPrefix(values[0], security.BearerPrefix))) {
			return nil, status.Error(codes.Unauthenticated, "missing or invalid filer authorization")
		}
		return handler(ctx, req)
	}))
	filer_pb.RegisterSeaweedFilerServer(server, filer)
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(func() {
		server.Stop()
		_ = listener.Close()
	})
	port := listener.Addr().(*net.TCPAddr).Port
	return pb.NewServerAddressWithGrpcPort("127.0.0.1:8888", port), grpc.WithTransportCredentials(insecure.NewCredentials())
}

func TestReadMountMappingsUsesProtectedFilerCall(t *testing.T) {
	key := remoteMappingAuthKey(t)
	content, err := proto.Marshal(&remote_pb.RemoteStorageMapping{Mappings: map[string]*remote_pb.RemoteStorageLocation{
		"/buckets/archive": {Name: "archive", Bucket: "bucket"},
	}})
	if err != nil {
		t.Fatal(err)
	}
	filer := &remoteMappingAuthFiler{key: key, content: content}
	address, dial := startRemoteMappingAuthFiler(t, filer)
	tokenSource := security.FilerTokenSource(func(context.Context) (security.EncodedJwt, error) {
		return security.GenJwtForFilerAdmin(key, 60), nil
	})
	mappings, err := ReadMountMappings(dial, address, tokenSource)
	if err != nil {
		t.Fatalf("read protected mount mappings: %v", err)
	}
	if got := mappings.Mappings["/buckets/archive"].Bucket; got != "bucket" {
		t.Fatalf("mount mapping bucket = %q, want bucket", got)
	}
}
