package shell

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"net"
	"strconv"
	"sync"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/security"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

type shellAuthFiler struct {
	filer_pb.UnimplementedSeaweedFilerServer

	mu      sync.Mutex
	updates []*filer_pb.UpdateEntryRequest
}

func (s *shellAuthFiler) LookupDirectoryEntry(context.Context, *filer_pb.LookupDirectoryEntryRequest) (*filer_pb.LookupDirectoryEntryResponse, error) {
	return &filer_pb.LookupDirectoryEntryResponse{Entry: &filer_pb.Entry{
		Name:       filer.FilerConfName,
		Attributes: &filer_pb.FuseAttributes{},
	}}, nil
}

func (s *shellAuthFiler) UpdateEntry(_ context.Context, req *filer_pb.UpdateEntryRequest) (*filer_pb.UpdateEntryResponse, error) {
	s.mu.Lock()
	s.updates = append(s.updates, req)
	s.mu.Unlock()
	return &filer_pb.UpdateEntryResponse{}, nil
}

func shellAuthCheck(ctx context.Context, key security.SigningKey, require bool) error {
	md, _ := metadata.FromIncomingContext(ctx)
	values := md.Get("authorization")
	if len(key) == 0 {
		if require && len(values) == 0 {
			return status.Error(codes.Unauthenticated, "authentication required")
		}
		if len(values) == 0 {
			return nil
		}
	}
	if len(values) != 1 {
		return status.Error(codes.Unauthenticated, "authentication required")
	}
	const prefix = "Bearer "
	if len(values[0]) <= len(prefix) || values[0][:len(prefix)] != prefix || !security.IsFilerAdminToken(key, security.EncodedJwt(values[0][len(prefix):])) {
		return status.Error(codes.Unauthenticated, "invalid authentication")
	}
	return nil
}

func startShellAuthFiler(t *testing.T, key security.SigningKey, require bool) (*shellAuthFiler, pb.ServerAddress) {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal("listen")
	}
	filerServer := &shellAuthFiler{}
	server := grpc.NewServer(
		grpc.UnaryInterceptor(func(ctx context.Context, req any, _ *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
			if err := shellAuthCheck(ctx, key, require); err != nil {
				return nil, err
			}
			return handler(ctx, req)
		}),
		grpc.StreamInterceptor(func(srv any, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
			if err := shellAuthCheck(stream.Context(), key, require); err != nil {
				return err
			}
			return handler(srv, stream)
		}),
	)
	filer_pb.RegisterSeaweedFilerServer(server, filerServer)
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(func() {
		server.Stop()
		_ = listener.Close()
		pb.InvalidateGrpcConnection(shellAuthAddress(listener.Addr().String()).ToGrpcAddress())
	})
	return filerServer, shellAuthAddress(listener.Addr().String())
}

func shellAuthAddress(address string) pb.ServerAddress {
	_, portText, err := net.SplitHostPort(address)
	if err != nil {
		return pb.ServerAddress(address)
	}
	port, err := strconv.Atoi(portText)
	if err != nil {
		return pb.ServerAddress(address)
	}
	return pb.NewServerAddressWithGrpcPort(address, port)
}

func shellTestKey(t *testing.T) security.SigningKey {
	t.Helper()
	bytes := make([]byte, 32)
	if _, err := rand.Read(bytes); err != nil {
		t.Fatal("generate disposable signing key")
	}
	return security.SigningKey(hex.EncodeToString(bytes))
}

func TestShellProtectedNamespaceOperationUsesPerCallAdminToken(t *testing.T) {
	key := shellTestKey(t)
	otherKey := shellTestKey(t)
	for _, tc := range []struct {
		name    string
		require bool
		source  security.FilerTokenSource
		want    codes.Code
	}{
		{
			name:    "valid token",
			require: true,
			source: func(context.Context) (security.EncodedJwt, error) {
				return security.GenJwtForFilerAdmin(key, 10), nil
			},
			want: codes.OK,
		},
		{
			name:    "wrong key",
			require: true,
			source: func(context.Context) (security.EncodedJwt, error) {
				return security.GenJwtForFilerAdmin(otherKey, 10), nil
			},
			want: codes.Unauthenticated,
		},
		{
			name:    "missing token",
			require: true,
			want:    codes.Unauthenticated,
		},
		{
			name:    "optional no key",
			require: false,
			want:    codes.OK,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			serverKey := key
			if tc.name == "optional no key" {
				serverKey = nil
			}
			server, address := startShellAuthFiler(t, serverKey, tc.require)
			env := &CommandEnv{option: &ShellOptions{
				FilerAddress:     address,
				GrpcDialOption:   grpc.WithTransportCredentials(insecure.NewCredentials()),
				FilerTokenSource: tc.source,
			}}
			err := env.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
				return filer.SaveInsideFiler(context.Background(), client, filer.DirectoryEtcSeaweedFS, filer.FilerConfName, []byte("protected shell config"))
			})
			if status.Code(err) != tc.want {
				t.Fatalf("protected shell operation code=%v, want %v", status.Code(err), tc.want)
			}
			server.mu.Lock()
			updates := len(server.updates)
			server.mu.Unlock()
			if tc.want == codes.OK && updates != 1 {
				t.Fatalf("protected shell operation updates=%d, want one", updates)
			}
			if tc.want != codes.OK && updates != 0 {
				t.Fatalf("refused protected shell operation reached handler")
			}
		})
	}
}
