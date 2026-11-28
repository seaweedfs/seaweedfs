package dash

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"net"
	"strconv"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/security"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

type dashAuthFiler struct {
	filer_pb.UnimplementedSeaweedFilerServer
}

func (dashAuthFiler) GetFilerConfiguration(context.Context, *filer_pb.GetFilerConfigurationRequest) (*filer_pb.GetFilerConfigurationResponse, error) {
	return &filer_pb.GetFilerConfigurationResponse{DirBuckets: "/buckets", FilerGroup: "test"}, nil
}

func (dashAuthFiler) LookupDirectoryEntry(context.Context, *filer_pb.LookupDirectoryEntryRequest) (*filer_pb.LookupDirectoryEntryResponse, error) {
	return &filer_pb.LookupDirectoryEntryResponse{Entry: &filer_pb.Entry{}}, nil
}

func dashAuthCheck(ctx context.Context, key security.SigningKey, require bool) error {
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

func dashAuthAddress(address string) pb.ServerAddress {
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

func startDashAuthFiler(t *testing.T, key security.SigningKey, require bool) pb.ServerAddress {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal("listen")
	}
	server := grpc.NewServer(
		grpc.UnaryInterceptor(func(ctx context.Context, req any, _ *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
			if err := dashAuthCheck(ctx, key, require); err != nil {
				return nil, err
			}
			return handler(ctx, req)
		}),
		grpc.StreamInterceptor(func(srv any, stream grpc.ServerStream, _ *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
			if err := dashAuthCheck(stream.Context(), key, require); err != nil {
				return err
			}
			return handler(srv, stream)
		}),
	)
	filer_pb.RegisterSeaweedFilerServer(server, dashAuthFiler{})
	go func() { _ = server.Serve(listener) }()
	address := dashAuthAddress(listener.Addr().String())
	t.Cleanup(func() {
		server.Stop()
		_ = listener.Close()
		pb.InvalidateGrpcConnection(address.ToGrpcAddress())
	})
	return address
}

func dashTestKey(t *testing.T) security.SigningKey {
	t.Helper()
	bytes := make([]byte, 32)
	if _, err := rand.Read(bytes); err != nil {
		t.Fatal("generate disposable signing key")
	}
	return security.SigningKey(hex.EncodeToString(bytes))
}

func TestAdminFilerReadsUsePerCallAdminToken(t *testing.T) {
	key := dashTestKey(t)
	otherKey := dashTestKey(t)
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
			address := startDashAuthFiler(t, serverKey, tc.require)
			server := &AdminServer{
				grpcDialOption:       grpc.WithTransportCredentials(insecure.NewCredentials()),
				filerTokenSource:     tc.source,
				cachedFilers:         []string{string(address)},
				lastFilerUpdate:      time.Now(),
				filerCacheExpiration: time.Hour,
			}
			config, err := server.getFilerConfig()
			if status.Code(err) != tc.want {
				t.Fatalf("admin filer configuration read code=%v, want %v", status.Code(err), tc.want)
			}
			if tc.want == codes.OK {
				if config.BucketsPath != "/buckets" || config.FilerGroup != "test" {
					t.Fatal("admin filer configuration response was not preserved")
				}
				if _, err := server.getFilerConf(); err != nil {
					t.Fatal("admin filer.conf read was refused")
				}
			}
		})
	}
}
