package command

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"net"
	"strings"
	"sync"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/security"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

type backupAuthKVFiler struct {
	filer_pb.UnimplementedSeaweedFilerServer
	key security.SigningKey
	mu  sync.Mutex
	kv  map[string][]byte
}

func (s *backupAuthKVFiler) authenticate(ctx context.Context) error {
	md, _ := metadata.FromIncomingContext(ctx)
	values := md.Get("authorization")
	if len(values) != 1 || !strings.HasPrefix(values[0], security.BearerPrefix) ||
		!security.IsFilerAdminToken(s.key, security.EncodedJwt(strings.TrimPrefix(values[0], security.BearerPrefix))) {
		return status.Error(codes.Unauthenticated, "missing or invalid filer authorization")
	}
	return nil
}

func (s *backupAuthKVFiler) KvGet(_ context.Context, request *filer_pb.KvGetRequest) (*filer_pb.KvGetResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return &filer_pb.KvGetResponse{Value: append([]byte(nil), s.kv[string(request.Key)]...)}, nil
}

func (s *backupAuthKVFiler) KvPut(_ context.Context, request *filer_pb.KvPutRequest) (*filer_pb.KvPutResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.kv[string(request.Key)] = append([]byte(nil), request.Value...)
	return &filer_pb.KvPutResponse{}, nil
}

func backupAuthKey(t *testing.T) security.SigningKey {
	t.Helper()
	buf := make([]byte, 32)
	if _, err := rand.Read(buf); err != nil {
		t.Fatal(err)
	}
	return security.SigningKey(hex.EncodeToString(buf))
}

func startBackupAuthKVFiler(t *testing.T, filer *backupAuthKVFiler) (pb.ServerAddress, grpc.DialOption) {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server := grpc.NewServer(grpc.UnaryInterceptor(func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		if err := filer.authenticate(ctx); err != nil {
			return nil, err
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

func TestBackupOffsetUsesProtectedPerCallOperation(t *testing.T) {
	key := backupAuthKey(t)
	filer := &backupAuthKVFiler{key: key, kv: make(map[string][]byte)}
	address, dial := startBackupAuthKVFiler(t, filer)
	tokenSource := security.FilerTokenSource(func(context.Context) (security.EncodedJwt, error) {
		return security.GenJwtForFilerAdmin(key, 60), nil
	})
	prefix := BackupKeyPrefix
	const signature = int32(43)
	if err := setOffset(dial, address, tokenSource, prefix, signature, 1234); err != nil {
		t.Fatalf("protected backup offset write: %v", err)
	}
	got, err := getOffset(dial, address, tokenSource, prefix, signature)
	if err != nil {
		t.Fatalf("protected backup offset read: %v", err)
	}
	if got != 1234 {
		t.Fatalf("backup offset = %d, want 1234", got)
	}

	// Keep this assertion at the stored-value boundary: it proves the real KV
	// calls completed through the protected adapter, while avoiding token output.
	keyBytes := []byte(prefix + "____")
	util.Uint32toBytes(keyBytes[len(prefix):], uint32(signature))
	filer.mu.Lock()
	defer filer.mu.Unlock()
	if _, ok := filer.kv[string(keyBytes)]; !ok {
		t.Fatal("protected backup offset was not persisted")
	}
}
