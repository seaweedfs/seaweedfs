package engine

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"net"
	"strings"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/security"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

type queryAuthFiler struct {
	filer_pb.UnimplementedSeaweedFilerServer
	key         security.SigningKey
	authResults chan bool
}

func (f *queryAuthFiler) LookupDirectoryEntry(ctx context.Context, req *filer_pb.LookupDirectoryEntryRequest) (*filer_pb.LookupDirectoryEntryResponse, error) {
	md, _ := metadata.FromIncomingContext(ctx)
	values := md.Get("authorization")
	valid := len(values) == 1 && strings.HasPrefix(values[0], security.BearerPrefix) &&
		security.IsFilerAdminToken(f.key, security.EncodedJwt(strings.TrimPrefix(values[0], security.BearerPrefix)))
	f.authResults <- valid
	if !valid {
		return nil, status.Error(codes.Unauthenticated, "missing or invalid filer authorization")
	}
	return &filer_pb.LookupDirectoryEntryResponse{
		Entry: &filer_pb.Entry{Name: req.Name, Attributes: &filer_pb.FuseAttributes{}},
	}, nil
}

func queryTestSigningKey(t *testing.T) security.SigningKey {
	t.Helper()
	bytes := make([]byte, 32)
	if _, err := rand.Read(bytes); err != nil {
		t.Fatal(err)
	}
	return security.SigningKey(hex.EncodeToString(bytes))
}

func TestFilerClientImplSendsAdminToken(t *testing.T) {
	key := queryTestSigningKey(t)
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	filer := &queryAuthFiler{key: key, authResults: make(chan bool, 1)}
	server := grpc.NewServer()
	filer_pb.RegisterSeaweedFilerServer(server, filer)
	go server.Serve(listener)
	t.Cleanup(func() {
		server.Stop()
		_ = listener.Close()
	})

	client := &filerClientImpl{
		filerAddress:   listener.Addr().String(),
		grpcDialOption: grpc.WithTransportCredentials(insecure.NewCredentials()),
		tokenSource: func(context.Context) (security.EncodedJwt, error) {
			return security.GenJwtForFilerAdmin(key, 60), nil
		},
	}
	err = client.WithFilerClient(false, func(filerClient filer_pb.SeaweedFilerClient) error {
		_, err := filerClient.LookupDirectoryEntry(context.Background(), &filer_pb.LookupDirectoryEntryRequest{Directory: "/", Name: "entry"})
		return err
	})
	if err != nil {
		t.Fatalf("WithFilerClient: %v", err)
	}
	select {
	case valid := <-filer.authResults:
		if !valid {
			t.Fatal("filer rejected the per-call admin token")
		}
	default:
		t.Fatal("filer did not receive the request")
	}
}
