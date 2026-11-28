package broker

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"net"
	"strconv"
	"strings"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/security"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

type brokerAuthFiler struct {
	filer_pb.UnimplementedSeaweedFilerServer
	key         security.SigningKey
	authResults chan bool
}

func (f *brokerAuthFiler) LookupDirectoryEntry(ctx context.Context, req *filer_pb.LookupDirectoryEntryRequest) (*filer_pb.LookupDirectoryEntryResponse, error) {
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

func brokerTestSigningKey(t *testing.T) security.SigningKey {
	t.Helper()
	bytes := make([]byte, 32)
	if _, err := rand.Read(bytes); err != nil {
		t.Fatal(err)
	}
	return security.SigningKey(hex.EncodeToString(bytes))
}

func TestWithFilerClientSendsAdminToken(t *testing.T) {
	key := brokerTestSigningKey(t)
	t.Setenv("WEED_JWT_FILER_SIGNING_GRPC_KEY", string(key))
	t.Setenv("WEED_JWT_FILER_SIGNING_KEY", "")

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	_, portText, err := net.SplitHostPort(listener.Addr().String())
	if err != nil {
		t.Fatalf("split listener address: %v", err)
	}
	grpcPort, err := strconv.Atoi(portText)
	if err != nil {
		t.Fatalf("parse listener port: %v", err)
	}
	filer := &brokerAuthFiler{key: key, authResults: make(chan bool, 1)}
	server := grpc.NewServer()
	filer_pb.RegisterSeaweedFilerServer(server, filer)
	go server.Serve(listener)
	t.Cleanup(func() {
		server.Stop()
		_ = listener.Close()
	})

	broker := &MessageQueueBroker{
		currentFiler:   pb.NewServerAddressWithGrpcPort(listener.Addr().String(), grpcPort),
		grpcDialOption: grpc.WithTransportCredentials(insecure.NewCredentials()),
	}
	err = broker.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		_, err := client.LookupDirectoryEntry(context.Background(), &filer_pb.LookupDirectoryEntryRequest{Directory: "/", Name: "entry"})
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
