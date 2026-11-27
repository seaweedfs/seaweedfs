package pb

import (
	"context"
	"net"
	"strconv"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/security"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

type retryingAuthFiler struct {
	authRecordingFiler
}

func (f *retryingAuthFiler) LookupDirectoryEntry(ctx context.Context, _ *filer_pb.LookupDirectoryEntryRequest) (*filer_pb.LookupDirectoryEntryResponse, error) {
	f.record(ctx)
	return nil, status.Error(codes.Unavailable, "retry test")
}

func startRetryingAuthFiler(t *testing.T) (*retryingAuthFiler, ServerAddress) {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server := grpc.NewServer()
	filer := &retryingAuthFiler{}
	filer_pb.RegisterSeaweedFilerServer(server, filer)
	go func() {
		_ = server.Serve(listener)
	}()
	t.Cleanup(func() {
		server.Stop()
		_ = listener.Close()
	})
	_, portText, err := net.SplitHostPort(listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	grpcPort, err := strconv.Atoi(portText)
	if err != nil {
		t.Fatal(err)
	}
	return filer, NewServerAddressWithGrpcPort(listener.Addr().String(), grpcPort)
}

func TestWithOneOfGrpcFilerClientsAuthPreservesFailoverAndPerCallSource(t *testing.T) {
	first, firstAddress := startRetryingAuthFiler(t)
	second, secondAddress := startAuthRecordingFiler(t)
	t.Cleanup(func() {
		InvalidateGrpcConnection(firstAddress.ToGrpcAddress())
		InvalidateGrpcConnection(secondAddress.ToGrpcAddress())
	})
	keys := []security.SigningKey{filerAuthTestKey(t), filerAuthTestKey(t)}
	tokens := []security.EncodedJwt{
		security.GenJwtForFilerAdmin(keys[0], 60),
		security.GenJwtForFilerAdmin(keys[1], 60),
	}
	resolutions := 0
	err := WithOneOfGrpcFilerClientsAuth(false, []ServerAddress{firstAddress, secondAddress}, grpc.WithTransportCredentials(insecure.NewCredentials()), func(context.Context) (security.EncodedJwt, error) {
		token := tokens[resolutions]
		resolutions++
		return token, nil
	}, func(client filer_pb.SeaweedFilerClient) error {
		_, err := client.LookupDirectoryEntry(context.Background(), &filer_pb.LookupDirectoryEntryRequest{Directory: "/", Name: "entry"})
		return err
	})
	if err != nil {
		t.Fatalf("failover call: %v", err)
	}
	if resolutions != len(tokens) {
		t.Fatalf("token source called %d times, want %d", resolutions, len(tokens))
	}
	firstAuth := first.authorizationValues()
	if len(firstAuth) != 1 || !validAdminAuthorization(firstAuth[0], keys[0]) {
		t.Fatal("first filer did not receive its per-attempt admin token")
	}
	secondAuth := second.authorizationValues()
	if len(secondAuth) != 1 || !validAdminAuthorization(secondAuth[0], keys[1]) {
		t.Fatal("second filer did not receive its per-attempt admin token")
	}
}
