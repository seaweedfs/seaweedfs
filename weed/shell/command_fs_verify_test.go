package shell

import (
	"bytes"
	"context"
	"fmt"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/wdclient"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

type fsVerifyMasterServer struct {
	master_pb.UnimplementedSeaweedServer
}

func (s *fsVerifyMasterServer) KeepConnected(stream grpc.BidiStreamingServer[master_pb.KeepConnectedRequest, master_pb.KeepConnectedResponse]) error {
	if _, err := stream.Recv(); err != nil {
		return err
	}
	if err := stream.Send(&master_pb.KeepConnectedResponse{}); err != nil {
		return err
	}
	<-stream.Context().Done()
	return stream.Context().Err()
}

func (s *fsVerifyMasterServer) VolumeList(context.Context, *master_pb.VolumeListRequest) (*master_pb.VolumeListResponse, error) {
	return nil, status.Error(codes.Unavailable, "volume list unavailable")
}

type fsVerifyFilerServer struct {
	filer_pb.UnimplementedSeaweedFilerServer
}

func (s *fsVerifyFilerServer) LookupDirectoryEntry(_ context.Context, req *filer_pb.LookupDirectoryEntryRequest) (*filer_pb.LookupDirectoryEntryResponse, error) {
	if req.Directory == "/" && req.Name == "" {
		return &filer_pb.LookupDirectoryEntryResponse{
			Entry: &filer_pb.Entry{
				Name:        "",
				IsDirectory: true,
				Attributes:  &filer_pb.FuseAttributes{},
			},
		}, nil
	}
	return nil, status.Error(codes.NotFound, "not found")
}

func (s *fsVerifyFilerServer) ListEntries(*filer_pb.ListEntriesRequest, grpc.ServerStreamingServer[filer_pb.ListEntriesResponse]) error {
	return nil
}

func TestFsVerifyPropagatesCollectVolumeIdsError(t *testing.T) {
	commandEnv, cleanup := newFsVerifyTestCommandEnv(t)
	defer cleanup()

	var output bytes.Buffer
	err := (&commandFsVerify{}).Do([]string{"/"}, commandEnv, &output)
	if err == nil {
		t.Fatal("expected volume list error, got nil")
	}
	if !strings.Contains(err.Error(), "volume list unavailable") {
		t.Fatalf("expected volume list error, got %v", err)
	}
}

func newFsVerifyTestCommandEnv(t *testing.T) (*CommandEnv, func()) {
	t.Helper()

	masterAddress, stopMaster := startFsVerifyMasterServer(t)
	filerAddress, stopFiler := startFsVerifyFilerServer(t)

	masters := pb.ServerAddresses(string(masterAddress)).ToServiceDiscovery()
	masterClient := wdclient.NewMasterClient(
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		"", "test", "", "", "",
		*masters,
	)
	ctx, cancel := context.WithCancel(context.Background())
	go masterClient.KeepConnectedToMaster(ctx)

	waitCtx, waitCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer waitCancel()
	if master := masterClient.GetMaster(waitCtx); master == "" {
		cancel()
		stopMaster()
		stopFiler()
		t.Fatal("master client did not connect")
	}

	cleanup := func() {
		cancel()
		stopMaster()
		stopFiler()
	}

	return &CommandEnv{
		MasterClient: masterClient,
		option: &ShellOptions{
			FilerAddress:   filerAddress,
			GrpcDialOption: grpc.WithTransportCredentials(insecure.NewCredentials()),
			Directory:      "/",
		},
	}, cleanup
}

func startFsVerifyMasterServer(t *testing.T) (pb.ServerAddress, func()) {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen master: %v", err)
	}
	grpcServer := grpc.NewServer()
	master_pb.RegisterSeaweedServer(grpcServer, &fsVerifyMasterServer{})
	go func() {
		_ = grpcServer.Serve(listener)
	}()

	_, port, err := net.SplitHostPort(listener.Addr().String())
	if err != nil {
		t.Fatalf("split master address: %v", err)
	}
	return pb.ServerAddress(fmt.Sprintf("127.0.0.1:9333.%s", port)), func() {
		grpcServer.Stop()
		_ = listener.Close()
	}
}

func startFsVerifyFilerServer(t *testing.T) (pb.ServerAddress, func()) {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen filer: %v", err)
	}
	grpcServer := grpc.NewServer()
	filer_pb.RegisterSeaweedFilerServer(grpcServer, &fsVerifyFilerServer{})
	go func() {
		_ = grpcServer.Serve(listener)
	}()

	_, port, err := net.SplitHostPort(listener.Addr().String())
	if err != nil {
		t.Fatalf("split filer address: %v", err)
	}
	return pb.ServerAddress(fmt.Sprintf("127.0.0.1:8888.%s", port)), func() {
		grpcServer.Stop()
		_ = listener.Close()
	}
}
