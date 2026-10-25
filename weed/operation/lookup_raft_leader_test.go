package operation

import (
	"context"
	"errors"
	"fmt"
	"net"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type fakeLeaderConfigServer struct {
	master_pb.UnimplementedSeaweedServer
	leader string
}

func (s *fakeLeaderConfigServer) GetMasterConfiguration(_ context.Context, _ *master_pb.GetMasterConfigurationRequest) (*master_pb.GetMasterConfigurationResponse, error) {
	return &master_pb.GetMasterConfigurationResponse{Leader: s.leader}, nil
}

func startFakeMasterServer(t *testing.T, srv master_pb.SeaweedServer) pb.ServerAddress {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	grpcServer := grpc.NewServer()
	master_pb.RegisterSeaweedServer(grpcServer, srv)
	go func() { _ = grpcServer.Serve(lis) }()
	t.Cleanup(grpcServer.GracefulStop)

	_, port, _ := net.SplitHostPort(lis.Addr().String())
	return pb.ServerAddress(fmt.Sprintf("127.0.0.1:0.%s", port))
}

func TestLookupRaftLeaderMasterFromFollower(t *testing.T) {
	follower := startFakeMasterServer(t, &fakeLeaderConfigServer{
		leader: "leader.example.com:9333.19333",
	})

	leader, err := LookupRaftLeaderMaster(
		context.Background(),
		[]pb.ServerAddress{follower},
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("lookup: %v", err)
	}
	if got, want := leader.ToHttpAddress(), "leader.example.com:9333"; got != want {
		t.Fatalf("leader = %q, want %q", got, want)
	}
}

func TestLookupRaftLeaderMasterTriesNextPeer(t *testing.T) {
	down := pb.ServerAddress("127.0.0.1:1")
	peer := startFakeMasterServer(t, &fakeLeaderConfigServer{
		leader: "leader.example.com:9333.19333",
	})

	leader, err := LookupRaftLeaderMaster(
		context.Background(),
		[]pb.ServerAddress{down, peer},
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("lookup: %v", err)
	}
	if got, want := leader.ToHttpAddress(), "leader.example.com:9333"; got != want {
		t.Fatalf("leader = %q, want %q", got, want)
	}
}

func TestLookupRaftLeaderMasterNoPeers(t *testing.T) {
	_, err := LookupRaftLeaderMaster(context.Background(), nil, nil)
	if err == nil {
		t.Fatal("expected error for empty peers")
	}
}

func TestLookupRaftLeaderMasterCanceledContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := LookupRaftLeaderMaster(
		ctx,
		[]pb.ServerAddress{"127.0.0.1:1"},
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err == nil {
		t.Fatal("expected error for canceled context")
	}
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}
}
