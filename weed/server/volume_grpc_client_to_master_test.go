package weed_server

import (
	"context"
	"fmt"
	"net"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func TestLookupRaftLeaderMasterDoesNotUpdateCurrentMaster(t *testing.T) {
	follower := startFakeMasterServerForLeaderLookup(t, &fakeLeaderConfigServer{
		leader: "leader.example.com:9333.19333",
	})
	heartbeatPeer := pb.ServerAddress("10.0.0.1:9333")
	vs := &VolumeServer{
		SeedMasterNodes: []pb.ServerAddress{follower},
		grpcDialOption:  grpc.WithTransportCredentials(insecure.NewCredentials()),
		store:           &storage.Store{Ip: "127.0.0.1", Port: 8080},
	}
	vs.setCurrentMaster(heartbeatPeer)

	leader, err := vs.lookupRaftLeaderMaster(context.Background())
	if err != nil {
		t.Fatalf("lookup: %v", err)
	}
	if got, want := leader.ToHttpAddress(), "leader.example.com:9333"; got != want {
		t.Fatalf("leader = %q, want %q", got, want)
	}
	if got := vs.getCurrentMaster(); got != heartbeatPeer {
		t.Fatalf("currentMaster = %v, want unchanged %v", got, heartbeatPeer)
	}
}

type fakeLeaderConfigServer struct {
	master_pb.UnimplementedSeaweedServer
	leader string
}

func (s *fakeLeaderConfigServer) GetMasterConfiguration(_ context.Context, _ *master_pb.GetMasterConfigurationRequest) (*master_pb.GetMasterConfigurationResponse, error) {
	return &master_pb.GetMasterConfigurationResponse{Leader: s.leader}, nil
}

func startFakeMasterServerForLeaderLookup(t *testing.T, srv master_pb.SeaweedServer) pb.ServerAddress {
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
