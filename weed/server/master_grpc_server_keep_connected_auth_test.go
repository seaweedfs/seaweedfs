package weed_server

import (
	"context"
	"io"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/seaweedfs/seaweedfs/weed/cluster"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/security"
	"github.com/seaweedfs/seaweedfs/weed/topology"
	"github.com/seaweedfs/seaweedfs/weed/wdclient"
)

func TestKeepConnected_RejectsUnknownMasterFromUnwhitelistedPeer(t *testing.T) {
	leader, _ := newInmemRaftLeader(t, "localhost:9333")
	topo := topology.NewTopology("test", nil, 0, 0, false)
	topo.HashicorpRaft = leader
	ms := &MasterServer{
		option: &MasterOption{Master: "localhost:9333"},
		Topo:   topo,
		guard:  security.NewGuard([]string{"127.0.0.1"}, "", 0, "", 0, "", ""),
	}
	stream := &keepConnectedOnce{
		ctx: ctxFromHost("10.13.37.66"),
		req: &master_pb.KeepConnectedRequest{ClientType: cluster.MasterType, ClientAddress: "10.99.99.99:19999"},
	}
	if err := ms.KeepConnected(stream); status.Code(err) != codes.PermissionDenied {
		t.Fatalf("expected PermissionDenied, got %v", err)
	}
	if servers := leader.GetConfiguration().Configuration().Servers; len(servers) != 1 {
		t.Fatalf("unknown master was admitted into raft: %v", servers)
	}
}

func TestKeepConnected_RejectsConfiguredPeerHostOnAnotherGrpcPort(t *testing.T) {
	leader, _ := newInmemRaftLeader(t, "localhost:9333")
	topo := topology.NewTopology("test", nil, 0, 0, false)
	topo.HashicorpRaft = leader
	peers := map[string]pb.ServerAddress{"10.0.0.2:9333": "10.0.0.2:9333.19333"}
	ms := &MasterServer{
		option:       &MasterOption{Master: "localhost:9333"},
		Topo:         topo,
		guard:        security.NewGuard([]string{"127.0.0.1"}, "", 0, "", 0, "", ""),
		MasterClient: wdclient.NewMasterClient(grpc.EmptyDialOption{}, "", cluster.MasterType, "localhost:9333", "", "", *pb.NewServiceDiscoveryFromMap(peers)),
	}
	stream := &keepConnectedOnce{
		ctx: ctxFromHost("10.13.37.66"),
		req: &master_pb.KeepConnectedRequest{ClientType: cluster.MasterType, ClientAddress: "10.0.0.2:9333.29999"},
	}
	if err := ms.KeepConnected(stream); status.Code(err) != codes.PermissionDenied {
		t.Fatalf("expected PermissionDenied, got %v", err)
	}
	if servers := leader.GetConfiguration().Configuration().Servers; len(servers) != 1 {
		t.Fatalf("caller-chosen endpoint was admitted into raft: %v", servers)
	}
	for _, addr := range []pb.ServerAddress{"10.0.0.2:9333", "10.0.0.2:9333.19333", "localhost:9333.19333"} {
		if !ms.isConfiguredMaster(addr) {
			t.Errorf("isConfiguredMaster(%q) = false, want true", addr)
		}
	}
	if ms.isConfiguredMaster("10.0.0.2:9333.29999") {
		t.Errorf("isConfiguredMaster accepted a configured host on another gRPC port")
	}
}

type keepConnectedOnce struct {
	grpc.ServerStream
	ctx  context.Context
	req  *master_pb.KeepConnectedRequest
	sent bool
}

func (s *keepConnectedOnce) Context() context.Context { return s.ctx }

func (s *keepConnectedOnce) Recv() (*master_pb.KeepConnectedRequest, error) {
	if s.sent {
		return nil, io.EOF
	}
	s.sent = true
	return s.req, nil
}

func (s *keepConnectedOnce) Send(*master_pb.KeepConnectedResponse) error { return nil }
