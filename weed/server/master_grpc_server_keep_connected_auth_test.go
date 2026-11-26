package weed_server

import (
	"context"
	"io"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/seaweedfs/seaweedfs/weed/cluster"
	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/security"
	"github.com/seaweedfs/seaweedfs/weed/topology"
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
