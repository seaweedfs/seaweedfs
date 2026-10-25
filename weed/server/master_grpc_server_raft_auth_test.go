package weed_server

import (
	"context"
	"net"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"

	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/security"
	"github.com/seaweedfs/seaweedfs/weed/topology"
)

// ctxFromHost builds a gRPC context whose peer IP is host, so the whitelist gate
// sees a concrete caller instead of the in-process "@" address tests default to.
func ctxFromHost(host string) context.Context {
	return peer.NewContext(context.Background(), &peer.Peer{
		Addr: &net.TCPAddr{IP: net.ParseIP(host), Port: 40000},
	})
}

// TestRaftMembershipRPCs_RejectUnwhitelisted verifies the mutating raft
// membership RPCs deny a caller that isn't in a configured whitelist. Before the
// gate they accepted any caller and could rewrite raft quorum.
func TestRaftMembershipRPCs_RejectUnwhitelisted(t *testing.T) {
	ms := &MasterServer{
		Topo:  topology.NewTopology("test", nil, 0, 0, false),
		guard: security.NewGuard([]string{"127.0.0.1"}, "", 0, "", 0),
	}
	ctx := ctxFromHost("10.13.37.66")

	assertDenied := func(name string, err error) {
		t.Helper()
		if status.Code(err) != codes.PermissionDenied {
			t.Errorf("%s: expected PermissionDenied, got %v", name, err)
		}
	}

	_, err := ms.RaftAddServer(ctx, &master_pb.RaftAddServerRequest{Id: "evil", Address: "10.13.37.66:19333", Voter: true})
	assertDenied("RaftAddServer", err)

	_, err = ms.RaftRemoveServer(ctx, &master_pb.RaftRemoveServerRequest{Id: "127.0.0.1:9333"})
	assertDenied("RaftRemoveServer", err)

	_, err = ms.RaftLeadershipTransfer(ctx, &master_pb.RaftLeadershipTransferRequest{})
	assertDenied("RaftLeadershipTransfer", err)
}

// TestRaftMembershipRPCs_AllowWhitelisted confirms a whitelisted caller passes
// the gate and reaches the handler body (which no-ops here because raft is not
// initialized in single-master test mode).
func TestRaftMembershipRPCs_AllowWhitelisted(t *testing.T) {
	ms := &MasterServer{
		Topo:  topology.NewTopology("test", nil, 0, 0, false),
		guard: security.NewGuard([]string{"10.13.37.66"}, "", 0, "", 0),
	}
	ctx := ctxFromHost("10.13.37.66")

	if _, err := ms.RaftAddServer(ctx, &master_pb.RaftAddServerRequest{Id: "peer", Address: "10.13.37.66:19333", Voter: true}); err != nil {
		t.Errorf("RaftAddServer: whitelisted caller unexpectedly rejected: %v", err)
	}
	if _, err := ms.RaftRemoveServer(ctx, &master_pb.RaftRemoveServerRequest{Id: "peer"}); err != nil {
		t.Errorf("RaftRemoveServer: whitelisted caller unexpectedly rejected: %v", err)
	}
}

// TestRaftMembershipRPCs_NoWhitelistFailsOpen documents that with no whitelist
// configured the gate allows every caller, matching the volume server's admin
// gate and keeping default / single-master deployments working.
func TestRaftMembershipRPCs_NoWhitelistFailsOpen(t *testing.T) {
	ms := &MasterServer{
		Topo:  topology.NewTopology("test", nil, 0, 0, false),
		guard: security.NewGuard(nil, "", 0, "", 0),
	}
	ctx := ctxFromHost("10.13.37.66")

	if _, err := ms.RaftAddServer(ctx, &master_pb.RaftAddServerRequest{Id: "peer", Address: "10.13.37.66:19333", Voter: true}); err != nil {
		t.Errorf("RaftAddServer: empty whitelist should fail open, got %v", err)
	}
}
