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
		guard: security.NewGuard([]string{"127.0.0.1"}, "", 0, "", 0, "", ""),
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
		guard: security.NewGuard([]string{"10.13.37.66"}, "", 0, "", 0, "", ""),
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
		guard: security.NewGuard(nil, "", 0, "", 0, "", ""),
	}
	ctx := ctxFromHost("10.13.37.66")

	if _, err := ms.RaftAddServer(ctx, &master_pb.RaftAddServerRequest{Id: "peer", Address: "10.13.37.66:19333", Voter: true}); err != nil {
		t.Errorf("RaftAddServer: empty whitelist should fail open, got %v", err)
	}
}

func TestMasterAdminRPCs_RejectUnwhitelisted(t *testing.T) {
	ms := newAdminLockMaster()
	ms.guard = security.NewGuard([]string{"127.0.0.1"}, "", 0, "", 0, "", "")
	ctx := ctxFromHost("10.13.37.66")

	calls := map[string]func() error{
		"CollectionDelete": func() error {
			_, err := ms.CollectionDelete(ctx, &master_pb.CollectionDeleteRequest{Name: "c"})
			return err
		},
		"DisableVacuum": func() error {
			_, err := ms.DisableVacuum(ctx, &master_pb.DisableVacuumRequest{})
			return err
		},
		"EnableVacuum": func() error {
			_, err := ms.EnableVacuum(ctx, &master_pb.EnableVacuumRequest{})
			return err
		},
		"VolumeGrow": func() error {
			_, err := ms.VolumeGrow(ctx, &master_pb.VolumeGrowRequest{WritableVolumeCount: 1})
			return err
		},
		"ResumeVolumeGrowth": func() error {
			_, err := ms.ResumeVolumeGrowth(ctx, &master_pb.ResumeVolumeGrowthRequest{})
			return err
		},
		"LeaseAdminToken": func() error {
			_, err := ms.LeaseAdminToken(ctx, &master_pb.LeaseAdminTokenRequest{LockName: "admin"})
			return err
		},
		"ReleaseAdminToken": func() error {
			_, err := ms.ReleaseAdminToken(ctx, &master_pb.ReleaseAdminTokenRequest{LockName: "admin"})
			return err
		},
	}
	for name, call := range calls {
		if err := call(); status.Code(err) != codes.PermissionDenied {
			t.Errorf("%s: expected PermissionDenied, got %v", name, err)
		}
	}
	if ms.Topo.IsVacuumDisabled() {
		t.Error("DisableVacuum from an unwhitelisted caller disabled vacuum")
	}
}

func TestMasterAdminRPCs_AllowLocalSocketAndGrowthLoop(t *testing.T) {
	ms := &MasterServer{
		Topo:  topology.NewTopology("test", nil, 0, 0, false),
		guard: security.NewGuard([]string{"10.0.0.1"}, "", 0, "", 0, "", ""),
	}
	local := peer.NewContext(context.Background(), &peer.Peer{Addr: &net.UnixAddr{Name: "@", Net: "unix"}})
	if _, err := ms.DisableVacuum(local, &master_pb.DisableVacuumRequest{}); err != nil {
		t.Fatalf("DisableVacuum over the local socket: %v", err)
	}
	if !ms.Topo.IsVacuumDisabled() {
		t.Error("DisableVacuum over the local socket did not disable vacuum")
	}
	if _, err := ms.VolumeGrow(context.Background(), &master_pb.VolumeGrowRequest{WritableVolumeCount: 1}); status.Code(err) == codes.PermissionDenied {
		t.Errorf("in-process VolumeGrow was refused: %v", err)
	}
}
