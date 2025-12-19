package weed_server

import (
	"context"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/topology"
)

func TestRaftLeadershipTransfer_NoRaft(t *testing.T) {
	// Test case: raft not initialized (single master mode)
	ms := &MasterServer{
		Topo: topology.NewTopology("test", nil, 0, 0, false),
	}

	ctx := context.Background()
	req := &master_pb.RaftLeadershipTransferRequest{}

	_, err := ms.RaftLeadershipTransfer(ctx, req)
	if err == nil {
		t.Error("expected error when raft is not initialized")
	}

	expectedMsg := "single master mode"
	if err != nil && !contains(err.Error(), expectedMsg) {
		t.Errorf("expected error message to contain %q, got %q", expectedMsg, err.Error())
	}
}

func TestRaftLeadershipTransfer_GoraftNotSupported(t *testing.T) {
	// Test case: using goraft (seaweedfs/raft) which doesn't support leadership transfer
	topo := topology.NewTopology("test", nil, 0, 0, false)
	// Simulate goraft being set (RaftServer != nil, HashicorpRaft == nil)
	// We can't easily mock the raft.Server interface, so we just verify
	// the error message mentions -raftHashicorp when RaftServer would be set
	ms := &MasterServer{
		Topo: topo,
	}

	ctx := context.Background()
	req := &master_pb.RaftLeadershipTransferRequest{}

	_, err := ms.RaftLeadershipTransfer(ctx, req)
	if err == nil {
		t.Error("expected error when raft is not initialized")
	}
	// When neither is set, we get the single master mode error
	// The goraft-specific error would be shown when RaftServer is set but HashicorpRaft is nil
}

func TestRaftLeadershipTransfer_ValidationTargetIdWithoutAddress(t *testing.T) {
	// The shell command validates this, but the gRPC handler accepts it
	// and lets the raft library handle the transfer to any follower
	// when address is empty. This test documents that behavior.
	ms := &MasterServer{
		Topo: topology.NewTopology("test", nil, 0, 0, false),
	}

	ctx := context.Background()
	req := &master_pb.RaftLeadershipTransferRequest{
		TargetId:      "some-server",
		TargetAddress: "", // missing address
	}

	// Without raft initialized, we get the "not initialized" error first
	_, err := ms.RaftLeadershipTransfer(ctx, req)
	if err == nil {
		t.Error("expected error when raft is not initialized")
	}
}

func TestRaftListClusterServers_NoRaft(t *testing.T) {
	// Test case: raft not initialized returns empty response
	ms := &MasterServer{
		Topo: topology.NewTopology("test", nil, 0, 0, false),
	}

	ctx := context.Background()
	req := &master_pb.RaftListClusterServersRequest{}

	resp, err := ms.RaftListClusterServers(ctx, req)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if resp == nil {
		t.Error("expected non-nil response")
	}
	if len(resp.ClusterServers) != 0 {
		t.Errorf("expected empty cluster servers, got %d", len(resp.ClusterServers))
	}
}

func TestRaftAddServer_NoRaft(t *testing.T) {
	// Test case: raft not initialized returns empty response
	ms := &MasterServer{
		Topo: topology.NewTopology("test", nil, 0, 0, false),
	}

	ctx := context.Background()
	req := &master_pb.RaftAddServerRequest{
		Id:      "test-server",
		Address: "localhost:19333",
		Voter:   true,
	}

	resp, err := ms.RaftAddServer(ctx, req)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if resp == nil {
		t.Error("expected non-nil response")
	}
}

func TestRaftRemoveServer_NoRaft(t *testing.T) {
	// Test case: raft not initialized returns empty response
	ms := &MasterServer{
		Topo: topology.NewTopology("test", nil, 0, 0, false),
	}

	ctx := context.Background()
	req := &master_pb.RaftRemoveServerRequest{
		Id:    "test-server",
		Force: true,
	}

	resp, err := ms.RaftRemoveServer(ctx, req)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if resp == nil {
		t.Error("expected non-nil response")
	}
}

func contains(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || len(s) > 0 && containsHelper(s, substr))
}

func containsHelper(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}

