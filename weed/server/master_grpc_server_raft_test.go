package weed_server

// These tests cover the Raft gRPC handlers in scenarios where Raft is not initialized
// (single master mode). Testing with an initialized Raft cluster requires integration
// tests with a multi-master setup, as hashicorp/raft uses concrete types that cannot
// be easily mocked.
//
// Integration tests for RaftLeadershipTransfer should cover:
// - Successful leadership transfer to any follower (auto-selection)
// - Successful leadership transfer to a specific target server
// - Error when caller is not the current leader
// - Error when target server is not a voting member
// - Error when target server is unreachable
//
// These scenarios are best tested with test/multi_master/ integration tests
// using a real 3-node master cluster with -raftHashicorp=true.

import (
	"context"
	"strings"
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
	if err != nil && !strings.Contains(err.Error(), expectedMsg) {
		t.Errorf("expected error message to contain %q, got %q", expectedMsg, err.Error())
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
