package weed_server

import (
	"context"
	"github.com/chrislusf/seaweedfs/weed/pb/master_pb"
	"github.com/hashicorp/raft"
)

func (ms *MasterServer) RaftListClusterServers(ctx context.Context, req *master_pb.RaftListClusterServersRequest) (*master_pb.RaftListClusterServersResponse, error) {
	resp := &master_pb.RaftListClusterServersResponse{}

	servers := ms.Topo.HashicorpRaft.GetConfiguration().Configuration().Servers

	for _, server := range servers {
		resp.ClusterServers = append(resp.ClusterServers, &master_pb.RaftListClusterServersResponse_ClusterServers{
			Id:       string(server.ID),
			Address:  string(server.Address),
			Suffrage: server.Suffrage.String(),
		})
	}
	return resp, nil
}

func (ms *MasterServer) RaftAddServer(ctx context.Context, req *master_pb.RaftAddServerRequest) (*master_pb.RaftAddServerResponse, error) {
	resp := &master_pb.RaftAddServerResponse{}

	var idxFuture raft.IndexFuture
	if req.Voter {
		idxFuture = ms.Topo.HashicorpRaft.AddVoter(raft.ServerID(req.Id), raft.ServerAddress(req.Address), 0, 0)
	} else {
		idxFuture = ms.Topo.HashicorpRaft.AddNonvoter(raft.ServerID(req.Id), raft.ServerAddress(req.Address), 0, 0)
	}

	if err := idxFuture.Error(); err != nil {
		return nil, err
	}
	return resp, nil
}

func (ms *MasterServer) RaftRemoveServer(ctx context.Context, req *master_pb.RaftRemoveServerRequest) (*master_pb.RaftRemoveServerResponse, error) {
	resp := &master_pb.RaftRemoveServerResponse{}

	idxFuture := ms.Topo.HashicorpRaft.RemoveServer(raft.ServerID(req.Id), 0, 0)
	if err := idxFuture.Error(); err != nil {
		return nil, err
	}
	return resp, nil
}
