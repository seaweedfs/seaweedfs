package weed_server

import (
	"context"
	"fmt"

	"github.com/hashicorp/raft"

	"github.com/seaweedfs/seaweedfs/weed/cluster"
	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
)

func (ms *MasterServer) RaftListClusterServers(ctx context.Context, req *master_pb.RaftListClusterServersRequest) (*master_pb.RaftListClusterServersResponse, error) {
	resp := &master_pb.RaftListClusterServersResponse{}

	ms.Topo.RaftServerAccessLock.RLock()
	if ms.Topo.HashicorpRaft == nil && ms.Topo.RaftServer == nil {
		ms.Topo.RaftServerAccessLock.RUnlock()
		return resp, nil
	}

	if ms.Topo.HashicorpRaft != nil {
		servers := ms.Topo.HashicorpRaft.GetConfiguration().Configuration().Servers
		_, leaderId := ms.Topo.HashicorpRaft.LeaderWithID()
		ms.Topo.RaftServerAccessLock.RUnlock()

		for _, server := range servers {
			resp.ClusterServers = append(resp.ClusterServers, &master_pb.RaftListClusterServersResponse_ClusterServers{
				Id:       string(server.ID),
				Address:  string(server.Address),
				Suffrage: server.Suffrage.String(),
				IsLeader: server.ID == leaderId,
			})
		}
	} else if ms.Topo.RaftServer != nil {
		peers := ms.Topo.RaftServer.Peers()
		leader := ms.Topo.RaftServer.Leader()
		currentServerName := ms.Topo.RaftServer.Name()
		ms.Topo.RaftServerAccessLock.RUnlock()

		// Add the current server itself (Peers() only returns other peers)
		resp.ClusterServers = append(resp.ClusterServers, &master_pb.RaftListClusterServersResponse_ClusterServers{
			Id:       currentServerName,
			Address:  string(ms.option.Master),
			Suffrage: "Voter",
			IsLeader: currentServerName == leader,
		})

		// Add all other peers
		for _, peer := range peers {
			resp.ClusterServers = append(resp.ClusterServers, &master_pb.RaftListClusterServersResponse_ClusterServers{
				Id:       peer.Name,
				Address:  peer.ConnectionString,
				Suffrage: "Voter",
				IsLeader: peer.Name == leader,
			})
		}
	}
	return resp, nil
}

func (ms *MasterServer) RaftAddServer(ctx context.Context, req *master_pb.RaftAddServerRequest) (*master_pb.RaftAddServerResponse, error) {
	resp := &master_pb.RaftAddServerResponse{}

	ms.Topo.RaftServerAccessLock.RLock()
	defer ms.Topo.RaftServerAccessLock.RUnlock()

	if ms.Topo.HashicorpRaft == nil {
		return resp, nil
	}

	if ms.Topo.HashicorpRaft.State() != raft.Leader {
		return nil, fmt.Errorf("raft add server %s failed: %s is no current leader", req.Id, ms.Topo.HashicorpRaft.String())
	}

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

	ms.Topo.RaftServerAccessLock.RLock()
	defer ms.Topo.RaftServerAccessLock.RUnlock()

	if ms.Topo.HashicorpRaft == nil {
		return resp, nil
	}

	if ms.Topo.HashicorpRaft.State() != raft.Leader {
		return nil, fmt.Errorf("raft remove server %s failed: %s is no current leader", req.Id, ms.Topo.HashicorpRaft.String())
	}

	if !req.Force {
		ms.clientChansLock.RLock()
		_, ok := ms.clientChans[fmt.Sprintf("%s@%s", cluster.MasterType, req.Id)]
		ms.clientChansLock.RUnlock()
		if ok {
			return resp, fmt.Errorf("raft remove server %s failed: client connection to master exists", req.Id)
		}
	}

	idxFuture := ms.Topo.HashicorpRaft.RemoveServer(raft.ServerID(req.Id), 0, 0)
	if err := idxFuture.Error(); err != nil {
		return nil, err
	}
	return resp, nil
}

func (ms *MasterServer) RaftLeadershipTransfer(ctx context.Context, req *master_pb.RaftLeadershipTransferRequest) (*master_pb.RaftLeadershipTransferResponse, error) {
	resp := &master_pb.RaftLeadershipTransferResponse{}

	ms.Topo.RaftServerAccessLock.RLock()
	defer ms.Topo.RaftServerAccessLock.RUnlock()

	// Leadership transfer is only supported with hashicorp raft (-raftHashicorp=true)
	// The default seaweedfs/raft (goraft) implementation does not support this feature
	if ms.Topo.HashicorpRaft == nil {
		if ms.Topo.RaftServer != nil {
			return nil, fmt.Errorf("leadership transfer requires -raftHashicorp=true; the default raft implementation does not support this feature")
		}
		return nil, fmt.Errorf("raft not initialized (single master mode)")
	}

	if ms.Topo.HashicorpRaft.State() != raft.Leader {
		leaderAddr, _ := ms.Topo.HashicorpRaft.LeaderWithID()
		return nil, fmt.Errorf("this server is not the leader; current leader is %s", leaderAddr)
	}

	// Record previous leader
	_, previousLeaderId := ms.Topo.HashicorpRaft.LeaderWithID()
	resp.PreviousLeader = string(previousLeaderId)

	var future raft.Future
	if req.TargetId != "" && req.TargetAddress != "" {
		// Transfer to specific server
		future = ms.Topo.HashicorpRaft.LeadershipTransferToServer(
			raft.ServerID(req.TargetId),
			raft.ServerAddress(req.TargetAddress),
		)
	} else {
		// Transfer to any eligible follower
		future = ms.Topo.HashicorpRaft.LeadershipTransfer()
	}

	if err := future.Error(); err != nil {
		return nil, fmt.Errorf("leadership transfer failed: %v", err)
	}

	// Get new leader info
	_, newLeaderId := ms.Topo.HashicorpRaft.LeaderWithID()
	resp.NewLeader = string(newLeaderId)

	return resp, nil
}
