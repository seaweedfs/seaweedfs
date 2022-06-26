package weed_server

import (
	"context"
	"github.com/chrislusf/seaweedfs/weed/cluster"
	"github.com/chrislusf/seaweedfs/weed/pb"
	"github.com/chrislusf/seaweedfs/weed/pb/master_pb"
	"math/rand"
)

func (ms *MasterServer) ListClusterNodes(ctx context.Context, req *master_pb.ListClusterNodesRequest) (*master_pb.ListClusterNodesResponse, error) {
	resp := &master_pb.ListClusterNodesResponse{}
	filerGroup := cluster.FilerGroup(req.FilerGroup)
	clusterNodes := ms.Cluster.ListClusterNode(filerGroup, req.ClientType)

	for _, node := range clusterNodes {
		resp.ClusterNodes = append(resp.ClusterNodes, &master_pb.ListClusterNodesResponse_ClusterNode{
			Address:     string(node.Address),
			Version:     node.Version,
			IsLeader:    ms.Cluster.IsOneLeader(filerGroup, node.Address),
			CreatedAtNs: node.CreatedTs.UnixNano(),
		})
	}
	return resp, nil
}

func (ms *MasterServer) GetOneFiler(filerGroup cluster.FilerGroup) pb.ServerAddress {

	clusterNodes := ms.Cluster.ListClusterNode(filerGroup, cluster.FilerType)

	var filers []pb.ServerAddress
	for _, node := range clusterNodes {
		if ms.Cluster.IsOneLeader(filerGroup, node.Address) {
			filers = append(filers, node.Address)
		}
	}
	if len(filers) > 0 {
		return filers[rand.Intn(len(filers))]
	}
	return "localhost:8888"
}
