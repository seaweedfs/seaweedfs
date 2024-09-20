package weed_server

import (
	"context"
	"github.com/seaweedfs/seaweedfs/weed/cluster"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"math/rand/v2"
)

func (ms *MasterServer) ListClusterNodes(ctx context.Context, req *master_pb.ListClusterNodesRequest) (*master_pb.ListClusterNodesResponse, error) {
	resp := &master_pb.ListClusterNodesResponse{}
	filerGroup := cluster.FilerGroupName(req.FilerGroup)

	clusterNodes := ms.Cluster.ListClusterNode(filerGroup, req.ClientType)
	clusterNodes = limitTo(clusterNodes, req.Limit)
	for _, node := range clusterNodes {
		resp.ClusterNodes = append(resp.ClusterNodes, &master_pb.ListClusterNodesResponse_ClusterNode{
			Address:     string(node.Address),
			Version:     node.Version,
			CreatedAtNs: node.CreatedTs.UnixNano(),
			DataCenter:  string(node.DataCenter),
			Rack:        string(node.Rack),
		})
	}
	return resp, nil
}

func (ms *MasterServer) GetOneFiler(filerGroup cluster.FilerGroupName) pb.ServerAddress {

	filers := ms.Cluster.ListClusterNode(filerGroup, cluster.FilerType)

	if len(filers) > 0 {
		return filers[rand.IntN(len(filers))].Address
	}
	return "localhost:8888"
}

func limitTo(nodes []*cluster.ClusterNode, limit int32) (selected []*cluster.ClusterNode) {
	if limit <= 0 || len(nodes) < int(limit) {
		return nodes
	}
	selectedSet := make(map[pb.ServerAddress]*cluster.ClusterNode)
	for i := 0; i < int(limit)*3; i++ {
		x := rand.IntN(len(nodes))
		if _, found := selectedSet[nodes[x].Address]; found {
			continue
		}
		selectedSet[nodes[x].Address] = nodes[x]
	}
	for _, node := range selectedSet {
		selected = append(selected, node)
	}
	return
}
