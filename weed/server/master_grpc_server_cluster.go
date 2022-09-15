package weed_server

import (
	"context"
	"github.com/seaweedfs/seaweedfs/weed/cluster"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"math/rand"
)

func (ms *MasterServer) ListClusterNodes(ctx context.Context, req *master_pb.ListClusterNodesRequest) (*master_pb.ListClusterNodesResponse, error) {
	resp := &master_pb.ListClusterNodesResponse{}
	filerGroup := cluster.FilerGroupName(req.FilerGroup)

	if req.IsLeaderOnly {
		leaders := ms.Cluster.ListClusterNodeLeaders(filerGroup, req.ClientType)
		for _, node := range leaders {
			resp.ClusterNodes = append(resp.ClusterNodes, &master_pb.ListClusterNodesResponse_ClusterNode{
				Address:  string(node),
				IsLeader: true,
			})
		}
	} else {
		clusterNodes := ms.Cluster.ListClusterNode(filerGroup, req.ClientType)
		clusterNodes = limitTo(clusterNodes, req.Limit)
		for _, node := range clusterNodes {
			resp.ClusterNodes = append(resp.ClusterNodes, &master_pb.ListClusterNodesResponse_ClusterNode{
				Address:     string(node.Address),
				Version:     node.Version,
				IsLeader:    ms.Cluster.IsOneLeader(filerGroup, req.ClientType, node.Address),
				CreatedAtNs: node.CreatedTs.UnixNano(),
				DataCenter:  string(node.DataCenter),
				Rack:        string(node.Rack),
			})
		}
	}
	return resp, nil
}

func (ms *MasterServer) GetOneFiler(filerGroup cluster.FilerGroupName) pb.ServerAddress {

	clusterNodes := ms.Cluster.ListClusterNode(filerGroup, cluster.FilerType)

	var filers []pb.ServerAddress
	for _, node := range clusterNodes {
		if ms.Cluster.IsOneLeader(filerGroup, cluster.FilerType, node.Address) {
			filers = append(filers, node.Address)
		}
	}
	if len(filers) > 0 {
		return filers[rand.Intn(len(filers))]
	}
	return "localhost:8888"
}

func limitTo(nodes []*cluster.ClusterNode, limit int32) (selected []*cluster.ClusterNode) {
	if limit <= 0 || len(nodes) < int(limit) {
		return nodes
	}
	selectedSet := make(map[pb.ServerAddress]*cluster.ClusterNode)
	for i := 0; i < int(limit)*3; i++ {
		x := rand.Intn(len(nodes))
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
