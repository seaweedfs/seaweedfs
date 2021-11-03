package weed_server

import (
	"context"
	"github.com/chrislusf/seaweedfs/weed/pb/master_pb"
)

func (ms *MasterServer) ListClusterNodes(ctx context.Context, req *master_pb.ListClusterNodesRequest) (*master_pb.ListClusterNodesResponse, error) {
	resp := &master_pb.ListClusterNodesResponse{}

	clusterNodes := ms.Cluster.ListClusterNode(req.ClientType)

	for _, node := range clusterNodes {
		resp.ClusterNodes = append(resp.ClusterNodes, &master_pb.ListClusterNodesResponse_ClusterNode{
			Address: string(node.address),
			Version: node.version,
		})
	}
	return resp, nil
}
