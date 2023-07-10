package cluster

import (
	"context"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"google.golang.org/grpc"
)

func ListExistingPeerUpdates(master pb.ServerAddress, grpcDialOption grpc.DialOption, filerGroup string, clientType string) (existingNodes []*master_pb.ClusterNodeUpdate) {

	if grpcErr := pb.WithMasterClient(false, master, grpcDialOption, false, func(client master_pb.SeaweedClient) error {
		resp, err := client.ListClusterNodes(context.Background(), &master_pb.ListClusterNodesRequest{
			ClientType: clientType,
			FilerGroup: filerGroup,
		})

		glog.V(0).Infof("the cluster has %d %s\n", len(resp.ClusterNodes), clientType)
		for _, node := range resp.ClusterNodes {
			existingNodes = append(existingNodes, &master_pb.ClusterNodeUpdate{
				NodeType:    FilerType,
				Address:     node.Address,
				IsAdd:       true,
				CreatedAtNs: node.CreatedAtNs,
			})
		}
		return err
	}); grpcErr != nil {
		glog.V(0).Infof("connect to %s: %v", master, grpcErr)
	}
	return
}
