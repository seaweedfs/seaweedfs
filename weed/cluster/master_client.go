package cluster

import (
	"context"

	"github.com/dustin/go-humanize/english"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"google.golang.org/grpc"
)

// ListExistingPeerUpdates lists the cluster nodes one master currently tracks.
// An empty result is only a successful empty answer; a failed RPC returns the
// error so callers do not mistake "unknown" for "absent".
func ListExistingPeerUpdates(ctx context.Context, master pb.ServerAddress, grpcDialOption grpc.DialOption, filerGroup string, clientType string) (existingNodes []*master_pb.ClusterNodeUpdate, err error) {

	err = pb.WithMasterClient(ctx, false, master, grpcDialOption, false, func(client master_pb.SeaweedClient) error {
		resp, err := client.ListClusterNodes(ctx, &master_pb.ListClusterNodesRequest{
			ClientType: clientType,
			FilerGroup: filerGroup,
		})
		if err != nil {
			return err
		}

		glog.V(0).Infof("the cluster has %d %s\n", len(resp.ClusterNodes), english.PluralWord(len(resp.ClusterNodes), clientType, ""))
		for _, node := range resp.ClusterNodes {
			existingNodes = append(existingNodes, &master_pb.ClusterNodeUpdate{
				NodeType:    FilerType,
				Address:     node.Address,
				IsAdd:       true,
				CreatedAtNs: node.CreatedAtNs,
			})
		}
		return nil
	})
	if err != nil {
		glog.V(0).Infof("connect to %s: %v", master, err)
	}
	return
}

// LookupClusterLeader asks one master for the raft leader it currently sees.
// Any reachable master answers, including followers; it returns "" while the
// cluster is mid-election.
func LookupClusterLeader(ctx context.Context, master pb.ServerAddress, grpcDialOption grpc.DialOption) (leader pb.ServerAddress, err error) {
	err = pb.WithMasterClient(ctx, false, master, grpcDialOption, false, func(client master_pb.SeaweedClient) error {
		resp, err := client.GetMasterConfiguration(ctx, &master_pb.GetMasterConfigurationRequest{})
		if err != nil {
			return err
		}
		leader = pb.ServerAddress(resp.Leader)
		return nil
	})
	return
}
