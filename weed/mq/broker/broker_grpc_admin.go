package broker

import (
	"context"
	"github.com/seaweedfs/seaweedfs/weed/cluster"
	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/mq_pb"
)

func (b *MessageQueueBroker) FindBrokerLeader(c context.Context, request *mq_pb.FindBrokerLeaderRequest) (*mq_pb.FindBrokerLeaderResponse, error) {
	ret := &mq_pb.FindBrokerLeaderResponse{}
	err := b.withMasterClient(false, b.MasterClient.GetMaster(), func(client master_pb.SeaweedClient) error {
		resp, err := client.ListClusterNodes(context.Background(), &master_pb.ListClusterNodesRequest{
			ClientType: cluster.BrokerType,
			FilerGroup: request.FilerGroup,
		})
		if err != nil {
			return err
		}
		if len(resp.ClusterNodes) == 0 {
			return nil
		}
		ret.Broker = resp.ClusterNodes[0].Address
		return nil
	})
	return ret, err
}
