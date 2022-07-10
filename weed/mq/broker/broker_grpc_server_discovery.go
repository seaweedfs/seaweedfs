package broker

import (
	"context"
	"github.com/chrislusf/seaweedfs/weed/cluster"
	"github.com/chrislusf/seaweedfs/weed/pb"
	"time"

	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/pb/filer_pb"
	"github.com/chrislusf/seaweedfs/weed/pb/master_pb"
)

func (broker *MessageQueueBroker) checkFilers() {

	// contact a filer about masters
	var masters []pb.ServerAddress
	found := false
	for !found {
		for _, filer := range broker.option.Filers {
			err := broker.withFilerClient(false, filer, func(client filer_pb.SeaweedFilerClient) error {
				resp, err := client.GetFilerConfiguration(context.Background(), &filer_pb.GetFilerConfigurationRequest{})
				if err != nil {
					return err
				}
				for _, m := range resp.Masters {
					masters = append(masters, pb.ServerAddress(m))
				}
				return nil
			})
			if err == nil {
				found = true
				break
			}
			glog.V(0).Infof("failed to read masters from %+v: %v", broker.option.Filers, err)
			time.Sleep(time.Second)
		}
	}
	glog.V(0).Infof("received master list: %s", masters)

	// contact each masters for filers
	var filers []pb.ServerAddress
	found = false
	for !found {
		for _, master := range masters {
			err := broker.withMasterClient(false, master, func(client master_pb.SeaweedClient) error {
				resp, err := client.ListClusterNodes(context.Background(), &master_pb.ListClusterNodesRequest{
					ClientType: cluster.FilerType,
				})
				if err != nil {
					return err
				}

				for _, clusterNode := range resp.ClusterNodes {
					filers = append(filers, pb.ServerAddress(clusterNode.Address))
				}

				return nil
			})
			if err == nil {
				found = true
				break
			}
			glog.V(0).Infof("failed to list filers: %v", err)
			time.Sleep(time.Second)
		}
	}
	glog.V(0).Infof("received filer list: %s", filers)

	broker.option.Filers = filers

}
