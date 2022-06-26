package broker

import (
	"context"
	"fmt"
	"github.com/chrislusf/seaweedfs/weed/cluster"
	"github.com/chrislusf/seaweedfs/weed/pb"
	"time"

	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/pb/filer_pb"
	"github.com/chrislusf/seaweedfs/weed/pb/master_pb"
	"github.com/chrislusf/seaweedfs/weed/pb/messaging_pb"
)

/*
Topic discovery:

When pub or sub connects, it ask for the whole broker list, and run consistent hashing to find the broker.

The broker will check peers whether it is already hosted by some other broker, if that broker is alive and acknowledged alive, redirect to it.
Otherwise, just host the topic.

So, if the pub or sub connects around the same time, they would connect to the same broker. Everyone is happy.
If one of the pub or sub connects very late, and the system topo changed quite a bit with new servers added or old servers died, checking peers will help.

*/

func (broker *MessageBroker) FindBroker(c context.Context, request *messaging_pb.FindBrokerRequest) (*messaging_pb.FindBrokerResponse, error) {

	t := &messaging_pb.FindBrokerResponse{}
	var peers []string

	targetTopicPartition := fmt.Sprintf(TopicPartitionFmt, request.Namespace, request.Topic, request.Parition)

	for _, filer := range broker.option.Filers {
		err := broker.withFilerClient(false, filer, func(client filer_pb.SeaweedFilerClient) error {
			resp, err := client.LocateBroker(context.Background(), &filer_pb.LocateBrokerRequest{
				Resource: targetTopicPartition,
			})
			if err != nil {
				return err
			}
			if resp.Found && len(resp.Resources) > 0 {
				t.Broker = resp.Resources[0].GrpcAddresses
				return nil
			}
			for _, b := range resp.Resources {
				peers = append(peers, b.GrpcAddresses)
			}
			return nil
		})
		if err != nil {
			return nil, err
		}
	}

	t.Broker = PickMember(peers, []byte(targetTopicPartition))

	return t, nil

}

func (broker *MessageBroker) checkFilers() {

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
