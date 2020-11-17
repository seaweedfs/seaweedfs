package broker

import (
	"context"
	"fmt"
	"time"

	"github.com/chrislusf/seaweedfs/weed/util/log"
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
		err := broker.withFilerClient(filer, func(client filer_pb.SeaweedFilerClient) error {
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
	var masters []string
	found := false
	for !found {
		for _, filer := range broker.option.Filers {
			err := broker.withFilerClient(filer, func(client filer_pb.SeaweedFilerClient) error {
				resp, err := client.GetFilerConfiguration(context.Background(), &filer_pb.GetFilerConfigurationRequest{})
				if err != nil {
					return err
				}
				masters = append(masters, resp.Masters...)
				return nil
			})
			if err == nil {
				found = true
				break
			}
			log.Infof("failed to read masters from %+v: %v", broker.option.Filers, err)
			time.Sleep(time.Second)
		}
	}
	log.Infof("received master list: %s", masters)

	// contact each masters for filers
	var filers []string
	found = false
	for !found {
		for _, master := range masters {
			err := broker.withMasterClient(master, func(client master_pb.SeaweedClient) error {
				resp, err := client.ListMasterClients(context.Background(), &master_pb.ListMasterClientsRequest{
					ClientType: "filer",
				})
				if err != nil {
					return err
				}

				filers = append(filers, resp.GrpcAddresses...)

				return nil
			})
			if err == nil {
				found = true
				break
			}
			log.Infof("failed to list filers: %v", err)
			time.Sleep(time.Second)
		}
	}
	log.Infof("received filer list: %s", filers)

	broker.option.Filers = filers

}
