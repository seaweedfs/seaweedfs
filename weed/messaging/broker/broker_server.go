package broker

import (
	"context"
	"time"

	"google.golang.org/grpc"

	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/pb"
	"github.com/chrislusf/seaweedfs/weed/pb/filer_pb"
	"github.com/chrislusf/seaweedfs/weed/pb/master_pb"
)

type MessageBrokerOption struct {
	Filers             []string
	DefaultReplication string
	MaxMB              int
	Port               int
	Cipher             bool
}

type MessageBroker struct {
	option         *MessageBrokerOption
	grpcDialOption grpc.DialOption
	topicLocks     *TopicLocks
}

func NewMessageBroker(option *MessageBrokerOption, grpcDialOption grpc.DialOption) (messageBroker *MessageBroker, err error) {

	messageBroker = &MessageBroker{
		option:         option,
		grpcDialOption: grpcDialOption,
	}

	messageBroker.topicLocks = NewTopicLocks(messageBroker)

	messageBroker.checkPeers()

	// go messageBroker.loopForEver()

	return messageBroker, nil
}

func (broker *MessageBroker) loopForEver() {

	for {
		broker.checkPeers()
		time.Sleep(3 * time.Second)
	}

}

func (broker *MessageBroker) checkPeers() {

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
			glog.V(0).Infof("failed to read masters from %+v: %v", broker.option.Filers, err)
			time.Sleep(time.Second)
		}
	}
	glog.V(0).Infof("received master list: %s", masters)

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
			glog.V(0).Infof("failed to list filers: %v", err)
			time.Sleep(time.Second)
		}
	}
	glog.V(0).Infof("received filer list: %s", filers)

	broker.option.Filers = filers

}

func (broker *MessageBroker) withFilerClient(filer string, fn func(filer_pb.SeaweedFilerClient) error) error {

	return pb.WithFilerClient(filer, broker.grpcDialOption, fn)

}

func (broker *MessageBroker) withMasterClient(master string, fn func(client master_pb.SeaweedClient) error) error {

	return pb.WithMasterClient(master, broker.grpcDialOption, func(client master_pb.SeaweedClient) error {
		return fn(client)
	})

}
