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
	Ip                 string
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

	go messageBroker.keepConnectedToOneFiler()

	return messageBroker, nil
}

func (broker *MessageBroker) keepConnectedToOneFiler() {

	for {
		for _, filer := range broker.option.Filers {
			broker.withFilerClient(filer, func(client filer_pb.SeaweedFilerClient) error {
				stream, err := client.KeepConnected(context.Background())
				if err != nil {
					glog.V(0).Infof("%s:%d failed to keep connected to %s: %v", broker.option.Ip, broker.option.Port, filer, err)
					return err
				}
				glog.V(0).Infof("conntected with filer: %v", filer)
				for {
					if err := stream.Send(&filer_pb.KeepConnectedRequest{
						Name:     broker.option.Ip,
						GrpcPort: uint32(broker.option.Port),
					}); err != nil {
						glog.V(0).Infof("%s:%d failed to sendto %s: %v", broker.option.Ip, broker.option.Port, filer, err)
						return err
					}
					// println("send heartbeat")
					if _, err := stream.Recv(); err != nil {
						glog.V(0).Infof("%s:%d failed to receive from %s: %v", broker.option.Ip, broker.option.Port, filer, err)
						return err
					}
					// println("received reply")
					time.Sleep(11*time.Second)
					// println("woke up")
				}
				return nil
			})
			time.Sleep(3*time.Second)
		}
	}

}

func (broker *MessageBroker) withFilerClient(filer string, fn func(filer_pb.SeaweedFilerClient) error) error {

	return pb.WithFilerClient(filer, broker.grpcDialOption, fn)

}

func (broker *MessageBroker) withMasterClient(master string, fn func(client master_pb.SeaweedClient) error) error {

	return pb.WithMasterClient(master, broker.grpcDialOption, func(client master_pb.SeaweedClient) error {
		return fn(client)
	})

}
