package broker

import (
	"context"
	"github.com/chrislusf/seaweedfs/weed/pb/messaging_pb"
	"time"

	"google.golang.org/grpc"

	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/pb"
	"github.com/chrislusf/seaweedfs/weed/pb/filer_pb"
	"github.com/chrislusf/seaweedfs/weed/pb/master_pb"
)

type MessageBrokerOption struct {
	Filers             []pb.ServerAddress
	DefaultReplication string
	MaxMB              int
	Ip                 string
	Port               int
	Cipher             bool
}

type MessageBroker struct {
	messaging_pb.UnimplementedSeaweedMessagingServer
	option         *MessageBrokerOption
	grpcDialOption grpc.DialOption
	topicManager   *TopicManager
}

func NewMessageBroker(option *MessageBrokerOption, grpcDialOption grpc.DialOption) (messageBroker *MessageBroker, err error) {

	messageBroker = &MessageBroker{
		option:         option,
		grpcDialOption: grpcDialOption,
	}

	messageBroker.topicManager = NewTopicManager(messageBroker)

	messageBroker.checkFilers()

	go messageBroker.keepConnectedToOneFiler()

	return messageBroker, nil
}

func (broker *MessageBroker) keepConnectedToOneFiler() {

	for {
		for _, filer := range broker.option.Filers {
			broker.withFilerClient(false, filer, func(client filer_pb.SeaweedFilerClient) error {
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				stream, err := client.KeepConnected(ctx)
				if err != nil {
					glog.V(0).Infof("%s:%d failed to keep connected to %s: %v", broker.option.Ip, broker.option.Port, filer, err)
					return err
				}

				initRequest := &filer_pb.KeepConnectedRequest{
					Name:     broker.option.Ip,
					GrpcPort: uint32(broker.option.Port),
				}
				for _, tp := range broker.topicManager.ListTopicPartitions() {
					initRequest.Resources = append(initRequest.Resources, tp.String())
				}
				if err := stream.Send(&filer_pb.KeepConnectedRequest{
					Name:     broker.option.Ip,
					GrpcPort: uint32(broker.option.Port),
				}); err != nil {
					glog.V(0).Infof("broker %s:%d failed to init at %s: %v", broker.option.Ip, broker.option.Port, filer, err)
					return err
				}

				// TODO send events of adding/removing topics

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
					time.Sleep(11 * time.Second)
					// println("woke up")
				}
				return nil
			})
			time.Sleep(3 * time.Second)
		}
	}

}

func (broker *MessageBroker) withFilerClient(streamingMode bool, filer pb.ServerAddress, fn func(filer_pb.SeaweedFilerClient) error) error {

	return pb.WithFilerClient(streamingMode, filer, broker.grpcDialOption, fn)

}

func (broker *MessageBroker) withMasterClient(streamingMode bool, master pb.ServerAddress, fn func(client master_pb.SeaweedClient) error) error {

	return pb.WithMasterClient(streamingMode, master, broker.grpcDialOption, func(client master_pb.SeaweedClient) error {
		return fn(client)
	})

}
