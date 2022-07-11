package broker

import (
	"github.com/chrislusf/seaweedfs/weed/cluster"
	"github.com/chrislusf/seaweedfs/weed/pb/mq_pb"
	"github.com/chrislusf/seaweedfs/weed/wdclient"
	"google.golang.org/grpc"

	"github.com/chrislusf/seaweedfs/weed/pb"
	"github.com/chrislusf/seaweedfs/weed/pb/filer_pb"
	"github.com/chrislusf/seaweedfs/weed/pb/master_pb"
)

type MessageQueueBrokerOption struct {
	Masters            map[string]pb.ServerAddress
	FilerGroup         string
	DataCenter         string
	Rack               string
	Filers             []pb.ServerAddress
	DefaultReplication string
	MaxMB              int
	Ip                 string
	Port               int
	Cipher             bool
}

type MessageQueueBroker struct {
	mq_pb.UnimplementedSeaweedMessagingServer
	option         *MessageQueueBrokerOption
	grpcDialOption grpc.DialOption
	MasterClient   *wdclient.MasterClient
}

func NewMessageBroker(option *MessageQueueBrokerOption, grpcDialOption grpc.DialOption) (mqBroker *MessageQueueBroker, err error) {

	mqBroker = &MessageQueueBroker{
		option:         option,
		grpcDialOption: grpcDialOption,
		MasterClient:   wdclient.NewMasterClient(grpcDialOption, option.FilerGroup, cluster.BrokerType, pb.NewServerAddress(option.Ip, option.Port, 0), option.DataCenter, option.Rack, option.Masters),
	}

	mqBroker.checkFilers()

	go mqBroker.MasterClient.KeepConnectedToMaster()

	return mqBroker, nil
}

func (broker *MessageQueueBroker) withFilerClient(streamingMode bool, filer pb.ServerAddress, fn func(filer_pb.SeaweedFilerClient) error) error {

	return pb.WithFilerClient(streamingMode, filer, broker.grpcDialOption, fn)

}

func (broker *MessageQueueBroker) withMasterClient(streamingMode bool, master pb.ServerAddress, fn func(client master_pb.SeaweedClient) error) error {

	return pb.WithMasterClient(streamingMode, master, broker.grpcDialOption, func(client master_pb.SeaweedClient) error {
		return fn(client)
	})

}

func (broker *MessageQueueBroker) withBrokerClient(streamingMode bool, server pb.ServerAddress, fn func(client mq_pb.SeaweedMessagingClient) error) error {

	return pb.WithBrokerClient(streamingMode, server, broker.grpcDialOption, func(client mq_pb.SeaweedMessagingClient) error {
		return fn(client)
	})

}
