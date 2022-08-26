package broker

import (
	"time"

	"github.com/seaweedfs/seaweedfs/weed/cluster"
	"github.com/seaweedfs/seaweedfs/weed/pb/mq_pb"
	"github.com/seaweedfs/seaweedfs/weed/wdclient"
	"google.golang.org/grpc"

	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
)

type MessageQueueBrokerOption struct {
	Masters            map[string]pb.ServerAddress
	FilerGroup         string
	DataCenter         string
	Rack               string
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
	filers         map[pb.ServerAddress]struct{}
	currentFiler   pb.ServerAddress
}

func NewMessageBroker(option *MessageQueueBrokerOption, grpcDialOption grpc.DialOption) (mqBroker *MessageQueueBroker, err error) {

	mqBroker = &MessageQueueBroker{
		option:         option,
		grpcDialOption: grpcDialOption,
		MasterClient:   wdclient.NewMasterClient(grpcDialOption, option.FilerGroup, cluster.BrokerType, pb.NewServerAddress(option.Ip, option.Port, 0), option.DataCenter, option.Rack, option.Masters),
		filers:         make(map[pb.ServerAddress]struct{}),
	}
	mqBroker.MasterClient.SetOnPeerUpdateFn(mqBroker.OnBrokerUpdate)

	go mqBroker.MasterClient.KeepConnectedToMaster()

	existingNodes := cluster.ListExistingPeerUpdates(mqBroker.MasterClient.GetMaster(), grpcDialOption, option.FilerGroup, cluster.FilerType)
	for _, newNode := range existingNodes {
		mqBroker.OnBrokerUpdate(newNode, time.Now())
	}

	return mqBroker, nil
}

func (broker *MessageQueueBroker) OnBrokerUpdate(update *master_pb.ClusterNodeUpdate, startFrom time.Time) {
	if update.NodeType != cluster.FilerType {
		return
	}

	address := pb.ServerAddress(update.Address)
	if update.IsAdd {
		broker.filers[address] = struct{}{}
		if broker.currentFiler == "" {
			broker.currentFiler = address
		}
	} else {
		delete(broker.filers, address)
		if broker.currentFiler == address {
			for filer := range broker.filers {
				broker.currentFiler = filer
				break
			}
		}
	}

}

func (broker *MessageQueueBroker) GetFiler() pb.ServerAddress {
	return broker.currentFiler
}

func (broker *MessageQueueBroker) WithFilerClient(streamingMode bool, fn func(filer_pb.SeaweedFilerClient) error) error {

	return pb.WithFilerClient(streamingMode, broker.GetFiler(), broker.grpcDialOption, fn)

}

func (broker *MessageQueueBroker) AdjustedUrl(location *filer_pb.Location) string {

	return location.Url

}

func (broker *MessageQueueBroker) GetDataCenter() string {

	return ""

}

func (broker *MessageQueueBroker) withMasterClient(streamingMode bool, master pb.ServerAddress, fn func(client master_pb.SeaweedClient) error) error {

	return pb.WithMasterClient(streamingMode, master, broker.grpcDialOption, false, func(client master_pb.SeaweedClient) error {
		return fn(client)
	})

}

func (broker *MessageQueueBroker) withBrokerClient(streamingMode bool, server pb.ServerAddress, fn func(client mq_pb.SeaweedMessagingClient) error) error {

	return pb.WithBrokerClient(streamingMode, server, broker.grpcDialOption, func(client mq_pb.SeaweedMessagingClient) error {
		return fn(client)
	})

}
