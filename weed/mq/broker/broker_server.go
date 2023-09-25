package broker

import (
	"fmt"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/mq/balancer"
	"github.com/seaweedfs/seaweedfs/weed/mq/topic"
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
	option            *MessageQueueBrokerOption
	grpcDialOption    grpc.DialOption
	MasterClient      *wdclient.MasterClient
	filers            map[pb.ServerAddress]struct{}
	currentFiler      pb.ServerAddress
	localTopicManager *topic.LocalTopicManager
	Balancer          *balancer.Balancer
	lockAsBalancer    *cluster.LiveLock
	currentBalancer   pb.ServerAddress
}

func NewMessageBroker(option *MessageQueueBrokerOption, grpcDialOption grpc.DialOption) (mqBroker *MessageQueueBroker, err error) {

	mqBroker = &MessageQueueBroker{
		option:            option,
		grpcDialOption:    grpcDialOption,
		MasterClient:      wdclient.NewMasterClient(grpcDialOption, option.FilerGroup, cluster.BrokerType, pb.NewServerAddress(option.Ip, option.Port, 0), option.DataCenter, option.Rack, *pb.NewServiceDiscoveryFromMap(option.Masters)),
		filers:            make(map[pb.ServerAddress]struct{}),
		localTopicManager: topic.NewLocalTopicManager(),
		Balancer:          balancer.NewBalancer(),
	}
	mqBroker.MasterClient.SetOnPeerUpdateFn(mqBroker.OnBrokerUpdate)

	go mqBroker.MasterClient.KeepConnectedToMaster()

	existingNodes := cluster.ListExistingPeerUpdates(mqBroker.MasterClient.GetMaster(), grpcDialOption, option.FilerGroup, cluster.FilerType)
	for _, newNode := range existingNodes {
		mqBroker.OnBrokerUpdate(newNode, time.Now())
	}

	// keep connecting to balancer
	go func() {
		for mqBroker.currentFiler == "" {
			time.Sleep(time.Millisecond * 237)
		}
		self := fmt.Sprintf("%s:%d", option.Ip, option.Port)
		glog.V(1).Infof("broker %s found filer %s", self, mqBroker.currentFiler)

		lockClient := cluster.NewLockClient(grpcDialOption, mqBroker.currentFiler)
		mqBroker.lockAsBalancer = lockClient.StartLock(balancer.LockBrokerBalancer, self)
		for {
			err := mqBroker.BrokerConnectToBalancer(self)
			if err != nil {
				fmt.Printf("BrokerConnectToBalancer: %v\n", err)
			}
			time.Sleep(time.Second)
		}
	}()

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

	return pb.WithFilerClient(streamingMode, 0, broker.GetFiler(), broker.grpcDialOption, fn)

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

	return pb.WithBrokerGrpcClient(streamingMode, server.String(), broker.grpcDialOption, func(client mq_pb.SeaweedMessagingClient) error {
		return fn(client)
	})

}
