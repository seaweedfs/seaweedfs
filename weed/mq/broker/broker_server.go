package broker

import (
	"context"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/mq/pub_balancer"
	"github.com/seaweedfs/seaweedfs/weed/mq/sub_coordinator"
	"github.com/seaweedfs/seaweedfs/weed/mq/topic"
	"sync"
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
	VolumeServerAccess string // how to access volume servers
}

func (option *MessageQueueBrokerOption) BrokerAddress() pb.ServerAddress {
	return pb.NewServerAddress(option.Ip, option.Port, 0)
}

type MessageQueueBroker struct {
	mq_pb.UnimplementedSeaweedMessagingServer
	option            *MessageQueueBrokerOption
	grpcDialOption    grpc.DialOption
	MasterClient      *wdclient.MasterClient
	filers            map[pb.ServerAddress]struct{}
	currentFiler      pb.ServerAddress
	localTopicManager *topic.LocalTopicManager
	Balancer          *pub_balancer.Balancer
	lockAsBalancer    *cluster.LiveLock
	Coordinator       *sub_coordinator.Coordinator
	accessLock        sync.Mutex
}

func NewMessageBroker(option *MessageQueueBrokerOption, grpcDialOption grpc.DialOption) (mqBroker *MessageQueueBroker, err error) {

	pub_broker_balancer := pub_balancer.NewBalancer()
	coordinator := sub_coordinator.NewCoordinator(pub_broker_balancer)

	mqBroker = &MessageQueueBroker{
		option:            option,
		grpcDialOption:    grpcDialOption,
		MasterClient:      wdclient.NewMasterClient(grpcDialOption, option.FilerGroup, cluster.BrokerType, option.BrokerAddress(), option.DataCenter, option.Rack, *pb.NewServiceDiscoveryFromMap(option.Masters)),
		filers:            make(map[pb.ServerAddress]struct{}),
		localTopicManager: topic.NewLocalTopicManager(),
		Balancer:          pub_broker_balancer,
		Coordinator:       coordinator,
	}
	mqBroker.MasterClient.SetOnPeerUpdateFn(mqBroker.OnBrokerUpdate)
	pub_broker_balancer.OnPartitionChange = mqBroker.Coordinator.OnPartitionChange
	pub_broker_balancer.OnAddBroker = mqBroker.Coordinator.OnSubAddBroker
	pub_broker_balancer.OnRemoveBroker = mqBroker.Coordinator.OnSubRemoveBroker

	go mqBroker.MasterClient.KeepConnectedToMaster(context.Background())

	existingNodes := cluster.ListExistingPeerUpdates(mqBroker.MasterClient.GetMaster(context.Background()), grpcDialOption, option.FilerGroup, cluster.FilerType)
	for _, newNode := range existingNodes {
		mqBroker.OnBrokerUpdate(newNode, time.Now())
	}

	// keep connecting to balancer
	go func() {
		for mqBroker.currentFiler == "" {
			time.Sleep(time.Millisecond * 237)
		}
		self := option.BrokerAddress()
		glog.V(0).Infof("broker %s found filer %s", self, mqBroker.currentFiler)

		newBrokerBalancerCh := make(chan string, 1)
		lockClient := cluster.NewLockClient(grpcDialOption, mqBroker.currentFiler)
		mqBroker.lockAsBalancer = lockClient.StartLongLivedLock(pub_balancer.LockBrokerBalancer, string(self), func(newLockOwner string) {
			glog.V(0).Infof("broker %s found balanacer %s", self, newLockOwner)
			newBrokerBalancerCh <- newLockOwner
		})
		mqBroker.KeepConnectedToBrokerBalancer(newBrokerBalancerCh)
	}()

	return mqBroker, nil
}

func (b *MessageQueueBroker) OnBrokerUpdate(update *master_pb.ClusterNodeUpdate, startFrom time.Time) {
	if update.NodeType != cluster.FilerType {
		return
	}

	address := pb.ServerAddress(update.Address)
	if update.IsAdd {
		b.filers[address] = struct{}{}
		if b.currentFiler == "" {
			b.currentFiler = address
		}
	} else {
		delete(b.filers, address)
		if b.currentFiler == address {
			for filer := range b.filers {
				b.currentFiler = filer
				break
			}
		}
	}

}

func (b *MessageQueueBroker) GetFiler() pb.ServerAddress {
	return b.currentFiler
}

func (b *MessageQueueBroker) WithFilerClient(streamingMode bool, fn func(filer_pb.SeaweedFilerClient) error) error {

	return pb.WithFilerClient(streamingMode, 0, b.GetFiler(), b.grpcDialOption, fn)

}

func (b *MessageQueueBroker) AdjustedUrl(location *filer_pb.Location) string {

	return location.Url

}

func (b *MessageQueueBroker) GetDataCenter() string {

	return ""

}

func (b *MessageQueueBroker) withMasterClient(streamingMode bool, master pb.ServerAddress, fn func(client master_pb.SeaweedClient) error) error {

	return pb.WithMasterClient(streamingMode, master, b.grpcDialOption, false, func(client master_pb.SeaweedClient) error {
		return fn(client)
	})

}

func (b *MessageQueueBroker) withBrokerClient(streamingMode bool, server pb.ServerAddress, fn func(client mq_pb.SeaweedMessagingClient) error) error {

	return pb.WithBrokerGrpcClient(streamingMode, server.String(), b.grpcDialOption, func(client mq_pb.SeaweedMessagingClient) error {
		return fn(client)
	})

}
