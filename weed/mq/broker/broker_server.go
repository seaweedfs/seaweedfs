package broker

import (
	"context"
	"sync"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/filer_client"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/mq/pub_balancer"
	"github.com/seaweedfs/seaweedfs/weed/mq/sub_coordinator"
	"github.com/seaweedfs/seaweedfs/weed/mq/topic"

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
	LogFlushInterval   int    // log buffer flush interval in seconds
}

func (option *MessageQueueBrokerOption) BrokerAddress() pb.ServerAddress {
	return pb.NewServerAddress(option.Ip, option.Port, 0)
}

// topicCacheEntry caches both topic existence and configuration
// If conf is nil, topic doesn't exist (negative cache)
// If conf is non-nil, topic exists with this configuration (positive cache)
type topicCacheEntry struct {
	conf      *mq_pb.ConfigureTopicResponse // nil = topic doesn't exist
	expiresAt time.Time
}

type MessageQueueBroker struct {
	mq_pb.UnimplementedSeaweedMessagingServer
	option            *MessageQueueBrokerOption
	grpcDialOption    grpc.DialOption
	MasterClient      *wdclient.MasterClient
	filers            map[pb.ServerAddress]struct{}
	currentFiler      pb.ServerAddress
	localTopicManager *topic.LocalTopicManager
	PubBalancer       *pub_balancer.PubBalancer
	lockAsBalancer    *cluster.LiveLock
	// TODO: Add native offset management to broker
	// ASSUMPTION: BrokerOffsetManager handles all partition offset assignment
	offsetManager  *BrokerOffsetManager
	SubCoordinator *sub_coordinator.SubCoordinator
	// Removed gatewayRegistry - no longer needed
	accessLock sync.Mutex
	fca        *filer_client.FilerClientAccessor
	// Unified topic cache for both existence and configuration
	// Caches topic config (positive: conf != nil) and non-existence (negative: conf == nil)
	// Eliminates 60% CPU overhead from repeated filer reads and JSON unmarshaling
	topicCache    map[string]*topicCacheEntry
	topicCacheMu  sync.RWMutex
	topicCacheTTL time.Duration
}

func NewMessageBroker(option *MessageQueueBrokerOption, grpcDialOption grpc.DialOption) (mqBroker *MessageQueueBroker, err error) {

	pubBalancer := pub_balancer.NewPubBalancer()
	subCoordinator := sub_coordinator.NewSubCoordinator()

	mqBroker = &MessageQueueBroker{
		option:            option,
		grpcDialOption:    grpcDialOption,
		MasterClient:      wdclient.NewMasterClient(grpcDialOption, option.FilerGroup, cluster.BrokerType, option.BrokerAddress(), option.DataCenter, option.Rack, *pb.NewServiceDiscoveryFromMap(option.Masters)),
		filers:            make(map[pb.ServerAddress]struct{}),
		localTopicManager: topic.NewLocalTopicManager(),
		PubBalancer:       pubBalancer,
		SubCoordinator:    subCoordinator,
		offsetManager:     nil, // Will be initialized below
		topicCache:        make(map[string]*topicCacheEntry),
		topicCacheTTL:     30 * time.Second, // Unified cache for existence + config (eliminates 60% CPU overhead)
	}
	// Create FilerClientAccessor that adapts broker's single filer to the new multi-filer interface
	fca := &filer_client.FilerClientAccessor{
		GetGrpcDialOption: mqBroker.GetGrpcDialOption,
		GetFilers: func() []pb.ServerAddress {
			filer := mqBroker.GetFiler()
			if filer != "" {
				return []pb.ServerAddress{filer}
			}
			return []pb.ServerAddress{}
		},
	}
	mqBroker.fca = fca
	subCoordinator.FilerClientAccessor = fca

	mqBroker.MasterClient.SetOnPeerUpdateFn(mqBroker.OnBrokerUpdate)
	pubBalancer.OnPartitionChange = mqBroker.SubCoordinator.OnPartitionChange

	go mqBroker.MasterClient.KeepConnectedToMaster(context.Background())

	// Initialize offset manager using the filer accessor
	// The filer accessor will automatically use the current filer address as it gets discovered
	// No hardcoded namespace/topic - offset storage now derives paths from actual topic information
	mqBroker.offsetManager = NewBrokerOffsetManagerWithFilerAccessor(fca)
	glog.V(0).Infof("broker initialized offset manager with filer accessor (current filer: %s)", mqBroker.GetFiler())

	// Start idle partition cleanup task
	// Cleans up partitions with no publishers/subscribers after 5 minutes of idle time
	// Checks every 1 minute to avoid memory bloat from short-lived topics
	mqBroker.localTopicManager.StartIdlePartitionCleanup(
		context.Background(),
		1*time.Minute, // Check interval
		5*time.Minute, // Idle timeout - clean up after 5 minutes of no activity
	)
	glog.V(0).Info("Started idle partition cleanup task (check: 1m, timeout: 5m)")

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
			// The offset manager will automatically use the updated filer through the filer accessor
			glog.V(0).Infof("broker discovered filer %s (offset manager will automatically use it via filer accessor)", address)
		}
	} else {
		delete(b.filers, address)
		if b.currentFiler == address {
			for filer := range b.filers {
				b.currentFiler = filer
				// The offset manager will automatically use the new filer through the filer accessor
				glog.V(0).Infof("broker switched to filer %s (offset manager will automatically use it)", filer)
				break
			}
		}
	}

}

func (b *MessageQueueBroker) GetGrpcDialOption() grpc.DialOption {
	return b.grpcDialOption
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
