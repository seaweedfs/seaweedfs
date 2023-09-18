package pub_client

import (
	"github.com/rdleal/intervalst/interval"
	"github.com/seaweedfs/seaweedfs/weed/mq/balancer"
	"github.com/seaweedfs/seaweedfs/weed/pb/mq_pb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"sync"
	"time"
)

type PublisherConfiguration struct {
}

type PublishClient struct {
	mq_pb.SeaweedMessaging_PublishClient
	Broker string
	Err    error
}
type TopicPublisher struct {
	namespace        string
	topic            string
	partition2Broker *interval.SearchTree[*PublishClient, int32]
	grpcDialOption   grpc.DialOption
	sync.Mutex       // protects grpc
}

func NewTopicPublisher(namespace, topic string) *TopicPublisher {
	return &TopicPublisher{
		namespace: namespace,
		topic:     topic,
		partition2Broker: interval.NewSearchTree[*PublishClient](func(a, b int32) int {
			return int(a - b)
		}),
		grpcDialOption: grpc.WithTransportCredentials(insecure.NewCredentials()),
	}
}

func (p *TopicPublisher) Connect(bootstrapBroker string) error {
	if err := p.doLookup(bootstrapBroker); err != nil {
		return err
	}
	return nil
}

func (p *TopicPublisher) Shutdown() error {

	if clients, found := p.partition2Broker.AllIntersections(0, balancer.MaxPartitionCount); found {
		for _, client := range clients {
			client.CloseSend()
		}
	}
	time.Sleep(1100 * time.Millisecond)

	return nil
}
