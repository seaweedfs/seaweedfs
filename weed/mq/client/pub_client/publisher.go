package pub_client

import (
	cmap "github.com/orcaman/concurrent-map/v2"
	"github.com/rdleal/intervalst/interval"
	"github.com/seaweedfs/seaweedfs/weed/pb/mq_pb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type PublisherConfiguration struct {
}
type TopicPublisher struct {
	namespace            string
	topic                string
	partition2Broker     *interval.SearchTree[string, int32]
	broker2PublishClient cmap.ConcurrentMap[string, mq_pb.SeaweedMessaging_PublishClient]
	grpcDialOption       grpc.DialOption
}

func NewTopicPublisher(namespace, topic string) *TopicPublisher {
	return &TopicPublisher{
		namespace: namespace,
		topic:     topic,
		partition2Broker: interval.NewSearchTree[string](func(a, b int32) int {
			return int(a - b)
		}),
		broker2PublishClient: cmap.New[mq_pb.SeaweedMessaging_PublishClient](),
		grpcDialOption:       grpc.WithTransportCredentials(insecure.NewCredentials()),
	}
}

func (p *TopicPublisher) Connect(bootstrapBroker string) error {
	if err := p.doLookup(bootstrapBroker); err != nil {
		return err
	}
	return nil
}
