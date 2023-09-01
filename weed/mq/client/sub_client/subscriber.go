package sub_client

import (
	"github.com/seaweedfs/seaweedfs/weed/pb/mq_pb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type SubscriberConfiguration struct {
	ConsumerGroup string
	ConsumerId    string
}

type TopicSubscriber struct {
	config                     *SubscriberConfiguration
	namespace                  string
	topic                      string
	brokerPartitionAssignments []*mq_pb.BrokerPartitionAssignment
	grpcDialOption             grpc.DialOption
}

func NewTopicSubscriber(config *SubscriberConfiguration, namespace, topic string) *TopicSubscriber {
	return &TopicSubscriber{
		config:         config,
		namespace:      namespace,
		topic:          topic,
		grpcDialOption: grpc.WithTransportCredentials(insecure.NewCredentials()),
	}
}

func (sub *TopicSubscriber) Connect(bootstrapBroker string) error {
	if err := sub.doLookup(bootstrapBroker); err != nil {
		return err
	}
	return nil
}
