package sub_client

import (
	"github.com/seaweedfs/seaweedfs/weed/pb/mq_pb"
	"google.golang.org/grpc"
)

type SubscriberConfiguration struct {
	ClientId          string
	GroupId           string
	GroupInstanceId   string
	GroupMinimumPeers int32
	GroupMaximumPeers int32
	BootstrapServers  []string
	GrpcDialOption    grpc.DialOption
}

type ContentConfiguration struct {
	Namespace string
	Topic     string
	Filter    string
}

type OnEachMessageFunc func(key, value []byte) (shouldContinue bool)
type OnCompletionFunc func()

type TopicSubscriber struct {
	SubscriberConfig           *SubscriberConfiguration
	ContentConfig              *ContentConfiguration
	brokerPartitionAssignments []*mq_pb.BrokerPartitionAssignment
	OnEachMessageFunc          OnEachMessageFunc
	OnCompletionFunc           OnCompletionFunc
	bootstrapBroker            string
}

func NewTopicSubscriber(bootstrapBroker string, subscriber *SubscriberConfiguration, content *ContentConfiguration) *TopicSubscriber {
	return &TopicSubscriber{
		SubscriberConfig: subscriber,
		ContentConfig:    content,
		bootstrapBroker:  bootstrapBroker,
	}
}

func (sub *TopicSubscriber) SetEachMessageFunc(onEachMessageFn OnEachMessageFunc) {
	sub.OnEachMessageFunc = onEachMessageFn
}

func (sub *TopicSubscriber) SetCompletionFunc(onCompeletionFn OnCompletionFunc) {
	sub.OnCompletionFunc = onCompeletionFn
}
