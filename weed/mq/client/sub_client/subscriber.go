package sub_client

import (
	"github.com/seaweedfs/seaweedfs/weed/mq/topic"
	"github.com/seaweedfs/seaweedfs/weed/pb/mq_pb"
	"google.golang.org/grpc"
	"time"
)

type SubscriberConfiguration struct {
	ClientId                string
	ConsumerGroup           string
	ConsumerGroupInstanceId string
	GroupMinimumPeers       int32
	GroupMaximumPeers       int32
	BootstrapServers        []string
	GrpcDialOption          grpc.DialOption
}

type ContentConfiguration struct {
	Topic     topic.Topic
	Filter    string
	StartTime time.Time
}

type ProcessorConfiguration struct {
	ConcurrentPartitionLimit int // how many partitions to process concurrently
}

type OnEachMessageFunc func(key, value []byte) (shouldContinue bool, err error)
type OnCompletionFunc func()

type TopicSubscriber struct {
	SubscriberConfig           *SubscriberConfiguration
	ContentConfig              *ContentConfiguration
	ProcessorConfig            *ProcessorConfiguration
	brokerPartitionAssignments []*mq_pb.BrokerPartitionAssignment
	OnEachMessageFunc          OnEachMessageFunc
	OnCompletionFunc           OnCompletionFunc
	bootstrapBrokers           []string
	waitForMoreMessage         bool
	alreadyProcessedTsNs       int64
}

func NewTopicSubscriber(bootstrapBrokers []string, subscriber *SubscriberConfiguration, content *ContentConfiguration, processor ProcessorConfiguration) *TopicSubscriber {
	return &TopicSubscriber{
		SubscriberConfig:     subscriber,
		ContentConfig:        content,
		ProcessorConfig:      &processor,
		bootstrapBrokers:     bootstrapBrokers,
		waitForMoreMessage:   true,
		alreadyProcessedTsNs: content.StartTime.UnixNano(),
	}
}

func (sub *TopicSubscriber) SetEachMessageFunc(onEachMessageFn OnEachMessageFunc) {
	sub.OnEachMessageFunc = onEachMessageFn
}

func (sub *TopicSubscriber) SetCompletionFunc(onCompletionFn OnCompletionFunc) {
	sub.OnCompletionFunc = onCompletionFn
}
