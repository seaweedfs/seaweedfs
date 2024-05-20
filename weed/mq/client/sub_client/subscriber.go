package sub_client

import (
	"github.com/seaweedfs/seaweedfs/weed/mq/topic"
	"github.com/seaweedfs/seaweedfs/weed/pb/mq_pb"
	"google.golang.org/grpc"
	"sync"
	"time"
)

type SubscriberConfiguration struct {
	ClientId                string
	ConsumerGroup           string
	ConsumerGroupInstanceId string
	GrpcDialOption          grpc.DialOption
}

type ContentConfiguration struct {
	Topic     topic.Topic
	Filter    string
	StartTime time.Time
}

type ProcessorConfiguration struct {
	MaxPartitionCount       int32 // how many partitions to process concurrently
	PerPartitionConcurrency int32 // how many messages to process concurrently per partition
}

type OnEachMessageFunc func(key, value []byte) (err error)
type OnCompletionFunc func()

type TopicSubscriber struct {
	SubscriberConfig              *SubscriberConfiguration
	ContentConfig                 *ContentConfiguration
	ProcessorConfig               *ProcessorConfiguration
	brokerPartitionAssignmentChan chan *mq_pb.BrokerPartitionAssignment
	OnEachMessageFunc             OnEachMessageFunc
	OnCompletionFunc              OnCompletionFunc
	bootstrapBrokers              []string
	waitForMoreMessage            bool
	activeProcessors              map[topic.Partition]*ProcessorState
	activeProcessorsLock          sync.Mutex
}

func NewTopicSubscriber(bootstrapBrokers []string, subscriber *SubscriberConfiguration, content *ContentConfiguration, processor ProcessorConfiguration) *TopicSubscriber {
	return &TopicSubscriber{
		SubscriberConfig:              subscriber,
		ContentConfig:                 content,
		ProcessorConfig:               &processor,
		brokerPartitionAssignmentChan: make(chan *mq_pb.BrokerPartitionAssignment, 1024),
		bootstrapBrokers:              bootstrapBrokers,
		waitForMoreMessage:            true,
		activeProcessors:              make(map[topic.Partition]*ProcessorState),
	}
}

func (sub *TopicSubscriber) SetEachMessageFunc(onEachMessageFn OnEachMessageFunc) {
	sub.OnEachMessageFunc = onEachMessageFn
}

func (sub *TopicSubscriber) SetCompletionFunc(onCompletionFn OnCompletionFunc) {
	sub.OnCompletionFunc = onCompletionFn
}
