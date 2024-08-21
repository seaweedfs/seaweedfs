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
	MaxPartitionCount       int32 // how many partitions to process concurrently
	PerPartitionConcurrency int32 // how many messages to process concurrently per partition
}

type ContentConfiguration struct {
	Topic     topic.Topic
	Filter    string
	StartTime time.Time
}

type OnEachMessageFunc func(key, value []byte) (err error)
type OnCompletionFunc func()

type TopicSubscriber struct {
	SubscriberConfig                 *SubscriberConfiguration
	ContentConfig                    *ContentConfiguration
	brokerPartitionAssignmentChan    chan *mq_pb.SubscriberToSubCoordinatorResponse
	brokerPartitionAssignmentAckChan chan *mq_pb.SubscriberToSubCoordinatorRequest
	OnEachMessageFunc                OnEachMessageFunc
	OnCompletionFunc                 OnCompletionFunc
	bootstrapBrokers                 []string
	waitForMoreMessage               bool
	activeProcessors                 map[topic.Partition]*ProcessorState
	activeProcessorsLock             sync.Mutex
}

func NewTopicSubscriber(bootstrapBrokers []string, subscriber *SubscriberConfiguration, content *ContentConfiguration) *TopicSubscriber {
	return &TopicSubscriber{
		SubscriberConfig:                 subscriber,
		ContentConfig:                    content,
		brokerPartitionAssignmentChan:    make(chan *mq_pb.SubscriberToSubCoordinatorResponse, 1024),
		brokerPartitionAssignmentAckChan: make(chan *mq_pb.SubscriberToSubCoordinatorRequest, 1024),
		bootstrapBrokers:                 bootstrapBrokers,
		waitForMoreMessage:               true,
		activeProcessors:                 make(map[topic.Partition]*ProcessorState),
	}
}

func (sub *TopicSubscriber) SetEachMessageFunc(onEachMessageFn OnEachMessageFunc) {
	sub.OnEachMessageFunc = onEachMessageFn
}

func (sub *TopicSubscriber) SetCompletionFunc(onCompletionFn OnCompletionFunc) {
	sub.OnCompletionFunc = onCompletionFn
}
