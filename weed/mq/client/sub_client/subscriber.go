package sub_client

import (
	"github.com/seaweedfs/seaweedfs/weed/mq/topic"
	"github.com/seaweedfs/seaweedfs/weed/pb/mq_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/schema_pb"
	"google.golang.org/grpc"
	"sync"
)

type SubscriberConfiguration struct {
	ClientId                string
	ConsumerGroup           string
	ConsumerGroupInstanceId string
	GrpcDialOption          grpc.DialOption
	MaxPartitionCount       int32 // how many partitions to process concurrently
	SlidingWindowSize       int32 // how many messages to process concurrently per partition
}

type ContentConfiguration struct {
	Topic            topic.Topic
	Filter           string
	PartitionOffsets []*schema_pb.PartitionOffset
}

type OnDataMessageFn func(m *mq_pb.SubscribeMessageResponse_Data)
type OnEachMessageFunc func(key, value []byte) (err error)
type OnCompletionFunc func()

type TopicSubscriber struct {
	SubscriberConfig                 *SubscriberConfiguration
	ContentConfig                    *ContentConfiguration
	brokerPartitionAssignmentChan    chan *mq_pb.SubscriberToSubCoordinatorResponse
	brokerPartitionAssignmentAckChan chan *mq_pb.SubscriberToSubCoordinatorRequest
	OnDataMessageFnnc                OnDataMessageFn
	OnEachMessageFunc                OnEachMessageFunc
	OnCompletionFunc                 OnCompletionFunc
	bootstrapBrokers                 []string
	waitForMoreMessage               bool
	activeProcessors                 map[topic.Partition]*ProcessorState
	activeProcessorsLock             sync.Mutex
	PartitionOffsetChan              chan KeyedOffset
}

func NewTopicSubscriber(bootstrapBrokers []string, subscriber *SubscriberConfiguration, content *ContentConfiguration, partitionOffsetChan chan KeyedOffset) *TopicSubscriber {
	return &TopicSubscriber{
		SubscriberConfig:                 subscriber,
		ContentConfig:                    content,
		brokerPartitionAssignmentChan:    make(chan *mq_pb.SubscriberToSubCoordinatorResponse, 1024),
		brokerPartitionAssignmentAckChan: make(chan *mq_pb.SubscriberToSubCoordinatorRequest, 1024),
		bootstrapBrokers:                 bootstrapBrokers,
		waitForMoreMessage:               true,
		activeProcessors:                 make(map[topic.Partition]*ProcessorState),
		PartitionOffsetChan:              partitionOffsetChan,
	}
}

func (sub *TopicSubscriber) SetEachMessageFunc(onEachMessageFn OnEachMessageFunc) {
	sub.OnEachMessageFunc = onEachMessageFn
}

func (sub *TopicSubscriber) SetOnDataMessageFn(fn OnDataMessageFn) {
	sub.OnDataMessageFnnc = fn
}

func (sub *TopicSubscriber) SetCompletionFunc(onCompletionFn OnCompletionFunc) {
	sub.OnCompletionFunc = onCompletionFn
}
