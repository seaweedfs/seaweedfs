package sub_client

import (
	"context"
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

func (s *SubscriberConfiguration) String() string {
	return "ClientId: " + s.ClientId + ", ConsumerGroup: " + s.ConsumerGroup + ", ConsumerGroupInstanceId: " + s.ConsumerGroupInstanceId
}

type ContentConfiguration struct {
	Topic            topic.Topic
	Filter           string
	PartitionOffsets []*schema_pb.PartitionOffset
	OffsetType       schema_pb.OffsetType
	OffsetTsNs       int64
}

type OnDataMessageFn func(m *mq_pb.SubscribeMessageResponse_Data)
type OnCompletionFunc func()

type TopicSubscriber struct {
	ctx                              context.Context
	SubscriberConfig                 *SubscriberConfiguration
	ContentConfig                    *ContentConfiguration
	brokerPartitionAssignmentChan    chan *mq_pb.SubscriberToSubCoordinatorResponse
	brokerPartitionAssignmentAckChan chan *mq_pb.SubscriberToSubCoordinatorRequest
	OnDataMessageFunc                OnDataMessageFn
	OnCompletionFunc                 OnCompletionFunc
	bootstrapBrokers                 []string
	activeProcessors                 map[topic.Partition]*ProcessorState
	activeProcessorsLock             sync.Mutex
	PartitionOffsetChan              chan KeyedOffset
}

func NewTopicSubscriber(ctx context.Context, bootstrapBrokers []string, subscriber *SubscriberConfiguration, content *ContentConfiguration, partitionOffsetChan chan KeyedOffset) *TopicSubscriber {
	return &TopicSubscriber{
		ctx:                              ctx,
		SubscriberConfig:                 subscriber,
		ContentConfig:                    content,
		brokerPartitionAssignmentChan:    make(chan *mq_pb.SubscriberToSubCoordinatorResponse, 1024),
		brokerPartitionAssignmentAckChan: make(chan *mq_pb.SubscriberToSubCoordinatorRequest, 1024),
		bootstrapBrokers:                 bootstrapBrokers,
		activeProcessors:                 make(map[topic.Partition]*ProcessorState),
		PartitionOffsetChan:              partitionOffsetChan,
	}
}

func (sub *TopicSubscriber) SetOnDataMessageFn(fn OnDataMessageFn) {
	sub.OnDataMessageFunc = fn
}

func (sub *TopicSubscriber) SetCompletionFunc(onCompletionFn OnCompletionFunc) {
	sub.OnCompletionFunc = onCompletionFn
}
