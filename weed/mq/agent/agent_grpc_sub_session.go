package agent

import (
	"context"
	"github.com/seaweedfs/seaweedfs/weed/mq/client/sub_client"
	"github.com/seaweedfs/seaweedfs/weed/mq/topic"
	"github.com/seaweedfs/seaweedfs/weed/pb/mq_agent_pb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"math/rand/v2"
	"time"
)

func (a *MessageQueueAgent) StartSubscribeSession(ctx context.Context, req *mq_agent_pb.StartSubscribeSessionRequest) (*mq_agent_pb.StartSubscribeSessionResponse, error) {
	sessionId := rand.Int64()

	subscriberConfig := &sub_client.SubscriberConfiguration{
		ConsumerGroup:           req.ConsumerGroup,
		ConsumerGroupInstanceId: req.ConsumerGroupInstanceId,
		GrpcDialOption:          grpc.WithTransportCredentials(insecure.NewCredentials()),
		MaxPartitionCount:       req.MaxSubscribedPartitions,
		SlidingWindowSize:       req.SlidingWindowSize,
	}

	contentConfig := &sub_client.ContentConfiguration{
		Topic:            topic.FromPbTopic(req.Topic),
		Filter:           req.Filter,
		PartitionOffsets: req.PartitionOffsets,
	}

	topicSubscriber := sub_client.NewTopicSubscriber(
		a.brokersList(),
		subscriberConfig,
		contentConfig,
		make(chan sub_client.KeyedOffset, 1024),
	)

	a.subscribersLock.Lock()
	// remove inactive publishers to avoid memory leak
	for k, entry := range a.subscribers {
		if entry.lastActiveTsNs == 0 {
			// this is an active session
			continue
		}
		if time.Unix(0, entry.lastActiveTsNs).Add(10 * time.Hour).Before(time.Now()) {
			delete(a.subscribers, k)
		}
	}
	a.subscribers[SessionId(sessionId)] = &SessionEntry[*sub_client.TopicSubscriber]{
		entry: topicSubscriber,
	}
	a.subscribersLock.Unlock()

	return &mq_agent_pb.StartSubscribeSessionResponse{
		SessionId: sessionId,
	}, nil
}
