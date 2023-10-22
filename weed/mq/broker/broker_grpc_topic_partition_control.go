package broker

import (
	"context"
	"github.com/seaweedfs/seaweedfs/weed/mq/topic"
	"github.com/seaweedfs/seaweedfs/weed/pb/mq_pb"
)

func (broker *MessageQueueBroker) ClosePublishers(ctx context.Context, request *mq_pb.ClosePublishersRequest) (resp *mq_pb.ClosePublishersResponse, err error) {
	resp = &mq_pb.ClosePublishersResponse{}

	broker.localTopicManager.ClosePublishers(topic.FromPbTopic(request.Topic), request.UnixTimeNs)

	return
}

func (broker *MessageQueueBroker) CloseSubscribers(ctx context.Context, request *mq_pb.CloseSubscribersRequest) (resp *mq_pb.CloseSubscribersResponse, err error) {
	resp = &mq_pb.CloseSubscribersResponse{}

	broker.localTopicManager.CloseSubscribers(topic.FromPbTopic(request.Topic), request.UnixTimeNs)

	return
}
