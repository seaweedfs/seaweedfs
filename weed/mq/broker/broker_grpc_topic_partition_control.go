package broker

import (
	"context"
	"github.com/seaweedfs/seaweedfs/weed/mq/topic"
	"github.com/seaweedfs/seaweedfs/weed/pb/mq_pb"
)

func (b *MessageQueueBroker) ClosePublishers(ctx context.Context, request *mq_pb.ClosePublishersRequest) (resp *mq_pb.ClosePublishersResponse, err error) {
	resp = &mq_pb.ClosePublishersResponse{}

	t := topic.FromPbTopic(request.Topic)

	b.localTopicManager.ClosePublishers(t, request.UnixTimeNs)

	// wait until all publishers are closed
	b.localTopicManager.WaitUntilNoPublishers(t)

	return
}

func (b *MessageQueueBroker) CloseSubscribers(ctx context.Context, request *mq_pb.CloseSubscribersRequest) (resp *mq_pb.CloseSubscribersResponse, err error) {
	resp = &mq_pb.CloseSubscribersResponse{}

	b.localTopicManager.CloseSubscribers(topic.FromPbTopic(request.Topic), request.UnixTimeNs)

	return
}
