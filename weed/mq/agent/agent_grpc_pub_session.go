package agent

import (
	"context"
	"github.com/seaweedfs/seaweedfs/weed/mq/client/pub_client"
	"github.com/seaweedfs/seaweedfs/weed/mq/topic"
	"github.com/seaweedfs/seaweedfs/weed/pb/mq_agent_pb"
	"log/slog"
	"math/rand/v2"
)

func (a *MessageQueueAgent) StartPublishSession(ctx context.Context, req *mq_agent_pb.StartPublishSessionRequest) (*mq_agent_pb.StartPublishSessionResponse, error) {
	sessionId := rand.Int64()

	topicPublisher, err := pub_client.NewTopicPublisher(
		&pub_client.PublisherConfiguration{
			Topic:          topic.NewTopic(req.Topic.Namespace, req.Topic.Name),
			PartitionCount: req.PartitionCount,
			Brokers:        a.brokersList(),
			PublisherName:  req.PublisherName,
			RecordType:     req.RecordType,
		})
	if err != nil {
		return nil, err
	}

	a.publishersLock.Lock()
	a.publishers[SessionId(sessionId)] = &SessionEntry[*pub_client.TopicPublisher]{
		entry: topicPublisher,
	}
	a.publishersLock.Unlock()

	return &mq_agent_pb.StartPublishSessionResponse{
		SessionId: sessionId,
	}, nil
}

func (a *MessageQueueAgent) ClosePublishSession(ctx context.Context, req *mq_agent_pb.ClosePublishSessionRequest) (*mq_agent_pb.ClosePublishSessionResponse, error) {
	var finishErr string
	a.publishersLock.Lock()
	publisherEntry, found := a.publishers[SessionId(req.SessionId)]
	if found {
		if err := publisherEntry.entry.FinishPublish(); err != nil {
			finishErr = err.Error()
			slog.Warn("failed to finish publish", "error", err)
		}
		delete(a.publishers, SessionId(req.SessionId))
	}
	a.publishersLock.Unlock()
	return &mq_agent_pb.ClosePublishSessionResponse{
		Error: finishErr,
	}, nil
}
