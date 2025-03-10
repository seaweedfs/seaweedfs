package agent_client

import (
	"context"
	"fmt"
	"github.com/seaweedfs/seaweedfs/weed/mq/topic"
	"github.com/seaweedfs/seaweedfs/weed/pb/mq_agent_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/schema_pb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type SubscribeOption struct {
	ConsumerGroup           string
	ConsumerGroupInstanceId string
	Topic                   topic.Topic
	OffsetType              schema_pb.OffsetType
	OffsetTsNs              int64
	Filter                  string
	MaxSubscribedPartitions int32
	SlidingWindowSize       int32
}

type SubscribeSession struct {
	Option *SubscribeOption
	stream grpc.BidiStreamingClient[mq_agent_pb.SubscribeRecordRequest, mq_agent_pb.SubscribeRecordResponse]
}

func NewSubscribeSession(agentAddress string, option *SubscribeOption) (*SubscribeSession, error) {
	// call local agent grpc server to create a new session
	clientConn, err := grpc.NewClient(agentAddress, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("dial agent server %s: %v", agentAddress, err)
	}
	agentClient := mq_agent_pb.NewSeaweedMessagingAgentClient(clientConn)

	initRequest := &mq_agent_pb.SubscribeRecordRequest_InitSubscribeRecordRequest{
		ConsumerGroup:           option.ConsumerGroup,
		ConsumerGroupInstanceId: option.ConsumerGroupInstanceId,
		Topic: &schema_pb.Topic{
			Namespace: option.Topic.Namespace,
			Name:      option.Topic.Name,
		},
		OffsetType:              option.OffsetType,
		OffsetTsNs:              option.OffsetTsNs,
		MaxSubscribedPartitions: option.MaxSubscribedPartitions,
		Filter:                  option.Filter,
		SlidingWindowSize:       option.SlidingWindowSize,
	}

	stream, err := agentClient.SubscribeRecord(context.Background())
	if err != nil {
		return nil, fmt.Errorf("subscribe record: %v", err)
	}

	if err = stream.Send(&mq_agent_pb.SubscribeRecordRequest{
		Init: initRequest,
	}); err != nil {
		return nil, fmt.Errorf("send session id: %v", err)
	}

	return &SubscribeSession{
		Option: option,
		stream: stream,
	}, nil
}

func (s *SubscribeSession) CloseSession() error {
	err := s.stream.CloseSend()
	return err
}

func (a *SubscribeSession) SubscribeMessageRecord(
	onEachMessageFn func(key []byte, record *schema_pb.RecordValue),
	onCompletionFn func()) error {
	for {
		resp, err := a.stream.Recv()
		if err != nil {
			return err
		}
		onEachMessageFn(resp.Key, resp.Value)
	}
	if onCompletionFn != nil {
		onCompletionFn()
	}
	return nil
}
