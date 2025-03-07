package agent_client

import (
	"context"
	"fmt"
	"github.com/seaweedfs/seaweedfs/weed/mq/schema"
	"github.com/seaweedfs/seaweedfs/weed/pb/mq_agent_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/schema_pb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type PublishSession struct {
	schema         *schema.Schema
	partitionCount int
	publisherName  string
	stream         grpc.BidiStreamingClient[mq_agent_pb.PublishRecordRequest, mq_agent_pb.PublishRecordResponse]
}

func NewPublishSession(agentAddress string, topicSchema *schema.Schema, partitionCount int, publisherName string) (*PublishSession, error) {

	// call local agent grpc server to create a new session
	clientConn, err := grpc.NewClient(agentAddress, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("dial agent server %s: %v", agentAddress, err)
	}
	agentClient := mq_agent_pb.NewSeaweedMessagingAgentClient(clientConn)

	resp, err := agentClient.StartPublishSession(context.Background(), &mq_agent_pb.StartPublishSessionRequest{
		Topic: &schema_pb.Topic{
			Namespace: topicSchema.Namespace,
			Name:      topicSchema.Name,
		},
		PartitionCount: int32(partitionCount),
		RecordType:     topicSchema.RecordType,
		PublisherName:  publisherName,
	})
	if err != nil {
		return nil, err
	}
	if resp.Error != "" {
		return nil, fmt.Errorf("start publish session: %v", resp.Error)
	}

	stream, err := agentClient.PublishRecord(context.Background())
	if err != nil {
		return nil, fmt.Errorf("publish record: %v", err)
	}

	if err = stream.Send(&mq_agent_pb.PublishRecordRequest{
		SessionId: resp.SessionId,
	}); err != nil {
		return nil, fmt.Errorf("send session id: %v", err)
	}

	return &PublishSession{
		schema:         topicSchema,
		partitionCount: partitionCount,
		publisherName:  publisherName,
		stream:         stream,
	}, nil
}

func (a *PublishSession) CloseSession() error {
	if a.schema == nil {
		return nil
	}
	err := a.stream.CloseSend()
	if err != nil {
		return fmt.Errorf("close send: %v", err)
	}
	a.schema = nil
	return err
}

func (a *PublishSession) PublishMessageRecord(key []byte, record *schema_pb.RecordValue) error {
	return a.stream.Send(&mq_agent_pb.PublishRecordRequest{
		Key:   key,
		Value: record,
	})
}
