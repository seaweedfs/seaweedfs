package agent_client

import (
	"context"
	"fmt"
	"github.com/seaweedfs/seaweedfs/weed/mq/schema"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/mq_agent_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/schema_pb"
	"google.golang.org/grpc"
)

type PublishSession struct {
	schema         *schema.Schema
	partitionCount int
	publisherName  string
	stream         grpc.BidiStreamingClient[mq_agent_pb.PublishRecordRequest, mq_agent_pb.PublishRecordResponse]
	sessionId      int64
}

func NewPublishSession(agentAddress string, topicSchema *schema.Schema, partitionCount int, publisherName string) (*PublishSession, error) {

	// call local agent grpc server to create a new session
	clientConn, err := pb.GrpcDial(context.Background(), agentAddress, true, grpc.WithInsecure())
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

	return &PublishSession{
		schema:         topicSchema,
		partitionCount: partitionCount,
		publisherName:  publisherName,
		stream:         stream,
		sessionId:      resp.SessionId,
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
