package agent_client

import (
	"context"
	"fmt"
	"github.com/seaweedfs/seaweedfs/weed/mq/schema"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/mq_agent_pb"
	"google.golang.org/grpc"
)

type AgentSession struct {
	schema         *schema.Schema
	partitionCount int
	publisherName  string
	stream         grpc.BidiStreamingClient[mq_agent_pb.PublishRecordRequest, mq_agent_pb.PublishRecordResponse]
	sessionId      int64
}

func NewAgentSession(address string, topicSchema *schema.Schema, partitionCount int, publisherName string) (*AgentSession, error) {

	// call local agent grpc server to create a new session
	clientConn, err := pb.GrpcDial(context.Background(), address, true, grpc.WithInsecure())
	if err != nil {
		return nil, fmt.Errorf("dial agent server %s: %v", address, err)
	}
	agentClient := mq_agent_pb.NewSeaweedMessagingAgentClient(clientConn)

	resp, err := agentClient.StartPublishSession(context.Background(), &mq_agent_pb.StartPublishSessionRequest{
		Topic: &mq_agent_pb.Topic{
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

	return &AgentSession{
		schema:         topicSchema,
		partitionCount: partitionCount,
		publisherName:  publisherName,
		stream:         stream,
		sessionId:      resp.SessionId,
	}, nil
}

func (a *AgentSession) CloseSession() error {
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
