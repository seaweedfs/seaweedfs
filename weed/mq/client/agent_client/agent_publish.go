package agent_client

import (
	"github.com/seaweedfs/seaweedfs/weed/pb/mq_agent_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/schema_pb"
)

func (a *PublishSession) PublishMessageRecord(key []byte, record *schema_pb.RecordValue) error {
	return a.stream.Send(&mq_agent_pb.PublishRecordRequest{
		SessionId: a.sessionId,
		Key:       key,
		Value:     record,
	})
}
