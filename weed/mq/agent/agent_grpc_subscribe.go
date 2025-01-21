package agent

import (
	"fmt"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/mq/client/sub_client"
	"github.com/seaweedfs/seaweedfs/weed/pb/mq_agent_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/mq_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/schema_pb"
	"google.golang.org/protobuf/proto"
	"time"
)

func (a *MessageQueueAgent) SubscribeRecordRequest(stream mq_agent_pb.SeaweedMessagingAgent_SubscribeRecordServer) error {
	// the first message is the subscribe request
	// it should only contain the session id
	m, err := stream.Recv()
	if err != nil {
		return err
	}
	a.subscribersLock.RLock()
	subscriberEntry, found := a.subscribers[SessionId(m.SessionId)]
	a.subscribersLock.RUnlock()
	if !found {
		return fmt.Errorf("subscribe session id %d not found", m.SessionId)
	}
	defer func() {
		subscriberEntry.lastActiveTsNs = time.Now().UnixNano()
	}()
	subscriberEntry.lastActiveTsNs = 0

	var lastErr error
	subscriberEntry.entry.SetOnDataMessageFn(func(m *mq_pb.SubscribeMessageResponse_Data) {
		record := &schema_pb.RecordValue{}
		err := proto.Unmarshal(m.Data.Value, record)
		if err != nil {
			if lastErr == nil {
				lastErr = err
			}
			return
		}
		if sendErr := stream.Send(&mq_agent_pb.SubscribeRecordResponse{
			Key:   m.Data.Key,
			Value: record,
			TsNs:  m.Data.TsNs,
		}); sendErr != nil {
			if lastErr == nil {
				lastErr = sendErr
			}
		}
	})

	go func() {
		subErr := subscriberEntry.entry.Subscribe()
		if subErr != nil {
			glog.V(0).Infof("subscriber %d subscribe: %v", m.SessionId, subErr)
			if lastErr == nil {
				lastErr = subErr
			}
		}
	}()

	for {
		m, err := stream.Recv()
		if err != nil {
			return err
		}
		if m != nil {
			subscriberEntry.entry.PartitionOffsetChan <- sub_client.KeyedOffset{
				Key:    m.AckKey,
				Offset: m.AckSequence,
			}
		}
	}
}
