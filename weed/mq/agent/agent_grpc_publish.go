package agent

import (
	"fmt"
	"github.com/seaweedfs/seaweedfs/weed/pb/mq_agent_pb"
	"time"
)

func (a *MessageQueueAgent) PublishRecordRequest(stream mq_agent_pb.SeaweedMessagingAgent_PublishRecordServer) error {
	m, err := stream.Recv()
	if err != nil {
		return err
	}
	a.publishersLock.RLock()
	publisherEntry, found := a.publishers[SessionId(m.SessionId)]
	a.publishersLock.RUnlock()
	if !found {
		return fmt.Errorf("publish session id %d not found", m.SessionId)
	}
	defer func() {
		publisherEntry.lastActiveTsNs = time.Now().UnixNano()
	}()
	publisherEntry.lastActiveTsNs = 0

	if m.Value != nil {
		if err := publisherEntry.entry.PublishRecord(m.Key, m.Value); err != nil {
			return err
		}
	}

	for {
		m, err := stream.Recv()
		if err != nil {
			return err
		}
		if m.Value == nil {
			continue
		}
		if err := publisherEntry.entry.PublishRecord(m.Key, m.Value); err != nil {
			return err
		}
	}
}
