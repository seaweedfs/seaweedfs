package agent

import (
	"fmt"
	"github.com/seaweedfs/seaweedfs/weed/pb/mq_agent_pb"
)

func (a *MessageQueueAgent) PublishRecord(stream mq_agent_pb.SeaweedMessagingAgent_PublishRecordServer) error {
	m, err := stream.Recv()
	if err != nil {
		return err
	}
	sessionId := SessionId(m.SessionId)
	a.publishersLock.RLock()
	publisherEntry, found := a.publishers[sessionId]
	a.publishersLock.RUnlock()
	if !found {
		return fmt.Errorf("publish session id %d not found", sessionId)
	}
	defer func() {
		a.publishersLock.Lock()
		delete(a.publishers, sessionId)
		a.publishersLock.Unlock()
	}()

	if m.Value != nil {
		if err := publisherEntry.entry.PublishRecord(m.Key, m.Value); err != nil {
			return err
		}
	}

	for {
		m, err = stream.Recv()
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
