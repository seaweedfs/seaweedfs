package agent

import (
	"fmt"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/mq/client/sub_client"
	"github.com/seaweedfs/seaweedfs/weed/pb/mq_agent_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/mq_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/schema_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"google.golang.org/protobuf/proto"
)

func (a *MessageQueueAgent) SubscribeRecord(stream mq_agent_pb.SeaweedMessagingAgent_SubscribeRecordServer) error {
	// the first message is the subscribe request
	// it should only contain the session id
	initMessage, err := stream.Recv()
	if err != nil {
		return err
	}
	sessionId := SessionId(initMessage.SessionId)
	a.subscribersLock.RLock()
	subscriberEntry, found := a.subscribers[sessionId]
	a.subscribersLock.RUnlock()
	if !found {
		return fmt.Errorf("subscribe session id %d not found", sessionId)
	}
	defer func() {
		a.subscribersLock.Lock()
		delete(a.subscribers, sessionId)
		a.subscribersLock.Unlock()
	}()

	var lastErr error
	executors := util.NewLimitedConcurrentExecutor(int(subscriberEntry.entry.SubscriberConfig.SlidingWindowSize))
	subscriberEntry.entry.SetOnDataMessageFn(func(m *mq_pb.SubscribeMessageResponse_Data) {
		executors.Execute(func() {
			record := &schema_pb.RecordValue{}
			err := proto.Unmarshal(m.Data.Value, record)
			if err != nil {
				glog.V(0).Infof("unmarshal record value: %v", err)
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
				glog.V(0).Infof("send record: %v", sendErr)
				if lastErr == nil {
					lastErr = sendErr
				}
			}
		})
	})

	go func() {
		subErr := subscriberEntry.entry.Subscribe()
		if subErr != nil {
			glog.V(0).Infof("subscriber %d subscribe: %v", sessionId, subErr)
			if lastErr == nil {
				lastErr = subErr
			}
		}
	}()

	for {
		m, err := stream.Recv()
		if err != nil {
			glog.V(0).Infof("subscriber %d receive: %v", sessionId, err)
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
