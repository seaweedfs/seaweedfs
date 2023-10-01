package broker

import (
	"fmt"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/mq/topic"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/mq_pb"
	"time"
)

func (broker *MessageQueueBroker) Subscribe(req *mq_pb.SubscribeRequest, stream mq_pb.SeaweedMessaging_SubscribeServer) error {

	localTopicPartition := broker.localTopicManager.GetTopicPartition(topic.FromPbTopic(req.GetInit().Topic),
		topic.FromPbPartition(req.GetInit().Partition))
	if localTopicPartition == nil {
		stream.Send(&mq_pb.SubscribeResponse{
			Message: &mq_pb.SubscribeResponse_Ctrl{
				Ctrl: &mq_pb.SubscribeResponse_CtrlMessage{
					Error: "not initialized",
				},
			},
		})
		return nil
	}

	clientName := fmt.Sprintf("%s/%s-%s", req.GetInit().ConsumerGroup, req.GetInit().ConsumerId, req.GetInit().ClientId)

	localTopicPartition.Subscribe(clientName, time.Now(), func(logEntry *filer_pb.LogEntry) error {
		value := logEntry.GetData()
		if err := stream.Send(&mq_pb.SubscribeResponse{Message: &mq_pb.SubscribeResponse_Data{
			Data: &mq_pb.DataMessage{
				Key:   []byte(fmt.Sprintf("key-%d", logEntry.PartitionKeyHash)),
				Value: value,
			},
		}}); err != nil {
			glog.Errorf("Error sending setup response: %v", err)
			return err
		}
		return nil
	})

	return nil
}
