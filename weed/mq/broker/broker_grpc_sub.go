package broker

import (
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/mq/topic"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/mq_pb"
	"time"
)

func (broker *MessageQueueBroker) Subscribe(req *mq_pb.SubscribeRequest, stream mq_pb.SeaweedMessaging_SubscribeServer) error {

	localTopicPartition := broker.localTopicManager.GetTopicPartition(topic.FromPbTopic(req.Init.Topic),
		topic.FromPbPartition(req.Init.Partition))
	if localTopicPartition == nil {
		stream.Send(&mq_pb.SubscribeResponse{
			Message: &mq_pb.SubscribeResponse_Ctrl{
				&mq_pb.SubscribeResponse_CtrlMessage{
					Error: "not found",
				},
			},
		})
		return nil
	}

	localTopicPartition.Subscribe("client", time.Now(), func(logEntry *filer_pb.LogEntry) error {
		if err := stream.Send(&mq_pb.SubscribeResponse{Message: &mq_pb.SubscribeResponse_Data{
			Data: &mq_pb.DataMessage{
				Key:   []byte("hello"),
				Value: []byte("world"),
			},
		}}); err != nil {
			glog.Errorf("Error sending setup response: %v", err)
			return err
		}
		return nil
	})

	return nil
}
