package broker

import (
	"context"
	"fmt"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/mq/topic"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/mq_pb"
	"github.com/seaweedfs/seaweedfs/weed/util/log_buffer"
	"time"
)

func (b *MessageQueueBroker) SubscribeMessage(req *mq_pb.SubscribeMessageRequest, stream mq_pb.SeaweedMessaging_SubscribeMessageServer) error {

	ctx := stream.Context()
	clientName := fmt.Sprintf("%s/%s-%s", req.GetInit().ConsumerGroup, req.GetInit().ConsumerId, req.GetInit().ClientId)

	t := topic.FromPbTopic(req.GetInit().Topic)
	partition := topic.FromPbPartition(req.GetInit().GetPartitionOffset().GetPartition())

	glog.V(0).Infof("Subscriber %s on %v %v connected", req.GetInit().ConsumerId, t, partition)

	var localTopicPartition *topic.LocalPartition
	localTopicPartition = b.localTopicManager.GetTopicPartition(t, partition)
	for localTopicPartition == nil {
		stream.Send(&mq_pb.SubscribeMessageResponse{
			Message: &mq_pb.SubscribeMessageResponse_Ctrl{
				Ctrl: &mq_pb.SubscribeMessageResponse_CtrlMessage{
					Error: "not initialized",
				},
			},
		})
		time.Sleep(337 * time.Millisecond)
		// Check if the client has disconnected by monitoring the context
		select {
		case <-ctx.Done():
			err := ctx.Err()
			if err == context.Canceled {
				// Client disconnected
				return nil
			}
			glog.V(0).Infof("Subscriber %s disconnected: %v", clientName, err)
			return nil
		default:
			// Continue processing the request
		}
		localTopicPartition = b.localTopicManager.GetTopicPartition(t, partition)
	}

	localTopicPartition.Subscribers.AddSubscriber(clientName, topic.NewLocalSubscriber())
	glog.V(0).Infof("Subscriber %s connected on %v %v", clientName, t, partition)
	isConnected := true
	sleepIntervalCount := 0
	defer func() {
		isConnected = false
		localTopicPartition.Subscribers.RemoveSubscriber(clientName)
		glog.V(0).Infof("Subscriber %s on %v %v disconnected", clientName, t, partition)
	}()

	var startPosition log_buffer.MessagePosition
	var inMemoryOnly bool
	if req.GetInit()!=nil && req.GetInit().GetPartitionOffset() != nil {
		offset := req.GetInit().GetPartitionOffset()
		if offset.StartTsNs != 0 {
			startPosition = log_buffer.NewMessagePosition(offset.StartTsNs, -2)
		}
		if offset.StartType == mq_pb.PartitionOffsetStartType_EARLIEST {
			startPosition = log_buffer.NewMessagePosition(1, -2)
		} else if offset.StartType == mq_pb.PartitionOffsetStartType_LATEST {
			startPosition = log_buffer.NewMessagePosition(time.Now().UnixNano(), -2)
		} else if offset.StartType == mq_pb.PartitionOffsetStartType_EARLIEST_IN_MEMORY {
			inMemoryOnly = true
			for !localTopicPartition.HasData() {
				time.Sleep(337 * time.Millisecond)
			}
			memPosition := localTopicPartition.GetEarliestInMemoryMessagePosition()
			if startPosition.Before(memPosition.Time) {
				startPosition = memPosition
			}
		}
	}

	localTopicPartition.Subscribe(clientName, startPosition, inMemoryOnly, func() bool {
		if !isConnected {
			return false
		}
		sleepIntervalCount++
		if sleepIntervalCount > 10 {
			sleepIntervalCount = 10
		}
		time.Sleep(time.Duration(sleepIntervalCount) * 337 * time.Millisecond)

		// Check if the client has disconnected by monitoring the context
		select {
		case <-ctx.Done():
			err := ctx.Err()
			if err == context.Canceled {
				// Client disconnected
				return false
			}
			glog.V(0).Infof("Subscriber %s disconnected: %v", clientName, err)
			return false
		default:
			// Continue processing the request
		}

		return true
	}, func(logEntry *filer_pb.LogEntry) error {
		// reset the sleep interval count
		sleepIntervalCount = 0

		value := logEntry.GetData()
		if err := stream.Send(&mq_pb.SubscribeMessageResponse{Message: &mq_pb.SubscribeMessageResponse_Data{
			Data: &mq_pb.DataMessage{
				Key:   []byte(fmt.Sprintf("key-%d", logEntry.PartitionKeyHash)),
				Value: value,
				TsNs:  logEntry.TsNs,
			},
		}}); err != nil {
			glog.Errorf("Error sending setup response: %v", err)
			return err
		}
		return nil
	})

	return nil
}
