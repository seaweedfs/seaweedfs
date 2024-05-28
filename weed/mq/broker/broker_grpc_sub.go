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

func (b *MessageQueueBroker) SubscribeMessage(req *mq_pb.SubscribeMessageRequest, stream mq_pb.SeaweedMessaging_SubscribeMessageServer) (err error) {

	ctx := stream.Context()
	clientName := fmt.Sprintf("%s/%s-%s", req.GetInit().ConsumerGroup, req.GetInit().ConsumerId, req.GetInit().ClientId)

	initMessage := req.GetInit()
	if initMessage == nil {
		glog.Errorf("missing init message")
		return fmt.Errorf("missing init message")
	}

	t := topic.FromPbTopic(req.GetInit().Topic)
	partition := topic.FromPbPartition(req.GetInit().GetPartitionOffset().GetPartition())

	glog.V(0).Infof("Subscriber %s on %v %v connected", req.GetInit().ConsumerId, t, partition)

	localTopicPartition, getOrGenErr := b.GetOrGenerateLocalPartition(t, partition)
	if getOrGenErr != nil {
		return getOrGenErr
	}

	localTopicPartition.Subscribers.AddSubscriber(clientName, topic.NewLocalSubscriber())
	glog.V(0).Infof("Subscriber %s connected on %v %v", clientName, t, partition)
	isConnected := true
	sleepIntervalCount := 0

	var counter int64
	defer func() {
		isConnected = false
		localTopicPartition.Subscribers.RemoveSubscriber(clientName)
		glog.V(0).Infof("Subscriber %s on %v %v disconnected, sent %d", clientName, t, partition, counter)
		if localTopicPartition.MaybeShutdownLocalPartition() {
			b.localTopicManager.RemoveLocalPartition(t, partition)
		}
	}()

	var startPosition log_buffer.MessagePosition
	if req.GetInit() != nil && req.GetInit().GetPartitionOffset() != nil {
		startPosition = getRequestPosition(req.GetInit().GetPartitionOffset())
	}

	return localTopicPartition.Subscribe(clientName, startPosition, func() bool {
		if !isConnected {
			return false
		}
		sleepIntervalCount++
		if sleepIntervalCount > 32 {
			sleepIntervalCount = 32
		}
		time.Sleep(time.Duration(sleepIntervalCount) * 137 * time.Millisecond)

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
	}, func(logEntry *filer_pb.LogEntry) (bool, error) {
		// reset the sleep interval count
		sleepIntervalCount = 0

		if err := stream.Send(&mq_pb.SubscribeMessageResponse{Message: &mq_pb.SubscribeMessageResponse_Data{
			Data: &mq_pb.DataMessage{
				Key:   logEntry.Key,
				Value: logEntry.Data,
				TsNs:  logEntry.TsNs,
			},
		}}); err != nil {
			glog.Errorf("Error sending data: %v", err)
			return false, err
		}

		counter++
		return false, nil
	})
}

func getRequestPosition(offset *mq_pb.PartitionOffset) (startPosition log_buffer.MessagePosition) {
	if offset.StartTsNs != 0 {
		startPosition = log_buffer.NewMessagePosition(offset.StartTsNs, -2)
	}
	if offset.StartType == mq_pb.PartitionOffsetStartType_EARLIEST {
		startPosition = log_buffer.NewMessagePosition(1, -3)
	} else if offset.StartType == mq_pb.PartitionOffsetStartType_LATEST {
		startPosition = log_buffer.NewMessagePosition(time.Now().UnixNano(), -4)
	}
	return
}
