package broker

import (
	"context"
	"fmt"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/mq/topic"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/mq_pb"
	"github.com/seaweedfs/seaweedfs/weed/util/log_buffer"
	"sync/atomic"
	"time"
)

func (b *MessageQueueBroker) SubscribeMessage(req *mq_pb.SubscribeMessageRequest, stream mq_pb.SeaweedMessaging_SubscribeMessageServer) (err error) {

	ctx := stream.Context()
	clientName := fmt.Sprintf("%s/%s-%s", req.GetInit().ConsumerGroup, req.GetInit().ConsumerId, req.GetInit().ClientId)

	t := topic.FromPbTopic(req.GetInit().Topic)
	partition := topic.FromPbPartition(req.GetInit().GetPartitionOffset().GetPartition())

	glog.V(0).Infof("Subscriber %s on %v %v connected", req.GetInit().ConsumerId, t, partition)

	waitIntervalCount := 0

	var localTopicPartition *topic.LocalPartition
	for localTopicPartition == nil {
		localTopicPartition, _, err = b.GetOrGenLocalPartition(t, partition)
		if err != nil {
			glog.V(1).Infof("topic %v partition %v not setup", t, partition)
		}
		if localTopicPartition != nil {
			break
		}
		waitIntervalCount++
		if waitIntervalCount > 10 {
			waitIntervalCount = 10
		}
		time.Sleep(time.Duration(waitIntervalCount) * 337 * time.Millisecond)
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
			b.localTopicManager.RemoveTopicPartition(t, partition)
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

func (b *MessageQueueBroker) FollowInMemoryMessages(req *mq_pb.FollowInMemoryMessagesRequest, stream mq_pb.SeaweedMessaging_FollowInMemoryMessagesServer) (err error) {
	ctx := stream.Context()
	clientName := req.GetInit().ConsumerId

	t := topic.FromPbTopic(req.GetInit().Topic)
	partition := topic.FromPbPartition(req.GetInit().GetPartitionOffset().GetPartition())

	glog.V(0).Infof("FollowInMemoryMessages %s on %v %v connected", clientName, t, partition)

	waitIntervalCount := 0

	var localTopicPartition *topic.LocalPartition
	for localTopicPartition == nil {
		localTopicPartition, _, err = b.GetOrGenLocalPartition(t, partition)
		if err != nil {
			glog.V(1).Infof("topic %v partition %v not setup", t, partition)
		}
		if localTopicPartition != nil {
			break
		}
		waitIntervalCount++
		if waitIntervalCount > 32 {
			waitIntervalCount = 32
		}
		time.Sleep(time.Duration(waitIntervalCount) * 137 * time.Millisecond)
		// Check if the client has disconnected by monitoring the context
		select {
		case <-ctx.Done():
			err := ctx.Err()
			if err == context.Canceled {
				// Client disconnected
				return nil
			}
			glog.V(0).Infof("FollowInMemoryMessages %s disconnected: %v", clientName, err)
			return nil
		default:
			// Continue processing the request
		}
	}

	// set the current follower id
	followerId := req.GetInit().FollowerId
	atomic.StoreInt32(&localTopicPartition.FollowerId, followerId)

	glog.V(0).Infof("FollowInMemoryMessages %s connected on %v %v", clientName, t, partition)
	isConnected := true
	sleepIntervalCount := 0

	var counter int64
	defer func() {
		isConnected = false
		glog.V(0).Infof("FollowInMemoryMessages %s on %v %v disconnected, sent %d", clientName, t, partition, counter)
	}()

	// send first hello message
	// to indicate the follower is connected
	stream.Send(&mq_pb.FollowInMemoryMessagesResponse{
		Message: &mq_pb.FollowInMemoryMessagesResponse_Ctrl{
			Ctrl: &mq_pb.FollowInMemoryMessagesResponse_CtrlMessage{},
		},
	})

	var startPosition log_buffer.MessagePosition
	if req.GetInit() != nil && req.GetInit().GetPartitionOffset() != nil {
		startPosition = getRequestPosition(req.GetInit().GetPartitionOffset())
	}

	var prevFlushTsNs int64

	_, _, err = localTopicPartition.LogBuffer.LoopProcessLogData(clientName, startPosition, 0, func() bool {
		if !isConnected {
			return false
		}
		sleepIntervalCount++
		if sleepIntervalCount > 32 {
			sleepIntervalCount = 32
		}
		time.Sleep(time.Duration(sleepIntervalCount) * 137 * time.Millisecond)

		if localTopicPartition.LogBuffer.IsStopping() {
			newFollowerId := atomic.LoadInt32(&localTopicPartition.FollowerId)
			glog.V(0).Infof("FollowInMemoryMessages1 %s on %v %v follower id changed to %d", clientName, t, partition, newFollowerId)
			stream.Send(&mq_pb.FollowInMemoryMessagesResponse{
				Message: &mq_pb.FollowInMemoryMessagesResponse_Ctrl{
					Ctrl: &mq_pb.FollowInMemoryMessagesResponse_CtrlMessage{
						FollowerChangedToId: newFollowerId,
					},
				},
			})
			return false
		}

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

		// send the last flushed sequence
		flushTsNs := atomic.LoadInt64(&localTopicPartition.LogBuffer.LastFlushTsNs)
		if flushTsNs != prevFlushTsNs {
			prevFlushTsNs = flushTsNs
			stream.Send(&mq_pb.FollowInMemoryMessagesResponse{
				Message: &mq_pb.FollowInMemoryMessagesResponse_Ctrl{
					Ctrl: &mq_pb.FollowInMemoryMessagesResponse_CtrlMessage{
						FlushedSequence: flushTsNs,
					},
				},
			})
		}

		return true
	}, func(logEntry *filer_pb.LogEntry) (bool, error) {
		// reset the sleep interval count
		sleepIntervalCount = 0

		// check the follower id
		newFollowerId := atomic.LoadInt32(&localTopicPartition.FollowerId)
		if newFollowerId != followerId {
			glog.V(0).Infof("FollowInMemoryMessages2 %s on %v %v follower id %d changed to %d", clientName, t, partition, followerId, newFollowerId)
			stream.Send(&mq_pb.FollowInMemoryMessagesResponse{
				Message: &mq_pb.FollowInMemoryMessagesResponse_Ctrl{
					Ctrl: &mq_pb.FollowInMemoryMessagesResponse_CtrlMessage{
						FollowerChangedToId: newFollowerId,
					},
				},
			})
			return true, nil
		}

		// send the last flushed sequence
		flushTsNs := atomic.LoadInt64(&localTopicPartition.LogBuffer.LastFlushTsNs)
		if flushTsNs != prevFlushTsNs {
			prevFlushTsNs = flushTsNs
			stream.Send(&mq_pb.FollowInMemoryMessagesResponse{
				Message: &mq_pb.FollowInMemoryMessagesResponse_Ctrl{
					Ctrl: &mq_pb.FollowInMemoryMessagesResponse_CtrlMessage{
						FlushedSequence: flushTsNs,
					},
				},
			})
		}

		// send the log entry
		if err := stream.Send(&mq_pb.FollowInMemoryMessagesResponse{
			Message: &mq_pb.FollowInMemoryMessagesResponse_Data{
				Data: &mq_pb.DataMessage{
					Key:   logEntry.Key,
					Value: logEntry.Data,
					TsNs:  logEntry.TsNs,
				},
			}}); err != nil {
			glog.Errorf("Error sending setup response: %v", err)
			return false, err
		}

		counter++
		return false, nil
	})

	return err
}
