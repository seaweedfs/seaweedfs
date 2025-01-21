package broker

import (
	"context"
	"fmt"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/mq/sub_coordinator"
	"github.com/seaweedfs/seaweedfs/weed/mq/topic"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/mq_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/schema_pb"
	"github.com/seaweedfs/seaweedfs/weed/util/log_buffer"
	"io"
	"time"
)

func (b *MessageQueueBroker) SubscribeMessage(stream mq_pb.SeaweedMessaging_SubscribeMessageServer) error {

	req, err := stream.Recv()
	if err != nil {
		return err
	}
	if req.GetInit() == nil {
		glog.Errorf("missing init message")
		return fmt.Errorf("missing init message")
	}

	ctx := stream.Context()
	clientName := fmt.Sprintf("%s/%s-%s", req.GetInit().ConsumerGroup, req.GetInit().ConsumerId, req.GetInit().ClientId)

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

	startPosition := b.getRequestPosition(req.GetInit())
	imt := sub_coordinator.NewInflightMessageTracker(int(req.GetInit().SlidingWindowSize))

	// connect to the follower
	var subscribeFollowMeStream mq_pb.SeaweedMessaging_SubscribeFollowMeClient
	glog.V(0).Infof("follower broker: %v", req.GetInit().FollowerBroker)
	if req.GetInit().FollowerBroker != "" {
		follower := req.GetInit().FollowerBroker
		if followerGrpcConnection, err := pb.GrpcDial(ctx, follower, true, b.grpcDialOption); err != nil {
			return fmt.Errorf("fail to dial %s: %v", follower, err)
		} else {
			defer func() {
				println("closing SubscribeFollowMe connection", follower)
				subscribeFollowMeStream.CloseSend()
				// followerGrpcConnection.Close()
			}()
			followerClient := mq_pb.NewSeaweedMessagingClient(followerGrpcConnection)
			if subscribeFollowMeStream, err = followerClient.SubscribeFollowMe(ctx); err != nil {
				return fmt.Errorf("fail to subscribe to %s: %v", follower, err)
			} else {
				if err := subscribeFollowMeStream.Send(&mq_pb.SubscribeFollowMeRequest{
					Message: &mq_pb.SubscribeFollowMeRequest_Init{
						Init: &mq_pb.SubscribeFollowMeRequest_InitMessage{
							Topic:         req.GetInit().Topic,
							Partition:     req.GetInit().GetPartitionOffset().Partition,
							ConsumerGroup: req.GetInit().ConsumerGroup,
						},
					},
				}); err != nil {
					return fmt.Errorf("fail to send init to %s: %v", follower, err)
				}
			}
		}
		glog.V(0).Infof("follower %s connected", follower)
	}

	go func() {
		var lastOffset int64
		for {
			ack, err := stream.Recv()
			if err != nil {
				if err == io.EOF {
					// the client has called CloseSend(). This is to ack the close.
					stream.Send(&mq_pb.SubscribeMessageResponse{Message: &mq_pb.SubscribeMessageResponse_Ctrl{
						Ctrl: &mq_pb.SubscribeMessageResponse_SubscribeCtrlMessage{
							IsEndOfStream: true,
						},
					}})
					break
				}
				glog.V(0).Infof("topic %v partition %v subscriber %s lastOffset %d error: %v", t, partition, clientName, lastOffset, err)
				break
			}
			if ack.GetAck().Key == nil {
				// skip ack for control messages
				continue
			}
			imt.AcknowledgeMessage(ack.GetAck().Key, ack.GetAck().Sequence)
			currentLastOffset := imt.GetOldestAckedTimestamp()
			// fmt.Printf("%+v recv (%s,%d), oldest %d\n", partition, string(ack.GetAck().Key), ack.GetAck().Sequence, currentLastOffset)
			if subscribeFollowMeStream != nil && currentLastOffset > lastOffset {
				if err := subscribeFollowMeStream.Send(&mq_pb.SubscribeFollowMeRequest{
					Message: &mq_pb.SubscribeFollowMeRequest_Ack{
						Ack: &mq_pb.SubscribeFollowMeRequest_AckMessage{
							TsNs: currentLastOffset,
						},
					},
				}); err != nil {
					glog.Errorf("Error sending ack to follower: %v", err)
					break
				}
				lastOffset = currentLastOffset
				// fmt.Printf("%+v forwarding ack %d\n", partition, lastOffset)
			}
		}
		if lastOffset > 0 {
			glog.V(0).Infof("saveConsumerGroupOffset %v %v %v %v", t, partition, req.GetInit().ConsumerGroup, lastOffset)
			if err := b.saveConsumerGroupOffset(t, partition, req.GetInit().ConsumerGroup, lastOffset); err != nil {
				glog.Errorf("saveConsumerGroupOffset partition %v lastOffset %d: %v", partition, lastOffset, err)
			}
		}
		if subscribeFollowMeStream != nil {
			if err := subscribeFollowMeStream.Send(&mq_pb.SubscribeFollowMeRequest{
				Message: &mq_pb.SubscribeFollowMeRequest_Close{
					Close: &mq_pb.SubscribeFollowMeRequest_CloseMessage{},
				},
			}); err != nil {
				glog.Errorf("Error sending close to follower: %v", err)
			}
		}
	}()

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

		for imt.IsInflight(logEntry.Key) {
			time.Sleep(137 * time.Millisecond)
		}
		if logEntry.Key != nil {
			imt.EnflightMessage(logEntry.Key, logEntry.TsNs)
		}

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

func (b *MessageQueueBroker) getRequestPosition(initMessage *mq_pb.SubscribeMessageRequest_InitMessage) (startPosition log_buffer.MessagePosition) {
	if initMessage == nil {
		return
	}
	offset := initMessage.GetPartitionOffset()
	if offset.StartTsNs != 0 {
		startPosition = log_buffer.NewMessagePosition(offset.StartTsNs, -2)
		return
	}
	if storedOffset, err := b.readConsumerGroupOffset(initMessage); err == nil {
		glog.V(0).Infof("resume from saved offset %v %v %v: %v", initMessage.Topic, initMessage.PartitionOffset.Partition, initMessage.ConsumerGroup, storedOffset)
		startPosition = log_buffer.NewMessagePosition(storedOffset, -2)
		return
	}

	if offset.StartType == schema_pb.PartitionOffsetStartType_EARLIEST {
		startPosition = log_buffer.NewMessagePosition(1, -3)
	} else if offset.StartType == schema_pb.PartitionOffsetStartType_LATEST {
		startPosition = log_buffer.NewMessagePosition(time.Now().UnixNano(), -4)
	}
	return
}
