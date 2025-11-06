package broker

import (
	"context"
	"fmt"
	"io"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/mq/sub_coordinator"
	"github.com/seaweedfs/seaweedfs/weed/mq/topic"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/mq_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/schema_pb"
	"github.com/seaweedfs/seaweedfs/weed/util/log_buffer"
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

	// Create a cancellable context so we can properly clean up when the client disconnects
	ctx, cancel := context.WithCancel(stream.Context())
	defer cancel() // Ensure context is cancelled when function exits

	clientName := fmt.Sprintf("%s/%s-%s", req.GetInit().ConsumerGroup, req.GetInit().ConsumerId, req.GetInit().ClientId)

	t := topic.FromPbTopic(req.GetInit().Topic)
	partition := topic.FromPbPartition(req.GetInit().GetPartitionOffset().GetPartition())

	glog.V(0).Infof("Subscriber %s on %v %v connected", req.GetInit().ConsumerId, t, partition)

	glog.V(4).Infof("Calling GetOrGenerateLocalPartition for %s %s", t, partition)
	localTopicPartition, getOrGenErr := b.GetOrGenerateLocalPartition(t, partition)
	if getOrGenErr != nil {
		glog.V(4).Infof("GetOrGenerateLocalPartition failed: %v", getOrGenErr)
		return getOrGenErr
	}
	glog.V(4).Infof("GetOrGenerateLocalPartition succeeded, localTopicPartition=%v", localTopicPartition != nil)
	if localTopicPartition == nil {
		return fmt.Errorf("failed to get or generate local partition for topic %v partition %v", t, partition)
	}

	subscriber := topic.NewLocalSubscriber()
	localTopicPartition.Subscribers.AddSubscriber(clientName, subscriber)
	glog.V(0).Infof("Subscriber %s connected on %v %v", clientName, t, partition)
	isConnected := true

	var counter int64
	startPosition := b.getRequestPosition(req.GetInit())
	imt := sub_coordinator.NewInflightMessageTracker(int(req.GetInit().SlidingWindowSize))

	defer func() {
		isConnected = false
		// Clean up any in-flight messages to prevent them from blocking other subscribers
		if cleanedCount := imt.Cleanup(); cleanedCount > 0 {
			glog.V(0).Infof("Subscriber %s cleaned up %d in-flight messages on disconnect", clientName, cleanedCount)
		}
		localTopicPartition.Subscribers.RemoveSubscriber(clientName)
		glog.V(0).Infof("Subscriber %s on %v %v disconnected, sent %d", clientName, t, partition, counter)
		// Use topic-aware shutdown logic to prevent aggressive removal of system topics
		if localTopicPartition.MaybeShutdownLocalPartitionForTopic(t.Name) {
			b.localTopicManager.RemoveLocalPartition(t, partition)
		}
	}()

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
				if subscribeFollowMeStream != nil {
					subscribeFollowMeStream.CloseSend()
				}
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

	// Channel to handle seek requests - signals Subscribe loop to restart from new offset
	seekChan := make(chan *mq_pb.SubscribeMessageRequest_SeekMessage, 1)

	go func() {
		defer cancel() // CRITICAL: Cancel context when Recv goroutine exits (client disconnect)

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
			// Handle seek messages
			if seekMsg := ack.GetSeek(); seekMsg != nil {
				glog.V(0).Infof("Subscriber %s received seek request to offset %d (type %v)",
					clientName, seekMsg.Offset, seekMsg.OffsetType)

				// Send seek request to Subscribe loop
				select {
				case seekChan <- seekMsg:
					glog.V(0).Infof("Subscriber %s seek request queued", clientName)
				default:
					glog.V(0).Infof("Subscriber %s seek request dropped (already pending)", clientName)
					// Send error response if seek is already in progress
					stream.Send(&mq_pb.SubscribeMessageResponse{Message: &mq_pb.SubscribeMessageResponse_Ctrl{
						Ctrl: &mq_pb.SubscribeMessageResponse_SubscribeCtrlMessage{
							Error: "Seek already in progress",
						},
					}})
				}
				continue
			}

			if ack.GetAck().Key == nil {
				// skip ack for control messages
				continue
			}
			imt.AcknowledgeMessage(ack.GetAck().Key, ack.GetAck().TsNs)

			currentLastOffset := imt.GetOldestAckedTimestamp()
			// Update acknowledged offset and last seen time for this subscriber when it sends an ack
			subscriber.UpdateAckedOffset(currentLastOffset)
			// fmt.Printf("%+v recv (%s,%d), oldest %d\n", partition, string(ack.GetAck().Key), ack.GetAck().TsNs, currentLastOffset)
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
				if err != io.EOF {
					glog.Errorf("Error sending close to follower: %v", err)
				}
			}
		}
	}()

	// Create a goroutine to handle context cancellation and wake up the condition variable
	// This is created ONCE per subscriber, not per callback invocation
	go func() {
		<-ctx.Done()
		// Wake up the condition variable when context is cancelled
		localTopicPartition.ListenersLock.Lock()
		localTopicPartition.ListenersCond.Broadcast()
		localTopicPartition.ListenersLock.Unlock()
	}()

	// Subscribe loop - can be restarted when seek is requested
	currentPosition := startPosition
subscribeLoop:
	for {
		// Context for this iteration of Subscribe (can be cancelled by seek)
		subscribeCtx, subscribeCancel := context.WithCancel(ctx)

		// Start Subscribe in a goroutine so we can interrupt it with seek
		subscribeDone := make(chan error, 1)
		go func() {
			subscribeErr := localTopicPartition.Subscribe(clientName, currentPosition, func() bool {
				// Check cancellation before waiting
				if subscribeCtx.Err() != nil || !isConnected {
					return false
				}

				// Wait for new data using condition variable (blocking, not polling)
				localTopicPartition.ListenersLock.Lock()
				localTopicPartition.ListenersCond.Wait()
				localTopicPartition.ListenersLock.Unlock()

				// After waking up, check if we should stop
				return subscribeCtx.Err() == nil && isConnected
			}, func(logEntry *filer_pb.LogEntry) (bool, error) {
				// Wait for the message to be acknowledged with a timeout to prevent infinite loops
				const maxWaitTime = 30 * time.Second
				const checkInterval = 137 * time.Millisecond
				startTime := time.Now()

				for imt.IsInflight(logEntry.Key) {
					// Check if we've exceeded the maximum wait time
					if time.Since(startTime) > maxWaitTime {
						glog.Warningf("Subscriber %s: message with key %s has been in-flight for more than %v, forcing acknowledgment",
							clientName, string(logEntry.Key), maxWaitTime)
						// Force remove the message from in-flight tracking to prevent infinite loop
						imt.AcknowledgeMessage(logEntry.Key, logEntry.TsNs)
						break
					}

					time.Sleep(checkInterval)

					// Check if the client has disconnected by monitoring the context
					select {
					case <-subscribeCtx.Done():
						err := subscribeCtx.Err()
						if err == context.Canceled {
							// Subscribe cancelled (seek or disconnect)
							return false, nil
						}
						glog.V(0).Infof("Subscriber %s disconnected: %v", clientName, err)
						return false, nil
					default:
						// Continue processing the request
					}
				}
				if logEntry.Key != nil {
					imt.EnflightMessage(logEntry.Key, logEntry.TsNs)
				}

				// Create the message to send
				dataMsg := &mq_pb.DataMessage{
					Key:   logEntry.Key,
					Value: logEntry.Data,
					TsNs:  logEntry.TsNs,
				}

				if err := stream.Send(&mq_pb.SubscribeMessageResponse{Message: &mq_pb.SubscribeMessageResponse_Data{
					Data: dataMsg,
				}}); err != nil {
					glog.Errorf("Error sending data: %v", err)
					return false, err
				}

				// Update received offset and last seen time for this subscriber
				subscriber.UpdateReceivedOffset(logEntry.TsNs)

				counter++
				return false, nil
			})
			subscribeDone <- subscribeErr
		}()

		// Wait for either Subscribe to complete or a seek request
		select {
		case err = <-subscribeDone:
			subscribeCancel()
			if err != nil || ctx.Err() != nil {
				// Subscribe finished with error or main context cancelled - exit loop
				break subscribeLoop
			}
			// Subscribe completed normally (shouldn't happen in streaming mode)
			break subscribeLoop

		case seekMsg := <-seekChan:
			// Seek requested - cancel current Subscribe and restart from new offset
			glog.V(0).Infof("Subscriber %s seeking from offset %d to offset %d (type %v)",
				clientName, currentPosition.GetOffset(), seekMsg.Offset, seekMsg.OffsetType)

			// Cancel current Subscribe iteration
			subscribeCancel()

			// Wait for Subscribe to finish cancelling
			<-subscribeDone

			// Update position for next iteration
			currentPosition = b.getRequestPositionFromSeek(seekMsg)
			glog.V(0).Infof("Subscriber %s restarting Subscribe from new offset %d", clientName, seekMsg.Offset)

			// Send acknowledgment that seek completed
			stream.Send(&mq_pb.SubscribeMessageResponse{Message: &mq_pb.SubscribeMessageResponse_Ctrl{
				Ctrl: &mq_pb.SubscribeMessageResponse_SubscribeCtrlMessage{
					Error: "", // Empty error means success
				},
			}})

			// Loop will restart with new position
		}
	}

	return err
}

func (b *MessageQueueBroker) getRequestPosition(initMessage *mq_pb.SubscribeMessageRequest_InitMessage) (startPosition log_buffer.MessagePosition) {
	if initMessage == nil {
		return
	}
	offset := initMessage.GetPartitionOffset()
	offsetType := initMessage.OffsetType

	// reset to earliest or latest
	if offsetType == schema_pb.OffsetType_RESET_TO_EARLIEST {
		startPosition = log_buffer.NewMessagePosition(1, -3)
		return
	}
	if offsetType == schema_pb.OffsetType_RESET_TO_LATEST {
		startPosition = log_buffer.NewMessagePosition(time.Now().UnixNano(), -4)
		return
	}

	// use the exact timestamp
	if offsetType == schema_pb.OffsetType_EXACT_TS_NS {
		startPosition = log_buffer.NewMessagePosition(offset.StartTsNs, -2)
		return
	}

	// use exact offset (native offset-based positioning)
	if offsetType == schema_pb.OffsetType_EXACT_OFFSET {
		startPosition = log_buffer.NewMessagePositionFromOffset(offset.StartOffset)
		return
	}

	// reset to specific offset
	if offsetType == schema_pb.OffsetType_RESET_TO_OFFSET {
		startPosition = log_buffer.NewMessagePositionFromOffset(offset.StartOffset)
		return
	}

	// try to resume
	if storedOffset, err := b.readConsumerGroupOffset(initMessage); err == nil {
		glog.V(0).Infof("resume from saved offset %v %v %v: %v", initMessage.Topic, initMessage.PartitionOffset.Partition, initMessage.ConsumerGroup, storedOffset)
		startPosition = log_buffer.NewMessagePosition(storedOffset, -2)
		return
	}

	if offsetType == schema_pb.OffsetType_RESUME_OR_EARLIEST {
		startPosition = log_buffer.NewMessagePosition(1, -5)
	} else if offsetType == schema_pb.OffsetType_RESUME_OR_LATEST {
		startPosition = log_buffer.NewMessagePosition(time.Now().UnixNano(), -6)
	}
	return
}

// getRequestPositionFromSeek converts a seek request to a MessagePosition
// This is used when implementing full seek support in Subscribe loop
func (b *MessageQueueBroker) getRequestPositionFromSeek(seekMsg *mq_pb.SubscribeMessageRequest_SeekMessage) (startPosition log_buffer.MessagePosition) {
	if seekMsg == nil {
		return
	}

	offsetType := seekMsg.OffsetType
	offset := seekMsg.Offset

	// reset to earliest or latest
	if offsetType == schema_pb.OffsetType_RESET_TO_EARLIEST {
		startPosition = log_buffer.NewMessagePosition(1, -3)
		return
	}
	if offsetType == schema_pb.OffsetType_RESET_TO_LATEST {
		startPosition = log_buffer.NewMessagePosition(time.Now().UnixNano(), -4)
		return
	}

	// use the exact timestamp
	if offsetType == schema_pb.OffsetType_EXACT_TS_NS {
		startPosition = log_buffer.NewMessagePosition(offset, -2)
		return
	}

	// use exact offset (native offset-based positioning)
	if offsetType == schema_pb.OffsetType_EXACT_OFFSET {
		startPosition = log_buffer.NewMessagePositionFromOffset(offset)
		return
	}

	// reset to specific offset
	if offsetType == schema_pb.OffsetType_RESET_TO_OFFSET {
		startPosition = log_buffer.NewMessagePositionFromOffset(offset)
		return
	}

	// default to exact offset
	startPosition = log_buffer.NewMessagePositionFromOffset(offset)
	return
}
