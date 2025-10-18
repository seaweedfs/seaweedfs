package broker

import (
	"context"
	"fmt"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/mq/offset"
	"github.com/seaweedfs/seaweedfs/weed/mq/topic"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/mq_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/schema_pb"
	"github.com/seaweedfs/seaweedfs/weed/util/log_buffer"
)

// SubscribeWithOffset handles subscription requests with offset-based positioning
// TODO: This extends the broker with offset-aware subscription support
// ASSUMPTION: This will eventually be integrated into the main SubscribeMessage method
func (b *MessageQueueBroker) SubscribeWithOffset(
	ctx context.Context,
	req *mq_pb.SubscribeMessageRequest,
	stream mq_pb.SeaweedMessaging_SubscribeMessageServer,
	offsetType schema_pb.OffsetType,
	startOffset int64,
) error {

	initMessage := req.GetInit()
	if initMessage == nil {
		return fmt.Errorf("missing init message")
	}

	// Extract partition information from the request
	t := topic.FromPbTopic(initMessage.Topic)

	// Get partition from the request's partition_offset field
	if initMessage.PartitionOffset == nil || initMessage.PartitionOffset.Partition == nil {
		return fmt.Errorf("missing partition information in request")
	}

	// Use the partition information from the request
	p := topic.Partition{
		RingSize:   initMessage.PartitionOffset.Partition.RingSize,
		RangeStart: initMessage.PartitionOffset.Partition.RangeStart,
		RangeStop:  initMessage.PartitionOffset.Partition.RangeStop,
		UnixTimeNs: initMessage.PartitionOffset.Partition.UnixTimeNs,
	}

	// Create offset-based subscription
	subscriptionID := fmt.Sprintf("%s-%s-%d", initMessage.ConsumerGroup, initMessage.ConsumerId, startOffset)
	subscription, err := b.offsetManager.CreateSubscription(subscriptionID, t, p, offsetType, startOffset)
	if err != nil {
		return fmt.Errorf("failed to create offset subscription: %w", err)
	}

	defer func() {
		if closeErr := b.offsetManager.CloseSubscription(subscriptionID); closeErr != nil {
			glog.V(0).Infof("Failed to close subscription %s: %v", subscriptionID, closeErr)
		}
	}()

	// Get local partition for reading
	localTopicPartition, err := b.GetOrGenerateLocalPartition(t, p)
	if err != nil {
		return fmt.Errorf("topic %v partition %v not found: %v", t, p, err)
	}

	// Subscribe to messages using offset-based positioning
	return b.subscribeWithOffsetSubscription(ctx, localTopicPartition, subscription, stream, initMessage)
}

// subscribeWithOffsetSubscription handles the actual message consumption with offset tracking
func (b *MessageQueueBroker) subscribeWithOffsetSubscription(
	ctx context.Context,
	localPartition *topic.LocalPartition,
	subscription *offset.OffsetSubscription,
	stream mq_pb.SeaweedMessaging_SubscribeMessageServer,
	initMessage *mq_pb.SubscribeMessageRequest_InitMessage,
) error {

	clientName := fmt.Sprintf("%s-%s", initMessage.ConsumerGroup, initMessage.ConsumerId)

	// TODO: Implement offset-based message reading
	// ASSUMPTION: For now, we'll use the existing subscription mechanism and track offsets separately
	// This should be replaced with proper offset-based reading from storage

	// Convert the subscription's current offset to a proper MessagePosition
	startPosition, err := b.convertOffsetToMessagePosition(subscription)
	if err != nil {
		return fmt.Errorf("failed to convert offset to message position: %w", err)
	}

	glog.V(0).Infof("[%s] Starting Subscribe for topic %s partition %d-%d at offset %d",
		clientName, subscription.TopicName, subscription.Partition.RangeStart, subscription.Partition.RangeStop, subscription.CurrentOffset)

	return localPartition.Subscribe(clientName,
		startPosition,
		func() bool {
			// Check if context is cancelled (client disconnected)
			select {
			case <-ctx.Done():
				glog.V(0).Infof("[%s] Context cancelled, stopping", clientName)
				return false
			default:
			}

			// Check if subscription is still active and not at end
			if !subscription.IsActive {
				glog.V(0).Infof("[%s] Subscription not active, stopping", clientName)
				return false
			}

			atEnd, err := subscription.IsAtEnd()
			if err != nil {
				glog.V(0).Infof("[%s] Error checking if subscription at end: %v", clientName, err)
				return false
			}

			if atEnd {
				glog.V(4).Infof("[%s] At end of subscription, stopping", clientName)
				return false
			}

			// Add a small sleep to avoid CPU busy-wait when checking for new data
			time.Sleep(10 * time.Millisecond)
			return true
		},
		func(logEntry *filer_pb.LogEntry) (bool, error) {
			// Check if this message matches our offset requirements
			currentOffset := subscription.GetNextOffset()

			if logEntry.Offset < currentOffset {
				// Skip messages before our current offset
				return false, nil
			}

			// Send message to client
			if err := stream.Send(&mq_pb.SubscribeMessageResponse{
				Message: &mq_pb.SubscribeMessageResponse_Data{
					Data: &mq_pb.DataMessage{
						Key:   logEntry.Key,
						Value: logEntry.Data,
						TsNs:  logEntry.TsNs,
					},
				},
			}); err != nil {
				glog.Errorf("Error sending data to %s: %v", clientName, err)
				return false, err
			}

			// Advance subscription offset
			subscription.AdvanceOffset()

			// Check context for cancellation
			select {
			case <-ctx.Done():
				return true, ctx.Err()
			default:
				return false, nil
			}
		})
}

// GetSubscriptionInfo returns information about an active subscription
func (b *MessageQueueBroker) GetSubscriptionInfo(subscriptionID string) (map[string]interface{}, error) {
	subscription, err := b.offsetManager.GetSubscription(subscriptionID)
	if err != nil {
		return nil, err
	}

	lag, err := subscription.GetLag()
	if err != nil {
		return nil, err
	}

	atEnd, err := subscription.IsAtEnd()
	if err != nil {
		return nil, err
	}

	return map[string]interface{}{
		"subscription_id": subscription.ID,
		"start_offset":    subscription.StartOffset,
		"current_offset":  subscription.CurrentOffset,
		"offset_type":     subscription.OffsetType.String(),
		"is_active":       subscription.IsActive,
		"lag":             lag,
		"at_end":          atEnd,
	}, nil
}

// ListActiveSubscriptions returns information about all active subscriptions
func (b *MessageQueueBroker) ListActiveSubscriptions() ([]map[string]interface{}, error) {
	subscriptions, err := b.offsetManager.ListActiveSubscriptions()
	if err != nil {
		return nil, err
	}

	result := make([]map[string]interface{}, len(subscriptions))
	for i, subscription := range subscriptions {
		lag, _ := subscription.GetLag()
		atEnd, _ := subscription.IsAtEnd()

		result[i] = map[string]interface{}{
			"subscription_id": subscription.ID,
			"start_offset":    subscription.StartOffset,
			"current_offset":  subscription.CurrentOffset,
			"offset_type":     subscription.OffsetType.String(),
			"is_active":       subscription.IsActive,
			"lag":             lag,
			"at_end":          atEnd,
		}
	}

	return result, nil
}

// SeekSubscription seeks an existing subscription to a specific offset
func (b *MessageQueueBroker) SeekSubscription(subscriptionID string, offset int64) error {
	subscription, err := b.offsetManager.GetSubscription(subscriptionID)
	if err != nil {
		return err
	}

	return subscription.SeekToOffset(offset)
}

// convertOffsetToMessagePosition converts a subscription's current offset to a MessagePosition for log_buffer
func (b *MessageQueueBroker) convertOffsetToMessagePosition(subscription *offset.OffsetSubscription) (log_buffer.MessagePosition, error) {
	currentOffset := subscription.GetNextOffset()

	// Handle special offset cases
	switch subscription.OffsetType {
	case schema_pb.OffsetType_RESET_TO_EARLIEST:
		return log_buffer.NewMessagePosition(1, -3), nil

	case schema_pb.OffsetType_RESET_TO_LATEST:
		return log_buffer.NewMessagePosition(time.Now().UnixNano(), -4), nil

	case schema_pb.OffsetType_EXACT_OFFSET:
		// Use proper offset-based positioning that provides consistent results
		// This uses the same approach as the main subscription handler in broker_grpc_sub.go
		return log_buffer.NewMessagePositionFromOffset(currentOffset), nil

	case schema_pb.OffsetType_EXACT_TS_NS:
		// For exact timestamps, use the timestamp directly
		return log_buffer.NewMessagePosition(currentOffset, -2), nil

	default:
		// Default to starting from current time for unknown offset types
		return log_buffer.NewMessagePosition(time.Now().UnixNano(), -2), nil
	}
}
