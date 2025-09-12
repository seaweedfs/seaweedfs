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

	// TODO: Fix partition access - SubscribeMessageRequest_InitMessage may not have Partition field
	// ASSUMPTION: Using a default partition for now
	t := topic.FromPbTopic(initMessage.Topic)
	p := topic.Partition{
		RingSize:   1024,
		RangeStart: 0,
		RangeStop:  31,
		UnixTimeNs: time.Now().UnixNano(),
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
	
	return localPartition.Subscribe(clientName, 
		// Start position - TODO: Convert offset to MessagePosition
		log_buffer.MessagePosition{}, 
		func() bool {
			// Check if subscription is still active and not at end
			if !subscription.IsActive {
				return false
			}
			
			atEnd, err := subscription.IsAtEnd()
			if err != nil {
				glog.V(0).Infof("Error checking if subscription at end: %v", err)
				return false
			}
			
			return !atEnd
		},
		func(logEntry *filer_pb.LogEntry) (bool, error) {
			// Check if this message matches our offset requirements
			currentOffset := subscription.GetNextOffset()
			
			// TODO: Map LogEntry to offset - for now using timestamp as proxy
			// ASSUMPTION: LogEntry.Offset field should be populated by the publish flow
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
		"subscription_id":  subscription.ID,
		"start_offset":     subscription.StartOffset,
		"current_offset":   subscription.CurrentOffset,
		"offset_type":      subscription.OffsetType.String(),
		"is_active":        subscription.IsActive,
		"lag":              lag,
		"at_end":           atEnd,
	}, nil
}

// ListActiveSubscriptions returns information about all active subscriptions
func (b *MessageQueueBroker) ListActiveSubscriptions() ([]map[string]interface{}, error) {
	// TODO: Implement subscription listing
	// ASSUMPTION: This would require extending the offset manager to track all subscriptions
	return []map[string]interface{}{}, nil
}

// SeekSubscription seeks an existing subscription to a specific offset
func (b *MessageQueueBroker) SeekSubscription(subscriptionID string, offset int64) error {
	subscription, err := b.offsetManager.GetSubscription(subscriptionID)
	if err != nil {
		return err
	}
	
	return subscription.SeekToOffset(offset)
}
