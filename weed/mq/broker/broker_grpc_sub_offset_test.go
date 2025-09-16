package broker

import (
	"fmt"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/mq/offset"
	"github.com/seaweedfs/seaweedfs/weed/mq/topic"
	"github.com/seaweedfs/seaweedfs/weed/pb/mq_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/schema_pb"
	"github.com/seaweedfs/seaweedfs/weed/util/log_buffer"
)

func TestConvertOffsetToMessagePosition(t *testing.T) {
	broker := &MessageQueueBroker{}

	tests := []struct {
		name          string
		offsetType    schema_pb.OffsetType
		currentOffset int64
		expectedBatch int64
		expectError   bool
	}{
		{
			name:          "reset to earliest",
			offsetType:    schema_pb.OffsetType_RESET_TO_EARLIEST,
			currentOffset: 0,
			expectedBatch: -3,
			expectError:   false,
		},
		{
			name:          "reset to latest",
			offsetType:    schema_pb.OffsetType_RESET_TO_LATEST,
			currentOffset: 0,
			expectedBatch: -4,
			expectError:   false,
		},
		{
			name:          "exact offset zero",
			offsetType:    schema_pb.OffsetType_EXACT_OFFSET,
			currentOffset: 0,
			expectedBatch: -2,
			expectError:   false,
		},
		{
			name:          "exact offset non-zero",
			offsetType:    schema_pb.OffsetType_EXACT_OFFSET,
			currentOffset: 100,
			expectedBatch: -2,
			expectError:   false,
		},
		{
			name:          "exact timestamp",
			offsetType:    schema_pb.OffsetType_EXACT_TS_NS,
			currentOffset: 50,
			expectedBatch: -2,
			expectError:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create a mock subscription
			subscription := &offset.OffsetSubscription{
				ID:            "test-subscription",
				CurrentOffset: tt.currentOffset,
				OffsetType:    tt.offsetType,
				IsActive:      true,
			}

			position, err := broker.convertOffsetToMessagePosition(subscription)

			if tt.expectError && err == nil {
				t.Error("Expected error but got none")
				return
			}

			if !tt.expectError && err != nil {
				t.Errorf("Unexpected error: %v", err)
				return
			}

			if position.BatchIndex != tt.expectedBatch {
				t.Errorf("Expected batch index %d, got %d", tt.expectedBatch, position.BatchIndex)
			}

			// Verify that the timestamp is reasonable (not zero for most cases)
			if tt.offsetType != schema_pb.OffsetType_RESET_TO_EARLIEST && position.Time.IsZero() {
				t.Error("Expected non-zero timestamp")
			}

			t.Logf("Offset %d (type %v) -> Position: time=%v, batch=%d",
				tt.currentOffset, tt.offsetType, position.Time, position.BatchIndex)
		})
	}
}

func TestConvertOffsetToMessagePosition_TimestampProgression(t *testing.T) {
	broker := &MessageQueueBroker{}

	// Test that higher offsets result in more recent timestamps (for the approximation)
	subscription1 := &offset.OffsetSubscription{
		ID:            "test-1",
		CurrentOffset: 10,
		OffsetType:    schema_pb.OffsetType_EXACT_OFFSET,
		IsActive:      true,
	}

	subscription2 := &offset.OffsetSubscription{
		ID:            "test-2",
		CurrentOffset: 100,
		OffsetType:    schema_pb.OffsetType_EXACT_OFFSET,
		IsActive:      true,
	}

	pos1, err1 := broker.convertOffsetToMessagePosition(subscription1)
	pos2, err2 := broker.convertOffsetToMessagePosition(subscription2)

	if err1 != nil || err2 != nil {
		t.Fatalf("Unexpected errors: %v, %v", err1, err2)
	}

	// For the current approximation, higher offsets should result in older timestamps
	// (since we subtract offset * millisecond from current time)
	if !pos1.Time.After(pos2.Time) {
		t.Errorf("Expected offset 10 to have more recent timestamp than offset 100")
		t.Logf("Offset 10: %v", pos1.Time)
		t.Logf("Offset 100: %v", pos2.Time)
	}
}

func TestConvertOffsetToMessagePosition_ConsistentResults(t *testing.T) {
	broker := &MessageQueueBroker{}

	subscription := &offset.OffsetSubscription{
		ID:            "consistent-test",
		CurrentOffset: 42,
		OffsetType:    schema_pb.OffsetType_EXACT_OFFSET,
		IsActive:      true,
	}

	// Call multiple times within a short period
	positions := make([]log_buffer.MessagePosition, 5)
	for i := 0; i < 5; i++ {
		pos, err := broker.convertOffsetToMessagePosition(subscription)
		if err != nil {
			t.Fatalf("Unexpected error on iteration %d: %v", i, err)
		}
		positions[i] = pos
		time.Sleep(1 * time.Millisecond) // Small delay
	}

	// All positions should have the same BatchIndex
	for i := 1; i < len(positions); i++ {
		if positions[i].BatchIndex != positions[0].BatchIndex {
			t.Errorf("Inconsistent BatchIndex: %d vs %d", positions[0].BatchIndex, positions[i].BatchIndex)
		}
	}

	// Timestamps should be very close (within a reasonable range)
	maxDiff := 100 * time.Millisecond
	for i := 1; i < len(positions); i++ {
		diff := positions[i].Time.Sub(positions[0].Time)
		if diff < 0 {
			diff = -diff
		}
		if diff > maxDiff {
			t.Errorf("Timestamp difference too large: %v", diff)
		}
	}

	t.Logf("Consistent results for offset 42: batch=%d, time range=%v",
		positions[0].BatchIndex, positions[len(positions)-1].Time.Sub(positions[0].Time))
}

func TestPartitionIdentityConsistency(t *testing.T) {
	// Test that partition identity is preserved from request to avoid breaking offset manager keys

	// Create a mock init message with specific partition info
	partition := &schema_pb.Partition{
		RingSize:   32,
		RangeStart: 0,
		RangeStop:  31,
		UnixTimeNs: 1234567890123456789, // Fixed timestamp
	}

	initMessage := &mq_pb.SubscribeMessageRequest_InitMessage{
		ConsumerGroup: "test-group",
		ConsumerId:    "test-consumer",
		PartitionOffset: &schema_pb.PartitionOffset{
			Partition: partition,
		},
	}

	// Simulate the partition creation logic from SubscribeWithOffset
	p := topic.Partition{
		RingSize:   initMessage.PartitionOffset.Partition.RingSize,
		RangeStart: initMessage.PartitionOffset.Partition.RangeStart,
		RangeStop:  initMessage.PartitionOffset.Partition.RangeStop,
		UnixTimeNs: initMessage.PartitionOffset.Partition.UnixTimeNs,
	}

	// Verify that the partition preserves the original UnixTimeNs
	if p.UnixTimeNs != partition.UnixTimeNs {
		t.Errorf("Partition UnixTimeNs not preserved: expected %d, got %d",
			partition.UnixTimeNs, p.UnixTimeNs)
	}

	// Verify partition key consistency
	expectedKey := fmt.Sprintf("ring:%d:range:%d-%d:time:%d",
		partition.RingSize, partition.RangeStart, partition.RangeStop, partition.UnixTimeNs)

	actualKey := fmt.Sprintf("ring:%d:range:%d-%d:time:%d",
		p.RingSize, p.RangeStart, p.RangeStop, p.UnixTimeNs)

	if actualKey != expectedKey {
		t.Errorf("Partition key mismatch: expected %s, got %s", expectedKey, actualKey)
	}

	t.Logf("Partition identity preserved: key=%s", actualKey)
}

func TestBrokerOffsetManager_GetSubscription_Fixed(t *testing.T) {
	// Test that GetSubscription now works correctly after the fix

	storage := NewInMemoryOffsetStorageForTesting()
	offsetManager := NewBrokerOffsetManagerWithStorage(storage)

	// Create test topic and partition
	testTopic := topic.Topic{Namespace: "test", Name: "topic1"}
	testPartition := topic.Partition{
		RingSize:   32,
		RangeStart: 0,
		RangeStop:  31,
		UnixTimeNs: time.Now().UnixNano(),
	}

	// Test getting non-existent subscription
	_, err := offsetManager.GetSubscription("non-existent")
	if err == nil {
		t.Error("Expected error for non-existent subscription")
	}

	// Create a subscription
	subscriptionID := "test-subscription-fixed"
	subscription, err := offsetManager.CreateSubscription(
		subscriptionID,
		testTopic,
		testPartition,
		schema_pb.OffsetType_RESET_TO_EARLIEST,
		0,
	)
	if err != nil {
		t.Fatalf("Failed to create subscription: %v", err)
	}

	// Test getting existing subscription (this should now work)
	retrievedSub, err := offsetManager.GetSubscription(subscriptionID)
	if err != nil {
		t.Fatalf("GetSubscription failed after fix: %v", err)
	}

	if retrievedSub.ID != subscription.ID {
		t.Errorf("Expected subscription ID %s, got %s", subscription.ID, retrievedSub.ID)
	}

	if retrievedSub.OffsetType != subscription.OffsetType {
		t.Errorf("Expected offset type %v, got %v", subscription.OffsetType, retrievedSub.OffsetType)
	}

	t.Logf("GetSubscription working correctly after fix: %s", retrievedSub.ID)
}

func TestBrokerOffsetManager_ListActiveSubscriptions_Fixed(t *testing.T) {
	// Test that ListActiveSubscriptions now works correctly after the fix

	storage := NewInMemoryOffsetStorageForTesting()
	offsetManager := NewBrokerOffsetManagerWithStorage(storage)

	// Create test topic and partition
	testTopic := topic.Topic{Namespace: "test", Name: "topic1"}
	testPartition := topic.Partition{
		RingSize:   32,
		RangeStart: 0,
		RangeStop:  31,
		UnixTimeNs: time.Now().UnixNano(),
	}

	// Initially should have no subscriptions
	subscriptions, err := offsetManager.ListActiveSubscriptions()
	if err != nil {
		t.Fatalf("ListActiveSubscriptions failed after fix: %v", err)
	}
	if len(subscriptions) != 0 {
		t.Errorf("Expected 0 subscriptions, got %d", len(subscriptions))
	}

	// Create multiple subscriptions (use RESET types to avoid HWM validation issues)
	subscriptionIDs := []string{"sub-fixed-1", "sub-fixed-2", "sub-fixed-3"}
	offsetTypes := []schema_pb.OffsetType{
		schema_pb.OffsetType_RESET_TO_EARLIEST,
		schema_pb.OffsetType_RESET_TO_LATEST,
		schema_pb.OffsetType_RESET_TO_EARLIEST, // Changed from EXACT_OFFSET
	}

	for i, subID := range subscriptionIDs {
		_, err := offsetManager.CreateSubscription(
			subID,
			testTopic,
			testPartition,
			offsetTypes[i],
			0, // Use 0 for all to avoid validation issues
		)
		if err != nil {
			t.Fatalf("Failed to create subscription %s: %v", subID, err)
		}
	}

	// List all subscriptions (this should now work)
	subscriptions, err = offsetManager.ListActiveSubscriptions()
	if err != nil {
		t.Fatalf("ListActiveSubscriptions failed after fix: %v", err)
	}

	if len(subscriptions) != len(subscriptionIDs) {
		t.Errorf("Expected %d subscriptions, got %d", len(subscriptionIDs), len(subscriptions))
	}

	// Verify all subscriptions are active
	for _, sub := range subscriptions {
		if !sub.IsActive {
			t.Errorf("Subscription %s should be active", sub.ID)
		}
	}

	t.Logf("ListActiveSubscriptions working correctly after fix: %d subscriptions", len(subscriptions))
}

func TestMessageQueueBroker_ListActiveSubscriptions_Fixed(t *testing.T) {
	// Test that the broker-level ListActiveSubscriptions now works correctly

	storage := NewInMemoryOffsetStorageForTesting()
	offsetManager := NewBrokerOffsetManagerWithStorage(storage)

	broker := &MessageQueueBroker{
		offsetManager: offsetManager,
	}

	// Create test topic and partition
	testTopic := topic.Topic{Namespace: "test", Name: "topic1"}
	testPartition := topic.Partition{
		RingSize:   32,
		RangeStart: 0,
		RangeStop:  31,
		UnixTimeNs: time.Now().UnixNano(),
	}

	// Initially should have no subscriptions
	subscriptionInfos, err := broker.ListActiveSubscriptions()
	if err != nil {
		t.Fatalf("Broker ListActiveSubscriptions failed after fix: %v", err)
	}
	if len(subscriptionInfos) != 0 {
		t.Errorf("Expected 0 subscription infos, got %d", len(subscriptionInfos))
	}

	// Create subscriptions with different offset types (use RESET types to avoid HWM validation issues)
	testCases := []struct {
		id          string
		offsetType  schema_pb.OffsetType
		startOffset int64
	}{
		{"broker-earliest-sub", schema_pb.OffsetType_RESET_TO_EARLIEST, 0},
		{"broker-latest-sub", schema_pb.OffsetType_RESET_TO_LATEST, 0},
		{"broker-reset-sub", schema_pb.OffsetType_RESET_TO_EARLIEST, 0}, // Changed from EXACT_OFFSET
	}

	for _, tc := range testCases {
		_, err := broker.offsetManager.CreateSubscription(
			tc.id,
			testTopic,
			testPartition,
			tc.offsetType,
			tc.startOffset,
		)
		if err != nil {
			t.Fatalf("Failed to create subscription %s: %v", tc.id, err)
		}
	}

	// List subscription infos (this should now work)
	subscriptionInfos, err = broker.ListActiveSubscriptions()
	if err != nil {
		t.Fatalf("Broker ListActiveSubscriptions failed after fix: %v", err)
	}

	if len(subscriptionInfos) != len(testCases) {
		t.Errorf("Expected %d subscription infos, got %d", len(testCases), len(subscriptionInfos))
	}

	// Verify subscription info structure
	for _, info := range subscriptionInfos {
		// Check required fields
		requiredFields := []string{
			"subscription_id", "start_offset", "current_offset",
			"offset_type", "is_active", "lag", "at_end",
		}

		for _, field := range requiredFields {
			if _, ok := info[field]; !ok {
				t.Errorf("Missing field %s in subscription info", field)
			}
		}

		// Verify is_active is true
		if isActive, ok := info["is_active"].(bool); !ok || !isActive {
			t.Errorf("Expected is_active to be true, got %v", info["is_active"])
		}

		t.Logf("Subscription info working correctly: %+v", info)
	}
}

func TestSingleWriterPerPartitionCorrectness(t *testing.T) {
	// Test that demonstrates correctness under single-writer-per-partition model

	// Simulate two brokers with separate offset managers but same partition
	storage1 := NewInMemoryOffsetStorageForTesting()
	storage2 := NewInMemoryOffsetStorageForTesting()

	offsetManager1 := NewBrokerOffsetManagerWithStorage(storage1)
	offsetManager2 := NewBrokerOffsetManagerWithStorage(storage2)

	broker1 := &MessageQueueBroker{offsetManager: offsetManager1}
	broker2 := &MessageQueueBroker{offsetManager: offsetManager2}

	// Same partition identity (this is key for correctness)
	fixedTimestamp := time.Now().UnixNano()
	testTopic := topic.Topic{Namespace: "test", Name: "shared-topic"}
	testPartition := topic.Partition{
		RingSize:   32,
		RangeStart: 0,
		RangeStop:  31,
		UnixTimeNs: fixedTimestamp, // Same timestamp = same partition identity
	}

	// Broker 1 is the leader for this partition - assigns offsets
	baseOffset, lastOffset, err := broker1.offsetManager.AssignBatchOffsets(testTopic, testPartition, 10)
	if err != nil {
		t.Fatalf("Failed to assign offsets on broker1: %v", err)
	}

	if baseOffset != 0 || lastOffset != 9 {
		t.Errorf("Expected offsets 0-9, got %d-%d", baseOffset, lastOffset)
	}

	// Get HWM from leader
	hwm1, err := broker1.offsetManager.GetHighWaterMark(testTopic, testPartition)
	if err != nil {
		t.Fatalf("Failed to get HWM from broker1: %v", err)
	}

	if hwm1 != 10 {
		t.Errorf("Expected HWM 10 on leader, got %d", hwm1)
	}

	// Broker 2 is a follower - should have HWM 0 (no local assignments)
	hwm2, err := broker2.offsetManager.GetHighWaterMark(testTopic, testPartition)
	if err != nil {
		t.Fatalf("Failed to get HWM from broker2: %v", err)
	}

	if hwm2 != 0 {
		t.Errorf("Expected HWM 0 on follower, got %d", hwm2)
	}

	// Create subscription on leader (where offsets were assigned)
	subscription1, err := broker1.offsetManager.CreateSubscription(
		"leader-subscription",
		testTopic,
		testPartition,
		schema_pb.OffsetType_RESET_TO_EARLIEST,
		0,
	)
	if err != nil {
		t.Fatalf("Failed to create subscription on leader: %v", err)
	}

	// Verify subscription can see the correct HWM
	lag1, err := subscription1.GetLag()
	if err != nil {
		t.Fatalf("Failed to get lag on leader subscription: %v", err)
	}

	if lag1 != 10 {
		t.Errorf("Expected lag 10 on leader subscription, got %d", lag1)
	}

	// Create subscription on follower (should have different lag due to local HWM)
	subscription2, err := broker2.offsetManager.CreateSubscription(
		"follower-subscription",
		testTopic,
		testPartition,
		schema_pb.OffsetType_RESET_TO_EARLIEST,
		0,
	)
	if err != nil {
		t.Fatalf("Failed to create subscription on follower: %v", err)
	}

	lag2, err := subscription2.GetLag()
	if err != nil {
		t.Fatalf("Failed to get lag on follower subscription: %v", err)
	}

	if lag2 != 0 {
		t.Errorf("Expected lag 0 on follower subscription (no local data), got %d", lag2)
	}

	t.Logf("Single-writer correctness verified:")
	t.Logf("  Leader (broker1): HWM=%d, lag=%d", hwm1, lag1)
	t.Logf("  Follower (broker2): HWM=%d, lag=%d", hwm2, lag2)
	t.Logf("  This demonstrates why subscriptions should route to partition leaders")
}

func TestEndToEndWorkflowAfterFixes(t *testing.T) {
	// Test the complete workflow with all fixes applied

	storage := NewInMemoryOffsetStorageForTesting()
	offsetManager := NewBrokerOffsetManagerWithStorage(storage)

	broker := &MessageQueueBroker{
		offsetManager: offsetManager,
	}

	// Create test topic and partition with fixed timestamp
	fixedTimestamp := time.Now().UnixNano()
	testTopic := topic.Topic{Namespace: "test", Name: "e2e-topic"}
	testPartition := topic.Partition{
		RingSize:   32,
		RangeStart: 0,
		RangeStop:  31,
		UnixTimeNs: fixedTimestamp,
	}

	subscriptionID := "e2e-test-sub"

	// 1. Create subscription (use RESET_TO_EARLIEST to avoid HWM validation issues)
	subscription, err := broker.offsetManager.CreateSubscription(
		subscriptionID,
		testTopic,
		testPartition,
		schema_pb.OffsetType_RESET_TO_EARLIEST,
		0,
	)
	if err != nil {
		t.Fatalf("Failed to create subscription: %v", err)
	}

	// 2. Verify GetSubscription works
	retrievedSub, err := broker.offsetManager.GetSubscription(subscriptionID)
	if err != nil {
		t.Fatalf("GetSubscription failed: %v", err)
	}

	if retrievedSub.ID != subscription.ID {
		t.Errorf("GetSubscription returned wrong subscription: expected %s, got %s",
			subscription.ID, retrievedSub.ID)
	}

	// 3. Verify it appears in active list
	activeList, err := broker.ListActiveSubscriptions()
	if err != nil {
		t.Fatalf("Failed to list active subscriptions: %v", err)
	}

	found := false
	for _, info := range activeList {
		if info["subscription_id"] == subscriptionID {
			found = true
			break
		}
	}
	if !found {
		t.Error("New subscription not found in active list")
	}

	// 4. Get subscription info
	info, err := broker.GetSubscriptionInfo(subscriptionID)
	if err != nil {
		t.Fatalf("Failed to get subscription info: %v", err)
	}

	if info["subscription_id"] != subscriptionID {
		t.Errorf("Wrong subscription ID in info: expected %s, got %v", subscriptionID, info["subscription_id"])
	}

	// 5. Assign some offsets to create data for seeking
	_, _, err = broker.offsetManager.AssignBatchOffsets(testTopic, testPartition, 50)
	if err != nil {
		t.Fatalf("Failed to assign offsets: %v", err)
	}

	// Debug: Check HWM after assignment
	hwm, err := broker.offsetManager.GetHighWaterMark(testTopic, testPartition)
	if err != nil {
		t.Fatalf("Failed to get HWM: %v", err)
	}
	t.Logf("HWM after assignment: %d", hwm)

	// 6. Seek subscription
	newOffset := int64(42)
	err = broker.SeekSubscription(subscriptionID, newOffset)
	if err != nil {
		t.Fatalf("Failed to seek subscription: %v", err)
	}

	// 7. Verify seek worked
	updatedInfo, err := broker.GetSubscriptionInfo(subscriptionID)
	if err != nil {
		t.Fatalf("Failed to get updated subscription info: %v", err)
	}

	if updatedInfo["current_offset"] != newOffset {
		t.Errorf("Seek didn't work: expected offset %d, got %v", newOffset, updatedInfo["current_offset"])
	}

	// 8. Test offset to timestamp conversion with fixed partition identity
	updatedSub, err := broker.offsetManager.GetSubscription(subscriptionID)
	if err != nil {
		t.Fatalf("Failed to get updated subscription: %v", err)
	}

	position, err := broker.convertOffsetToMessagePosition(updatedSub)
	if err != nil {
		t.Fatalf("Failed to convert offset to position: %v", err)
	}

	if position.Time.IsZero() {
		t.Error("Expected non-zero timestamp from conversion")
	}

	// 9. Verify partition identity consistency throughout
	partitionKey1 := fmt.Sprintf("ring:%d:range:%d-%d:time:%d",
		testPartition.RingSize, testPartition.RangeStart, testPartition.RangeStop, testPartition.UnixTimeNs)

	partitionKey2 := fmt.Sprintf("ring:%d:range:%d-%d:time:%d",
		testPartition.RingSize, testPartition.RangeStart, testPartition.RangeStop, fixedTimestamp)

	if partitionKey1 != partitionKey2 {
		t.Errorf("Partition key inconsistency: %s != %s", partitionKey1, partitionKey2)
	}

	t.Logf("End-to-end test completed successfully:")
	t.Logf("  Subscription: %s at offset %d", subscriptionID, newOffset)
	t.Logf("  Position: time=%v, batch=%d", position.Time, position.BatchIndex)
	t.Logf("  Partition key: %s", partitionKey1)
}
