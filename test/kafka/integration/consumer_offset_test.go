package integration

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/mq/kafka/integration"
	"github.com/seaweedfs/seaweedfs/weed/pb/mq_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/schema_pb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestConsumerOffsetCommitAndFetch tests the basic offset commit and fetch operations
func TestConsumerOffsetCommitAndFetch(t *testing.T) {
	ctx := context.Background()
	handler := integration.NewTestSeaweedMQHandler(t)
	defer handler.Cleanup()

	topic := "test.offset-test"
	partition := int32(0)
	consumerGroup := "test-consumer-group"

	// Produce some messages
	for i := 0; i < 10; i++ {
		record := &schema_pb.RecordValue{
			Fields: []*schema_pb.Field{
				{Name: "message", Value: &schema_pb.Value{Kind: &schema_pb.Value_StringValue{StringValue: fmt.Sprintf("message-%d", i)}}},
			},
		}
		err := handler.ProduceMessage(ctx, topic, fmt.Sprintf("%d", partition), record, nil)
		require.NoError(t, err, "Failed to produce message %d", i)
	}

	// Commit offset 5 for the consumer group
	commitOffset := int64(5)
	err := handler.CommitOffset(ctx, consumerGroup, topic, partition, commitOffset, "test-metadata")
	require.NoError(t, err, "Failed to commit offset")

	// Fetch the committed offset
	fetchedOffset, metadata, err := handler.FetchOffset(ctx, consumerGroup, topic, partition)
	require.NoError(t, err, "Failed to fetch offset")
	assert.Equal(t, commitOffset, fetchedOffset, "Fetched offset should match committed offset")
	assert.Equal(t, "test-metadata", metadata, "Metadata should match")
}

// TestConsumerGroupPersistence tests that consumer group state persists across handler restarts
func TestConsumerGroupPersistence(t *testing.T) {
	ctx := context.Background()
	handler1 := integration.NewTestSeaweedMQHandler(t)

	topic := mq_pb.Topic{Namespace: "test", Name: "persistence-test"}
	partition := int32(0)
	consumerGroup := "persistent-group"

	// Produce messages
	for i := 0; i < 20; i++ {
		record := &schema_pb.RecordValue{
			Fields: []*schema_pb.Field{
				{Name: "message", Value: &schema_pb.Value{Kind: &schema_pb.Value_StringValue{StringValue: fmt.Sprintf("msg-%d", i)}}},
			},
		}
		err := handler1.ProduceMessage(ctx, topic, partition, record, nil)
		require.NoError(t, err)
	}

	// Commit offset with first handler
	commitOffset := int64(15)
	err := handler1.CommitOffset(ctx, consumerGroup, topic, partition, commitOffset, "persistent-metadata")
	require.NoError(t, err)

	// Cleanup first handler (simulates restart)
	handler1.Cleanup()

	// Create new handler (simulates restart)
	handler2 := integration.NewTestSeaweedMQHandler(t)
	defer handler2.Cleanup()

	// Fetch offset with second handler
	fetchedOffset, metadata, err := handler2.FetchOffset(ctx, consumerGroup, topic, partition)
	require.NoError(t, err, "Failed to fetch offset after restart")
	assert.Equal(t, commitOffset, fetchedOffset, "Offset should persist across restarts")
	assert.Equal(t, "persistent-metadata", metadata, "Metadata should persist across restarts")
}

// TestFetchFromOffset tests that fetch returns messages from the correct offset
func TestFetchFromOffset(t *testing.T) {
	ctx := context.Background()
	handler := integration.NewTestSeaweedMQHandler(t)
	defer handler.Cleanup()

	topic := mq_pb.Topic{Namespace: "test", Name: "fetch-offset-test"}
	partition := int32(0)

	// Produce 10 messages
	for i := 0; i < 10; i++ {
		record := &schema_pb.RecordValue{
			Fields: []*schema_pb.Field{
				{Name: "index", Value: &schema_pb.Value{Kind: &schema_pb.Value_Int64Value{Int64Value: int64(i)}}},
			},
		}
		err := handler.ProduceMessage(ctx, topic, partition, record, nil)
		require.NoError(t, err)
	}

	// Fetch from offset 5
	fetchOffset := int64(5)
	messages, err := handler.FetchMessages(ctx, topic, partition, fetchOffset, 100)
	require.NoError(t, err, "Failed to fetch messages")

	// Should get messages 5-9 (5 messages total)
	assert.Equal(t, 5, len(messages), "Should fetch 5 messages from offset 5")

	// Verify first message is from offset 5
	if len(messages) > 0 {
		firstMessage := messages[0]
		assert.GreaterOrEqual(t, firstMessage.Offset, fetchOffset, "First message offset should be >= fetch offset")
	}
}

// TestOffsetReset tests AUTO_OFFSET_RESET_CONFIG behavior
func TestOffsetReset(t *testing.T) {
	ctx := context.Background()
	handler := integration.NewTestSeaweedMQHandler(t)
	defer handler.Cleanup()

	topic := mq_pb.Topic{Namespace: "test", Name: "offset-reset-test"}
	partition := int32(0)
	consumerGroup := "reset-test-group"

	// Produce 10 messages
	for i := 0; i < 10; i++ {
		record := &schema_pb.RecordValue{
			Fields: []*schema_pb.Field{
				{Name: "message", Value: &schema_pb.Value{Kind: &schema_pb.Value_StringValue{StringValue: fmt.Sprintf("msg-%d", i)}}},
			},
		}
		err := handler.ProduceMessage(ctx, topic, partition, record, nil)
		require.NoError(t, err)
	}

	// Try to fetch offset for non-existent consumer group
	// Should return -1 (no offset committed)
	fetchedOffset, _, err := handler.FetchOffset(ctx, consumerGroup, topic, partition)

	// Different Kafka versions handle this differently
	// Some return error, some return -1
	if err == nil {
		assert.Equal(t, int64(-1), fetchedOffset, "Should return -1 for non-existent offset")
	} else {
		// Error is acceptable for non-existent consumer group
		t.Logf("Fetch returned error for non-existent group: %v", err)
	}

	// Commit an offset
	err = handler.CommitOffset(ctx, consumerGroup, topic, partition, 5, "")
	require.NoError(t, err)

	// Now fetch should return the committed offset
	fetchedOffset, _, err = handler.FetchOffset(ctx, consumerGroup, topic, partition)
	require.NoError(t, err)
	assert.Equal(t, int64(5), fetchedOffset)
}

// TestMultiplePartitionOffsets tests offset management for multiple partitions
func TestMultiplePartitionOffsets(t *testing.T) {
	ctx := context.Background()
	handler := integration.NewTestSeaweedMQHandler(t)
	defer handler.Cleanup()

	topic := mq_pb.Topic{Namespace: "test", Name: "multi-partition-test"}
	partition1 := int32(0)
	partition2 := int32(0)
	consumerGroup := "multi-partition-group"

	// Commit different offsets for different partitions
	err := handler.CommitOffset(ctx, consumerGroup, topic, partition1, 10, "partition1-metadata")
	require.NoError(t, err)

	err = handler.CommitOffset(ctx, consumerGroup, topic, partition2, 20, "partition2-metadata")
	require.NoError(t, err)

	// Fetch offsets for both partitions
	offset1, metadata1, err := handler.FetchOffset(ctx, consumerGroup, topic, partition1)
	require.NoError(t, err)
	assert.Equal(t, int64(10), offset1)
	assert.Equal(t, "partition1-metadata", metadata1)

	offset2, metadata2, err := handler.FetchOffset(ctx, consumerGroup, topic, partition2)
	require.NoError(t, err)
	assert.Equal(t, int64(20), offset2)
	assert.Equal(t, "partition2-metadata", metadata2)
}

// TestConcurrentOffsetCommits tests concurrent offset commits from multiple consumers
func TestConcurrentOffsetCommits(t *testing.T) {
	ctx := context.Background()
	handler := integration.NewTestSeaweedMQHandler(t)
	defer handler.Cleanup()

	topic := mq_pb.Topic{Namespace: "test", Name: "concurrent-test"}
	partition := int32(0)
	consumerGroup := "concurrent-group"

	// Launch multiple goroutines to commit offsets concurrently
	done := make(chan bool, 10)
	for i := 0; i < 10; i++ {
		go func(offset int64) {
			err := handler.CommitOffset(ctx, consumerGroup, topic, partition, offset, fmt.Sprintf("concurrent-%d", offset))
			assert.NoError(t, err)

			// Small delay to simulate real consumer behavior
			time.Sleep(10 * time.Millisecond)
			done <- true
		}(int64(i))
	}

	// Wait for all commits to complete
	for i := 0; i < 10; i++ {
		<-done
	}

	// Fetch the final offset (should be one of the committed values)
	finalOffset, _, err := handler.FetchOffset(ctx, consumerGroup, topic, partition)
	require.NoError(t, err)
	assert.GreaterOrEqual(t, finalOffset, int64(0), "Final offset should be >= 0")
	assert.LessOrEqual(t, finalOffset, int64(9), "Final offset should be <= 9")
}
