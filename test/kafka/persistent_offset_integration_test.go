package kafka

import (
	"fmt"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/mq/kafka/integration"
	"github.com/seaweedfs/seaweedfs/weed/mq/kafka/offset"
	"github.com/seaweedfs/seaweedfs/weed/pb/schema_pb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPersistentOffsetIntegration(t *testing.T) {
	// Skip if no brokers available
	brokers := []string{"localhost:17777"}

	t.Run("OffsetPersistenceAndRecovery", func(t *testing.T) {
		testOffsetPersistenceAndRecovery(t, brokers)
	})

	t.Run("SMQPublisherIntegration", func(t *testing.T) {
		testSMQPublisherIntegration(t, brokers)
	})

	t.Run("SMQSubscriberIntegration", func(t *testing.T) {
		testSMQSubscriberIntegration(t, brokers)
	})

	t.Run("EndToEndPublishSubscribe", func(t *testing.T) {
		testEndToEndPublishSubscribe(t, brokers)
	})

	t.Run("OffsetMappingConsistency", func(t *testing.T) {
		testOffsetMappingConsistency(t, brokers)
	})
}

func testOffsetPersistenceAndRecovery(t *testing.T, brokers []string) {
	// Create offset storage
	storage, err := offset.NewSeaweedMQStorage(brokers)
	require.NoError(t, err)
	defer storage.Close()

	topicPartition := "test-persistence-topic-0"

	// Create first ledger and add some entries
	ledger1, err := offset.NewPersistentLedger(topicPartition, storage)
	require.NoError(t, err)

	// Add test entries
	testEntries := []struct {
		kafkaOffset int64
		timestamp   int64
		size        int32
	}{
		{0, time.Now().UnixNano(), 100},
		{1, time.Now().UnixNano() + 1000, 150},
		{2, time.Now().UnixNano() + 2000, 200},
	}

	for _, entry := range testEntries {
		offset := ledger1.AssignOffsets(1)
		assert.Equal(t, entry.kafkaOffset, offset)

		err := ledger1.AppendRecord(entry.kafkaOffset, entry.timestamp, entry.size)
		require.NoError(t, err)
	}

	// Verify ledger state
	assert.Equal(t, int64(3), ledger1.GetHighWaterMark())
	assert.Equal(t, int64(0), ledger1.GetEarliestOffset())
	assert.Equal(t, int64(2), ledger1.GetLatestOffset())

	// Wait for persistence
	time.Sleep(2 * time.Second)

	// Create second ledger (simulating restart)
	ledger2, err := offset.NewPersistentLedger(topicPartition, storage)
	require.NoError(t, err)

	// Verify recovered state
	assert.Equal(t, ledger1.GetHighWaterMark(), ledger2.GetHighWaterMark())
	assert.Equal(t, ledger1.GetEarliestOffset(), ledger2.GetEarliestOffset())
	assert.Equal(t, ledger1.GetLatestOffset(), ledger2.GetLatestOffset())

	// Verify entries are recovered
	entries1 := ledger1.GetEntries()
	entries2 := ledger2.GetEntries()
	assert.Equal(t, len(entries1), len(entries2))

	for i, entry1 := range entries1 {
		entry2 := entries2[i]
		assert.Equal(t, entry1.KafkaOffset, entry2.KafkaOffset)
		assert.Equal(t, entry1.Timestamp, entry2.Timestamp)
		assert.Equal(t, entry1.Size, entry2.Size)
	}

	t.Logf("Successfully persisted and recovered %d offset entries", len(entries1))
}

func testSMQPublisherIntegration(t *testing.T, brokers []string) {
	publisher, err := integration.NewSMQPublisher(brokers)
	require.NoError(t, err)
	defer publisher.Close()

	kafkaTopic := "test-smq-publisher"
	kafkaPartition := int32(0)

	// Create test record type
	recordType := &schema_pb.RecordType{
		Fields: []*schema_pb.Field{
			{
				Name:       "user_id",
				FieldIndex: 0,
				Type: &schema_pb.Type{
					Kind: &schema_pb.Type_ScalarType{ScalarType: schema_pb.ScalarType_STRING},
				},
				IsRequired: true,
			},
			{
				Name:       "action",
				FieldIndex: 1,
				Type: &schema_pb.Type{
					Kind: &schema_pb.Type_ScalarType{ScalarType: schema_pb.ScalarType_STRING},
				},
				IsRequired: true,
			},
		},
	}

	// Publish test messages
	testMessages := []struct {
		key    string
		userId string
		action string
	}{
		{"user1", "user123", "login"},
		{"user2", "user456", "purchase"},
		{"user3", "user789", "logout"},
	}

	var publishedOffsets []int64

	for _, msg := range testMessages {
		record := &schema_pb.RecordValue{
			Fields: map[string]*schema_pb.Value{
				"user_id": {
					Kind: &schema_pb.Value_StringValue{StringValue: msg.userId},
				},
				"action": {
					Kind: &schema_pb.Value_StringValue{StringValue: msg.action},
				},
			},
		}

		offset, err := publisher.PublishMessage(
			kafkaTopic, kafkaPartition, []byte(msg.key), record, recordType)
		require.NoError(t, err)

		publishedOffsets = append(publishedOffsets, offset)
		t.Logf("Published message with key=%s, offset=%d", msg.key, offset)
	}

	// Verify sequential offsets
	for i, offset := range publishedOffsets {
		assert.Equal(t, int64(i), offset)
	}

	// Get ledger and verify state
	ledger := publisher.GetLedger(kafkaTopic, kafkaPartition)
	require.NotNil(t, ledger)

	assert.Equal(t, int64(3), ledger.GetHighWaterMark())
	assert.Equal(t, int64(0), ledger.GetEarliestOffset())
	assert.Equal(t, int64(2), ledger.GetLatestOffset())

	// Get topic stats
	stats := publisher.GetTopicStats(kafkaTopic)
	assert.True(t, stats["exists"].(bool))
	assert.Contains(t, stats["smq_topic"].(string), kafkaTopic)

	t.Logf("SMQ Publisher integration successful: %+v", stats)
}

func testSMQSubscriberIntegration(t *testing.T, brokers []string) {
	// First publish some messages
	publisher, err := integration.NewSMQPublisher(brokers)
	require.NoError(t, err)
	defer publisher.Close()

	kafkaTopic := "test-smq-subscriber"
	kafkaPartition := int32(0)
	consumerGroup := "test-consumer-group"

	recordType := &schema_pb.RecordType{
		Fields: []*schema_pb.Field{
			{
				Name:       "message",
				FieldIndex: 0,
				Type: &schema_pb.Type{
					Kind: &schema_pb.Type_ScalarType{ScalarType: schema_pb.ScalarType_STRING},
				},
				IsRequired: true,
			},
		},
	}

	// Publish test messages
	for i := 0; i < 5; i++ {
		record := &schema_pb.RecordValue{
			Fields: map[string]*schema_pb.Value{
				"message": {
					Kind: &schema_pb.Value_StringValue{StringValue: fmt.Sprintf("test-message-%d", i)},
				},
			},
		}

		_, err := publisher.PublishMessage(
			kafkaTopic, kafkaPartition, []byte(fmt.Sprintf("key-%d", i)), record, recordType)
		require.NoError(t, err)
	}

	// Wait for messages to be available
	time.Sleep(2 * time.Second)

	// Create subscriber
	subscriber, err := integration.NewSMQSubscriber(brokers)
	require.NoError(t, err)
	defer subscriber.Close()

	// Subscribe from offset 0
	subscription, err := subscriber.Subscribe(kafkaTopic, kafkaPartition, 0, consumerGroup)
	require.NoError(t, err)
	_ = subscription // Use the subscription variable

	// Wait for subscription to be active
	time.Sleep(2 * time.Second)

	// Fetch messages
	messages, err := subscriber.FetchMessages(kafkaTopic, kafkaPartition, 0, 1024*1024, consumerGroup)
	require.NoError(t, err)

	t.Logf("Fetched %d messages", len(messages))

	// Verify messages
	assert.True(t, len(messages) > 0, "Should have received messages")

	for i, msg := range messages {
		assert.Equal(t, int64(i), msg.Offset)
		assert.Equal(t, kafkaPartition, msg.Partition)
		assert.Equal(t, fmt.Sprintf("key-%d", i), string(msg.Key))

		t.Logf("Message %d: offset=%d, key=%s, partition=%d",
			i, msg.Offset, string(msg.Key), msg.Partition)
	}

	// Test offset commit
	err = subscriber.CommitOffset(kafkaTopic, kafkaPartition, 2, consumerGroup)
	require.NoError(t, err)

	// Get subscription stats
	stats := subscriber.GetSubscriptionStats(kafkaTopic, kafkaPartition, consumerGroup)
	assert.True(t, stats["exists"].(bool))
	assert.Equal(t, kafkaTopic, stats["kafka_topic"])
	assert.Equal(t, kafkaPartition, stats["kafka_partition"])

	t.Logf("SMQ Subscriber integration successful: %+v", stats)
}

func testEndToEndPublishSubscribe(t *testing.T, brokers []string) {
	kafkaTopic := "test-e2e-pubsub"
	kafkaPartition := int32(0)
	consumerGroup := "e2e-consumer"

	// Create publisher and subscriber
	publisher, err := integration.NewSMQPublisher(brokers)
	require.NoError(t, err)
	defer publisher.Close()

	subscriber, err := integration.NewSMQSubscriber(brokers)
	require.NoError(t, err)
	defer subscriber.Close()

	// Create subscription first
	_, err = subscriber.Subscribe(kafkaTopic, kafkaPartition, 0, consumerGroup)
	require.NoError(t, err)

	time.Sleep(1 * time.Second) // Let subscription initialize

	recordType := &schema_pb.RecordType{
		Fields: []*schema_pb.Field{
			{
				Name:       "data",
				FieldIndex: 0,
				Type: &schema_pb.Type{
					Kind: &schema_pb.Type_ScalarType{ScalarType: schema_pb.ScalarType_STRING},
				},
				IsRequired: true,
			},
		},
	}

	// Publish messages
	numMessages := 10
	for i := 0; i < numMessages; i++ {
		record := &schema_pb.RecordValue{
			Fields: map[string]*schema_pb.Value{
				"data": {
					Kind: &schema_pb.Value_StringValue{StringValue: fmt.Sprintf("e2e-data-%d", i)},
				},
			},
		}

		offset, err := publisher.PublishMessage(
			kafkaTopic, kafkaPartition, []byte(fmt.Sprintf("e2e-key-%d", i)), record, recordType)
		require.NoError(t, err)
		assert.Equal(t, int64(i), offset)

		t.Logf("Published E2E message %d with offset %d", i, offset)
	}

	// Wait for messages to propagate
	time.Sleep(3 * time.Second)

	// Fetch all messages
	messages, err := subscriber.FetchMessages(kafkaTopic, kafkaPartition, 0, 1024*1024, consumerGroup)
	require.NoError(t, err)

	t.Logf("Fetched %d messages in E2E test", len(messages))

	// Verify we got all messages
	assert.Equal(t, numMessages, len(messages), "Should receive all published messages")

	// Verify message content and order
	for i, msg := range messages {
		assert.Equal(t, int64(i), msg.Offset)
		assert.Equal(t, fmt.Sprintf("e2e-key-%d", i), string(msg.Key))

		// Verify timestamp is reasonable (within last minute)
		assert.True(t, msg.Timestamp > time.Now().Add(-time.Minute).UnixNano())
		assert.True(t, msg.Timestamp <= time.Now().UnixNano())
	}

	// Test fetching from specific offset
	messagesFromOffset5, err := subscriber.FetchMessages(kafkaTopic, kafkaPartition, 5, 1024*1024, consumerGroup)
	require.NoError(t, err)

	expectedFromOffset5 := numMessages - 5
	assert.Equal(t, expectedFromOffset5, len(messagesFromOffset5), "Should get messages from offset 5 onwards")

	if len(messagesFromOffset5) > 0 {
		assert.Equal(t, int64(5), messagesFromOffset5[0].Offset)
	}

	t.Logf("E2E test successful: published %d, fetched %d, fetched from offset 5: %d",
		numMessages, len(messages), len(messagesFromOffset5))
}

func testOffsetMappingConsistency(t *testing.T, brokers []string) {
	kafkaTopic := "test-offset-consistency"
	kafkaPartition := int32(0)

	// Create publisher
	publisher, err := integration.NewSMQPublisher(brokers)
	require.NoError(t, err)
	defer publisher.Close()

	recordType := &schema_pb.RecordType{
		Fields: []*schema_pb.Field{
			{
				Name:       "value",
				FieldIndex: 0,
				Type: &schema_pb.Type{
					Kind: &schema_pb.Type_ScalarType{ScalarType: schema_pb.ScalarType_INT64},
				},
				IsRequired: true,
			},
		},
	}

	// Publish messages and track offsets
	numMessages := 20
	publishedOffsets := make([]int64, numMessages)

	for i := 0; i < numMessages; i++ {
		record := &schema_pb.RecordValue{
			Fields: map[string]*schema_pb.Value{
				"value": {
					Kind: &schema_pb.Value_Int64Value{Int64Value: int64(i * 100)},
				},
			},
		}

		offset, err := publisher.PublishMessage(
			kafkaTopic, kafkaPartition, []byte(fmt.Sprintf("key-%d", i)), record, recordType)
		require.NoError(t, err)

		publishedOffsets[i] = offset
	}

	// Verify offsets are sequential
	for i, offset := range publishedOffsets {
		assert.Equal(t, int64(i), offset, "Offsets should be sequential starting from 0")
	}

	// Get ledger and verify consistency
	ledger := publisher.GetLedger(kafkaTopic, kafkaPartition)
	require.NotNil(t, ledger)

	// Verify high water mark
	expectedHighWaterMark := int64(numMessages)
	assert.Equal(t, expectedHighWaterMark, ledger.GetHighWaterMark())

	// Verify earliest and latest offsets
	assert.Equal(t, int64(0), ledger.GetEarliestOffset())
	assert.Equal(t, int64(numMessages-1), ledger.GetLatestOffset())

	// Test offset mapping
	mapper := offset.NewKafkaToSMQMapper(ledger.Ledger)

	for i := int64(0); i < int64(numMessages); i++ {
		// Test Kafka to SMQ mapping
		partitionOffset, err := mapper.KafkaOffsetToSMQPartitionOffset(i, kafkaTopic, kafkaPartition)
		require.NoError(t, err)

		assert.Equal(t, int32(0), partitionOffset.Partition.RangeStart) // Partition 0 maps to range [0-31]
		assert.Equal(t, int32(31), partitionOffset.Partition.RangeStop)
		assert.True(t, partitionOffset.StartTsNs > 0, "SMQ timestamp should be positive")

		// Test reverse mapping
		kafkaOffset, err := mapper.SMQPartitionOffsetToKafkaOffset(partitionOffset)
		require.NoError(t, err)
		assert.Equal(t, i, kafkaOffset, "Reverse mapping should return original offset")
	}

	// Test mapping validation
	err = mapper.ValidateMapping(kafkaTopic, kafkaPartition)
	assert.NoError(t, err, "Offset mapping should be valid")

	// Test offset range queries
	entries := ledger.GetEntries()
	if len(entries) >= 2 {
		startTime := entries[0].Timestamp
		endTime := entries[len(entries)-1].Timestamp

		startOffset, endOffset, err := mapper.GetOffsetRange(startTime, endTime)
		require.NoError(t, err)

		assert.Equal(t, int64(0), startOffset)
		assert.Equal(t, int64(numMessages-1), endOffset)
	}

	t.Logf("Offset mapping consistency verified for %d messages", numMessages)
	t.Logf("High water mark: %d, Earliest: %d, Latest: %d",
		ledger.GetHighWaterMark(), ledger.GetEarliestOffset(), ledger.GetLatestOffset())
}

// Helper function to create test record
func createTestRecord(fields map[string]interface{}) *schema_pb.RecordValue {
	record := &schema_pb.RecordValue{
		Fields: make(map[string]*schema_pb.Value),
	}

	for key, value := range fields {
		switch v := value.(type) {
		case string:
			record.Fields[key] = &schema_pb.Value{
				Kind: &schema_pb.Value_StringValue{StringValue: v},
			}
		case int64:
			record.Fields[key] = &schema_pb.Value{
				Kind: &schema_pb.Value_Int64Value{Int64Value: v},
			}
		case int32:
			record.Fields[key] = &schema_pb.Value{
				Kind: &schema_pb.Value_Int32Value{Int32Value: v},
			}
		case bool:
			record.Fields[key] = &schema_pb.Value{
				Kind: &schema_pb.Value_BoolValue{BoolValue: v},
			}
		}
	}

	return record
}
