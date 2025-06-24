package integration

import (
	"fmt"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/mq/schema"
	"github.com/seaweedfs/seaweedfs/weed/pb/mq_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/schema_pb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBasicPublishSubscribe(t *testing.T) {
	suite := NewIntegrationTestSuite(t)
	require.NoError(t, suite.Setup())

	namespace := "test"
	topicName := fmt.Sprintf("basic-pubsub-%d", time.Now().UnixNano()) // Unique topic name per run
	testSchema := CreateTestSchema()
	messageCount := 10

	// Create publisher
	pubConfig := &PublisherTestConfig{
		Namespace:      namespace,
		TopicName:      topicName,
		PartitionCount: 1,
		PublisherName:  "basic-publisher",
		RecordType:     testSchema,
	}

	publisher, err := suite.CreatePublisher(pubConfig)
	require.NoError(t, err)

	// Create subscriber
	subConfig := &SubscriberTestConfig{
		Namespace:          namespace,
		TopicName:          topicName,
		ConsumerGroup:      "test-group",
		ConsumerInstanceId: "consumer-1",
		MaxPartitionCount:  1,
		SlidingWindowSize:  10,
		OffsetType:         schema_pb.OffsetType_RESET_TO_EARLIEST,
	}

	subscriber, err := suite.CreateSubscriber(subConfig)
	require.NoError(t, err, "Failed to create subscriber")

	// Set up message collector
	collector := NewMessageCollector(messageCount)
	subscriber.SetOnDataMessageFn(func(m *mq_pb.SubscribeMessageResponse_Data) {
		t.Logf("[Subscriber] Received message with key: %s, ts: %d", string(m.Data.Key), m.Data.TsNs)
		collector.AddMessage(TestMessage{
			ID:        fmt.Sprintf("msg-%d", len(collector.GetMessages())),
			Content:   m.Data.Value,
			Timestamp: time.Unix(0, m.Data.TsNs),
			Key:       m.Data.Key,
		})
	})

	// Start subscriber
	go func() {
		err := subscriber.Subscribe()
		if err != nil {
			t.Logf("Subscriber error: %v", err)
		}
	}()

	// Wait for subscriber to be ready
	t.Logf("[Test] Waiting for subscriber to be ready...")
	time.Sleep(2 * time.Second)

	// Publish test messages
	for i := 0; i < messageCount; i++ {
		record := schema.RecordBegin().
			SetString("id", fmt.Sprintf("msg-%d", i)).
			SetInt64("timestamp", time.Now().UnixNano()).
			SetString("content", fmt.Sprintf("Test message %d", i)).
			SetInt32("sequence", int32(i)).
			RecordEnd()

		key := []byte(fmt.Sprintf("key-%d", i))
		t.Logf("[Publisher] Publishing message %d with key: %s", i, string(key))
		err := publisher.PublishRecord(key, record)
		require.NoError(t, err, "Failed to publish message %d", i)
	}

	t.Logf("[Test] Waiting for messages to be received...")
	messages := collector.WaitForMessages(30 * time.Second)
	t.Logf("[Test] WaitForMessages returned. Received %d messages.", len(messages))

	// Verify all messages were received
	assert.Len(t, messages, messageCount, "Expected %d messages, got %d", messageCount, len(messages))

	// Verify message content
	for i, msg := range messages {
		assert.NotEmpty(t, msg.Content, "Message %d should have content", i)
		assert.NotEmpty(t, msg.Key, "Message %d should have key", i)
	}

	t.Logf("[Test] TestBasicPublishSubscribe completed.")
}

func TestMultipleConsumers(t *testing.T) {
	suite := NewIntegrationTestSuite(t)
	require.NoError(t, suite.Setup())

	namespace := "test"
	topicName := "multi-consumer"
	testSchema := CreateTestSchema()
	messageCount := 20
	consumerCount := 3

	// Create publisher
	pubConfig := &PublisherTestConfig{
		Namespace:      namespace,
		TopicName:      topicName,
		PartitionCount: 3, // Multiple partitions for load distribution
		PublisherName:  "multi-publisher",
		RecordType:     testSchema,
	}

	publisher, err := suite.CreatePublisher(pubConfig)
	require.NoError(t, err)

	// Create multiple consumers
	collectors := make([]*MessageCollector, consumerCount)
	for i := 0; i < consumerCount; i++ {
		collectors[i] = NewMessageCollector(messageCount / consumerCount) // Expect roughly equal distribution

		subConfig := &SubscriberTestConfig{
			Namespace:          namespace,
			TopicName:          topicName,
			ConsumerGroup:      "multi-consumer-group", // Same group for load balancing
			ConsumerInstanceId: fmt.Sprintf("consumer-%d", i),
			MaxPartitionCount:  1,
			SlidingWindowSize:  10,
			OffsetType:         schema_pb.OffsetType_RESET_TO_EARLIEST,
		}

		subscriber, err := suite.CreateSubscriber(subConfig)
		require.NoError(t, err)

		// Set up message collection for this consumer
		collectorIndex := i
		subscriber.SetOnDataMessageFn(func(m *mq_pb.SubscribeMessageResponse_Data) {
			collectors[collectorIndex].AddMessage(TestMessage{
				ID:        fmt.Sprintf("consumer-%d-msg-%d", collectorIndex, len(collectors[collectorIndex].GetMessages())),
				Content:   m.Data.Value,
				Timestamp: time.Unix(0, m.Data.TsNs),
				Key:       m.Data.Key,
			})
		})

		// Start subscriber
		go func() {
			subscriber.Subscribe()
		}()
	}

	// Wait for subscribers to be ready
	time.Sleep(3 * time.Second)

	// Publish messages with different keys to distribute across partitions
	for i := 0; i < messageCount; i++ {
		record := schema.RecordBegin().
			SetString("id", fmt.Sprintf("multi-msg-%d", i)).
			SetInt64("timestamp", time.Now().UnixNano()).
			SetString("content", fmt.Sprintf("Multi consumer test message %d", i)).
			SetInt32("sequence", int32(i)).
			RecordEnd()

		key := []byte(fmt.Sprintf("partition-key-%d", i%3)) // Distribute across 3 partitions
		err := publisher.PublishRecord(key, record)
		require.NoError(t, err)
	}

	// Wait for all messages to be consumed
	time.Sleep(10 * time.Second)

	// Verify message distribution
	totalReceived := 0
	for i, collector := range collectors {
		messages := collector.GetMessages()
		t.Logf("Consumer %d received %d messages", i, len(messages))
		totalReceived += len(messages)
	}

	// All messages should be consumed across all consumers
	assert.Equal(t, messageCount, totalReceived, "Total messages received should equal messages sent")
}

func TestMessageOrdering(t *testing.T) {
	suite := NewIntegrationTestSuite(t)
	require.NoError(t, suite.Setup())

	namespace := "test"
	topicName := "ordering-test"
	testSchema := CreateTestSchema()
	messageCount := 15

	// Create publisher
	pubConfig := &PublisherTestConfig{
		Namespace:      namespace,
		TopicName:      topicName,
		PartitionCount: 1, // Single partition to guarantee ordering
		PublisherName:  "ordering-publisher",
		RecordType:     testSchema,
	}

	publisher, err := suite.CreatePublisher(pubConfig)
	require.NoError(t, err)

	// Create subscriber
	subConfig := &SubscriberTestConfig{
		Namespace:          namespace,
		TopicName:          topicName,
		ConsumerGroup:      "ordering-group",
		ConsumerInstanceId: "ordering-consumer",
		MaxPartitionCount:  1,
		SlidingWindowSize:  5,
		OffsetType:         schema_pb.OffsetType_RESET_TO_EARLIEST,
	}

	subscriber, err := suite.CreateSubscriber(subConfig)
	require.NoError(t, err)

	// Set up message collector
	collector := NewMessageCollector(messageCount)
	subscriber.SetOnDataMessageFn(func(m *mq_pb.SubscribeMessageResponse_Data) {
		collector.AddMessage(TestMessage{
			ID:        fmt.Sprintf("ordered-msg"),
			Content:   m.Data.Value,
			Timestamp: time.Unix(0, m.Data.TsNs),
			Key:       m.Data.Key,
		})
	})

	// Start subscriber
	go func() {
		subscriber.Subscribe()
	}()

	// Wait for consumer to be ready
	time.Sleep(2 * time.Second)

	// Publish messages with same key to ensure they go to same partition
	publishTimes := make([]time.Time, messageCount)
	for i := 0; i < messageCount; i++ {
		publishTimes[i] = time.Now()

		record := schema.RecordBegin().
			SetString("id", fmt.Sprintf("ordered-%d", i)).
			SetInt64("timestamp", publishTimes[i].UnixNano()).
			SetString("content", fmt.Sprintf("Ordered message %d", i)).
			SetInt32("sequence", int32(i)).
			RecordEnd()

		key := []byte("same-partition-key") // Same key ensures same partition
		err := publisher.PublishRecord(key, record)
		require.NoError(t, err)

		// Small delay to ensure different timestamps
		time.Sleep(10 * time.Millisecond)
	}

	// Wait for all messages
	messages := collector.WaitForMessages(30 * time.Second)
	require.Len(t, messages, messageCount)

	// Verify ordering within the partition
	suite.AssertMessageOrdering(t, messages)
}

func TestSchemaValidation(t *testing.T) {
	suite := NewIntegrationTestSuite(t)
	require.NoError(t, suite.Setup())

	namespace := "test"
	topicName := "schema-validation"

	// Test with simple schema
	simpleSchema := CreateTestSchema()

	pubConfig := &PublisherTestConfig{
		Namespace:      namespace,
		TopicName:      topicName,
		PartitionCount: 1,
		PublisherName:  "schema-publisher",
		RecordType:     simpleSchema,
	}

	publisher, err := suite.CreatePublisher(pubConfig)
	require.NoError(t, err)

	// Test valid record
	validRecord := schema.RecordBegin().
		SetString("id", "valid-msg").
		SetInt64("timestamp", time.Now().UnixNano()).
		SetString("content", "Valid message").
		SetInt32("sequence", 1).
		RecordEnd()

	err = publisher.PublishRecord([]byte("test-key"), validRecord)
	assert.NoError(t, err, "Valid record should be published successfully")

	// Test with complex nested schema
	complexSchema := CreateComplexTestSchema()

	complexPubConfig := &PublisherTestConfig{
		Namespace:      namespace,
		TopicName:      topicName + "-complex",
		PartitionCount: 1,
		PublisherName:  "complex-publisher",
		RecordType:     complexSchema,
	}

	complexPublisher, err := suite.CreatePublisher(complexPubConfig)
	require.NoError(t, err)

	// Test complex nested record
	complexRecord := schema.RecordBegin().
		SetString("user_id", "user123").
		SetString("name", "John Doe").
		SetInt32("age", 30).
		SetStringList("emails", "john@example.com", "john.doe@company.com").
		SetRecord("address",
			schema.RecordBegin().
				SetString("street", "123 Main St").
				SetString("city", "New York").
				SetString("zipcode", "10001").
				RecordEnd()).
		SetInt64("created_at", time.Now().UnixNano()).
		RecordEnd()

	err = complexPublisher.PublishRecord([]byte("complex-key"), complexRecord)
	assert.NoError(t, err, "Complex nested record should be published successfully")
}
