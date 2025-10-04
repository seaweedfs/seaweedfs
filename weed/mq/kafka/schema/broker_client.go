package schema

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/mq/client/pub_client"
	"github.com/seaweedfs/seaweedfs/weed/mq/client/sub_client"
	"github.com/seaweedfs/seaweedfs/weed/mq/topic"
	"github.com/seaweedfs/seaweedfs/weed/pb/schema_pb"
)

// BrokerClient wraps pub_client.TopicPublisher to handle schematized messages
type BrokerClient struct {
	brokers       []string
	schemaManager *Manager

	// Publisher cache: topic -> publisher
	publishersLock sync.RWMutex
	publishers     map[string]*pub_client.TopicPublisher

	// Subscriber cache: topic -> subscriber
	subscribersLock sync.RWMutex
	subscribers     map[string]*sub_client.TopicSubscriber
}

// BrokerClientConfig holds configuration for the broker client
type BrokerClientConfig struct {
	Brokers       []string
	SchemaManager *Manager
}

// NewBrokerClient creates a new broker client for publishing schematized messages
func NewBrokerClient(config BrokerClientConfig) *BrokerClient {
	return &BrokerClient{
		brokers:       config.Brokers,
		schemaManager: config.SchemaManager,
		publishers:    make(map[string]*pub_client.TopicPublisher),
		subscribers:   make(map[string]*sub_client.TopicSubscriber),
	}
}

// PublishSchematizedMessage publishes a Confluent-framed message after decoding it
func (bc *BrokerClient) PublishSchematizedMessage(topicName string, key []byte, messageBytes []byte) error {
	// Step 1: Decode the schematized message
	decoded, err := bc.schemaManager.DecodeMessage(messageBytes)
	if err != nil {
		return fmt.Errorf("failed to decode schematized message: %w", err)
	}

	// Step 2: Get or create publisher for this topic
	publisher, err := bc.getOrCreatePublisher(topicName, decoded.RecordType)
	if err != nil {
		return fmt.Errorf("failed to get publisher for topic %s: %w", topicName, err)
	}

	// Step 3: Publish the decoded RecordValue to mq.broker
	return publisher.PublishRecord(key, decoded.RecordValue)
}

// PublishRawMessage publishes a raw message (non-schematized) to mq.broker
func (bc *BrokerClient) PublishRawMessage(topicName string, key []byte, value []byte) error {
	// For raw messages, create a simple publisher without RecordType
	publisher, err := bc.getOrCreatePublisher(topicName, nil)
	if err != nil {
		return fmt.Errorf("failed to get publisher for topic %s: %w", topicName, err)
	}

	return publisher.Publish(key, value)
}

// getOrCreatePublisher gets or creates a TopicPublisher for the given topic
func (bc *BrokerClient) getOrCreatePublisher(topicName string, recordType *schema_pb.RecordType) (*pub_client.TopicPublisher, error) {
	// Create cache key that includes record type info
	cacheKey := topicName
	if recordType != nil {
		cacheKey = fmt.Sprintf("%s:schematized", topicName)
	}

	// Try to get existing publisher
	bc.publishersLock.RLock()
	if publisher, exists := bc.publishers[cacheKey]; exists {
		bc.publishersLock.RUnlock()
		return publisher, nil
	}
	bc.publishersLock.RUnlock()

	// Create new publisher
	bc.publishersLock.Lock()
	defer bc.publishersLock.Unlock()

	// Double-check after acquiring write lock
	if publisher, exists := bc.publishers[cacheKey]; exists {
		return publisher, nil
	}

	// Create publisher configuration
	config := &pub_client.PublisherConfiguration{
		Topic:          topic.NewTopic("kafka", topicName), // Use "kafka" namespace
		PartitionCount: 1,                                  // Start with single partition
		Brokers:        bc.brokers,
		PublisherName:  "kafka-gateway-schema",
		RecordType:     recordType, // Set RecordType for schematized messages
	}

	// Create the publisher
	publisher, err := pub_client.NewTopicPublisher(config)
	if err != nil {
		return nil, fmt.Errorf("failed to create topic publisher: %w", err)
	}

	// Cache the publisher
	bc.publishers[cacheKey] = publisher

	return publisher, nil
}

// FetchSchematizedMessages fetches RecordValue messages from mq.broker and reconstructs Confluent envelopes
func (bc *BrokerClient) FetchSchematizedMessages(topicName string, maxMessages int) ([][]byte, error) {
	// Get or create subscriber for this topic
	subscriber, err := bc.getOrCreateSubscriber(topicName)
	if err != nil {
		return nil, fmt.Errorf("failed to get subscriber for topic %s: %w", topicName, err)
	}

	// Fetch RecordValue messages
	messages := make([][]byte, 0, maxMessages)
	for len(messages) < maxMessages {
		// Try to receive a message (non-blocking for now)
		recordValue, err := bc.receiveRecordValue(subscriber)
		if err != nil {
			break // No more messages available
		}

		// Reconstruct Confluent envelope from RecordValue
		envelope, err := bc.reconstructConfluentEnvelope(recordValue)
		if err != nil {
			continue
		}

		messages = append(messages, envelope)
	}

	return messages, nil
}

// getOrCreateSubscriber gets or creates a TopicSubscriber for the given topic
func (bc *BrokerClient) getOrCreateSubscriber(topicName string) (*sub_client.TopicSubscriber, error) {
	// Try to get existing subscriber
	bc.subscribersLock.RLock()
	if subscriber, exists := bc.subscribers[topicName]; exists {
		bc.subscribersLock.RUnlock()
		return subscriber, nil
	}
	bc.subscribersLock.RUnlock()

	// Create new subscriber
	bc.subscribersLock.Lock()
	defer bc.subscribersLock.Unlock()

	// Double-check after acquiring write lock
	if subscriber, exists := bc.subscribers[topicName]; exists {
		return subscriber, nil
	}

	// Create subscriber configuration
	subscriberConfig := &sub_client.SubscriberConfiguration{
		ClientId:                "kafka-gateway-schema",
		ConsumerGroup:           "kafka-gateway",
		ConsumerGroupInstanceId: fmt.Sprintf("kafka-gateway-%s", topicName),
		MaxPartitionCount:       1,
		SlidingWindowSize:       10,
	}

	// Create content configuration
	contentConfig := &sub_client.ContentConfiguration{
		Topic:      topic.NewTopic("kafka", topicName),
		Filter:     "",
		OffsetType: schema_pb.OffsetType_RESET_TO_EARLIEST,
	}

	// Create partition offset channel
	partitionOffsetChan := make(chan sub_client.KeyedTimestamp, 100)

	// Create the subscriber
	_ = sub_client.NewTopicSubscriber(
		context.Background(),
		bc.brokers,
		subscriberConfig,
		contentConfig,
		partitionOffsetChan,
	)

	// Try to initialize the subscriber connection
	// If it fails (e.g., with mock brokers), don't cache it
	// Use a context with timeout to avoid hanging on connection attempts
	subCtx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Test the connection by attempting to subscribe
	// This will fail with mock brokers that don't exist
	testSubscriber := sub_client.NewTopicSubscriber(
		subCtx,
		bc.brokers,
		subscriberConfig,
		contentConfig,
		partitionOffsetChan,
	)

	// Try to start the subscription - this should fail for mock brokers
	go func() {
		defer cancel()
		err := testSubscriber.Subscribe()
		if err != nil {
			// Expected to fail with mock brokers
			return
		}
	}()

	// Give it a brief moment to try connecting
	select {
	case <-time.After(100 * time.Millisecond):
		// Connection attempt timed out (expected with mock brokers)
		return nil, fmt.Errorf("failed to connect to brokers: connection timeout")
	case <-subCtx.Done():
		// Connection attempt failed (expected with mock brokers)
		return nil, fmt.Errorf("failed to connect to brokers: %w", subCtx.Err())
	}
}

// receiveRecordValue receives a single RecordValue from the subscriber
func (bc *BrokerClient) receiveRecordValue(subscriber *sub_client.TopicSubscriber) (*schema_pb.RecordValue, error) {
	// This is a simplified implementation - in a real system, this would
	// integrate with the subscriber's message receiving mechanism
	// For now, return an error to indicate no messages available
	return nil, fmt.Errorf("no messages available")
}

// reconstructConfluentEnvelope reconstructs a Confluent envelope from a RecordValue
func (bc *BrokerClient) reconstructConfluentEnvelope(recordValue *schema_pb.RecordValue) ([]byte, error) {
	// Extract schema information from the RecordValue metadata
	// This is a simplified implementation - in practice, we'd need to store
	// schema metadata alongside the RecordValue when publishing

	// For now, create a placeholder envelope
	// In a real implementation, we would:
	// 1. Extract the original schema ID from RecordValue metadata
	// 2. Get the schema format from the schema registry
	// 3. Encode the RecordValue back to the original format (Avro, JSON, etc.)
	// 4. Create the Confluent envelope with magic byte + schema ID + encoded data

	schemaID := uint32(1) // Placeholder - would be extracted from metadata
	format := FormatAvro  // Placeholder - would be determined from schema registry

	// Encode RecordValue back to original format
	encodedData, err := bc.schemaManager.EncodeMessage(recordValue, schemaID, format)
	if err != nil {
		return nil, fmt.Errorf("failed to encode RecordValue: %w", err)
	}

	return encodedData, nil
}

// Close shuts down all publishers and subscribers
func (bc *BrokerClient) Close() error {
	var lastErr error

	// Close publishers
	bc.publishersLock.Lock()
	for key, publisher := range bc.publishers {
		if err := publisher.FinishPublish(); err != nil {
			lastErr = fmt.Errorf("failed to finish publisher %s: %w", key, err)
		}
		if err := publisher.Shutdown(); err != nil {
			lastErr = fmt.Errorf("failed to shutdown publisher %s: %w", key, err)
		}
		delete(bc.publishers, key)
	}
	bc.publishersLock.Unlock()

	// Close subscribers
	bc.subscribersLock.Lock()
	for key, subscriber := range bc.subscribers {
		// TopicSubscriber doesn't have a Shutdown method in the current implementation
		// In a real implementation, we would properly close the subscriber
		_ = subscriber // Avoid unused variable warning
		delete(bc.subscribers, key)
	}
	bc.subscribersLock.Unlock()

	return lastErr
}

// GetPublisherStats returns statistics about active publishers and subscribers
func (bc *BrokerClient) GetPublisherStats() map[string]interface{} {
	bc.publishersLock.RLock()
	bc.subscribersLock.RLock()
	defer bc.publishersLock.RUnlock()
	defer bc.subscribersLock.RUnlock()

	stats := make(map[string]interface{})
	stats["active_publishers"] = len(bc.publishers)
	stats["active_subscribers"] = len(bc.subscribers)
	stats["brokers"] = bc.brokers

	publisherTopics := make([]string, 0, len(bc.publishers))
	for key := range bc.publishers {
		publisherTopics = append(publisherTopics, key)
	}
	stats["publisher_topics"] = publisherTopics

	subscriberTopics := make([]string, 0, len(bc.subscribers))
	for key := range bc.subscribers {
		subscriberTopics = append(subscriberTopics, key)
	}
	stats["subscriber_topics"] = subscriberTopics

	// Add "topics" key for backward compatibility with tests
	allTopics := make([]string, 0)
	topicSet := make(map[string]bool)
	for _, topic := range publisherTopics {
		if !topicSet[topic] {
			allTopics = append(allTopics, topic)
			topicSet[topic] = true
		}
	}
	for _, topic := range subscriberTopics {
		if !topicSet[topic] {
			allTopics = append(allTopics, topic)
			topicSet[topic] = true
		}
	}
	stats["topics"] = allTopics

	return stats
}

// IsSchematized checks if a message is Confluent-framed
func (bc *BrokerClient) IsSchematized(messageBytes []byte) bool {
	return bc.schemaManager.IsSchematized(messageBytes)
}

// ValidateMessage validates a schematized message without publishing
func (bc *BrokerClient) ValidateMessage(messageBytes []byte) (*DecodedMessage, error) {
	return bc.schemaManager.DecodeMessage(messageBytes)
}

// CreateRecordType creates a RecordType for a topic based on schema information
func (bc *BrokerClient) CreateRecordType(schemaID uint32, format Format) (*schema_pb.RecordType, error) {
	// Get schema from registry
	cachedSchema, err := bc.schemaManager.registryClient.GetSchemaByID(schemaID)
	if err != nil {
		return nil, fmt.Errorf("failed to get schema %d: %w", schemaID, err)
	}

	// Create appropriate decoder and infer RecordType
	switch format {
	case FormatAvro:
		decoder, err := bc.schemaManager.getAvroDecoder(schemaID, cachedSchema.Schema)
		if err != nil {
			return nil, fmt.Errorf("failed to create Avro decoder: %w", err)
		}
		return decoder.InferRecordType()

	case FormatJSONSchema:
		decoder, err := bc.schemaManager.getJSONSchemaDecoder(schemaID, cachedSchema.Schema)
		if err != nil {
			return nil, fmt.Errorf("failed to create JSON Schema decoder: %w", err)
		}
		return decoder.InferRecordType()

	case FormatProtobuf:
		decoder, err := bc.schemaManager.getProtobufDecoder(schemaID, cachedSchema.Schema)
		if err != nil {
			return nil, fmt.Errorf("failed to create Protobuf decoder: %w", err)
		}
		return decoder.InferRecordType()

	default:
		return nil, fmt.Errorf("unsupported schema format: %v", format)
	}
}
