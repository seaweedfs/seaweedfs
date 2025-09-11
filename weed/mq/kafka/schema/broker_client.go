package schema

import (
	"fmt"
	"sync"

	"github.com/seaweedfs/seaweedfs/weed/mq/client/pub_client"
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

// Close shuts down all publishers
func (bc *BrokerClient) Close() error {
	bc.publishersLock.Lock()
	defer bc.publishersLock.Unlock()

	var lastErr error
	for key, publisher := range bc.publishers {
		if err := publisher.FinishPublish(); err != nil {
			lastErr = fmt.Errorf("failed to finish publisher %s: %w", key, err)
		}
		if err := publisher.Shutdown(); err != nil {
			lastErr = fmt.Errorf("failed to shutdown publisher %s: %w", key, err)
		}
		delete(bc.publishers, key)
	}

	return lastErr
}

// GetPublisherStats returns statistics about active publishers
func (bc *BrokerClient) GetPublisherStats() map[string]interface{} {
	bc.publishersLock.RLock()
	defer bc.publishersLock.RUnlock()

	stats := make(map[string]interface{})
	stats["active_publishers"] = len(bc.publishers)
	stats["brokers"] = bc.brokers

	topicList := make([]string, 0, len(bc.publishers))
	for key := range bc.publishers {
		topicList = append(topicList, key)
	}
	stats["topics"] = topicList

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
