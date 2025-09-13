package integration

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/mq/client/pub_client"
	"github.com/seaweedfs/seaweedfs/weed/mq/kafka/offset"
	"github.com/seaweedfs/seaweedfs/weed/mq/topic"
	"github.com/seaweedfs/seaweedfs/weed/pb/schema_pb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// SMQPublisher handles publishing Kafka messages to SeaweedMQ with offset tracking
type SMQPublisher struct {
	brokers        []string
	grpcDialOption grpc.DialOption
	ctx            context.Context

	// Topic publishers - one per Kafka topic
	publishersLock sync.RWMutex
	publishers     map[string]*TopicPublisherWrapper

	// Offset persistence
	offsetStorage *offset.SMQOffsetStorage

	// Ledgers for offset tracking
	ledgersLock sync.RWMutex
	ledgers     map[string]*offset.PersistentLedger // key: topic-partition
}

// TopicPublisherWrapper wraps a SMQ publisher with Kafka-specific metadata
type TopicPublisherWrapper struct {
	publisher  *pub_client.TopicPublisher
	kafkaTopic string
	smqTopic   topic.Topic
	recordType *schema_pb.RecordType
	createdAt  time.Time
}

// NewSMQPublisher creates a new SMQ publisher for Kafka messages
func NewSMQPublisher(brokers []string) (*SMQPublisher, error) {
	// Create offset storage
	// Use first broker as filer address for offset storage
	filerAddress := brokers[0]
	offsetStorage, err := offset.NewSMQOffsetStorage(filerAddress)
	if err != nil {
		return nil, fmt.Errorf("failed to create offset storage: %w", err)
	}

	return &SMQPublisher{
		brokers:        brokers,
		grpcDialOption: grpc.WithTransportCredentials(insecure.NewCredentials()),
		ctx:            context.Background(),
		publishers:     make(map[string]*TopicPublisherWrapper),
		offsetStorage:  offsetStorage,
		ledgers:        make(map[string]*offset.PersistentLedger),
	}, nil
}

// PublishMessage publishes a Kafka message to SMQ with offset tracking
func (p *SMQPublisher) PublishMessage(
	kafkaTopic string,
	kafkaPartition int32,
	key []byte,
	value *schema_pb.RecordValue,
	recordType *schema_pb.RecordType,
) (int64, error) {

	// Get or create publisher for this topic
	publisher, err := p.getOrCreatePublisher(kafkaTopic, recordType)
	if err != nil {
		return -1, fmt.Errorf("failed to get publisher: %w", err)
	}

	// Get or create ledger for offset tracking
	ledger, err := p.getOrCreateLedger(kafkaTopic, kafkaPartition)
	if err != nil {
		return -1, fmt.Errorf("failed to get ledger: %w", err)
	}

	// Assign Kafka offset
	kafkaOffset := ledger.AssignOffsets(1)

	// Add Kafka metadata to the record
	enrichedValue := p.enrichRecordWithKafkaMetadata(value, kafkaOffset, kafkaPartition)

	// Publish to SMQ
	if err := publisher.publisher.PublishRecord(key, enrichedValue); err != nil {
		return -1, fmt.Errorf("failed to publish to SMQ: %w", err)
	}

	// Record the offset mapping
	smqTimestamp := time.Now().UnixNano()
	if err := ledger.AppendRecord(kafkaOffset, smqTimestamp, int32(len(key)+estimateRecordSize(enrichedValue))); err != nil {
		return -1, fmt.Errorf("failed to record offset mapping: %w", err)
	}

	return kafkaOffset, nil
}

// getOrCreatePublisher gets or creates a SMQ publisher for the given Kafka topic
func (p *SMQPublisher) getOrCreatePublisher(kafkaTopic string, recordType *schema_pb.RecordType) (*TopicPublisherWrapper, error) {
	p.publishersLock.RLock()
	if publisher, exists := p.publishers[kafkaTopic]; exists {
		p.publishersLock.RUnlock()
		return publisher, nil
	}
	p.publishersLock.RUnlock()

	p.publishersLock.Lock()
	defer p.publishersLock.Unlock()

	// Double-check after acquiring write lock
	if publisher, exists := p.publishers[kafkaTopic]; exists {
		return publisher, nil
	}

	// Create SMQ topic name (namespace: kafka, name: original topic)
	smqTopic := topic.NewTopic("kafka", kafkaTopic)

	// Enhance record type with Kafka metadata fields
	enhancedRecordType := p.enhanceRecordTypeWithKafkaMetadata(recordType)

	// Create SMQ publisher
	publisher, err := pub_client.NewTopicPublisher(&pub_client.PublisherConfiguration{
		Topic:          smqTopic,
		PartitionCount: 16, // Use multiple partitions for better distribution
		Brokers:        p.brokers,
		PublisherName:  fmt.Sprintf("kafka-gateway-%s", kafkaTopic),
		RecordType:     enhancedRecordType,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create SMQ publisher: %w", err)
	}

	wrapper := &TopicPublisherWrapper{
		publisher:  publisher,
		kafkaTopic: kafkaTopic,
		smqTopic:   smqTopic,
		recordType: enhancedRecordType,
		createdAt:  time.Now(),
	}

	p.publishers[kafkaTopic] = wrapper
	return wrapper, nil
}

// getOrCreateLedger gets or creates a persistent ledger for offset tracking
func (p *SMQPublisher) getOrCreateLedger(kafkaTopic string, partition int32) (*offset.PersistentLedger, error) {
	key := fmt.Sprintf("%s-%d", kafkaTopic, partition)

	p.ledgersLock.RLock()
	if ledger, exists := p.ledgers[key]; exists {
		p.ledgersLock.RUnlock()
		return ledger, nil
	}
	p.ledgersLock.RUnlock()

	p.ledgersLock.Lock()
	defer p.ledgersLock.Unlock()

	// Double-check after acquiring write lock
	if ledger, exists := p.ledgers[key]; exists {
		return ledger, nil
	}

	// Create persistent ledger
	ledger := offset.NewPersistentLedger(key, p.offsetStorage)

	p.ledgers[key] = ledger
	return ledger, nil
}

// enhanceRecordTypeWithKafkaMetadata adds Kafka-specific fields to the record type
func (p *SMQPublisher) enhanceRecordTypeWithKafkaMetadata(originalType *schema_pb.RecordType) *schema_pb.RecordType {
	if originalType == nil {
		originalType = &schema_pb.RecordType{}
	}

	// Create enhanced record type with Kafka metadata
	enhanced := &schema_pb.RecordType{
		Fields: make([]*schema_pb.Field, 0, len(originalType.Fields)+3),
	}

	// Copy original fields
	for _, field := range originalType.Fields {
		enhanced.Fields = append(enhanced.Fields, field)
	}

	// Add Kafka metadata fields
	nextIndex := int32(len(originalType.Fields))

	enhanced.Fields = append(enhanced.Fields, &schema_pb.Field{
		Name:       "_kafka_offset",
		FieldIndex: nextIndex,
		Type: &schema_pb.Type{
			Kind: &schema_pb.Type_ScalarType{ScalarType: schema_pb.ScalarType_INT64},
		},
		IsRequired: true,
		IsRepeated: false,
	})
	nextIndex++

	enhanced.Fields = append(enhanced.Fields, &schema_pb.Field{
		Name:       "_kafka_partition",
		FieldIndex: nextIndex,
		Type: &schema_pb.Type{
			Kind: &schema_pb.Type_ScalarType{ScalarType: schema_pb.ScalarType_INT32},
		},
		IsRequired: true,
		IsRepeated: false,
	})
	nextIndex++

	enhanced.Fields = append(enhanced.Fields, &schema_pb.Field{
		Name:       "_kafka_timestamp",
		FieldIndex: nextIndex,
		Type: &schema_pb.Type{
			Kind: &schema_pb.Type_ScalarType{ScalarType: schema_pb.ScalarType_INT64},
		},
		IsRequired: true,
		IsRepeated: false,
	})

	return enhanced
}

// enrichRecordWithKafkaMetadata adds Kafka metadata to the record value
func (p *SMQPublisher) enrichRecordWithKafkaMetadata(
	originalValue *schema_pb.RecordValue,
	kafkaOffset int64,
	kafkaPartition int32,
) *schema_pb.RecordValue {
	if originalValue == nil {
		originalValue = &schema_pb.RecordValue{Fields: make(map[string]*schema_pb.Value)}
	}

	// Create enhanced record value
	enhanced := &schema_pb.RecordValue{
		Fields: make(map[string]*schema_pb.Value),
	}

	// Copy original fields
	for key, value := range originalValue.Fields {
		enhanced.Fields[key] = value
	}

	// Add Kafka metadata
	enhanced.Fields["_kafka_offset"] = &schema_pb.Value{
		Kind: &schema_pb.Value_Int64Value{Int64Value: kafkaOffset},
	}

	enhanced.Fields["_kafka_partition"] = &schema_pb.Value{
		Kind: &schema_pb.Value_Int32Value{Int32Value: kafkaPartition},
	}

	enhanced.Fields["_kafka_timestamp"] = &schema_pb.Value{
		Kind: &schema_pb.Value_Int64Value{Int64Value: time.Now().UnixNano()},
	}

	return enhanced
}

// GetLedger returns the ledger for a topic-partition
func (p *SMQPublisher) GetLedger(kafkaTopic string, partition int32) *offset.PersistentLedger {
	key := fmt.Sprintf("%s-%d", kafkaTopic, partition)

	p.ledgersLock.RLock()
	defer p.ledgersLock.RUnlock()

	return p.ledgers[key]
}

// Close shuts down all publishers and storage
func (p *SMQPublisher) Close() error {
	var lastErr error

	// Close all publishers
	p.publishersLock.Lock()
	for _, wrapper := range p.publishers {
		if err := wrapper.publisher.Shutdown(); err != nil {
			lastErr = err
		}
	}
	p.publishers = make(map[string]*TopicPublisherWrapper)
	p.publishersLock.Unlock()

	// Close offset storage
	if err := p.offsetStorage.Close(); err != nil {
		lastErr = err
	}

	return lastErr
}

// estimateRecordSize estimates the size of a RecordValue in bytes
func estimateRecordSize(record *schema_pb.RecordValue) int {
	if record == nil {
		return 0
	}

	size := 0
	for key, value := range record.Fields {
		size += len(key) + 8 // Key + overhead

		switch v := value.Kind.(type) {
		case *schema_pb.Value_StringValue:
			size += len(v.StringValue)
		case *schema_pb.Value_BytesValue:
			size += len(v.BytesValue)
		case *schema_pb.Value_Int32Value, *schema_pb.Value_FloatValue:
			size += 4
		case *schema_pb.Value_Int64Value, *schema_pb.Value_DoubleValue:
			size += 8
		case *schema_pb.Value_BoolValue:
			size += 1
		default:
			size += 16 // Estimate for complex types
		}
	}

	return size
}

// GetTopicStats returns statistics for a Kafka topic
func (p *SMQPublisher) GetTopicStats(kafkaTopic string) map[string]interface{} {
	stats := make(map[string]interface{})

	p.publishersLock.RLock()
	wrapper, exists := p.publishers[kafkaTopic]
	p.publishersLock.RUnlock()

	if !exists {
		stats["exists"] = false
		return stats
	}

	stats["exists"] = true
	stats["smq_topic"] = wrapper.smqTopic.String()
	stats["created_at"] = wrapper.createdAt
	stats["record_type_fields"] = len(wrapper.recordType.Fields)

	// Collect partition stats
	partitionStats := make(map[string]interface{})
	p.ledgersLock.RLock()
	for key, ledger := range p.ledgers {
		if len(key) > len(kafkaTopic) && key[:len(kafkaTopic)] == kafkaTopic {
			partitionStats[key] = map[string]interface{}{
				"high_water_mark": ledger.GetHighWaterMark(),
				"earliest_offset": ledger.GetEarliestOffset(),
				"latest_offset":   ledger.GetLatestOffset(),
				"entry_count":     len(ledger.GetEntries()),
			}
		}
	}
	p.ledgersLock.RUnlock()

	stats["partitions"] = partitionStats
	return stats
}
