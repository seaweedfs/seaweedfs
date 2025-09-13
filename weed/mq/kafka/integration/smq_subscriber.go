package integration

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/mq/client/sub_client"
	"github.com/seaweedfs/seaweedfs/weed/mq/kafka/offset"
	"github.com/seaweedfs/seaweedfs/weed/mq/topic"
	"github.com/seaweedfs/seaweedfs/weed/pb/mq_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/schema_pb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/proto"
)

// SMQSubscriber handles subscribing to SeaweedMQ messages for Kafka fetch requests
type SMQSubscriber struct {
	brokers        []string
	grpcDialOption grpc.DialOption
	ctx            context.Context

	// Active subscriptions
	subscriptionsLock sync.RWMutex
	subscriptions     map[string]*SubscriptionWrapper // key: topic-partition-consumerGroup

	// Offset mapping
	offsetMapper  *offset.KafkaToSMQMapper
	offsetStorage *offset.SMQIntegratedStorage
}

// SubscriptionWrapper wraps a SMQ subscription with Kafka-specific metadata
type SubscriptionWrapper struct {
	subscriber     *sub_client.TopicSubscriber
	kafkaTopic     string
	kafkaPartition int32
	consumerGroup  string
	startOffset    int64

	// Message buffer for Kafka fetch responses
	messageBuffer chan *KafkaMessage
	isActive      bool
	createdAt     time.Time

	// Offset tracking
	ledger            *offset.PersistentLedger
	lastFetchedOffset int64
}

// KafkaMessage represents a message converted from SMQ to Kafka format
type KafkaMessage struct {
	Key       []byte
	Value     []byte
	Offset    int64
	Partition int32
	Timestamp int64
	Headers   map[string][]byte

	// Original SMQ data for reference
	SMQTimestamp int64
	SMQRecord    *schema_pb.RecordValue
}

// NewSMQSubscriber creates a new SMQ subscriber for Kafka messages
func NewSMQSubscriber(brokers []string) (*SMQSubscriber, error) {
	// Create offset storage
	offsetStorage, err := offset.NewSMQIntegratedStorage(brokers)
	if err != nil {
		return nil, fmt.Errorf("failed to create offset storage: %w", err)
	}

	return &SMQSubscriber{
		brokers:        brokers,
		grpcDialOption: grpc.WithTransportCredentials(insecure.NewCredentials()),
		ctx:            context.Background(),
		subscriptions:  make(map[string]*SubscriptionWrapper),
		offsetStorage:  offsetStorage,
	}, nil
}

// Subscribe creates a subscription for Kafka fetch requests
func (s *SMQSubscriber) Subscribe(
	kafkaTopic string,
	kafkaPartition int32,
	startOffset int64,
	consumerGroup string,
) (*SubscriptionWrapper, error) {

	key := fmt.Sprintf("%s-%d-%s", kafkaTopic, kafkaPartition, consumerGroup)

	s.subscriptionsLock.Lock()
	defer s.subscriptionsLock.Unlock()

	// Check if subscription already exists
	if existing, exists := s.subscriptions[key]; exists {
		return existing, nil
	}

	// Create persistent ledger for offset mapping
	ledgerKey := fmt.Sprintf("%s-%d", kafkaTopic, kafkaPartition)
	ledger, err := offset.NewPersistentLedger(ledgerKey, s.offsetStorage)
	if err != nil {
		return nil, fmt.Errorf("failed to create ledger: %w", err)
	}

	// Create offset mapper
	offsetMapper := offset.NewKafkaToSMQMapper(ledger.Ledger)

	// Convert Kafka offset to SMQ PartitionOffset
	partitionOffset, offsetType, err := offsetMapper.CreateSMQSubscriptionRequest(
		kafkaTopic, kafkaPartition, startOffset, consumerGroup)
	if err != nil {
		return nil, fmt.Errorf("failed to create SMQ subscription request: %w", err)
	}

	// Create SMQ subscriber configuration
	subscriberConfig := &sub_client.SubscriberConfiguration{
		ConsumerGroup:           fmt.Sprintf("kafka-%s", consumerGroup),
		ConsumerGroupInstanceId: fmt.Sprintf("kafka-%s-%s-%d", consumerGroup, kafkaTopic, kafkaPartition),
		GrpcDialOption:          s.grpcDialOption,
		MaxPartitionCount:       1,
		SlidingWindowSize:       100,
	}

	contentConfig := &sub_client.ContentConfiguration{
		Topic:            topic.NewTopic("kafka", kafkaTopic),
		PartitionOffsets: []*schema_pb.PartitionOffset{partitionOffset},
		OffsetType:       offsetType,
	}

	// Create SMQ subscriber
	subscriber := sub_client.NewTopicSubscriber(
		s.ctx,
		s.brokers,
		subscriberConfig,
		contentConfig,
		make(chan sub_client.KeyedOffset, 100),
	)

	// Create subscription wrapper
	wrapper := &SubscriptionWrapper{
		subscriber:        subscriber,
		kafkaTopic:        kafkaTopic,
		kafkaPartition:    kafkaPartition,
		consumerGroup:     consumerGroup,
		startOffset:       startOffset,
		messageBuffer:     make(chan *KafkaMessage, 1000),
		isActive:          true,
		createdAt:         time.Now(),
		ledger:            ledger,
		lastFetchedOffset: startOffset - 1,
	}

	// Set up message handler
	subscriber.SetOnDataMessageFn(func(m *mq_pb.SubscribeMessageResponse_Data) {
		kafkaMsg := s.convertSMQToKafkaMessage(m, wrapper)
		if kafkaMsg != nil {
			select {
			case wrapper.messageBuffer <- kafkaMsg:
				wrapper.lastFetchedOffset = kafkaMsg.Offset
			default:
				// Buffer full, drop message (or implement backpressure)
			}
		}
	})

	// Start subscription in background
	go func() {
		if err := subscriber.Subscribe(); err != nil {
			fmt.Printf("SMQ subscription error for %s: %v\n", key, err)
		}
	}()

	s.subscriptions[key] = wrapper
	return wrapper, nil
}

// FetchMessages retrieves messages for a Kafka fetch request
func (s *SMQSubscriber) FetchMessages(
	kafkaTopic string,
	kafkaPartition int32,
	fetchOffset int64,
	maxBytes int32,
	consumerGroup string,
) ([]*KafkaMessage, error) {

	key := fmt.Sprintf("%s-%d-%s", kafkaTopic, kafkaPartition, consumerGroup)

	s.subscriptionsLock.RLock()
	wrapper, exists := s.subscriptions[key]
	s.subscriptionsLock.RUnlock()

	if !exists {
		// Create subscription if it doesn't exist
		var err error
		wrapper, err = s.Subscribe(kafkaTopic, kafkaPartition, fetchOffset, consumerGroup)
		if err != nil {
			return nil, fmt.Errorf("failed to create subscription: %w", err)
		}
	}

	// Collect messages from buffer
	var messages []*KafkaMessage
	var totalBytes int32 = 0
	timeout := time.After(100 * time.Millisecond) // Short timeout for fetch

	for totalBytes < maxBytes && len(messages) < 1000 {
		select {
		case msg := <-wrapper.messageBuffer:
			// Only include messages at or after the requested offset
			if msg.Offset >= fetchOffset {
				messages = append(messages, msg)
				totalBytes += int32(len(msg.Key) + len(msg.Value) + 50) // Estimate overhead
			}
		case <-timeout:
			// Timeout reached, return what we have
			goto done
		}
	}

done:
	return messages, nil
}

// convertSMQToKafkaMessage converts a SMQ message to Kafka format
func (s *SMQSubscriber) convertSMQToKafkaMessage(
	smqMsg *mq_pb.SubscribeMessageResponse_Data,
	wrapper *SubscriptionWrapper,
) *KafkaMessage {

	// Unmarshal SMQ record
	record := &schema_pb.RecordValue{}
	if err := proto.Unmarshal(smqMsg.Data.Value, record); err != nil {
		return nil
	}

	// Extract Kafka metadata from the record
	kafkaOffsetField := record.Fields["_kafka_offset"]
	kafkaPartitionField := record.Fields["_kafka_partition"]
	kafkaTimestampField := record.Fields["_kafka_timestamp"]

	if kafkaOffsetField == nil || kafkaPartitionField == nil {
		// This might be a non-Kafka message, skip it
		return nil
	}

	kafkaOffset := kafkaOffsetField.GetInt64Value()
	kafkaPartition := kafkaPartitionField.GetInt32Value()
	kafkaTimestamp := smqMsg.Data.TsNs

	if kafkaTimestampField != nil {
		kafkaTimestamp = kafkaTimestampField.GetInt64Value()
	}

	// Extract original message content (remove Kafka metadata)
	originalRecord := &schema_pb.RecordValue{
		Fields: make(map[string]*schema_pb.Value),
	}

	for key, value := range record.Fields {
		if !isKafkaMetadataField(key) {
			originalRecord.Fields[key] = value
		}
	}

	// Convert record back to bytes for Kafka
	valueBytes, err := proto.Marshal(originalRecord)
	if err != nil {
		return nil
	}

	return &KafkaMessage{
		Key:          smqMsg.Data.Key,
		Value:        valueBytes,
		Offset:       kafkaOffset,
		Partition:    kafkaPartition,
		Timestamp:    kafkaTimestamp,
		Headers:      make(map[string][]byte),
		SMQTimestamp: smqMsg.Data.TsNs,
		SMQRecord:    record,
	}
}

// isKafkaMetadataField checks if a field is Kafka metadata
func isKafkaMetadataField(fieldName string) bool {
	return fieldName == "_kafka_offset" ||
		fieldName == "_kafka_partition" ||
		fieldName == "_kafka_timestamp"
}

// GetSubscriptionStats returns statistics for a subscription
func (s *SMQSubscriber) GetSubscriptionStats(
	kafkaTopic string,
	kafkaPartition int32,
	consumerGroup string,
) map[string]interface{} {

	key := fmt.Sprintf("%s-%d-%s", kafkaTopic, kafkaPartition, consumerGroup)

	s.subscriptionsLock.RLock()
	wrapper, exists := s.subscriptions[key]
	s.subscriptionsLock.RUnlock()

	if !exists {
		return map[string]interface{}{"exists": false}
	}

	return map[string]interface{}{
		"exists":              true,
		"kafka_topic":         wrapper.kafkaTopic,
		"kafka_partition":     wrapper.kafkaPartition,
		"consumer_group":      wrapper.consumerGroup,
		"start_offset":        wrapper.startOffset,
		"last_fetched_offset": wrapper.lastFetchedOffset,
		"buffer_size":         len(wrapper.messageBuffer),
		"is_active":           wrapper.isActive,
		"created_at":          wrapper.createdAt,
	}
}

// CommitOffset commits a consumer offset
func (s *SMQSubscriber) CommitOffset(
	kafkaTopic string,
	kafkaPartition int32,
	offset int64,
	consumerGroup string,
) error {

	key := fmt.Sprintf("%s-%d-%s", kafkaTopic, kafkaPartition, consumerGroup)

	s.subscriptionsLock.RLock()
	wrapper, exists := s.subscriptions[key]
	s.subscriptionsLock.RUnlock()

	if !exists {
		return fmt.Errorf("subscription not found: %s", key)
	}

	// Update the subscription's committed offset
	// In a full implementation, this would persist the offset to SMQ
	wrapper.lastFetchedOffset = offset

	return nil
}

// CloseSubscription closes a specific subscription
func (s *SMQSubscriber) CloseSubscription(
	kafkaTopic string,
	kafkaPartition int32,
	consumerGroup string,
) error {

	key := fmt.Sprintf("%s-%d-%s", kafkaTopic, kafkaPartition, consumerGroup)

	s.subscriptionsLock.Lock()
	defer s.subscriptionsLock.Unlock()

	wrapper, exists := s.subscriptions[key]
	if !exists {
		return nil // Already closed
	}

	wrapper.isActive = false
	close(wrapper.messageBuffer)
	delete(s.subscriptions, key)

	return nil
}

// Close shuts down all subscriptions
func (s *SMQSubscriber) Close() error {
	s.subscriptionsLock.Lock()
	defer s.subscriptionsLock.Unlock()

	for key, wrapper := range s.subscriptions {
		wrapper.isActive = false
		close(wrapper.messageBuffer)
		delete(s.subscriptions, key)
	}

	return s.offsetStorage.Close()
}

// GetHighWaterMark returns the high water mark for a topic-partition
func (s *SMQSubscriber) GetHighWaterMark(kafkaTopic string, kafkaPartition int32) (int64, error) {
	ledgerKey := fmt.Sprintf("%s-%d", kafkaTopic, kafkaPartition)
	return s.offsetStorage.GetHighWaterMark(ledgerKey)
}

// GetEarliestOffset returns the earliest available offset for a topic-partition
func (s *SMQSubscriber) GetEarliestOffset(kafkaTopic string, kafkaPartition int32) (int64, error) {
	ledgerKey := fmt.Sprintf("%s-%d", kafkaTopic, kafkaPartition)
	entries, err := s.offsetStorage.LoadOffsetMappings(ledgerKey)
	if err != nil {
		return 0, err
	}

	if len(entries) == 0 {
		return 0, nil
	}

	return entries[0].KafkaOffset, nil
}
