package offset

import (
	"context"
	"fmt"
	"sort"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/mq/client/pub_client"
	"github.com/seaweedfs/seaweedfs/weed/mq/client/sub_client"
	"github.com/seaweedfs/seaweedfs/weed/mq/pub_balancer"
	"github.com/seaweedfs/seaweedfs/weed/mq/topic"
	"github.com/seaweedfs/seaweedfs/weed/pb/mq_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/schema_pb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/proto"
)

// PersistentLedger extends Ledger with persistence capabilities
type PersistentLedger struct {
	*Ledger
	topicPartition string
	storage        LedgerStorage
}

// LedgerStorage interface for persisting offset mappings
type LedgerStorage interface {
	// SaveOffsetMapping persists a Kafka offset -> SMQ timestamp mapping
	SaveOffsetMapping(topicPartition string, kafkaOffset, smqTimestamp int64, size int32) error

	// LoadOffsetMappings restores all offset mappings for a topic-partition
	LoadOffsetMappings(topicPartition string) ([]OffsetEntry, error)

	// GetHighWaterMark returns the highest Kafka offset for a topic-partition
	GetHighWaterMark(topicPartition string) (int64, error)
}

// NewPersistentLedger creates a ledger that persists to storage
func NewPersistentLedger(topicPartition string, storage LedgerStorage) (*PersistentLedger, error) {
	// Try to restore from storage
	entries, err := storage.LoadOffsetMappings(topicPartition)
	if err != nil {
		return nil, fmt.Errorf("failed to load offset mappings: %w", err)
	}

	// Determine next offset
	var nextOffset int64 = 0
	if len(entries) > 0 {
		// Sort entries by offset to find the highest
		sort.Slice(entries, func(i, j int) bool {
			return entries[i].KafkaOffset < entries[j].KafkaOffset
		})
		nextOffset = entries[len(entries)-1].KafkaOffset + 1
	}

	// Create base ledger with restored state
	ledger := &Ledger{
		entries:    entries,
		nextOffset: nextOffset,
	}

	// Update earliest/latest timestamps
	if len(entries) > 0 {
		ledger.earliestTime = entries[0].Timestamp
		ledger.latestTime = entries[len(entries)-1].Timestamp
	}

	return &PersistentLedger{
		Ledger:         ledger,
		topicPartition: topicPartition,
		storage:        storage,
	}, nil
}

// AppendRecord persists the offset mapping in addition to in-memory storage
func (pl *PersistentLedger) AppendRecord(kafkaOffset, timestamp int64, size int32) error {
	// First persist to storage
	if err := pl.storage.SaveOffsetMapping(pl.topicPartition, kafkaOffset, timestamp, size); err != nil {
		return fmt.Errorf("failed to persist offset mapping: %w", err)
	}

	// Then update in-memory ledger
	return pl.Ledger.AppendRecord(kafkaOffset, timestamp, size)
}

// GetEntries returns the offset entries from the underlying ledger
func (pl *PersistentLedger) GetEntries() []OffsetEntry {
	return pl.Ledger.GetEntries()
}

// SeaweedMQStorage implements LedgerStorage using SeaweedMQ as the backend
type SeaweedMQStorage struct {
	brokers        []string
	grpcDialOption grpc.DialOption
	ctx            context.Context
	publisher      *pub_client.TopicPublisher
	offsetTopic    topic.Topic
}

// NewSeaweedMQStorage creates a new SeaweedMQ-backed storage
func NewSeaweedMQStorage(brokers []string) (*SeaweedMQStorage, error) {
	storage := &SeaweedMQStorage{
		brokers:        brokers,
		grpcDialOption: grpc.WithTransportCredentials(insecure.NewCredentials()),
		ctx:            context.Background(),
		offsetTopic:    topic.NewTopic("kafka-system", "offset-mappings"),
	}

	// Create record type for offset mappings
	recordType := &schema_pb.RecordType{
		Fields: []*schema_pb.Field{
			{
				Name:       "topic_partition",
				FieldIndex: 0,
				Type: &schema_pb.Type{
					Kind: &schema_pb.Type_ScalarType{ScalarType: schema_pb.ScalarType_STRING},
				},
				IsRequired: true,
			},
			{
				Name:       "kafka_offset",
				FieldIndex: 1,
				Type: &schema_pb.Type{
					Kind: &schema_pb.Type_ScalarType{ScalarType: schema_pb.ScalarType_INT64},
				},
				IsRequired: true,
			},
			{
				Name:       "smq_timestamp",
				FieldIndex: 2,
				Type: &schema_pb.Type{
					Kind: &schema_pb.Type_ScalarType{ScalarType: schema_pb.ScalarType_INT64},
				},
				IsRequired: true,
			},
			{
				Name:       "message_size",
				FieldIndex: 3,
				Type: &schema_pb.Type{
					Kind: &schema_pb.Type_ScalarType{ScalarType: schema_pb.ScalarType_INT32},
				},
				IsRequired: true,
			},
		},
	}

	// Create publisher for offset mappings
	publisher, err := pub_client.NewTopicPublisher(&pub_client.PublisherConfiguration{
		Topic:          storage.offsetTopic,
		PartitionCount: 16, // Multiple partitions for offset storage
		Brokers:        brokers,
		PublisherName:  "kafka-offset-storage",
		RecordType:     recordType,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create offset publisher: %w", err)
	}

	storage.publisher = publisher
	return storage, nil
}

// SaveOffsetMapping stores the offset mapping in SeaweedMQ
func (s *SeaweedMQStorage) SaveOffsetMapping(topicPartition string, kafkaOffset, smqTimestamp int64, size int32) error {
	// Create record for the offset mapping
	record := &schema_pb.RecordValue{
		Fields: map[string]*schema_pb.Value{
			"topic_partition": {
				Kind: &schema_pb.Value_StringValue{StringValue: topicPartition},
			},
			"kafka_offset": {
				Kind: &schema_pb.Value_Int64Value{Int64Value: kafkaOffset},
			},
			"smq_timestamp": {
				Kind: &schema_pb.Value_Int64Value{Int64Value: smqTimestamp},
			},
			"message_size": {
				Kind: &schema_pb.Value_Int32Value{Int32Value: size},
			},
		},
	}

	// Use topic-partition as key for consistent partitioning
	key := []byte(topicPartition)

	// Publish the offset mapping
	if err := s.publisher.PublishRecord(key, record); err != nil {
		return fmt.Errorf("failed to publish offset mapping: %w", err)
	}

	return nil
}

// LoadOffsetMappings retrieves all offset mappings from SeaweedMQ
func (s *SeaweedMQStorage) LoadOffsetMappings(topicPartition string) ([]OffsetEntry, error) {
	// Create subscriber to read offset mappings
	subscriberConfig := &sub_client.SubscriberConfiguration{
		ConsumerGroup:           "kafka-offset-loader",
		ConsumerGroupInstanceId: fmt.Sprintf("offset-loader-%s", topicPartition),
		GrpcDialOption:          s.grpcDialOption,
		MaxPartitionCount:       16,
		SlidingWindowSize:       100,
	}

	contentConfig := &sub_client.ContentConfiguration{
		Topic: s.offsetTopic,
		PartitionOffsets: []*schema_pb.PartitionOffset{
			{
				Partition: &schema_pb.Partition{
					RingSize:   pub_balancer.MaxPartitionCount,
					RangeStart: 0,
					RangeStop:  pub_balancer.MaxPartitionCount - 1,
				},
				StartTsNs: 0, // Read from beginning
			},
		},
		OffsetType: schema_pb.OffsetType_RESET_TO_EARLIEST,
		Filter:     fmt.Sprintf("topic_partition == '%s'", topicPartition), // Filter by topic-partition
	}

	subscriber := sub_client.NewTopicSubscriber(
		s.ctx,
		s.brokers,
		subscriberConfig,
		contentConfig,
		make(chan sub_client.KeyedOffset, 100),
	)

	var entries []OffsetEntry
	entriesChan := make(chan OffsetEntry, 1000)
	done := make(chan bool, 1)

	// Set up message handler
	subscriber.SetOnDataMessageFn(func(m *mq_pb.SubscribeMessageResponse_Data) {
		record := &schema_pb.RecordValue{}
		if err := proto.Unmarshal(m.Data.Value, record); err != nil {
			return
		}

		// Extract fields
		topicPartField := record.Fields["topic_partition"]
		kafkaOffsetField := record.Fields["kafka_offset"]
		smqTimestampField := record.Fields["smq_timestamp"]
		messageSizeField := record.Fields["message_size"]

		if topicPartField == nil || kafkaOffsetField == nil ||
			smqTimestampField == nil || messageSizeField == nil {
			return
		}

		// Only process records for our topic-partition
		if topicPartField.GetStringValue() != topicPartition {
			return
		}

		entry := OffsetEntry{
			KafkaOffset: kafkaOffsetField.GetInt64Value(),
			Timestamp:   smqTimestampField.GetInt64Value(),
			Size:        messageSizeField.GetInt32Value(),
		}

		entriesChan <- entry
	})

	// Subscribe in background
	go func() {
		defer close(done)
		if err := subscriber.Subscribe(); err != nil {
			fmt.Printf("Subscribe error: %v\n", err)
		}
	}()

	// Collect entries for a reasonable time
	timeout := time.After(3 * time.Second)
	collecting := true

	for collecting {
		select {
		case entry := <-entriesChan:
			entries = append(entries, entry)
		case <-timeout:
			collecting = false
		case <-done:
			// Drain remaining entries
			for {
				select {
				case entry := <-entriesChan:
					entries = append(entries, entry)
				default:
					collecting = false
					goto done_collecting
				}
			}
		}
	}
done_collecting:

	// Sort entries by Kafka offset
	sort.Slice(entries, func(i, j int) bool {
		return entries[i].KafkaOffset < entries[j].KafkaOffset
	})

	return entries, nil
}

// GetHighWaterMark returns the next available offset
func (s *SeaweedMQStorage) GetHighWaterMark(topicPartition string) (int64, error) {
	entries, err := s.LoadOffsetMappings(topicPartition)
	if err != nil {
		return 0, err
	}

	if len(entries) == 0 {
		return 0, nil
	}

	// Find highest offset
	var maxOffset int64 = -1
	for _, entry := range entries {
		if entry.KafkaOffset > maxOffset {
			maxOffset = entry.KafkaOffset
		}
	}

	return maxOffset + 1, nil
}

// Close shuts down the storage
func (s *SeaweedMQStorage) Close() error {
	if s.publisher != nil {
		return s.publisher.Shutdown()
	}
	return nil
}
