package integration

import (
	"context"
	"encoding/binary"
	"fmt"
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/seaweedfs/seaweedfs/weed/cluster"
	"github.com/seaweedfs/seaweedfs/weed/mq/kafka/offset"
	"github.com/seaweedfs/seaweedfs/weed/mq/pub_balancer"
	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/mq_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/schema_pb"
)

// SeaweedMQHandler integrates Kafka protocol handlers with real SeaweedMQ storage
type SeaweedMQHandler struct {
	agentClient  *AgentClient  // For agent-based connections
	brokerClient *BrokerClient // For broker-based connections
	useBroker    bool          // Flag to determine which client to use

	// Topic registry - still keep track of Kafka topics
	topicsMu sync.RWMutex
	topics   map[string]*KafkaTopicInfo

	// Offset ledgers for Kafka offset translation
	ledgersMu sync.RWMutex
	ledgers   map[TopicPartitionKey]*offset.Ledger
}

// KafkaTopicInfo holds Kafka-specific topic information
type KafkaTopicInfo struct {
	Name       string
	Partitions int32
	CreatedAt  int64

	// SeaweedMQ integration
	SeaweedTopic *schema_pb.Topic
}

// TopicPartitionKey uniquely identifies a topic partition
type TopicPartitionKey struct {
	Topic     string
	Partition int32
}

// NewSeaweedMQHandler creates a new handler with SeaweedMQ integration
func NewSeaweedMQHandler(agentAddress string) (*SeaweedMQHandler, error) {
	agentClient, err := NewAgentClient(agentAddress)
	if err != nil {
		return nil, fmt.Errorf("failed to create agent client: %v", err)
	}

	// Test the connection
	if err := agentClient.HealthCheck(); err != nil {
		agentClient.Close()
		return nil, fmt.Errorf("agent health check failed: %v", err)
	}

	return &SeaweedMQHandler{
		agentClient: agentClient,
		useBroker:   false,
		topics:      make(map[string]*KafkaTopicInfo),
		ledgers:     make(map[TopicPartitionKey]*offset.Ledger),
	}, nil
}

// GetStoredRecords retrieves records from SeaweedMQ storage
// This implements the core integration between Kafka Fetch API and SeaweedMQ storage
func (h *SeaweedMQHandler) GetStoredRecords(topic string, partition int32, fromOffset int64, maxRecords int) ([]offset.SMQRecord, error) {
	// Verify topic exists
	if !h.TopicExists(topic) {
		return nil, fmt.Errorf("topic %s does not exist", topic)
	}

	// Get the offset ledger to translate Kafka offsets to SeaweedMQ timestamps
	ledger := h.GetLedger(topic, partition)
	if ledger == nil {
		// No messages yet, return empty
		return nil, nil
	}

	highWaterMark := ledger.GetHighWaterMark()

	// If fromOffset is at or beyond high water mark, no records to return
	if fromOffset >= highWaterMark {
		return nil, nil
	}

	// Calculate how many records to fetch, respecting the limit
	recordsToFetch := int(highWaterMark - fromOffset)
	if maxRecords > 0 && recordsToFetch > maxRecords {
		recordsToFetch = maxRecords
	}
	if recordsToFetch > 100 {
		recordsToFetch = 100 // Reasonable batch size limit
	}

	// Get or create subscriber session for this topic/partition
	var seaweedRecords []*SeaweedRecord
	var err error

	// Read records using appropriate client (broker or agent)
	if h.useBroker && h.brokerClient != nil {
		brokerSubscriber, subErr := h.brokerClient.GetOrCreateSubscriber(topic, partition, fromOffset)
		if subErr != nil {
			return nil, fmt.Errorf("failed to get broker subscriber: %v", subErr)
		}
		seaweedRecords, err = h.brokerClient.ReadRecords(brokerSubscriber, recordsToFetch)
	} else if h.agentClient != nil {
		agentSubscriber, subErr := h.agentClient.GetOrCreateSubscriber(topic, partition, fromOffset)
		if subErr != nil {
			return nil, fmt.Errorf("failed to get agent subscriber: %v", subErr)
		}
		seaweedRecords, err = h.agentClient.ReadRecords(agentSubscriber, recordsToFetch)
	} else {
		return nil, fmt.Errorf("no SeaweedMQ client available")
	}

	if err != nil {
		// Return empty instead of error for better Kafka compatibility
		return nil, nil
	}

	if len(seaweedRecords) == 0 {
		return nil, nil
	}

	// Convert SeaweedMQ records to SMQRecord interface with proper Kafka offsets
	smqRecords := make([]offset.SMQRecord, 0, len(seaweedRecords))
	for i, seaweedRecord := range seaweedRecords {
		kafkaOffset := fromOffset + int64(i)
		smqRecord := &SeaweedSMQRecord{
			key:       seaweedRecord.Key,
			value:     seaweedRecord.Value,
			timestamp: seaweedRecord.Timestamp,
			offset:    kafkaOffset,
		}
		smqRecords = append(smqRecords, smqRecord)
	}

	return smqRecords, nil
}

// SeaweedSMQRecord implements the offset.SMQRecord interface for SeaweedMQ records
type SeaweedSMQRecord struct {
	key       []byte
	value     []byte
	timestamp int64
	offset    int64
}

// GetKey returns the record key
func (r *SeaweedSMQRecord) GetKey() []byte {
	return r.key
}

// GetValue returns the record value
func (r *SeaweedSMQRecord) GetValue() []byte {
	return r.value
}

// GetTimestamp returns the record timestamp
func (r *SeaweedSMQRecord) GetTimestamp() int64 {
	return r.timestamp
}

// GetOffset returns the Kafka offset for this record
func (r *SeaweedSMQRecord) GetOffset() int64 {
	return r.offset
}

// Close shuts down the handler and all connections
func (h *SeaweedMQHandler) Close() error {
	if h.useBroker && h.brokerClient != nil {
		return h.brokerClient.Close()
	} else if h.agentClient != nil {
		return h.agentClient.Close()
	}
	return nil
}

// CreateTopic creates a new topic in both Kafka registry and SeaweedMQ
func (h *SeaweedMQHandler) CreateTopic(name string, partitions int32) error {
	h.topicsMu.Lock()
	defer h.topicsMu.Unlock()

	// Check if topic already exists
	if _, exists := h.topics[name]; exists {
		return fmt.Errorf("topic %s already exists", name)
	}

	// Create SeaweedMQ topic reference
	seaweedTopic := &schema_pb.Topic{
		Namespace: "kafka",
		Name:      name,
	}

	// Create Kafka topic info
	topicInfo := &KafkaTopicInfo{
		Name:         name,
		Partitions:   partitions,
		CreatedAt:    time.Now().UnixNano(),
		SeaweedTopic: seaweedTopic,
	}

	// Store in registry
	h.topics[name] = topicInfo

	// Initialize offset ledgers for all partitions
	for partitionID := int32(0); partitionID < partitions; partitionID++ {
		key := TopicPartitionKey{Topic: name, Partition: partitionID}
		h.ledgersMu.Lock()
		h.ledgers[key] = offset.NewLedger()
		h.ledgersMu.Unlock()
	}

	return nil
}

// DeleteTopic removes a topic from both Kafka registry and SeaweedMQ
func (h *SeaweedMQHandler) DeleteTopic(name string) error {
	h.topicsMu.Lock()
	defer h.topicsMu.Unlock()

	topicInfo, exists := h.topics[name]
	if !exists {
		return fmt.Errorf("topic %s does not exist", name)
	}

	// Close all publisher sessions for this topic
	for partitionID := int32(0); partitionID < topicInfo.Partitions; partitionID++ {
		if h.useBroker && h.brokerClient != nil {
			h.brokerClient.ClosePublisher(name, partitionID)
		} else if h.agentClient != nil {
			h.agentClient.ClosePublisher(name, partitionID)
		}
	}

	// Remove from registry
	delete(h.topics, name)

	// Clean up offset ledgers
	h.ledgersMu.Lock()
	for partitionID := int32(0); partitionID < topicInfo.Partitions; partitionID++ {
		key := TopicPartitionKey{Topic: name, Partition: partitionID}
		delete(h.ledgers, key)
	}
	h.ledgersMu.Unlock()

	return nil
}

// TopicExists checks if a topic exists
func (h *SeaweedMQHandler) TopicExists(name string) bool {
	h.topicsMu.RLock()
	defer h.topicsMu.RUnlock()

	_, exists := h.topics[name]
	return exists
}

// GetTopicInfo returns information about a topic
func (h *SeaweedMQHandler) GetTopicInfo(name string) (*KafkaTopicInfo, bool) {
	h.topicsMu.RLock()
	defer h.topicsMu.RUnlock()

	info, exists := h.topics[name]
	return info, exists
}

// ListTopics returns all topic names
func (h *SeaweedMQHandler) ListTopics() []string {
	h.topicsMu.RLock()
	defer h.topicsMu.RUnlock()

	topics := make([]string, 0, len(h.topics))
	for name := range h.topics {
		topics = append(topics, name)
	}
	return topics
}

// ProduceRecord publishes a record to SeaweedMQ and updates Kafka offset tracking
func (h *SeaweedMQHandler) ProduceRecord(topic string, partition int32, key []byte, value []byte) (int64, error) {
	// Verify topic exists
	if !h.TopicExists(topic) {
		return 0, fmt.Errorf("topic %s does not exist", topic)
	}

	// Get current timestamp
	timestamp := time.Now().UnixNano()

	// Publish to SeaweedMQ
	var publishErr error
	if h.useBroker && h.brokerClient != nil {
		_, publishErr = h.brokerClient.PublishRecord(topic, partition, key, value, timestamp)
	} else if h.agentClient != nil {
		_, publishErr = h.agentClient.PublishRecord(topic, partition, key, value, timestamp)
	} else {
		publishErr = fmt.Errorf("no client available")
	}

	if publishErr != nil {
		return 0, fmt.Errorf("failed to publish to SeaweedMQ: %v", publishErr)
	}

	// Update Kafka offset ledger
	ledger := h.GetOrCreateLedger(topic, partition)
	kafkaOffset := ledger.AssignOffsets(1) // Assign one Kafka offset

	// Map SeaweedMQ sequence to Kafka offset
	if err := ledger.AppendRecord(kafkaOffset, timestamp, int32(len(value))); err != nil {
		// Log the error but don't fail the produce operation
		fmt.Printf("Warning: failed to update offset ledger: %v\n", err)
	}

	return kafkaOffset, nil
}

// GetOrCreateLedger returns the offset ledger for a topic-partition
func (h *SeaweedMQHandler) GetOrCreateLedger(topic string, partition int32) *offset.Ledger {
	key := TopicPartitionKey{Topic: topic, Partition: partition}

	// Try to get existing ledger
	h.ledgersMu.RLock()
	ledger, exists := h.ledgers[key]
	h.ledgersMu.RUnlock()

	if exists {
		return ledger
	}

	// Create new ledger
	h.ledgersMu.Lock()
	defer h.ledgersMu.Unlock()

	// Double-check after acquiring write lock
	if ledger, exists := h.ledgers[key]; exists {
		return ledger
	}

	// Create and store new ledger
	ledger = offset.NewLedger()
	h.ledgers[key] = ledger
	return ledger
}

// GetLedger returns the offset ledger for a topic-partition, or nil if not found
func (h *SeaweedMQHandler) GetLedger(topic string, partition int32) *offset.Ledger {
	key := TopicPartitionKey{Topic: topic, Partition: partition}

	h.ledgersMu.RLock()
	defer h.ledgersMu.RUnlock()

	return h.ledgers[key]
}

// FetchRecords retrieves records from SeaweedMQ for a Kafka fetch request
func (h *SeaweedMQHandler) FetchRecords(topic string, partition int32, fetchOffset int64, maxBytes int32) ([]byte, error) {
	// Verify topic exists
	if !h.TopicExists(topic) {
		return nil, fmt.Errorf("topic %s does not exist", topic)
	}

	ledger := h.GetLedger(topic, partition)
	if ledger == nil {
		// No messages yet, return empty record batch
		return []byte{}, nil
	}

	highWaterMark := ledger.GetHighWaterMark()

	// If fetch offset is at or beyond high water mark, no records to return
	if fetchOffset >= highWaterMark {
		return []byte{}, nil
	}

	// Get or create subscriber session for this topic/partition
	var seaweedRecords []*SeaweedRecord
	var err error

	// Calculate how many records to fetch
	recordsToFetch := int(highWaterMark - fetchOffset)
	if recordsToFetch > 100 {
		recordsToFetch = 100 // Limit batch size
	}

	// Read records using appropriate client
	if h.useBroker && h.brokerClient != nil {
		brokerSubscriber, subErr := h.brokerClient.GetOrCreateSubscriber(topic, partition, fetchOffset)
		if subErr != nil {
			return nil, fmt.Errorf("failed to get broker subscriber: %v", subErr)
		}
		seaweedRecords, err = h.brokerClient.ReadRecords(brokerSubscriber, recordsToFetch)
	} else if h.agentClient != nil {
		agentSubscriber, subErr := h.agentClient.GetOrCreateSubscriber(topic, partition, fetchOffset)
		if subErr != nil {
			return nil, fmt.Errorf("failed to get agent subscriber: %v", subErr)
		}
		seaweedRecords, err = h.agentClient.ReadRecords(agentSubscriber, recordsToFetch)
	} else {
		return nil, fmt.Errorf("no client available")
	}

	if err != nil {
		// If no records available, return empty batch instead of error
		return []byte{}, nil
	}

	// Map SeaweedMQ records to Kafka offsets and update ledger
	kafkaRecords, err := h.mapSeaweedToKafkaOffsets(topic, partition, seaweedRecords, fetchOffset)
	if err != nil {
		return nil, fmt.Errorf("failed to map offsets: %v", err)
	}

	// Convert mapped records to Kafka record batch format
	return h.convertSeaweedToKafkaRecordBatch(kafkaRecords, fetchOffset, maxBytes)
}

// constructKafkaRecordBatch creates a Kafka-compatible record batch
func (h *SeaweedMQHandler) constructKafkaRecordBatch(ledger *offset.Ledger, fetchOffset, highWaterMark int64, maxBytes int32) ([]byte, error) {
	recordsToFetch := highWaterMark - fetchOffset
	if recordsToFetch <= 0 {
		return []byte{}, nil
	}

	// Limit records to prevent overly large batches
	if recordsToFetch > 100 {
		recordsToFetch = 100
	}

	// For Phase 2, create a stub record batch with placeholder data
	// This represents what would come from SeaweedMQ subscriber
	batch := make([]byte, 0, 512)

	// Record batch header
	baseOffsetBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(baseOffsetBytes, uint64(fetchOffset))
	batch = append(batch, baseOffsetBytes...) // base offset

	// Batch length (placeholder, will be filled at end)
	batchLengthPos := len(batch)
	batch = append(batch, 0, 0, 0, 0)

	batch = append(batch, 0, 0, 0, 0) // partition leader epoch
	batch = append(batch, 2)          // magic byte (version 2)

	// CRC placeholder
	batch = append(batch, 0, 0, 0, 0)

	// Batch attributes
	batch = append(batch, 0, 0)

	// Last offset delta
	lastOffsetDelta := uint32(recordsToFetch - 1)
	lastOffsetDeltaBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(lastOffsetDeltaBytes, lastOffsetDelta)
	batch = append(batch, lastOffsetDeltaBytes...)

	// Timestamps
	currentTime := time.Now().UnixNano()
	firstTimestampBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(firstTimestampBytes, uint64(currentTime))
	batch = append(batch, firstTimestampBytes...)

	maxTimestamp := currentTime + recordsToFetch*1000000 // 1ms apart
	maxTimestampBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(maxTimestampBytes, uint64(maxTimestamp))
	batch = append(batch, maxTimestampBytes...)

	// Producer info (simplified)
	batch = append(batch, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF) // producer ID (-1)
	batch = append(batch, 0xFF, 0xFF)                                     // producer epoch (-1)
	batch = append(batch, 0xFF, 0xFF, 0xFF, 0xFF)                         // base sequence (-1)

	// Record count
	recordCountBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(recordCountBytes, uint32(recordsToFetch))
	batch = append(batch, recordCountBytes...)

	// Add simple records (placeholders representing SeaweedMQ data)
	for i := int64(0); i < recordsToFetch; i++ {
		record := h.constructSingleRecord(i, fetchOffset+i)
		recordLength := byte(len(record))
		batch = append(batch, recordLength)
		batch = append(batch, record...)
	}

	// Fill in the batch length
	batchLength := uint32(len(batch) - batchLengthPos - 4)
	binary.BigEndian.PutUint32(batch[batchLengthPos:batchLengthPos+4], batchLength)

	return batch, nil
}

// constructSingleRecord creates a single Kafka record
func (h *SeaweedMQHandler) constructSingleRecord(index, offset int64) []byte {
	record := make([]byte, 0, 64)

	// Record attributes
	record = append(record, 0)

	// Timestamp delta (varint - simplified)
	record = append(record, byte(index))

	// Offset delta (varint - simplified)
	record = append(record, byte(index))

	// Key length (-1 = null key)
	record = append(record, 0xFF)

	// Value (represents data that would come from SeaweedMQ)
	value := fmt.Sprintf("seaweedmq-message-%d", offset)
	record = append(record, byte(len(value)))
	record = append(record, []byte(value)...)

	// Headers count (0)
	record = append(record, 0)

	return record
}

// mapSeaweedToKafkaOffsets maps SeaweedMQ records to proper Kafka offsets
func (h *SeaweedMQHandler) mapSeaweedToKafkaOffsets(topic string, partition int32, seaweedRecords []*SeaweedRecord, startOffset int64) ([]*SeaweedRecord, error) {
	if len(seaweedRecords) == 0 {
		return seaweedRecords, nil
	}

	ledger := h.GetOrCreateLedger(topic, partition)
	mappedRecords := make([]*SeaweedRecord, 0, len(seaweedRecords))

	// Assign the required offsets first (this ensures offsets are reserved in sequence)
	// Note: In a real scenario, these offsets would have been assigned during produce
	// but for fetch operations we need to ensure the ledger state is consistent
	for i, seaweedRecord := range seaweedRecords {
		currentKafkaOffset := startOffset + int64(i)

		// Create a copy of the record with proper Kafka offset assignment
		mappedRecord := &SeaweedRecord{
			Key:       seaweedRecord.Key,
			Value:     seaweedRecord.Value,
			Timestamp: seaweedRecord.Timestamp,
			Sequence:  currentKafkaOffset, // Use Kafka offset as sequence for consistency
		}

		// Update the offset ledger to track the mapping between SeaweedMQ sequence and Kafka offset
		recordSize := int32(len(seaweedRecord.Value))
		if err := ledger.AppendRecord(currentKafkaOffset, seaweedRecord.Timestamp, recordSize); err != nil {
			// Log warning but continue processing
			fmt.Printf("Warning: failed to update offset ledger for topic %s partition %d offset %d: %v\n",
				topic, partition, currentKafkaOffset, err)
		}

		mappedRecords = append(mappedRecords, mappedRecord)
	}

	return mappedRecords, nil
}

// convertSeaweedToKafkaRecordBatch converts SeaweedMQ records to Kafka record batch format
func (h *SeaweedMQHandler) convertSeaweedToKafkaRecordBatch(seaweedRecords []*SeaweedRecord, fetchOffset int64, maxBytes int32) ([]byte, error) {
	if len(seaweedRecords) == 0 {
		return []byte{}, nil
	}

	batch := make([]byte, 0, 512)

	// Record batch header
	baseOffsetBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(baseOffsetBytes, uint64(fetchOffset))
	batch = append(batch, baseOffsetBytes...) // base offset

	// Batch length (placeholder, will be filled at end)
	batchLengthPos := len(batch)
	batch = append(batch, 0, 0, 0, 0)

	batch = append(batch, 0, 0, 0, 0) // partition leader epoch
	batch = append(batch, 2)          // magic byte (version 2)

	// CRC placeholder
	batch = append(batch, 0, 0, 0, 0)

	// Batch attributes
	batch = append(batch, 0, 0)

	// Last offset delta
	lastOffsetDelta := uint32(len(seaweedRecords) - 1)
	lastOffsetDeltaBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(lastOffsetDeltaBytes, lastOffsetDelta)
	batch = append(batch, lastOffsetDeltaBytes...)

	// Timestamps - use actual timestamps from SeaweedMQ records
	var firstTimestamp, maxTimestamp int64
	if len(seaweedRecords) > 0 {
		firstTimestamp = seaweedRecords[0].Timestamp
		maxTimestamp = firstTimestamp
		for _, record := range seaweedRecords {
			if record.Timestamp > maxTimestamp {
				maxTimestamp = record.Timestamp
			}
		}
	}

	firstTimestampBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(firstTimestampBytes, uint64(firstTimestamp))
	batch = append(batch, firstTimestampBytes...)

	maxTimestampBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(maxTimestampBytes, uint64(maxTimestamp))
	batch = append(batch, maxTimestampBytes...)

	// Producer info (simplified)
	batch = append(batch, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF) // producer ID (-1)
	batch = append(batch, 0xFF, 0xFF)                                     // producer epoch (-1)
	batch = append(batch, 0xFF, 0xFF, 0xFF, 0xFF)                         // base sequence (-1)

	// Record count
	recordCountBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(recordCountBytes, uint32(len(seaweedRecords)))
	batch = append(batch, recordCountBytes...)

	// Add actual records from SeaweedMQ
	for i, seaweedRecord := range seaweedRecords {
		record := h.convertSingleSeaweedRecord(seaweedRecord, int64(i), fetchOffset)
		recordLength := byte(len(record))
		batch = append(batch, recordLength)
		batch = append(batch, record...)

		// Check if we're approaching maxBytes limit
		if int32(len(batch)) > maxBytes*3/4 {
			// Leave room for remaining headers and stop adding records
			break
		}
	}

	// Fill in the batch length
	batchLength := uint32(len(batch) - batchLengthPos - 4)
	binary.BigEndian.PutUint32(batch[batchLengthPos:batchLengthPos+4], batchLength)

	return batch, nil
}

// convertSingleSeaweedRecord converts a single SeaweedMQ record to Kafka format
func (h *SeaweedMQHandler) convertSingleSeaweedRecord(seaweedRecord *SeaweedRecord, index, baseOffset int64) []byte {
	record := make([]byte, 0, 64)

	// Record attributes
	record = append(record, 0)

	// Timestamp delta (varint - simplified)
	timestampDelta := seaweedRecord.Timestamp - baseOffset // Simple delta calculation
	if timestampDelta < 0 {
		timestampDelta = 0
	}
	record = append(record, byte(timestampDelta&0xFF)) // Simplified varint encoding

	// Offset delta (varint - simplified)
	record = append(record, byte(index))

	// Key length and key
	if len(seaweedRecord.Key) > 0 {
		record = append(record, byte(len(seaweedRecord.Key)))
		record = append(record, seaweedRecord.Key...)
	} else {
		// Null key
		record = append(record, 0xFF)
	}

	// Value length and value
	if len(seaweedRecord.Value) > 0 {
		record = append(record, byte(len(seaweedRecord.Value)))
		record = append(record, seaweedRecord.Value...)
	} else {
		// Empty value
		record = append(record, 0)
	}

	// Headers count (0)
	record = append(record, 0)

	return record
}

// NewSeaweedMQBrokerHandler creates a new handler with SeaweedMQ broker integration
func NewSeaweedMQBrokerHandler(masters string, filerGroup string) (*SeaweedMQHandler, error) {
	if masters == "" {
		return nil, fmt.Errorf("masters required - SeaweedMQ infrastructure must be configured")
	}

	// Parse master addresses
	masterAddresses := strings.Split(masters, ",")
	if len(masterAddresses) == 0 {
		return nil, fmt.Errorf("no master addresses provided")
	}

	// Discover brokers from masters
	brokerAddresses, err := discoverBrokers(masterAddresses, filerGroup)
	if err != nil {
		return nil, fmt.Errorf("failed to discover brokers: %v", err)
	}

	if len(brokerAddresses) == 0 {
		return nil, fmt.Errorf("no brokers discovered from masters")
	}

	// For now, use the first broker (can be enhanced later for load balancing)
	brokerAddress := brokerAddresses[0]

	// Create broker client (reuse AgentClient structure but connect to broker)
	brokerClient, err := NewBrokerClient(brokerAddress)
	if err != nil {
		return nil, fmt.Errorf("failed to create broker client: %v", err)
	}

	// Test the connection
	if err := brokerClient.HealthCheck(); err != nil {
		brokerClient.Close()
		return nil, fmt.Errorf("broker health check failed: %v", err)
	}

	return &SeaweedMQHandler{
		brokerClient: brokerClient,
		useBroker:    true,
		topics:       make(map[string]*KafkaTopicInfo),
		ledgers:      make(map[TopicPartitionKey]*offset.Ledger),
	}, nil
}

// discoverBrokers queries masters for available brokers
func discoverBrokers(masterAddresses []string, filerGroup string) ([]string, error) {
	var brokers []string

	// Try each master until we get a response
	for _, masterAddr := range masterAddresses {
		conn, err := grpc.Dial(masterAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			continue // Try next master
		}
		defer conn.Close()

		client := master_pb.NewSeaweedClient(conn)

		resp, err := client.ListClusterNodes(context.Background(), &master_pb.ListClusterNodesRequest{
			ClientType: cluster.BrokerType,
			FilerGroup: filerGroup,
			Limit:      100,
		})
		if err != nil {
			continue // Try next master
		}

		// Extract broker addresses from response
		for _, node := range resp.ClusterNodes {
			if node.Address != "" {
				brokers = append(brokers, node.Address)
			}
		}

		if len(brokers) > 0 {
			break // Found brokers, no need to try other masters
		}
	}

	return brokers, nil
}

// BrokerClient wraps the SeaweedMQ Broker gRPC client for Kafka gateway integration
type BrokerClient struct {
	brokerAddress string
	conn          *grpc.ClientConn
	client        mq_pb.SeaweedMessagingClient

	// Publisher streams: topic-partition -> stream info
	publishersLock sync.RWMutex
	publishers     map[string]*BrokerPublisherSession

	// Subscriber streams for offset tracking
	subscribersLock sync.RWMutex
	subscribers     map[string]*BrokerSubscriberSession

	ctx    context.Context
	cancel context.CancelFunc
}

// BrokerPublisherSession tracks a publishing stream to SeaweedMQ broker
type BrokerPublisherSession struct {
	Topic     string
	Partition int32
	Stream    mq_pb.SeaweedMessaging_PublishMessageClient
}

// BrokerSubscriberSession tracks a subscription stream for offset management
type BrokerSubscriberSession struct {
	Topic     string
	Partition int32
	Stream    mq_pb.SeaweedMessaging_SubscribeMessageClient
}

// NewBrokerClient creates a client that connects to a SeaweedMQ broker
func NewBrokerClient(brokerAddress string) (*BrokerClient, error) {
	ctx, cancel := context.WithCancel(context.Background())

	conn, err := grpc.DialContext(ctx, brokerAddress,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		cancel()
		return nil, fmt.Errorf("failed to connect to broker %s: %v", brokerAddress, err)
	}

	client := mq_pb.NewSeaweedMessagingClient(conn)

	return &BrokerClient{
		brokerAddress: brokerAddress,
		conn:          conn,
		client:        client,
		publishers:    make(map[string]*BrokerPublisherSession),
		subscribers:   make(map[string]*BrokerSubscriberSession),
		ctx:           ctx,
		cancel:        cancel,
	}, nil
}

// Close shuts down the broker client and all streams
func (bc *BrokerClient) Close() error {
	bc.cancel()

	// Close all publisher streams
	bc.publishersLock.Lock()
	for key := range bc.publishers {
		delete(bc.publishers, key)
	}
	bc.publishersLock.Unlock()

	// Close all subscriber streams
	bc.subscribersLock.Lock()
	for key := range bc.subscribers {
		delete(bc.subscribers, key)
	}
	bc.subscribersLock.Unlock()

	return bc.conn.Close()
}

// PublishRecord publishes a single record to SeaweedMQ broker
func (bc *BrokerClient) PublishRecord(topic string, partition int32, key []byte, value []byte, timestamp int64) (int64, error) {
	session, err := bc.getOrCreatePublisher(topic, partition)
	if err != nil {
		return 0, err
	}

	// Send data message using broker API format
	dataMsg := &mq_pb.DataMessage{
		Key:   key,
		Value: value,
		TsNs:  timestamp,
	}

	if err := session.Stream.Send(&mq_pb.PublishMessageRequest{
		Message: &mq_pb.PublishMessageRequest_Data{
			Data: dataMsg,
		},
	}); err != nil {
		return 0, fmt.Errorf("failed to send data: %v", err)
	}

	// Read acknowledgment
	resp, err := session.Stream.Recv()
	if err != nil {
		return 0, fmt.Errorf("failed to receive ack: %v", err)
	}

	if resp.Error != "" {
		return 0, fmt.Errorf("publish error: %s", resp.Error)
	}

	return resp.AckSequence, nil
}

// getOrCreatePublisher gets or creates a publisher stream for a topic-partition
func (bc *BrokerClient) getOrCreatePublisher(topic string, partition int32) (*BrokerPublisherSession, error) {
	key := fmt.Sprintf("%s-%d", topic, partition)

	// Try to get existing publisher
	bc.publishersLock.RLock()
	if session, exists := bc.publishers[key]; exists {
		bc.publishersLock.RUnlock()
		return session, nil
	}
	bc.publishersLock.RUnlock()

	// Create new publisher stream
	bc.publishersLock.Lock()
	defer bc.publishersLock.Unlock()

	// Double-check after acquiring write lock
	if session, exists := bc.publishers[key]; exists {
		return session, nil
	}

	// Create the stream
	stream, err := bc.client.PublishMessage(bc.ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to create publish stream: %v", err)
	}

	// Send init message
	if err := stream.Send(&mq_pb.PublishMessageRequest{
		Message: &mq_pb.PublishMessageRequest_Init{
			Init: &mq_pb.PublishMessageRequest_InitMessage{
				Topic: &schema_pb.Topic{
					Namespace: "kafka",
					Name:      topic,
				},
				Partition: &schema_pb.Partition{
					RingSize:   pub_balancer.MaxPartitionCount, // Use the standard ring size constant
					RangeStart: partition * 32,
					RangeStop:  (partition+1)*32 - 1,
				},
				AckInterval:   100,
				PublisherName: "kafka-gateway",
			},
		},
	}); err != nil {
		return nil, fmt.Errorf("failed to send init message: %v", err)
	}

	session := &BrokerPublisherSession{
		Topic:     topic,
		Partition: partition,
		Stream:    stream,
	}

	bc.publishers[key] = session
	return session, nil
}

// GetOrCreateSubscriber gets or creates a subscriber for offset tracking
func (bc *BrokerClient) GetOrCreateSubscriber(topic string, partition int32, startOffset int64) (*BrokerSubscriberSession, error) {
	key := fmt.Sprintf("%s-%d", topic, partition)

	bc.subscribersLock.RLock()
	if session, exists := bc.subscribers[key]; exists {
		bc.subscribersLock.RUnlock()
		return session, nil
	}
	bc.subscribersLock.RUnlock()

	// Create new subscriber stream
	bc.subscribersLock.Lock()
	defer bc.subscribersLock.Unlock()

	if session, exists := bc.subscribers[key]; exists {
		return session, nil
	}

	stream, err := bc.client.SubscribeMessage(bc.ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to create subscribe stream: %v", err)
	}

	// Send init message
	if err := stream.Send(&mq_pb.SubscribeMessageRequest{
		Message: &mq_pb.SubscribeMessageRequest_Init{
			Init: &mq_pb.SubscribeMessageRequest_InitMessage{
				ConsumerGroup: "kafka-gateway",
				ConsumerId:    fmt.Sprintf("kafka-gateway-%s-%d", topic, partition),
				ClientId:      "kafka-gateway",
				Topic: &schema_pb.Topic{
					Namespace: "kafka",
					Name:      topic,
				},
				PartitionOffset: &schema_pb.PartitionOffset{
					Partition: &schema_pb.Partition{
						RingSize:   pub_balancer.MaxPartitionCount,
						RangeStart: partition * 32,
						RangeStop:  (partition+1)*32 - 1,
					},
					StartTsNs: startOffset,
				},
				OffsetType:        schema_pb.OffsetType_EXACT_TS_NS,
				SlidingWindowSize: 10,
			},
		},
	}); err != nil {
		return nil, fmt.Errorf("failed to send subscribe init: %v", err)
	}

	session := &BrokerSubscriberSession{
		Topic:     topic,
		Partition: partition,
		Stream:    stream,
	}

	bc.subscribers[key] = session
	return session, nil
}

// ReadRecords reads available records from the subscriber stream
func (bc *BrokerClient) ReadRecords(session *BrokerSubscriberSession, maxRecords int) ([]*SeaweedRecord, error) {
	if session == nil {
		return nil, fmt.Errorf("subscriber session cannot be nil")
	}

	var records []*SeaweedRecord

	for len(records) < maxRecords {
		// Try to receive a message with timeout
		ctx, cancel := context.WithTimeout(bc.ctx, 100*time.Millisecond)

		select {
		case <-ctx.Done():
			cancel()
			return records, nil // Return what we have so far
		default:
			resp, err := session.Stream.Recv()
			cancel()

			if err != nil {
				// If we have some records, return them; otherwise return error
				if len(records) > 0 {
					return records, nil
				}
				return nil, fmt.Errorf("failed to receive record: %v", err)
			}

			if dataMsg := resp.GetData(); dataMsg != nil {
				record := &SeaweedRecord{
					Key:       dataMsg.Key,
					Value:     dataMsg.Value,
					Timestamp: dataMsg.TsNs,
					Sequence:  0, // Will be set by offset ledger
				}
				records = append(records, record)
			}
		}
	}

	return records, nil
}

// HealthCheck verifies the broker connection is working
func (bc *BrokerClient) HealthCheck() error {
	// Create a timeout context for health check
	ctx, cancel := context.WithTimeout(bc.ctx, 2*time.Second)
	defer cancel()

	// Try to list topics as a health check
	_, err := bc.client.ListTopics(ctx, &mq_pb.ListTopicsRequest{})
	if err != nil {
		return fmt.Errorf("broker health check failed: %v", err)
	}

	return nil
}

// ClosePublisher closes a specific publisher session
func (bc *BrokerClient) ClosePublisher(topic string, partition int32) error {
	key := fmt.Sprintf("%s-%d", topic, partition)

	bc.publishersLock.Lock()
	defer bc.publishersLock.Unlock()

	session, exists := bc.publishers[key]
	if !exists {
		return nil // Already closed or never existed
	}

	if session.Stream != nil {
		session.Stream.CloseSend()
	}
	delete(bc.publishers, key)
	return nil
}
