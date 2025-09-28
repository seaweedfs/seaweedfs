package integration

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"sync"
	"time"

	"google.golang.org/grpc"

	"github.com/seaweedfs/seaweedfs/weed/cluster"
	"github.com/seaweedfs/seaweedfs/weed/filer_client"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/mq/kafka/offset"
	"github.com/seaweedfs/seaweedfs/weed/mq/schema"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/mq_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/schema_pb"
	"github.com/seaweedfs/seaweedfs/weed/security"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"github.com/seaweedfs/seaweedfs/weed/wdclient"
)

// SeaweedMQHandler integrates Kafka protocol handlers with real SeaweedMQ storage
type SeaweedMQHandler struct {
	// Shared filer client accessor for all components
	filerClientAccessor *filer_client.FilerClientAccessor

	brokerClient *BrokerClient // For broker-based connections

	// Master client for service discovery
	masterClient *wdclient.MasterClient

	// Discovered broker addresses (for Metadata responses)
	brokerAddresses []string

	// Topic registry removed - always read directly from filer for consistency
	// topicsMu sync.RWMutex  // No longer needed
	// topics   map[string]*KafkaTopicInfo  // No longer needed

	// Offset ledgers REMOVED - SMQ handles Kafka offsets natively
	// No need for external offset tracking when SMQ stores real Kafka offsets
}

// Ledger methods REMOVED - SMQ handles Kafka offsets natively

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

// SeaweedRecord represents a record received from SeaweedMQ
type SeaweedRecord struct {
	Key       []byte
	Value     []byte
	Timestamp int64
	Sequence  int64 // SeaweedMQ sequence number
	Offset    int64 // Kafka offset (real offset from SMQ)
}

// GetStoredRecords retrieves records from SeaweedMQ storage using DIRECT LOG READING
// This eliminates the need for offset ledgers by using SMQ's native offset storage
func (h *SeaweedMQHandler) GetStoredRecords(topic string, partition int32, fromOffset int64, maxRecords int) ([]offset.SMQRecord, error) {
	glog.V(1).Infof("[DEBUG_FETCH] GetStoredRecords: topic=%s partition=%d fromOffset=%d maxRecords=%d", topic, partition, fromOffset, maxRecords)

	// Verify topic exists
	if !h.TopicExists(topic) {
		glog.V(1).Infof("[DEBUG_FETCH] Topic %s does not exist", topic)
		return nil, fmt.Errorf("topic %s does not exist", topic)
	}

	// Use SMQ's direct log reading API to get records with REAL Kafka offsets
	// This bypasses the streaming API that loses offset information
	smqRecords, err := h.readRecordsDirectlyFromSMQ(topic, partition, fromOffset, maxRecords)
	if err != nil {
		return nil, fmt.Errorf("failed to read records directly from SMQ: %v", err)
	}

	glog.V(1).Infof("[DEBUG_FETCH] Successfully read %d records directly from SMQ", len(smqRecords))
	return smqRecords, nil
}

// readRecordsDirectlyFromSMQ reads records directly from SMQ using the log reading API
// This preserves the real Kafka offsets stored in LogEntry.Offset, eliminating the need for ledgers
func (h *SeaweedMQHandler) readRecordsDirectlyFromSMQ(topicName string, partitionID int32, fromOffset int64, maxRecords int) ([]offset.SMQRecord, error) {
	glog.V(1).Infof("[DEBUG_FETCH] readRecordsDirectlyFromSMQ: topic=%s partition=%d fromOffset=%d maxRecords=%d", topicName, partitionID, fromOffset, maxRecords)

	// Create SMQ topic structure
	_ = topicName // We'll use this in the implementation

	// Get partition information from broker client
	if h.brokerClient == nil {
		return nil, fmt.Errorf("no broker client available")
	}

	// Use the broker client to get the LocalPartition directly
	// This bypasses the streaming API and gives us access to the log reading API with real offsets
	smqRecords, err := h.brokerClient.ReadRecordsWithOffsets(topicName, partitionID, fromOffset, maxRecords)
	if err != nil {
		return nil, fmt.Errorf("failed to read records with offsets: %v", err)
	}

	glog.V(1).Infof("[DEBUG_FETCH] Successfully read %d records with real offsets from SMQ", len(smqRecords))
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

// WithFilerClient executes a function with a filer client
func (h *SeaweedMQHandler) WithFilerClient(streamingMode bool, fn func(client filer_pb.SeaweedFilerClient) error) error {
	if h.brokerClient == nil {
		return fmt.Errorf("no broker client available")
	}
	return h.brokerClient.WithFilerClient(streamingMode, fn)
}

// GetFilerAddress returns the filer address used by this handler
func (h *SeaweedMQHandler) GetFilerAddress() string {
	if h.brokerClient != nil {
		return h.brokerClient.GetFilerAddress()
	}
	return ""
}

// GetBrokerAddresses returns the discovered SMQ broker addresses
func (h *SeaweedMQHandler) GetBrokerAddresses() []string {
	return h.brokerAddresses
}

// Close shuts down the handler and all connections
func (h *SeaweedMQHandler) Close() error {
	if h.brokerClient != nil {
		return h.brokerClient.Close()
	}
	return nil
}

// CreateTopic creates a new topic in both Kafka registry and SeaweedMQ
func (h *SeaweedMQHandler) CreateTopic(name string, partitions int32) error {
	return h.CreateTopicWithSchema(name, partitions, nil)
}

// CreateTopicWithSchema creates a topic with optional value schema
func (h *SeaweedMQHandler) CreateTopicWithSchema(name string, partitions int32, recordType *schema_pb.RecordType) error {
	return h.CreateTopicWithSchemas(name, partitions, recordType, nil)
}

// CreateTopicWithSchemas creates a topic with optional key and value schemas
func (h *SeaweedMQHandler) CreateTopicWithSchemas(name string, partitions int32, valueRecordType *schema_pb.RecordType, keyRecordType *schema_pb.RecordType) error {
	// Check if topic already exists in filer
	if h.checkTopicInFiler(name) {
		return fmt.Errorf("topic %s already exists", name)
	}

	// Create SeaweedMQ topic reference
	seaweedTopic := &schema_pb.Topic{
		Namespace: "kafka",
		Name:      name,
	}

	glog.V(1).Infof("Creating topic %s with %d partitions in SeaweedMQ broker", name, partitions)

	// Configure topic with SeaweedMQ broker via gRPC
	if len(h.brokerAddresses) > 0 {
		brokerAddress := h.brokerAddresses[0] // Use first available broker
		glog.V(1).Infof("Configuring topic %s with broker %s", name, brokerAddress)

		// Load security configuration for broker connection
		util.LoadSecurityConfiguration()
		grpcDialOption := security.LoadClientTLS(util.GetViper(), "grpc.mq")

		err := pb.WithBrokerGrpcClient(false, brokerAddress, grpcDialOption, func(client mq_pb.SeaweedMessagingClient) error {
			// Convert dual schemas to flat schema format
			var flatSchema *schema_pb.RecordType
			var keyColumns []string
			if keyRecordType != nil || valueRecordType != nil {
				flatSchema, keyColumns = schema.CombineFlatSchemaFromKeyValue(keyRecordType, valueRecordType)
			}

			_, err := client.ConfigureTopic(context.Background(), &mq_pb.ConfigureTopicRequest{
				Topic:             seaweedTopic,
				PartitionCount:    partitions,
				MessageRecordType: flatSchema,
				KeyColumns:        keyColumns,
			})
			if err != nil {
				glog.Errorf("❌ Failed to configure topic %s with broker: %v", name, err)
				return fmt.Errorf("configure topic with broker: %w", err)
			}
			glog.V(1).Infof("✅ Successfully configured topic %s with broker", name)
			return nil
		})
		if err != nil {
			return fmt.Errorf("failed to configure topic %s with broker %s: %w", name, brokerAddress, err)
		}
	} else {
		glog.Warningf("No brokers available - creating topic %s in gateway memory only (testing mode)", name)
	}

	// Topic is now stored in filer only via SeaweedMQ broker
	// No need to create in-memory topic info structure

	// Ledger initialization REMOVED - SMQ handles offsets natively

	glog.V(1).Infof("Topic %s created successfully with %d partitions", name, partitions)
	return nil
}

// CreateTopicWithRecordType creates a topic with flat schema and key columns
func (h *SeaweedMQHandler) CreateTopicWithRecordType(name string, partitions int32, flatSchema *schema_pb.RecordType, keyColumns []string) error {
	// Check if topic already exists in filer
	if h.checkTopicInFiler(name) {
		return fmt.Errorf("topic %s already exists", name)
	}

	// Create SeaweedMQ topic reference
	seaweedTopic := &schema_pb.Topic{
		Namespace: "kafka",
		Name:      name,
	}

	glog.V(1).Infof("Creating topic %s with %d partitions in SeaweedMQ broker using flat schema", name, partitions)

	// Configure topic with SeaweedMQ broker via gRPC
	if len(h.brokerAddresses) > 0 {
		brokerAddress := h.brokerAddresses[0] // Use first available broker
		glog.V(1).Infof("Configuring topic %s with broker %s", name, brokerAddress)

		// Load security configuration for broker connection
		util.LoadSecurityConfiguration()
		grpcDialOption := security.LoadClientTLS(util.GetViper(), "grpc.mq")

		err := pb.WithBrokerGrpcClient(false, brokerAddress, grpcDialOption, func(client mq_pb.SeaweedMessagingClient) error {
			_, err := client.ConfigureTopic(context.Background(), &mq_pb.ConfigureTopicRequest{
				Topic:             seaweedTopic,
				PartitionCount:    partitions,
				MessageRecordType: flatSchema,
				KeyColumns:        keyColumns,
			})
			if err != nil {
				glog.Errorf("❌ Failed to configure topic %s with broker: %v", name, err)
				return fmt.Errorf("failed to configure topic: %w", err)
			}

			glog.V(1).Infof("✅ Successfully configured topic %s with broker", name)
			return nil
		})

		if err != nil {
			return err
		}
	} else {
		glog.Warningf("No broker addresses configured, topic %s not created in SeaweedMQ", name)
	}

	// Topic is now stored in filer only via SeaweedMQ broker
	// No need to create in-memory topic info structure

	glog.V(1).Infof("Topic %s created successfully with %d partitions using flat schema", name, partitions)
	return nil
}

// DeleteTopic removes a topic from both Kafka registry and SeaweedMQ
func (h *SeaweedMQHandler) DeleteTopic(name string) error {
	// Check if topic exists in filer
	if !h.checkTopicInFiler(name) {
		return fmt.Errorf("topic %s does not exist", name)
	}

	// Get topic info to determine partition count for cleanup
	topicInfo, exists := h.GetTopicInfo(name)
	if !exists {
		return fmt.Errorf("topic %s info not found", name)
	}

	// Close all publisher sessions for this topic
	for partitionID := int32(0); partitionID < topicInfo.Partitions; partitionID++ {
		if h.brokerClient != nil {
			h.brokerClient.ClosePublisher(name, partitionID)
		}
	}

	// Topic removal from filer would be handled by SeaweedMQ broker
	// No in-memory cache to clean up

	// Ledger cleanup REMOVED - SMQ handles offsets natively

	return nil
}

// TopicExists checks if a topic exists in filer directly
func (h *SeaweedMQHandler) TopicExists(name string) bool {
	// Always check filer directly for consistency
	result := h.checkTopicInFiler(name)
	glog.V(1).Infof("TopicExists: topic=%s, result=%v", name, result)
	return result
}

// GetTopicInfo returns information about a topic from filer
func (h *SeaweedMQHandler) GetTopicInfo(name string) (*KafkaTopicInfo, bool) {
	// Check if topic exists in filer
	if !h.checkTopicInFiler(name) {
		return nil, false
	}

	// Create basic topic info - in a real implementation, this could read
	// topic configuration from filer metadata
	topicInfo := &KafkaTopicInfo{
		Name:       name,
		Partitions: 1, // Default to 1 partition
		CreatedAt:  0, // Could be read from filer metadata
	}

	return topicInfo, true
}

// ListTopics returns all topic names from filer directly
func (h *SeaweedMQHandler) ListTopics() []string {
	// Always read directly from filer for consistency
	topics := h.listTopicsFromFiler()
	fmt.Printf("ListTopics: Found %d topics from filer: %v\n", len(topics), topics)
	return topics
}

// ProduceRecord publishes a record to SeaweedMQ
// SMQ handles Kafka offset assignment natively - no need for external ledgers
func (h *SeaweedMQHandler) ProduceRecord(topic string, partition int32, key []byte, value []byte) (int64, error) {
	// Verify topic exists
	if !h.TopicExists(topic) {
		return 0, fmt.Errorf("topic %s does not exist", topic)
	}

	// Get current timestamp
	timestamp := time.Now().UnixNano()

	// Publish to SeaweedMQ - SMQ will assign and return the Kafka offset
	if h.brokerClient == nil {
		return 0, fmt.Errorf("no broker client available")
	}

	kafkaOffset, err := h.brokerClient.PublishRecord(topic, partition, key, value, timestamp)
	if err != nil {
		return 0, fmt.Errorf("failed to publish to SeaweedMQ: %v", err)
	}

	glog.V(1).Infof("[DEBUG_PRODUCE] Successfully produced record to topic %s partition %d at offset %d", topic, partition, kafkaOffset)
	return kafkaOffset, nil
}

// ProduceRecordValue produces a record using RecordValue format to SeaweedMQ
// SMQ handles Kafka offset assignment natively - no need for external ledgers
func (h *SeaweedMQHandler) ProduceRecordValue(topic string, partition int32, key []byte, recordValueBytes []byte) (int64, error) {
	// Verify topic exists
	if !h.TopicExists(topic) {
		return 0, fmt.Errorf("topic %s does not exist", topic)
	}

	// Get current timestamp
	timestamp := time.Now().UnixNano()

	// Publish RecordValue to SeaweedMQ - SMQ will assign and return the Kafka offset
	if h.brokerClient == nil {
		return 0, fmt.Errorf("no broker client available")
	}

	kafkaOffset, err := h.brokerClient.PublishRecordValue(topic, partition, key, recordValueBytes, timestamp)
	if err != nil {
		return 0, fmt.Errorf("failed to publish RecordValue to SeaweedMQ: %v", err)
	}

	glog.V(1).Infof("[DEBUG_PRODUCE] Successfully produced RecordValue to topic %s partition %d at offset %d", topic, partition, kafkaOffset)
	return kafkaOffset, nil
}

// Ledger methods REMOVED - SMQ handles Kafka offsets natively
// GetOrCreateLedger and GetLedger are no longer needed

// FetchRecords retrieves records from SeaweedMQ for a Kafka fetch request
// Uses SMQ's native offset storage - no external ledgers needed
func (h *SeaweedMQHandler) FetchRecords(topic string, partition int32, fetchOffset int64, maxBytes int32) ([]byte, error) {
	// Verify topic exists
	if !h.TopicExists(topic) {
		return nil, fmt.Errorf("topic %s does not exist", topic)
	}

	// Use direct SMQ reading to get records with real Kafka offsets
	maxRecords := 100 // Reasonable batch size limit
	smqRecords, err := h.readRecordsDirectlyFromSMQ(topic, partition, fetchOffset, maxRecords)
	if err != nil {
		return nil, fmt.Errorf("failed to read records from SMQ: %v", err)
	}

	// If no records available, return empty record batch
	if len(smqRecords) == 0 {
		return []byte{}, nil
	}

	// Convert SMQ records (which already have real Kafka offsets) to Kafka record batch format
	return h.convertSMQRecordsToKafkaRecordBatch(smqRecords, maxBytes)
}

// convertSMQRecordsToKafkaRecordBatch converts SMQ records to Kafka record batch format
// SMQ records already contain real Kafka offsets, so no mapping is needed
func (h *SeaweedMQHandler) convertSMQRecordsToKafkaRecordBatch(smqRecords []offset.SMQRecord, maxBytes int32) ([]byte, error) {
	if len(smqRecords) == 0 {
		return []byte{}, nil
	}

	// Convert SMQ records to the format expected by existing conversion logic
	seaweedRecords := make([]*SeaweedRecord, len(smqRecords))
	for i, smqRecord := range smqRecords {
		seaweedRecords[i] = &SeaweedRecord{
			Key:       smqRecord.GetKey(),
			Value:     smqRecord.GetValue(),
			Timestamp: smqRecord.GetTimestamp(),
			Offset:    smqRecord.GetOffset(), // Real Kafka offset from SMQ
		}
	}

	// Use existing conversion logic but with real offsets
	firstOffset := seaweedRecords[0].Offset
	return h.convertSeaweedToKafkaRecordBatch(seaweedRecords, firstOffset, maxBytes)
}

// Old mapping methods REMOVED - SMQ provides real Kafka offsets directly
// No need for external offset mapping when SMQ stores the real offsets

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

	// Timestamps - convert from SeaweedMQ nanoseconds to Kafka milliseconds
	var firstTimestamp, maxTimestamp int64
	if len(seaweedRecords) > 0 {
		// Convert nanoseconds to milliseconds for Kafka compatibility
		firstTimestamp = seaweedRecords[0].Timestamp / 1000000
		maxTimestamp = firstTimestamp
		for _, record := range seaweedRecords {
			recordTimestampMs := record.Timestamp / 1000000
			if recordTimestampMs > maxTimestamp {
				maxTimestamp = recordTimestampMs
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
		record := h.convertSingleSeaweedRecord(seaweedRecord, int64(i), firstTimestamp)
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
func (h *SeaweedMQHandler) convertSingleSeaweedRecord(seaweedRecord *SeaweedRecord, index, baseTimestampMs int64) []byte {
	record := make([]byte, 0, 64)

	// Record attributes
	record = append(record, 0)

	// Timestamp delta (varint - simplified) - convert nanoseconds to milliseconds and calculate delta
	recordTimestampMs := seaweedRecord.Timestamp / 1000000 // Convert nanoseconds to milliseconds
	timestampDelta := recordTimestampMs - baseTimestampMs  // Calculate delta from base timestamp (both in ms)
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
func NewSeaweedMQBrokerHandler(masters string, filerGroup string, clientHost string) (*SeaweedMQHandler, error) {
	if masters == "" {
		return nil, fmt.Errorf("masters required - SeaweedMQ infrastructure must be configured")
	}

	// Parse master addresses using SeaweedFS utilities
	masterServerAddresses := pb.ServerAddresses(masters).ToAddresses()
	if len(masterServerAddresses) == 0 {
		return nil, fmt.Errorf("no valid master addresses provided")
	}

	// Load security configuration for gRPC connections
	util.LoadSecurityConfiguration()
	grpcDialOption := security.LoadClientTLS(util.GetViper(), "grpc.mq")
	masterDiscovery := pb.ServerAddresses(masters).ToServiceDiscovery()

	// Use provided client host for proper gRPC connection
	// This is critical for MasterClient to establish streaming connections
	clientHostAddr := pb.ServerAddress(clientHost)

	masterClient := wdclient.NewMasterClient(grpcDialOption, filerGroup, "kafka-gateway", clientHostAddr, "", "", *masterDiscovery)

	glog.V(1).Infof("🚀 Created MasterClient with clientHost=%s, masters=%s", clientHost, masters)

	// Start KeepConnectedToMaster in background to maintain connection
	glog.V(1).Infof("🔄 Starting KeepConnectedToMaster background goroutine...")
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		defer cancel()
		masterClient.KeepConnectedToMaster(ctx)
	}()

	// Give the connection a moment to establish
	time.Sleep(2 * time.Second)
	glog.V(1).Infof("Initial connection delay completed")

	// Discover brokers from masters using master client
	glog.V(1).Infof("About to call discoverBrokersWithMasterClient...")
	brokerAddresses, err := discoverBrokersWithMasterClient(masterClient, filerGroup)
	if err != nil {
		glog.Errorf("Broker discovery failed: %v", err)
		return nil, fmt.Errorf("failed to discover brokers: %v", err)
	}
	glog.V(1).Infof("Broker discovery returned: %v", brokerAddresses)

	if len(brokerAddresses) == 0 {
		return nil, fmt.Errorf("no brokers discovered from masters")
	}

	// Discover filers from masters using master client
	filerAddresses, err := discoverFilersWithMasterClient(masterClient, filerGroup)
	if err != nil {
		return nil, fmt.Errorf("failed to discover filers: %v", err)
	}

	// Create shared filer client accessor for all components
	sharedFilerAccessor := filer_client.NewFilerClientAccessor(
		filerAddresses,
		grpcDialOption,
	)

	// For now, use the first broker (can be enhanced later for load balancing)
	brokerAddress := brokerAddresses[0]

	// Create broker client with shared filer accessor
	brokerClient, err := NewBrokerClientWithFilerAccessor(brokerAddress, sharedFilerAccessor)
	if err != nil {
		return nil, fmt.Errorf("failed to create broker client: %v", err)
	}

	// Test the connection
	if err := brokerClient.HealthCheck(); err != nil {
		brokerClient.Close()
		return nil, fmt.Errorf("broker health check failed: %v", err)
	}

	return &SeaweedMQHandler{
		filerClientAccessor: sharedFilerAccessor,
		brokerClient:        brokerClient,
		masterClient:        masterClient,
		brokerAddresses:     brokerAddresses, // Store all discovered broker addresses
		// Ledger initialization REMOVED - SMQ handles Kafka offsets natively
	}, nil
}

// discoverBrokersWithMasterClient queries masters for available brokers using reusable master client
func discoverBrokersWithMasterClient(masterClient *wdclient.MasterClient, filerGroup string) ([]string, error) {
	var brokers []string

	glog.V(1).Infof("Starting broker discovery with MasterClient for filer group: %q", filerGroup)

	err := masterClient.WithClient(false, func(client master_pb.SeaweedClient) error {
		glog.V(1).Infof("Inside MasterClient.WithClient callback - client obtained successfully")
		resp, err := client.ListClusterNodes(context.Background(), &master_pb.ListClusterNodesRequest{
			ClientType: cluster.BrokerType,
			FilerGroup: filerGroup,
			Limit:      1000,
		})
		if err != nil {
			glog.Errorf("❌ ListClusterNodes gRPC call failed: %v", err)
			return err
		}

		glog.V(1).Infof("✅ ListClusterNodes successful - found %d cluster nodes", len(resp.ClusterNodes))

		// Extract broker addresses from response
		for _, node := range resp.ClusterNodes {
			if node.Address != "" {
				brokers = append(brokers, node.Address)
				glog.V(1).Infof("🌐 Discovered broker: %s", node.Address)
			}
		}

		return nil
	})

	if err != nil {
		glog.Errorf("MasterClient.WithClient failed: %v", err)
	} else {
		glog.V(1).Infof("Broker discovery completed successfully - found %d brokers: %v", len(brokers), brokers)
	}

	return brokers, err
}

// discoverFilersWithMasterClient queries masters for available filers using reusable master client
func discoverFilersWithMasterClient(masterClient *wdclient.MasterClient, filerGroup string) ([]pb.ServerAddress, error) {
	var filers []pb.ServerAddress

	err := masterClient.WithClient(false, func(client master_pb.SeaweedClient) error {
		resp, err := client.ListClusterNodes(context.Background(), &master_pb.ListClusterNodesRequest{
			ClientType: cluster.FilerType,
			FilerGroup: filerGroup,
			Limit:      1000,
		})
		if err != nil {
			return err
		}

		// Extract filer addresses from response - return as HTTP addresses (pb.ServerAddress)
		for _, node := range resp.ClusterNodes {
			if node.Address != "" {
				// Return HTTP address as pb.ServerAddress (no pre-conversion to gRPC)
				httpAddr := pb.ServerAddress(node.Address)
				fmt.Printf("FILER DISCOVERY: Found filer HTTP address %s\n", httpAddr)
				filers = append(filers, httpAddr)
			}
		}

		return nil
	})

	return filers, err
}

// checkTopicInFiler checks if a topic exists in the filer
func (h *SeaweedMQHandler) checkTopicInFiler(topicName string) bool {
	if h.filerClientAccessor == nil {
		return false
	}

	var exists bool
	h.filerClientAccessor.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		request := &filer_pb.LookupDirectoryEntryRequest{
			Directory: "/topics/kafka",
			Name:      topicName,
		}

		_, err := client.LookupDirectoryEntry(context.Background(), request)
		exists = (err == nil)
		glog.V(1).Infof("checkTopicInFiler: topic=%s, directory=/topics/kafka, exists=%v, err=%v", topicName, exists, err)
		return nil // Don't propagate error, just check existence
	})

	return exists
}

// listTopicsFromFiler lists all topics from the filer
func (h *SeaweedMQHandler) listTopicsFromFiler() []string {
	if h.filerClientAccessor == nil {
		fmt.Printf("listTopicsFromFiler: filerClientAccessor is nil\n")
		return []string{}
	}

	var topics []string
	fmt.Printf("listTopicsFromFiler: Attempting to list entries in /topics/kafka\n")

	h.filerClientAccessor.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		request := &filer_pb.ListEntriesRequest{
			Directory: "/topics/kafka",
		}

		stream, err := client.ListEntries(context.Background(), request)
		if err != nil {
			glog.V(1).Infof("listTopicsFromFiler: ListEntries failed: %v", err)
			return nil // Don't propagate error, just return empty list
		}

		glog.V(1).Infof("listTopicsFromFiler: ListEntries stream created successfully")
		for {
			resp, err := stream.Recv()
			if err != nil {
				glog.V(1).Infof("listTopicsFromFiler: Stream recv ended: %v", err)
				break // End of stream or error
			}

			if resp.Entry != nil && resp.Entry.IsDirectory {
				glog.V(1).Infof("listTopicsFromFiler: Found directory: %s", resp.Entry.Name)
				topics = append(topics, resp.Entry.Name)
			} else if resp.Entry != nil {
				glog.V(1).Infof("listTopicsFromFiler: Found non-directory: %s (isDir=%v)", resp.Entry.Name, resp.Entry.IsDirectory)
			}
		}
		return nil
	})

	glog.V(1).Infof("listTopicsFromFiler: Returning %d topics: %v", len(topics), topics)
	return topics
}

// GetFilerClientAccessor returns the shared filer client accessor
func (h *SeaweedMQHandler) GetFilerClientAccessor() *filer_client.FilerClientAccessor {
	return h.filerClientAccessor
}

// BrokerClient wraps the SeaweedMQ Broker gRPC client for Kafka gateway integration
type BrokerClient struct {
	// Reference to shared filer client accessor
	filerClientAccessor *filer_client.FilerClientAccessor

	brokerAddress string
	conn          *grpc.ClientConn
	client        mq_pb.SeaweedMessagingClient

	// Filer addresses for metadata access (lazy connection)
	filerAddresses []pb.ServerAddress

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
	// Track the requested start offset used to initialize this stream
	StartOffset int64
}

// NewBrokerClientWithFilerAccessor creates a client with a shared filer accessor
func NewBrokerClientWithFilerAccessor(brokerAddress string, filerClientAccessor *filer_client.FilerClientAccessor) (*BrokerClient, error) {
	ctx, cancel := context.WithCancel(context.Background())

	// Use background context for gRPC connections to prevent them from being canceled
	// when BrokerClient.Close() is called. This allows subscriber streams to continue
	// operating even during client shutdown, which is important for testing scenarios.
	dialCtx := context.Background()

	// Connect to broker
	// Load security configuration for broker connection
	util.LoadSecurityConfiguration()
	grpcDialOption := security.LoadClientTLS(util.GetViper(), "grpc.mq")

	conn, err := grpc.DialContext(dialCtx, brokerAddress,
		grpcDialOption,
	)
	if err != nil {
		cancel()
		return nil, fmt.Errorf("failed to connect to broker %s: %v", brokerAddress, err)
	}

	client := mq_pb.NewSeaweedMessagingClient(conn)

	// Extract filer addresses from the accessor for backward compatibility
	var filerAddresses []pb.ServerAddress
	if filerClientAccessor.GetFilers != nil {
		filerAddresses = filerClientAccessor.GetFilers()
	}

	return &BrokerClient{
		filerClientAccessor: filerClientAccessor,
		brokerAddress:       brokerAddress,
		conn:                conn,
		client:              client,
		filerAddresses:      filerAddresses,
		publishers:          make(map[string]*BrokerPublisherSession),
		subscribers:         make(map[string]*BrokerSubscriberSession),
		ctx:                 ctx,
		cancel:              cancel,
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

// GetFilerAddress returns the first filer address used by this broker client (for backward compatibility)
func (bc *BrokerClient) GetFilerAddress() string {
	if len(bc.filerAddresses) > 0 {
		return string(bc.filerAddresses[0])
	}
	return ""
}

// Delegate methods to the shared filer client accessor
func (bc *BrokerClient) WithFilerClient(streamingMode bool, fn func(client filer_pb.SeaweedFilerClient) error) error {
	return bc.filerClientAccessor.WithFilerClient(streamingMode, fn)
}

func (bc *BrokerClient) GetFilers() []pb.ServerAddress {
	return bc.filerClientAccessor.GetFilers()
}

func (bc *BrokerClient) GetGrpcDialOption() grpc.DialOption {
	return bc.filerClientAccessor.GetGrpcDialOption()
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

	// Handle structured broker errors
	if kafkaErrorCode, errorMsg, handleErr := HandleBrokerResponse(resp); handleErr != nil {
		return 0, handleErr
	} else if kafkaErrorCode != 0 {
		// Return error with Kafka error code information for better debugging
		return 0, fmt.Errorf("broker error (Kafka code %d): %s", kafkaErrorCode, errorMsg)
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

	// Get the actual partition assignment from the broker instead of using Kafka partition mapping
	actualPartition, err := bc.getActualPartitionAssignment(topic, partition)
	if err != nil {
		return nil, fmt.Errorf("failed to get actual partition assignment: %v", err)
	}

	// Send init message using the actual partition structure that the broker allocated
	if err := stream.Send(&mq_pb.PublishMessageRequest{
		Message: &mq_pb.PublishMessageRequest_Init{
			Init: &mq_pb.PublishMessageRequest_InitMessage{
				Topic: &schema_pb.Topic{
					Namespace: "kafka",
					Name:      topic,
				},
				Partition:     actualPartition,
				AckInterval:   1, // Fast acknowledgments for Kafka Gateway (Schema Registry needs < 500ms)
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

// getActualPartitionAssignment looks up the actual partition assignment from the broker configuration
func (bc *BrokerClient) getActualPartitionAssignment(topic string, kafkaPartition int32) (*schema_pb.Partition, error) {
	// Look up the topic configuration from the broker to get the actual partition assignments
	lookupResp, err := bc.client.LookupTopicBrokers(bc.ctx, &mq_pb.LookupTopicBrokersRequest{
		Topic: &schema_pb.Topic{
			Namespace: "kafka",
			Name:      topic,
		},
	})
	if err != nil {
		return nil, fmt.Errorf("failed to lookup topic brokers: %v", err)
	}

	if len(lookupResp.BrokerPartitionAssignments) == 0 {
		return nil, fmt.Errorf("no partition assignments found for topic %s", topic)
	}

	totalPartitions := int32(len(lookupResp.BrokerPartitionAssignments))
	if kafkaPartition >= totalPartitions {
		return nil, fmt.Errorf("kafka partition %d out of range, topic %s has %d partitions",
			kafkaPartition, topic, totalPartitions)
	}

	// Calculate expected range for this Kafka partition
	// Ring is divided equally among partitions, with last partition getting any remainder
	const ringSize = int32(2520) // MaxPartitionCount constant
	rangeSize := ringSize / totalPartitions
	expectedRangeStart := kafkaPartition * rangeSize
	var expectedRangeStop int32

	if kafkaPartition == totalPartitions-1 {
		// Last partition gets the remainder to fill the entire ring
		expectedRangeStop = ringSize
	} else {
		expectedRangeStop = (kafkaPartition + 1) * rangeSize
	}

	glog.V(2).Infof("Looking for Kafka partition %d in topic %s: expected range [%d, %d] out of %d partitions",
		kafkaPartition, topic, expectedRangeStart, expectedRangeStop, totalPartitions)

	// Find the broker assignment that matches this range
	for _, assignment := range lookupResp.BrokerPartitionAssignments {
		if assignment.Partition == nil {
			continue
		}

		// Check if this assignment's range matches our expected range
		if assignment.Partition.RangeStart == expectedRangeStart && assignment.Partition.RangeStop == expectedRangeStop {
			glog.V(1).Infof("🎯 Found matching partition assignment for %s[%d]: {RingSize: %d, RangeStart: %d, RangeStop: %d, UnixTimeNs: %d}",
				topic, kafkaPartition, assignment.Partition.RingSize, assignment.Partition.RangeStart,
				assignment.Partition.RangeStop, assignment.Partition.UnixTimeNs)
			return assignment.Partition, nil
		}
	}

	// If no exact match found, log all available assignments for debugging
	glog.Warningf("❌ No partition assignment found for Kafka partition %d in topic %s with expected range [%d, %d]",
		kafkaPartition, topic, expectedRangeStart, expectedRangeStop)
	glog.Warningf("Available assignments:")
	for i, assignment := range lookupResp.BrokerPartitionAssignments {
		if assignment.Partition != nil {
			glog.Warningf("  Assignment[%d]: {RangeStart: %d, RangeStop: %d, RingSize: %d}",
				i, assignment.Partition.RangeStart, assignment.Partition.RangeStop, assignment.Partition.RingSize)
		}
	}

	return nil, fmt.Errorf("no broker assignment found for Kafka partition %d with expected range [%d, %d]",
		kafkaPartition, expectedRangeStart, expectedRangeStop)
}

// GetOrCreateSubscriber gets or creates a subscriber for offset tracking
func (bc *BrokerClient) GetOrCreateSubscriber(topic string, partition int32, startOffset int64) (*BrokerSubscriberSession, error) {
	key := fmt.Sprintf("%s-%d", topic, partition)

	bc.subscribersLock.RLock()
	if session, exists := bc.subscribers[key]; exists {
		// If the existing session was created with a different start offset,
		// re-create the subscriber to honor the requested position.
		if session.StartOffset != startOffset {
			bc.subscribersLock.RUnlock()
			// Close and delete the old session before creating a new one
			bc.subscribersLock.Lock()
			if old, ok := bc.subscribers[key]; ok {
				if old.Stream != nil {
					_ = old.Stream.CloseSend()
				}
				delete(bc.subscribers, key)
			}
			bc.subscribersLock.Unlock()
		} else {
			bc.subscribersLock.RUnlock()
			return session, nil
		}
	} else {
		bc.subscribersLock.RUnlock()
	}

	// Create new subscriber stream
	bc.subscribersLock.Lock()
	defer bc.subscribersLock.Unlock()

	if session, exists := bc.subscribers[key]; exists {
		return session, nil
	}

	// Create a dedicated context for this subscriber that won't be canceled with the main BrokerClient context
	// This prevents subscriber streams from being canceled when BrokerClient.Close() is called during test cleanup
	subscriberCtx := context.Background() // Use background context instead of bc.ctx

	stream, err := bc.client.SubscribeMessage(subscriberCtx)
	if err != nil {
		return nil, fmt.Errorf("failed to create subscribe stream: %v", err)
	}

	// Get the actual partition assignment from the broker instead of using Kafka partition mapping
	actualPartition, err := bc.getActualPartitionAssignment(topic, partition)
	if err != nil {
		return nil, fmt.Errorf("failed to get actual partition assignment for subscribe: %v", err)
	}

	// Convert Kafka offset to appropriate SeaweedMQ OffsetType and parameters
	var offsetType schema_pb.OffsetType
	var startTimestamp int64
	var startOffsetValue int64

	if startOffset == 0 {
		// For Kafka offset 0 (read from beginning), use RESET_TO_EARLIEST
		offsetType = schema_pb.OffsetType_RESET_TO_EARLIEST
		startTimestamp = 0   // Not used with RESET_TO_EARLIEST
		startOffsetValue = 0 // Not used with RESET_TO_EARLIEST
		glog.V(1).Infof("Using RESET_TO_EARLIEST for Kafka offset 0")
	} else if startOffset == -1 {
		// Kafka offset -1 typically means "latest"
		offsetType = schema_pb.OffsetType_RESET_TO_LATEST
		startTimestamp = 0   // Not used with RESET_TO_LATEST
		startOffsetValue = 0 // Not used with RESET_TO_LATEST
		glog.V(1).Infof("Using RESET_TO_LATEST for Kafka offset -1 (read latest)")
	} else {
		// For specific offsets, use native SeaweedMQ offset-based positioning
		offsetType = schema_pb.OffsetType_EXACT_OFFSET
		startTimestamp = 0             // Not used with EXACT_OFFSET
		startOffsetValue = startOffset // Use the Kafka offset directly
		glog.V(1).Infof("Using EXACT_OFFSET for Kafka offset %d (native offset-based positioning)", startOffset)
	}

	glog.V(1).Infof("[DEBUG_FETCH] Creating subscriber for topic=%s partition=%d: Kafka offset %d -> SeaweedMQ %s (timestamp=%d, startOffsetValue=%d)",
		topic, partition, startOffset, offsetType, startTimestamp, startOffsetValue)

	// Send init message using the actual partition structure that the broker allocated
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
					Partition:   actualPartition,
					StartTsNs:   startTimestamp,
					StartOffset: startOffsetValue,
				},
				OffsetType:        offsetType, // Use the correct offset type
				SlidingWindowSize: 10,
			},
		},
	}); err != nil {
		glog.V(1).Infof("[DEBUG_FETCH] Failed to send subscribe init: %v", err)
		return nil, fmt.Errorf("failed to send subscribe init: %v", err)
	}

	glog.V(1).Infof("[DEBUG_FETCH] Successfully sent subscribe init message for topic=%s partition=%d", topic, partition)

	session := &BrokerSubscriberSession{
		Topic:       topic,
		Partition:   partition,
		Stream:      stream,
		StartOffset: startOffset,
	}

	bc.subscribers[key] = session
	return session, nil
}

// PublishRecordValue publishes a RecordValue message to SeaweedMQ via broker
func (bc *BrokerClient) PublishRecordValue(topic string, partition int32, key []byte, recordValueBytes []byte, timestamp int64) (int64, error) {
	session, err := bc.getOrCreatePublisher(topic, partition)
	if err != nil {
		return 0, err
	}

	// Send data message with RecordValue in the Value field
	dataMsg := &mq_pb.DataMessage{
		Key:   key,
		Value: recordValueBytes, // This contains the marshaled RecordValue
		TsNs:  timestamp,
	}

	if err := session.Stream.Send(&mq_pb.PublishMessageRequest{
		Message: &mq_pb.PublishMessageRequest_Data{
			Data: dataMsg,
		},
	}); err != nil {
		return 0, fmt.Errorf("failed to send RecordValue data: %v", err)
	}

	// Read acknowledgment
	resp, err := session.Stream.Recv()
	if err != nil {
		return 0, fmt.Errorf("failed to receive RecordValue ack: %v", err)
	}

	// Handle structured broker errors
	if kafkaErrorCode, errorMsg, handleErr := HandleBrokerResponse(resp); handleErr != nil {
		return 0, handleErr
	} else if kafkaErrorCode != 0 {
		// Return error with Kafka error code information for better debugging
		return 0, fmt.Errorf("RecordValue broker error (Kafka code %d): %s", kafkaErrorCode, errorMsg)
	}

	return resp.AckSequence, nil
}

// ReadRecords reads available records from the subscriber stream
func (bc *BrokerClient) ReadRecords(session *BrokerSubscriberSession, maxRecords int) ([]*SeaweedRecord, error) {
	if session == nil {
		return nil, fmt.Errorf("subscriber session cannot be nil")
	}

	glog.V(1).Infof("[DEBUG_FETCH] ReadRecords: topic=%s partition=%d maxRecords=%d startOffset=%d",
		session.Topic, session.Partition, maxRecords, session.StartOffset)

	var records []*SeaweedRecord

	for len(records) < maxRecords {
		glog.V(2).Infof("[DEBUG_FETCH] Calling session.Stream.Recv() for topic=%s partition=%d (records so far: %d)",
			session.Topic, session.Partition, len(records))
		resp, err := session.Stream.Recv()

		if err != nil {
			glog.V(1).Infof("[DEBUG_FETCH] Stream.Recv() failed: %v (records collected: %d)", err, len(records))
			// If we have some records, return them; otherwise return error
			if len(records) > 0 {
				glog.V(1).Infof("[DEBUG_FETCH] Returning %d records despite error", len(records))
				return records, nil
			}
			return nil, fmt.Errorf("failed to receive record: %v", err)
		}

		if dataMsg := resp.GetData(); dataMsg != nil {
			glog.V(2).Infof("[DEBUG_FETCH] Received data message: key_len=%d value_len=%d ts=%d",
				len(dataMsg.Key), len(dataMsg.Value), dataMsg.TsNs)
			record := &SeaweedRecord{
				Key:       dataMsg.Key,
				Value:     dataMsg.Value,
				Timestamp: dataMsg.TsNs,
				Sequence:  0, // Will be set by offset ledger
			}
			records = append(records, record)

			// Important: return early after receiving at least one record to avoid
			// blocking while waiting for an entire batch in environments where data
			// arrives slowly. The fetch layer will invoke subsequent reads as needed.
			if len(records) >= 1 {
				glog.V(2).Infof("[DEBUG_FETCH] Got first record, breaking early")
				break
			}
		} else {
			glog.V(2).Infof("[DEBUG_FETCH] Received non-data message from stream")
		}
	}

	glog.V(1).Infof("[DEBUG_FETCH] ReadRecords returning %d records", len(records))
	return records, nil
}

// ReadRecordsWithOffsets reads records directly from SMQ with real Kafka offsets
// This bypasses the streaming API to access LogEntry.Offset directly
func (bc *BrokerClient) ReadRecordsWithOffsets(topicName string, partitionID int32, fromOffset int64, maxRecords int) ([]offset.SMQRecord, error) {
	glog.V(1).Infof("[DEBUG_FETCH] ReadRecordsWithOffsets: topic=%s partition=%d fromOffset=%d maxRecords=%d", topicName, partitionID, fromOffset, maxRecords)

	// Create topic and partition structures for SMQ
	t := schema_pb.Topic{
		Namespace: "kafka",
		Name:      topicName,
	}

	// For now, we need to create a partition structure
	// In a real implementation, we'd get this from the broker's partition registry
	// This is a simplified approach - we'll use a default partition structure
	p := schema_pb.Partition{
		RingSize:   1024,
		RangeStart: 0,
		RangeStop:  1024,
		UnixTimeNs: time.Now().UnixNano(),
	}

	// Connect to the broker to get access to LocalPartition
	// For now, we'll use a gRPC call to read records with offsets
	// This is a placeholder - in the real implementation, we'd access LocalPartition directly

	// Create a temporary subscriber to read records with offset filtering
	conn, err := grpc.Dial(bc.brokerAddress, bc.GetGrpcDialOption())
	if err != nil {
		return nil, fmt.Errorf("failed to connect to broker: %v", err)
	}
	defer conn.Close()

	client := mq_pb.NewSeaweedMessagingClient(conn)

	// Create a subscription request with exact offset positioning
	stream, err := client.SubscribeMessage(context.Background())
	if err != nil {
		return nil, fmt.Errorf("failed to create subscription stream: %v", err)
	}
	defer stream.CloseSend()

	// Send init message with exact offset positioning
	initMsg := &mq_pb.SubscribeMessageRequest{
		Message: &mq_pb.SubscribeMessageRequest_Init{
			Init: &mq_pb.SubscribeMessageRequest_InitMessage{
				ConsumerGroup: "kafka-gateway-direct-read",
				ConsumerId:    fmt.Sprintf("direct-reader-%d", time.Now().UnixNano()),
				ClientId:      "kafka-gateway",
				Topic:         &t,
				PartitionOffset: &schema_pb.PartitionOffset{
					Partition:   &p,
					StartOffset: fromOffset,
				},
				OffsetType: schema_pb.OffsetType_EXACT_OFFSET, // Use exact offset positioning
			},
		},
	}

	if err := stream.Send(initMsg); err != nil {
		return nil, fmt.Errorf("failed to send init message: %v", err)
	}

	// Read records with real offsets
	var smqRecords []offset.SMQRecord
	recordsRead := 0

	for recordsRead < maxRecords {
		resp, err := stream.Recv()
		if err != nil {
			if err == io.EOF {
				break
			}
			// If we have some records, return them; otherwise return error
			if len(smqRecords) > 0 {
				glog.V(1).Infof("[DEBUG_FETCH] Got %d records before error: %v", len(smqRecords), err)
				break
			}
			return nil, fmt.Errorf("failed to receive record: %v", err)
		}

		if dataMsg := resp.GetData(); dataMsg != nil {
			// Calculate the real Kafka offset for this record
			// Since we're using EXACT_OFFSET positioning, the records should be in order
			realKafkaOffset := fromOffset + int64(recordsRead)

			smqRecord := &SeaweedSMQRecord{
				key:       dataMsg.Key,
				value:     dataMsg.Value,
				timestamp: dataMsg.TsNs,
				offset:    realKafkaOffset, // Use calculated offset based on position
			}
			smqRecords = append(smqRecords, smqRecord)
			recordsRead++

			glog.V(2).Infof("[DEBUG_FETCH] Read record %d: offset=%d key_len=%d value_len=%d",
				recordsRead, realKafkaOffset, len(dataMsg.Key), len(dataMsg.Value))
		} else if ctrlMsg := resp.GetCtrl(); ctrlMsg != nil {
			if ctrlMsg.IsEndOfStream || ctrlMsg.IsEndOfTopic {
				glog.V(1).Infof("[DEBUG_FETCH] Reached end of stream/topic")
				break
			}
		}
	}

	glog.V(1).Infof("[DEBUG_FETCH] ReadRecordsWithOffsets returning %d records", len(smqRecords))
	return smqRecords, nil
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
