package integration

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc"

	"github.com/seaweedfs/seaweedfs/weed/cluster"
	"github.com/seaweedfs/seaweedfs/weed/filer_client"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/mq"
	"github.com/seaweedfs/seaweedfs/weed/mq/pub_balancer"
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

// SMQRecord interface for records from SeaweedMQ
type SMQRecord interface {
	GetKey() []byte
	GetValue() []byte
	GetTimestamp() int64
	GetOffset() int64
}

// hwmCacheEntry represents a cached high water mark value
type hwmCacheEntry struct {
	value     int64
	expiresAt time.Time
}

// topicExistsCacheEntry represents a cached topic existence check
type topicExistsCacheEntry struct {
	exists    bool
	expiresAt time.Time
}

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

	// Ledgers removed - SMQ broker handles all offset management directly

	// Reference to protocol handler for accessing connection context
	protocolHandler ProtocolHandler

	// High water mark cache to reduce broker queries
	hwmCache    map[string]*hwmCacheEntry // key: "topic:partition"
	hwmCacheMu  sync.RWMutex
	hwmCacheTTL time.Duration

	// Topic existence cache to reduce broker queries
	topicExistsCache    map[string]*topicExistsCacheEntry // key: "topic"
	topicExistsCacheMu  sync.RWMutex
	topicExistsCacheTTL time.Duration
}

// ConnectionContext holds connection-specific information for requests
// This is a local copy to avoid circular dependency with protocol package
type ConnectionContext struct {
	ClientID      string      // Kafka client ID from request headers
	ConsumerGroup string      // Consumer group (set by JoinGroup)
	MemberID      string      // Consumer group member ID (set by JoinGroup)
	BrokerClient  interface{} // Per-connection broker client (*BrokerClient)
}

// ProtocolHandler interface for accessing Handler's connection context
type ProtocolHandler interface {
	GetConnectionContext() *ConnectionContext
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

// SeaweedRecord represents a record received from SeaweedMQ
type SeaweedRecord struct {
	Key       []byte
	Value     []byte
	Timestamp int64
	Offset    int64
}

// GetStoredRecords retrieves records from SeaweedMQ using the proper subscriber API
func (h *SeaweedMQHandler) GetStoredRecords(topic string, partition int32, fromOffset int64, maxRecords int) ([]SMQRecord, error) {
	glog.Infof("[FETCH] GetStoredRecords: topic=%s partition=%d fromOffset=%d maxRecords=%d", topic, partition, fromOffset, maxRecords)

	// Verify topic exists
	if !h.TopicExists(topic) {
		return nil, fmt.Errorf("topic %s does not exist", topic)
	}

	// CRITICAL: Use per-connection BrokerClient to prevent gRPC stream interference
	// Each Kafka connection has its own isolated BrokerClient instance
	var brokerClient *BrokerClient
	consumerGroup := "kafka-fetch-consumer"                            // default
	consumerID := fmt.Sprintf("kafka-fetch-%d", time.Now().UnixNano()) // default

	// Get the per-connection broker client from connection context
	if h.protocolHandler != nil {
		connCtx := h.protocolHandler.GetConnectionContext()
		if connCtx != nil {
			// Extract per-connection broker client
			if connCtx.BrokerClient != nil {
				if bc, ok := connCtx.BrokerClient.(*BrokerClient); ok {
					brokerClient = bc
					glog.Infof("[FETCH] Using per-connection BrokerClient for topic=%s partition=%d", topic, partition)
				}
			}

			// Extract consumer group and client ID
			if connCtx.ConsumerGroup != "" {
				consumerGroup = connCtx.ConsumerGroup
				glog.Infof("[FETCH] Using actual consumer group from context: %s", consumerGroup)
			}
			if connCtx.MemberID != "" {
				consumerID = connCtx.MemberID
				glog.Infof("[FETCH] Using actual member ID from context: %s", consumerID)
			} else if connCtx.ClientID != "" {
				// Fallback to client ID if member ID not set (for clients not using consumer groups)
				consumerID = connCtx.ClientID
				glog.Infof("[FETCH] Using client ID from context: %s", consumerID)
			}
		}
	}

	// Fallback to shared broker client if per-connection client not available
	if brokerClient == nil {
		glog.Warningf("[FETCH] No per-connection BrokerClient, falling back to shared client")
		brokerClient = h.brokerClient
		if brokerClient == nil {
			return nil, fmt.Errorf("no broker client available")
		}
	}

	// CRITICAL FIX: Reuse existing subscriber if offset matches to avoid concurrent subscriber storm
	// Creating too many concurrent subscribers to the same offset causes the broker to return
	// the same data repeatedly, creating an infinite loop.
	glog.Infof("[FETCH] Getting or creating subscriber for topic=%s partition=%d fromOffset=%d", topic, partition, fromOffset)

	brokerSubscriber, err := brokerClient.GetOrCreateSubscriber(topic, partition, fromOffset)
	if err != nil {
		glog.Errorf("[FETCH] Failed to get/create subscriber: %v", err)
		return nil, fmt.Errorf("failed to get/create subscriber: %v", err)
	}
	glog.Infof("[FETCH] Subscriber ready")

	// CRITICAL FIX: If the subscriber has already consumed past the requested offset,
	// close it and create a fresh one to avoid broker tight loop
	if brokerSubscriber.StartOffset > fromOffset {
		glog.Infof("[FETCH] Subscriber already at offset %d (requested %d < current), closing and recreating",
			brokerSubscriber.StartOffset, fromOffset)

		// Close the old subscriber
		if brokerSubscriber.Stream != nil {
			_ = brokerSubscriber.Stream.CloseSend()
		}

		// Remove from cache
		key := fmt.Sprintf("%s-%d", topic, partition)
		brokerClient.subscribersLock.Lock()
		delete(brokerClient.subscribers, key)
		brokerClient.subscribersLock.Unlock()

		// Create a fresh subscriber at the requested offset
		brokerSubscriber, err = brokerClient.CreateFreshSubscriber(topic, partition, fromOffset, consumerGroup, consumerID)
		if err != nil {
			glog.Errorf("[FETCH] Failed to create fresh subscriber: %v", err)
			return nil, fmt.Errorf("failed to create fresh subscriber: %v", err)
		}
		glog.Infof("[FETCH] Created fresh subscriber at offset %d", fromOffset)
	}

	// NOTE: We DON'T close the subscriber here because we're reusing it across Fetch requests
	// The subscriber will be closed when the connection closes or when a different offset is requested

	// Read records using the subscriber
	glog.Infof("[FETCH] Calling ReadRecords for topic=%s partition=%d maxRecords=%d", topic, partition, maxRecords)
	seaweedRecords, err := brokerClient.ReadRecords(brokerSubscriber, maxRecords)
	if err != nil {
		glog.Errorf("[FETCH] ReadRecords failed: %v", err)
		return nil, fmt.Errorf("failed to read records: %v", err)
	}
	glog.Infof("[FETCH] ReadRecords returned %d records", len(seaweedRecords))

	// Convert SeaweedMQ records to SMQRecord interface with proper Kafka offsets
	smqRecords := make([]SMQRecord, 0, len(seaweedRecords))
	for i, seaweedRecord := range seaweedRecords {
		// CRITICAL FIX: Use the actual offset from SeaweedMQ
		// The SeaweedRecord.Offset field now contains the correct offset from the subscriber
		kafkaOffset := seaweedRecord.Offset

		// Validate that the offset makes sense
		expectedOffset := fromOffset + int64(i)
		if kafkaOffset != expectedOffset {
			glog.Warningf("[FETCH] Offset mismatch for record %d: got=%d, expected=%d", i, kafkaOffset, expectedOffset)
		}

		smqRecord := &SeaweedSMQRecord{
			key:       seaweedRecord.Key,
			value:     seaweedRecord.Value,
			timestamp: seaweedRecord.Timestamp,
			offset:    kafkaOffset,
		}
		smqRecords = append(smqRecords, smqRecord)

		glog.Infof("[FETCH] Record %d: offset=%d, keyLen=%d, valueLen=%d", i, kafkaOffset, len(seaweedRecord.Key), len(seaweedRecord.Value))
	}

	glog.Infof("[FETCH] Successfully read %d records from SMQ", len(smqRecords))
	return smqRecords, nil
}

// PartitionRangeInfo contains comprehensive range information for a partition
type PartitionRangeInfo struct {
	// Offset range information
	EarliestOffset int64
	LatestOffset   int64
	HighWaterMark  int64

	// Timestamp range information
	EarliestTimestampNs int64
	LatestTimestampNs   int64

	// Partition metadata
	RecordCount         int64
	ActiveSubscriptions int64
}

// GetEarliestOffset returns the earliest available offset for a topic partition
// ALWAYS queries SMQ broker directly - no ledger involved
func (h *SeaweedMQHandler) GetEarliestOffset(topic string, partition int32) (int64, error) {
	glog.V(4).Infof("[DEBUG_OFFSET] GetEarliestOffset called for topic=%s partition=%d", topic, partition)

	// Check if topic exists
	if !h.TopicExists(topic) {
		glog.V(4).Infof("[DEBUG_OFFSET] Topic %s does not exist", topic)
		return 0, nil // Empty topic starts at offset 0
	}

	// ALWAYS query SMQ broker directly for earliest offset
	if h.brokerClient != nil {
		glog.V(4).Infof("[DEBUG_OFFSET] Querying SMQ broker for earliest offset...")
		earliestOffset, err := h.brokerClient.GetEarliestOffset(topic, partition)
		if err != nil {
			glog.Errorf("[DEBUG_OFFSET] Failed to get earliest offset from broker: %v", err)
			return 0, err
		}
		glog.V(4).Infof("[DEBUG_OFFSET] Got earliest offset from broker: %d", earliestOffset)
		return earliestOffset, nil
	}

	// No broker client - this shouldn't happen in production
	glog.Errorf("[DEBUG_OFFSET] BrokerClient is nil - cannot query SMQ")
	return 0, fmt.Errorf("broker client not available")
}

// GetLatestOffset returns the latest available offset for a topic partition
// ALWAYS queries SMQ broker directly - no ledger involved
func (h *SeaweedMQHandler) GetLatestOffset(topic string, partition int32) (int64, error) {
	glog.V(4).Infof("[DEBUG_OFFSET] GetLatestOffset called for topic=%s partition=%d", topic, partition)

	// Check if topic exists
	if !h.TopicExists(topic) {
		glog.V(4).Infof("[DEBUG_OFFSET] Topic %s does not exist", topic)
		return 0, nil // Empty topic
	}

	// Check cache first
	cacheKey := fmt.Sprintf("%s:%d", topic, partition)
	h.hwmCacheMu.RLock()
	if entry, exists := h.hwmCache[cacheKey]; exists {
		if time.Now().Before(entry.expiresAt) {
			// Cache hit - return cached value
			h.hwmCacheMu.RUnlock()
			glog.V(4).Infof("[DEBUG_OFFSET] Cache hit for %s: hwm=%d", cacheKey, entry.value)
			return entry.value, nil
		}
	}
	h.hwmCacheMu.RUnlock()

	// Cache miss or expired - query SMQ broker
	if h.brokerClient != nil {
		glog.V(4).Infof("[DEBUG_OFFSET] Cache miss for %s, querying SMQ broker...", cacheKey)
		latestOffset, err := h.brokerClient.GetHighWaterMark(topic, partition)
		if err != nil {
			glog.Errorf("[DEBUG_OFFSET] Failed to get high water mark from broker: %v", err)
			return 0, err
		}

		// Update cache
		h.hwmCacheMu.Lock()
		h.hwmCache[cacheKey] = &hwmCacheEntry{
			value:     latestOffset,
			expiresAt: time.Now().Add(h.hwmCacheTTL),
		}
		h.hwmCacheMu.Unlock()

		glog.V(4).Infof("[DEBUG_OFFSET] Got high water mark from broker and cached: %d (TTL=%v)", latestOffset, h.hwmCacheTTL)
		return latestOffset, nil
	}

	// No broker client - this shouldn't happen in production
	glog.Errorf("[DEBUG_OFFSET] BrokerClient is nil - cannot query SMQ")
	return 0, fmt.Errorf("broker client not available")
}

// SeaweedSMQRecord implements the SMQRecord interface for SeaweedMQ records
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

// CreatePerConnectionBrokerClient creates a new BrokerClient instance for a specific connection
// CRITICAL: Each Kafka TCP connection gets its own BrokerClient to prevent gRPC stream interference
// This fixes the deadlock where CreateFreshSubscriber would block all connections
func (h *SeaweedMQHandler) CreatePerConnectionBrokerClient() (*BrokerClient, error) {
	// Use the same broker addresses as the shared client
	if len(h.brokerAddresses) == 0 {
		return nil, fmt.Errorf("no broker addresses available")
	}

	// Use the first broker address (in production, could use load balancing)
	brokerAddress := h.brokerAddresses[0]
	glog.V(4).Infof("[BROKER_CLIENT] Creating per-connection client to %s", brokerAddress)

	// Create a new client with the shared filer accessor
	client, err := NewBrokerClientWithFilerAccessor(brokerAddress, h.filerClientAccessor)
	if err != nil {
		return nil, fmt.Errorf("failed to create broker client: %w", err)
	}

	glog.V(4).Infof("[BROKER_CLIENT] Successfully created per-connection client")
	return client, nil
}

// CreateTopic creates a new topic in both Kafka registry and SeaweedMQ
func (h *SeaweedMQHandler) CreateTopic(name string, partitions int32) error {
	return h.CreateTopicWithSchema(name, partitions, nil)
}

// CreateTopicWithSchema creates a topic with optional value schema
func (h *SeaweedMQHandler) CreateTopicWithSchema(name string, partitions int32, recordType *schema_pb.RecordType) error {
	return h.CreateTopicWithSchemas(name, partitions, nil, recordType)
}

// CreateTopicWithSchemas creates a topic with optional key and value schemas
func (h *SeaweedMQHandler) CreateTopicWithSchemas(name string, partitions int32, keyRecordType *schema_pb.RecordType, valueRecordType *schema_pb.RecordType) error {
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

	// Offset management now handled directly by SMQ broker - no initialization needed

	// Invalidate cache after successful topic creation
	h.InvalidateTopicExistsCache(name)

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

	// Offset management handled by SMQ broker - no cleanup needed

	return nil
}

// TopicExists checks if a topic exists in SeaweedMQ broker (includes in-memory topics)
// Uses a 5-second cache to reduce broker queries
func (h *SeaweedMQHandler) TopicExists(name string) bool {
	// Check cache first
	h.topicExistsCacheMu.RLock()
	if entry, found := h.topicExistsCache[name]; found {
		if time.Now().Before(entry.expiresAt) {
			h.topicExistsCacheMu.RUnlock()
			glog.V(4).Infof("TopicExists cache HIT for %s: %v", name, entry.exists)
			return entry.exists
		}
	}
	h.topicExistsCacheMu.RUnlock()

	// Cache miss or expired - query broker
	glog.V(4).Infof("TopicExists cache MISS for %s, querying broker", name)

	var exists bool
	// Check via SeaweedMQ broker (includes in-memory topics)
	if h.brokerClient != nil {
		var err error
		exists, err = h.brokerClient.TopicExists(name)
		if err != nil {
			fmt.Printf("TopicExists: Failed to check topic %s via SMQ broker: %v\n", name, err)
			// Don't cache errors
			return false
		}
	} else {
		// Return false if broker is unavailable
		fmt.Printf("TopicExists: No broker client available for topic %s\n", name)
		return false
	}

	// Update cache
	h.topicExistsCacheMu.Lock()
	h.topicExistsCache[name] = &topicExistsCacheEntry{
		exists:    exists,
		expiresAt: time.Now().Add(h.topicExistsCacheTTL),
	}
	h.topicExistsCacheMu.Unlock()

	return exists
}

// InvalidateTopicExistsCache removes a topic from the existence cache
// Should be called after creating or deleting a topic
func (h *SeaweedMQHandler) InvalidateTopicExistsCache(name string) {
	h.topicExistsCacheMu.Lock()
	delete(h.topicExistsCache, name)
	h.topicExistsCacheMu.Unlock()
	glog.V(4).Infof("Invalidated TopicExists cache for %s", name)
}

// GetTopicInfo returns information about a topic from broker
func (h *SeaweedMQHandler) GetTopicInfo(name string) (*KafkaTopicInfo, bool) {
	// Get topic configuration from broker
	if h.brokerClient != nil {
		config, err := h.brokerClient.GetTopicConfiguration(name)
		if err == nil && config != nil {
			topicInfo := &KafkaTopicInfo{
				Name:       name,
				Partitions: config.PartitionCount,
				CreatedAt:  config.CreatedAtNs,
			}
			return topicInfo, true
		}
		glog.V(2).Infof("Failed to get topic configuration for %s from broker: %v", name, err)
	}

	// Fallback: check if topic exists in filer (for backward compatibility)
	if !h.checkTopicInFiler(name) {
		return nil, false
	}

	// Return default info if broker query failed but topic exists in filer
	topicInfo := &KafkaTopicInfo{
		Name:       name,
		Partitions: 1, // Default to 1 partition if broker query failed
		CreatedAt:  0,
	}

	return topicInfo, true
}

// ListTopics returns all topic names from SeaweedMQ broker (includes in-memory topics)
func (h *SeaweedMQHandler) ListTopics() []string {
	// Get topics from SeaweedMQ broker (includes in-memory topics)
	if h.brokerClient != nil {
		topics, err := h.brokerClient.ListTopics()
		if err == nil {
			fmt.Printf("ListTopics: Found %d topics from SMQ broker: %v\n", len(topics), topics)
			return topics
		}
		fmt.Printf("ListTopics: Failed to get topics from SMQ broker: %v\n", err)
	}

	// Return empty list if broker is unavailable
	fmt.Printf("ListTopics: No broker client available, returning empty list\n")
	return []string{}
}

// ProduceRecord publishes a record to SeaweedMQ and lets SMQ generate the offset
func (h *SeaweedMQHandler) ProduceRecord(topic string, partition int32, key []byte, value []byte) (int64, error) {
	fmt.Printf("🔥 PRODUCE DEBUG: Starting ProduceRecord for topic=%s partition=%d\n", topic, partition)
	fmt.Printf("🔥 PRODUCE DEBUG: Key length=%d, Value length=%d\n", len(key), len(value))
	if len(key) > 0 {
		fmt.Printf("🔥 PRODUCE DEBUG: Key content: %s\n", string(key))
	}
	if len(value) > 0 {
		fmt.Printf("🔥 PRODUCE DEBUG: Value content: %s\n", string(value))
	} else {
		fmt.Printf("🔥 PRODUCE DEBUG: Value is empty or nil!\n")
	}

	// Verify topic exists
	if !h.TopicExists(topic) {
		fmt.Printf("🔥 PRODUCE DEBUG: Topic %s does not exist\n", topic)
		return 0, fmt.Errorf("topic %s does not exist", topic)
	}
	fmt.Printf("🔥 PRODUCE DEBUG: Topic %s exists, proceeding\n", topic)

	// Get current timestamp
	timestamp := time.Now().UnixNano()

	// Publish to SeaweedMQ and let SMQ generate the offset
	var smqOffset int64
	var publishErr error
	if h.brokerClient == nil {
		fmt.Printf("🔥 PRODUCE DEBUG: No broker client available\n")
		publishErr = fmt.Errorf("no broker client available")
	} else {
		fmt.Printf("🔥 PRODUCE DEBUG: Calling brokerClient.PublishRecord\n")
		smqOffset, publishErr = h.brokerClient.PublishRecord(topic, partition, key, value, timestamp)
		fmt.Printf("🔥 PRODUCE DEBUG: brokerClient.PublishRecord returned offset=%d, err=%v\n", smqOffset, publishErr)
	}

	if publishErr != nil {
		return 0, fmt.Errorf("failed to publish to SeaweedMQ: %v", publishErr)
	}

	// SMQ should have generated and returned the offset - use it directly as the Kafka offset
	glog.V(2).Infof("Successfully produced record to topic %s partition %d at SMQ offset %d",
		topic, partition, smqOffset)

	// Invalidate HWM cache for this partition to ensure fresh reads
	// This is critical for read-your-own-write scenarios (e.g., Schema Registry)
	cacheKey := fmt.Sprintf("%s:%d", topic, partition)
	h.hwmCacheMu.Lock()
	delete(h.hwmCache, cacheKey)
	h.hwmCacheMu.Unlock()
	glog.V(4).Infof("[DEBUG_OFFSET] Invalidated HWM cache for %s after produce at offset %d", cacheKey, smqOffset)

	return smqOffset, nil
}

// ProduceRecordValue produces a record using RecordValue format to SeaweedMQ
// ALWAYS uses broker's assigned offset - no ledger involved
func (h *SeaweedMQHandler) ProduceRecordValue(topic string, partition int32, key []byte, recordValueBytes []byte) (int64, error) {
	// Verify topic exists
	if !h.TopicExists(topic) {
		return 0, fmt.Errorf("topic %s does not exist", topic)
	}

	// Get current timestamp
	timestamp := time.Now().UnixNano()

	// Publish RecordValue to SeaweedMQ and get the broker-assigned offset
	var smqOffset int64
	var publishErr error
	if h.brokerClient == nil {
		publishErr = fmt.Errorf("no broker client available")
	} else {
		smqOffset, publishErr = h.brokerClient.PublishRecordValue(topic, partition, key, recordValueBytes, timestamp)
	}

	if publishErr != nil {
		return 0, fmt.Errorf("failed to publish RecordValue to SeaweedMQ: %v", publishErr)
	}

	// SMQ broker has assigned the offset - use it directly as the Kafka offset
	glog.V(2).Infof("Successfully produced RecordValue to topic %s partition %d at SMQ offset %d",
		topic, partition, smqOffset)

	// Invalidate HWM cache for this partition to ensure fresh reads
	// This is critical for read-your-own-write scenarios (e.g., Schema Registry)
	cacheKey := fmt.Sprintf("%s:%d", topic, partition)
	h.hwmCacheMu.Lock()
	delete(h.hwmCache, cacheKey)
	h.hwmCacheMu.Unlock()
	glog.V(4).Infof("[DEBUG_OFFSET] Invalidated HWM cache for %s after produce at offset %d", cacheKey, smqOffset)

	return smqOffset, nil
}

// Ledger methods removed - SMQ broker handles all offset management directly

// FetchRecords DEPRECATED - only used in old tests
func (h *SeaweedMQHandler) FetchRecords(topic string, partition int32, fetchOffset int64, maxBytes int32) ([]byte, error) {
	// Verify topic exists
	if !h.TopicExists(topic) {
		return nil, fmt.Errorf("topic %s does not exist", topic)
	}

	// DEPRECATED: This function only used in old tests
	// Get HWM directly from broker
	highWaterMark, err := h.GetLatestOffset(topic, partition)
	if err != nil {
		return nil, err
	}

	// If fetch offset is at or beyond high water mark, no records to return
	if fetchOffset >= highWaterMark {
		return []byte{}, nil
	}

	// Get or create subscriber session for this topic/partition
	var seaweedRecords []*SeaweedRecord

	// Calculate how many records to fetch
	recordsToFetch := int(highWaterMark - fetchOffset)
	if recordsToFetch > 100 {
		recordsToFetch = 100 // Limit batch size
	}

	// Read records using broker client
	if h.brokerClient == nil {
		return nil, fmt.Errorf("no broker client available")
	}
	brokerSubscriber, subErr := h.brokerClient.GetOrCreateSubscriber(topic, partition, fetchOffset)
	if subErr != nil {
		return nil, fmt.Errorf("failed to get broker subscriber: %v", subErr)
	}
	seaweedRecords, err = h.brokerClient.ReadRecords(brokerSubscriber, recordsToFetch)

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

// mapSeaweedToKafkaOffsets maps SeaweedMQ records to proper Kafka offsets
func (h *SeaweedMQHandler) mapSeaweedToKafkaOffsets(topic string, partition int32, seaweedRecords []*SeaweedRecord, startOffset int64) ([]*SeaweedRecord, error) {
	if len(seaweedRecords) == 0 {
		return seaweedRecords, nil
	}

	// DEPRECATED: This function only used in old tests
	// Just map offsets sequentially
	mappedRecords := make([]*SeaweedRecord, 0, len(seaweedRecords))

	for i, seaweedRecord := range seaweedRecords {
		currentKafkaOffset := startOffset + int64(i)

		// Create a copy of the record with proper Kafka offset assignment
		mappedRecord := &SeaweedRecord{
			Key:       seaweedRecord.Key,
			Value:     seaweedRecord.Value,
			Timestamp: seaweedRecord.Timestamp,
			Offset:    currentKafkaOffset,
		}

		// Just skip any error handling since this is deprecated
		{
			// Log warning but continue processing
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
	fmt.Printf("🔥 GATEWAY: Connecting to broker at %s\n", brokerAddress)

	// Create broker client with shared filer accessor
	brokerClient, err := NewBrokerClientWithFilerAccessor(brokerAddress, sharedFilerAccessor)
	if err != nil {
		return nil, fmt.Errorf("failed to create broker client: %v", err)
	}
	fmt.Printf("🔥 GATEWAY: Successfully created broker client to %s\n", brokerAddress)

	// Test the connection
	if err := brokerClient.HealthCheck(); err != nil {
		brokerClient.Close()
		return nil, fmt.Errorf("broker health check failed: %v", err)
	}

	return &SeaweedMQHandler{
		filerClientAccessor: sharedFilerAccessor,
		brokerClient:        brokerClient,
		masterClient:        masterClient,
		// topics map removed - always read from filer directly
		// ledgers removed - SMQ broker handles all offset management
		brokerAddresses:     brokerAddresses, // Store all discovered broker addresses
		hwmCache:            make(map[string]*hwmCacheEntry),
		hwmCacheTTL:         2 * time.Second, // 2 second cache TTL to reduce broker queries
		topicExistsCache:    make(map[string]*topicExistsCacheEntry),
		topicExistsCacheTTL: 5 * time.Second, // 5 second cache TTL for topic existence
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

	fmt.Printf("🔥 FILER DISCOVERY: Requesting filers with filerGroup='%s'\n", filerGroup)

	err := masterClient.WithClient(false, func(client master_pb.SeaweedClient) error {
		resp, err := client.ListClusterNodes(context.Background(), &master_pb.ListClusterNodesRequest{
			ClientType: cluster.FilerType,
			FilerGroup: filerGroup,
			Limit:      1000,
		})
		if err != nil {
			fmt.Printf("🔥 FILER DISCOVERY: ListClusterNodes failed: %v\n", err)
			return err
		}

		fmt.Printf("🔥 FILER DISCOVERY: ListClusterNodes returned %d nodes\n", len(resp.ClusterNodes))

		// Extract filer addresses from response - return as HTTP addresses (pb.ServerAddress)
		for _, node := range resp.ClusterNodes {
			fmt.Printf("🔥 FILER DISCOVERY: Found node - Address=%s, DataCenter=%s\n", node.Address, node.DataCenter)
			if node.Address != "" {
				// Return HTTP address as pb.ServerAddress (no pre-conversion to gRPC)
				httpAddr := pb.ServerAddress(node.Address)
				fmt.Printf("🔥 FILER DISCOVERY: Adding filer HTTP address %s\n", httpAddr)
				filers = append(filers, httpAddr)
			}
		}

		return nil
	})

	fmt.Printf("🔥 FILER DISCOVERY: Returning %d filers, err=%v\n", len(filers), err)
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

// SetProtocolHandler sets the protocol handler reference for accessing connection context
func (h *SeaweedMQHandler) SetProtocolHandler(handler ProtocolHandler) {
	h.protocolHandler = handler
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
	// Context for canceling reads (used for timeout)
	Ctx    context.Context
	Cancel context.CancelFunc
	// Mutex to prevent concurrent reads from the same stream
	mu sync.Mutex
	// Cache of consumed records to avoid re-reading from broker
	consumedRecords  []*SeaweedRecord
	nextOffsetToRead int64
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
	for key, session := range bc.publishers {
		if session.Stream != nil {
			_ = session.Stream.CloseSend()
		}
		delete(bc.publishers, key)
	}
	bc.publishersLock.Unlock()

	// Close all subscriber streams
	bc.subscribersLock.Lock()
	for key, session := range bc.subscribers {
		if session.Stream != nil {
			_ = session.Stream.CloseSend()
		}
		if session.Cancel != nil {
			session.Cancel()
		}
		delete(bc.subscribers, key)
	}
	bc.subscribersLock.Unlock()

	return bc.conn.Close()
}

// GetPartitionRangeInfo gets comprehensive range information from SeaweedMQ broker's native range manager
func (bc *BrokerClient) GetPartitionRangeInfo(topic string, partition int32) (*PartitionRangeInfo, error) {
	glog.V(4).Infof("[DEBUG_OFFSET] GetPartitionRangeInfo called for topic=%s partition=%d", topic, partition)

	if bc.client == nil {
		return nil, fmt.Errorf("broker client not connected")
	}

	// Get the actual partition assignment from the broker instead of hardcoding
	pbTopic := &schema_pb.Topic{
		Namespace: "kafka",
		Name:      topic,
	}

	// Get the actual partition assignment for this Kafka partition
	actualPartition, err := bc.getActualPartitionAssignment(topic, partition)
	if err != nil {
		return nil, fmt.Errorf("failed to get actual partition assignment: %v", err)
	}

	// Call the broker's gRPC method
	resp, err := bc.client.GetPartitionRangeInfo(context.Background(), &mq_pb.GetPartitionRangeInfoRequest{
		Topic:     pbTopic,
		Partition: actualPartition,
	})
	if err != nil {
		glog.V(4).Infof("[DEBUG_OFFSET] Failed to call GetPartitionRangeInfo gRPC: %v", err)
		return nil, fmt.Errorf("failed to get partition range info from broker: %v", err)
	}

	if resp.Error != "" {
		glog.V(4).Infof("[DEBUG_OFFSET] Broker returned error: %s", resp.Error)
		return nil, fmt.Errorf("broker error: %s", resp.Error)
	}

	// Extract offset range information
	var earliestOffset, latestOffset, highWaterMark int64
	if resp.OffsetRange != nil {
		earliestOffset = resp.OffsetRange.EarliestOffset
		latestOffset = resp.OffsetRange.LatestOffset
		highWaterMark = resp.OffsetRange.HighWaterMark
	}

	// Extract timestamp range information
	var earliestTimestampNs, latestTimestampNs int64
	if resp.TimestampRange != nil {
		earliestTimestampNs = resp.TimestampRange.EarliestTimestampNs
		latestTimestampNs = resp.TimestampRange.LatestTimestampNs
	}

	info := &PartitionRangeInfo{
		EarliestOffset:      earliestOffset,
		LatestOffset:        latestOffset,
		HighWaterMark:       highWaterMark,
		EarliestTimestampNs: earliestTimestampNs,
		LatestTimestampNs:   latestTimestampNs,
		RecordCount:         resp.RecordCount,
		ActiveSubscriptions: resp.ActiveSubscriptions,
	}

	glog.V(4).Infof("[DEBUG_OFFSET] Got range info from broker: earliest=%d, latest=%d, hwm=%d, records=%d, ts_range=[%d,%d]",
		info.EarliestOffset, info.LatestOffset, info.HighWaterMark, info.RecordCount,
		info.EarliestTimestampNs, info.LatestTimestampNs)
	return info, nil
}

// GetHighWaterMark gets the high water mark for a topic partition
func (bc *BrokerClient) GetHighWaterMark(topic string, partition int32) (int64, error) {
	glog.V(4).Infof("[DEBUG_OFFSET] GetHighWaterMark called for topic=%s partition=%d", topic, partition)

	// Primary approach: Use SeaweedMQ's native range manager via gRPC
	info, err := bc.GetPartitionRangeInfo(topic, partition)
	if err != nil {
		glog.V(4).Infof("[DEBUG_OFFSET] Failed to get offset info from broker, falling back to chunk metadata: %v", err)
		// Fallback to chunk metadata approach
		highWaterMark, err := bc.getHighWaterMarkFromChunkMetadata(topic, partition)
		if err != nil {
			glog.V(4).Infof("[DEBUG_OFFSET] Failed to get high water mark from chunk metadata: %v", err)
			return 0, err
		}
		glog.V(4).Infof("[DEBUG_OFFSET] Got high water mark from chunk metadata fallback: %d", highWaterMark)
		return highWaterMark, nil
	}

	glog.V(4).Infof("[DEBUG_OFFSET] Successfully got high water mark from broker: %d", info.HighWaterMark)
	return info.HighWaterMark, nil
}

// getOffsetRangeFromChunkMetadata reads chunk metadata to find both earliest and latest offsets
func (bc *BrokerClient) getOffsetRangeFromChunkMetadata(topic string, partition int32) (earliestOffset int64, highWaterMark int64, err error) {
	if bc.filerClientAccessor == nil {
		return 0, 0, fmt.Errorf("filer client not available")
	}

	// Get the topic path and find the latest version
	topicPath := fmt.Sprintf("/topics/kafka/%s", topic)

	// First, list the topic versions to find the latest
	var latestVersion string
	err = bc.filerClientAccessor.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		stream, err := client.ListEntries(context.Background(), &filer_pb.ListEntriesRequest{
			Directory: topicPath,
		})
		if err != nil {
			return err
		}

		for {
			resp, err := stream.Recv()
			if err == io.EOF {
				break
			}
			if err != nil {
				return err
			}
			if resp.Entry.IsDirectory && strings.HasPrefix(resp.Entry.Name, "v") {
				if latestVersion == "" || resp.Entry.Name > latestVersion {
					latestVersion = resp.Entry.Name
				}
			}
		}
		return nil
	})
	if err != nil {
		return 0, 0, fmt.Errorf("failed to list topic versions: %v", err)
	}

	if latestVersion == "" {
		glog.V(4).Infof("[DEBUG_OFFSET] No version directory found for topic %s", topic)
		return 0, 0, nil
	}

	// Find the partition directory
	versionPath := fmt.Sprintf("%s/%s", topicPath, latestVersion)
	var partitionDir string
	err = bc.filerClientAccessor.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		stream, err := client.ListEntries(context.Background(), &filer_pb.ListEntriesRequest{
			Directory: versionPath,
		})
		if err != nil {
			return err
		}

		for {
			resp, err := stream.Recv()
			if err == io.EOF {
				break
			}
			if err != nil {
				return err
			}
			if resp.Entry.IsDirectory && strings.Contains(resp.Entry.Name, "-") {
				partitionDir = resp.Entry.Name
				break // Use the first partition directory we find
			}
		}
		return nil
	})
	if err != nil {
		return 0, 0, fmt.Errorf("failed to list partition directories: %v", err)
	}

	if partitionDir == "" {
		glog.V(4).Infof("[DEBUG_OFFSET] No partition directory found for topic %s", topic)
		return 0, 0, nil
	}

	// Scan all message files to find the highest offset_max and lowest offset_min
	partitionPath := fmt.Sprintf("%s/%s", versionPath, partitionDir)
	highWaterMark = 0
	earliestOffset = -1 // -1 indicates no data found yet

	err = bc.filerClientAccessor.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		stream, err := client.ListEntries(context.Background(), &filer_pb.ListEntriesRequest{
			Directory: partitionPath,
		})
		if err != nil {
			return err
		}

		for {
			resp, err := stream.Recv()
			if err == io.EOF {
				break
			}
			if err != nil {
				return err
			}
			if !resp.Entry.IsDirectory && resp.Entry.Name != "checkpoint.offset" {
				// Check for offset ranges in Extended attributes (both log files and parquet files)
				if resp.Entry.Extended != nil {
					fileType := "log"
					if strings.HasSuffix(resp.Entry.Name, ".parquet") {
						fileType = "parquet"
					}

					// Track maximum offset for high water mark
					if maxOffsetBytes, exists := resp.Entry.Extended[mq.ExtendedAttrOffsetMax]; exists && len(maxOffsetBytes) == 8 {
						maxOffset := int64(binary.BigEndian.Uint64(maxOffsetBytes))
						if maxOffset > highWaterMark {
							highWaterMark = maxOffset
						}
						glog.V(4).Infof("[DEBUG_OFFSET] %s file %s has offset_max=%d", fileType, resp.Entry.Name, maxOffset)
					}

					// Track minimum offset for earliest offset
					if minOffsetBytes, exists := resp.Entry.Extended[mq.ExtendedAttrOffsetMin]; exists && len(minOffsetBytes) == 8 {
						minOffset := int64(binary.BigEndian.Uint64(minOffsetBytes))
						if earliestOffset == -1 || minOffset < earliestOffset {
							earliestOffset = minOffset
						}
						glog.V(4).Infof("[DEBUG_OFFSET] %s file %s has offset_min=%d", fileType, resp.Entry.Name, minOffset)
					}
				}
			}
		}
		return nil
	})
	if err != nil {
		return 0, 0, fmt.Errorf("failed to scan message files: %v", err)
	}

	// High water mark is the next offset after the highest written offset
	if highWaterMark > 0 {
		highWaterMark++
	}

	// If no data found, set earliest offset to 0
	if earliestOffset == -1 {
		earliestOffset = 0
	}

	glog.V(4).Infof("[DEBUG_OFFSET] Offset range for topic %s partition %d: earliest=%d, highWaterMark=%d", topic, partition, earliestOffset, highWaterMark)
	return earliestOffset, highWaterMark, nil
}

// getHighWaterMarkFromChunkMetadata is a wrapper for backward compatibility
func (bc *BrokerClient) getHighWaterMarkFromChunkMetadata(topic string, partition int32) (int64, error) {
	_, highWaterMark, err := bc.getOffsetRangeFromChunkMetadata(topic, partition)
	return highWaterMark, err
}

// GetEarliestOffset gets the earliest offset from SeaweedMQ broker's native offset manager
func (bc *BrokerClient) GetEarliestOffset(topic string, partition int32) (int64, error) {
	glog.V(4).Infof("[DEBUG_OFFSET] BrokerClient.GetEarliestOffset called for topic=%s partition=%d", topic, partition)

	// Primary approach: Use SeaweedMQ's native range manager via gRPC
	info, err := bc.GetPartitionRangeInfo(topic, partition)
	if err != nil {
		glog.V(4).Infof("[DEBUG_OFFSET] Failed to get offset info from broker, falling back to chunk metadata: %v", err)
		// Fallback to chunk metadata approach
		earliestOffset, err := bc.getEarliestOffsetFromChunkMetadata(topic, partition)
		if err != nil {
			glog.V(4).Infof("[DEBUG_OFFSET] Failed to get earliest offset from chunk metadata: %v", err)
			return 0, err
		}
		glog.V(4).Infof("[DEBUG_OFFSET] Got earliest offset from chunk metadata fallback: %d", earliestOffset)
		return earliestOffset, nil
	}

	glog.V(4).Infof("[DEBUG_OFFSET] Successfully got earliest offset from broker: %d", info.EarliestOffset)
	return info.EarliestOffset, nil
}

// getEarliestOffsetFromChunkMetadata gets the earliest offset from chunk metadata (fallback)
func (bc *BrokerClient) getEarliestOffsetFromChunkMetadata(topic string, partition int32) (int64, error) {
	earliestOffset, _, err := bc.getOffsetRangeFromChunkMetadata(topic, partition)
	return earliestOffset, err
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
	fmt.Printf("🔥 BROKER DEBUG: Starting PublishRecord for topic=%s partition=%d\n", topic, partition)

	session, err := bc.getOrCreatePublisher(topic, partition)
	if err != nil {
		fmt.Printf("🔥 BROKER DEBUG: getOrCreatePublisher failed: %v\n", err)
		return 0, err
	}
	fmt.Printf("🔥 BROKER DEBUG: Got publisher session successfully\n")

	// Send data message using broker API format
	dataMsg := &mq_pb.DataMessage{
		Key:   key,
		Value: value,
		TsNs:  timestamp,
	}

	fmt.Printf("🔥 BROKER DEBUG: Sending data message to stream\n")
	fmt.Printf("🔥 BROKER DEBUG: DataMessage Key length=%d, Value length=%d\n", len(dataMsg.Key), len(dataMsg.Value))
	if len(dataMsg.Value) > 0 {
		fmt.Printf("🔥 BROKER DEBUG: DataMessage Value content: %s\n", string(dataMsg.Value))
	} else {
		fmt.Printf("🔥 BROKER DEBUG: DataMessage Value is empty or nil!\n")
	}
	if err := session.Stream.Send(&mq_pb.PublishMessageRequest{
		Message: &mq_pb.PublishMessageRequest_Data{
			Data: dataMsg,
		},
	}); err != nil {
		fmt.Printf("🔥 BROKER DEBUG: Stream.Send failed: %v\n", err)
		return 0, fmt.Errorf("failed to send data: %v", err)
	}
	fmt.Printf("🔥 BROKER DEBUG: Data message sent, waiting for ack\n")

	// Read acknowledgment
	resp, err := session.Stream.Recv()
	if err != nil {
		fmt.Printf("🔥 BROKER DEBUG: Stream.Recv failed: %v\n", err)
		return 0, fmt.Errorf("failed to receive ack: %v", err)
	}
	fmt.Printf("🔥 BROKER DEBUG: Received ack successfully, AssignedOffset=%d\n", resp.AssignedOffset)

	// Handle structured broker errors
	if kafkaErrorCode, errorMsg, handleErr := HandleBrokerResponse(resp); handleErr != nil {
		return 0, handleErr
	} else if kafkaErrorCode != 0 {
		// Return error with Kafka error code information for better debugging
		return 0, fmt.Errorf("broker error (Kafka code %d): %s", kafkaErrorCode, errorMsg)
	}

	// Use the assigned offset from SMQ, not the timestamp
	fmt.Printf("🔥 BROKER DEBUG: Returning offset=%d to caller\n", resp.AssignedOffset)
	return resp.AssignedOffset, nil
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
	fmt.Printf("🔥 PUBLISHER: Creating publish stream to broker=%s for topic=%s partition=%d\n", bc.brokerAddress, topic, partition)
	stream, err := bc.client.PublishMessage(bc.ctx)
	if err != nil {
		fmt.Printf("🔥 PUBLISHER: Failed to create publish stream: %v\n", err)
		return nil, fmt.Errorf("failed to create publish stream: %v", err)
	}
	fmt.Printf("🔥 PUBLISHER: Stream created successfully\n")

	// Get the actual partition assignment from the broker instead of using Kafka partition mapping
	actualPartition, err := bc.getActualPartitionAssignment(topic, partition)
	if err != nil {
		return nil, fmt.Errorf("failed to get actual partition assignment: %v", err)
	}
	fmt.Printf("🔥 PUBLISHER: Sending init message for topic=%s actualPartition=%v\n", topic, actualPartition)

	// Send init message using the actual partition structure that the broker allocated
	if err := stream.Send(&mq_pb.PublishMessageRequest{
		Message: &mq_pb.PublishMessageRequest_Init{
			Init: &mq_pb.PublishMessageRequest_InitMessage{
				Topic: &schema_pb.Topic{
					Namespace: "kafka",
					Name:      topic,
				},
				Partition:     actualPartition,
				AckInterval:   1,
				PublisherName: "kafka-gateway",
			},
		},
	}); err != nil {
		fmt.Printf("🔥 PUBLISHER: Failed to send init message: %v\n", err)
		return nil, fmt.Errorf("failed to send init message: %v", err)
	}
	fmt.Printf("🔥 PUBLISHER: Init message sent successfully\n")

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

	// Calculate expected range for this Kafka partition based on actual partition count
	// Ring is divided equally among partitions, with last partition getting any remainder
	rangeSize := int32(pub_balancer.MaxPartitionCount) / totalPartitions
	expectedRangeStart := kafkaPartition * rangeSize
	var expectedRangeStop int32

	if kafkaPartition == totalPartitions-1 {
		// Last partition gets the remainder to fill the entire ring
		expectedRangeStop = int32(pub_balancer.MaxPartitionCount)
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
// CreateFreshSubscriber creates a new subscriber session without caching
// This ensures each fetch gets fresh data from the requested offset
// consumerGroup and consumerID are passed from Kafka client for proper tracking in SMQ
func (bc *BrokerClient) CreateFreshSubscriber(topic string, partition int32, startOffset int64, consumerGroup string, consumerID string) (*BrokerSubscriberSession, error) {
	glog.Infof("🔍 CreateFreshSubscriber: topic=%s partition=%d startOffset=%d consumerGroup=%s consumerID=%s",
		topic, partition, startOffset, consumerGroup, consumerID)

	// Create a dedicated context for this subscriber
	subscriberCtx := context.Background()

	glog.Infof("🔍 CreateFreshSubscriber: Calling SubscribeMessage for topic=%s", topic)
	stream, err := bc.client.SubscribeMessage(subscriberCtx)
	if err != nil {
		glog.Errorf("🔍 CreateFreshSubscriber: SubscribeMessage FAILED: %v", err)
		return nil, fmt.Errorf("failed to create subscribe stream: %v", err)
	}
	glog.Infof("🔍 CreateFreshSubscriber: SubscribeMessage SUCCESS for topic=%s", topic)

	// Get the actual partition assignment from the broker
	glog.Infof("🔍 CreateFreshSubscriber: Getting actual partition assignment for topic=%s partition=%d", topic, partition)
	actualPartition, err := bc.getActualPartitionAssignment(topic, partition)
	if err != nil {
		glog.Errorf("🔍 CreateFreshSubscriber: getActualPartitionAssignment FAILED: %v", err)
		return nil, fmt.Errorf("failed to get actual partition assignment for subscribe: %v", err)
	}
	glog.Infof("🔍 CreateFreshSubscriber: Got actual partition: %v", actualPartition)

	// Convert Kafka offset to SeaweedMQ OffsetType
	var offsetType schema_pb.OffsetType
	var startTimestamp int64
	var startOffsetValue int64

	// Use EXACT_OFFSET to read from the specific offset
	offsetType = schema_pb.OffsetType_EXACT_OFFSET
	startTimestamp = 0
	startOffsetValue = startOffset

	// Send init message to start subscription with Kafka client's consumer group and ID
	initReq := &mq_pb.SubscribeMessageRequest{
		Message: &mq_pb.SubscribeMessageRequest_Init{
			Init: &mq_pb.SubscribeMessageRequest_InitMessage{
				ConsumerGroup: consumerGroup,
				ConsumerId:    consumerID,
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
				OffsetType:        offsetType,
				SlidingWindowSize: 10,
			},
		},
	}

	glog.Infof("🔍 CreateFreshSubscriber: Sending init request for topic=%s offset=%d", topic, startOffset)
	if err := stream.Send(initReq); err != nil {
		glog.Errorf("🔍 CreateFreshSubscriber: stream.Send FAILED: %v", err)
		return nil, fmt.Errorf("failed to send subscribe init: %v", err)
	}
	glog.Infof("🔍 CreateFreshSubscriber: Init request sent, waiting for response...")

	// IMPORTANT: Don't wait for init response here!
	// The broker may send the first data record as the "init response"
	// If we call Recv() here, we'll consume that first record and ReadRecords will block
	// waiting for the second record, causing a 30-second timeout.
	// Instead, let ReadRecords handle all Recv() calls.
	glog.Infof("🔍 CreateFreshSubscriber: Init request sent, NOT waiting for response (ReadRecords will handle it)")

	session := &BrokerSubscriberSession{
		Stream:      stream,
		Topic:       topic,
		Partition:   partition,
		StartOffset: startOffset,
	}

	glog.Infof("🔍 CreateFreshSubscriber: Session created successfully for topic=%s partition=%d offset=%d", topic, partition, startOffset)
	return session, nil
}

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
				if old.Cancel != nil {
					old.Cancel()
				}
				delete(bc.subscribers, key)
				glog.V(0).Infof("Closed old subscriber session for %s due to offset change", key)
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

	// Create a cancellable context for this subscriber so it can be cleaned up when the connection closes
	subscriberCtx, subscriberCancel := context.WithCancel(bc.ctx)

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

	if startOffset == -1 {
		// Kafka offset -1 typically means "latest"
		offsetType = schema_pb.OffsetType_RESET_TO_LATEST
		startTimestamp = 0   // Not used with RESET_TO_LATEST
		startOffsetValue = 0 // Not used with RESET_TO_LATEST
		glog.V(1).Infof("Using RESET_TO_LATEST for Kafka offset -1 (read latest)")
	} else {
		// CRITICAL FIX: Use EXACT_OFFSET to position subscriber at the exact Kafka offset
		// This allows the subscriber to read from both buffer and disk at the correct position
		offsetType = schema_pb.OffsetType_EXACT_OFFSET
		startTimestamp = 0             // Not used with EXACT_OFFSET
		startOffsetValue = startOffset // Use the exact Kafka offset
		glog.V(1).Infof("Using EXACT_OFFSET for Kafka offset %d (direct positioning)", startOffset)
	}

	glog.V(1).Infof("Creating subscriber for topic=%s partition=%d: Kafka offset %d -> SeaweedMQ %s (timestamp=%d)",
		topic, partition, startOffset, offsetType, startTimestamp)

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
		return nil, fmt.Errorf("failed to send subscribe init: %v", err)
	}

	session := &BrokerSubscriberSession{
		Topic:       topic,
		Partition:   partition,
		Stream:      stream,
		StartOffset: startOffset,
		Ctx:         subscriberCtx,
		Cancel:      subscriberCancel,
	}

	bc.subscribers[key] = session
	glog.V(0).Infof("Created subscriber session for %s with context cancellation support", key)
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

	// Use the assigned offset from SMQ, not the timestamp
	return resp.AssignedOffset, nil
}

// ReadRecords reads available records from the subscriber stream
// Uses a timeout-based approach to read multiple records without blocking indefinitely
func (bc *BrokerClient) ReadRecords(session *BrokerSubscriberSession, maxRecords int) ([]*SeaweedRecord, error) {
	if session == nil {
		return nil, fmt.Errorf("subscriber session cannot be nil")
	}

	// CRITICAL: Lock to prevent concurrent reads from the same stream
	// Multiple Fetch requests may try to read from the same subscriber concurrently,
	// causing the broker to return the same offset repeatedly
	session.mu.Lock()
	defer session.mu.Unlock()

	glog.Infof("[FETCH] ReadRecords: topic=%s partition=%d startOffset=%d maxRecords=%d",
		session.Topic, session.Partition, session.StartOffset, maxRecords)

	var records []*SeaweedRecord
	currentOffset := session.StartOffset

	// CRITICAL FIX: Return immediately if maxRecords is 0 or negative
	if maxRecords <= 0 {
		return records, nil
	}

	// CRITICAL FIX: Use cached records if available to avoid broker tight loop
	// If we've already consumed these records, return them from cache
	if len(session.consumedRecords) > 0 {
		cacheStartOffset := session.consumedRecords[0].Offset
		cacheEndOffset := session.consumedRecords[len(session.consumedRecords)-1].Offset

		if currentOffset >= cacheStartOffset && currentOffset <= cacheEndOffset {
			// Records are in cache
			glog.Infof("[FETCH] Returning cached records: requested offset %d is in cache [%d-%d]",
				currentOffset, cacheStartOffset, cacheEndOffset)

			// Find starting index in cache
			startIdx := int(currentOffset - cacheStartOffset)
			if startIdx < 0 || startIdx >= len(session.consumedRecords) {
				glog.Errorf("[FETCH] Cache index out of bounds: startIdx=%d, cache size=%d", startIdx, len(session.consumedRecords))
				return records, nil
			}

			// Return up to maxRecords from cache
			endIdx := startIdx + maxRecords
			if endIdx > len(session.consumedRecords) {
				endIdx = len(session.consumedRecords)
			}

			glog.Infof("[FETCH] Returning %d cached records from index %d to %d", endIdx-startIdx, startIdx, endIdx-1)
			return session.consumedRecords[startIdx:endIdx], nil
		}
	}

	// Read first record with timeout (important for empty topics)
	// Use longer timeout to avoid creating too many concurrent subscribers
	// Wait up to 10 seconds for first record
	// Broker now properly detects closed subscriptions so this is safe
	firstRecordTimeout := 10 * time.Second
	ctx, cancel := context.WithTimeout(context.Background(), firstRecordTimeout)
	defer cancel()

	type recvResult struct {
		resp *mq_pb.SubscribeMessageResponse
		err  error
	}
	recvChan := make(chan recvResult, 1)

	// Try to receive first record
	go func() {
		resp, err := session.Stream.Recv()
		select {
		case recvChan <- recvResult{resp: resp, err: err}:
		case <-ctx.Done():
			// Context cancelled, don't send (avoid blocking)
		}
	}()

	select {
	case result := <-recvChan:
		if result.err != nil {
			glog.Infof("[FETCH] Stream.Recv() error on first record: %v", result.err)
			return records, nil // Return empty - no error for empty topic
		}

		if dataMsg := result.resp.GetData(); dataMsg != nil {
			record := &SeaweedRecord{
				Key:       dataMsg.Key,
				Value:     dataMsg.Value,
				Timestamp: dataMsg.TsNs,
				Offset:    currentOffset,
			}
			records = append(records, record)
			currentOffset++
			glog.Infof("[FETCH] Received record: offset=%d, keyLen=%d, valueLen=%d",
				record.Offset, len(record.Key), len(record.Value))
		}

	case <-ctx.Done():
		// Timeout on first record - topic is empty or no data available
		glog.V(4).Infof("[FETCH] No data available (timeout on first record)")
		return records, nil
	}

	// If we got the first record, try to get more with shorter timeout
	additionalTimeout := 50 * time.Millisecond
	for len(records) < maxRecords {
		ctx2, cancel2 := context.WithTimeout(context.Background(), additionalTimeout)
		recvChan2 := make(chan recvResult, 1)

		go func() {
			resp, err := session.Stream.Recv()
			select {
			case recvChan2 <- recvResult{resp: resp, err: err}:
			case <-ctx2.Done():
				// Context cancelled
			}
		}()

		select {
		case result := <-recvChan2:
			cancel2()
			if result.err != nil {
				glog.Infof("[FETCH] Stream.Recv() error after %d records: %v", len(records), result.err)
				// Update session offset before returning
				session.StartOffset = currentOffset
				return records, nil
			}

			if dataMsg := result.resp.GetData(); dataMsg != nil {
				record := &SeaweedRecord{
					Key:       dataMsg.Key,
					Value:     dataMsg.Value,
					Timestamp: dataMsg.TsNs,
					Offset:    currentOffset,
				}
				records = append(records, record)
				currentOffset++
				glog.Infof("[FETCH] Received record: offset=%d, keyLen=%d, valueLen=%d",
					record.Offset, len(record.Key), len(record.Value))
			}

		case <-ctx2.Done():
			cancel2()
			// Timeout - return what we have
			glog.V(4).Infof("[FETCH] Read timeout after %d records, returning batch", len(records))
			// CRITICAL: Update session offset so next fetch knows where we left off
			session.StartOffset = currentOffset
			return records, nil
		}
	}

	glog.Infof("[FETCH] ReadRecords returning %d records (maxRecords reached)", len(records))
	// Update session offset after successful read
	session.StartOffset = currentOffset

	// CRITICAL: Cache the consumed records to avoid broker tight loop
	// Append new records to cache (keep last 100 records max)
	session.consumedRecords = append(session.consumedRecords, records...)
	if len(session.consumedRecords) > 100 {
		// Keep only the most recent 100 records
		session.consumedRecords = session.consumedRecords[len(session.consumedRecords)-100:]
	}
	glog.Infof("[FETCH] Updated cache: now contains %d records", len(session.consumedRecords))

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

// ListTopics gets all topics from SeaweedMQ broker (includes in-memory topics)
func (bc *BrokerClient) ListTopics() ([]string, error) {
	if bc.client == nil {
		return nil, fmt.Errorf("broker client not connected")
	}

	ctx, cancel := context.WithTimeout(bc.ctx, 5*time.Second)
	defer cancel()

	resp, err := bc.client.ListTopics(ctx, &mq_pb.ListTopicsRequest{})
	if err != nil {
		return nil, fmt.Errorf("failed to list topics from broker: %v", err)
	}

	var topics []string
	for _, topic := range resp.Topics {
		// Filter for kafka namespace topics
		if topic.Namespace == "kafka" {
			topics = append(topics, topic.Name)
		}
	}

	return topics, nil
}

// GetTopicConfiguration gets topic configuration including partition count from the broker
func (bc *BrokerClient) GetTopicConfiguration(topicName string) (*mq_pb.GetTopicConfigurationResponse, error) {
	if bc.client == nil {
		return nil, fmt.Errorf("broker client not connected")
	}

	ctx, cancel := context.WithTimeout(bc.ctx, 5*time.Second)
	defer cancel()

	resp, err := bc.client.GetTopicConfiguration(ctx, &mq_pb.GetTopicConfigurationRequest{
		Topic: &schema_pb.Topic{
			Namespace: "kafka",
			Name:      topicName,
		},
	})
	if err != nil {
		return nil, fmt.Errorf("failed to get topic configuration from broker: %v", err)
	}

	return resp, nil
}

// TopicExists checks if a topic exists in SeaweedMQ broker (includes in-memory topics)
func (bc *BrokerClient) TopicExists(topicName string) (bool, error) {
	if bc.client == nil {
		return false, fmt.Errorf("broker client not connected")
	}

	ctx, cancel := context.WithTimeout(bc.ctx, 5*time.Second)
	defer cancel()

	resp, err := bc.client.TopicExists(ctx, &mq_pb.TopicExistsRequest{
		Topic: &schema_pb.Topic{
			Namespace: "kafka",
			Name:      topicName,
		},
	})
	if err != nil {
		return false, fmt.Errorf("failed to check topic existence: %v", err)
	}

	return resp.Exists, nil
}
