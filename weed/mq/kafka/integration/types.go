package integration

import (
	"context"
	"fmt"
	"sync"
	"time"

	"google.golang.org/grpc"

	"github.com/seaweedfs/seaweedfs/weed/filer_client"
	"github.com/seaweedfs/seaweedfs/weed/pb/mq_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/schema_pb"
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

// BrokerClient wraps the SeaweedMQ Broker gRPC client for Kafka gateway integration
// FetchRequest tracks an in-flight fetch request with multiple waiters
type FetchRequest struct {
	topic      string
	partition  int32
	offset     int64
	resultChan chan FetchResult   // Single channel for the fetch result
	waiters    []chan FetchResult // Multiple waiters can subscribe
	mu         sync.Mutex
	inProgress bool
}

// FetchResult contains the result of a fetch operation
type FetchResult struct {
	records []*SeaweedRecord
	err     error
}

// partitionAssignmentCacheEntry caches LookupTopicBrokers results
type partitionAssignmentCacheEntry struct {
	assignments []*mq_pb.BrokerPartitionAssignment
	expiresAt   time.Time
}

type BrokerClient struct {
	// Reference to shared filer client accessor
	filerClientAccessor *filer_client.FilerClientAccessor

	brokerAddress string
	conn          *grpc.ClientConn
	client        mq_pb.SeaweedMessagingClient

	// Publisher streams: topic-partition -> stream info
	publishersLock sync.RWMutex
	publishers     map[string]*BrokerPublisherSession

	// Publisher creation locks to prevent concurrent creation attempts for the same topic-partition
	publisherCreationLocks map[string]*sync.Mutex

	// Subscriber streams for offset tracking
	subscribersLock sync.RWMutex
	subscribers     map[string]*BrokerSubscriberSession

	// Request deduplication for stateless fetches
	fetchRequestsLock sync.Mutex
	fetchRequests     map[string]*FetchRequest

	// Partition assignment cache to reduce LookupTopicBrokers calls (13.5% CPU overhead!)
	partitionAssignmentCache    map[string]*partitionAssignmentCacheEntry // Key: topic name
	partitionAssignmentCacheMu  sync.RWMutex
	partitionAssignmentCacheTTL time.Duration

	ctx    context.Context
	cancel context.CancelFunc
}

// BrokerPublisherSession tracks a publishing stream to SeaweedMQ broker
type BrokerPublisherSession struct {
	Topic     string
	Partition int32
	Stream    mq_pb.SeaweedMessaging_PublishMessageClient
	mu        sync.Mutex // Protects Send/Recv pairs from concurrent access
}

// BrokerSubscriberSession tracks a subscription stream for offset management
type BrokerSubscriberSession struct {
	Topic     string
	Partition int32
	Stream    mq_pb.SeaweedMessaging_SubscribeMessageClient
	// Track the requested start offset used to initialize this stream
	StartOffset int64
	// Consumer group identity for this session
	ConsumerGroup string
	ConsumerID    string
	// Context for canceling reads (used for timeout)
	Ctx    context.Context
	Cancel context.CancelFunc
	// Mutex to serialize all operations on this session
	mu sync.Mutex
	// Cache of consumed records to avoid re-reading from broker
	consumedRecords  []*SeaweedRecord
	nextOffsetToRead int64
	// Track what has actually been READ from the stream (not what was requested)
	// This is the HIGHEST offset that has been read from the stream
	// Used to determine if we need to seek or can continue reading
	lastReadOffset int64
	// Flag to indicate if this session has been initialized
	initialized bool
}

// Key generates a unique key for this subscriber session
// Includes consumer group and ID to prevent different consumers from sharing sessions
func (s *BrokerSubscriberSession) Key() string {
	return fmt.Sprintf("%s-%d-%s-%s", s.Topic, s.Partition, s.ConsumerGroup, s.ConsumerID)
}
