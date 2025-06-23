package integration

import (
	"context"
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/mq/agent"
	"github.com/seaweedfs/seaweedfs/weed/mq/client/pub_client"
	"github.com/seaweedfs/seaweedfs/weed/mq/client/sub_client"
	"github.com/seaweedfs/seaweedfs/weed/mq/schema"
	"github.com/seaweedfs/seaweedfs/weed/mq/topic"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/schema_pb"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// TestEnvironment holds the configuration for the test environment
type TestEnvironment struct {
	Masters      []string
	Brokers      []string
	Filers       []string
	TestTimeout  time.Duration
	CleanupFuncs []func()
	mutex        sync.Mutex
}

// IntegrationTestSuite provides the base test framework
type IntegrationTestSuite struct {
	env         *TestEnvironment
	agents      map[string]*agent.MessageQueueAgent
	publishers  map[string]*pub_client.TopicPublisher
	subscribers map[string]*sub_client.TopicSubscriber
	cleanupOnce sync.Once
	t           *testing.T
}

// NewIntegrationTestSuite creates a new test suite instance
func NewIntegrationTestSuite(t *testing.T) *IntegrationTestSuite {
	env := &TestEnvironment{
		Masters:     getEnvList("SEAWEED_MASTERS", []string{"localhost:19333"}),
		Brokers:     getEnvList("SEAWEED_BROKERS", []string{"localhost:17777"}),
		Filers:      getEnvList("SEAWEED_FILERS", []string{"localhost:18888"}),
		TestTimeout: getEnvDuration("GO_TEST_TIMEOUT", 30*time.Minute),
	}

	return &IntegrationTestSuite{
		env:         env,
		agents:      make(map[string]*agent.MessageQueueAgent),
		publishers:  make(map[string]*pub_client.TopicPublisher),
		subscribers: make(map[string]*sub_client.TopicSubscriber),
		t:           t,
	}
}

// Setup initializes the test environment
func (its *IntegrationTestSuite) Setup() error {
	// Wait for cluster to be ready
	if err := its.waitForClusterReady(); err != nil {
		return fmt.Errorf("cluster not ready: %v", err)
	}

	// Register cleanup
	its.t.Cleanup(its.Cleanup)

	return nil
}

// Cleanup performs cleanup operations
func (its *IntegrationTestSuite) Cleanup() {
	its.cleanupOnce.Do(func() {
		// Close all subscribers (they use context cancellation)
		for name, _ := range its.subscribers {
			its.t.Logf("Cleaned up subscriber: %s", name)
		}

		// Close all publishers
		for name, publisher := range its.publishers {
			if publisher != nil {
				publisher.Shutdown()
				its.t.Logf("Cleaned up publisher: %s", name)
			}
		}

		// Execute additional cleanup functions
		its.env.mutex.Lock()
		for _, cleanup := range its.env.CleanupFuncs {
			cleanup()
		}
		its.env.mutex.Unlock()
	})
}

// CreatePublisher creates a new topic publisher
func (its *IntegrationTestSuite) CreatePublisher(config *PublisherTestConfig) (*pub_client.TopicPublisher, error) {
	publisherConfig := &pub_client.PublisherConfiguration{
		Topic:          topic.NewTopic(config.Namespace, config.TopicName),
		PartitionCount: config.PartitionCount,
		Brokers:        its.env.Brokers,
		PublisherName:  config.PublisherName,
		RecordType:     config.RecordType,
	}

	publisher, err := pub_client.NewTopicPublisher(publisherConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create publisher: %v", err)
	}

	its.publishers[config.PublisherName] = publisher
	return publisher, nil
}

// CreateSubscriber creates a new topic subscriber
func (its *IntegrationTestSuite) CreateSubscriber(config *SubscriberTestConfig) (*sub_client.TopicSubscriber, error) {
	subscriberConfig := &sub_client.SubscriberConfiguration{
		ConsumerGroup:           config.ConsumerGroup,
		ConsumerGroupInstanceId: config.ConsumerInstanceId,
		GrpcDialOption:          grpc.WithTransportCredentials(insecure.NewCredentials()),
		MaxPartitionCount:       config.MaxPartitionCount,
		SlidingWindowSize:       config.SlidingWindowSize,
	}

	contentConfig := &sub_client.ContentConfiguration{
		Topic:            topic.NewTopic(config.Namespace, config.TopicName),
		Filter:           config.Filter,
		PartitionOffsets: config.PartitionOffsets,
		OffsetType:       config.OffsetType,
		OffsetTsNs:       config.OffsetTsNs,
	}

	offsetChan := make(chan sub_client.KeyedOffset, 1024)
	subscriber := sub_client.NewTopicSubscriber(
		context.Background(),
		its.env.Brokers,
		subscriberConfig,
		contentConfig,
		offsetChan,
	)

	its.subscribers[config.ConsumerInstanceId] = subscriber
	return subscriber, nil
}

// CreateAgent creates a new message queue agent
func (its *IntegrationTestSuite) CreateAgent(name string) (*agent.MessageQueueAgent, error) {
	var brokerAddresses []pb.ServerAddress
	for _, broker := range its.env.Brokers {
		brokerAddresses = append(brokerAddresses, pb.ServerAddress(broker))
	}

	agentOptions := &agent.MessageQueueAgentOptions{
		SeedBrokers: brokerAddresses,
	}

	mqAgent := agent.NewMessageQueueAgent(
		agentOptions,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)

	its.agents[name] = mqAgent
	return mqAgent, nil
}

// PublisherTestConfig holds configuration for creating test publishers
type PublisherTestConfig struct {
	Namespace      string
	TopicName      string
	PartitionCount int32
	PublisherName  string
	RecordType     *schema_pb.RecordType
}

// SubscriberTestConfig holds configuration for creating test subscribers
type SubscriberTestConfig struct {
	Namespace          string
	TopicName          string
	ConsumerGroup      string
	ConsumerInstanceId string
	MaxPartitionCount  int32
	SlidingWindowSize  int32
	Filter             string
	PartitionOffsets   []*schema_pb.PartitionOffset
	OffsetType         schema_pb.OffsetType
	OffsetTsNs         int64
}

// TestMessage represents a test message with metadata
type TestMessage struct {
	ID        string
	Content   []byte
	Timestamp time.Time
	Key       []byte
}

// MessageCollector collects received messages for verification
type MessageCollector struct {
	messages []TestMessage
	mutex    sync.RWMutex
	waitCh   chan struct{}
	expected int
}

// NewMessageCollector creates a new message collector
func NewMessageCollector(expectedCount int) *MessageCollector {
	return &MessageCollector{
		messages: make([]TestMessage, 0),
		waitCh:   make(chan struct{}),
		expected: expectedCount,
	}
}

// AddMessage adds a received message to the collector
func (mc *MessageCollector) AddMessage(msg TestMessage) {
	mc.mutex.Lock()
	defer mc.mutex.Unlock()

	mc.messages = append(mc.messages, msg)
	if len(mc.messages) >= mc.expected {
		close(mc.waitCh)
	}
}

// WaitForMessages waits for the expected number of messages or timeout
func (mc *MessageCollector) WaitForMessages(timeout time.Duration) []TestMessage {
	select {
	case <-mc.waitCh:
	case <-time.After(timeout):
	}

	mc.mutex.RLock()
	defer mc.mutex.RUnlock()

	result := make([]TestMessage, len(mc.messages))
	copy(result, mc.messages)
	return result
}

// GetMessages returns all collected messages
func (mc *MessageCollector) GetMessages() []TestMessage {
	mc.mutex.RLock()
	defer mc.mutex.RUnlock()

	result := make([]TestMessage, len(mc.messages))
	copy(result, mc.messages)
	return result
}

// CreateTestSchema creates a simple test schema
func CreateTestSchema() *schema_pb.RecordType {
	return schema.RecordTypeBegin().
		WithField("id", schema.TypeString).
		WithField("timestamp", schema.TypeInt64).
		WithField("content", schema.TypeString).
		WithField("sequence", schema.TypeInt32).
		RecordTypeEnd()
}

// CreateComplexTestSchema creates a complex test schema with nested structures
func CreateComplexTestSchema() *schema_pb.RecordType {
	addressType := schema.RecordTypeBegin().
		WithField("street", schema.TypeString).
		WithField("city", schema.TypeString).
		WithField("zipcode", schema.TypeString).
		RecordTypeEnd()

	return schema.RecordTypeBegin().
		WithField("user_id", schema.TypeString).
		WithField("name", schema.TypeString).
		WithField("age", schema.TypeInt32).
		WithField("emails", schema.ListOf(schema.TypeString)).
		WithRecordField("address", addressType).
		WithField("created_at", schema.TypeInt64).
		RecordTypeEnd()
}

// Helper functions

func getEnvList(key string, defaultValue []string) []string {
	value := os.Getenv(key)
	if value == "" {
		return defaultValue
	}
	return strings.Split(value, ",")
}

func getEnvDuration(key string, defaultValue time.Duration) time.Duration {
	value := os.Getenv(key)
	if value == "" {
		return defaultValue
	}

	duration, err := time.ParseDuration(value)
	if err != nil {
		return defaultValue
	}
	return duration
}

func (its *IntegrationTestSuite) waitForClusterReady() error {
	maxRetries := 30
	retryInterval := 2 * time.Second

	for i := 0; i < maxRetries; i++ {
		if its.isClusterReady() {
			return nil
		}
		its.t.Logf("Waiting for cluster to be ready... attempt %d/%d", i+1, maxRetries)
		time.Sleep(retryInterval)
	}

	return fmt.Errorf("cluster not ready after %d attempts", maxRetries)
}

func (its *IntegrationTestSuite) isClusterReady() bool {
	// Check if at least one broker is accessible
	for _, broker := range its.env.Brokers {
		if its.isBrokerReady(broker) {
			return true
		}
	}
	return false
}

func (its *IntegrationTestSuite) isBrokerReady(broker string) bool {
	// Simple connection test
	conn, err := grpc.NewClient(broker, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return false
	}
	defer conn.Close()

	// TODO: Add actual health check call here
	return true
}

// AssertMessagesReceived verifies that expected messages were received
func (its *IntegrationTestSuite) AssertMessagesReceived(t *testing.T, collector *MessageCollector, expectedCount int, timeout time.Duration) {
	messages := collector.WaitForMessages(timeout)
	require.Len(t, messages, expectedCount, "Expected %d messages, got %d", expectedCount, len(messages))
}

// AssertMessageOrdering verifies that messages are received in the expected order
func (its *IntegrationTestSuite) AssertMessageOrdering(t *testing.T, messages []TestMessage) {
	for i := 1; i < len(messages); i++ {
		require.True(t, messages[i].Timestamp.After(messages[i-1].Timestamp) || messages[i].Timestamp.Equal(messages[i-1].Timestamp),
			"Messages not in chronological order: message %d timestamp %v should be >= message %d timestamp %v",
			i, messages[i].Timestamp, i-1, messages[i-1].Timestamp)
	}
}
