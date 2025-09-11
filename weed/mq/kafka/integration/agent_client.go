package integration

import (
	"context"
	"fmt"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/seaweedfs/seaweedfs/weed/mq/kafka/offset"
	"github.com/seaweedfs/seaweedfs/weed/pb/mq_agent_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/schema_pb"
)

// AgentClient wraps the SeaweedMQ Agent gRPC client for Kafka gateway integration
type AgentClient struct {
	agentAddress string
	conn         *grpc.ClientConn
	client       mq_agent_pb.SeaweedMessagingAgentClient

	// Publisher sessions: topic-partition -> session info
	publishersLock sync.RWMutex
	publishers     map[string]*PublisherSession

	// Subscriber sessions for offset tracking
	subscribersLock sync.RWMutex
	subscribers     map[string]*SubscriberSession

	ctx    context.Context
	cancel context.CancelFunc
}

// PublisherSession tracks a publishing session to SeaweedMQ
type PublisherSession struct {
	SessionID    int64
	Topic        string
	Partition    int32
	Stream       mq_agent_pb.SeaweedMessagingAgent_PublishRecordClient
	RecordType   *schema_pb.RecordType
	LastSequence int64
}

// SubscriberSession tracks a subscription for offset management
type SubscriberSession struct {
	Topic        string
	Partition    int32
	Stream       mq_agent_pb.SeaweedMessagingAgent_SubscribeRecordClient
	OffsetLedger *offset.Ledger // Still use for Kafka offset translation
}

// NewAgentClient creates a new SeaweedMQ Agent client
func NewAgentClient(agentAddress string) (*AgentClient, error) {
	ctx, cancel := context.WithCancel(context.Background())

	conn, err := grpc.DialContext(ctx, agentAddress,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		// Don't block - fail fast for invalid addresses
	)
	if err != nil {
		cancel()
		return nil, fmt.Errorf("failed to connect to agent %s: %v", agentAddress, err)
	}

	client := mq_agent_pb.NewSeaweedMessagingAgentClient(conn)

	return &AgentClient{
		agentAddress: agentAddress,
		conn:         conn,
		client:       client,
		publishers:   make(map[string]*PublisherSession),
		subscribers:  make(map[string]*SubscriberSession),
		ctx:          ctx,
		cancel:       cancel,
	}, nil
}

// Close shuts down the agent client and all sessions
func (ac *AgentClient) Close() error {
	ac.cancel()

	// Close all publisher sessions
	ac.publishersLock.Lock()
	for key, session := range ac.publishers {
		ac.closePublishSessionLocked(session.SessionID)
		delete(ac.publishers, key)
	}
	ac.publishersLock.Unlock()

	// Close all subscriber sessions
	ac.subscribersLock.Lock()
	for key, session := range ac.subscribers {
		if session.Stream != nil {
			session.Stream.CloseSend()
		}
		delete(ac.subscribers, key)
	}
	ac.subscribersLock.Unlock()

	return ac.conn.Close()
}

// GetOrCreatePublisher gets or creates a publisher session for a topic-partition
func (ac *AgentClient) GetOrCreatePublisher(topic string, partition int32) (*PublisherSession, error) {
	key := fmt.Sprintf("%s-%d", topic, partition)

	// Try to get existing publisher
	ac.publishersLock.RLock()
	if session, exists := ac.publishers[key]; exists {
		ac.publishersLock.RUnlock()
		return session, nil
	}
	ac.publishersLock.RUnlock()

	// Create new publisher session
	ac.publishersLock.Lock()
	defer ac.publishersLock.Unlock()

	// Double-check after acquiring write lock
	if session, exists := ac.publishers[key]; exists {
		return session, nil
	}

	// Create the session
	session, err := ac.createPublishSession(topic, partition)
	if err != nil {
		return nil, err
	}

	ac.publishers[key] = session
	return session, nil
}

// createPublishSession creates a new publishing session with SeaweedMQ Agent
func (ac *AgentClient) createPublishSession(topic string, partition int32) (*PublisherSession, error) {
	// Create comprehensive Kafka record schema for SeaweedMQ
	recordType := ac.createKafkaRecordSchema()

	// Check if topic already exists in SeaweedMQ, create if needed
	if err := ac.ensureTopicExists(topic, recordType); err != nil {
		return nil, fmt.Errorf("failed to ensure topic exists: %v", err)
	}

	// Start publish session
	startReq := &mq_agent_pb.StartPublishSessionRequest{
		Topic: &schema_pb.Topic{
			Namespace: "kafka", // Use "kafka" namespace for Kafka messages
			Name:      topic,
		},
		PartitionCount: 1, // For Phase 2, use single partition
		RecordType:     recordType,
		PublisherName:  "kafka-gateway",
	}

	startResp, err := ac.client.StartPublishSession(ac.ctx, startReq)
	if err != nil {
		return nil, fmt.Errorf("failed to start publish session: %v", err)
	}

	if startResp.Error != "" {
		return nil, fmt.Errorf("publish session error: %s", startResp.Error)
	}

	// Create streaming connection
	stream, err := ac.client.PublishRecord(ac.ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to create publish stream: %v", err)
	}

	session := &PublisherSession{
		SessionID:  startResp.SessionId,
		Topic:      topic,
		Partition:  partition,
		Stream:     stream,
		RecordType: recordType,
	}

	return session, nil
}

// PublishRecord publishes a single record to SeaweedMQ
func (ac *AgentClient) PublishRecord(topic string, partition int32, key []byte, value []byte, timestamp int64) (int64, error) {
	session, err := ac.GetOrCreatePublisher(topic, partition)
	if err != nil {
		return 0, err
	}

	// Convert to SeaweedMQ record format using enhanced Kafka schema
	record := &schema_pb.RecordValue{
		Fields: map[string]*schema_pb.Value{
			"kafka_key": {
				Kind: &schema_pb.Value_BytesValue{BytesValue: key},
			},
			"kafka_value": {
				Kind: &schema_pb.Value_BytesValue{BytesValue: value},
			},
			"kafka_timestamp": {
				Kind: &schema_pb.Value_TimestampValue{
					TimestampValue: &schema_pb.TimestampValue{
						TimestampMicros: timestamp / 1000, // Convert nanoseconds to microseconds
						IsUtc:           true,
					},
				},
			},
			"kafka_headers": {
				Kind: &schema_pb.Value_BytesValue{BytesValue: []byte{}}, // Empty headers for now
			},
			"kafka_offset": {
				Kind: &schema_pb.Value_Int64Value{Int64Value: 0}, // Will be set by SeaweedMQ
			},
			"kafka_partition": {
				Kind: &schema_pb.Value_Int32Value{Int32Value: partition},
			},
		},
	}

	// Send publish request
	req := &mq_agent_pb.PublishRecordRequest{
		SessionId: session.SessionID,
		Key:       key,
		Value:     record,
	}

	if err := session.Stream.Send(req); err != nil {
		return 0, fmt.Errorf("failed to send record: %v", err)
	}

	// Read acknowledgment (this is a streaming API, so we should read the response)
	resp, err := session.Stream.Recv()
	if err != nil {
		return 0, fmt.Errorf("failed to receive ack: %v", err)
	}

	if resp.Error != "" {
		return 0, fmt.Errorf("publish error: %s", resp.Error)
	}

	session.LastSequence = resp.AckSequence
	return resp.AckSequence, nil
}

// GetOrCreateSubscriber gets or creates a subscriber for offset tracking
func (ac *AgentClient) GetOrCreateSubscriber(topic string, partition int32, startOffset int64) (*SubscriberSession, error) {
	key := fmt.Sprintf("%s-%d", topic, partition)

	ac.subscribersLock.RLock()
	if session, exists := ac.subscribers[key]; exists {
		ac.subscribersLock.RUnlock()
		return session, nil
	}
	ac.subscribersLock.RUnlock()

	// Create new subscriber session
	ac.subscribersLock.Lock()
	defer ac.subscribersLock.Unlock()

	if session, exists := ac.subscribers[key]; exists {
		return session, nil
	}

	session, err := ac.createSubscribeSession(topic, partition, startOffset)
	if err != nil {
		return nil, err
	}

	ac.subscribers[key] = session
	return session, nil
}

// createSubscribeSession creates a subscriber session for reading messages
func (ac *AgentClient) createSubscribeSession(topic string, partition int32, startOffset int64) (*SubscriberSession, error) {
	stream, err := ac.client.SubscribeRecord(ac.ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to create subscribe stream: %v", err)
	}

	// Send initial subscribe request
	initReq := &mq_agent_pb.SubscribeRecordRequest{
		Init: &mq_agent_pb.SubscribeRecordRequest_InitSubscribeRecordRequest{
			ConsumerGroup:           "kafka-gateway",
			ConsumerGroupInstanceId: fmt.Sprintf("kafka-gateway-%s-%d", topic, partition),
			Topic: &schema_pb.Topic{
				Namespace: "kafka",
				Name:      topic,
			},
			PartitionOffsets: []*schema_pb.PartitionOffset{
				{
					Partition: &schema_pb.Partition{
						RingSize:   1024, // Standard ring size
						RangeStart: 0,
						RangeStop:  1023,
					},
					StartTsNs: startOffset, // Use offset as timestamp for now
				},
			},
			OffsetType:              schema_pb.OffsetType_EXACT_TS_NS,
			MaxSubscribedPartitions: 1,
			SlidingWindowSize:       10,
		},
	}

	if err := stream.Send(initReq); err != nil {
		return nil, fmt.Errorf("failed to send subscribe init: %v", err)
	}

	session := &SubscriberSession{
		Topic:        topic,
		Partition:    partition,
		Stream:       stream,
		OffsetLedger: offset.NewLedger(), // Keep Kafka offset tracking
	}

	return session, nil
}

// ClosePublisher closes a specific publisher session
func (ac *AgentClient) ClosePublisher(topic string, partition int32) error {
	key := fmt.Sprintf("%s-%d", topic, partition)

	ac.publishersLock.Lock()
	defer ac.publishersLock.Unlock()

	session, exists := ac.publishers[key]
	if !exists {
		return nil // Already closed or never existed
	}

	err := ac.closePublishSessionLocked(session.SessionID)
	delete(ac.publishers, key)
	return err
}

// closePublishSessionLocked closes a publish session (must be called with lock held)
func (ac *AgentClient) closePublishSessionLocked(sessionID int64) error {
	closeReq := &mq_agent_pb.ClosePublishSessionRequest{
		SessionId: sessionID,
	}

	_, err := ac.client.ClosePublishSession(ac.ctx, closeReq)
	return err
}

// HealthCheck verifies the agent connection is working
func (ac *AgentClient) HealthCheck() error {
	// Create a timeout context for health check
	ctx, cancel := context.WithTimeout(ac.ctx, 2*time.Second)
	defer cancel()

	// Try to start and immediately close a dummy session
	req := &mq_agent_pb.StartPublishSessionRequest{
		Topic: &schema_pb.Topic{
			Namespace: "kafka",
			Name:      "_health_check",
		},
		PartitionCount: 1,
		RecordType: &schema_pb.RecordType{
			Fields: []*schema_pb.Field{
				{
					Name:       "test",
					FieldIndex: 0,
					Type: &schema_pb.Type{
						Kind: &schema_pb.Type_ScalarType{ScalarType: schema_pb.ScalarType_STRING},
					},
				},
			},
		},
		PublisherName: "health-check",
	}

	resp, err := ac.client.StartPublishSession(ctx, req)
	if err != nil {
		return fmt.Errorf("health check failed: %v", err)
	}

	if resp.Error != "" {
		return fmt.Errorf("health check error: %s", resp.Error)
	}

	// Close the health check session
	closeReq := &mq_agent_pb.ClosePublishSessionRequest{
		SessionId: resp.SessionId,
	}
	_, _ = ac.client.ClosePublishSession(ctx, closeReq)

	return nil
}

// createKafkaRecordSchema creates a comprehensive schema for Kafka messages in SeaweedMQ
func (ac *AgentClient) createKafkaRecordSchema() *schema_pb.RecordType {
	return &schema_pb.RecordType{
		Fields: []*schema_pb.Field{
			{
				Name:       "kafka_key",
				FieldIndex: 0,
				Type: &schema_pb.Type{
					Kind: &schema_pb.Type_ScalarType{ScalarType: schema_pb.ScalarType_BYTES},
				},
				IsRequired: false,
				IsRepeated: false,
			},
			{
				Name:       "kafka_value",
				FieldIndex: 1,
				Type: &schema_pb.Type{
					Kind: &schema_pb.Type_ScalarType{ScalarType: schema_pb.ScalarType_BYTES},
				},
				IsRequired: true,
				IsRepeated: false,
			},
			{
				Name:       "kafka_timestamp",
				FieldIndex: 2,
				Type: &schema_pb.Type{
					Kind: &schema_pb.Type_ScalarType{ScalarType: schema_pb.ScalarType_TIMESTAMP},
				},
				IsRequired: false,
				IsRepeated: false,
			},
			{
				Name:       "kafka_headers",
				FieldIndex: 3,
				Type: &schema_pb.Type{
					Kind: &schema_pb.Type_ScalarType{ScalarType: schema_pb.ScalarType_BYTES},
				},
				IsRequired: false,
				IsRepeated: false,
			},
			{
				Name:       "kafka_offset",
				FieldIndex: 4,
				Type: &schema_pb.Type{
					Kind: &schema_pb.Type_ScalarType{ScalarType: schema_pb.ScalarType_INT64},
				},
				IsRequired: false,
				IsRepeated: false,
			},
			{
				Name:       "kafka_partition",
				FieldIndex: 5,
				Type: &schema_pb.Type{
					Kind: &schema_pb.Type_ScalarType{ScalarType: schema_pb.ScalarType_INT32},
				},
				IsRequired: false,
				IsRepeated: false,
			},
		},
	}
}

// ensureTopicExists checks if topic exists in SeaweedMQ and creates it if needed
func (ac *AgentClient) ensureTopicExists(topic string, recordType *schema_pb.RecordType) error {
	// For Phase 1, we'll rely on SeaweedMQ's auto-creation during publish
	// In Phase 3, we'll implement proper topic discovery and creation
	return nil
}

// CreateTopicWithSchema creates a topic in SeaweedMQ with the specified schema
func (ac *AgentClient) CreateTopicWithSchema(topic string, partitions int32, recordType *schema_pb.RecordType) error {
	// This will be implemented in Phase 3 when we integrate with CreateTopics API
	// For now, topics are auto-created during first publish
	return nil
}
