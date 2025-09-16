package integration

import (
	"context"
	"fmt"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/seaweedfs/seaweedfs/weed/mq/kafka"
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

// SeaweedRecord represents a record received from SeaweedMQ
type SeaweedRecord struct {
	Key       []byte
	Value     []byte
	Timestamp int64
	Sequence  int64
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
	// Create a basic record type for Kafka messages
	recordType := &schema_pb.RecordType{
		Fields: []*schema_pb.Field{
			{
				Name:       "key",
				FieldIndex: 0,
				Type: &schema_pb.Type{
					Kind: &schema_pb.Type_ScalarType{ScalarType: schema_pb.ScalarType_BYTES},
				},
				IsRequired: false,
				IsRepeated: false,
			},
			{
				Name:       "value",
				FieldIndex: 1,
				Type: &schema_pb.Type{
					Kind: &schema_pb.Type_ScalarType{ScalarType: schema_pb.ScalarType_BYTES},
				},
				IsRequired: true,
				IsRepeated: false,
			},
			{
				Name:       "timestamp",
				FieldIndex: 2,
				Type: &schema_pb.Type{
					Kind: &schema_pb.Type_ScalarType{ScalarType: schema_pb.ScalarType_TIMESTAMP},
				},
				IsRequired: false,
				IsRepeated: false,
			},
		},
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

	// Convert to SeaweedMQ record format
	record := &schema_pb.RecordValue{
		Fields: map[string]*schema_pb.Value{
			"key": {
				Kind: &schema_pb.Value_BytesValue{BytesValue: key},
			},
			"value": {
				Kind: &schema_pb.Value_BytesValue{BytesValue: value},
			},
			"timestamp": {
				Kind: &schema_pb.Value_TimestampValue{
					TimestampValue: &schema_pb.TimestampValue{
						TimestampMicros: timestamp / 1000, // Convert nanoseconds to microseconds
						IsUtc:           true,
					},
				},
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
					// Map Kafka partition to specific SMQ ring range using centralized utility
					Partition: kafka.CreateSMQPartition(partition, time.Now().UnixNano()),
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

// ReadRecords reads available records from the subscriber session
func (ac *AgentClient) ReadRecords(session *SubscriberSession, maxRecords int) ([]*SeaweedRecord, error) {
	if session == nil {
		return nil, fmt.Errorf("subscriber session cannot be nil")
	}

	var records []*SeaweedRecord

	for len(records) < maxRecords {
		// Try to receive a message with timeout to avoid blocking indefinitely
		ctx, cancel := context.WithTimeout(ac.ctx, 100*time.Millisecond)

		select {
		case <-ctx.Done():
			cancel()
			return records, nil // Return what we have so far
		default:
			// Try to receive a record
			resp, err := session.Stream.Recv()
			cancel()

			if err != nil {
				// If we have some records, return them; otherwise return error
				if len(records) > 0 {
					return records, nil
				}
				return nil, fmt.Errorf("failed to receive record: %v", err)
			}

			if resp.Value != nil || resp.Key != nil {
				// Convert SeaweedMQ record to our format
				record := &SeaweedRecord{
					Sequence:  resp.Offset, // Use offset as sequence
					Timestamp: resp.TsNs,   // Timestamp in nanoseconds
					Key:       resp.Key,    // Raw key
				}

				// Extract value from the structured record
				if resp.Value != nil && resp.Value.Fields != nil {
					if valueValue, exists := resp.Value.Fields["value"]; exists && valueValue.GetBytesValue() != nil {
						record.Value = valueValue.GetBytesValue()
					}
					// Also check for key in structured fields if raw key is empty
					if len(record.Key) == 0 {
						if keyValue, exists := resp.Value.Fields["key"]; exists && keyValue.GetBytesValue() != nil {
							record.Key = keyValue.GetBytesValue()
						}
					}
					// Override timestamp if available in structured fields
					if timestampValue, exists := resp.Value.Fields["timestamp"]; exists && timestampValue.GetTimestampValue() != nil {
						record.Timestamp = timestampValue.GetTimestampValue().TimestampMicros * 1000 // Convert to nanoseconds
					}
				}

				records = append(records, record)
			}
		}
	}

	return records, nil
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
