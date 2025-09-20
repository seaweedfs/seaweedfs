package protocol

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"

	"github.com/seaweedfs/seaweedfs/weed/mq/kafka/integration"
	"github.com/seaweedfs/seaweedfs/weed/mq/kafka/offset"
	"github.com/seaweedfs/seaweedfs/weed/mq/kafka/schema"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/mq_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/schema_pb"
)

// TestSchemaPersistenceIntegration tests the complete schema persistence workflow
// via the broker API, including leader-only writes and schema retrieval
func TestSchemaPersistenceIntegration(t *testing.T) {
	// Create mock broker that implements ConfigureTopic and GetTopicConfiguration
	mockBroker := NewMockBrokerForSchemaPersistence(t)
	defer mockBroker.Stop()

	// Create handler with mock coordinator registry (leader)
	handler := createHandlerWithMockLeaderRegistry(t, mockBroker.Address())

	// Test schema registration via broker API
	t.Run("Schema_Registration_Via_Broker_API", func(t *testing.T) {
		topicName := "test-schema-topic"

		// Create test schema (RecordType)
		testSchema := &schema_pb.RecordType{
			Fields: []*schema_pb.Field{
				{
					Name: "id",
					Type: &schema_pb.Type{
						Kind: &schema_pb.Type_ScalarType{
							ScalarType: schema_pb.ScalarType_INT32,
						},
					},
				},
				{
					Name: "name",
					Type: &schema_pb.Type{
						Kind: &schema_pb.Type_ScalarType{
							ScalarType: schema_pb.ScalarType_STRING,
						},
					},
				},
			},
		}

		// Register schema via broker API
		err := handler.registerSchemaViaBrokerAPI(topicName, testSchema)
		if err != nil {
			t.Fatalf("Failed to register schema via broker API: %v", err)
		}

		// Verify schema was persisted in mock broker
		persistedSchema := mockBroker.GetPersistedSchema(topicName)
		if persistedSchema == nil {
			t.Fatal("Schema was not persisted in broker")
		}

		// Verify schema content matches
		if !proto.Equal(persistedSchema, testSchema) {
			t.Error("Persisted schema does not match original schema")
		}

		t.Log("Successfully registered schema via broker API")
	})

	t.Run("Schema_Update_Via_Broker_API", func(t *testing.T) {
		topicName := "test-update-topic"

		// Create initial schema
		initialSchema := &schema_pb.RecordType{
			Fields: []*schema_pb.Field{
				{
					Name: "id",
					Type: &schema_pb.Type{
						Kind: &schema_pb.Type_ScalarType{
							ScalarType: schema_pb.ScalarType_INT32,
						},
					},
				},
			},
		}

		// Register initial schema
		err := handler.registerSchemaViaBrokerAPI(topicName, initialSchema)
		if err != nil {
			t.Fatalf("Failed to register initial schema: %v", err)
		}

		// Create updated schema (add field)
		updatedSchema := &schema_pb.RecordType{
			Fields: []*schema_pb.Field{
				{
					Name: "id",
					Type: &schema_pb.Type{
						Kind: &schema_pb.Type_ScalarType{
							ScalarType: schema_pb.ScalarType_INT32,
						},
					},
				},
				{
					Name: "email",
					Type: &schema_pb.Type{
						Kind: &schema_pb.Type_ScalarType{
							ScalarType: schema_pb.ScalarType_STRING,
						},
					},
				},
			},
		}

		// Update schema
		err = handler.registerSchemaViaBrokerAPI(topicName, updatedSchema)
		if err != nil {
			t.Fatalf("Failed to update schema: %v", err)
		}

		// Verify updated schema was persisted
		persistedSchema := mockBroker.GetPersistedSchema(topicName)
		if persistedSchema == nil {
			t.Fatal("Updated schema was not persisted")
		}

		if len(persistedSchema.Fields) != 2 {
			t.Errorf("Expected 2 fields in updated schema, got %d", len(persistedSchema.Fields))
		}

		t.Log("Successfully updated schema via broker API")
	})

	t.Run("Leader_Only_Schema_Registration", func(t *testing.T) {
		topicName := "test-leader-only-topic"

		// Create handler with non-leader registry
		nonLeaderHandler := createHandlerWithMockNonLeaderRegistry(t, mockBroker.Address())

		testSchema := &schema_pb.RecordType{
			Fields: []*schema_pb.Field{
				{
					Name: "test_field",
					Type: &schema_pb.Type{
						Kind: &schema_pb.Type_ScalarType{
							ScalarType: schema_pb.ScalarType_STRING,
						},
					},
				},
			},
		}

		// Attempt to register schema as non-leader (should be skipped)
		err := nonLeaderHandler.registerSchemaViaBrokerAPI(topicName, testSchema)
		if err != nil {
			t.Fatalf("Non-leader registration should not fail: %v", err)
		}

		// Verify schema was NOT persisted (non-leader should skip)
		persistedSchema := mockBroker.GetPersistedSchema(topicName)
		if persistedSchema != nil {
			t.Error("Non-leader should not persist schema, but schema was found")
		}

		// Now register as leader
		err = handler.registerSchemaViaBrokerAPI(topicName, testSchema)
		if err != nil {
			t.Fatalf("Leader registration failed: %v", err)
		}

		// Verify schema was persisted by leader
		persistedSchema = mockBroker.GetPersistedSchema(topicName)
		if persistedSchema == nil {
			t.Fatal("Leader should have persisted schema, but none found")
		}

		t.Log("Successfully verified leader-only schema registration")
	})
}

// TestSchemaProducePathIntegration tests schema persistence during message production
func TestSchemaProducePathIntegration(t *testing.T) {
	// Create mock schema registry
	mockRegistry := createMockSchemaRegistryServer(t)
	defer mockRegistry.Close()

	// Create mock broker
	mockBroker := NewMockBrokerForSchemaPersistence(t)
	defer mockBroker.Stop()

	// Create handler with schema manager and leader registry
	handler := createHandlerWithSchemaManager(t, mockRegistry.URL, mockBroker.Address())

	t.Run("Schema_Persistence_During_Produce", func(t *testing.T) {
		topicName := "test-produce-schema-topic"

		// Create Confluent-framed Avro message
		testData := map[string]interface{}{
			"id":   int32(123),
			"name": "Test User",
		}

		avroSchema := getTestAvroSchema()
		confluentMsg := createConfluentAvroMessage(t, testData, avroSchema, 1)

		// Simulate produce path - decode schematized message
		decodedMsg, err := handler.schemaManager.DecodeMessage(confluentMsg)
		if err != nil {
			t.Fatalf("Failed to decode message: %v", err)
		}

		// Call the produce path method that should persist schema
		_, err = handler.produceSchemaBasedRecord(topicName, 0, []byte("test-key"), confluentMsg)
		if err != nil {
			t.Fatalf("Failed to produce schema-based record: %v", err)
		}

		// Wait for background schema registration to complete
		time.Sleep(200 * time.Millisecond)

		// Verify schema was persisted via broker API
		persistedSchema := mockBroker.GetPersistedSchema(topicName)
		if persistedSchema == nil {
			t.Fatal("Schema was not persisted during produce operation")
		}

		// Verify the persisted schema matches the decoded schema
		if !proto.Equal(persistedSchema, decodedMsg.RecordType) {
			t.Error("Persisted schema does not match decoded schema from message")
		}

		t.Log("Successfully persisted schema during produce operation")
	})

	t.Run("Schema_Retrieval_After_Persistence", func(t *testing.T) {
		topicName := "test-retrieval-topic"

		// Create and persist a schema
		testSchema := &schema_pb.RecordType{
			Fields: []*schema_pb.Field{
				{
					Name: "user_id",
					Type: &schema_pb.Type{
						Kind: &schema_pb.Type_ScalarType{
							ScalarType: schema_pb.ScalarType_INT64,
						},
					},
				},
				{
					Name: "username",
					Type: &schema_pb.Type{
						Kind: &schema_pb.Type_ScalarType{
							ScalarType: schema_pb.ScalarType_STRING,
						},
					},
				},
			},
		}

		err := handler.registerSchemaViaBrokerAPI(topicName, testSchema)
		if err != nil {
			t.Fatalf("Failed to register schema: %v", err)
		}

		// Simulate schema retrieval (e.g., during fetch operations)
		retrievedConfig := mockBroker.GetTopicConfigurationHelper(topicName)
		if retrievedConfig == nil {
			t.Fatal("Failed to retrieve topic configuration")
		}

		if retrievedConfig.RecordType == nil {
			t.Fatal("Retrieved configuration has no schema")
		}

		// Verify retrieved schema matches original
		if !proto.Equal(retrievedConfig.RecordType, testSchema) {
			t.Error("Retrieved schema does not match original schema")
		}

		t.Log("Successfully retrieved persisted schema")
	})
}

// TestSchemaEvolutionIntegration tests schema evolution scenarios
func TestSchemaEvolutionIntegration(t *testing.T) {
	mockBroker := NewMockBrokerForSchemaPersistence(t)
	defer mockBroker.Stop()

	handler := createHandlerWithMockLeaderRegistry(t, mockBroker.Address())

	t.Run("Schema_Evolution_Workflow", func(t *testing.T) {
		topicName := "test-evolution-topic"

		// Version 1: Basic user schema
		schemaV1 := &schema_pb.RecordType{
			Fields: []*schema_pb.Field{
				{
					Name: "id",
					Type: &schema_pb.Type{
						Kind: &schema_pb.Type_ScalarType{
							ScalarType: schema_pb.ScalarType_INT32,
						},
					},
				},
				{
					Name: "name",
					Type: &schema_pb.Type{
						Kind: &schema_pb.Type_ScalarType{
							ScalarType: schema_pb.ScalarType_STRING,
						},
					},
				},
			},
		}

		// Register V1 schema
		err := handler.registerSchemaViaBrokerAPI(topicName, schemaV1)
		if err != nil {
			t.Fatalf("Failed to register schema V1: %v", err)
		}

		// Version 2: Add optional email field
		schemaV2 := &schema_pb.RecordType{
			Fields: []*schema_pb.Field{
				{
					Name: "id",
					Type: &schema_pb.Type{
						Kind: &schema_pb.Type_ScalarType{
							ScalarType: schema_pb.ScalarType_INT32,
						},
					},
				},
				{
					Name: "name",
					Type: &schema_pb.Type{
						Kind: &schema_pb.Type_ScalarType{
							ScalarType: schema_pb.ScalarType_STRING,
						},
					},
				},
				{
					Name: "email",
					Type: &schema_pb.Type{
						Kind: &schema_pb.Type_ScalarType{
							ScalarType: schema_pb.ScalarType_STRING,
						},
					},
				},
			},
		}

		// Evolve to V2 schema
		err = handler.registerSchemaViaBrokerAPI(topicName, schemaV2)
		if err != nil {
			t.Fatalf("Failed to evolve schema to V2: %v", err)
		}

		// Verify final schema is V2
		persistedSchema := mockBroker.GetPersistedSchema(topicName)
		if persistedSchema == nil {
			t.Fatal("No schema found after evolution")
		}

		if len(persistedSchema.Fields) != 3 {
			t.Errorf("Expected 3 fields in evolved schema, got %d", len(persistedSchema.Fields))
		}

		// Verify field names
		fieldNames := make([]string, len(persistedSchema.Fields))
		for i, field := range persistedSchema.Fields {
			fieldNames[i] = field.Name
		}

		expectedFields := []string{"id", "name", "email"}
		for i, expected := range expectedFields {
			if i >= len(fieldNames) || fieldNames[i] != expected {
				t.Errorf("Expected field %d to be %s, got %s", i, expected, fieldNames[i])
			}
		}

		t.Log("Successfully evolved schema from V1 to V2")
	})
}

// Helper functions and mock implementations

// MockBrokerForSchemaPersistence simulates a SeaweedMQ broker for testing
type MockBrokerForSchemaPersistence struct {
	t            *testing.T
	address      string
	topicConfigs map[string]*mq_pb.ConfigureTopicResponse
	server       *MockGRPCServer
}

func NewMockBrokerForSchemaPersistence(t *testing.T) *MockBrokerForSchemaPersistence {
	// Use a dynamic port to avoid conflicts
	lis, err := net.Listen("tcp", "localhost:0")
	if err != nil {
		t.Fatalf("Failed to listen: %v", err)
	}
	address := lis.Addr().String()
	lis.Close() // Close temporarily, will be reopened by gRPC server

	broker := &MockBrokerForSchemaPersistence{
		t:            t,
		address:      address,
		topicConfigs: make(map[string]*mq_pb.ConfigureTopicResponse),
	}

	// Start mock gRPC server
	broker.server = NewMockGRPCServer(t, broker.address, broker)

	// Give the server a moment to start
	time.Sleep(100 * time.Millisecond)

	return broker
}

func (m *MockBrokerForSchemaPersistence) Address() string {
	return m.address
}

func (m *MockBrokerForSchemaPersistence) Stop() {
	if m.server != nil {
		m.server.Stop()
	}
}

// ConfigureTopic implements the broker's ConfigureTopic method
func (m *MockBrokerForSchemaPersistence) ConfigureTopic(ctx context.Context, req *mq_pb.ConfigureTopicRequest) (*mq_pb.ConfigureTopicResponse, error) {
	topicKey := fmt.Sprintf("%s.%s", req.Topic.Namespace, req.Topic.Name)
	m.t.Logf("MockBroker: ConfigureTopic called for %s with schema: %v", topicKey, req.RecordType != nil)

	// Get existing config or create new one
	config, exists := m.topicConfigs[topicKey]
	if !exists {
		config = &mq_pb.ConfigureTopicResponse{
			BrokerPartitionAssignments: []*mq_pb.BrokerPartitionAssignment{
				{
					Partition: &schema_pb.Partition{
						RingSize:   1,
						RangeStart: 0,
						RangeStop:  1000,
					},
					LeaderBroker:   m.address,
					FollowerBroker: "",
				},
			},
		}
	}

	// Update partition count if specified
	if req.PartitionCount > 0 {
		// Simulate partition assignment logic
		config.BrokerPartitionAssignments = make([]*mq_pb.BrokerPartitionAssignment, req.PartitionCount)
		for i := int32(0); i < req.PartitionCount; i++ {
			config.BrokerPartitionAssignments[i] = &mq_pb.BrokerPartitionAssignment{
				Partition: &schema_pb.Partition{
					RingSize:   req.PartitionCount,
					RangeStart: i * 1000 / req.PartitionCount,
					RangeStop:  (i + 1) * 1000 / req.PartitionCount,
				},
				LeaderBroker:   m.address,
				FollowerBroker: "",
			}
		}
	}

	// Update schema if provided
	if req.RecordType != nil {
		config.RecordType = req.RecordType
	}

	// Update retention if provided
	if req.Retention != nil {
		config.Retention = req.Retention
	}

	// Store updated config
	m.topicConfigs[topicKey] = config

	return config, nil
}

// GetTopicConfiguration implements the broker's GetTopicConfiguration method
func (m *MockBrokerForSchemaPersistence) GetTopicConfiguration(ctx context.Context, req *mq_pb.GetTopicConfigurationRequest) (*mq_pb.GetTopicConfigurationResponse, error) {
	topicKey := fmt.Sprintf("%s.%s", req.Topic.Namespace, req.Topic.Name)

	config, exists := m.topicConfigs[topicKey]
	if !exists {
		return nil, fmt.Errorf("topic not found: %s", topicKey)
	}

	return &mq_pb.GetTopicConfigurationResponse{
		Topic:                      req.Topic,
		PartitionCount:             int32(len(config.BrokerPartitionAssignments)),
		RecordType:                 config.RecordType,
		BrokerPartitionAssignments: config.BrokerPartitionAssignments,
		CreatedAtNs:                time.Now().UnixNano(),
		LastUpdatedNs:              time.Now().UnixNano(),
		Retention:                  config.Retention,
	}, nil
}

// Helper methods for testing
func (m *MockBrokerForSchemaPersistence) GetPersistedSchema(topicName string) *schema_pb.RecordType {
	topicKey := fmt.Sprintf("kafka.%s", topicName)
	config, exists := m.topicConfigs[topicKey]
	if !exists {
		return nil
	}
	return config.RecordType
}

func (m *MockBrokerForSchemaPersistence) GetTopicConfigurationHelper(topicName string) *mq_pb.ConfigureTopicResponse {
	topicKey := fmt.Sprintf("kafka.%s", topicName)
	return m.topicConfigs[topicKey]
}

// Helper functions for creating test handlers and mock registries

func createHandlerWithMockLeaderRegistry(t *testing.T, brokerAddress string) *Handler {
	// Create mock SeaweedMQ handler
	mockSMQHandler := &MockSeaweedMQHandler{
		brokerAddresses: []string{brokerAddress},
	}

	// Create mock coordinator registry (leader)
	mockRegistry := &MockCoordinatorRegistry{
		isLeader: true,
	}

	handler := &Handler{
		seaweedMQHandler:    mockSMQHandler,
		coordinatorRegistry: mockRegistry,
		topicSchemaConfigs:  make(map[string]*TopicSchemaConfig),
		topicMetadataCache:  make(map[string]*CachedTopicMetadata),
	}

	return handler
}

func createHandlerWithMockNonLeaderRegistry(t *testing.T, brokerAddress string) *Handler {
	// Create mock SeaweedMQ handler
	mockSMQHandler := &MockSeaweedMQHandler{
		brokerAddresses: []string{brokerAddress},
	}

	// Create mock coordinator registry (non-leader)
	mockRegistry := &MockCoordinatorRegistry{
		isLeader: false,
	}

	handler := &Handler{
		seaweedMQHandler:    mockSMQHandler,
		coordinatorRegistry: mockRegistry,
		topicSchemaConfigs:  make(map[string]*TopicSchemaConfig),
		topicMetadataCache:  make(map[string]*CachedTopicMetadata),
	}

	return handler
}

func createHandlerWithSchemaManager(t *testing.T, registryURL, brokerAddress string) *Handler {
	// Create schema manager
	config := schema.ManagerConfig{
		RegistryURL:    registryURL,
		ValidationMode: schema.ValidationPermissive,
	}

	schemaManager, err := schema.NewManager(config)
	if err != nil {
		t.Fatalf("Failed to create schema manager: %v", err)
	}

	// Create mock SeaweedMQ handler
	mockSMQHandler := &MockSeaweedMQHandler{
		brokerAddresses: []string{brokerAddress},
	}

	// Create mock coordinator registry (leader)
	mockRegistry := &MockCoordinatorRegistry{
		isLeader: true,
	}

	handler := &Handler{
		seaweedMQHandler:    mockSMQHandler,
		coordinatorRegistry: mockRegistry,
		schemaManager:       schemaManager,
		useSchema:           true,
		topicSchemaConfigs:  make(map[string]*TopicSchemaConfig),
		topicMetadataCache:  make(map[string]*CachedTopicMetadata),
	}

	return handler
}

// Mock implementations

type MockSeaweedMQHandler struct {
	brokerAddresses []string
}

func (m *MockSeaweedMQHandler) GetBrokerAddresses() []string {
	return m.brokerAddresses
}

func (m *MockSeaweedMQHandler) ProduceRecordValue(topicName string, partitionID int32, key []byte, recordValueBytes []byte) (int64, error) {
	// Simulate successful produce
	return time.Now().UnixNano(), nil
}

// Implement other required methods with no-ops for this test
func (m *MockSeaweedMQHandler) TopicExists(topic string) bool                    { return true }
func (m *MockSeaweedMQHandler) ListTopics() []string                             { return []string{} }
func (m *MockSeaweedMQHandler) CreateTopic(topic string, partitions int32) error { return nil }
func (m *MockSeaweedMQHandler) DeleteTopic(topic string) error                   { return nil }
func (m *MockSeaweedMQHandler) GetTopicInfo(topic string) (*integration.KafkaTopicInfo, bool) {
	return nil, false
}
func (m *MockSeaweedMQHandler) GetOrCreateLedger(topic string, partition int32) *offset.Ledger {
	return nil
}
func (m *MockSeaweedMQHandler) GetLedger(topic string, partition int32) *offset.Ledger { return nil }
func (m *MockSeaweedMQHandler) ProduceRecord(topicName string, partitionID int32, key, value []byte) (int64, error) {
	return 0, nil
}
func (m *MockSeaweedMQHandler) GetStoredRecords(topic string, partition int32, fromOffset int64, maxRecords int) ([]offset.SMQRecord, error) {
	return nil, nil
}
func (m *MockSeaweedMQHandler) WithFilerClient(streamingMode bool, fn func(client filer_pb.SeaweedFilerClient) error) error {
	return nil
}
func (m *MockSeaweedMQHandler) Close() error { return nil }

type MockCoordinatorRegistry struct {
	isLeader bool
}

func (m *MockCoordinatorRegistry) IsLeader() bool {
	return m.isLeader
}

func (m *MockCoordinatorRegistry) GetLeaderAddress() string {
	if m.isLeader {
		return "localhost:9092"
	}
	return "other-gateway:9092"
}

func (m *MockCoordinatorRegistry) WaitForLeader(timeout time.Duration) (string, error) {
	return m.GetLeaderAddress(), nil
}

func (m *MockCoordinatorRegistry) AssignCoordinator(consumerGroup string, requestingGateway string) (*CoordinatorAssignment, error) {
	return &CoordinatorAssignment{
		ConsumerGroup:     consumerGroup,
		CoordinatorAddr:   requestingGateway,
		CoordinatorNodeID: 1,
		AssignedAt:        time.Now(),
		LastHeartbeat:     time.Now(),
	}, nil
}

func (m *MockCoordinatorRegistry) GetCoordinator(consumerGroup string) (*CoordinatorAssignment, error) {
	return &CoordinatorAssignment{
		ConsumerGroup:     consumerGroup,
		CoordinatorAddr:   "localhost:9092",
		CoordinatorNodeID: 1,
		AssignedAt:        time.Now(),
		LastHeartbeat:     time.Now(),
	}, nil
}

func (m *MockCoordinatorRegistry) RegisterGateway(gatewayAddress string) error {
	return nil
}

func (m *MockCoordinatorRegistry) HeartbeatGateway(gatewayAddress string) error {
	return nil
}

// Test helper functions

func getTestAvroSchema() string {
	return `{
		"type": "record",
		"name": "TestUser",
		"fields": [
			{"name": "id", "type": "int"},
			{"name": "name", "type": "string"}
		]
	}`
}

func createConfluentAvroMessage(t *testing.T, data map[string]interface{}, avroSchema string, schemaID uint32) []byte {
	// This would normally use the Avro codec to create a proper Confluent message
	// For testing purposes, we'll create a simplified version
	// In a real implementation, this would use goavro to encode the data

	// Create a mock Confluent envelope
	payload := []byte(`{"id": 123, "name": "Test User"}`) // Simplified JSON payload
	return schema.CreateConfluentEnvelope(schema.FormatAvro, schemaID, nil, payload)
}

func createMockSchemaRegistryServer(t *testing.T) *httptest.Server {
	// This would be similar to the existing mock schema registry
	// but simplified for this test
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/schemas/ids/1":
			response := map[string]interface{}{
				"schema":  getTestAvroSchema(),
				"subject": "test-user-value",
				"version": 1,
			}
			json.NewEncoder(w).Encode(response)
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
}

// MockGRPCServer provides a mock gRPC server for testing
type MockGRPCServer struct {
	mq_pb.UnimplementedSeaweedMessagingServer
	server *grpc.Server
	broker *MockBrokerForSchemaPersistence
}

func NewMockGRPCServer(t *testing.T, address string, broker *MockBrokerForSchemaPersistence) *MockGRPCServer {
	lis, err := net.Listen("tcp", address)
	if err != nil {
		t.Fatalf("Failed to listen on %s: %v", address, err)
	}

	server := grpc.NewServer()
	mockServer := &MockGRPCServer{
		server: server,
		broker: broker,
	}

	// Register the broker service
	mq_pb.RegisterSeaweedMessagingServer(server, mockServer)

	go func() {
		if err := server.Serve(lis); err != nil {
			t.Logf("Mock gRPC server error: %v", err)
		}
	}()

	return mockServer
}

func (m *MockGRPCServer) Stop() {
	if m.server != nil {
		m.server.Stop()
	}
}

// Implement SeaweedMessagingServer interface methods
func (m *MockGRPCServer) ConfigureTopic(ctx context.Context, req *mq_pb.ConfigureTopicRequest) (*mq_pb.ConfigureTopicResponse, error) {
	return m.broker.ConfigureTopic(ctx, req)
}

func (m *MockGRPCServer) GetTopicConfiguration(ctx context.Context, req *mq_pb.GetTopicConfigurationRequest) (*mq_pb.GetTopicConfigurationResponse, error) {
	return m.broker.GetTopicConfiguration(ctx, req)
}

// Implement other required methods as no-ops
func (m *MockGRPCServer) LookupTopicBrokers(ctx context.Context, req *mq_pb.LookupTopicBrokersRequest) (*mq_pb.LookupTopicBrokersResponse, error) {
	return &mq_pb.LookupTopicBrokersResponse{}, nil
}

func (m *MockGRPCServer) ListTopics(ctx context.Context, req *mq_pb.ListTopicsRequest) (*mq_pb.ListTopicsResponse, error) {
	return &mq_pb.ListTopicsResponse{}, nil
}

func (m *MockGRPCServer) GetTopicPublishers(ctx context.Context, req *mq_pb.GetTopicPublishersRequest) (*mq_pb.GetTopicPublishersResponse, error) {
	return &mq_pb.GetTopicPublishersResponse{}, nil
}

func (m *MockGRPCServer) PublishMessage(stream mq_pb.SeaweedMessaging_PublishMessageServer) error {
	return nil
}

func (m *MockGRPCServer) SubscribeMessage(stream mq_pb.SeaweedMessaging_SubscribeMessageServer) error {
	return nil
}

func (m *MockGRPCServer) PublishFollowMe(stream mq_pb.SeaweedMessaging_PublishFollowMeServer) error {
	return nil
}

func (m *MockGRPCServer) SubscribeFollowMe(stream mq_pb.SeaweedMessaging_SubscribeFollowMeServer) error {
	return nil
}

func (m *MockGRPCServer) PublisherToPubBalancer(stream mq_pb.SeaweedMessaging_PublisherToPubBalancerServer) error {
	return nil
}

func (m *MockGRPCServer) AssignTopicPartitions(ctx context.Context, req *mq_pb.AssignTopicPartitionsRequest) (*mq_pb.AssignTopicPartitionsResponse, error) {
	return &mq_pb.AssignTopicPartitionsResponse{}, nil
}

func (m *MockGRPCServer) SubscriberToSubCoordinator(stream mq_pb.SeaweedMessaging_SubscriberToSubCoordinatorServer) error {
	return nil
}

// Additional required methods
func (m *MockGRPCServer) FindBrokerLeader(ctx context.Context, req *mq_pb.FindBrokerLeaderRequest) (*mq_pb.FindBrokerLeaderResponse, error) {
	return &mq_pb.FindBrokerLeaderResponse{}, nil
}

func (m *MockGRPCServer) BalanceTopics(ctx context.Context, req *mq_pb.BalanceTopicsRequest) (*mq_pb.BalanceTopicsResponse, error) {
	return &mq_pb.BalanceTopicsResponse{}, nil
}

func (m *MockGRPCServer) GetTopicSubscribers(ctx context.Context, req *mq_pb.GetTopicSubscribersRequest) (*mq_pb.GetTopicSubscribersResponse, error) {
	return &mq_pb.GetTopicSubscribersResponse{}, nil
}

func (m *MockGRPCServer) ClosePublishers(ctx context.Context, req *mq_pb.ClosePublishersRequest) (*mq_pb.ClosePublishersResponse, error) {
	return &mq_pb.ClosePublishersResponse{}, nil
}

func (m *MockGRPCServer) CloseSubscribers(ctx context.Context, req *mq_pb.CloseSubscribersRequest) (*mq_pb.CloseSubscribersResponse, error) {
	return &mq_pb.CloseSubscribersResponse{}, nil
}

func (m *MockGRPCServer) GetUnflushedMessages(req *mq_pb.GetUnflushedMessagesRequest, stream mq_pb.SeaweedMessaging_GetUnflushedMessagesServer) error {
	return nil
}
