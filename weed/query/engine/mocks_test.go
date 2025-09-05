package engine

import (
	"context"
	"fmt"

	"github.com/seaweedfs/seaweedfs/weed/mq/topic"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/schema_pb"
	util_http "github.com/seaweedfs/seaweedfs/weed/util/http"
	"google.golang.org/protobuf/proto"
)

// NewTestSchemaCatalog creates a schema catalog for testing with sample data
// Uses mock clients instead of real service connections
func NewTestSchemaCatalog() *SchemaCatalog {
	catalog := &SchemaCatalog{
		databases:             make(map[string]*DatabaseInfo),
		currentDatabase:       "default",
		brokerClient:          NewMockBrokerClient(), // Use mock instead of nil
		defaultPartitionCount: 6,                     // Default partition count for tests
	}

	// Pre-populate with sample data to avoid service discovery requirements
	initTestSampleData(catalog)
	return catalog
}

// initTestSampleData populates the catalog with sample schema data for testing
// This function is only available in test builds and not in production
func initTestSampleData(c *SchemaCatalog) {
	// Create sample databases and tables
	c.databases["default"] = &DatabaseInfo{
		Name: "default",
		Tables: map[string]*TableInfo{
			"user_events": {
				Name: "user_events",
				Columns: []ColumnInfo{
					{Name: "user_id", Type: "VARCHAR(100)", Nullable: true},
					{Name: "event_type", Type: "VARCHAR(50)", Nullable: true},
					{Name: "data", Type: "TEXT", Nullable: true},
					// System columns - hidden by default in SELECT *
					{Name: SW_COLUMN_NAME_TIMESTAMP, Type: "BIGINT", Nullable: false},
					{Name: SW_COLUMN_NAME_KEY, Type: "VARCHAR(255)", Nullable: true},
					{Name: SW_COLUMN_NAME_SOURCE, Type: "VARCHAR(50)", Nullable: false},
				},
			},
			"system_logs": {
				Name: "system_logs",
				Columns: []ColumnInfo{
					{Name: "level", Type: "VARCHAR(10)", Nullable: true},
					{Name: "message", Type: "TEXT", Nullable: true},
					{Name: "service", Type: "VARCHAR(50)", Nullable: true},
					// System columns
					{Name: SW_COLUMN_NAME_TIMESTAMP, Type: "BIGINT", Nullable: false},
					{Name: SW_COLUMN_NAME_KEY, Type: "VARCHAR(255)", Nullable: true},
					{Name: SW_COLUMN_NAME_SOURCE, Type: "VARCHAR(50)", Nullable: false},
				},
			},
		},
	}

	c.databases["test"] = &DatabaseInfo{
		Name: "test",
		Tables: map[string]*TableInfo{
			"test-topic": {
				Name: "test-topic",
				Columns: []ColumnInfo{
					{Name: "id", Type: "INT", Nullable: true},
					{Name: "name", Type: "VARCHAR(100)", Nullable: true},
					{Name: "value", Type: "DOUBLE", Nullable: true},
					// System columns
					{Name: SW_COLUMN_NAME_TIMESTAMP, Type: "BIGINT", Nullable: false},
					{Name: SW_COLUMN_NAME_KEY, Type: "VARCHAR(255)", Nullable: true},
					{Name: SW_COLUMN_NAME_SOURCE, Type: "VARCHAR(50)", Nullable: false},
				},
			},
		},
	}
}

// NewTestSQLEngine creates a new SQL execution engine for testing
// Does not attempt to connect to real SeaweedFS services
func NewTestSQLEngine() *SQLEngine {
	// Initialize global HTTP client if not already done
	// This is needed for reading partition data from the filer
	if util_http.GetGlobalHttpClient() == nil {
		util_http.InitGlobalHttpClient()
	}

	return &SQLEngine{
		catalog: NewTestSchemaCatalog(),
	}
}

// MockBrokerClient implements BrokerClient interface for testing
type MockBrokerClient struct {
	namespaces  []string
	topics      map[string][]string              // namespace -> topics
	schemas     map[string]*schema_pb.RecordType // "namespace.topic" -> schema
	shouldFail  bool
	failMessage string
}

// NewMockBrokerClient creates a new mock broker client with sample data
func NewMockBrokerClient() *MockBrokerClient {
	client := &MockBrokerClient{
		namespaces: []string{"default", "test"},
		topics: map[string][]string{
			"default": {"user_events", "system_logs"},
			"test":    {"test-topic"},
		},
		schemas: make(map[string]*schema_pb.RecordType),
	}

	// Add sample schemas
	client.schemas["default.user_events"] = &schema_pb.RecordType{
		Fields: []*schema_pb.Field{
			{Name: "user_id", Type: &schema_pb.Type{Kind: &schema_pb.Type_ScalarType{ScalarType: schema_pb.ScalarType_STRING}}},
			{Name: "event_type", Type: &schema_pb.Type{Kind: &schema_pb.Type_ScalarType{ScalarType: schema_pb.ScalarType_STRING}}},
			{Name: "data", Type: &schema_pb.Type{Kind: &schema_pb.Type_ScalarType{ScalarType: schema_pb.ScalarType_STRING}}},
		},
	}

	client.schemas["default.system_logs"] = &schema_pb.RecordType{
		Fields: []*schema_pb.Field{
			{Name: "level", Type: &schema_pb.Type{Kind: &schema_pb.Type_ScalarType{ScalarType: schema_pb.ScalarType_STRING}}},
			{Name: "message", Type: &schema_pb.Type{Kind: &schema_pb.Type_ScalarType{ScalarType: schema_pb.ScalarType_STRING}}},
			{Name: "service", Type: &schema_pb.Type{Kind: &schema_pb.Type_ScalarType{ScalarType: schema_pb.ScalarType_STRING}}},
		},
	}

	client.schemas["test.test-topic"] = &schema_pb.RecordType{
		Fields: []*schema_pb.Field{
			{Name: "id", Type: &schema_pb.Type{Kind: &schema_pb.Type_ScalarType{ScalarType: schema_pb.ScalarType_INT32}}},
			{Name: "name", Type: &schema_pb.Type{Kind: &schema_pb.Type_ScalarType{ScalarType: schema_pb.ScalarType_STRING}}},
			{Name: "value", Type: &schema_pb.Type{Kind: &schema_pb.Type_ScalarType{ScalarType: schema_pb.ScalarType_DOUBLE}}},
		},
	}

	return client
}

// SetFailure configures the mock to fail with the given message
func (m *MockBrokerClient) SetFailure(shouldFail bool, message string) {
	m.shouldFail = shouldFail
	m.failMessage = message
}

// ListNamespaces returns the mock namespaces
func (m *MockBrokerClient) ListNamespaces(ctx context.Context) ([]string, error) {
	if m.shouldFail {
		return nil, fmt.Errorf("mock broker failure: %s", m.failMessage)
	}
	return m.namespaces, nil
}

// ListTopics returns the mock topics for a namespace
func (m *MockBrokerClient) ListTopics(ctx context.Context, namespace string) ([]string, error) {
	if m.shouldFail {
		return nil, fmt.Errorf("mock broker failure: %s", m.failMessage)
	}

	if topics, exists := m.topics[namespace]; exists {
		return topics, nil
	}
	return []string{}, nil
}

// GetTopicSchema returns the mock schema for a topic
func (m *MockBrokerClient) GetTopicSchema(ctx context.Context, namespace, topic string) (*schema_pb.RecordType, error) {
	if m.shouldFail {
		return nil, fmt.Errorf("mock broker failure: %s", m.failMessage)
	}

	key := fmt.Sprintf("%s.%s", namespace, topic)
	if schema, exists := m.schemas[key]; exists {
		return schema, nil
	}
	return nil, fmt.Errorf("topic %s not found", key)
}

// GetFilerClient returns a mock filer client
func (m *MockBrokerClient) GetFilerClient() (filer_pb.FilerClient, error) {
	if m.shouldFail {
		return nil, fmt.Errorf("mock broker failure: %s", m.failMessage)
	}
	return NewMockFilerClient(), nil
}

// MockFilerClient implements filer_pb.FilerClient interface for testing
type MockFilerClient struct {
	shouldFail  bool
	failMessage string
}

// NewMockFilerClient creates a new mock filer client
func NewMockFilerClient() *MockFilerClient {
	return &MockFilerClient{}
}

// SetFailure configures the mock to fail with the given message
func (m *MockFilerClient) SetFailure(shouldFail bool, message string) {
	m.shouldFail = shouldFail
	m.failMessage = message
}

// WithFilerClient executes a function with a mock filer client
func (m *MockFilerClient) WithFilerClient(followRedirect bool, fn func(client filer_pb.SeaweedFilerClient) error) error {
	if m.shouldFail {
		return fmt.Errorf("mock filer failure: %s", m.failMessage)
	}

	// For testing, we can just return success since the actual filer operations
	// are not critical for SQL engine unit tests
	return nil
}

// AdjustedUrl implements the FilerClient interface (mock implementation)
func (m *MockFilerClient) AdjustedUrl(location *filer_pb.Location) string {
	if location != nil && location.Url != "" {
		return location.Url
	}
	return "mock://localhost:8080"
}

// GetDataCenter implements the FilerClient interface (mock implementation)
func (m *MockFilerClient) GetDataCenter() string {
	return "mock-datacenter"
}

// ConfigureTopic creates or updates a topic configuration (mock implementation)
func (m *MockBrokerClient) ConfigureTopic(ctx context.Context, namespace, topicName string, partitionCount int32, recordType *schema_pb.RecordType) error {
	if m.shouldFail {
		return fmt.Errorf("mock broker failure: %s", m.failMessage)
	}

	// Store the schema in our mock data
	key := fmt.Sprintf("%s.%s", namespace, topicName)
	m.schemas[key] = recordType

	// Add to topics list if not already present
	if topics, exists := m.topics[namespace]; exists {
		for _, topic := range topics {
			if topic == topicName {
				return nil // Already exists
			}
		}
		m.topics[namespace] = append(topics, topicName)
	} else {
		m.topics[namespace] = []string{topicName}
	}

	return nil
}

// DeleteTopic removes a topic and all its data (mock implementation)
func (m *MockBrokerClient) DeleteTopic(ctx context.Context, namespace, topicName string) error {
	if m.shouldFail {
		return fmt.Errorf("mock broker failure: %s", m.failMessage)
	}

	// Remove from schemas
	key := fmt.Sprintf("%s.%s", namespace, topicName)
	delete(m.schemas, key)

	// Remove from topics list
	if topics, exists := m.topics[namespace]; exists {
		newTopics := make([]string, 0, len(topics))
		for _, topic := range topics {
			if topic != topicName {
				newTopics = append(newTopics, topic)
			}
		}
		m.topics[namespace] = newTopics
	}

	return nil
}

// GetUnflushedMessages returns mock unflushed data for testing
// Returns sample data as LogEntries to provide test data for SQL engine
func (m *MockBrokerClient) GetUnflushedMessages(ctx context.Context, namespace, topicName string, partition topic.Partition, startTimeNs int64) ([]*filer_pb.LogEntry, error) {
	if m.shouldFail {
		return nil, fmt.Errorf("mock broker failed to get unflushed messages: %s", m.failMessage)
	}

	// Generate sample data as LogEntries for testing
	// This provides data that looks like it came from the broker's memory buffer
	allSampleData := generateSampleHybridData(topicName, HybridScanOptions{})

	var logEntries []*filer_pb.LogEntry
	for _, result := range allSampleData {
		// Only return live_log entries as unflushed messages
		// This matches real system behavior where unflushed messages come from broker memory
		// parquet_archive data would come from parquet files, not unflushed messages
		if result.Source != "live_log" {
			continue
		}

		// Convert sample data to protobuf LogEntry format
		recordValue := &schema_pb.RecordValue{Fields: make(map[string]*schema_pb.Value)}
		for k, v := range result.Values {
			recordValue.Fields[k] = v
		}

		// Serialize the RecordValue
		data, err := proto.Marshal(recordValue)
		if err != nil {
			continue // Skip invalid entries
		}

		logEntry := &filer_pb.LogEntry{
			TsNs: result.Timestamp,
			Key:  result.Key,
			Data: data,
		}
		logEntries = append(logEntries, logEntry)
	}

	return logEntries, nil
}
