package engine

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/mq/schema"
	"github.com/seaweedfs/seaweedfs/weed/mq/topic"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/schema_pb"
)

// BrokerClientInterface defines the interface for broker client operations
// Both real BrokerClient and MockBrokerClient implement this interface
type BrokerClientInterface interface {
	ListNamespaces(ctx context.Context) ([]string, error)
	ListTopics(ctx context.Context, namespace string) ([]string, error)
	GetTopicSchema(ctx context.Context, namespace, topic string) (*schema_pb.RecordType, error)
	GetFilerClient() (filer_pb.FilerClient, error)
	ConfigureTopic(ctx context.Context, namespace, topicName string, partitionCount int32, recordType *schema_pb.RecordType) error
	DeleteTopic(ctx context.Context, namespace, topicName string) error
	// GetUnflushedMessages returns only messages that haven't been flushed to disk yet
	// This prevents double-counting when combining with disk-based data
	GetUnflushedMessages(ctx context.Context, namespace, topicName string, partition topic.Partition, startTimeNs int64) ([]*filer_pb.LogEntry, error)
}

// SchemaCatalog manages the mapping between MQ topics and SQL tables
// Assumptions:
// 1. Each MQ namespace corresponds to a SQL database
// 2. Each MQ topic corresponds to a SQL table
// 3. Topic schemas are cached for performance
// 4. Schema evolution is tracked via RevisionId
type SchemaCatalog struct {
	mu sync.RWMutex

	// databases maps namespace names to database metadata
	// Assumption: Namespace names are valid SQL database identifiers
	databases map[string]*DatabaseInfo

	// currentDatabase tracks the active database context (for USE database)
	// Assumption: Single-threaded usage per SQL session
	currentDatabase string

	// brokerClient handles communication with MQ broker
	brokerClient BrokerClientInterface // Use interface for dependency injection

	// defaultPartitionCount is the default number of partitions for new topics
	// Can be overridden in CREATE TABLE statements with PARTITION COUNT option
	defaultPartitionCount int32
}

// DatabaseInfo represents a SQL database (MQ namespace)
type DatabaseInfo struct {
	Name   string
	Tables map[string]*TableInfo
}

// TableInfo represents a SQL table (MQ topic) with schema information
// Assumptions:
// 1. All topic messages conform to the same schema within a revision
// 2. Schema evolution maintains backward compatibility
// 3. Primary key is implicitly the message timestamp/offset
type TableInfo struct {
	Name       string
	Namespace  string
	Schema     *schema.Schema
	Columns    []ColumnInfo
	RevisionId uint32
}

// ColumnInfo represents a SQL column (MQ schema field)
type ColumnInfo struct {
	Name     string
	Type     string // SQL type representation
	Nullable bool   // Assumption: MQ fields are nullable by default
}

// NewSchemaCatalog creates a new schema catalog
// Uses master address for service discovery of filers and brokers
func NewSchemaCatalog(masterAddress string) *SchemaCatalog {
	return &SchemaCatalog{
		databases:             make(map[string]*DatabaseInfo),
		brokerClient:          NewBrokerClient(masterAddress),
		defaultPartitionCount: 6, // Default partition count, can be made configurable via environment variable
	}
}

// ListDatabases returns all available databases (MQ namespaces)
// Assumption: This would be populated from MQ broker metadata
func (c *SchemaCatalog) ListDatabases() []string {
	c.mu.RLock()
	defer c.mu.RUnlock()

	// Try to get real namespaces from broker first
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	namespaces, err := c.brokerClient.ListNamespaces(ctx)
	if err != nil {
		// Silently handle broker connection errors

		// Fallback to cached databases if broker unavailable
		databases := make([]string, 0, len(c.databases))
		for name := range c.databases {
			databases = append(databases, name)
		}

		// Return empty list if no cached data (no more sample data)
		return databases
	}

	return namespaces
}

// ListTables returns all tables in a database (MQ topics in namespace)
func (c *SchemaCatalog) ListTables(database string) ([]string, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	// Try to get real topics from broker first
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	topics, err := c.brokerClient.ListTopics(ctx, database)
	if err != nil {
		// Fallback to cached data if broker unavailable
		db, exists := c.databases[database]
		if !exists {
			// Return empty list if database not found (no more sample data)
			return []string{}, nil
		}

		tables := make([]string, 0, len(db.Tables))
		for name := range db.Tables {
			tables = append(tables, name)
		}
		return tables, nil
	}

	return topics, nil
}

// GetTableInfo returns detailed schema information for a table
// Assumption: Table exists and schema is accessible
func (c *SchemaCatalog) GetTableInfo(database, table string) (*TableInfo, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	db, exists := c.databases[database]
	if !exists {
		return nil, TableNotFoundError{
			Database: database,
			Table:    "",
		}
	}

	tableInfo, exists := db.Tables[table]
	if !exists {
		return nil, TableNotFoundError{
			Database: database,
			Table:    table,
		}
	}

	return tableInfo, nil
}

// RegisterTopic adds or updates a topic's schema information in the catalog
// Assumption: This is called when topics are created or schemas are modified
func (c *SchemaCatalog) RegisterTopic(namespace, topicName string, mqSchema *schema.Schema) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Ensure database exists
	db, exists := c.databases[namespace]
	if !exists {
		db = &DatabaseInfo{
			Name:   namespace,
			Tables: make(map[string]*TableInfo),
		}
		c.databases[namespace] = db
	}

	// Convert MQ schema to SQL table info
	tableInfo, err := c.convertMQSchemaToTableInfo(namespace, topicName, mqSchema)
	if err != nil {
		return fmt.Errorf("failed to convert MQ schema: %v", err)
	}

	db.Tables[topicName] = tableInfo
	return nil
}

// convertMQSchemaToTableInfo converts MQ schema to SQL table information
// Assumptions:
// 1. MQ scalar types map directly to SQL types
// 2. Complex types (arrays, maps) are serialized as JSON strings
// 3. All fields are nullable unless specifically marked otherwise
func (c *SchemaCatalog) convertMQSchemaToTableInfo(namespace, topicName string, mqSchema *schema.Schema) (*TableInfo, error) {
	columns := make([]ColumnInfo, len(mqSchema.RecordType.Fields))

	for i, field := range mqSchema.RecordType.Fields {
		sqlType, err := c.convertMQFieldTypeToSQL(field.Type)
		if err != nil {
			return nil, fmt.Errorf("unsupported field type for '%s': %v", field.Name, err)
		}

		columns[i] = ColumnInfo{
			Name:     field.Name,
			Type:     sqlType,
			Nullable: true, // Assumption: MQ fields are nullable by default
		}
	}

	return &TableInfo{
		Name:       topicName,
		Namespace:  namespace,
		Schema:     mqSchema,
		Columns:    columns,
		RevisionId: mqSchema.RevisionId,
	}, nil
}

// convertMQFieldTypeToSQL maps MQ field types to SQL types
// Uses standard SQL type mappings with PostgreSQL compatibility
func (c *SchemaCatalog) convertMQFieldTypeToSQL(fieldType *schema_pb.Type) (string, error) {
	switch t := fieldType.Kind.(type) {
	case *schema_pb.Type_ScalarType:
		switch t.ScalarType {
		case schema_pb.ScalarType_BOOL:
			return "BOOLEAN", nil
		case schema_pb.ScalarType_INT32:
			return "INT", nil
		case schema_pb.ScalarType_INT64:
			return "BIGINT", nil
		case schema_pb.ScalarType_FLOAT:
			return "FLOAT", nil
		case schema_pb.ScalarType_DOUBLE:
			return "DOUBLE", nil
		case schema_pb.ScalarType_BYTES:
			return "VARBINARY", nil
		case schema_pb.ScalarType_STRING:
			return "VARCHAR(255)", nil // Assumption: Default string length
		default:
			return "", fmt.Errorf("unsupported scalar type: %v", t.ScalarType)
		}
	case *schema_pb.Type_ListType:
		// Assumption: Lists are serialized as JSON strings in SQL
		return "TEXT", nil
	case *schema_pb.Type_RecordType:
		// Assumption: Nested records are serialized as JSON strings
		return "TEXT", nil
	default:
		return "", fmt.Errorf("unsupported field type: %T", t)
	}
}

// SetCurrentDatabase sets the active database context
// Assumption: Used for implementing "USE database" functionality
func (c *SchemaCatalog) SetCurrentDatabase(database string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	// TODO: Validate database exists in MQ broker
	c.currentDatabase = database
	return nil
}

// GetCurrentDatabase returns the currently active database
func (c *SchemaCatalog) GetCurrentDatabase() string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.currentDatabase
}

// SetDefaultPartitionCount sets the default number of partitions for new topics
func (c *SchemaCatalog) SetDefaultPartitionCount(count int32) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.defaultPartitionCount = count
}

// GetDefaultPartitionCount returns the default number of partitions for new topics
func (c *SchemaCatalog) GetDefaultPartitionCount() int32 {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.defaultPartitionCount
}
