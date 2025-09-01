package engine

import (
	"fmt"
	"sync"
	
	"github.com/seaweedfs/seaweedfs/weed/mq/schema"
	"github.com/seaweedfs/seaweedfs/weed/pb/schema_pb"
)

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
	Name        string
	Namespace   string  
	Schema      *schema.Schema
	Columns     []ColumnInfo
	RevisionId  uint32
}

// ColumnInfo represents a SQL column (MQ schema field)
type ColumnInfo struct {
	Name     string
	Type     string  // SQL type representation
	Nullable bool    // Assumption: MQ fields are nullable by default
}

// NewSchemaCatalog creates a new schema catalog
// Assumption: Catalog starts empty and is populated on-demand
func NewSchemaCatalog() *SchemaCatalog {
	return &SchemaCatalog{
		databases: make(map[string]*DatabaseInfo),
	}
}

// ListDatabases returns all available databases (MQ namespaces)
// Assumption: This would be populated from MQ broker metadata
func (c *SchemaCatalog) ListDatabases() []string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	
	databases := make([]string, 0, len(c.databases))
	for name := range c.databases {
		databases = append(databases, name)
	}
	
	// TODO: Query actual MQ broker for namespace list
	// For now, return sample data for testing
	if len(databases) == 0 {
		return []string{"default", "analytics", "logs"}
	}
	
	return databases
}

// ListTables returns all tables in a database (MQ topics in namespace)
func (c *SchemaCatalog) ListTables(database string) ([]string, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	
	db, exists := c.databases[database]
	if !exists {
		// TODO: Query MQ broker for actual topics in namespace
		// For now, return sample data
		switch database {
		case "default":
			return []string{"user_events", "system_logs"}, nil
		case "analytics": 
			return []string{"page_views", "click_events"}, nil
		case "logs":
			return []string{"error_logs", "access_logs"}, nil
		default:
			return nil, fmt.Errorf("database '%s' not found", database)
		}
	}
	
	tables := make([]string, 0, len(db.Tables))
	for name := range db.Tables {
		tables = append(tables, name)
	}
	
	return tables, nil
}

// GetTableInfo returns detailed schema information for a table
// Assumption: Table exists and schema is accessible
func (c *SchemaCatalog) GetTableInfo(database, table string) (*TableInfo, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	
	db, exists := c.databases[database]
	if !exists {
		return nil, fmt.Errorf("database '%s' not found", database)
	}
	
	tableInfo, exists := db.Tables[table]
	if !exists {
		return nil, fmt.Errorf("table '%s' not found in database '%s'", table, database)
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
// Assumption: Standard SQL type mappings with MySQL compatibility
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
