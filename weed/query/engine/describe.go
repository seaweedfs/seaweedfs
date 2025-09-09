package engine

import (
	"context"
	"fmt"
	"strings"

	"github.com/seaweedfs/seaweedfs/weed/query/sqltypes"
)

// executeDescribeStatement handles DESCRIBE table commands
// Shows table schema in PostgreSQL-compatible format
func (e *SQLEngine) executeDescribeStatement(ctx context.Context, tableName string, database string) (*QueryResult, error) {
	if database == "" {
		database = e.catalog.GetCurrentDatabase()
		if database == "" {
			database = "default"
		}
	}

	// Auto-discover and register topic if not already in catalog (same logic as SELECT)
	if _, err := e.catalog.GetTableInfo(database, tableName); err != nil {
		// Topic not in catalog, try to discover and register it
		if regErr := e.discoverAndRegisterTopic(ctx, database, tableName); regErr != nil {
			fmt.Printf("Warning: Failed to discover topic %s.%s: %v\n", database, tableName, regErr)
			return &QueryResult{Error: fmt.Errorf("topic %s.%s not found and auto-discovery failed: %v", database, tableName, regErr)}, regErr
		}
	}

	// Get topic schema from broker
	recordType, err := e.catalog.brokerClient.GetTopicSchema(ctx, database, tableName)
	if err != nil {
		return &QueryResult{Error: err}, err
	}

	// System columns to include in DESCRIBE output
	systemColumns := []struct {
		Name  string
		Type  string
		Extra string
	}{
		{"_ts", "TIMESTAMP", "System column: Message timestamp"},
		{"_key", "VARBINARY", "System column: Message key"},
		{"_source", "VARCHAR(255)", "System column: Data source (parquet/log)"},
	}

	// Format schema as DESCRIBE output (regular fields + system columns)
	totalRows := len(recordType.Fields) + len(systemColumns)
	result := &QueryResult{
		Columns: []string{"Field", "Type", "Null", "Key", "Default", "Extra"},
		Rows:    make([][]sqltypes.Value, totalRows),
	}

	// Add regular fields
	for i, field := range recordType.Fields {
		sqlType := e.convertMQTypeToSQL(field.Type)

		result.Rows[i] = []sqltypes.Value{
			sqltypes.NewVarChar(field.Name), // Field
			sqltypes.NewVarChar(sqlType),    // Type
			sqltypes.NewVarChar("YES"),      // Null (assume nullable)
			sqltypes.NewVarChar(""),         // Key (no keys for now)
			sqltypes.NewVarChar("NULL"),     // Default
			sqltypes.NewVarChar(""),         // Extra
		}
	}

	// Add system columns
	for i, sysCol := range systemColumns {
		rowIndex := len(recordType.Fields) + i
		result.Rows[rowIndex] = []sqltypes.Value{
			sqltypes.NewVarChar(sysCol.Name),  // Field
			sqltypes.NewVarChar(sysCol.Type),  // Type
			sqltypes.NewVarChar("YES"),        // Null
			sqltypes.NewVarChar(""),           // Key
			sqltypes.NewVarChar("NULL"),       // Default
			sqltypes.NewVarChar(sysCol.Extra), // Extra - description
		}
	}

	return result, nil
}

// Enhanced executeShowStatementWithDescribe handles SHOW statements including DESCRIBE
func (e *SQLEngine) executeShowStatementWithDescribe(ctx context.Context, stmt *ShowStatement) (*QueryResult, error) {
	switch strings.ToUpper(stmt.Type) {
	case "DATABASES":
		return e.showDatabases(ctx)
	case "TABLES":
		// Parse FROM clause for database specification, or use current database context
		database := ""
		// Check if there's a database specified in SHOW TABLES FROM database
		if stmt.Schema != "" {
			// Use schema field if set by parser
			database = stmt.Schema
		} else {
			// Try to get from OnTable.Name with proper nil checks
			if stmt.OnTable.Name != nil {
				if nameStr := stmt.OnTable.Name.String(); nameStr != "" {
					database = nameStr
				} else {
					database = e.catalog.GetCurrentDatabase()
				}
			} else {
				database = e.catalog.GetCurrentDatabase()
			}
		}
		if database == "" {
			// Use current database context
			database = e.catalog.GetCurrentDatabase()
		}
		return e.showTables(ctx, database)
	case "COLUMNS":
		// SHOW COLUMNS FROM table is equivalent to DESCRIBE
		var tableName, database string

		// Safely extract table name and database with proper nil checks
		if stmt.OnTable.Name != nil {
			tableName = stmt.OnTable.Name.String()
			if stmt.OnTable.Qualifier != nil {
				database = stmt.OnTable.Qualifier.String()
			}
		}

		if tableName != "" {
			return e.executeDescribeStatement(ctx, tableName, database)
		}
		fallthrough
	default:
		err := fmt.Errorf("unsupported SHOW statement: %s", stmt.Type)
		return &QueryResult{Error: err}, err
	}
}
