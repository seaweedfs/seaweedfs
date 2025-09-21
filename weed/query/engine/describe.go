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

	// Get both key and value schemas from broker
	keySchema, valueSchema, err := e.catalog.brokerClient.GetTopicSchemas(ctx, database, tableName)
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

	// Calculate total rows: value fields + key fields (if exists) + system columns
	totalRows := len(systemColumns)
	if valueSchema != nil {
		totalRows += len(valueSchema.Fields)
	}
	keyFieldCount := 0
	if keySchema != nil {
		keyFieldCount = len(keySchema.Fields)
		totalRows += keyFieldCount
	}

	result := &QueryResult{
		Columns: []string{"Field", "Type", "Null", "Key", "Default", "Extra"},
		Rows:    make([][]sqltypes.Value, totalRows),
	}

	rowIndex := 0

	// Add key schema fields first (if present)
	if keySchema != nil {
		for _, field := range keySchema.Fields {
			sqlType := e.convertMQTypeToSQL(field.Type)
			result.Rows[rowIndex] = []sqltypes.Value{
				sqltypes.NewVarChar(field.Name + "_key"), // Field - add suffix to distinguish from value fields
				sqltypes.NewVarChar(sqlType),             // Type
				sqltypes.NewVarChar("YES"),               // Null
				sqltypes.NewVarChar("KEY"),               // Key - mark as key schema field
				sqltypes.NewVarChar("NULL"),              // Default
				sqltypes.NewVarChar("Key schema field"),  // Extra
			}
			rowIndex++
		}
	}

	// Add value schema fields (if present)
	if valueSchema != nil {
		for _, field := range valueSchema.Fields {
			sqlType := e.convertMQTypeToSQL(field.Type)
			result.Rows[rowIndex] = []sqltypes.Value{
				sqltypes.NewVarChar(field.Name),           // Field
				sqltypes.NewVarChar(sqlType),              // Type
				sqltypes.NewVarChar("YES"),                // Null
				sqltypes.NewVarChar("VALUE"),              // Key - mark as value schema field
				sqltypes.NewVarChar("NULL"),               // Default
				sqltypes.NewVarChar("Value schema field"), // Extra
			}
			rowIndex++
		}
	}

	// Add system columns
	for _, sysCol := range systemColumns {
		result.Rows[rowIndex] = []sqltypes.Value{
			sqltypes.NewVarChar(sysCol.Name),  // Field
			sqltypes.NewVarChar(sysCol.Type),  // Type
			sqltypes.NewVarChar("YES"),        // Null
			sqltypes.NewVarChar("SYS"),        // Key - mark as system column
			sqltypes.NewVarChar("NULL"),       // Default
			sqltypes.NewVarChar(sysCol.Extra), // Extra - description
		}
		rowIndex++
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
