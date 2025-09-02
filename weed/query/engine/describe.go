package engine

import (
	"context"
	"fmt"
	"strings"

	"github.com/seaweedfs/seaweedfs/weed/query/sqltypes"
	"github.com/xwb1989/sqlparser"
)

// executeDescribeStatement handles DESCRIBE table commands
// Assumption: DESCRIBE shows table schema in MySQL-compatible format
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
		{"_timestamp_ns", "BIGINT", "System column: Message timestamp in nanoseconds"},
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
func (e *SQLEngine) executeShowStatementWithDescribe(ctx context.Context, stmt *sqlparser.Show) (*QueryResult, error) {
	switch strings.ToUpper(stmt.Type) {
	case "DATABASES":
		return e.showDatabases(ctx)
	case "TABLES":
		// Parse FROM clause for database specification, or use current database context
		database := ""
		if stmt.OnTable.Name.String() != "" {
			// SHOW TABLES FROM database_name
			database = stmt.OnTable.Name.String()
		} else {
			// Use current database context
			database = e.catalog.GetCurrentDatabase()
		}
		return e.showTables(ctx, database)
	case "COLUMNS":
		// SHOW COLUMNS FROM table is equivalent to DESCRIBE
		if stmt.OnTable.Name.String() != "" {
			tableName := stmt.OnTable.Name.String()
			database := ""
			if stmt.OnTable.Qualifier.String() != "" {
				database = stmt.OnTable.Qualifier.String()
			}
			return e.executeDescribeStatement(ctx, tableName, database)
		}
		fallthrough
	default:
		err := fmt.Errorf("unsupported SHOW statement: %s", stmt.Type)
		return &QueryResult{Error: err}, err
	}
}

// Add support for DESCRIBE/DESC as a separate statement type
// This would be called from ExecuteSQL if we detect a DESCRIBE/DESC statement
func (e *SQLEngine) handleDescribeCommand(ctx context.Context, sql string) (*QueryResult, error) {
	// Simple parsing for "DESCRIBE/DESC [TABLE] table_name" format
	// Handle both "DESCRIBE table_name", "DESC table_name", "DESCRIBE TABLE table_name", "DESC TABLE table_name"
	parts := strings.Fields(strings.TrimSpace(sql))
	if len(parts) < 2 {
		err := fmt.Errorf("DESCRIBE/DESC requires a table name")
		return &QueryResult{Error: err}, err
	}

	var tableName string
	// Check if "TABLE" keyword is used
	if len(parts) >= 3 && strings.ToUpper(parts[1]) == "TABLE" {
		// "DESCRIBE TABLE table_name" format
		tableName = parts[2]
	} else {
		// "DESCRIBE table_name" format
		tableName = parts[1]
	}

	// Remove backticks and semicolons from table name
	tableName = strings.Trim(tableName, "`")
	tableName = strings.TrimSuffix(tableName, ";")

	database := ""

	// Handle database.table format
	if strings.Contains(tableName, ".") {
		dbTableParts := strings.SplitN(tableName, ".", 2)
		database = strings.Trim(dbTableParts[0], "`") // Also strip backticks from database name
		database = strings.TrimSuffix(database, ";")
		tableName = strings.Trim(dbTableParts[1], "`")
		tableName = strings.TrimSuffix(tableName, ";")
	}

	return e.executeDescribeStatement(ctx, tableName, database)
}
