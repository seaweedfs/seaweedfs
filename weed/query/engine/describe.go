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

	// Get topic schema from broker
	recordType, err := e.catalog.brokerClient.GetTopicSchema(ctx, database, tableName)
	if err != nil {
		return &QueryResult{Error: err}, err
	}

	// Format schema as DESCRIBE output
	result := &QueryResult{
		Columns: []string{"Field", "Type", "Null", "Key", "Default", "Extra"},
		Rows:    make([][]sqltypes.Value, len(recordType.Fields)),
	}

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

// Add support for DESCRIBE as a separate statement type
// This would be called from ExecuteSQL if we detect a DESCRIBE statement
func (e *SQLEngine) handleDescribeCommand(ctx context.Context, sql string) (*QueryResult, error) {
	// Simple parsing for "DESCRIBE [TABLE] table_name" format
	// Handle both "DESCRIBE table_name" and "DESCRIBE TABLE table_name"
	parts := strings.Fields(strings.TrimSpace(sql))
	if len(parts) < 2 {
		err := fmt.Errorf("DESCRIBE requires a table name")
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

	database := ""

	// Handle database.table format
	if strings.Contains(tableName, ".") {
		dbTableParts := strings.SplitN(tableName, ".", 2)
		database = dbTableParts[0]
		tableName = dbTableParts[1]
	}

	return e.executeDescribeStatement(ctx, tableName, database)
}
