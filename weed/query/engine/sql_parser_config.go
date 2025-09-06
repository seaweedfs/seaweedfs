package engine

import (
	"context"
	"fmt"
	"strings"
	"time"
)

// SQLParserConfig controls SQL parsing behavior (PostgreSQL syntax only)
type SQLParserConfig struct {
	// Currently only PostgreSQL syntax is supported
	// This struct is kept for future extensibility
}

// PostgreSQLSQLParserConfig returns the default configuration (PostgreSQL only)
func PostgreSQLSQLParserConfig() *SQLParserConfig {
	return &SQLParserConfig{}
}

// ParseSQL parses SQL using PostgreSQL syntax
func (config *SQLParserConfig) ParseSQL(sql string) (Statement, error) {
	return ParseSQL(sql)
}

// SQLEngineWithParser extends SQLEngine with PostgreSQL parser
type SQLEngineWithParser struct {
	*SQLEngine
	ParserConfig *SQLParserConfig
}

// NewSQLEngineWithParser creates a new SQLEngine with PostgreSQL parser
func NewSQLEngineWithParser(masterAddr string, config *SQLParserConfig) *SQLEngineWithParser {
	if config == nil {
		config = PostgreSQLSQLParserConfig()
	}

	return &SQLEngineWithParser{
		SQLEngine:    NewSQLEngine(masterAddr),
		ParserConfig: config,
	}
}

// ExecuteSQL overrides the base ExecuteSQL to use PostgreSQL parser
func (e *SQLEngineWithParser) ExecuteSQL(ctx context.Context, sql string) (*QueryResult, error) {
	// Clean up the SQL
	sql = strings.TrimSpace(sql)
	if sql == "" {
		return &QueryResult{
			Error: fmt.Errorf("empty SQL statement"),
		}, fmt.Errorf("empty SQL statement")
	}

	sqlUpper := strings.ToUpper(sql)
	sqlTrimmed := strings.TrimSuffix(strings.TrimSpace(sql), ";")
	sqlTrimmed = strings.TrimSpace(sqlTrimmed)

	// Handle EXPLAIN as a special case
	if strings.HasPrefix(sqlUpper, "EXPLAIN") {
		actualSQL := strings.TrimSpace(sql[7:]) // Remove "EXPLAIN" prefix
		return e.executeExplain(ctx, actualSQL, time.Now())
	}

	// Handle DESCRIBE/DESC as a special case since it's not parsed as a standard statement
	if strings.HasPrefix(sqlUpper, "DESCRIBE") || strings.HasPrefix(sqlUpper, "DESC") {
		return e.handleDescribeCommand(ctx, sqlTrimmed)
	}

	// Parse the SQL statement using PostgreSQL parser
	stmt, err := e.ParserConfig.ParseSQL(sql)
	if err != nil {
		return &QueryResult{
			Error: fmt.Errorf("SQL parse error: %v", err),
		}, err
	}

	// Route to appropriate handler based on statement type
	// (same logic as the original SQLEngine)
	switch stmt := stmt.(type) {
	case *ShowStatement:
		return e.executeShowStatementWithDescribe(ctx, stmt)
	case *DDLStatement:
		return e.executeDDLStatement(ctx, stmt)
	case *SelectStatement:
		return e.executeSelectStatement(ctx, stmt)
	default:
		err := fmt.Errorf("unsupported SQL statement type: %T", stmt)
		return &QueryResult{Error: err}, err
	}
}
