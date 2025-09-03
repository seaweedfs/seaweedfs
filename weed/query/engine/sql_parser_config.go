package engine

import (
	"context"
	"fmt"
	"strings"
	"time"
)

// SQLParserConfig controls which SQL parser to use
type SQLParserConfig struct {
	UsePostgreSQLParser   bool // If true, use pg_query_go; if false, use mysql-dialect parser
	EnableDialectWarnings bool // If true, log warnings about dialect mismatches
}

// DefaultSQLParserConfig returns the default configuration (MySQL parser for now)
func DefaultSQLParserConfig() *SQLParserConfig {
	return &SQLParserConfig{
		UsePostgreSQLParser:   false, // Keep MySQL parser as default for stability
		EnableDialectWarnings: true,  // Enable warnings about dialect issues
	}
}

// PostgreSQLSQLParserConfig returns configuration for PostgreSQL parser
func PostgreSQLSQLParserConfig() *SQLParserConfig {
	return &SQLParserConfig{
		UsePostgreSQLParser:   true,
		EnableDialectWarnings: false, // No warnings needed when using correct parser
	}
}

// ParseSQL is a unified interface that can use either parser based on configuration
func (config *SQLParserConfig) ParseSQL(sql string) (Statement, error) {
	if config.UsePostgreSQLParser {
		return config.parseWithPostgreSQL(sql)
	}
	return config.parseWithMySQL(sql)
}

// parseWithMySQL uses the PostgreSQL parser (fallback for backward compatibility)
func (config *SQLParserConfig) parseWithMySQL(sql string) (Statement, error) {
	if config.EnableDialectWarnings {
		config.checkForPostgreSQLDialectFeatures(sql)
	}

	// Since we've removed the MySQL parser, use the PostgreSQL parser instead
	// This maintains backward compatibility while using the better parser
	return config.parseWithPostgreSQL(sql)
}

// parseWithPostgreSQL uses the new PostgreSQL parser
func (config *SQLParserConfig) parseWithPostgreSQL(sql string) (Statement, error) {
	// Use the PostgreSQL parser from engine.go
	return ParseSQL(sql)
}

// checkForPostgreSQLDialectFeatures logs warnings for PostgreSQL-specific syntax
func (config *SQLParserConfig) checkForPostgreSQLDialectFeatures(sql string) {
	sqlUpper := strings.ToUpper(sql)

	// Check for PostgreSQL-specific features
	if strings.Contains(sql, "\"") && !strings.Contains(sql, "'") {
		fmt.Printf("WARNING: Detected double-quoted identifiers (\") - PostgreSQL uses these, MySQL uses backticks (`)\n")
	}

	if strings.Contains(sqlUpper, "||") && !strings.Contains(sqlUpper, "CONCAT") {
		fmt.Printf("WARNING: Detected || string concatenation - PostgreSQL syntax, MySQL uses CONCAT()\n")
	}

	if strings.Contains(sqlUpper, "PG_") || strings.Contains(sqlUpper, "INFORMATION_SCHEMA") {
		fmt.Printf("WARNING: Detected PostgreSQL system functions/catalogs - may not work with MySQL parser\n")
	}

	if strings.Contains(sqlUpper, "LIMIT") && strings.Contains(sqlUpper, "OFFSET") {
		fmt.Printf("WARNING: LIMIT/OFFSET syntax may differ between PostgreSQL and MySQL\n")
	}
}

// SQLEngineWithParser extends SQLEngine with configurable parser
type SQLEngineWithParser struct {
	*SQLEngine
	ParserConfig *SQLParserConfig
}

// NewSQLEngineWithParser creates a new SQLEngine with parser configuration
func NewSQLEngineWithParser(masterAddr string, config *SQLParserConfig) *SQLEngineWithParser {
	if config == nil {
		config = DefaultSQLParserConfig()
	}

	return &SQLEngineWithParser{
		SQLEngine:    NewSQLEngine(masterAddr),
		ParserConfig: config,
	}
}

// ExecuteSQL overrides the base ExecuteSQL to use the configured parser
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

	// Parse the SQL statement using the configured parser
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
