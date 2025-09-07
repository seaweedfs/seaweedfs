package engine

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"math"
	"math/big"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/mq/schema"
	"github.com/seaweedfs/seaweedfs/weed/mq/topic"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/schema_pb"
	"github.com/seaweedfs/seaweedfs/weed/query/sqltypes"
	"github.com/seaweedfs/seaweedfs/weed/util"
	util_http "github.com/seaweedfs/seaweedfs/weed/util/http"
	"google.golang.org/protobuf/proto"
)

// SQL Function Name Constants
const (
	// Aggregation Functions
	FuncCOUNT = "COUNT"
	FuncSUM   = "SUM"
	FuncAVG   = "AVG"
	FuncMIN   = "MIN"
	FuncMAX   = "MAX"

	// String Functions
	FuncUPPER     = "UPPER"
	FuncLOWER     = "LOWER"
	FuncLENGTH    = "LENGTH"
	FuncTRIM      = "TRIM"
	FuncBTRIM     = "BTRIM" // CockroachDB's internal name for TRIM
	FuncLTRIM     = "LTRIM"
	FuncRTRIM     = "RTRIM"
	FuncSUBSTRING = "SUBSTRING"
	FuncLEFT      = "LEFT"
	FuncRIGHT     = "RIGHT"
	FuncCONCAT    = "CONCAT"

	// DateTime Functions
	FuncCURRENT_DATE      = "CURRENT_DATE"
	FuncCURRENT_TIME      = "CURRENT_TIME"
	FuncCURRENT_TIMESTAMP = "CURRENT_TIMESTAMP"
	FuncNOW               = "NOW"
	FuncEXTRACT           = "EXTRACT"
	FuncDATE_TRUNC        = "DATE_TRUNC"

	// PostgreSQL uses EXTRACT(part FROM date) instead of convenience functions like YEAR(), MONTH(), etc.
)

// PostgreSQL-compatible SQL AST types
type Statement interface {
	isStatement()
}

type ShowStatement struct {
	Type    string  // "databases", "tables", "columns"
	Table   string  // for SHOW COLUMNS FROM table
	Schema  string  // for database context
	OnTable NameRef // for compatibility with existing code that checks OnTable
}

func (s *ShowStatement) isStatement() {}

type UseStatement struct {
	Database string // database name to switch to
}

func (u *UseStatement) isStatement() {}

type DDLStatement struct {
	Action    string // "create", "alter", "drop"
	NewName   NameRef
	TableSpec *TableSpec
}

type NameRef struct {
	Name      StringGetter
	Qualifier StringGetter
}

type StringGetter interface {
	String() string
}

type stringValue string

func (s stringValue) String() string { return string(s) }

type TableSpec struct {
	Columns []ColumnDef
}

type ColumnDef struct {
	Name StringGetter
	Type TypeRef
}

type TypeRef struct {
	Type string
}

func (d *DDLStatement) isStatement() {}

type SelectStatement struct {
	SelectExprs     []SelectExpr
	From            []TableExpr
	Where           *WhereClause
	Limit           *LimitClause
	WindowFunctions []*WindowFunction
}

type WhereClause struct {
	Expr ExprNode
}

type LimitClause struct {
	Rowcount ExprNode
	Offset   ExprNode
}

func (s *SelectStatement) isStatement() {}

// Window function types for time-series analytics
type WindowSpec struct {
	PartitionBy []ExprNode
	OrderBy     []*OrderByClause
}

type WindowFunction struct {
	Function string     // ROW_NUMBER, RANK, LAG, LEAD
	Args     []ExprNode // Function arguments
	Over     *WindowSpec
	Alias    string // Column alias for the result
}

type OrderByClause struct {
	Column string
	Order  string // ASC or DESC
}

type SelectExpr interface {
	isSelectExpr()
}

type StarExpr struct{}

func (s *StarExpr) isSelectExpr() {}

type AliasedExpr struct {
	Expr ExprNode
	As   AliasRef
}

type AliasRef interface {
	IsEmpty() bool
	String() string
}

type aliasValue string

func (a aliasValue) IsEmpty() bool   { return string(a) == "" }
func (a aliasValue) String() string  { return string(a) }
func (a *AliasedExpr) isSelectExpr() {}

type TableExpr interface {
	isTableExpr()
}

type AliasedTableExpr struct {
	Expr interface{}
}

func (a *AliasedTableExpr) isTableExpr() {}

type TableName struct {
	Name      StringGetter
	Qualifier StringGetter
}

type ExprNode interface {
	isExprNode()
}

type FuncExpr struct {
	Name  StringGetter
	Exprs []SelectExpr
}

func (f *FuncExpr) isExprNode() {}

type ColName struct {
	Name StringGetter
}

func (c *ColName) isExprNode() {}

// ArithmeticExpr represents arithmetic operations like id+user_id and string concatenation like name||suffix
type ArithmeticExpr struct {
	Left     ExprNode
	Right    ExprNode
	Operator string // +, -, *, /, %, ||
}

func (a *ArithmeticExpr) isExprNode() {}

type ComparisonExpr struct {
	Left     ExprNode
	Right    ExprNode
	Operator string
}

func (c *ComparisonExpr) isExprNode() {}

type AndExpr struct {
	Left  ExprNode
	Right ExprNode
}

func (a *AndExpr) isExprNode() {}

type OrExpr struct {
	Left  ExprNode
	Right ExprNode
}

func (o *OrExpr) isExprNode() {}

type ParenExpr struct {
	Expr ExprNode
}

func (p *ParenExpr) isExprNode() {}

type SQLVal struct {
	Type int
	Val  []byte
}

func (s *SQLVal) isExprNode() {}

type ValTuple []ExprNode

func (v ValTuple) isExprNode() {}

// SQLVal types
const (
	IntVal = iota
	StrVal
	FloatVal
)

// Operator constants
const (
	CreateStr       = "create"
	AlterStr        = "alter"
	DropStr         = "drop"
	EqualStr        = "="
	LessThanStr     = "<"
	GreaterThanStr  = ">"
	LessEqualStr    = "<="
	GreaterEqualStr = ">="
	NotEqualStr     = "!="
)

// parseIdentifier properly parses a potentially quoted identifier (database/table name)
func parseIdentifier(identifier string) string {
	identifier = strings.TrimSpace(identifier)
	identifier = strings.TrimSuffix(identifier, ";") // Remove trailing semicolon

	// Handle double quotes (PostgreSQL standard)
	if len(identifier) >= 2 && identifier[0] == '"' && identifier[len(identifier)-1] == '"' {
		return identifier[1 : len(identifier)-1]
	}

	// Handle backticks (MySQL compatibility)
	if len(identifier) >= 2 && identifier[0] == '`' && identifier[len(identifier)-1] == '`' {
		return identifier[1 : len(identifier)-1]
	}

	return identifier
}

// ParseSQL parses PostgreSQL-compatible SQL statements using CockroachDB parser for SELECT queries
func ParseSQL(sql string) (Statement, error) {
	sql = strings.TrimSpace(sql)
	sqlUpper := strings.ToUpper(sql)

	// Handle USE statement
	if strings.HasPrefix(sqlUpper, "USE ") {
		parts := strings.Fields(sql)
		if len(parts) < 2 {
			return nil, fmt.Errorf("USE statement requires a database name")
		}
		// Parse the database name properly, handling quoted identifiers
		dbName := parseIdentifier(strings.Join(parts[1:], " "))
		return &UseStatement{Database: dbName}, nil
	}

	// Handle DESCRIBE/DESC statements as aliases for SHOW COLUMNS FROM
	if strings.HasPrefix(sqlUpper, "DESCRIBE ") || strings.HasPrefix(sqlUpper, "DESC ") {
		parts := strings.Fields(sql)
		if len(parts) < 2 {
			return nil, fmt.Errorf("DESCRIBE/DESC statement requires a table name")
		}

		var tableName string
		var database string

		// Get the raw table name (before parsing identifiers)
		var rawTableName string
		if len(parts) >= 3 && strings.ToUpper(parts[1]) == "TABLE" {
			rawTableName = parts[2]
		} else {
			rawTableName = parts[1]
		}

		// Parse database.table format first, then apply parseIdentifier to each part
		if strings.Contains(rawTableName, ".") {
			// Handle quoted database.table like "db"."table"
			if strings.HasPrefix(rawTableName, "\"") || strings.HasPrefix(rawTableName, "`") {
				// Find the closing quote and the dot
				var quoteChar byte = '"'
				if rawTableName[0] == '`' {
					quoteChar = '`'
				}

				// Find the matching closing quote
				closingIndex := -1
				for i := 1; i < len(rawTableName); i++ {
					if rawTableName[i] == quoteChar {
						closingIndex = i
						break
					}
				}

				if closingIndex != -1 && closingIndex+1 < len(rawTableName) && rawTableName[closingIndex+1] == '.' {
					// Valid quoted database name
					database = parseIdentifier(rawTableName[:closingIndex+1])
					tableName = parseIdentifier(rawTableName[closingIndex+2:])
				} else {
					// Fall back to simple split then parse
					dbTableParts := strings.SplitN(rawTableName, ".", 2)
					database = parseIdentifier(dbTableParts[0])
					tableName = parseIdentifier(dbTableParts[1])
				}
			} else {
				// Simple case: no quotes, just split then parse
				dbTableParts := strings.SplitN(rawTableName, ".", 2)
				database = parseIdentifier(dbTableParts[0])
				tableName = parseIdentifier(dbTableParts[1])
			}
		} else {
			// No database.table format, just parse the table name
			tableName = parseIdentifier(rawTableName)
		}

		stmt := &ShowStatement{Type: "columns"}
		stmt.OnTable.Name = stringValue(tableName)
		if database != "" {
			stmt.OnTable.Qualifier = stringValue(database)
		}
		return stmt, nil
	}

	// Handle SHOW statements (keep custom parsing for these simple cases)
	if strings.HasPrefix(sqlUpper, "SHOW DATABASES") || strings.HasPrefix(sqlUpper, "SHOW SCHEMAS") {
		return &ShowStatement{Type: "databases"}, nil
	}
	if strings.HasPrefix(sqlUpper, "SHOW TABLES") {
		stmt := &ShowStatement{Type: "tables"}
		// Handle "SHOW TABLES FROM database" syntax
		if strings.Contains(sqlUpper, "FROM") {
			partsUpper := strings.Fields(sqlUpper)
			partsOriginal := strings.Fields(sql) // Use original casing
			for i, part := range partsUpper {
				if part == "FROM" && i+1 < len(partsOriginal) {
					// Parse the database name properly
					dbName := parseIdentifier(partsOriginal[i+1])
					stmt.Schema = dbName                    // Set the Schema field for the test
					stmt.OnTable.Name = stringValue(dbName) // Keep for compatibility
					break
				}
			}
		}
		return stmt, nil
	}
	if strings.HasPrefix(sqlUpper, "SHOW COLUMNS FROM") {
		// Parse "SHOW COLUMNS FROM table" or "SHOW COLUMNS FROM database.table"
		parts := strings.Fields(sql)
		if len(parts) < 4 {
			return nil, fmt.Errorf("SHOW COLUMNS FROM statement requires a table name")
		}

		// Get the raw table name (before parsing identifiers)
		rawTableName := parts[3]
		var tableName string
		var database string

		// Parse database.table format first, then apply parseIdentifier to each part
		if strings.Contains(rawTableName, ".") {
			// Handle quoted database.table like "db"."table"
			if strings.HasPrefix(rawTableName, "\"") || strings.HasPrefix(rawTableName, "`") {
				// Find the closing quote and the dot
				var quoteChar byte = '"'
				if rawTableName[0] == '`' {
					quoteChar = '`'
				}

				// Find the matching closing quote
				closingIndex := -1
				for i := 1; i < len(rawTableName); i++ {
					if rawTableName[i] == quoteChar {
						closingIndex = i
						break
					}
				}

				if closingIndex != -1 && closingIndex+1 < len(rawTableName) && rawTableName[closingIndex+1] == '.' {
					// Valid quoted database name
					database = parseIdentifier(rawTableName[:closingIndex+1])
					tableName = parseIdentifier(rawTableName[closingIndex+2:])
				} else {
					// Fall back to simple split then parse
					dbTableParts := strings.SplitN(rawTableName, ".", 2)
					database = parseIdentifier(dbTableParts[0])
					tableName = parseIdentifier(dbTableParts[1])
				}
			} else {
				// Simple case: no quotes, just split then parse
				dbTableParts := strings.SplitN(rawTableName, ".", 2)
				database = parseIdentifier(dbTableParts[0])
				tableName = parseIdentifier(dbTableParts[1])
			}
		} else {
			// No database.table format, just parse the table name
			tableName = parseIdentifier(rawTableName)
		}

		stmt := &ShowStatement{Type: "columns"}
		stmt.OnTable.Name = stringValue(tableName)
		if database != "" {
			stmt.OnTable.Qualifier = stringValue(database)
		}
		return stmt, nil
	}

	// Use CockroachDB parser for SELECT statements
	if strings.HasPrefix(sqlUpper, "SELECT") {
		parser := NewCockroachSQLParser()
		return parser.ParseSQL(sql)
	}

	return nil, UnsupportedFeatureError{
		Feature: fmt.Sprintf("statement type: %s", strings.Fields(sqlUpper)[0]),
		Reason:  "statement parsing not implemented",
	}
}

// extractFunctionArguments extracts the arguments from a function call expression using CockroachDB parser
func extractFunctionArguments(expr string) ([]SelectExpr, error) {
	// Find the parentheses
	startParen := strings.Index(expr, "(")
	endParen := strings.LastIndex(expr, ")")

	if startParen == -1 || endParen == -1 || endParen <= startParen {
		return nil, fmt.Errorf("invalid function syntax")
	}

	// Extract arguments string
	argsStr := strings.TrimSpace(expr[startParen+1 : endParen])

	// Handle empty arguments
	if argsStr == "" {
		return []SelectExpr{}, nil
	}

	// Handle single * argument (for COUNT(*))
	if argsStr == "*" {
		return []SelectExpr{&StarExpr{}}, nil
	}

	// Parse multiple arguments separated by commas
	args := []SelectExpr{}
	argParts := strings.Split(argsStr, ",")

	// Use CockroachDB parser to parse each argument as a SELECT expression
	cockroachParser := NewCockroachSQLParser()

	for _, argPart := range argParts {
		argPart = strings.TrimSpace(argPart)
		if argPart == "*" {
			args = append(args, &StarExpr{})
		} else {
			// Create a dummy SELECT statement to parse the argument expression
			dummySelect := fmt.Sprintf("SELECT %s", argPart)

			// Parse using CockroachDB parser
			stmt, err := cockroachParser.ParseSQL(dummySelect)
			if err != nil {
				// If CockroachDB parser fails, fall back to simple column name
				args = append(args, &AliasedExpr{
					Expr: &ColName{Name: stringValue(argPart)},
				})
				continue
			}

			// Extract the expression from the parsed SELECT statement
			if selectStmt, ok := stmt.(*SelectStatement); ok && len(selectStmt.SelectExprs) > 0 {
				args = append(args, selectStmt.SelectExprs[0])
			} else {
				// Fallback to column name if parsing fails
				args = append(args, &AliasedExpr{
					Expr: &ColName{Name: stringValue(argPart)},
				})
			}
		}
	}

	return args, nil
}

// debugModeKey is used to store debug mode flag in context
type debugModeKey struct{}

// isDebugMode checks if we're in debug/explain mode
func isDebugMode(ctx context.Context) bool {
	debug, ok := ctx.Value(debugModeKey{}).(bool)
	return ok && debug
}

// withDebugMode returns a context with debug mode enabled
func withDebugMode(ctx context.Context) context.Context {
	return context.WithValue(ctx, debugModeKey{}, true)
}

// LogBufferStart tracks the starting buffer index for a file
// Buffer indexes are monotonically increasing, count = len(chunks)
type LogBufferStart struct {
	StartIndex int64 `json:"start_index"` // Starting buffer index (count = len(chunks))
}

// SQLEngine provides SQL query execution capabilities for SeaweedFS
// Assumptions:
// 1. MQ namespaces map directly to SQL databases
// 2. MQ topics map directly to SQL tables
// 3. Schema evolution is handled transparently with backward compatibility
// 4. Queries run against Parquet-stored MQ messages
type SQLEngine struct {
	catalog *SchemaCatalog
}

// NewSQLEngine creates a new SQL execution engine
// Uses master address for service discovery and initialization
func NewSQLEngine(masterAddress string) *SQLEngine {
	// Initialize global HTTP client if not already done
	// This is needed for reading partition data from the filer
	if util_http.GetGlobalHttpClient() == nil {
		util_http.InitGlobalHttpClient()
	}

	return &SQLEngine{
		catalog: NewSchemaCatalog(masterAddress),
	}
}

// NewSQLEngineWithCatalog creates a new SQL execution engine with a custom catalog
// Used for testing or when you want to provide a pre-configured catalog
func NewSQLEngineWithCatalog(catalog *SchemaCatalog) *SQLEngine {
	// Initialize global HTTP client if not already done
	// This is needed for reading partition data from the filer
	if util_http.GetGlobalHttpClient() == nil {
		util_http.InitGlobalHttpClient()
	}

	return &SQLEngine{
		catalog: catalog,
	}
}

// GetCatalog returns the schema catalog for external access
func (e *SQLEngine) GetCatalog() *SchemaCatalog {
	return e.catalog
}

// ExecuteSQL parses and executes a SQL statement
// Assumptions:
// 1. All SQL statements are PostgreSQL-compatible via pg_query_go
// 2. DDL operations (CREATE/ALTER/DROP) modify underlying MQ topics
// 3. DML operations (SELECT) query Parquet files directly
// 4. Error handling follows PostgreSQL conventions
func (e *SQLEngine) ExecuteSQL(ctx context.Context, sql string) (*QueryResult, error) {
	startTime := time.Now()

	// Handle EXPLAIN as a special case
	sqlTrimmed := strings.TrimSpace(sql)
	sqlUpper := strings.ToUpper(sqlTrimmed)
	if strings.HasPrefix(sqlUpper, "EXPLAIN") {
		// Extract the actual query after EXPLAIN
		actualSQL := strings.TrimSpace(sqlTrimmed[7:]) // Remove "EXPLAIN"
		return e.executeExplain(ctx, actualSQL, startTime)
	}

	// Parse the SQL statement using PostgreSQL parser
	stmt, err := ParseSQL(sql)
	if err != nil {
		return &QueryResult{
			Error: fmt.Errorf("SQL parse error: %v", err),
		}, err
	}

	// Route to appropriate handler based on statement type
	switch stmt := stmt.(type) {
	case *ShowStatement:
		return e.executeShowStatementWithDescribe(ctx, stmt)
	case *UseStatement:
		return e.executeUseStatement(ctx, stmt)
	case *DDLStatement:
		return e.executeDDLStatement(ctx, stmt)
	case *SelectStatement:
		return e.executeSelectStatement(ctx, stmt)
	default:
		err := fmt.Errorf("unsupported SQL statement type: %T", stmt)
		return &QueryResult{Error: err}, err
	}
}

// executeExplain handles EXPLAIN statements by executing the query with plan tracking
func (e *SQLEngine) executeExplain(ctx context.Context, actualSQL string, startTime time.Time) (*QueryResult, error) {
	// Enable debug mode for EXPLAIN queries
	ctx = withDebugMode(ctx)

	// Parse the actual SQL statement using PostgreSQL parser
	stmt, err := ParseSQL(actualSQL)
	if err != nil {
		return &QueryResult{
			Error: fmt.Errorf("SQL parse error in EXPLAIN query: %v", err),
		}, err
	}

	// Create execution plan
	plan := &QueryExecutionPlan{
		QueryType:         strings.ToUpper(strings.Fields(actualSQL)[0]),
		DataSources:       []string{},
		OptimizationsUsed: []string{},
		Details:           make(map[string]interface{}),
	}

	var result *QueryResult

	// Route to appropriate handler based on statement type (with plan tracking)
	switch stmt := stmt.(type) {
	case *SelectStatement:
		result, err = e.executeSelectStatementWithPlan(ctx, stmt, plan)
		if err != nil {
			plan.Details["error"] = err.Error()
		}
	case *ShowStatement:
		plan.QueryType = "SHOW"
		plan.ExecutionStrategy = "metadata_only"
		result, err = e.executeShowStatementWithDescribe(ctx, stmt)
	default:
		err := fmt.Errorf("EXPLAIN not supported for statement type: %T", stmt)
		return &QueryResult{Error: err}, err
	}

	// Calculate execution time
	plan.ExecutionTimeMs = float64(time.Since(startTime).Nanoseconds()) / 1e6

	// Format execution plan as result
	return e.formatExecutionPlan(plan, result, err)
}

// formatExecutionPlan converts execution plan to a hierarchical tree format for display
func (e *SQLEngine) formatExecutionPlan(plan *QueryExecutionPlan, originalResult *QueryResult, originalErr error) (*QueryResult, error) {
	columns := []string{"Query Execution Plan"}
	rows := [][]sqltypes.Value{}

	// Build hierarchical plan display
	planLines := e.buildHierarchicalPlan(plan, originalErr)

	for _, line := range planLines {
		rows = append(rows, []sqltypes.Value{
			sqltypes.NewVarChar(line),
		})
	}

	if originalErr != nil {
		return &QueryResult{
			Columns:       columns,
			Rows:          rows,
			ExecutionPlan: plan,
			Error:         originalErr,
		}, originalErr
	}

	return &QueryResult{
		Columns:       columns,
		Rows:          rows,
		ExecutionPlan: plan,
	}, nil
}

// buildHierarchicalPlan creates a tree-like structure for the execution plan
func (e *SQLEngine) buildHierarchicalPlan(plan *QueryExecutionPlan, err error) []string {
	var lines []string

	// Root node - Query type and strategy
	lines = append(lines, fmt.Sprintf("%s Query (%s)", plan.QueryType, plan.ExecutionStrategy))

	// Aggregations section (if present)
	if len(plan.Aggregations) > 0 {
		lines = append(lines, "├── Aggregations")
		for i, agg := range plan.Aggregations {
			if i == len(plan.Aggregations)-1 {
				lines = append(lines, fmt.Sprintf("│   └── %s", agg))
			} else {
				lines = append(lines, fmt.Sprintf("│   ├── %s", agg))
			}
		}
	}

	// Data Sources section
	if len(plan.DataSources) > 0 {
		hasMore := len(plan.OptimizationsUsed) > 0 || plan.TotalRowsProcessed > 0 || len(plan.Details) > 0 || err != nil
		if hasMore {
			lines = append(lines, "├── Data Sources")
		} else {
			lines = append(lines, "└── Data Sources")
		}

		for i, source := range plan.DataSources {
			prefix := "│   "
			if !hasMore && i == len(plan.DataSources)-1 {
				prefix = "    "
			}

			if i == len(plan.DataSources)-1 {
				lines = append(lines, fmt.Sprintf("%s└── %s", prefix, e.formatDataSource(source)))
			} else {
				lines = append(lines, fmt.Sprintf("%s├── %s", prefix, e.formatDataSource(source)))
			}
		}
	}

	// Optimizations section
	if len(plan.OptimizationsUsed) > 0 {
		hasMore := plan.TotalRowsProcessed > 0 || len(plan.Details) > 0 || err != nil
		if hasMore {
			lines = append(lines, "├── Optimizations")
		} else {
			lines = append(lines, "└── Optimizations")
		}

		for i, opt := range plan.OptimizationsUsed {
			prefix := "│   "
			if !hasMore && i == len(plan.OptimizationsUsed)-1 {
				prefix = "    "
			}

			if i == len(plan.OptimizationsUsed)-1 {
				lines = append(lines, fmt.Sprintf("%s└── %s", prefix, e.formatOptimization(opt)))
			} else {
				lines = append(lines, fmt.Sprintf("%s├── %s", prefix, e.formatOptimization(opt)))
			}
		}
	}

	// Check for data sources tree availability
	partitionPaths, hasPartitions := plan.Details["partition_paths"].([]string)
	parquetFiles, _ := plan.Details["parquet_files"].([]string)
	liveLogFiles, _ := plan.Details["live_log_files"].([]string)

	// Statistics section
	statisticsPresent := plan.PartitionsScanned > 0 || plan.ParquetFilesScanned > 0 ||
		plan.LiveLogFilesScanned > 0 || plan.TotalRowsProcessed > 0

	if statisticsPresent {
		// Check if there are sections after Statistics (Data Sources Tree, Details, Performance)
		hasDataSourcesTree := hasPartitions && len(partitionPaths) > 0
		hasMoreAfterStats := hasDataSourcesTree || len(plan.Details) > 0 || err != nil || true // Performance is always present
		if hasMoreAfterStats {
			lines = append(lines, "├── Statistics")
		} else {
			lines = append(lines, "└── Statistics")
		}

		stats := []string{}
		if plan.PartitionsScanned > 0 {
			stats = append(stats, fmt.Sprintf("Partitions Scanned: %d", plan.PartitionsScanned))
		}
		if plan.ParquetFilesScanned > 0 {
			stats = append(stats, fmt.Sprintf("Parquet Files: %d", plan.ParquetFilesScanned))
		}
		if plan.LiveLogFilesScanned > 0 {
			stats = append(stats, fmt.Sprintf("Live Log Files: %d", plan.LiveLogFilesScanned))
		}
		// Always show row statistics for aggregations, even if 0 (to show fast path efficiency)
		if resultsReturned, hasResults := plan.Details["results_returned"]; hasResults {
			stats = append(stats, fmt.Sprintf("Rows Scanned: %d", plan.TotalRowsProcessed))
			stats = append(stats, fmt.Sprintf("Results Returned: %v", resultsReturned))

			// Add fast path explanation when no rows were scanned
			if plan.TotalRowsProcessed == 0 {
				stats = append(stats, "Scan Method: Parquet Metadata Only")
			}
		} else if plan.TotalRowsProcessed > 0 {
			stats = append(stats, fmt.Sprintf("Rows Processed: %d", plan.TotalRowsProcessed))
		}

		// Broker buffer information
		if plan.BrokerBufferQueried {
			stats = append(stats, fmt.Sprintf("Broker Buffer Queried: Yes (%d messages)", plan.BrokerBufferMessages))
			if plan.BufferStartIndex > 0 {
				stats = append(stats, fmt.Sprintf("Buffer Start Index: %d (deduplication enabled)", plan.BufferStartIndex))
			}
		}

		for i, stat := range stats {
			if hasMoreAfterStats {
				// More sections after Statistics, so use │   prefix
				if i == len(stats)-1 {
					lines = append(lines, fmt.Sprintf("│   └── %s", stat))
				} else {
					lines = append(lines, fmt.Sprintf("│   ├── %s", stat))
				}
			} else {
				// This is the last main section, so use space prefix for final item
				if i == len(stats)-1 {
					lines = append(lines, fmt.Sprintf("    └── %s", stat))
				} else {
					lines = append(lines, fmt.Sprintf("    ├── %s", stat))
				}
			}
		}
	}

	// Data Sources Tree section (if file paths are available)
	if hasPartitions && len(partitionPaths) > 0 {
		// Check if there are more sections after this
		hasMore := len(plan.Details) > 0 || err != nil
		if hasMore {
			lines = append(lines, "├── Data Sources Tree")
		} else {
			lines = append(lines, "├── Data Sources Tree") // Performance always comes after
		}

		// Build a tree structure for each partition
		for i, partition := range partitionPaths {
			isLastPartition := i == len(partitionPaths)-1

			// Show partition directory
			partitionPrefix := "├── "
			if isLastPartition {
				partitionPrefix = "└── "
			}
			lines = append(lines, fmt.Sprintf("│   %s%s/", partitionPrefix, partition))

			// Show parquet files in this partition
			partitionParquetFiles := make([]string, 0)
			for _, file := range parquetFiles {
				if strings.HasPrefix(file, partition+"/") {
					fileName := file[len(partition)+1:]
					partitionParquetFiles = append(partitionParquetFiles, fileName)
				}
			}

			// Show live log files in this partition
			partitionLiveLogFiles := make([]string, 0)
			for _, file := range liveLogFiles {
				if strings.HasPrefix(file, partition+"/") {
					fileName := file[len(partition)+1:]
					partitionLiveLogFiles = append(partitionLiveLogFiles, fileName)
				}
			}

			// Display files with proper tree formatting
			totalFiles := len(partitionParquetFiles) + len(partitionLiveLogFiles)
			fileIndex := 0

			// Display parquet files
			for _, fileName := range partitionParquetFiles {
				fileIndex++
				isLastFile := fileIndex == totalFiles && isLastPartition

				var filePrefix string
				if isLastPartition {
					if isLastFile {
						filePrefix = "    └── "
					} else {
						filePrefix = "    ├── "
					}
				} else {
					if isLastFile {
						filePrefix = "│   └── "
					} else {
						filePrefix = "│   ├── "
					}
				}
				lines = append(lines, fmt.Sprintf("│   %s%s (parquet)", filePrefix, fileName))
			}

			// Display live log files
			for _, fileName := range partitionLiveLogFiles {
				fileIndex++
				isLastFile := fileIndex == totalFiles && isLastPartition

				var filePrefix string
				if isLastPartition {
					if isLastFile {
						filePrefix = "    └── "
					} else {
						filePrefix = "    ├── "
					}
				} else {
					if isLastFile {
						filePrefix = "│   └── "
					} else {
						filePrefix = "│   ├── "
					}
				}
				lines = append(lines, fmt.Sprintf("│   %s%s (live log)", filePrefix, fileName))
			}
		}
	}

	// Details section
	// Filter out details that are shown elsewhere
	filteredDetails := make([]string, 0)
	for key, value := range plan.Details {
		// Skip keys that are already formatted and displayed in the Statistics section
		if key != "results_returned" && key != "partition_paths" && key != "parquet_files" && key != "live_log_files" {
			filteredDetails = append(filteredDetails, fmt.Sprintf("%s: %v", key, value))
		}
	}

	if len(filteredDetails) > 0 {
		// Performance is always present, so check if there are errors after Details
		hasMore := err != nil
		if hasMore {
			lines = append(lines, "├── Details")
		} else {
			lines = append(lines, "├── Details") // Performance always comes after
		}

		for i, detail := range filteredDetails {
			if i == len(filteredDetails)-1 {
				lines = append(lines, fmt.Sprintf("│   └── %s", detail))
			} else {
				lines = append(lines, fmt.Sprintf("│   ├── %s", detail))
			}
		}
	}

	// Performance section (always present)
	if err != nil {
		lines = append(lines, "├── Performance")
		lines = append(lines, fmt.Sprintf("│   └── Execution Time: %.3fms", plan.ExecutionTimeMs))
		lines = append(lines, "└── Error")
		lines = append(lines, fmt.Sprintf("    └── %s", err.Error()))
	} else {
		lines = append(lines, "└── Performance")
		lines = append(lines, fmt.Sprintf("    └── Execution Time: %.3fms", plan.ExecutionTimeMs))
	}

	return lines
}

// formatDataSource provides user-friendly names for data sources
func (e *SQLEngine) formatDataSource(source string) string {
	switch source {
	case "parquet_stats":
		return "Parquet Statistics (fast path)"
	case "parquet_files":
		return "Parquet Files (full scan)"
	case "live_logs":
		return "Live Log Files"
	case "broker_buffer":
		return "Broker Buffer (real-time)"
	default:
		return source
	}
}

// formatOptimization provides user-friendly names for optimizations
func (e *SQLEngine) formatOptimization(opt string) string {
	switch opt {
	case "parquet_statistics":
		return "Parquet Statistics Usage"
	case "live_log_counting":
		return "Live Log Row Counting"
	case "deduplication":
		return "Duplicate Data Avoidance"
	case "predicate_pushdown":
		return "WHERE Clause Pushdown"
	case "column_projection":
		return "Column Selection"
	case "limit_pushdown":
		return "LIMIT Optimization"
	default:
		return opt
	}
}

// executeUseStatement handles USE database statements to switch current database context
func (e *SQLEngine) executeUseStatement(ctx context.Context, stmt *UseStatement) (*QueryResult, error) {
	// Validate database name
	if stmt.Database == "" {
		err := fmt.Errorf("database name cannot be empty")
		return &QueryResult{Error: err}, err
	}

	// Set the current database in the catalog
	e.catalog.SetCurrentDatabase(stmt.Database)

	// Return success message
	result := &QueryResult{
		Columns: []string{"message"},
		Rows: [][]sqltypes.Value{
			{sqltypes.MakeString([]byte(fmt.Sprintf("Database changed to: %s", stmt.Database)))},
		},
		Error: nil,
	}
	return result, nil
}

// executeDDLStatement handles CREATE operations only
// Note: ALTER TABLE and DROP TABLE are not supported to protect topic data
func (e *SQLEngine) executeDDLStatement(ctx context.Context, stmt *DDLStatement) (*QueryResult, error) {
	switch stmt.Action {
	case CreateStr:
		return e.createTable(ctx, stmt)
	case AlterStr:
		err := fmt.Errorf("ALTER TABLE is not supported")
		return &QueryResult{Error: err}, err
	case DropStr:
		err := fmt.Errorf("DROP TABLE is not supported")
		return &QueryResult{Error: err}, err
	default:
		err := fmt.Errorf("unsupported DDL action: %s", stmt.Action)
		return &QueryResult{Error: err}, err
	}
}

// executeSelectStatementWithPlan handles SELECT queries with execution plan tracking
func (e *SQLEngine) executeSelectStatementWithPlan(ctx context.Context, stmt *SelectStatement, plan *QueryExecutionPlan) (*QueryResult, error) {
	// Parse aggregations to populate plan
	var aggregations []AggregationSpec
	hasAggregations := false
	selectAll := false

	for _, selectExpr := range stmt.SelectExprs {
		switch expr := selectExpr.(type) {
		case *StarExpr:
			selectAll = true
		case *AliasedExpr:
			switch col := expr.Expr.(type) {
			case *FuncExpr:
				// This is an aggregation function
				aggSpec, err := e.parseAggregationFunction(col, expr)
				if err != nil {
					return &QueryResult{Error: err}, err
				}
				if aggSpec != nil {
					aggregations = append(aggregations, *aggSpec)
					hasAggregations = true
					plan.Aggregations = append(plan.Aggregations, aggSpec.Function+"("+aggSpec.Column+")")
				}
			}
		}
	}

	// Execute the query (handle aggregations specially for plan tracking)
	var result *QueryResult
	var err error

	if hasAggregations {
		// Extract table information for aggregation execution
		var database, tableName string
		if len(stmt.From) == 1 {
			if table, ok := stmt.From[0].(*AliasedTableExpr); ok {
				if tableExpr, ok := table.Expr.(TableName); ok {
					tableName = tableExpr.Name.String()
					if tableExpr.Qualifier.String() != "" {
						database = tableExpr.Qualifier.String()
					}
				}
			}
		}

		// Use current database if not specified
		if database == "" {
			database = e.catalog.currentDatabase
			if database == "" {
				database = "default"
			}
		}

		// Create hybrid scanner for aggregation execution
		var filerClient filer_pb.FilerClient
		if e.catalog.brokerClient != nil {
			filerClient, err = e.catalog.brokerClient.GetFilerClient()
			if err != nil {
				return &QueryResult{Error: err}, err
			}
		}

		hybridScanner, err := NewHybridMessageScanner(filerClient, e.catalog.brokerClient, database, tableName)
		if err != nil {
			return &QueryResult{Error: err}, err
		}

		// Execute aggregation query with plan tracking
		result, err = e.executeAggregationQueryWithPlan(ctx, hybridScanner, aggregations, stmt, plan)
	} else {
		// Regular SELECT query with plan tracking
		result, err = e.executeSelectStatementWithBrokerStats(ctx, stmt, plan)
	}

	if err == nil && result != nil {
		// Extract table name for use in execution strategy determination
		var tableName string
		if len(stmt.From) == 1 {
			if table, ok := stmt.From[0].(*AliasedTableExpr); ok {
				if tableExpr, ok := table.Expr.(TableName); ok {
					tableName = tableExpr.Name.String()
				}
			}
		}

		// Try to get topic information for partition count and row processing stats
		if tableName != "" {
			// Try to discover partitions for statistics
			if partitions, discoverErr := e.discoverTopicPartitions("test", tableName); discoverErr == nil {
				plan.PartitionsScanned = len(partitions)
			}

			// For aggregations, determine actual processing based on execution strategy
			if hasAggregations {
				plan.Details["results_returned"] = len(result.Rows)

				// Determine actual work done based on execution strategy
				if stmt.Where == nil {
					// Use the same logic as actual execution to determine if fast path was used
					var filerClient filer_pb.FilerClient
					if e.catalog.brokerClient != nil {
						filerClient, _ = e.catalog.brokerClient.GetFilerClient()
					}

					hybridScanner, scannerErr := NewHybridMessageScanner(filerClient, e.catalog.brokerClient, "test", tableName)
					var canUseFastPath bool
					if scannerErr == nil {
						// Test if fast path can be used (same as actual execution)
						_, canOptimize := e.tryFastParquetAggregation(ctx, hybridScanner, aggregations)
						canUseFastPath = canOptimize
					} else {
						// Fallback to simple check
						canUseFastPath = true
						for _, spec := range aggregations {
							if !e.canUseParquetStatsForAggregation(spec) {
								canUseFastPath = false
								break
							}
						}
					}

					if canUseFastPath {
						// Fast path: minimal scanning (only live logs that weren't converted)
						if actualScanCount, countErr := e.getActualRowsScannedForFastPath(ctx, "test", tableName); countErr == nil {
							plan.TotalRowsProcessed = actualScanCount
						} else {
							plan.TotalRowsProcessed = 0 // Parquet stats only, no scanning
						}
					} else {
						// Full scan: count all rows
						if actualRowCount, countErr := e.getTopicTotalRowCount(ctx, "test", tableName); countErr == nil {
							plan.TotalRowsProcessed = actualRowCount
						} else {
							plan.TotalRowsProcessed = int64(len(result.Rows))
							plan.Details["note"] = "scan_count_unavailable"
						}
					}
				} else {
					// With WHERE clause: full scan required
					if actualRowCount, countErr := e.getTopicTotalRowCount(ctx, "test", tableName); countErr == nil {
						plan.TotalRowsProcessed = actualRowCount
					} else {
						plan.TotalRowsProcessed = int64(len(result.Rows))
						plan.Details["note"] = "scan_count_unavailable"
					}
				}
			} else {
				// For non-aggregations, result count is meaningful
				plan.TotalRowsProcessed = int64(len(result.Rows))
			}
		}

		// Determine execution strategy based on query type (reuse fast path detection from above)
		if hasAggregations {
			// Skip execution strategy determination if plan was already populated by aggregation execution
			// This prevents overwriting the correctly built plan from BuildAggregationPlan
			if plan.ExecutionStrategy == "" {
				// For aggregations, determine if fast path conditions are met
				if stmt.Where == nil {
					// Reuse the same logic used above for row counting
					var canUseFastPath bool
					if tableName != "" {
						var filerClient filer_pb.FilerClient
						if e.catalog.brokerClient != nil {
							filerClient, _ = e.catalog.brokerClient.GetFilerClient()
						}

						if filerClient != nil {
							hybridScanner, scannerErr := NewHybridMessageScanner(filerClient, e.catalog.brokerClient, "test", tableName)
							if scannerErr == nil {
								// Test if fast path can be used (same as actual execution)
								_, canOptimize := e.tryFastParquetAggregation(ctx, hybridScanner, aggregations)
								canUseFastPath = canOptimize
							} else {
								canUseFastPath = false
							}
						} else {
							// Fallback check
							canUseFastPath = true
							for _, spec := range aggregations {
								if !e.canUseParquetStatsForAggregation(spec) {
									canUseFastPath = false
									break
								}
							}
						}
					} else {
						canUseFastPath = false
					}

					if canUseFastPath {
						plan.ExecutionStrategy = "hybrid_fast_path"
						plan.OptimizationsUsed = append(plan.OptimizationsUsed, "parquet_statistics", "live_log_counting", "deduplication")
						plan.DataSources = []string{"parquet_stats", "live_logs"}
					} else {
						plan.ExecutionStrategy = "full_scan"
						plan.DataSources = []string{"live_logs", "parquet_files"}
					}
				} else {
					plan.ExecutionStrategy = "full_scan"
					plan.DataSources = []string{"live_logs", "parquet_files"}
					plan.OptimizationsUsed = append(plan.OptimizationsUsed, "predicate_pushdown")
				}
			}
		} else {
			// For regular SELECT queries
			if selectAll {
				plan.ExecutionStrategy = "hybrid_scan"
				plan.DataSources = []string{"live_logs", "parquet_files"}
			} else {
				plan.ExecutionStrategy = "column_projection"
				plan.DataSources = []string{"live_logs", "parquet_files"}
				plan.OptimizationsUsed = append(plan.OptimizationsUsed, "column_projection")
			}
		}

		// Add WHERE clause information
		if stmt.Where != nil {
			// Only add predicate_pushdown if not already added
			alreadyHasPredicate := false
			for _, opt := range plan.OptimizationsUsed {
				if opt == "predicate_pushdown" {
					alreadyHasPredicate = true
					break
				}
			}
			if !alreadyHasPredicate {
				plan.OptimizationsUsed = append(plan.OptimizationsUsed, "predicate_pushdown")
			}
			plan.Details["where_clause"] = "present"
		}

		// Add LIMIT information
		if stmt.Limit != nil {
			plan.OptimizationsUsed = append(plan.OptimizationsUsed, "limit_pushdown")
			if stmt.Limit.Rowcount != nil {
				if limitExpr, ok := stmt.Limit.Rowcount.(*SQLVal); ok && limitExpr.Type == IntVal {
					plan.Details["limit"] = string(limitExpr.Val)
				}
			}
		}
	}

	return result, err
}

// executeSelectStatement handles SELECT queries
// Assumptions:
// 1. Queries run against Parquet files in MQ topics
// 2. Predicate pushdown is used for efficiency
// 3. Cross-topic joins are supported via partition-aware execution
func (e *SQLEngine) executeSelectStatement(ctx context.Context, stmt *SelectStatement) (*QueryResult, error) {
	// Parse FROM clause to get table (topic) information
	if len(stmt.From) != 1 {
		err := fmt.Errorf("SELECT supports single table queries only")
		return &QueryResult{Error: err}, err
	}

	// Extract table reference
	var database, tableName string
	switch table := stmt.From[0].(type) {
	case *AliasedTableExpr:
		switch tableExpr := table.Expr.(type) {
		case TableName:
			tableName = tableExpr.Name.String()
			if tableExpr.Qualifier != nil && tableExpr.Qualifier.String() != "" {
				database = tableExpr.Qualifier.String()
			}
		default:
			err := fmt.Errorf("unsupported table expression: %T", tableExpr)
			return &QueryResult{Error: err}, err
		}
	default:
		err := fmt.Errorf("unsupported FROM clause: %T", table)
		return &QueryResult{Error: err}, err
	}

	// Use current database context if not specified
	if database == "" {
		database = e.catalog.GetCurrentDatabase()
		if database == "" {
			database = "default"
		}
	}

	// Auto-discover and register topic if not already in catalog
	if _, err := e.catalog.GetTableInfo(database, tableName); err != nil {
		// Topic not in catalog, try to discover and register it
		if regErr := e.discoverAndRegisterTopic(ctx, database, tableName); regErr != nil {
			// Return error immediately for non-existent topics instead of falling back to sample data
			return &QueryResult{Error: regErr}, regErr
		}
	}

	// Create HybridMessageScanner for the topic (reads both live logs + Parquet files)
	// Get filerClient from broker connection (works with both real and mock brokers)
	var filerClient filer_pb.FilerClient
	var filerClientErr error
	filerClient, filerClientErr = e.catalog.brokerClient.GetFilerClient()
	if filerClientErr != nil {
		// Return error if filer client is not available for topic access
		return &QueryResult{Error: filerClientErr}, filerClientErr
	}

	hybridScanner, err := NewHybridMessageScanner(filerClient, e.catalog.brokerClient, database, tableName)
	if err != nil {
		// Handle quiet topics gracefully: topics exist but have no active schema/brokers
		if IsNoSchemaError(err) {
			// Return empty result for quiet topics (normal in production environments)
			return &QueryResult{
				Columns:  []string{},
				Rows:     [][]sqltypes.Value{},
				Database: database,
				Table:    tableName,
			}, nil
		}
		// Return error for other access issues (truly non-existent topics, etc.)
		topicErr := fmt.Errorf("failed to access topic %s.%s: %v", database, tableName, err)
		return &QueryResult{Error: topicErr}, topicErr
	}

	// Parse SELECT columns and detect aggregation functions
	var columns []string
	var aggregations []AggregationSpec
	selectAll := false
	hasAggregations := false
	_ = hasAggregations // Used later in aggregation routing
	// Track required base columns for arithmetic expressions
	baseColumnsSet := make(map[string]bool)

	for _, selectExpr := range stmt.SelectExprs {
		switch expr := selectExpr.(type) {
		case *StarExpr:
			selectAll = true
		case *AliasedExpr:
			switch col := expr.Expr.(type) {
			case *ColName:
				colName := col.Name.String()

				// Check if this "column" is actually an arithmetic expression with functions
				if arithmeticExpr := e.parseColumnLevelCalculation(colName); arithmeticExpr != nil {
					columns = append(columns, e.getArithmeticExpressionAlias(arithmeticExpr))
					e.extractBaseColumns(arithmeticExpr, baseColumnsSet)
				} else {
					columns = append(columns, colName)
					baseColumnsSet[colName] = true
				}
			case *ArithmeticExpr:
				// Handle arithmetic expressions like id+user_id and string concatenation like name||suffix
				columns = append(columns, e.getArithmeticExpressionAlias(col))
				// Extract base columns needed for this arithmetic expression
				e.extractBaseColumns(col, baseColumnsSet)
			case *SQLVal:
				// Handle string/numeric literals like 'good', 123, etc.
				columns = append(columns, e.getSQLValAlias(col))
			case *FuncExpr:
				// Distinguish between aggregation functions and string functions
				funcName := strings.ToUpper(col.Name.String())
				if e.isAggregationFunction(funcName) {
					// Handle aggregation functions
					aggSpec, err := e.parseAggregationFunction(col, expr)
					if err != nil {
						return &QueryResult{Error: err}, err
					}
					aggregations = append(aggregations, *aggSpec)
					hasAggregations = true
				} else if e.isStringFunction(funcName) {
					// Handle string functions like UPPER, LENGTH, etc.
					columns = append(columns, e.getStringFunctionAlias(col))
					// Extract base columns needed for this string function
					e.extractBaseColumnsFromFunction(col, baseColumnsSet)
				} else if e.isDateTimeFunction(funcName) {
					// Handle datetime functions like CURRENT_DATE, NOW, EXTRACT, DATE_TRUNC
					columns = append(columns, e.getDateTimeFunctionAlias(col))
					// Extract base columns needed for this datetime function
					e.extractBaseColumnsFromFunction(col, baseColumnsSet)
				} else {
					return &QueryResult{Error: fmt.Errorf("unsupported function: %s", funcName)}, fmt.Errorf("unsupported function: %s", funcName)
				}
			default:
				err := fmt.Errorf("unsupported SELECT expression: %T", col)
				return &QueryResult{Error: err}, err
			}
		default:
			err := fmt.Errorf("unsupported SELECT expression: %T", expr)
			return &QueryResult{Error: err}, err
		}
	}

	// If we have aggregations, use aggregation query path
	if hasAggregations {
		return e.executeAggregationQuery(ctx, hybridScanner, aggregations, stmt)
	}

	// Parse WHERE clause for predicate pushdown
	var predicate func(*schema_pb.RecordValue) bool
	if stmt.Where != nil {
		predicate, err = e.buildPredicateWithContext(stmt.Where.Expr, stmt.SelectExprs)
		if err != nil {
			return &QueryResult{Error: err}, err
		}
	}

	// Parse LIMIT and OFFSET clauses
	// Use -1 to distinguish "no LIMIT" from "LIMIT 0"
	limit := -1
	offset := 0
	if stmt.Limit != nil && stmt.Limit.Rowcount != nil {
		switch limitExpr := stmt.Limit.Rowcount.(type) {
		case *SQLVal:
			if limitExpr.Type == IntVal {
				var parseErr error
				limit64, parseErr := strconv.ParseInt(string(limitExpr.Val), 10, 64)
				if parseErr != nil {
					return &QueryResult{Error: parseErr}, parseErr
				}
				if limit64 > math.MaxInt32 || limit64 < 0 {
					return &QueryResult{Error: fmt.Errorf("LIMIT value %d is out of valid range", limit64)}, fmt.Errorf("LIMIT value %d is out of valid range", limit64)
				}
				limit = int(limit64)
			}
		}
	}

	// Parse OFFSET clause if present
	if stmt.Limit != nil && stmt.Limit.Offset != nil {
		switch offsetExpr := stmt.Limit.Offset.(type) {
		case *SQLVal:
			if offsetExpr.Type == IntVal {
				var parseErr error
				offset64, parseErr := strconv.ParseInt(string(offsetExpr.Val), 10, 64)
				if parseErr != nil {
					return &QueryResult{Error: parseErr}, parseErr
				}
				if offset64 > math.MaxInt32 || offset64 < 0 {
					return &QueryResult{Error: fmt.Errorf("OFFSET value %d is out of valid range", offset64)}, fmt.Errorf("OFFSET value %d is out of valid range", offset64)
				}
				offset = int(offset64)
			}
		}
	}

	// Build hybrid scan options
	// Extract time filters from WHERE clause to optimize scanning
	startTimeNs, stopTimeNs := int64(0), int64(0)
	if stmt.Where != nil {
		startTimeNs, stopTimeNs = e.extractTimeFilters(stmt.Where.Expr)
	}

	hybridScanOptions := HybridScanOptions{
		StartTimeNs: startTimeNs, // Extracted from WHERE clause time comparisons
		StopTimeNs:  stopTimeNs,  // Extracted from WHERE clause time comparisons
		Limit:       limit,
		Offset:      offset,
		Predicate:   predicate,
	}

	if !selectAll {
		// Convert baseColumnsSet to slice for hybrid scan options
		baseColumns := make([]string, 0, len(baseColumnsSet))
		for columnName := range baseColumnsSet {
			baseColumns = append(baseColumns, columnName)
		}
		// Use base columns (not expression aliases) for data retrieval
		if len(baseColumns) > 0 {
			hybridScanOptions.Columns = baseColumns
		} else {
			// If no base columns found (shouldn't happen), use original columns
			hybridScanOptions.Columns = columns
		}
	}

	// Execute the hybrid scan (live logs + Parquet files)
	results, err := hybridScanner.Scan(ctx, hybridScanOptions)
	if err != nil {
		return &QueryResult{Error: err}, err
	}

	// Convert to SQL result format
	if selectAll {
		if len(columns) > 0 {
			// SELECT *, specific_columns - include both auto-discovered and explicit columns
			return hybridScanner.ConvertToSQLResultWithMixedColumns(results, columns), nil
		} else {
			// SELECT * only - let converter determine all columns (excludes system columns)
			columns = nil
			return hybridScanner.ConvertToSQLResult(results, columns), nil
		}
	}

	// Handle custom column expressions (including arithmetic)
	return e.ConvertToSQLResultWithExpressions(hybridScanner, results, stmt.SelectExprs), nil
}

// executeSelectStatementWithBrokerStats handles SELECT queries with broker buffer statistics capture
// This is used by EXPLAIN queries to capture complete data source information including broker memory
func (e *SQLEngine) executeSelectStatementWithBrokerStats(ctx context.Context, stmt *SelectStatement, plan *QueryExecutionPlan) (*QueryResult, error) {
	// Parse FROM clause to get table (topic) information
	if len(stmt.From) != 1 {
		err := fmt.Errorf("SELECT supports single table queries only")
		return &QueryResult{Error: err}, err
	}

	// Extract table reference
	var database, tableName string
	switch table := stmt.From[0].(type) {
	case *AliasedTableExpr:
		switch tableExpr := table.Expr.(type) {
		case TableName:
			tableName = tableExpr.Name.String()
			if tableExpr.Qualifier != nil && tableExpr.Qualifier.String() != "" {
				database = tableExpr.Qualifier.String()
			}
		default:
			err := fmt.Errorf("unsupported table expression: %T", tableExpr)
			return &QueryResult{Error: err}, err
		}
	default:
		err := fmt.Errorf("unsupported FROM clause: %T", table)
		return &QueryResult{Error: err}, err
	}

	// Use current database context if not specified
	if database == "" {
		database = e.catalog.GetCurrentDatabase()
		if database == "" {
			database = "default"
		}
	}

	// Auto-discover and register topic if not already in catalog
	if _, err := e.catalog.GetTableInfo(database, tableName); err != nil {
		// Topic not in catalog, try to discover and register it
		if regErr := e.discoverAndRegisterTopic(ctx, database, tableName); regErr != nil {
			// Return error immediately for non-existent topics instead of falling back to sample data
			return &QueryResult{Error: regErr}, regErr
		}
	}

	// Create HybridMessageScanner for the topic (reads both live logs + Parquet files)
	// Get filerClient from broker connection (works with both real and mock brokers)
	var filerClient filer_pb.FilerClient
	var filerClientErr error
	filerClient, filerClientErr = e.catalog.brokerClient.GetFilerClient()
	if filerClientErr != nil {
		// Return error if filer client is not available for topic access
		return &QueryResult{Error: filerClientErr}, filerClientErr
	}

	hybridScanner, err := NewHybridMessageScanner(filerClient, e.catalog.brokerClient, database, tableName)
	if err != nil {
		// Handle quiet topics gracefully: topics exist but have no active schema/brokers
		if IsNoSchemaError(err) {
			// Return empty result for quiet topics (normal in production environments)
			return &QueryResult{
				Columns:  []string{},
				Rows:     [][]sqltypes.Value{},
				Database: database,
				Table:    tableName,
			}, nil
		}
		// Return error for other access issues (truly non-existent topics, etc.)
		topicErr := fmt.Errorf("failed to access topic %s.%s: %v", database, tableName, err)
		return &QueryResult{Error: topicErr}, topicErr
	}

	// Parse SELECT columns and detect aggregation functions
	var columns []string
	var aggregations []AggregationSpec
	selectAll := false
	hasAggregations := false
	_ = hasAggregations // Used later in aggregation routing
	// Track required base columns for arithmetic expressions
	baseColumnsSet := make(map[string]bool)

	for _, selectExpr := range stmt.SelectExprs {
		switch expr := selectExpr.(type) {
		case *StarExpr:
			selectAll = true
		case *AliasedExpr:
			switch col := expr.Expr.(type) {
			case *ColName:
				colName := col.Name.String()
				columns = append(columns, colName)
				baseColumnsSet[colName] = true
			case *ArithmeticExpr:
				// Handle arithmetic expressions like id+user_id and string concatenation like name||suffix
				columns = append(columns, e.getArithmeticExpressionAlias(col))
				// Extract base columns needed for this arithmetic expression
				e.extractBaseColumns(col, baseColumnsSet)
			case *SQLVal:
				// Handle string/numeric literals like 'good', 123, etc.
				columns = append(columns, e.getSQLValAlias(col))
			case *FuncExpr:
				// Distinguish between aggregation functions and string functions
				funcName := strings.ToUpper(col.Name.String())
				if e.isAggregationFunction(funcName) {
					// Handle aggregation functions
					aggSpec, err := e.parseAggregationFunction(col, expr)
					if err != nil {
						return &QueryResult{Error: err}, err
					}
					aggregations = append(aggregations, *aggSpec)
					hasAggregations = true
				} else if e.isStringFunction(funcName) {
					// Handle string functions like UPPER, LENGTH, etc.
					columns = append(columns, e.getStringFunctionAlias(col))
					// Extract base columns needed for this string function
					e.extractBaseColumnsFromFunction(col, baseColumnsSet)
				} else if e.isDateTimeFunction(funcName) {
					// Handle datetime functions like CURRENT_DATE, NOW, EXTRACT, DATE_TRUNC
					columns = append(columns, e.getDateTimeFunctionAlias(col))
					// Extract base columns needed for this datetime function
					e.extractBaseColumnsFromFunction(col, baseColumnsSet)
				} else {
					return &QueryResult{Error: fmt.Errorf("unsupported function: %s", funcName)}, fmt.Errorf("unsupported function: %s", funcName)
				}
			default:
				err := fmt.Errorf("unsupported SELECT expression: %T", col)
				return &QueryResult{Error: err}, err
			}
		default:
			err := fmt.Errorf("unsupported SELECT expression: %T", expr)
			return &QueryResult{Error: err}, err
		}
	}

	// If we have aggregations, use aggregation query path
	if hasAggregations {
		return e.executeAggregationQuery(ctx, hybridScanner, aggregations, stmt)
	}

	// Parse WHERE clause for predicate pushdown
	var predicate func(*schema_pb.RecordValue) bool
	if stmt.Where != nil {
		predicate, err = e.buildPredicateWithContext(stmt.Where.Expr, stmt.SelectExprs)
		if err != nil {
			return &QueryResult{Error: err}, err
		}
	}

	// Parse LIMIT and OFFSET clauses
	// Use -1 to distinguish "no LIMIT" from "LIMIT 0"
	limit := -1
	offset := 0
	if stmt.Limit != nil && stmt.Limit.Rowcount != nil {
		switch limitExpr := stmt.Limit.Rowcount.(type) {
		case *SQLVal:
			if limitExpr.Type == IntVal {
				var parseErr error
				limit64, parseErr := strconv.ParseInt(string(limitExpr.Val), 10, 64)
				if parseErr != nil {
					return &QueryResult{Error: parseErr}, parseErr
				}
				if limit64 > math.MaxInt32 || limit64 < 0 {
					return &QueryResult{Error: fmt.Errorf("LIMIT value %d is out of valid range", limit64)}, fmt.Errorf("LIMIT value %d is out of valid range", limit64)
				}
				limit = int(limit64)
			}
		}
	}

	// Parse OFFSET clause if present
	if stmt.Limit != nil && stmt.Limit.Offset != nil {
		switch offsetExpr := stmt.Limit.Offset.(type) {
		case *SQLVal:
			if offsetExpr.Type == IntVal {
				var parseErr error
				offset64, parseErr := strconv.ParseInt(string(offsetExpr.Val), 10, 64)
				if parseErr != nil {
					return &QueryResult{Error: parseErr}, parseErr
				}
				if offset64 > math.MaxInt32 || offset64 < 0 {
					return &QueryResult{Error: fmt.Errorf("OFFSET value %d is out of valid range", offset64)}, fmt.Errorf("OFFSET value %d is out of valid range", offset64)
				}
				offset = int(offset64)
			}
		}
	}

	// Build hybrid scan options
	// Extract time filters from WHERE clause to optimize scanning
	startTimeNs, stopTimeNs := int64(0), int64(0)
	if stmt.Where != nil {
		startTimeNs, stopTimeNs = e.extractTimeFilters(stmt.Where.Expr)
	}

	hybridScanOptions := HybridScanOptions{
		StartTimeNs: startTimeNs, // Extracted from WHERE clause time comparisons
		StopTimeNs:  stopTimeNs,  // Extracted from WHERE clause time comparisons
		Limit:       limit,
		Offset:      offset,
		Predicate:   predicate,
	}

	if !selectAll {
		// Convert baseColumnsSet to slice for hybrid scan options
		baseColumns := make([]string, 0, len(baseColumnsSet))
		for columnName := range baseColumnsSet {
			baseColumns = append(baseColumns, columnName)
		}
		// Use base columns (not expression aliases) for data retrieval
		if len(baseColumns) > 0 {
			hybridScanOptions.Columns = baseColumns
		} else {
			// If no base columns found (shouldn't happen), use original columns
			hybridScanOptions.Columns = columns
		}
	}

	// Execute the hybrid scan with stats capture for EXPLAIN
	var results []HybridScanResult
	if plan != nil {
		// EXPLAIN mode - capture broker buffer stats
		var stats *HybridScanStats
		results, stats, err = hybridScanner.ScanWithStats(ctx, hybridScanOptions)
		if err != nil {
			return &QueryResult{Error: err}, err
		}

		// Populate plan with broker buffer information
		if stats != nil {
			plan.BrokerBufferQueried = stats.BrokerBufferQueried
			plan.BrokerBufferMessages = stats.BrokerBufferMessages
			plan.BufferStartIndex = stats.BufferStartIndex

			// Add broker_buffer to data sources if buffer was queried
			if stats.BrokerBufferQueried {
				// Check if broker_buffer is already in data sources
				hasBrokerBuffer := false
				for _, source := range plan.DataSources {
					if source == "broker_buffer" {
						hasBrokerBuffer = true
						break
					}
				}
				if !hasBrokerBuffer {
					plan.DataSources = append(plan.DataSources, "broker_buffer")
				}
			}
		}
	} else {
		// Normal mode - just get results
		results, err = hybridScanner.Scan(ctx, hybridScanOptions)
		if err != nil {
			return &QueryResult{Error: err}, err
		}
	}

	// Convert to SQL result format
	if selectAll {
		if len(columns) > 0 {
			// SELECT *, specific_columns - include both auto-discovered and explicit columns
			return hybridScanner.ConvertToSQLResultWithMixedColumns(results, columns), nil
		} else {
			// SELECT * only - let converter determine all columns (excludes system columns)
			columns = nil
			return hybridScanner.ConvertToSQLResult(results, columns), nil
		}
	}

	// Handle custom column expressions (including arithmetic)
	return e.ConvertToSQLResultWithExpressions(hybridScanner, results, stmt.SelectExprs), nil
}

// extractTimeFilters extracts time range filters from WHERE clause for optimization
// This allows push-down of time-based queries to improve scan performance
// Returns (startTimeNs, stopTimeNs) where 0 means unbounded
func (e *SQLEngine) extractTimeFilters(expr ExprNode) (int64, int64) {
	startTimeNs, stopTimeNs := int64(0), int64(0)

	// Recursively extract time filters from expression tree
	e.extractTimeFiltersRecursive(expr, &startTimeNs, &stopTimeNs)

	// Special case: if startTimeNs == stopTimeNs, treat it like an equality query
	// to avoid premature scan termination. The predicate will handle exact matching.
	if startTimeNs != 0 && startTimeNs == stopTimeNs {
		stopTimeNs = 0
	}

	return startTimeNs, stopTimeNs
}

// extractTimeFiltersRecursive recursively processes WHERE expressions to find time comparisons
func (e *SQLEngine) extractTimeFiltersRecursive(expr ExprNode, startTimeNs, stopTimeNs *int64) {
	switch exprType := expr.(type) {
	case *ComparisonExpr:
		e.extractTimeFromComparison(exprType, startTimeNs, stopTimeNs)
	case *AndExpr:
		// For AND expressions, combine time filters (intersection)
		e.extractTimeFiltersRecursive(exprType.Left, startTimeNs, stopTimeNs)
		e.extractTimeFiltersRecursive(exprType.Right, startTimeNs, stopTimeNs)
	case *OrExpr:
		// For OR expressions, we can't easily optimize time ranges
		// Skip time filter extraction for OR clauses to avoid incorrect results
		return
	case *ParenExpr:
		// Unwrap parentheses and continue
		e.extractTimeFiltersRecursive(exprType.Expr, startTimeNs, stopTimeNs)
	}
}

// extractTimeFromComparison extracts time bounds from comparison expressions
// Handles comparisons against timestamp columns (system columns and schema-defined timestamp types)
func (e *SQLEngine) extractTimeFromComparison(comp *ComparisonExpr, startTimeNs, stopTimeNs *int64) {
	// Check if this is a time-related column comparison
	leftCol := e.getColumnName(comp.Left)
	rightCol := e.getColumnName(comp.Right)

	var valueExpr ExprNode
	var reversed bool

	// Determine which side is the time column (using schema types)
	if e.isTimestampColumn(leftCol) {
		valueExpr = comp.Right
		reversed = false
	} else if e.isTimestampColumn(rightCol) {
		valueExpr = comp.Left
		reversed = true
	} else {
		// Not a time comparison
		return
	}

	// Extract the time value
	timeValue := e.extractTimeValue(valueExpr)
	if timeValue == 0 {
		// Couldn't parse time value
		return
	}

	// Apply the comparison operator to determine time bounds
	operator := comp.Operator
	if reversed {
		// Reverse the operator if column and value are swapped
		operator = e.reverseOperator(operator)
	}

	switch operator {
	case GreaterThanStr: // timestamp > value
		if *startTimeNs == 0 || timeValue > *startTimeNs {
			*startTimeNs = timeValue
		}
	case GreaterEqualStr: // timestamp >= value
		if *startTimeNs == 0 || timeValue >= *startTimeNs {
			*startTimeNs = timeValue
		}
	case LessThanStr: // timestamp < value
		if *stopTimeNs == 0 || timeValue < *stopTimeNs {
			*stopTimeNs = timeValue
		}
	case LessEqualStr: // timestamp <= value
		if *stopTimeNs == 0 || timeValue <= *stopTimeNs {
			*stopTimeNs = timeValue
		}
	case EqualStr: // timestamp = value (point query)
		// For exact matches, we set startTimeNs slightly before the target
		// This works around a scan boundary bug where >= X starts after X instead of at X
		// The predicate function will handle exact matching
		*startTimeNs = timeValue - 1
		// Do NOT set stopTimeNs - let the predicate handle exact matching
	}
}

// isTimestampColumn checks if a column is a timestamp using schema type information
func (e *SQLEngine) isTimestampColumn(columnName string) bool {
	if columnName == "" {
		return false
	}

	// System timestamp columns are always time columns
	if columnName == SW_COLUMN_NAME_TIMESTAMP {
		return true
	}

	// For user-defined columns, check actual schema type information
	if e.catalog != nil {
		currentDB := e.catalog.GetCurrentDatabase()
		if currentDB == "" {
			currentDB = "default"
		}

		// Get current table context from query execution
		// Note: This is a limitation - we need table context here
		// In a full implementation, this would be passed from the query context
		tableInfo, err := e.getCurrentTableInfo(currentDB)
		if err == nil && tableInfo != nil {
			for _, col := range tableInfo.Columns {
				if strings.EqualFold(col.Name, columnName) {
					// Use actual SQL type to determine if this is a timestamp
					return e.isSQLTypeTimestamp(col.Type)
				}
			}
		}
	}

	// Only return true if we have explicit type information
	// No guessing based on column names
	return false
}

// isSQLTypeTimestamp checks if a SQL type string represents a timestamp type
func (e *SQLEngine) isSQLTypeTimestamp(sqlType string) bool {
	upperType := strings.ToUpper(strings.TrimSpace(sqlType))

	// Handle type with precision/length specifications
	if idx := strings.Index(upperType, "("); idx != -1 {
		upperType = upperType[:idx]
	}

	switch upperType {
	case "TIMESTAMP", "DATETIME":
		return true
	case "BIGINT":
		// BIGINT could be a timestamp if it follows the pattern for timestamp storage
		// This is a heuristic - in a better system, we'd have semantic type information
		return false // Conservative approach - require explicit TIMESTAMP type
	default:
		return false
	}
}

// getCurrentTableInfo attempts to get table info for the current query context
// This is a simplified implementation - ideally table context would be passed explicitly
func (e *SQLEngine) getCurrentTableInfo(database string) (*TableInfo, error) {
	// This is a limitation of the current architecture
	// In practice, we'd need the table context from the current query
	// For now, return nil to fallback to naming conventions
	// TODO: Enhance architecture to pass table context through query execution
	return nil, fmt.Errorf("table context not available in current architecture")
}

// getColumnName extracts column name from expression (handles ColName types)
func (e *SQLEngine) getColumnName(expr ExprNode) string {
	switch exprType := expr.(type) {
	case *ColName:
		return exprType.Name.String()
	}
	return ""
}

// resolveColumnAlias tries to resolve a column name that might be an alias
func (e *SQLEngine) resolveColumnAlias(columnName string, selectExprs []SelectExpr) string {
	if selectExprs == nil {
		return columnName
	}

	// Check if this column name is actually an alias in the SELECT list
	for _, selectExpr := range selectExprs {
		if aliasedExpr, ok := selectExpr.(*AliasedExpr); ok && aliasedExpr != nil {
			// Check if the alias matches our column name
			if aliasedExpr.As != nil && !aliasedExpr.As.IsEmpty() && aliasedExpr.As.String() == columnName {
				// If the aliased expression is a column, return the actual column name
				if colExpr, ok := aliasedExpr.Expr.(*ColName); ok && colExpr != nil {
					return colExpr.Name.String()
				}
			}
		}
	}

	// If no alias found, return the original column name
	return columnName
}

// extractTimeValue parses time values from SQL expressions
// Supports nanosecond timestamps, ISO dates, and relative times
func (e *SQLEngine) extractTimeValue(expr ExprNode) int64 {
	switch exprType := expr.(type) {
	case *SQLVal:
		switch exprType.Type {
		case IntVal:
			// Parse as nanosecond timestamp
			if val, err := strconv.ParseInt(string(exprType.Val), 10, 64); err == nil {
				return val
			}
		case StrVal:
			// Parse as ISO date or other string formats
			timeStr := string(exprType.Val)

			// Try parsing as RFC3339 (ISO 8601)
			if t, err := time.Parse(time.RFC3339, timeStr); err == nil {
				return t.UnixNano()
			}

			// Try parsing as RFC3339 with nanoseconds
			if t, err := time.Parse(time.RFC3339Nano, timeStr); err == nil {
				return t.UnixNano()
			}

			// Try parsing as date only (YYYY-MM-DD)
			if t, err := time.Parse("2006-01-02", timeStr); err == nil {
				return t.UnixNano()
			}

			// Try parsing as datetime (YYYY-MM-DD HH:MM:SS)
			if t, err := time.Parse("2006-01-02 15:04:05", timeStr); err == nil {
				return t.UnixNano()
			}
		}
	}

	return 0 // Couldn't parse
}

// reverseOperator reverses comparison operators when column and value are swapped
func (e *SQLEngine) reverseOperator(op string) string {
	switch op {
	case GreaterThanStr:
		return LessThanStr
	case GreaterEqualStr:
		return LessEqualStr
	case LessThanStr:
		return GreaterThanStr
	case LessEqualStr:
		return GreaterEqualStr
	case EqualStr:
		return EqualStr
	case NotEqualStr:
		return NotEqualStr
	default:
		return op
	}
}

// buildPredicate creates a predicate function from a WHERE clause expression
// This is a simplified implementation - a full implementation would be much more complex
func (e *SQLEngine) buildPredicate(expr ExprNode) (func(*schema_pb.RecordValue) bool, error) {
	return e.buildPredicateWithContext(expr, nil)
}

// buildPredicateWithContext creates a predicate function with SELECT context for alias resolution
func (e *SQLEngine) buildPredicateWithContext(expr ExprNode, selectExprs []SelectExpr) (func(*schema_pb.RecordValue) bool, error) {
	switch exprType := expr.(type) {
	case *ComparisonExpr:
		return e.buildComparisonPredicateWithContext(exprType, selectExprs)
	case *AndExpr:
		leftPred, err := e.buildPredicateWithContext(exprType.Left, selectExprs)
		if err != nil {
			return nil, err
		}
		rightPred, err := e.buildPredicateWithContext(exprType.Right, selectExprs)
		if err != nil {
			return nil, err
		}
		return func(record *schema_pb.RecordValue) bool {
			return leftPred(record) && rightPred(record)
		}, nil
	case *OrExpr:
		leftPred, err := e.buildPredicateWithContext(exprType.Left, selectExprs)
		if err != nil {
			return nil, err
		}
		rightPred, err := e.buildPredicateWithContext(exprType.Right, selectExprs)
		if err != nil {
			return nil, err
		}
		return func(record *schema_pb.RecordValue) bool {
			return leftPred(record) || rightPred(record)
		}, nil
	default:
		return nil, fmt.Errorf("unsupported WHERE expression: %T", expr)
	}
}

// buildPredicateWithAliases creates a predicate function with alias resolution support
func (e *SQLEngine) buildPredicateWithAliases(expr ExprNode, aliases map[string]ExprNode) (func(*schema_pb.RecordValue) bool, error) {
	switch exprType := expr.(type) {
	case *ComparisonExpr:
		return e.buildComparisonPredicateWithAliases(exprType, aliases)
	case *AndExpr:
		leftPred, err := e.buildPredicateWithAliases(exprType.Left, aliases)
		if err != nil {
			return nil, err
		}
		rightPred, err := e.buildPredicateWithAliases(exprType.Right, aliases)
		if err != nil {
			return nil, err
		}
		return func(record *schema_pb.RecordValue) bool {
			return leftPred(record) && rightPred(record)
		}, nil
	case *OrExpr:
		leftPred, err := e.buildPredicateWithAliases(exprType.Left, aliases)
		if err != nil {
			return nil, err
		}
		rightPred, err := e.buildPredicateWithAliases(exprType.Right, aliases)
		if err != nil {
			return nil, err
		}
		return func(record *schema_pb.RecordValue) bool {
			return leftPred(record) || rightPred(record)
		}, nil
	default:
		return nil, fmt.Errorf("unsupported WHERE expression: %T", expr)
	}
}

// buildComparisonPredicateWithAliases creates a predicate for comparison operations with alias support
func (e *SQLEngine) buildComparisonPredicateWithAliases(expr *ComparisonExpr, aliases map[string]ExprNode) (func(*schema_pb.RecordValue) bool, error) {
	var columnName string
	var compareValue interface{}
	var operator string

	// Extract the comparison details, resolving aliases if needed
	leftCol := e.getColumnNameWithAliases(expr.Left, aliases)
	rightCol := e.getColumnNameWithAliases(expr.Right, aliases)
	operator = e.normalizeOperator(expr.Operator)

	if leftCol != "" && rightCol == "" {
		// Left side is column, right side is value
		columnName = leftCol
		val, err := e.extractValueFromExpr(expr.Right)
		if err != nil {
			return nil, err
		}
		compareValue = val
	} else if rightCol != "" && leftCol == "" {
		// Right side is column, left side is value
		columnName = rightCol
		val, err := e.extractValueFromExpr(expr.Left)
		if err != nil {
			return nil, err
		}
		compareValue = val
		// Reverse the operator when column is on the right
		operator = e.reverseOperator(operator)
	} else if leftCol != "" && rightCol != "" {
		return nil, fmt.Errorf("column-to-column comparisons not yet supported")
	} else {
		return nil, fmt.Errorf("at least one side of comparison must be a column")
	}

	return func(record *schema_pb.RecordValue) bool {
		fieldValue, exists := record.Fields[columnName]
		if !exists {
			return false
		}
		return e.evaluateComparison(fieldValue, operator, compareValue)
	}, nil
}

// buildComparisonPredicate creates a predicate for comparison operations (=, <, >, etc.)
// Handles column names on both left and right sides of the comparison
func (e *SQLEngine) buildComparisonPredicate(expr *ComparisonExpr) (func(*schema_pb.RecordValue) bool, error) {
	return e.buildComparisonPredicateWithContext(expr, nil)
}

// buildComparisonPredicateWithContext creates a predicate for comparison operations with alias support
func (e *SQLEngine) buildComparisonPredicateWithContext(expr *ComparisonExpr, selectExprs []SelectExpr) (func(*schema_pb.RecordValue) bool, error) {
	var columnName string
	var compareValue interface{}
	var operator string

	// Check if column is on the left side (normal case: column > value)
	if colName, ok := expr.Left.(*ColName); ok {
		rawColumnName := colName.Name.String()
		// Resolve potential alias to actual column name
		columnName = e.resolveColumnAlias(rawColumnName, selectExprs)
		operator = expr.Operator

		// Extract comparison value from right side
		val, err := e.extractComparisonValue(expr.Right)
		if err != nil {
			return nil, fmt.Errorf("failed to extract right-side value: %v", err)
		}
		compareValue = val

	} else if colName, ok := expr.Right.(*ColName); ok {
		// Column is on the right side (reversed case: value < column)
		rawColumnName := colName.Name.String()
		// Resolve potential alias to actual column name
		columnName = e.resolveColumnAlias(rawColumnName, selectExprs)

		// Reverse the operator when column is on right side
		operator = e.reverseOperator(expr.Operator)

		// Extract comparison value from left side
		val, err := e.extractComparisonValue(expr.Left)
		if err != nil {
			return nil, fmt.Errorf("failed to extract left-side value: %v", err)
		}
		compareValue = val

	} else {
		// Handle literal-only comparisons like 1 = 0, 'a' = 'b', etc.
		leftVal, leftErr := e.extractComparisonValue(expr.Left)
		rightVal, rightErr := e.extractComparisonValue(expr.Right)

		if leftErr != nil || rightErr != nil {
			return nil, fmt.Errorf("no column name found in comparison expression, left: %T, right: %T", expr.Left, expr.Right)
		}

		// Evaluate the literal comparison once
		result := e.compareLiteralValues(leftVal, rightVal, expr.Operator)

		// Return a constant predicate
		return func(record *schema_pb.RecordValue) bool {
			return result
		}, nil
	}

	// Return the predicate function
	return func(record *schema_pb.RecordValue) bool {
		fieldValue, exists := record.Fields[columnName]
		if !exists {
			return false // Column doesn't exist in record
		}

		// Use the comparison evaluation function
		return e.evaluateComparison(fieldValue, operator, compareValue)
	}, nil
}

// getColumnNameWithAliases extracts column name from expression, resolving aliases if needed
func (e *SQLEngine) getColumnNameWithAliases(expr ExprNode, aliases map[string]ExprNode) string {
	switch exprType := expr.(type) {
	case *ColName:
		colName := exprType.Name.String()
		// Check if this is an alias that should be resolved
		if aliases != nil {
			if actualExpr, exists := aliases[colName]; exists {
				// Recursively resolve the aliased expression
				return e.getColumnNameWithAliases(actualExpr, nil) // Don't recurse aliases
			}
		}
		return colName
	}
	return ""
}

// extractValueFromExpr extracts a value from an expression node (for alias support)
func (e *SQLEngine) extractValueFromExpr(expr ExprNode) (interface{}, error) {
	return e.extractComparisonValue(expr)
}

// normalizeOperator normalizes comparison operators
func (e *SQLEngine) normalizeOperator(op string) string {
	return op // For now, just return as-is
}

// extractSelectAliases builds a map of aliases to their underlying expressions
func (e *SQLEngine) extractSelectAliases(selectExprs []SelectExpr) map[string]ExprNode {
	aliases := make(map[string]ExprNode)

	if selectExprs == nil {
		return aliases
	}

	for _, selectExpr := range selectExprs {
		if selectExpr == nil {
			continue
		}
		if aliasedExpr, ok := selectExpr.(*AliasedExpr); ok && aliasedExpr != nil {
			// Additional safety checks
			if aliasedExpr.As != nil && !aliasedExpr.As.IsEmpty() && aliasedExpr.Expr != nil {
				// Map the alias name to the underlying expression
				aliases[aliasedExpr.As.String()] = aliasedExpr.Expr
			}
		}
	}

	return aliases
}

// extractComparisonValue extracts the comparison value from a SQL expression
func (e *SQLEngine) extractComparisonValue(expr ExprNode) (interface{}, error) {
	switch val := expr.(type) {
	case *SQLVal:
		switch val.Type {
		case IntVal:
			intVal, err := strconv.ParseInt(string(val.Val), 10, 64)
			if err != nil {
				return nil, err
			}
			return intVal, nil
		case StrVal:
			return string(val.Val), nil
		case FloatVal:
			floatVal, err := strconv.ParseFloat(string(val.Val), 64)
			if err != nil {
				return nil, err
			}
			return floatVal, nil
		default:
			return nil, fmt.Errorf("unsupported SQL value type: %v", val.Type)
		}
	case ValTuple:
		// Handle IN expressions with multiple values: column IN (value1, value2, value3)
		var inValues []interface{}
		for _, tupleVal := range val {
			switch v := tupleVal.(type) {
			case *SQLVal:
				switch v.Type {
				case IntVal:
					intVal, err := strconv.ParseInt(string(v.Val), 10, 64)
					if err != nil {
						return nil, err
					}
					inValues = append(inValues, intVal)
				case StrVal:
					inValues = append(inValues, string(v.Val))
				case FloatVal:
					floatVal, err := strconv.ParseFloat(string(v.Val), 64)
					if err != nil {
						return nil, err
					}
					inValues = append(inValues, floatVal)
				}
			}
		}
		return inValues, nil
	default:
		return nil, fmt.Errorf("unsupported comparison value type: %T", expr)
	}
}

// evaluateComparison performs the actual comparison
func (e *SQLEngine) evaluateComparison(fieldValue *schema_pb.Value, operator string, compareValue interface{}) bool {
	// This is a simplified implementation
	// A full implementation would handle type coercion and all comparison operators

	switch operator {
	case "=":
		return e.valuesEqual(fieldValue, compareValue)
	case "<":
		return e.valueLessThan(fieldValue, compareValue)
	case ">":
		return e.valueGreaterThan(fieldValue, compareValue)
	case "<=":
		return e.valuesEqual(fieldValue, compareValue) || e.valueLessThan(fieldValue, compareValue)
	case ">=":
		return e.valuesEqual(fieldValue, compareValue) || e.valueGreaterThan(fieldValue, compareValue)
	case "!=", "<>":
		return !e.valuesEqual(fieldValue, compareValue)
	case "LIKE", "like":
		return e.valueLike(fieldValue, compareValue)
	case "IN", "in":
		return e.valueIn(fieldValue, compareValue)
	default:
		return false
	}
}

// Helper functions for value comparison with proper type coercion
func (e *SQLEngine) valuesEqual(fieldValue *schema_pb.Value, compareValue interface{}) bool {
	// Handle string comparisons first
	if strField, ok := fieldValue.Kind.(*schema_pb.Value_StringValue); ok {
		if strVal, ok := compareValue.(string); ok {
			return strField.StringValue == strVal
		}
		return false
	}

	// Handle boolean comparisons
	if boolField, ok := fieldValue.Kind.(*schema_pb.Value_BoolValue); ok {
		if boolVal, ok := compareValue.(bool); ok {
			return boolField.BoolValue == boolVal
		}
		return false
	}

	// Handle logical type comparisons
	if timestampField, ok := fieldValue.Kind.(*schema_pb.Value_TimestampValue); ok {
		if timestampVal, ok := compareValue.(int64); ok {
			return timestampField.TimestampValue.TimestampMicros == timestampVal
		}
		return false
	}

	if dateField, ok := fieldValue.Kind.(*schema_pb.Value_DateValue); ok {
		if dateVal, ok := compareValue.(int32); ok {
			return dateField.DateValue.DaysSinceEpoch == dateVal
		}
		return false
	}

	// Handle DecimalValue comparison (convert to string for comparison)
	if decimalField, ok := fieldValue.Kind.(*schema_pb.Value_DecimalValue); ok {
		if decimalStr, ok := compareValue.(string); ok {
			// Convert decimal bytes back to string for comparison
			decimalValue := e.decimalToString(decimalField.DecimalValue)
			return decimalValue == decimalStr
		}
		return false
	}

	if timeField, ok := fieldValue.Kind.(*schema_pb.Value_TimeValue); ok {
		if timeVal, ok := compareValue.(int64); ok {
			return timeField.TimeValue.TimeMicros == timeVal
		}
		return false
	}

	// Handle direct int64 comparisons for timestamp precision (before float64 conversion)
	if int64Field, ok := fieldValue.Kind.(*schema_pb.Value_Int64Value); ok {
		if int64Val, ok := compareValue.(int64); ok {
			result := int64Field.Int64Value == int64Val
			// Removed debug logging
			return result
		}
		if intVal, ok := compareValue.(int); ok {
			return int64Field.Int64Value == int64(intVal)
		}
	}

	// Handle direct int32 comparisons
	if int32Field, ok := fieldValue.Kind.(*schema_pb.Value_Int32Value); ok {
		if int32Val, ok := compareValue.(int32); ok {
			return int32Field.Int32Value == int32Val
		}
		if intVal, ok := compareValue.(int); ok {
			return int32Field.Int32Value == int32(intVal)
		}
		if int64Val, ok := compareValue.(int64); ok && int64Val >= math.MinInt32 && int64Val <= math.MaxInt32 {
			return int32Field.Int32Value == int32(int64Val)
		}
	}

	// Handle numeric comparisons with type coercion (fallback for other numeric types)
	fieldNum := e.convertToNumber(fieldValue)
	compareNum := e.convertCompareValueToNumber(compareValue)

	if fieldNum != nil && compareNum != nil {
		return *fieldNum == *compareNum
	}

	return false
}

// convertCompareValueToNumber converts compare values from SQL queries to float64
func (e *SQLEngine) convertCompareValueToNumber(compareValue interface{}) *float64 {
	switch v := compareValue.(type) {
	case int:
		result := float64(v)
		return &result
	case int32:
		result := float64(v)
		return &result
	case int64:
		result := float64(v)
		return &result
	case float32:
		result := float64(v)
		return &result
	case float64:
		return &v
	case string:
		// Try to parse string as number for flexible comparisons
		if parsed, err := strconv.ParseFloat(v, 64); err == nil {
			return &parsed
		}
	}
	return nil
}

// decimalToString converts a DecimalValue back to string representation
func (e *SQLEngine) decimalToString(decimalValue *schema_pb.DecimalValue) string {
	if decimalValue == nil || decimalValue.Value == nil {
		return "0"
	}

	// Convert bytes back to big.Int
	intValue := new(big.Int).SetBytes(decimalValue.Value)

	// Convert to string with proper decimal placement
	str := intValue.String()

	// Handle decimal placement based on scale
	scale := int(decimalValue.Scale)
	if scale > 0 && len(str) > scale {
		// Insert decimal point
		decimalPos := len(str) - scale
		return str[:decimalPos] + "." + str[decimalPos:]
	}

	return str
}

func (e *SQLEngine) valueLessThan(fieldValue *schema_pb.Value, compareValue interface{}) bool {
	// Handle string comparisons lexicographically
	if strField, ok := fieldValue.Kind.(*schema_pb.Value_StringValue); ok {
		if strVal, ok := compareValue.(string); ok {
			return strField.StringValue < strVal
		}
		return false
	}

	// Handle logical type comparisons
	if timestampField, ok := fieldValue.Kind.(*schema_pb.Value_TimestampValue); ok {
		if timestampVal, ok := compareValue.(int64); ok {
			return timestampField.TimestampValue.TimestampMicros < timestampVal
		}
		return false
	}

	if dateField, ok := fieldValue.Kind.(*schema_pb.Value_DateValue); ok {
		if dateVal, ok := compareValue.(int32); ok {
			return dateField.DateValue.DaysSinceEpoch < dateVal
		}
		return false
	}

	if timeField, ok := fieldValue.Kind.(*schema_pb.Value_TimeValue); ok {
		if timeVal, ok := compareValue.(int64); ok {
			return timeField.TimeValue.TimeMicros < timeVal
		}
		return false
	}

	// Handle direct int64 comparisons for timestamp precision (before float64 conversion)
	if int64Field, ok := fieldValue.Kind.(*schema_pb.Value_Int64Value); ok {
		if int64Val, ok := compareValue.(int64); ok {
			return int64Field.Int64Value < int64Val
		}
		if intVal, ok := compareValue.(int); ok {
			return int64Field.Int64Value < int64(intVal)
		}
	}

	// Handle direct int32 comparisons
	if int32Field, ok := fieldValue.Kind.(*schema_pb.Value_Int32Value); ok {
		if int32Val, ok := compareValue.(int32); ok {
			return int32Field.Int32Value < int32Val
		}
		if intVal, ok := compareValue.(int); ok {
			return int32Field.Int32Value < int32(intVal)
		}
		if int64Val, ok := compareValue.(int64); ok && int64Val >= math.MinInt32 && int64Val <= math.MaxInt32 {
			return int32Field.Int32Value < int32(int64Val)
		}
	}

	// Handle numeric comparisons with type coercion (fallback for other numeric types)
	fieldNum := e.convertToNumber(fieldValue)
	compareNum := e.convertCompareValueToNumber(compareValue)

	if fieldNum != nil && compareNum != nil {
		return *fieldNum < *compareNum
	}

	return false
}

func (e *SQLEngine) valueGreaterThan(fieldValue *schema_pb.Value, compareValue interface{}) bool {
	// Handle string comparisons lexicographically
	if strField, ok := fieldValue.Kind.(*schema_pb.Value_StringValue); ok {
		if strVal, ok := compareValue.(string); ok {
			return strField.StringValue > strVal
		}
		return false
	}

	// Handle logical type comparisons
	if timestampField, ok := fieldValue.Kind.(*schema_pb.Value_TimestampValue); ok {
		if timestampVal, ok := compareValue.(int64); ok {
			return timestampField.TimestampValue.TimestampMicros > timestampVal
		}
		return false
	}

	if dateField, ok := fieldValue.Kind.(*schema_pb.Value_DateValue); ok {
		if dateVal, ok := compareValue.(int32); ok {
			return dateField.DateValue.DaysSinceEpoch > dateVal
		}
		return false
	}

	if timeField, ok := fieldValue.Kind.(*schema_pb.Value_TimeValue); ok {
		if timeVal, ok := compareValue.(int64); ok {
			return timeField.TimeValue.TimeMicros > timeVal
		}
		return false
	}

	// Handle direct int64 comparisons for timestamp precision (before float64 conversion)
	if int64Field, ok := fieldValue.Kind.(*schema_pb.Value_Int64Value); ok {
		if int64Val, ok := compareValue.(int64); ok {
			return int64Field.Int64Value > int64Val
		}
		if intVal, ok := compareValue.(int); ok {
			return int64Field.Int64Value > int64(intVal)
		}
	}

	// Handle direct int32 comparisons
	if int32Field, ok := fieldValue.Kind.(*schema_pb.Value_Int32Value); ok {
		if int32Val, ok := compareValue.(int32); ok {
			return int32Field.Int32Value > int32Val
		}
		if intVal, ok := compareValue.(int); ok {
			return int32Field.Int32Value > int32(intVal)
		}
		if int64Val, ok := compareValue.(int64); ok && int64Val >= math.MinInt32 && int64Val <= math.MaxInt32 {
			return int32Field.Int32Value > int32(int64Val)
		}
	}

	// Handle numeric comparisons with type coercion (fallback for other numeric types)
	fieldNum := e.convertToNumber(fieldValue)
	compareNum := e.convertCompareValueToNumber(compareValue)

	if fieldNum != nil && compareNum != nil {
		return *fieldNum > *compareNum
	}

	return false
}

// valueLike implements SQL LIKE pattern matching with % and _ wildcards
func (e *SQLEngine) valueLike(fieldValue *schema_pb.Value, compareValue interface{}) bool {
	// Only support LIKE for string values
	stringVal, ok := fieldValue.Kind.(*schema_pb.Value_StringValue)
	if !ok {
		return false
	}

	pattern, ok := compareValue.(string)
	if !ok {
		return false
	}

	// Convert SQL LIKE pattern to Go regex pattern
	// % matches any sequence of characters (.*), _ matches single character (.)
	regexPattern := strings.ReplaceAll(pattern, "%", ".*")
	regexPattern = strings.ReplaceAll(regexPattern, "_", ".")
	regexPattern = "^" + regexPattern + "$" // Anchor to match entire string

	// Compile and match regex
	regex, err := regexp.Compile(regexPattern)
	if err != nil {
		return false // Invalid pattern
	}

	return regex.MatchString(stringVal.StringValue)
}

// valueIn implements SQL IN operator for checking if value exists in a list
func (e *SQLEngine) valueIn(fieldValue *schema_pb.Value, compareValue interface{}) bool {
	// For now, handle simple case where compareValue is a slice of values
	// In a full implementation, this would handle SQL IN expressions properly
	values, ok := compareValue.([]interface{})
	if !ok {
		return false
	}

	// Check if fieldValue matches any value in the list
	for _, value := range values {
		if e.valuesEqual(fieldValue, value) {
			return true
		}
	}

	return false
}

// Helper methods for specific operations

func (e *SQLEngine) showDatabases(ctx context.Context) (*QueryResult, error) {
	databases := e.catalog.ListDatabases()

	result := &QueryResult{
		Columns: []string{"Database"},
		Rows:    make([][]sqltypes.Value, len(databases)),
	}

	for i, db := range databases {
		result.Rows[i] = []sqltypes.Value{
			sqltypes.NewVarChar(db),
		}
	}

	return result, nil
}

func (e *SQLEngine) showTables(ctx context.Context, dbName string) (*QueryResult, error) {
	// Use current database context if no database specified
	if dbName == "" {
		dbName = e.catalog.GetCurrentDatabase()
		if dbName == "" {
			dbName = "default"
		}
	}

	tables, err := e.catalog.ListTables(dbName)
	if err != nil {
		return &QueryResult{Error: err}, err
	}

	result := &QueryResult{
		Columns: []string{"Tables_in_" + dbName},
		Rows:    make([][]sqltypes.Value, len(tables)),
	}

	for i, table := range tables {
		result.Rows[i] = []sqltypes.Value{
			sqltypes.NewVarChar(table),
		}
	}

	return result, nil
}

// compareLiteralValues compares two literal values with the given operator
func (e *SQLEngine) compareLiteralValues(left, right interface{}, operator string) bool {
	switch operator {
	case "=", "==":
		return e.literalValuesEqual(left, right)
	case "!=", "<>":
		return !e.literalValuesEqual(left, right)
	case "<":
		return e.compareLiteralNumber(left, right) < 0
	case "<=":
		return e.compareLiteralNumber(left, right) <= 0
	case ">":
		return e.compareLiteralNumber(left, right) > 0
	case ">=":
		return e.compareLiteralNumber(left, right) >= 0
	default:
		// For unsupported operators, default to false
		return false
	}
}

// literalValuesEqual checks if two literal values are equal
func (e *SQLEngine) literalValuesEqual(left, right interface{}) bool {
	// Convert both to strings for comparison
	leftStr := fmt.Sprintf("%v", left)
	rightStr := fmt.Sprintf("%v", right)
	return leftStr == rightStr
}

// compareLiteralNumber compares two values as numbers
func (e *SQLEngine) compareLiteralNumber(left, right interface{}) int {
	leftNum, leftOk := e.convertToFloat64(left)
	rightNum, rightOk := e.convertToFloat64(right)

	if !leftOk || !rightOk {
		// Fall back to string comparison if not numeric
		leftStr := fmt.Sprintf("%v", left)
		rightStr := fmt.Sprintf("%v", right)
		if leftStr < rightStr {
			return -1
		} else if leftStr > rightStr {
			return 1
		} else {
			return 0
		}
	}

	if leftNum < rightNum {
		return -1
	} else if leftNum > rightNum {
		return 1
	} else {
		return 0
	}
}

// convertToFloat64 attempts to convert a value to float64
func (e *SQLEngine) convertToFloat64(value interface{}) (float64, bool) {
	switch v := value.(type) {
	case int64:
		return float64(v), true
	case int32:
		return float64(v), true
	case int:
		return float64(v), true
	case float64:
		return v, true
	case float32:
		return float64(v), true
	case string:
		if num, err := strconv.ParseFloat(v, 64); err == nil {
			return num, true
		}
		return 0, false
	default:
		return 0, false
	}
}

func (e *SQLEngine) createTable(ctx context.Context, stmt *DDLStatement) (*QueryResult, error) {
	// Parse CREATE TABLE statement
	// Assumption: Table name format is [database.]table_name
	tableName := stmt.NewName.Name.String()
	database := ""

	// Check if database is specified in table name
	if stmt.NewName.Qualifier.String() != "" {
		database = stmt.NewName.Qualifier.String()
	} else {
		// Use current database context or default
		database = e.catalog.GetCurrentDatabase()
		if database == "" {
			database = "default"
		}
	}

	// Parse column definitions from CREATE TABLE
	// Assumption: stmt.TableSpec contains column definitions
	if stmt.TableSpec == nil || len(stmt.TableSpec.Columns) == 0 {
		err := fmt.Errorf("CREATE TABLE requires column definitions")
		return &QueryResult{Error: err}, err
	}

	// Convert SQL columns to MQ schema fields
	fields := make([]*schema_pb.Field, len(stmt.TableSpec.Columns))
	for i, col := range stmt.TableSpec.Columns {
		fieldType, err := e.convertSQLTypeToMQ(col.Type)
		if err != nil {
			return &QueryResult{Error: err}, err
		}

		fields[i] = &schema_pb.Field{
			Name: col.Name.String(),
			Type: fieldType,
		}
	}

	// Create record type for the topic
	recordType := &schema_pb.RecordType{
		Fields: fields,
	}

	// Create the topic via broker using configurable partition count
	partitionCount := e.catalog.GetDefaultPartitionCount()
	err := e.catalog.brokerClient.ConfigureTopic(ctx, database, tableName, partitionCount, recordType)
	if err != nil {
		return &QueryResult{Error: err}, err
	}

	// Register the new topic in catalog
	mqSchema := &schema.Schema{
		Namespace:  database,
		Name:       tableName,
		RecordType: recordType,
		RevisionId: 1, // Initial revision
	}

	err = e.catalog.RegisterTopic(database, tableName, mqSchema)
	if err != nil {
		return &QueryResult{Error: err}, err
	}

	// Return success result
	result := &QueryResult{
		Columns: []string{"Result"},
		Rows: [][]sqltypes.Value{
			{sqltypes.NewVarChar(fmt.Sprintf("Table '%s.%s' created successfully", database, tableName))},
		},
	}

	return result, nil
}

// ExecutionPlanBuilder handles building execution plans for queries
type ExecutionPlanBuilder struct {
	engine *SQLEngine
}

// NewExecutionPlanBuilder creates a new execution plan builder
func NewExecutionPlanBuilder(engine *SQLEngine) *ExecutionPlanBuilder {
	return &ExecutionPlanBuilder{engine: engine}
}

// BuildAggregationPlan builds an execution plan for aggregation queries
func (builder *ExecutionPlanBuilder) BuildAggregationPlan(
	stmt *SelectStatement,
	aggregations []AggregationSpec,
	strategy AggregationStrategy,
	dataSources *TopicDataSources,
) *QueryExecutionPlan {

	plan := &QueryExecutionPlan{
		QueryType:           "SELECT",
		ExecutionStrategy:   builder.determineExecutionStrategy(stmt, strategy),
		DataSources:         builder.buildDataSourcesList(strategy, dataSources),
		PartitionsScanned:   dataSources.PartitionsCount,
		ParquetFilesScanned: builder.countParquetFiles(dataSources),
		LiveLogFilesScanned: builder.countLiveLogFiles(dataSources),
		OptimizationsUsed:   builder.buildOptimizationsList(stmt, strategy),
		Aggregations:        builder.buildAggregationsList(aggregations),
		Details:             make(map[string]interface{}),
	}

	// Set row counts based on strategy
	if strategy.CanUseFastPath {
		plan.TotalRowsProcessed = dataSources.LiveLogRowCount // Only live logs are scanned, parquet uses metadata
		plan.Details["scan_method"] = "Parquet Metadata Only"
	} else {
		plan.TotalRowsProcessed = dataSources.ParquetRowCount + dataSources.LiveLogRowCount
		plan.Details["scan_method"] = "Full Data Scan"
	}

	return plan
}

// determineExecutionStrategy determines the execution strategy based on query characteristics
func (builder *ExecutionPlanBuilder) determineExecutionStrategy(stmt *SelectStatement, strategy AggregationStrategy) string {
	if stmt.Where != nil {
		return "full_scan"
	}

	if strategy.CanUseFastPath {
		return "hybrid_fast_path"
	}

	return "full_scan"
}

// buildDataSourcesList builds the list of data sources used
func (builder *ExecutionPlanBuilder) buildDataSourcesList(strategy AggregationStrategy, dataSources *TopicDataSources) []string {
	sources := []string{}

	if strategy.CanUseFastPath {
		sources = append(sources, "parquet_stats")
		if dataSources.LiveLogRowCount > 0 {
			sources = append(sources, "live_logs")
		}
	} else {
		sources = append(sources, "live_logs", "parquet_files")
	}

	// Note: broker_buffer is added dynamically during execution when broker is queried
	// See aggregations.go lines 397-409 for the broker buffer data source addition logic

	return sources
}

// countParquetFiles counts the total number of parquet files across all partitions
func (builder *ExecutionPlanBuilder) countParquetFiles(dataSources *TopicDataSources) int {
	count := 0
	for _, fileStats := range dataSources.ParquetFiles {
		count += len(fileStats)
	}
	return count
}

// countLiveLogFiles returns the total number of live log files across all partitions
func (builder *ExecutionPlanBuilder) countLiveLogFiles(dataSources *TopicDataSources) int {
	return dataSources.LiveLogFilesCount
}

// buildOptimizationsList builds the list of optimizations used
func (builder *ExecutionPlanBuilder) buildOptimizationsList(stmt *SelectStatement, strategy AggregationStrategy) []string {
	optimizations := []string{}

	if strategy.CanUseFastPath {
		optimizations = append(optimizations, "parquet_statistics", "live_log_counting", "deduplication")
	}

	if stmt.Where != nil {
		// Check if "predicate_pushdown" is already in the list
		found := false
		for _, opt := range optimizations {
			if opt == "predicate_pushdown" {
				found = true
				break
			}
		}
		if !found {
			optimizations = append(optimizations, "predicate_pushdown")
		}
	}

	return optimizations
}

// buildAggregationsList builds the list of aggregations for display
func (builder *ExecutionPlanBuilder) buildAggregationsList(aggregations []AggregationSpec) []string {
	aggList := make([]string, len(aggregations))
	for i, spec := range aggregations {
		aggList[i] = fmt.Sprintf("%s(%s)", spec.Function, spec.Column)
	}
	return aggList
}

// parseAggregationFunction parses an aggregation function expression
func (e *SQLEngine) parseAggregationFunction(funcExpr *FuncExpr, aliasExpr *AliasedExpr) (*AggregationSpec, error) {
	funcName := strings.ToUpper(funcExpr.Name.String())

	spec := &AggregationSpec{
		Function: funcName,
	}

	// Parse function arguments
	switch funcName {
	case FuncCOUNT:
		if len(funcExpr.Exprs) != 1 {
			return nil, fmt.Errorf("COUNT function expects exactly 1 argument")
		}

		switch arg := funcExpr.Exprs[0].(type) {
		case *StarExpr:
			spec.Column = "*"
			spec.Alias = "COUNT(*)"
		case *AliasedExpr:
			if colName, ok := arg.Expr.(*ColName); ok {
				spec.Column = colName.Name.String()
				spec.Alias = fmt.Sprintf("COUNT(%s)", spec.Column)
			} else {
				return nil, fmt.Errorf("COUNT argument must be a column name or *")
			}
		default:
			return nil, fmt.Errorf("unsupported COUNT argument: %T", arg)
		}

	case FuncSUM, FuncAVG, FuncMIN, FuncMAX:
		if len(funcExpr.Exprs) != 1 {
			return nil, fmt.Errorf("%s function expects exactly 1 argument", funcName)
		}

		switch arg := funcExpr.Exprs[0].(type) {
		case *AliasedExpr:
			if colName, ok := arg.Expr.(*ColName); ok {
				spec.Column = colName.Name.String()
				spec.Alias = fmt.Sprintf("%s(%s)", funcName, spec.Column)
			} else {
				return nil, fmt.Errorf("%s argument must be a column name", funcName)
			}
		default:
			return nil, fmt.Errorf("unsupported %s argument: %T", funcName, arg)
		}

	default:
		return nil, fmt.Errorf("unsupported aggregation function: %s", funcName)
	}

	// Override with user-specified alias if provided
	if aliasExpr != nil && aliasExpr.As != nil && !aliasExpr.As.IsEmpty() {
		spec.Alias = aliasExpr.As.String()
	}

	return spec, nil
}

// computeLiveLogMinMax scans live log files to find MIN/MAX values for a specific column
func (e *SQLEngine) computeLiveLogMinMax(partitionPath string, columnName string, parquetSourceFiles map[string]bool) (interface{}, interface{}, error) {
	if e.catalog.brokerClient == nil {
		return nil, nil, fmt.Errorf("no broker client available")
	}

	filerClient, err := e.catalog.brokerClient.GetFilerClient()
	if err != nil {
		return nil, nil, fmt.Errorf("failed to get filer client: %v", err)
	}

	var minValue, maxValue interface{}
	var minSchemaValue, maxSchemaValue *schema_pb.Value

	// Process each live log file
	err = filer_pb.ReadDirAllEntries(context.Background(), filerClient, util.FullPath(partitionPath), "", func(entry *filer_pb.Entry, isLast bool) error {
		// Skip parquet files and directories
		if entry.IsDirectory || strings.HasSuffix(entry.Name, ".parquet") {
			return nil
		}
		// Skip files that have been converted to parquet (deduplication)
		if parquetSourceFiles[entry.Name] {
			return nil
		}

		filePath := partitionPath + "/" + entry.Name

		// Scan this log file for MIN/MAX values
		fileMin, fileMax, err := e.computeFileMinMax(filerClient, filePath, columnName)
		if err != nil {
			fmt.Printf("Warning: failed to compute min/max for file %s: %v\n", filePath, err)
			return nil // Continue with other files
		}

		// Update global min/max
		if fileMin != nil {
			if minSchemaValue == nil || e.compareValues(fileMin, minSchemaValue) < 0 {
				minSchemaValue = fileMin
				minValue = e.extractRawValue(fileMin)
			}
		}

		if fileMax != nil {
			if maxSchemaValue == nil || e.compareValues(fileMax, maxSchemaValue) > 0 {
				maxSchemaValue = fileMax
				maxValue = e.extractRawValue(fileMax)
			}
		}

		return nil
	})

	if err != nil {
		return nil, nil, fmt.Errorf("failed to process partition directory %s: %v", partitionPath, err)
	}

	return minValue, maxValue, nil
}

// computeFileMinMax scans a single log file to find MIN/MAX values for a specific column
func (e *SQLEngine) computeFileMinMax(filerClient filer_pb.FilerClient, filePath string, columnName string) (*schema_pb.Value, *schema_pb.Value, error) {
	var minValue, maxValue *schema_pb.Value

	err := e.eachLogEntryInFile(filerClient, filePath, func(logEntry *filer_pb.LogEntry) error {
		// Convert log entry to record value
		recordValue, _, err := e.convertLogEntryToRecordValue(logEntry)
		if err != nil {
			return err // This will stop processing this file but not fail the overall query
		}

		// Extract the requested column value
		var columnValue *schema_pb.Value
		if e.isSystemColumn(columnName) {
			// Handle system columns
			switch strings.ToLower(columnName) {
			case SW_COLUMN_NAME_TIMESTAMP:
				columnValue = &schema_pb.Value{Kind: &schema_pb.Value_Int64Value{Int64Value: logEntry.TsNs}}
			case SW_COLUMN_NAME_KEY:
				columnValue = &schema_pb.Value{Kind: &schema_pb.Value_BytesValue{BytesValue: logEntry.Key}}
			case SW_COLUMN_NAME_SOURCE:
				columnValue = &schema_pb.Value{Kind: &schema_pb.Value_StringValue{StringValue: "live_log"}}
			}
		} else {
			// Handle regular data columns
			if value, exists := recordValue.Fields[columnName]; exists {
				columnValue = value
			}
		}

		if columnValue == nil {
			return nil // Skip this record
		}

		// Update min/max
		if minValue == nil || e.compareValues(columnValue, minValue) < 0 {
			minValue = columnValue
		}
		if maxValue == nil || e.compareValues(columnValue, maxValue) > 0 {
			maxValue = columnValue
		}

		return nil
	})

	return minValue, maxValue, err
}

// eachLogEntryInFile reads a log file and calls the provided function for each log entry
func (e *SQLEngine) eachLogEntryInFile(filerClient filer_pb.FilerClient, filePath string, fn func(*filer_pb.LogEntry) error) error {
	// Extract directory and filename
	// filePath is like "partitionPath/filename"
	lastSlash := strings.LastIndex(filePath, "/")
	if lastSlash == -1 {
		return fmt.Errorf("invalid file path: %s", filePath)
	}

	dirPath := filePath[:lastSlash]
	fileName := filePath[lastSlash+1:]

	// Get file entry
	var fileEntry *filer_pb.Entry
	err := filer_pb.ReadDirAllEntries(context.Background(), filerClient, util.FullPath(dirPath), "", func(entry *filer_pb.Entry, isLast bool) error {
		if entry.Name == fileName {
			fileEntry = entry
		}
		return nil
	})

	if err != nil {
		return fmt.Errorf("failed to find file %s: %v", filePath, err)
	}

	if fileEntry == nil {
		return fmt.Errorf("file not found: %s", filePath)
	}

	lookupFileIdFn := filer.LookupFn(filerClient)

	// eachChunkFn processes each chunk's data (pattern from countRowsInLogFile)
	eachChunkFn := func(buf []byte) error {
		for pos := 0; pos+4 < len(buf); {
			size := util.BytesToUint32(buf[pos : pos+4])
			if pos+4+int(size) > len(buf) {
				break
			}

			entryData := buf[pos+4 : pos+4+int(size)]

			logEntry := &filer_pb.LogEntry{}
			if err := proto.Unmarshal(entryData, logEntry); err != nil {
				pos += 4 + int(size)
				continue // Skip corrupted entries
			}

			// Call the provided function for each log entry
			if err := fn(logEntry); err != nil {
				return err
			}

			pos += 4 + int(size)
		}
		return nil
	}

	// Read file chunks and process them (pattern from countRowsInLogFile)
	fileSize := filer.FileSize(fileEntry)
	visibleIntervals, _ := filer.NonOverlappingVisibleIntervals(context.Background(), lookupFileIdFn, fileEntry.Chunks, 0, int64(fileSize))
	chunkViews := filer.ViewFromVisibleIntervals(visibleIntervals, 0, int64(fileSize))

	for x := chunkViews.Front(); x != nil; x = x.Next {
		chunk := x.Value
		urlStrings, err := lookupFileIdFn(context.Background(), chunk.FileId)
		if err != nil {
			fmt.Printf("Warning: failed to lookup chunk %s: %v\n", chunk.FileId, err)
			continue
		}

		if len(urlStrings) == 0 {
			continue
		}

		// Read chunk data
		// urlStrings[0] is already a complete URL (http://server:port/fileId)
		data, _, err := util_http.Get(urlStrings[0])
		if err != nil {
			fmt.Printf("Warning: failed to read chunk %s from %s: %v\n", chunk.FileId, urlStrings[0], err)
			continue
		}

		// Process this chunk
		if err := eachChunkFn(data); err != nil {
			return err
		}
	}

	return nil
}

// convertLogEntryToRecordValue helper method (reuse existing logic)
func (e *SQLEngine) convertLogEntryToRecordValue(logEntry *filer_pb.LogEntry) (*schema_pb.RecordValue, string, error) {
	// Parse the log entry data as Protocol Buffer (not JSON!)
	recordValue := &schema_pb.RecordValue{}
	if err := proto.Unmarshal(logEntry.Data, recordValue); err != nil {
		return nil, "", fmt.Errorf("failed to unmarshal log entry protobuf: %v", err)
	}

	// Ensure Fields map exists
	if recordValue.Fields == nil {
		recordValue.Fields = make(map[string]*schema_pb.Value)
	}

	// Add system columns
	recordValue.Fields[SW_COLUMN_NAME_TIMESTAMP] = &schema_pb.Value{
		Kind: &schema_pb.Value_Int64Value{Int64Value: logEntry.TsNs},
	}
	recordValue.Fields[SW_COLUMN_NAME_KEY] = &schema_pb.Value{
		Kind: &schema_pb.Value_BytesValue{BytesValue: logEntry.Key},
	}

	// User data fields are already present in the protobuf-deserialized recordValue
	// No additional processing needed since proto.Unmarshal already populated the Fields map

	return recordValue, "live_log", nil
}

// extractTimestampFromFilename extracts timestamp from parquet filename
// Format: YYYY-MM-DD-HH-MM-SS.parquet
func (e *SQLEngine) extractTimestampFromFilename(filename string) int64 {
	// Remove .parquet extension
	filename = strings.TrimSuffix(filename, ".parquet")

	// Parse timestamp format: 2006-01-02-15-04-05
	t, err := time.Parse("2006-01-02-15-04-05", filename)
	if err != nil {
		return 0
	}

	return t.UnixNano()
}

// hasLiveLogFiles checks if there are any live log files (non-parquet files) in a partition
func (e *SQLEngine) hasLiveLogFiles(partitionPath string) (bool, error) {
	// Get FilerClient from BrokerClient
	filerClient, err := e.catalog.brokerClient.GetFilerClient()
	if err != nil {
		return false, err
	}

	hasLiveLogs := false

	// Read all files in the partition directory
	err = filer_pb.ReadDirAllEntries(context.Background(), filerClient, util.FullPath(partitionPath), "", func(entry *filer_pb.Entry, isLast bool) error {
		// Skip directories and parquet files
		if entry.IsDirectory || strings.HasSuffix(entry.Name, ".parquet") {
			return nil
		}

		// Found a non-parquet file (live log)
		hasLiveLogs = true
		return nil // Can continue or return early, doesn't matter for existence check
	})

	return hasLiveLogs, err
}

// countLiveLogRows counts the total number of rows in live log files (non-parquet files) in a partition
func (e *SQLEngine) countLiveLogRows(partitionPath string) (int64, error) {
	filerClient, err := e.catalog.brokerClient.GetFilerClient()
	if err != nil {
		return 0, err
	}

	totalRows := int64(0)
	err = filer_pb.ReadDirAllEntries(context.Background(), filerClient, util.FullPath(partitionPath), "", func(entry *filer_pb.Entry, isLast bool) error {
		if entry.IsDirectory || strings.HasSuffix(entry.Name, ".parquet") {
			return nil // Skip directories and parquet files
		}

		// Count rows in live log file
		rowCount, err := e.countRowsInLogFile(filerClient, partitionPath, entry)
		if err != nil {
			fmt.Printf("Warning: failed to count rows in %s/%s: %v\n", partitionPath, entry.Name, err)
			return nil // Continue with other files
		}
		totalRows += rowCount
		return nil
	})
	return totalRows, err
}

// extractParquetSourceFiles extracts source log file names from parquet file metadata for deduplication
func (e *SQLEngine) extractParquetSourceFiles(fileStats []*ParquetFileStats) map[string]bool {
	sourceFiles := make(map[string]bool)

	for _, fileStat := range fileStats {
		// Each ParquetFileStats should have a reference to the original file entry
		// but we need to get it through the hybrid scanner to access Extended metadata
		// This is a simplified approach - in practice we'd need to access the filer entry

		// For now, we'll use filename-based deduplication as a fallback
		// Extract timestamp from parquet filename (YYYY-MM-DD-HH-MM-SS.parquet)
		if strings.HasSuffix(fileStat.FileName, ".parquet") {
			timeStr := strings.TrimSuffix(fileStat.FileName, ".parquet")
			// Mark this timestamp range as covered by parquet
			sourceFiles[timeStr] = true
		}
	}

	return sourceFiles
}

// countLiveLogRowsExcludingParquetSources counts live log rows but excludes files that were converted to parquet and duplicate log buffer data
func (e *SQLEngine) countLiveLogRowsExcludingParquetSources(ctx context.Context, partitionPath string, parquetSourceFiles map[string]bool) (int64, error) {
	filerClient, err := e.catalog.brokerClient.GetFilerClient()
	if err != nil {
		return 0, err
	}

	// First, get the actual source files from parquet metadata
	actualSourceFiles, err := e.getParquetSourceFilesFromMetadata(partitionPath)
	if err != nil {
		// If we can't read parquet metadata, use filename-based fallback
		fmt.Printf("Warning: failed to read parquet metadata, using filename-based deduplication: %v\n", err)
		actualSourceFiles = parquetSourceFiles
	}

	// Second, get duplicate files from log buffer metadata
	logBufferDuplicates, err := e.buildLogBufferDeduplicationMap(ctx, partitionPath)
	if err != nil {
		if isDebugMode(ctx) {
			fmt.Printf("Warning: failed to build log buffer deduplication map: %v\n", err)
		}
		logBufferDuplicates = make(map[string]bool)
	}

	// Debug: Show deduplication status (only in explain mode)
	if isDebugMode(ctx) {
		if len(actualSourceFiles) > 0 {
			fmt.Printf("Excluding %d converted log files from %s\n", len(actualSourceFiles), partitionPath)
		}
		if len(logBufferDuplicates) > 0 {
			fmt.Printf("Excluding %d duplicate log buffer files from %s\n", len(logBufferDuplicates), partitionPath)
		}
	}

	totalRows := int64(0)
	err = filer_pb.ReadDirAllEntries(context.Background(), filerClient, util.FullPath(partitionPath), "", func(entry *filer_pb.Entry, isLast bool) error {
		if entry.IsDirectory || strings.HasSuffix(entry.Name, ".parquet") {
			return nil // Skip directories and parquet files
		}

		// Skip files that have been converted to parquet
		if actualSourceFiles[entry.Name] {
			if isDebugMode(ctx) {
				fmt.Printf("Skipping %s (already converted to parquet)\n", entry.Name)
			}
			return nil
		}

		// Skip files that are duplicated due to log buffer metadata
		if logBufferDuplicates[entry.Name] {
			if isDebugMode(ctx) {
				fmt.Printf("Skipping %s (duplicate log buffer data)\n", entry.Name)
			}
			return nil
		}

		// Count rows in live log file
		rowCount, err := e.countRowsInLogFile(filerClient, partitionPath, entry)
		if err != nil {
			fmt.Printf("Warning: failed to count rows in %s/%s: %v\n", partitionPath, entry.Name, err)
			return nil // Continue with other files
		}
		totalRows += rowCount
		return nil
	})
	return totalRows, err
}

// getParquetSourceFilesFromMetadata reads parquet file metadata to get actual source log files
func (e *SQLEngine) getParquetSourceFilesFromMetadata(partitionPath string) (map[string]bool, error) {
	filerClient, err := e.catalog.brokerClient.GetFilerClient()
	if err != nil {
		return nil, err
	}

	sourceFiles := make(map[string]bool)

	err = filer_pb.ReadDirAllEntries(context.Background(), filerClient, util.FullPath(partitionPath), "", func(entry *filer_pb.Entry, isLast bool) error {
		if entry.IsDirectory || !strings.HasSuffix(entry.Name, ".parquet") {
			return nil
		}

		// Read source files from Extended metadata
		if entry.Extended != nil && entry.Extended["sources"] != nil {
			var sources []string
			if err := json.Unmarshal(entry.Extended["sources"], &sources); err == nil {
				for _, source := range sources {
					sourceFiles[source] = true
				}
			}
		}

		return nil
	})

	return sourceFiles, err
}

// getLogBufferStartFromFile reads buffer start from file extended attributes
func (e *SQLEngine) getLogBufferStartFromFile(entry *filer_pb.Entry) (*LogBufferStart, error) {
	if entry.Extended == nil {
		return nil, nil
	}

	// Only support binary buffer_start format
	if startData, exists := entry.Extended["buffer_start"]; exists {
		if len(startData) == 8 {
			startIndex := int64(binary.BigEndian.Uint64(startData))
			if startIndex > 0 {
				return &LogBufferStart{StartIndex: startIndex}, nil
			}
		} else {
			return nil, fmt.Errorf("invalid buffer_start format: expected 8 bytes, got %d", len(startData))
		}
	}

	return nil, nil
}

// buildLogBufferDeduplicationMap creates a map to track duplicate files based on buffer ranges (ultra-efficient)
func (e *SQLEngine) buildLogBufferDeduplicationMap(ctx context.Context, partitionPath string) (map[string]bool, error) {
	if e.catalog.brokerClient == nil {
		return make(map[string]bool), nil
	}

	filerClient, err := e.catalog.brokerClient.GetFilerClient()
	if err != nil {
		return make(map[string]bool), nil // Don't fail the query, just skip deduplication
	}

	// Track buffer ranges instead of individual indexes (much more efficient)
	type BufferRange struct {
		start, end int64
	}

	processedRanges := make([]BufferRange, 0)
	duplicateFiles := make(map[string]bool)

	err = filer_pb.ReadDirAllEntries(context.Background(), filerClient, util.FullPath(partitionPath), "", func(entry *filer_pb.Entry, isLast bool) error {
		if entry.IsDirectory || strings.HasSuffix(entry.Name, ".parquet") {
			return nil // Skip directories and parquet files
		}

		// Get buffer start for this file (most efficient)
		bufferStart, err := e.getLogBufferStartFromFile(entry)
		if err != nil || bufferStart == nil {
			return nil // No buffer info, can't deduplicate
		}

		// Calculate range for this file: [start, start + chunkCount - 1]
		chunkCount := int64(len(entry.GetChunks()))
		if chunkCount == 0 {
			return nil // Empty file, skip
		}

		fileRange := BufferRange{
			start: bufferStart.StartIndex,
			end:   bufferStart.StartIndex + chunkCount - 1,
		}

		// Check if this range overlaps with any processed range
		isDuplicate := false
		for _, processedRange := range processedRanges {
			if fileRange.start <= processedRange.end && fileRange.end >= processedRange.start {
				// Ranges overlap - this file contains duplicate buffer indexes
				isDuplicate = true
				if isDebugMode(ctx) {
					fmt.Printf("Marking %s as duplicate (buffer range [%d-%d] overlaps with [%d-%d])\n",
						entry.Name, fileRange.start, fileRange.end, processedRange.start, processedRange.end)
				}
				break
			}
		}

		if isDuplicate {
			duplicateFiles[entry.Name] = true
		} else {
			// Add this range to processed ranges
			processedRanges = append(processedRanges, fileRange)
		}

		return nil
	})

	if err != nil {
		return make(map[string]bool), nil // Don't fail the query
	}

	return duplicateFiles, nil
}

// countRowsInLogFile counts rows in a single log file using SeaweedFS patterns
func (e *SQLEngine) countRowsInLogFile(filerClient filer_pb.FilerClient, partitionPath string, entry *filer_pb.Entry) (int64, error) {
	lookupFileIdFn := filer.LookupFn(filerClient)

	rowCount := int64(0)

	// eachChunkFn processes each chunk's data (pattern from read_log_from_disk.go)
	eachChunkFn := func(buf []byte) error {
		for pos := 0; pos+4 < len(buf); {
			size := util.BytesToUint32(buf[pos : pos+4])
			if pos+4+int(size) > len(buf) {
				break
			}

			entryData := buf[pos+4 : pos+4+int(size)]

			logEntry := &filer_pb.LogEntry{}
			if err := proto.Unmarshal(entryData, logEntry); err != nil {
				pos += 4 + int(size)
				continue // Skip corrupted entries
			}

			rowCount++
			pos += 4 + int(size)
		}
		return nil
	}

	// Read file chunks and process them (pattern from read_log_from_disk.go)
	fileSize := filer.FileSize(entry)
	visibleIntervals, _ := filer.NonOverlappingVisibleIntervals(context.Background(), lookupFileIdFn, entry.Chunks, 0, int64(fileSize))
	chunkViews := filer.ViewFromVisibleIntervals(visibleIntervals, 0, int64(fileSize))

	for x := chunkViews.Front(); x != nil; x = x.Next {
		chunk := x.Value
		urlStrings, err := lookupFileIdFn(context.Background(), chunk.FileId)
		if err != nil {
			fmt.Printf("Warning: failed to lookup chunk %s: %v\n", chunk.FileId, err)
			continue
		}

		if len(urlStrings) == 0 {
			continue
		}

		// Read chunk data
		// urlStrings[0] is already a complete URL (http://server:port/fileId)
		data, _, err := util_http.Get(urlStrings[0])
		if err != nil {
			fmt.Printf("Warning: failed to read chunk %s from %s: %v\n", chunk.FileId, urlStrings[0], err)
			continue
		}

		// Process this chunk
		if err := eachChunkFn(data); err != nil {
			return rowCount, err
		}
	}

	return rowCount, nil
}

// discoverTopicPartitions discovers all partitions for a given topic using centralized logic
func (e *SQLEngine) discoverTopicPartitions(namespace, topicName string) ([]string, error) {
	// Use centralized topic partition discovery
	t := topic.NewTopic(namespace, topicName)

	// Get FilerClient from BrokerClient
	filerClient, err := e.catalog.brokerClient.GetFilerClient()
	if err != nil {
		return nil, err
	}

	return t.DiscoverPartitions(context.Background(), filerClient)
}

// getTopicTotalRowCount returns the total number of rows in a topic (combining parquet and live logs)
func (e *SQLEngine) getTopicTotalRowCount(ctx context.Context, namespace, topicName string) (int64, error) {
	// Create a hybrid scanner to access parquet statistics
	var filerClient filer_pb.FilerClient
	if e.catalog.brokerClient != nil {
		var filerClientErr error
		filerClient, filerClientErr = e.catalog.brokerClient.GetFilerClient()
		if filerClientErr != nil {
			return 0, filerClientErr
		}
	}

	hybridScanner, err := NewHybridMessageScanner(filerClient, e.catalog.brokerClient, namespace, topicName)
	if err != nil {
		return 0, err
	}

	// Get all partitions for this topic
	// Note: discoverTopicPartitions always returns absolute paths
	partitions, err := e.discoverTopicPartitions(namespace, topicName)
	if err != nil {
		return 0, err
	}

	totalRowCount := int64(0)

	// For each partition, count both parquet and live log rows
	for _, partition := range partitions {
		// Count parquet rows
		parquetStats, parquetErr := hybridScanner.ReadParquetStatistics(partition)
		if parquetErr == nil {
			for _, stats := range parquetStats {
				totalRowCount += stats.RowCount
			}
		}

		// Count live log rows (with deduplication)
		parquetSourceFiles := make(map[string]bool)
		if parquetErr == nil {
			parquetSourceFiles = e.extractParquetSourceFiles(parquetStats)
		}

		liveLogCount, liveLogErr := e.countLiveLogRowsExcludingParquetSources(ctx, partition, parquetSourceFiles)
		if liveLogErr == nil {
			totalRowCount += liveLogCount
		}
	}

	return totalRowCount, nil
}

// getActualRowsScannedForFastPath returns only the rows that need to be scanned for fast path aggregations
// (i.e., live log rows that haven't been converted to parquet - parquet uses metadata only)
func (e *SQLEngine) getActualRowsScannedForFastPath(ctx context.Context, namespace, topicName string) (int64, error) {
	// Create a hybrid scanner to access parquet statistics
	var filerClient filer_pb.FilerClient
	if e.catalog.brokerClient != nil {
		var filerClientErr error
		filerClient, filerClientErr = e.catalog.brokerClient.GetFilerClient()
		if filerClientErr != nil {
			return 0, filerClientErr
		}
	}

	hybridScanner, err := NewHybridMessageScanner(filerClient, e.catalog.brokerClient, namespace, topicName)
	if err != nil {
		return 0, err
	}

	// Get all partitions for this topic
	// Note: discoverTopicPartitions always returns absolute paths
	partitions, err := e.discoverTopicPartitions(namespace, topicName)
	if err != nil {
		return 0, err
	}

	totalScannedRows := int64(0)

	// For each partition, count ONLY the live log rows that need scanning
	// (parquet files use metadata/statistics, so they contribute 0 to scan count)
	for _, partition := range partitions {
		// Get parquet files to determine what was converted
		parquetStats, parquetErr := hybridScanner.ReadParquetStatistics(partition)
		parquetSourceFiles := make(map[string]bool)
		if parquetErr == nil {
			parquetSourceFiles = e.extractParquetSourceFiles(parquetStats)
		}

		// Count only live log rows that haven't been converted to parquet
		liveLogCount, liveLogErr := e.countLiveLogRowsExcludingParquetSources(ctx, partition, parquetSourceFiles)
		if liveLogErr == nil {
			totalScannedRows += liveLogCount
		}

		// Note: Parquet files contribute 0 to scan count since we use their metadata/statistics
	}

	return totalScannedRows, nil
}

// findColumnValue performs case-insensitive lookup of column values
// Now includes support for system columns stored in HybridScanResult
func (e *SQLEngine) findColumnValue(result HybridScanResult, columnName string) *schema_pb.Value {
	// Check system columns first (stored separately in HybridScanResult)
	lowerColumnName := strings.ToLower(columnName)
	switch lowerColumnName {
	case SW_COLUMN_NAME_TIMESTAMP:
		return &schema_pb.Value{Kind: &schema_pb.Value_Int64Value{Int64Value: result.Timestamp}}
	case SW_COLUMN_NAME_KEY:
		return &schema_pb.Value{Kind: &schema_pb.Value_BytesValue{BytesValue: result.Key}}
	case SW_COLUMN_NAME_SOURCE:
		return &schema_pb.Value{Kind: &schema_pb.Value_StringValue{StringValue: result.Source}}
	}

	// Then check regular columns in Values map
	// First try exact match
	if value, exists := result.Values[columnName]; exists {
		return value
	}

	// Then try case-insensitive match
	for key, value := range result.Values {
		if strings.ToLower(key) == lowerColumnName {
			return value
		}
	}

	return nil
}

// discoverAndRegisterTopic attempts to discover an existing topic and register it in the SQL catalog
func (e *SQLEngine) discoverAndRegisterTopic(ctx context.Context, database, tableName string) error {
	// First, check if topic exists by trying to get its schema from the broker/filer
	recordType, err := e.catalog.brokerClient.GetTopicSchema(ctx, database, tableName)
	if err != nil {
		return fmt.Errorf("topic %s.%s not found or no schema available: %v", database, tableName, err)
	}

	// Create a schema object from the discovered record type
	mqSchema := &schema.Schema{
		Namespace:  database,
		Name:       tableName,
		RecordType: recordType,
		RevisionId: 1, // Default to revision 1 for discovered topics
	}

	// Register the topic in the SQL catalog
	err = e.catalog.RegisterTopic(database, tableName, mqSchema)
	if err != nil {
		return fmt.Errorf("failed to register discovered topic %s.%s: %v", database, tableName, err)
	}

	// Note: This is a discovery operation, not query execution, so it's okay to always log
	return nil
}

// getArithmeticExpressionAlias generates a display alias for arithmetic expressions
func (e *SQLEngine) getArithmeticExpressionAlias(expr *ArithmeticExpr) string {
	leftAlias := e.getExpressionAlias(expr.Left)
	rightAlias := e.getExpressionAlias(expr.Right)
	return leftAlias + expr.Operator + rightAlias
}

// getExpressionAlias generates an alias for any expression node
func (e *SQLEngine) getExpressionAlias(expr ExprNode) string {
	switch exprType := expr.(type) {
	case *ColName:
		return exprType.Name.String()
	case *ArithmeticExpr:
		return e.getArithmeticExpressionAlias(exprType)
	case *SQLVal:
		return e.getSQLValAlias(exprType)
	default:
		return "expr"
	}
}

// evaluateArithmeticExpression evaluates an arithmetic expression for a given record
func (e *SQLEngine) evaluateArithmeticExpression(expr *ArithmeticExpr, result HybridScanResult) (*schema_pb.Value, error) {
	// Get left operand value
	leftValue, err := e.evaluateExpressionValue(expr.Left, result)
	if err != nil {
		return nil, fmt.Errorf("error evaluating left operand: %v", err)
	}

	// Get right operand value
	rightValue, err := e.evaluateExpressionValue(expr.Right, result)
	if err != nil {
		return nil, fmt.Errorf("error evaluating right operand: %v", err)
	}

	// Handle string concatenation operator
	if expr.Operator == "||" {
		return e.Concat(leftValue, rightValue)
	}

	// Perform arithmetic operation
	var op ArithmeticOperator
	switch expr.Operator {
	case "+":
		op = OpAdd
	case "-":
		op = OpSub
	case "*":
		op = OpMul
	case "/":
		op = OpDiv
	case "%":
		op = OpMod
	default:
		return nil, fmt.Errorf("unsupported arithmetic operator: %s", expr.Operator)
	}

	return e.EvaluateArithmeticExpression(leftValue, rightValue, op)
}

// evaluateExpressionValue evaluates any expression to get its value from a record
func (e *SQLEngine) evaluateExpressionValue(expr ExprNode, result HybridScanResult) (*schema_pb.Value, error) {
	switch exprType := expr.(type) {
	case *ColName:
		columnName := exprType.Name.String()
		upperColumnName := strings.ToUpper(columnName)

		// Check if this is actually a string literal that was parsed as ColName
		if (strings.HasPrefix(columnName, "'") && strings.HasSuffix(columnName, "'")) ||
			(strings.HasPrefix(columnName, "\"") && strings.HasSuffix(columnName, "\"")) {
			// This is a string literal that was incorrectly parsed as a column name
			literal := strings.Trim(strings.Trim(columnName, "'"), "\"")
			return &schema_pb.Value{Kind: &schema_pb.Value_StringValue{StringValue: literal}}, nil
		}

		// Check if this is actually a function call that was parsed as ColName
		if strings.Contains(columnName, "(") && strings.Contains(columnName, ")") {
			// This is a function call that was parsed incorrectly as a column name
			// We need to manually evaluate it as a function
			return e.evaluateColumnNameAsFunction(columnName, result)
		}

		// Check if this is a datetime constant
		if upperColumnName == FuncCURRENT_DATE || upperColumnName == FuncCURRENT_TIME ||
			upperColumnName == FuncCURRENT_TIMESTAMP || upperColumnName == FuncNOW {
			switch upperColumnName {
			case FuncCURRENT_DATE:
				return e.CurrentDate()
			case FuncCURRENT_TIME:
				return e.CurrentTime()
			case FuncCURRENT_TIMESTAMP:
				return e.CurrentTimestamp()
			case FuncNOW:
				return e.Now()
			}
		}

		// Check if this is actually a numeric literal disguised as a column name
		if val, err := strconv.ParseInt(columnName, 10, 64); err == nil {
			return &schema_pb.Value{Kind: &schema_pb.Value_Int64Value{Int64Value: val}}, nil
		}
		if val, err := strconv.ParseFloat(columnName, 64); err == nil {
			return &schema_pb.Value{Kind: &schema_pb.Value_DoubleValue{DoubleValue: val}}, nil
		}

		// Otherwise, treat as a regular column lookup
		value := e.findColumnValue(result, columnName)
		if value == nil {
			return nil, nil
		}
		return value, nil
	case *ArithmeticExpr:
		return e.evaluateArithmeticExpression(exprType, result)
	case *SQLVal:
		// Handle literal values
		return e.convertSQLValToSchemaValue(exprType), nil
	case *FuncExpr:
		// Handle function calls that are part of arithmetic expressions
		funcName := strings.ToUpper(exprType.Name.String())

		// Route to appropriate function evaluator based on function type
		if e.isDateTimeFunction(funcName) {
			// Use datetime function evaluator
			return e.evaluateDateTimeFunction(exprType, result)
		} else {
			// Use string function evaluator
			return e.evaluateStringFunction(exprType, result)
		}
	default:
		return nil, fmt.Errorf("unsupported expression type: %T", expr)
	}
}

// convertSQLValToSchemaValue converts SQLVal literal to schema_pb.Value
func (e *SQLEngine) convertSQLValToSchemaValue(sqlVal *SQLVal) *schema_pb.Value {
	switch sqlVal.Type {
	case IntVal:
		if val, err := strconv.ParseInt(string(sqlVal.Val), 10, 64); err == nil {
			return &schema_pb.Value{Kind: &schema_pb.Value_Int64Value{Int64Value: val}}
		}
	case FloatVal:
		if val, err := strconv.ParseFloat(string(sqlVal.Val), 64); err == nil {
			return &schema_pb.Value{Kind: &schema_pb.Value_DoubleValue{DoubleValue: val}}
		}
	case StrVal:
		return &schema_pb.Value{Kind: &schema_pb.Value_StringValue{StringValue: string(sqlVal.Val)}}
	}
	// Default to string if parsing fails
	return &schema_pb.Value{Kind: &schema_pb.Value_StringValue{StringValue: string(sqlVal.Val)}}
}

// ConvertToSQLResultWithExpressions converts HybridScanResults to SQL query results with expression evaluation
func (e *SQLEngine) ConvertToSQLResultWithExpressions(hms *HybridMessageScanner, results []HybridScanResult, selectExprs []SelectExpr) *QueryResult {
	if len(results) == 0 {
		columns := make([]string, 0, len(selectExprs))
		for _, selectExpr := range selectExprs {
			switch expr := selectExpr.(type) {
			case *AliasedExpr:
				// Check if alias is available and use it
				if expr.As != nil && !expr.As.IsEmpty() {
					columns = append(columns, expr.As.String())
				} else {
					// Fall back to expression-based column naming
					switch col := expr.Expr.(type) {
					case *ColName:
						columnName := col.Name.String()
						upperColumnName := strings.ToUpper(columnName)

						// Check if this is an arithmetic expression embedded in a ColName
						if arithmeticExpr := e.parseColumnLevelCalculation(columnName); arithmeticExpr != nil {
							columns = append(columns, e.getArithmeticExpressionAlias(arithmeticExpr))
						} else if upperColumnName == FuncCURRENT_DATE || upperColumnName == FuncCURRENT_TIME ||
							upperColumnName == FuncCURRENT_TIMESTAMP || upperColumnName == FuncNOW {
							// Use lowercase for datetime constants in column headers
							columns = append(columns, strings.ToLower(columnName))
						} else {
							columns = append(columns, columnName)
						}
					case *ArithmeticExpr:
						columns = append(columns, e.getArithmeticExpressionAlias(col))
					case *FuncExpr:
						columns = append(columns, e.getStringFunctionAlias(col))
					case *SQLVal:
						columns = append(columns, e.getSQLValAlias(col))
					default:
						columns = append(columns, "expr")
					}
				}
			}
		}

		return &QueryResult{
			Columns:  columns,
			Rows:     [][]sqltypes.Value{},
			Database: hms.topic.Namespace,
			Table:    hms.topic.Name,
		}
	}

	// Build columns from SELECT expressions
	columns := make([]string, 0, len(selectExprs))
	for _, selectExpr := range selectExprs {
		switch expr := selectExpr.(type) {
		case *AliasedExpr:
			// Check if alias is available and use it
			if expr.As != nil && !expr.As.IsEmpty() {
				columns = append(columns, expr.As.String())
			} else {
				// Fall back to expression-based column naming
				switch col := expr.Expr.(type) {
				case *ColName:
					columnName := col.Name.String()
					upperColumnName := strings.ToUpper(columnName)

					// Check if this is an arithmetic expression embedded in a ColName
					if arithmeticExpr := e.parseColumnLevelCalculation(columnName); arithmeticExpr != nil {
						columns = append(columns, e.getArithmeticExpressionAlias(arithmeticExpr))
					} else if upperColumnName == FuncCURRENT_DATE || upperColumnName == FuncCURRENT_TIME ||
						upperColumnName == FuncCURRENT_TIMESTAMP || upperColumnName == FuncNOW {
						// Use lowercase for datetime constants in column headers
						columns = append(columns, strings.ToLower(columnName))
					} else {
						columns = append(columns, columnName)
					}
				case *ArithmeticExpr:
					columns = append(columns, e.getArithmeticExpressionAlias(col))
				case *FuncExpr:
					columns = append(columns, e.getStringFunctionAlias(col))
				case *SQLVal:
					columns = append(columns, e.getSQLValAlias(col))
				default:
					columns = append(columns, "expr")
				}
			}
		}
	}

	// Convert to SQL rows with expression evaluation
	rows := make([][]sqltypes.Value, len(results))
	for i, result := range results {
		row := make([]sqltypes.Value, len(selectExprs))
		for j, selectExpr := range selectExprs {
			switch expr := selectExpr.(type) {
			case *AliasedExpr:
				switch col := expr.Expr.(type) {
				case *ColName:
					// Handle regular column, datetime constants, or arithmetic expressions
					columnName := col.Name.String()
					upperColumnName := strings.ToUpper(columnName)

					// Check if this is an arithmetic expression embedded in a ColName
					if arithmeticExpr := e.parseColumnLevelCalculation(columnName); arithmeticExpr != nil {
						// Handle as arithmetic expression
						if value, err := e.evaluateArithmeticExpression(arithmeticExpr, result); err == nil && value != nil {
							row[j] = convertSchemaValueToSQL(value)
						} else {
							row[j] = sqltypes.NULL
						}
					} else if upperColumnName == "CURRENT_DATE" || upperColumnName == "CURRENT_TIME" ||
						upperColumnName == "CURRENT_TIMESTAMP" || upperColumnName == "NOW" {
						// Handle as datetime function
						var value *schema_pb.Value
						var err error
						switch upperColumnName {
						case FuncCURRENT_DATE:
							value, err = e.CurrentDate()
						case FuncCURRENT_TIME:
							value, err = e.CurrentTime()
						case FuncCURRENT_TIMESTAMP:
							value, err = e.CurrentTimestamp()
						case FuncNOW:
							value, err = e.Now()
						}

						if err == nil && value != nil {
							row[j] = convertSchemaValueToSQL(value)
						} else {
							row[j] = sqltypes.NULL
						}
					} else {
						// Handle as regular column
						if value := e.findColumnValue(result, columnName); value != nil {
							row[j] = convertSchemaValueToSQL(value)
						} else {
							row[j] = sqltypes.NULL
						}
					}
				case *ArithmeticExpr:
					// Handle arithmetic expression
					if value, err := e.evaluateArithmeticExpression(col, result); err == nil && value != nil {
						row[j] = convertSchemaValueToSQL(value)
					} else {
						row[j] = sqltypes.NULL
					}
				case *FuncExpr:
					// Handle function - route to appropriate evaluator
					funcName := strings.ToUpper(col.Name.String())
					var value *schema_pb.Value
					var err error

					// Check if it's a datetime function
					if e.isDateTimeFunction(funcName) {
						value, err = e.evaluateDateTimeFunction(col, result)
					} else {
						// Default to string function evaluator
						value, err = e.evaluateStringFunction(col, result)
					}

					if err == nil && value != nil {
						row[j] = convertSchemaValueToSQL(value)
					} else {
						row[j] = sqltypes.NULL
					}
				case *SQLVal:
					// Handle literal value
					value := e.convertSQLValToSchemaValue(col)
					row[j] = convertSchemaValueToSQL(value)
				default:
					row[j] = sqltypes.NULL
				}
			default:
				row[j] = sqltypes.NULL
			}
		}
		rows[i] = row
	}

	return &QueryResult{
		Columns:  columns,
		Rows:     rows,
		Database: hms.topic.Namespace,
		Table:    hms.topic.Name,
	}
}

// extractBaseColumns recursively extracts base column names from arithmetic expressions
func (e *SQLEngine) extractBaseColumns(expr *ArithmeticExpr, baseColumnsSet map[string]bool) {
	// Extract columns from left operand
	e.extractBaseColumnsFromExpression(expr.Left, baseColumnsSet)
	// Extract columns from right operand
	e.extractBaseColumnsFromExpression(expr.Right, baseColumnsSet)
}

// extractBaseColumnsFromExpression extracts base column names from any expression node
func (e *SQLEngine) extractBaseColumnsFromExpression(expr ExprNode, baseColumnsSet map[string]bool) {
	switch exprType := expr.(type) {
	case *ColName:
		columnName := exprType.Name.String()
		// Check if it's a literal number disguised as a column name
		if _, err := strconv.ParseInt(columnName, 10, 64); err != nil {
			if _, err := strconv.ParseFloat(columnName, 64); err != nil {
				// Not a numeric literal, treat as actual column name
				baseColumnsSet[columnName] = true
			}
		}
	case *ArithmeticExpr:
		// Recursively handle nested arithmetic expressions
		e.extractBaseColumns(exprType, baseColumnsSet)
	}
}

// isAggregationFunction checks if a function name is an aggregation function
func (e *SQLEngine) isAggregationFunction(funcName string) bool {
	// Convert to uppercase for case-insensitive comparison
	upperFuncName := strings.ToUpper(funcName)
	switch upperFuncName {
	case FuncCOUNT, FuncSUM, FuncAVG, FuncMIN, FuncMAX:
		return true
	default:
		return false
	}
}

// isStringFunction checks if a function name is a string function
func (e *SQLEngine) isStringFunction(funcName string) bool {
	switch funcName {
	case FuncUPPER, FuncLOWER, FuncLENGTH, FuncTRIM, FuncBTRIM, FuncLTRIM, FuncRTRIM, FuncSUBSTRING, FuncLEFT, FuncRIGHT, FuncCONCAT:
		return true
	default:
		return false
	}
}

// isDateTimeFunction checks if a function name is a datetime function
func (e *SQLEngine) isDateTimeFunction(funcName string) bool {
	switch funcName {
	case FuncCURRENT_DATE, FuncCURRENT_TIME, FuncCURRENT_TIMESTAMP, FuncNOW, FuncEXTRACT, FuncDATE_TRUNC:
		return true
	default:
		return false
	}
}

// getStringFunctionAlias generates an alias for string functions
func (e *SQLEngine) getStringFunctionAlias(funcExpr *FuncExpr) string {
	funcName := funcExpr.Name.String()
	if len(funcExpr.Exprs) == 1 {
		if aliasedExpr, ok := funcExpr.Exprs[0].(*AliasedExpr); ok {
			if colName, ok := aliasedExpr.Expr.(*ColName); ok {
				return fmt.Sprintf("%s(%s)", funcName, colName.Name.String())
			}
		}
	}
	return fmt.Sprintf("%s(...)", funcName)
}

// getDateTimeFunctionAlias generates an alias for datetime functions
func (e *SQLEngine) getDateTimeFunctionAlias(funcExpr *FuncExpr) string {
	funcName := funcExpr.Name.String()

	// Handle zero-argument functions like CURRENT_DATE, NOW
	if len(funcExpr.Exprs) == 0 {
		// Use lowercase for datetime constants in column headers
		return strings.ToLower(funcName)
	}

	// Handle EXTRACT function specially to create unique aliases
	if strings.ToUpper(funcName) == "EXTRACT" && len(funcExpr.Exprs) == 2 {
		// Try to extract the date part to make the alias unique
		if aliasedExpr, ok := funcExpr.Exprs[0].(*AliasedExpr); ok {
			if sqlVal, ok := aliasedExpr.Expr.(*SQLVal); ok && sqlVal.Type == StrVal {
				datePart := strings.ToLower(string(sqlVal.Val))
				return fmt.Sprintf("extract_%s", datePart)
			}
		}
		// Fallback to generic if we can't extract the date part
		return fmt.Sprintf("%s(...)", funcName)
	}

	// Handle other multi-argument functions like DATE_TRUNC
	if len(funcExpr.Exprs) == 2 {
		return fmt.Sprintf("%s(...)", funcName)
	}

	return fmt.Sprintf("%s(...)", funcName)
}

// extractBaseColumnsFromFunction extracts base columns needed by a string function
func (e *SQLEngine) extractBaseColumnsFromFunction(funcExpr *FuncExpr, baseColumnsSet map[string]bool) {
	for _, expr := range funcExpr.Exprs {
		if aliasedExpr, ok := expr.(*AliasedExpr); ok {
			e.extractBaseColumnsFromExpression(aliasedExpr.Expr, baseColumnsSet)
		}
	}
}

// getSQLValAlias generates an alias for SQL literal values
func (e *SQLEngine) getSQLValAlias(sqlVal *SQLVal) string {
	switch sqlVal.Type {
	case StrVal:
		// Escape single quotes by replacing ' with '' (SQL standard escaping)
		escapedVal := strings.ReplaceAll(string(sqlVal.Val), "'", "''")
		return fmt.Sprintf("'%s'", escapedVal)
	case IntVal:
		return string(sqlVal.Val)
	case FloatVal:
		return string(sqlVal.Val)
	default:
		return "literal"
	}
}

// evaluateStringFunction evaluates a string function for a given record
func (e *SQLEngine) evaluateStringFunction(funcExpr *FuncExpr, result HybridScanResult) (*schema_pb.Value, error) {
	funcName := strings.ToUpper(funcExpr.Name.String())

	// Most string functions require exactly 1 argument
	if len(funcExpr.Exprs) != 1 {
		return nil, fmt.Errorf("function %s expects exactly 1 argument", funcName)
	}

	// Get the argument value
	var argValue *schema_pb.Value
	if aliasedExpr, ok := funcExpr.Exprs[0].(*AliasedExpr); ok {
		var err error
		argValue, err = e.evaluateExpressionValue(aliasedExpr.Expr, result)
		if err != nil {
			return nil, fmt.Errorf("error evaluating function argument: %v", err)
		}
	} else {
		return nil, fmt.Errorf("unsupported function argument type")
	}

	if argValue == nil {
		return nil, nil // NULL input produces NULL output
	}

	// Call the appropriate string function
	switch funcName {
	case FuncUPPER:
		return e.Upper(argValue)
	case FuncLOWER:
		return e.Lower(argValue)
	case FuncLENGTH:
		return e.Length(argValue)
	case FuncTRIM, FuncBTRIM: // CockroachDB converts TRIM to BTRIM
		return e.Trim(argValue)
	case FuncLTRIM:
		return e.LTrim(argValue)
	case FuncRTRIM:
		return e.RTrim(argValue)
	default:
		return nil, fmt.Errorf("unsupported string function: %s", funcName)
	}
}

// evaluateDateTimeFunction evaluates a datetime function for a given record
func (e *SQLEngine) evaluateDateTimeFunction(funcExpr *FuncExpr, result HybridScanResult) (*schema_pb.Value, error) {
	funcName := strings.ToUpper(funcExpr.Name.String())

	switch funcName {
	case FuncEXTRACT:
		// EXTRACT requires exactly 2 arguments: date part and value
		if len(funcExpr.Exprs) != 2 {
			return nil, fmt.Errorf("EXTRACT function expects exactly 2 arguments (date_part, value), got %d", len(funcExpr.Exprs))
		}

		// Get the first argument (date part)
		var datePartValue *schema_pb.Value
		if aliasedExpr, ok := funcExpr.Exprs[0].(*AliasedExpr); ok {
			var err error
			datePartValue, err = e.evaluateExpressionValue(aliasedExpr.Expr, result)
			if err != nil {
				return nil, fmt.Errorf("error evaluating EXTRACT date part argument: %v", err)
			}
		} else {
			return nil, fmt.Errorf("unsupported EXTRACT date part argument type")
		}

		if datePartValue == nil {
			return nil, fmt.Errorf("EXTRACT date part cannot be NULL")
		}

		// Convert date part to string
		var datePart string
		if stringVal, ok := datePartValue.Kind.(*schema_pb.Value_StringValue); ok {
			datePart = strings.ToUpper(stringVal.StringValue)
		} else {
			return nil, fmt.Errorf("EXTRACT date part must be a string")
		}

		// Get the second argument (value to extract from)
		var extractValue *schema_pb.Value
		if aliasedExpr, ok := funcExpr.Exprs[1].(*AliasedExpr); ok {
			var err error
			extractValue, err = e.evaluateExpressionValue(aliasedExpr.Expr, result)
			if err != nil {
				return nil, fmt.Errorf("error evaluating EXTRACT value argument: %v", err)
			}
		} else {
			return nil, fmt.Errorf("unsupported EXTRACT value argument type")
		}

		if extractValue == nil {
			return nil, nil // NULL input produces NULL output
		}

		// Call the Extract function
		return e.Extract(DatePart(datePart), extractValue)

	case FuncDATE_TRUNC:
		// DATE_TRUNC requires exactly 2 arguments: precision and value
		if len(funcExpr.Exprs) != 2 {
			return nil, fmt.Errorf("DATE_TRUNC function expects exactly 2 arguments (precision, value), got %d", len(funcExpr.Exprs))
		}

		// Get the first argument (precision)
		var precisionValue *schema_pb.Value
		if aliasedExpr, ok := funcExpr.Exprs[0].(*AliasedExpr); ok {
			var err error
			precisionValue, err = e.evaluateExpressionValue(aliasedExpr.Expr, result)
			if err != nil {
				return nil, fmt.Errorf("error evaluating DATE_TRUNC precision argument: %v", err)
			}
		} else {
			return nil, fmt.Errorf("unsupported DATE_TRUNC precision argument type")
		}

		if precisionValue == nil {
			return nil, fmt.Errorf("DATE_TRUNC precision cannot be NULL")
		}

		// Convert precision to string
		var precision string
		if stringVal, ok := precisionValue.Kind.(*schema_pb.Value_StringValue); ok {
			precision = stringVal.StringValue
		} else {
			return nil, fmt.Errorf("DATE_TRUNC precision must be a string")
		}

		// Get the second argument (value to truncate)
		var truncateValue *schema_pb.Value
		if aliasedExpr, ok := funcExpr.Exprs[1].(*AliasedExpr); ok {
			var err error
			truncateValue, err = e.evaluateExpressionValue(aliasedExpr.Expr, result)
			if err != nil {
				return nil, fmt.Errorf("error evaluating DATE_TRUNC value argument: %v", err)
			}
		} else {
			return nil, fmt.Errorf("unsupported DATE_TRUNC value argument type")
		}

		if truncateValue == nil {
			return nil, nil // NULL input produces NULL output
		}

		// Call the DateTrunc function
		return e.DateTrunc(precision, truncateValue)

	case FuncCURRENT_DATE:
		// CURRENT_DATE is a zero-argument function
		if len(funcExpr.Exprs) != 0 {
			return nil, fmt.Errorf("CURRENT_DATE function expects no arguments, got %d", len(funcExpr.Exprs))
		}
		return e.CurrentDate()

	case FuncCURRENT_TIME:
		// CURRENT_TIME is a zero-argument function
		if len(funcExpr.Exprs) != 0 {
			return nil, fmt.Errorf("CURRENT_TIME function expects no arguments, got %d", len(funcExpr.Exprs))
		}
		return e.CurrentTime()

	case FuncCURRENT_TIMESTAMP:
		// CURRENT_TIMESTAMP is a zero-argument function
		if len(funcExpr.Exprs) != 0 {
			return nil, fmt.Errorf("CURRENT_TIMESTAMP function expects no arguments, got %d", len(funcExpr.Exprs))
		}
		return e.CurrentTimestamp()

	case FuncNOW:
		// NOW is a zero-argument function (but often used with () syntax)
		if len(funcExpr.Exprs) != 0 {
			return nil, fmt.Errorf("NOW function expects no arguments, got %d", len(funcExpr.Exprs))
		}
		return e.Now()

	// PostgreSQL uses EXTRACT(part FROM date) instead of convenience functions like YEAR(date)

	default:
		return nil, fmt.Errorf("unsupported datetime function: %s", funcName)
	}
}

// evaluateColumnNameAsFunction handles function calls that were incorrectly parsed as column names
func (e *SQLEngine) evaluateColumnNameAsFunction(columnName string, result HybridScanResult) (*schema_pb.Value, error) {
	// Simple parser for basic function calls like TRIM('hello world')
	// Extract function name and argument
	parenPos := strings.Index(columnName, "(")
	if parenPos == -1 {
		return nil, fmt.Errorf("invalid function format: %s", columnName)
	}

	funcName := strings.ToUpper(strings.TrimSpace(columnName[:parenPos]))
	argsString := columnName[parenPos+1:]

	// Find the closing parenthesis (handling nested quotes)
	closeParen := strings.LastIndex(argsString, ")")
	if closeParen == -1 {
		return nil, fmt.Errorf("missing closing parenthesis in function: %s", columnName)
	}

	argString := strings.TrimSpace(argsString[:closeParen])

	// Parse the argument - for now handle simple cases
	var argValue *schema_pb.Value
	var err error

	if strings.HasPrefix(argString, "'") && strings.HasSuffix(argString, "'") {
		// String literal argument
		literal := strings.Trim(argString, "'")
		argValue = &schema_pb.Value{Kind: &schema_pb.Value_StringValue{StringValue: literal}}
	} else if strings.Contains(argString, "(") && strings.Contains(argString, ")") {
		// Nested function call - recursively evaluate it
		argValue, err = e.evaluateColumnNameAsFunction(argString, result)
		if err != nil {
			return nil, fmt.Errorf("error evaluating nested function argument: %v", err)
		}
	} else {
		// Column name or other expression
		return nil, fmt.Errorf("unsupported argument type in function: %s", argString)
	}

	if argValue == nil {
		return nil, nil
	}

	// Call the appropriate function
	switch funcName {
	case FuncUPPER:
		return e.Upper(argValue)
	case FuncLOWER:
		return e.Lower(argValue)
	case FuncLENGTH:
		return e.Length(argValue)
	case FuncTRIM, FuncBTRIM: // CockroachDB converts TRIM to BTRIM
		return e.Trim(argValue)
	case FuncLTRIM:
		return e.LTrim(argValue)
	case FuncRTRIM:
		return e.RTrim(argValue)
	// PostgreSQL-only: Use EXTRACT(YEAR FROM date) instead of YEAR(date)
	default:
		return nil, fmt.Errorf("unsupported function in column name: %s", funcName)
	}
}

// parseColumnLevelCalculation detects and parses arithmetic expressions that contain function calls
// This handles cases where the SQL parser incorrectly treats "LENGTH('hello') + 10" as a single ColName
func (e *SQLEngine) parseColumnLevelCalculation(expression string) *ArithmeticExpr {
	// First check if this looks like an arithmetic expression
	if !e.containsArithmeticOperator(expression) {
		return nil
	}

	// Build AST for the arithmetic expression
	return e.buildArithmeticAST(expression)
}

// containsArithmeticOperator checks if the expression contains arithmetic operators outside of function calls
func (e *SQLEngine) containsArithmeticOperator(expr string) bool {
	operators := []string{"+", "-", "*", "/", "%", "||"}

	parenLevel := 0
	quoteLevel := false

	for i, char := range expr {
		switch char {
		case '(':
			if !quoteLevel {
				parenLevel++
			}
		case ')':
			if !quoteLevel {
				parenLevel--
			}
		case '\'':
			quoteLevel = !quoteLevel
		default:
			// Only check for operators outside of parentheses and quotes
			if parenLevel == 0 && !quoteLevel {
				for _, op := range operators {
					if strings.HasPrefix(expr[i:], op) {
						return true
					}
				}
			}
		}
	}

	return false
}

// buildArithmeticAST builds an Abstract Syntax Tree for arithmetic expressions containing function calls
func (e *SQLEngine) buildArithmeticAST(expr string) *ArithmeticExpr {
	// Remove leading/trailing spaces
	expr = strings.TrimSpace(expr)

	// Find the main operator (outside of parentheses)
	operators := []string{"||", "+", "-", "*", "/", "%"} // Order matters for precedence

	for _, op := range operators {
		opPos := e.findMainOperator(expr, op)
		if opPos != -1 {
			leftExpr := strings.TrimSpace(expr[:opPos])
			rightExpr := strings.TrimSpace(expr[opPos+len(op):])

			if leftExpr != "" && rightExpr != "" {
				return &ArithmeticExpr{
					Left:     e.parseASTExpressionNode(leftExpr),
					Right:    e.parseASTExpressionNode(rightExpr),
					Operator: op,
				}
			}
		}
	}

	return nil
}

// findMainOperator finds the position of an operator that's not inside parentheses or quotes
func (e *SQLEngine) findMainOperator(expr string, operator string) int {
	parenLevel := 0
	quoteLevel := false

	for i := 0; i <= len(expr)-len(operator); i++ {
		char := expr[i]

		switch char {
		case '(':
			if !quoteLevel {
				parenLevel++
			}
		case ')':
			if !quoteLevel {
				parenLevel--
			}
		case '\'':
			quoteLevel = !quoteLevel
		default:
			// Check for operator only at top level (not inside parentheses or quotes)
			if parenLevel == 0 && !quoteLevel && strings.HasPrefix(expr[i:], operator) {
				return i
			}
		}
	}

	return -1
}

// parseASTExpressionNode parses an expression into the appropriate ExprNode type
func (e *SQLEngine) parseASTExpressionNode(expr string) ExprNode {
	expr = strings.TrimSpace(expr)

	// Check if it's a function call (contains parentheses)
	if strings.Contains(expr, "(") && strings.Contains(expr, ")") {
		// This should be parsed as a function expression, but since our SQL parser
		// has limitations, we'll create a special ColName that represents the function
		return &ColName{Name: stringValue(expr)}
	}

	// Check if it's a numeric literal
	if _, err := strconv.ParseInt(expr, 10, 64); err == nil {
		return &SQLVal{Type: IntVal, Val: []byte(expr)}
	}

	if _, err := strconv.ParseFloat(expr, 64); err == nil {
		return &SQLVal{Type: FloatVal, Val: []byte(expr)}
	}

	// Check if it's a string literal
	if strings.HasPrefix(expr, "'") && strings.HasSuffix(expr, "'") {
		return &SQLVal{Type: StrVal, Val: []byte(strings.Trim(expr, "'"))}
	}

	// Check for nested arithmetic expressions
	if nestedArithmetic := e.buildArithmeticAST(expr); nestedArithmetic != nil {
		return nestedArithmetic
	}

	// Default to column name
	return &ColName{Name: stringValue(expr)}
}
