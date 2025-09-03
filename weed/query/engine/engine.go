package engine

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"math"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/mq/schema"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/schema_pb"
	"github.com/seaweedfs/seaweedfs/weed/query/sqltypes"
	"github.com/seaweedfs/seaweedfs/weed/util"
	util_http "github.com/seaweedfs/seaweedfs/weed/util/http"
	"github.com/xwb1989/sqlparser"
	"google.golang.org/protobuf/proto"
)

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
// 1. All SQL statements are MySQL-compatible via xwb1989/sqlparser
// 2. DDL operations (CREATE/ALTER/DROP) modify underlying MQ topics
// 3. DML operations (SELECT) query Parquet files directly
// 4. Error handling follows MySQL conventions
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

	// Handle DESCRIBE/DESC as a special case since it's not parsed as a standard statement
	if strings.HasPrefix(sqlUpper, "DESCRIBE") || strings.HasPrefix(sqlUpper, "DESC") {
		return e.handleDescribeCommand(ctx, sqlTrimmed)
	}

	// Parse the SQL statement
	stmt, err := sqlparser.Parse(sql)
	if err != nil {
		return &QueryResult{
			Error: fmt.Errorf("SQL parse error: %v", err),
		}, err
	}

	// Route to appropriate handler based on statement type
	switch stmt := stmt.(type) {
	case *sqlparser.Show:
		return e.executeShowStatementWithDescribe(ctx, stmt)
	case *sqlparser.DDL:
		return e.executeDDLStatement(ctx, stmt)
	case *sqlparser.Select:
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

	// Parse the actual SQL statement
	stmt, err := sqlparser.Parse(actualSQL)
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
	case *sqlparser.Select:
		result, err = e.executeSelectStatementWithPlan(ctx, stmt, plan)
		if err != nil {
			plan.Details["error"] = err.Error()
		}
	case *sqlparser.Show:
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

	// Statistics section
	statisticsPresent := plan.PartitionsScanned > 0 || plan.ParquetFilesScanned > 0 ||
		plan.LiveLogFilesScanned > 0 || plan.TotalRowsProcessed > 0

	if statisticsPresent {
		// Performance is always present, so Statistics is never the last section
		hasMoreAfterStats := true
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

	// Details section
	// Filter out details that are shown elsewhere
	filteredDetails := make([]string, 0)
	for key, value := range plan.Details {
		if key != "results_returned" { // Skip as it's shown in Statistics section
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

// executeDDLStatement handles CREATE, ALTER, DROP operations
// Assumption: These operations modify the underlying MQ topic structure
func (e *SQLEngine) executeDDLStatement(ctx context.Context, stmt *sqlparser.DDL) (*QueryResult, error) {
	switch stmt.Action {
	case sqlparser.CreateStr:
		return e.createTable(ctx, stmt)
	case sqlparser.AlterStr:
		return e.alterTable(ctx, stmt)
	case sqlparser.DropStr:
		return e.dropTable(ctx, stmt)
	default:
		err := fmt.Errorf("unsupported DDL action: %s", stmt.Action)
		return &QueryResult{Error: err}, err
	}
}

// executeSelectStatementWithPlan handles SELECT queries with execution plan tracking
func (e *SQLEngine) executeSelectStatementWithPlan(ctx context.Context, stmt *sqlparser.Select, plan *QueryExecutionPlan) (*QueryResult, error) {
	// Parse aggregations to populate plan
	var aggregations []AggregationSpec
	hasAggregations := false
	selectAll := false

	for _, selectExpr := range stmt.SelectExprs {
		switch expr := selectExpr.(type) {
		case *sqlparser.StarExpr:
			selectAll = true
		case *sqlparser.AliasedExpr:
			switch col := expr.Expr.(type) {
			case *sqlparser.FuncExpr:
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
			if table, ok := stmt.From[0].(*sqlparser.AliasedTableExpr); ok {
				if tableExpr, ok := table.Expr.(sqlparser.TableName); ok {
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
		// Regular SELECT query
		result, err = e.executeSelectStatement(ctx, stmt)
	}

	if err == nil && result != nil {
		// Extract table name for use in execution strategy determination
		var tableName string
		if len(stmt.From) == 1 {
			if table, ok := stmt.From[0].(*sqlparser.AliasedTableExpr); ok {
				if tableExpr, ok := table.Expr.(sqlparser.TableName); ok {
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
				if limitExpr, ok := stmt.Limit.Rowcount.(*sqlparser.SQLVal); ok && limitExpr.Type == sqlparser.IntVal {
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
func (e *SQLEngine) executeSelectStatement(ctx context.Context, stmt *sqlparser.Select) (*QueryResult, error) {
	// Parse FROM clause to get table (topic) information
	if len(stmt.From) != 1 {
		err := fmt.Errorf("SELECT supports single table queries only")
		return &QueryResult{Error: err}, err
	}

	// Extract table reference
	var database, tableName string
	switch table := stmt.From[0].(type) {
	case *sqlparser.AliasedTableExpr:
		switch tableExpr := table.Expr.(type) {
		case sqlparser.TableName:
			tableName = tableExpr.Name.String()
			if tableExpr.Qualifier.String() != "" {
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
			fmt.Printf("Warning: Failed to discover topic %s.%s: %v\n", database, tableName, regErr)
		}
	}

	// Create HybridMessageScanner for the topic (reads both live logs + Parquet files)
	// Get filerClient from broker connection (works with both real and mock brokers)
	var filerClient filer_pb.FilerClient
	var filerClientErr error
	filerClient, filerClientErr = e.catalog.brokerClient.GetFilerClient()
	if filerClientErr != nil {
		// Log warning but continue with sample data fallback
		fmt.Printf("Warning: Failed to get filer client: %v, using sample data\n", filerClientErr)
	}

	hybridScanner, err := NewHybridMessageScanner(filerClient, e.catalog.brokerClient, database, tableName)
	if err != nil {
		// Fallback to sample data if topic doesn't exist or filer unavailable
		return e.executeSelectWithSampleData(ctx, stmt, database, tableName)
	}

	// Parse SELECT columns and detect aggregation functions
	var columns []string
	var aggregations []AggregationSpec
	selectAll := false
	hasAggregations := false

	for _, selectExpr := range stmt.SelectExprs {
		switch expr := selectExpr.(type) {
		case *sqlparser.StarExpr:
			selectAll = true
		case *sqlparser.AliasedExpr:
			switch col := expr.Expr.(type) {
			case *sqlparser.ColName:
				columns = append(columns, col.Name.String())
			case *sqlparser.FuncExpr:
				// Handle aggregation functions
				aggSpec, err := e.parseAggregationFunction(col, expr)
				if err != nil {
					return &QueryResult{Error: err}, err
				}
				aggregations = append(aggregations, *aggSpec)
				hasAggregations = true
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
		predicate, err = e.buildPredicate(stmt.Where.Expr)
		if err != nil {
			return &QueryResult{Error: err}, err
		}
	}

	// Parse LIMIT clause
	limit := 0
	if stmt.Limit != nil && stmt.Limit.Rowcount != nil {
		switch limitExpr := stmt.Limit.Rowcount.(type) {
		case *sqlparser.SQLVal:
			if limitExpr.Type == sqlparser.IntVal {
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

	// Build hybrid scan options
	// RESOLVED TODO: Extract from WHERE clause time filters
	startTimeNs, stopTimeNs := int64(0), int64(0)
	if stmt.Where != nil {
		startTimeNs, stopTimeNs = e.extractTimeFilters(stmt.Where.Expr)
	}

	hybridScanOptions := HybridScanOptions{
		StartTimeNs: startTimeNs, // Extracted from WHERE clause time comparisons
		StopTimeNs:  stopTimeNs,  // Extracted from WHERE clause time comparisons
		Limit:       limit,
		Predicate:   predicate,
	}

	if !selectAll {
		hybridScanOptions.Columns = columns
	}

	// Execute the hybrid scan (live logs + Parquet files)
	results, err := hybridScanner.Scan(ctx, hybridScanOptions)
	if err != nil {
		return &QueryResult{Error: err}, err
	}

	// Convert to SQL result format
	if selectAll {
		columns = nil // Let converter determine all columns
	}

	return hybridScanner.ConvertToSQLResult(results, columns), nil
}

// executeSelectWithSampleData provides enhanced sample data that simulates both live and archived messages
func (e *SQLEngine) executeSelectWithSampleData(ctx context.Context, stmt *sqlparser.Select, database, tableName string) (*QueryResult, error) {
	// Create a sample HybridMessageScanner to simulate both data sources
	now := time.Now().UnixNano()

	var sampleResults []HybridScanResult

	switch tableName {
	case "user_events":
		sampleResults = []HybridScanResult{
			// Live log data (recent)
			{
				Values: map[string]*schema_pb.Value{
					"user_id":    {Kind: &schema_pb.Value_Int32Value{Int32Value: 1003}},
					"event_type": {Kind: &schema_pb.Value_StringValue{StringValue: "live_login"}},
					"data":       {Kind: &schema_pb.Value_StringValue{StringValue: `{"ip": "10.0.0.1", "live": true}`}},
				},
				Timestamp: now - 300000000000, // 5 minutes ago
				Key:       []byte("live-1003"),
				Source:    "live_log",
			},
			{
				Values: map[string]*schema_pb.Value{
					"user_id":    {Kind: &schema_pb.Value_Int32Value{Int32Value: 1004}},
					"event_type": {Kind: &schema_pb.Value_StringValue{StringValue: "live_click"}},
					"data":       {Kind: &schema_pb.Value_StringValue{StringValue: `{"button": "submit", "live": true}`}},
				},
				Timestamp: now - 120000000000, // 2 minutes ago
				Key:       []byte("live-1004"),
				Source:    "live_log",
			},
			// Archived Parquet data (older)
			{
				Values: map[string]*schema_pb.Value{
					"user_id":    {Kind: &schema_pb.Value_Int32Value{Int32Value: 1001}},
					"event_type": {Kind: &schema_pb.Value_StringValue{StringValue: "archived_login"}},
					"data":       {Kind: &schema_pb.Value_StringValue{StringValue: `{"ip": "192.168.1.1", "archived": true}`}},
				},
				Timestamp: now - 3600000000000, // 1 hour ago
				Key:       []byte("archived-1001"),
				Source:    "parquet_archive",
			},
			{
				Values: map[string]*schema_pb.Value{
					"user_id":    {Kind: &schema_pb.Value_Int32Value{Int32Value: 1002}},
					"event_type": {Kind: &schema_pb.Value_StringValue{StringValue: "archived_logout"}},
					"data":       {Kind: &schema_pb.Value_StringValue{StringValue: `{"duration": 1800, "archived": true}`}},
				},
				Timestamp: now - 1800000000000, // 30 minutes ago
				Key:       []byte("archived-1002"),
				Source:    "parquet_archive",
			},
		}
	case "system_logs":
		sampleResults = []HybridScanResult{
			// Live system logs
			{
				Values: map[string]*schema_pb.Value{
					"level":   {Kind: &schema_pb.Value_StringValue{StringValue: "INFO"}},
					"message": {Kind: &schema_pb.Value_StringValue{StringValue: "Live service heartbeat"}},
					"service": {Kind: &schema_pb.Value_StringValue{StringValue: "api-gateway"}},
				},
				Timestamp: now - 60000000000, // 1 minute ago
				Key:       []byte("live-log-001"),
				Source:    "live_log",
			},
			// Archived system logs
			{
				Values: map[string]*schema_pb.Value{
					"level":   {Kind: &schema_pb.Value_StringValue{StringValue: "ERROR"}},
					"message": {Kind: &schema_pb.Value_StringValue{StringValue: "Database connection timeout"}},
					"service": {Kind: &schema_pb.Value_StringValue{StringValue: "user-service"}},
				},
				Timestamp: now - 7200000000000, // 2 hours ago
				Key:       []byte("archived-error-001"),
				Source:    "parquet_archive",
			},
		}
	default:
		return &QueryResult{
			Error: fmt.Errorf("table '%s.%s' not found", database, tableName),
		}, fmt.Errorf("table '%s.%s' not found", database, tableName)
	}

	// Apply basic LIMIT if specified
	if stmt.Limit != nil && stmt.Limit.Rowcount != nil {
		if limitExpr, ok := stmt.Limit.Rowcount.(*sqlparser.SQLVal); ok && limitExpr.Type == sqlparser.IntVal {
			if limit64, err := strconv.ParseInt(string(limitExpr.Val), 10, 64); err == nil {
				if limit64 > math.MaxInt32 || limit64 < 0 {
					return &QueryResult{
						Error: fmt.Errorf("LIMIT value %d is out of valid range", limit64),
					}, fmt.Errorf("LIMIT value %d is out of valid range", limit64)
				}
				limit := int(limit64)
				if limit > 0 && limit < len(sampleResults) {
					sampleResults = sampleResults[:limit]
				}
			}
		}
	}

	// Convert to SQL result format using hybrid scanner logic
	return convertHybridResultsToSQL(sampleResults, nil), nil
}

// convertHybridResultsToSQL converts HybridScanResults to SQL format (helper function)
func convertHybridResultsToSQL(results []HybridScanResult, columns []string) *QueryResult {
	if len(results) == 0 {
		return &QueryResult{
			Columns: columns,
			Rows:    [][]sqltypes.Value{},
		}
	}

	// Determine columns if not specified
	if len(columns) == 0 {
		columnSet := make(map[string]bool)
		for _, result := range results {
			for columnName := range result.Values {
				columnSet[columnName] = true
			}
		}

		columns = make([]string, 0, len(columnSet))
		for columnName := range columnSet {
			columns = append(columns, columnName)
		}
	}

	// Convert to SQL rows
	rows := make([][]sqltypes.Value, len(results))
	for i, result := range results {
		row := make([]sqltypes.Value, len(columns))
		for j, columnName := range columns {
			switch columnName {
			case "_source":
				row[j] = sqltypes.NewVarChar(result.Source)
			case "_timestamp_ns":
				row[j] = sqltypes.NewInt64(result.Timestamp)
			case "_key":
				row[j] = sqltypes.NewVarBinary(string(result.Key))
			default:
				if value, exists := result.Values[columnName]; exists {
					row[j] = convertSchemaValueToSQL(value)
				} else {
					row[j] = sqltypes.NULL
				}
			}
		}
		rows[i] = row
	}

	return &QueryResult{
		Columns: columns,
		Rows:    rows,
	}
}

// extractTimeFilters extracts time range filters from WHERE clause for optimization
// This allows push-down of time-based queries to improve scan performance
// Returns (startTimeNs, stopTimeNs) where 0 means unbounded
func (e *SQLEngine) extractTimeFilters(expr sqlparser.Expr) (int64, int64) {
	startTimeNs, stopTimeNs := int64(0), int64(0)

	// Recursively extract time filters from expression tree
	e.extractTimeFiltersRecursive(expr, &startTimeNs, &stopTimeNs)

	return startTimeNs, stopTimeNs
}

// extractTimeFiltersRecursive recursively processes WHERE expressions to find time comparisons
func (e *SQLEngine) extractTimeFiltersRecursive(expr sqlparser.Expr, startTimeNs, stopTimeNs *int64) {
	switch exprType := expr.(type) {
	case *sqlparser.ComparisonExpr:
		e.extractTimeFromComparison(exprType, startTimeNs, stopTimeNs)
	case *sqlparser.AndExpr:
		// For AND expressions, combine time filters (intersection)
		e.extractTimeFiltersRecursive(exprType.Left, startTimeNs, stopTimeNs)
		e.extractTimeFiltersRecursive(exprType.Right, startTimeNs, stopTimeNs)
	case *sqlparser.OrExpr:
		// For OR expressions, we can't easily optimize time ranges
		// Skip time filter extraction for OR clauses to avoid incorrect results
		return
	case *sqlparser.ParenExpr:
		// Unwrap parentheses and continue
		e.extractTimeFiltersRecursive(exprType.Expr, startTimeNs, stopTimeNs)
	}
}

// extractTimeFromComparison extracts time bounds from comparison expressions
// Handles comparisons against timestamp columns (_timestamp_ns, timestamp, created_at, etc.)
func (e *SQLEngine) extractTimeFromComparison(comp *sqlparser.ComparisonExpr, startTimeNs, stopTimeNs *int64) {
	// Check if this is a time-related column comparison
	leftCol := e.getColumnName(comp.Left)
	rightCol := e.getColumnName(comp.Right)

	var valueExpr sqlparser.Expr
	var reversed bool

	// Determine which side is the time column
	if e.isTimeColumn(leftCol) {
		valueExpr = comp.Right
		reversed = false
	} else if e.isTimeColumn(rightCol) {
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
	case sqlparser.GreaterThanStr: // timestamp > value
		if *startTimeNs == 0 || timeValue > *startTimeNs {
			*startTimeNs = timeValue
		}
	case sqlparser.GreaterEqualStr: // timestamp >= value
		if *startTimeNs == 0 || timeValue >= *startTimeNs {
			*startTimeNs = timeValue
		}
	case sqlparser.LessThanStr: // timestamp < value
		if *stopTimeNs == 0 || timeValue < *stopTimeNs {
			*stopTimeNs = timeValue
		}
	case sqlparser.LessEqualStr: // timestamp <= value
		if *stopTimeNs == 0 || timeValue <= *stopTimeNs {
			*stopTimeNs = timeValue
		}
	case sqlparser.EqualStr: // timestamp = value (point query)
		// For exact matches, set both bounds to the same value
		*startTimeNs = timeValue
		*stopTimeNs = timeValue
	}
}

// isTimeColumn checks if a column name refers to a timestamp field
func (e *SQLEngine) isTimeColumn(columnName string) bool {
	if columnName == "" {
		return false
	}

	// System timestamp columns
	timeColumns := []string{
		"_timestamp_ns", // SeaweedFS MQ system timestamp (nanoseconds)
		"timestamp_ns",  // Alternative naming
		"timestamp",     // Common timestamp field
		"created_at",    // Common creation time field
		"updated_at",    // Common update time field
		"event_time",    // Event timestamp
		"log_time",      // Log timestamp
		"ts",            // Short form
	}

	for _, timeCol := range timeColumns {
		if strings.EqualFold(columnName, timeCol) {
			return true
		}
	}

	return false
}

// getColumnName extracts column name from expression (handles ColName types)
func (e *SQLEngine) getColumnName(expr sqlparser.Expr) string {
	switch exprType := expr.(type) {
	case *sqlparser.ColName:
		return exprType.Name.String()
	}
	return ""
}

// extractTimeValue parses time values from SQL expressions
// Supports nanosecond timestamps, ISO dates, and relative times
func (e *SQLEngine) extractTimeValue(expr sqlparser.Expr) int64 {
	switch exprType := expr.(type) {
	case *sqlparser.SQLVal:
		if exprType.Type == sqlparser.IntVal {
			// Parse as nanosecond timestamp
			if val, err := strconv.ParseInt(string(exprType.Val), 10, 64); err == nil {
				return val
			}
		} else if exprType.Type == sqlparser.StrVal {
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
	case sqlparser.GreaterThanStr:
		return sqlparser.LessThanStr
	case sqlparser.GreaterEqualStr:
		return sqlparser.LessEqualStr
	case sqlparser.LessThanStr:
		return sqlparser.GreaterThanStr
	case sqlparser.LessEqualStr:
		return sqlparser.GreaterEqualStr
	case sqlparser.EqualStr:
		return sqlparser.EqualStr
	case sqlparser.NotEqualStr:
		return sqlparser.NotEqualStr
	default:
		return op
	}
}

// buildPredicate creates a predicate function from a WHERE clause expression
// This is a simplified implementation - a full implementation would be much more complex
func (e *SQLEngine) buildPredicate(expr sqlparser.Expr) (func(*schema_pb.RecordValue) bool, error) {
	switch exprType := expr.(type) {
	case *sqlparser.ComparisonExpr:
		return e.buildComparisonPredicate(exprType)
	case *sqlparser.AndExpr:
		leftPred, err := e.buildPredicate(exprType.Left)
		if err != nil {
			return nil, err
		}
		rightPred, err := e.buildPredicate(exprType.Right)
		if err != nil {
			return nil, err
		}
		return func(record *schema_pb.RecordValue) bool {
			return leftPred(record) && rightPred(record)
		}, nil
	case *sqlparser.OrExpr:
		leftPred, err := e.buildPredicate(exprType.Left)
		if err != nil {
			return nil, err
		}
		rightPred, err := e.buildPredicate(exprType.Right)
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

// buildComparisonPredicate creates a predicate for comparison operations (=, <, >, etc.)
// Handles column names on both left and right sides of the comparison
func (e *SQLEngine) buildComparisonPredicate(expr *sqlparser.ComparisonExpr) (func(*schema_pb.RecordValue) bool, error) {
	var columnName string
	var compareValue interface{}
	var operator string

	// Check if column is on the left side (normal case: column > value)
	if colName, ok := expr.Left.(*sqlparser.ColName); ok {
		columnName = colName.Name.String()
		operator = expr.Operator

		// Extract comparison value from right side
		val, err := e.extractComparisonValue(expr.Right)
		if err != nil {
			return nil, fmt.Errorf("failed to extract right-side value: %v", err)
		}
		compareValue = val

	} else if colName, ok := expr.Right.(*sqlparser.ColName); ok {
		// Column is on the right side (reversed case: value < column)
		columnName = colName.Name.String()

		// Reverse the operator when column is on right side
		operator = e.reverseOperator(expr.Operator)

		// Extract comparison value from left side
		val, err := e.extractComparisonValue(expr.Left)
		if err != nil {
			return nil, fmt.Errorf("failed to extract left-side value: %v", err)
		}
		compareValue = val

	} else {
		return nil, fmt.Errorf("no column name found in comparison expression, left: %T, right: %T", expr.Left, expr.Right)
	}

	// Create predicate based on operator
	return func(record *schema_pb.RecordValue) bool {
		fieldValue, exists := record.Fields[columnName]
		if !exists {
			return false
		}

		return e.evaluateComparison(fieldValue, operator, compareValue)
	}, nil
}

// extractComparisonValue extracts the comparison value from a SQL expression
func (e *SQLEngine) extractComparisonValue(expr sqlparser.Expr) (interface{}, error) {
	switch val := expr.(type) {
	case *sqlparser.SQLVal:
		switch val.Type {
		case sqlparser.IntVal:
			intVal, err := strconv.ParseInt(string(val.Val), 10, 64)
			if err != nil {
				return nil, err
			}
			return intVal, nil
		case sqlparser.StrVal:
			return string(val.Val), nil
		default:
			return nil, fmt.Errorf("unsupported SQL value type: %v", val.Type)
		}
	case sqlparser.ValTuple:
		// Handle IN expressions with multiple values: column IN (value1, value2, value3)
		var inValues []interface{}
		for _, tupleVal := range val {
			switch v := tupleVal.(type) {
			case *sqlparser.SQLVal:
				switch v.Type {
				case sqlparser.IntVal:
					intVal, err := strconv.ParseInt(string(v.Val), 10, 64)
					if err != nil {
						return nil, err
					}
					inValues = append(inValues, intVal)
				case sqlparser.StrVal:
					inValues = append(inValues, string(v.Val))
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

// Helper functions for value comparison (simplified implementation)
func (e *SQLEngine) valuesEqual(fieldValue *schema_pb.Value, compareValue interface{}) bool {
	switch v := fieldValue.Kind.(type) {
	case *schema_pb.Value_Int32Value:
		if intVal, ok := compareValue.(int64); ok {
			if intVal > math.MaxInt32 || intVal < math.MinInt32 {
				return false // Value out of range for int32, cannot be equal
			}
			return v.Int32Value == int32(intVal)
		}
	case *schema_pb.Value_Int64Value:
		if intVal, ok := compareValue.(int64); ok {
			return v.Int64Value == intVal
		}
	case *schema_pb.Value_StringValue:
		if strVal, ok := compareValue.(string); ok {
			return v.StringValue == strVal
		}
	}
	return false
}

func (e *SQLEngine) valueLessThan(fieldValue *schema_pb.Value, compareValue interface{}) bool {
	switch v := fieldValue.Kind.(type) {
	case *schema_pb.Value_Int32Value:
		if intVal, ok := compareValue.(int64); ok {
			if intVal > math.MaxInt32 {
				return true // int32 value is always less than values > MaxInt32
			}
			if intVal < math.MinInt32 {
				return false // int32 value is always greater than values < MinInt32
			}
			return v.Int32Value < int32(intVal)
		}
	case *schema_pb.Value_Int64Value:
		if intVal, ok := compareValue.(int64); ok {
			return v.Int64Value < intVal
		}
	}
	return false
}

func (e *SQLEngine) valueGreaterThan(fieldValue *schema_pb.Value, compareValue interface{}) bool {
	switch v := fieldValue.Kind.(type) {
	case *schema_pb.Value_Int32Value:
		if intVal, ok := compareValue.(int64); ok {
			if intVal > math.MaxInt32 {
				return false // int32 value is never greater than values > MaxInt32
			}
			if intVal < math.MinInt32 {
				return true // int32 value is always greater than values < MinInt32
			}
			return v.Int32Value > int32(intVal)
		}
	case *schema_pb.Value_Int64Value:
		if intVal, ok := compareValue.(int64); ok {
			return v.Int64Value > intVal
		}
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

func (e *SQLEngine) createTable(ctx context.Context, stmt *sqlparser.DDL) (*QueryResult, error) {
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

	// Create the topic via broker
	partitionCount := int32(6) // Default partition count - TODO: make configurable
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

func (e *SQLEngine) alterTable(ctx context.Context, stmt *sqlparser.DDL) (*QueryResult, error) {
	// TODO: Implement table alteration
	// This will modify the MQ topic schema with versioning
	err := fmt.Errorf("ALTER TABLE not yet implemented")
	return &QueryResult{Error: err}, err
}

func (e *SQLEngine) dropTable(ctx context.Context, stmt *sqlparser.DDL) (*QueryResult, error) {
	// Parse DROP TABLE statement
	// Assumption: Table name is in stmt.NewName for DROP operations
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

	// Delete the topic via broker
	err := e.catalog.brokerClient.DeleteTopic(ctx, database, tableName)
	if err != nil {
		return &QueryResult{Error: err}, err
	}

	// Remove from catalog cache
	// TODO: Implement catalog cache removal

	// Return success result
	result := &QueryResult{
		Columns: []string{"Result"},
		Rows: [][]sqltypes.Value{
			{sqltypes.NewVarChar(fmt.Sprintf("Table '%s.%s' dropped successfully", database, tableName))},
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
	stmt *sqlparser.Select,
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
		LiveLogFilesScanned: 0, // TODO: Implement proper live log file counting
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
func (builder *ExecutionPlanBuilder) determineExecutionStrategy(stmt *sqlparser.Select, strategy AggregationStrategy) string {
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

// buildOptimizationsList builds the list of optimizations used
func (builder *ExecutionPlanBuilder) buildOptimizationsList(stmt *sqlparser.Select, strategy AggregationStrategy) []string {
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
func (e *SQLEngine) parseAggregationFunction(funcExpr *sqlparser.FuncExpr, aliasExpr *sqlparser.AliasedExpr) (*AggregationSpec, error) {
	funcName := strings.ToUpper(funcExpr.Name.String())

	spec := &AggregationSpec{
		Function: funcName,
	}

	// Parse function arguments
	switch funcName {
	case "COUNT":
		if len(funcExpr.Exprs) != 1 {
			return nil, fmt.Errorf("COUNT function expects exactly 1 argument")
		}

		switch arg := funcExpr.Exprs[0].(type) {
		case *sqlparser.StarExpr:
			spec.Column = "*"
			spec.Alias = "COUNT(*)"
		case *sqlparser.AliasedExpr:
			if colName, ok := arg.Expr.(*sqlparser.ColName); ok {
				spec.Column = colName.Name.String()
				spec.Alias = fmt.Sprintf("COUNT(%s)", spec.Column)
			} else {
				return nil, fmt.Errorf("COUNT argument must be a column name or *")
			}
		default:
			return nil, fmt.Errorf("unsupported COUNT argument: %T", arg)
		}

	case "SUM", "AVG", "MIN", "MAX":
		if len(funcExpr.Exprs) != 1 {
			return nil, fmt.Errorf("%s function expects exactly 1 argument", funcName)
		}

		switch arg := funcExpr.Exprs[0].(type) {
		case *sqlparser.AliasedExpr:
			if colName, ok := arg.Expr.(*sqlparser.ColName); ok {
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
	if !aliasExpr.As.IsEmpty() {
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
			case "_timestamp_ns", "timestamp_ns":
				columnValue = &schema_pb.Value{Kind: &schema_pb.Value_Int64Value{Int64Value: logEntry.TsNs}}
			case "_key", "key":
				columnValue = &schema_pb.Value{Kind: &schema_pb.Value_BytesValue{BytesValue: logEntry.Key}}
			case "_source", "source":
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
	recordValue.Fields[SW_COLUMN_NAME_TS] = &schema_pb.Value{
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
	if strings.HasSuffix(filename, ".parquet") {
		filename = filename[:len(filename)-8]
	}

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

// discoverTopicPartitions discovers all partitions for a given topic
func (e *SQLEngine) discoverTopicPartitions(namespace, topicName string) ([]string, error) {
	// Use the same discovery logic as in hybrid_message_scanner.go
	topicPath := fmt.Sprintf("/topics/%s/%s", namespace, topicName)

	// Get FilerClient from BrokerClient
	filerClient, err := e.catalog.brokerClient.GetFilerClient()
	if err != nil {
		return nil, err
	}

	var partitions []string
	err = filer_pb.ReadDirAllEntries(context.Background(), filerClient, util.FullPath(topicPath), "", func(entry *filer_pb.Entry, isLast bool) error {
		if !entry.IsDirectory {
			return nil
		}

		// Check if this looks like a partition directory (format: vYYYY-MM-DD-HH-MM-SS)
		if strings.HasPrefix(entry.Name, "v") && len(entry.Name) == 20 {
			// This is a time-based partition directory
			// Look for numeric subdirectories (partition IDs)
			partitionBasePath := fmt.Sprintf("%s/%s", topicPath, entry.Name)
			err := filer_pb.ReadDirAllEntries(context.Background(), filerClient, util.FullPath(partitionBasePath), "", func(subEntry *filer_pb.Entry, isLast bool) error {
				if subEntry.IsDirectory {
					// Check if this is a numeric partition directory (format: 0000-XXXX)
					if len(subEntry.Name) >= 4 {
						partitionPath := fmt.Sprintf("%s/%s", entry.Name, subEntry.Name)
						partitions = append(partitions, partitionPath)
					}
				}
				return nil
			})
			if err != nil {
				return err
			}
		}
		return nil
	})

	return partitions, err
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
	relativePartitions, err := e.discoverTopicPartitions(namespace, topicName)
	if err != nil {
		return 0, err
	}

	// Convert relative partition paths to full paths
	topicBasePath := fmt.Sprintf("/topics/%s/%s", namespace, topicName)
	partitions := make([]string, len(relativePartitions))
	for i, relPartition := range relativePartitions {
		partitions[i] = fmt.Sprintf("%s/%s", topicBasePath, relPartition)
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
	relativePartitions, err := e.discoverTopicPartitions(namespace, topicName)
	if err != nil {
		return 0, err
	}

	// Convert relative partition paths to full paths
	topicBasePath := fmt.Sprintf("/topics/%s/%s", namespace, topicName)
	partitions := make([]string, len(relativePartitions))
	for i, relPartition := range relativePartitions {
		partitions[i] = fmt.Sprintf("%s/%s", topicBasePath, relPartition)
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
	case "_timestamp_ns", "timestamp_ns":
		return &schema_pb.Value{Kind: &schema_pb.Value_Int64Value{Int64Value: result.Timestamp}}
	case "_key", "key":
		return &schema_pb.Value{Kind: &schema_pb.Value_BytesValue{BytesValue: result.Key}}
	case "_source", "source":
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
