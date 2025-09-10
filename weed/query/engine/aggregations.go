package engine

import (
	"context"
	"fmt"
	"math"
	"strconv"
	"strings"

	"github.com/seaweedfs/seaweedfs/weed/mq/topic"
	"github.com/seaweedfs/seaweedfs/weed/pb/schema_pb"
	"github.com/seaweedfs/seaweedfs/weed/query/sqltypes"
)

// AggregationSpec defines an aggregation function to be computed
type AggregationSpec struct {
	Function string // COUNT, SUM, AVG, MIN, MAX
	Column   string // Column name, or "*" for COUNT(*)
	Alias    string // Optional alias for the result column
	Distinct bool   // Support for DISTINCT keyword
}

// AggregationResult holds the computed result of an aggregation
type AggregationResult struct {
	Count int64
	Sum   float64
	Min   interface{}
	Max   interface{}
}

// AggregationStrategy represents the strategy for executing aggregations
type AggregationStrategy struct {
	CanUseFastPath   bool
	Reason           string
	UnsupportedSpecs []AggregationSpec
}

// TopicDataSources represents the data sources available for a topic
type TopicDataSources struct {
	ParquetFiles         map[string][]*ParquetFileStats // partitionPath -> parquet file stats
	ParquetRowCount      int64
	LiveLogRowCount      int64
	LiveLogFilesCount    int // Total count of live log files across all partitions
	PartitionsCount      int
	BrokerUnflushedCount int64
}

// FastPathOptimizer handles fast path aggregation optimization decisions
type FastPathOptimizer struct {
	engine *SQLEngine
}

// NewFastPathOptimizer creates a new fast path optimizer
func NewFastPathOptimizer(engine *SQLEngine) *FastPathOptimizer {
	return &FastPathOptimizer{engine: engine}
}

// DetermineStrategy analyzes aggregations and determines if fast path can be used
func (opt *FastPathOptimizer) DetermineStrategy(aggregations []AggregationSpec) AggregationStrategy {
	strategy := AggregationStrategy{
		CanUseFastPath:   true,
		Reason:           "all_aggregations_supported",
		UnsupportedSpecs: []AggregationSpec{},
	}

	for _, spec := range aggregations {
		if !opt.engine.canUseParquetStatsForAggregation(spec) {
			strategy.CanUseFastPath = false
			strategy.Reason = "unsupported_aggregation_functions"
			strategy.UnsupportedSpecs = append(strategy.UnsupportedSpecs, spec)
		}
	}

	return strategy
}

// CollectDataSources gathers information about available data sources for a topic
func (opt *FastPathOptimizer) CollectDataSources(ctx context.Context, hybridScanner *HybridMessageScanner) (*TopicDataSources, error) {
	return opt.CollectDataSourcesWithTimeFilter(ctx, hybridScanner, 0, 0)
}

// CollectDataSourcesWithTimeFilter gathers information about available data sources for a topic
// with optional time filtering to skip irrelevant parquet files
func (opt *FastPathOptimizer) CollectDataSourcesWithTimeFilter(ctx context.Context, hybridScanner *HybridMessageScanner, startTimeNs, stopTimeNs int64) (*TopicDataSources, error) {
	dataSources := &TopicDataSources{
		ParquetFiles:      make(map[string][]*ParquetFileStats),
		ParquetRowCount:   0,
		LiveLogRowCount:   0,
		LiveLogFilesCount: 0,
		PartitionsCount:   0,
	}

	if isDebugMode(ctx) {
		fmt.Printf("Collecting data sources for: %s/%s\n", hybridScanner.topic.Namespace, hybridScanner.topic.Name)
	}

	// Discover partitions for the topic
	partitionPaths, err := opt.engine.discoverTopicPartitions(hybridScanner.topic.Namespace, hybridScanner.topic.Name)
	if err != nil {
		if isDebugMode(ctx) {
			fmt.Printf("ERROR: Partition discovery failed: %v\n", err)
		}
		return dataSources, DataSourceError{
			Source: "partition_discovery",
			Cause:  err,
		}
	}

	// DEBUG: Log discovered partitions
	if isDebugMode(ctx) {
		fmt.Printf("Discovered %d partitions: %v\n", len(partitionPaths), partitionPaths)
	}

	// Collect stats from each partition
	// Note: discoverTopicPartitions always returns absolute paths starting with "/topics/"
	for _, partitionPath := range partitionPaths {
		if isDebugMode(ctx) {
			fmt.Printf("\nProcessing partition: %s\n", partitionPath)
		}

		// Read parquet file statistics
		parquetStats, err := hybridScanner.ReadParquetStatistics(partitionPath)
		if err != nil {
			if isDebugMode(ctx) {
				fmt.Printf("  ERROR: Failed to read parquet statistics: %v\n", err)
			}
		} else if len(parquetStats) == 0 {
			if isDebugMode(ctx) {
				fmt.Printf("  No parquet files found in partition\n")
			}
		} else {
			// Prune by time range using parquet column statistics
			filtered := pruneParquetFilesByTime(ctx, parquetStats, hybridScanner, startTimeNs, stopTimeNs)
			dataSources.ParquetFiles[partitionPath] = filtered
			partitionParquetRows := int64(0)
			for _, stat := range filtered {
				partitionParquetRows += stat.RowCount
				dataSources.ParquetRowCount += stat.RowCount
			}
			if isDebugMode(ctx) {
				fmt.Printf("  Found %d parquet files with %d total rows\n", len(filtered), partitionParquetRows)
			}
		}

		// Count live log files (excluding those converted to parquet)
		parquetSources := opt.engine.extractParquetSourceFiles(dataSources.ParquetFiles[partitionPath])
		liveLogCount, liveLogErr := opt.engine.countLiveLogRowsExcludingParquetSources(ctx, partitionPath, parquetSources)
		if liveLogErr != nil {
			if isDebugMode(ctx) {
				fmt.Printf("  ERROR: Failed to count live log rows: %v\n", liveLogErr)
			}
		} else {
			dataSources.LiveLogRowCount += liveLogCount
			if isDebugMode(ctx) {
				fmt.Printf("  Found %d live log rows (excluding %d parquet sources)\n", liveLogCount, len(parquetSources))
			}
		}

		// Count live log files for partition with proper range values
		// Extract partition name from absolute path (e.g., "0000-2520" from "/topics/.../v2025.../0000-2520")
		partitionName := partitionPath[strings.LastIndex(partitionPath, "/")+1:]
		partitionParts := strings.Split(partitionName, "-")
		if len(partitionParts) == 2 {
			rangeStart, err1 := strconv.Atoi(partitionParts[0])
			rangeStop, err2 := strconv.Atoi(partitionParts[1])
			if err1 == nil && err2 == nil {
				partition := topic.Partition{
					RangeStart: int32(rangeStart),
					RangeStop:  int32(rangeStop),
				}
				liveLogFileCount, err := hybridScanner.countLiveLogFiles(partition)
				if err == nil {
					dataSources.LiveLogFilesCount += liveLogFileCount
				}

				// Count broker unflushed messages for this partition
				if hybridScanner.brokerClient != nil {
					entries, err := hybridScanner.brokerClient.GetUnflushedMessages(ctx, hybridScanner.topic.Namespace, hybridScanner.topic.Name, partition, 0)
					if err == nil {
						dataSources.BrokerUnflushedCount += int64(len(entries))
						if isDebugMode(ctx) {
							fmt.Printf("  Found %d unflushed broker messages\n", len(entries))
						}
					} else if isDebugMode(ctx) {
						fmt.Printf("  ERROR: Failed to get unflushed broker messages: %v\n", err)
					}
				}
			}
		}
	}

	dataSources.PartitionsCount = len(partitionPaths)

	if isDebugMode(ctx) {
		fmt.Printf("Data sources collected: %d partitions, %d parquet rows, %d live log rows, %d broker buffer rows\n",
			dataSources.PartitionsCount, dataSources.ParquetRowCount, dataSources.LiveLogRowCount, dataSources.BrokerUnflushedCount)
	}

	return dataSources, nil
}

// AggregationComputer handles the computation of aggregations using fast path
type AggregationComputer struct {
	engine *SQLEngine
}

// NewAggregationComputer creates a new aggregation computer
func NewAggregationComputer(engine *SQLEngine) *AggregationComputer {
	return &AggregationComputer{engine: engine}
}

// ComputeFastPathAggregations computes aggregations using parquet statistics and live log data
func (comp *AggregationComputer) ComputeFastPathAggregations(
	ctx context.Context,
	aggregations []AggregationSpec,
	dataSources *TopicDataSources,
	partitions []string,
) ([]AggregationResult, error) {

	aggResults := make([]AggregationResult, len(aggregations))

	for i, spec := range aggregations {
		switch spec.Function {
		case FuncCOUNT:
			if spec.Column == "*" {
				aggResults[i].Count = dataSources.ParquetRowCount + dataSources.LiveLogRowCount + dataSources.BrokerUnflushedCount
			} else {
				// For specific columns, we might need to account for NULLs in the future
				aggResults[i].Count = dataSources.ParquetRowCount + dataSources.LiveLogRowCount + dataSources.BrokerUnflushedCount
			}

		case FuncMIN:
			globalMin, err := comp.computeGlobalMin(spec, dataSources, partitions)
			if err != nil {
				return nil, AggregationError{
					Operation: spec.Function,
					Column:    spec.Column,
					Cause:     err,
				}
			}
			aggResults[i].Min = globalMin

		case FuncMAX:
			globalMax, err := comp.computeGlobalMax(spec, dataSources, partitions)
			if err != nil {
				return nil, AggregationError{
					Operation: spec.Function,
					Column:    spec.Column,
					Cause:     err,
				}
			}
			aggResults[i].Max = globalMax

		default:
			return nil, OptimizationError{
				Strategy: "fast_path_aggregation",
				Reason:   fmt.Sprintf("unsupported aggregation function: %s", spec.Function),
			}
		}
	}

	return aggResults, nil
}

// computeGlobalMin computes the global minimum value across all data sources
func (comp *AggregationComputer) computeGlobalMin(spec AggregationSpec, dataSources *TopicDataSources, partitions []string) (interface{}, error) {
	var globalMin interface{}
	var globalMinValue *schema_pb.Value
	hasParquetStats := false

	// Step 1: Get minimum from parquet statistics
	for _, fileStats := range dataSources.ParquetFiles {
		for _, fileStat := range fileStats {
			// Try case-insensitive column lookup
			var colStats *ParquetColumnStats
			var found bool

			// First try exact match
			if stats, exists := fileStat.ColumnStats[spec.Column]; exists {
				colStats = stats
				found = true
			} else {
				// Try case-insensitive lookup
				for colName, stats := range fileStat.ColumnStats {
					if strings.EqualFold(colName, spec.Column) {
						colStats = stats
						found = true
						break
					}
				}
			}

			if found && colStats != nil && colStats.MinValue != nil {
				if globalMinValue == nil || comp.engine.compareValues(colStats.MinValue, globalMinValue) < 0 {
					globalMinValue = colStats.MinValue
					extractedValue := comp.engine.extractRawValue(colStats.MinValue)
					if extractedValue != nil {
						globalMin = extractedValue
						hasParquetStats = true
					}
				}
			}
		}
	}

	// Step 2: Get minimum from live log data (only if no live logs or if we need to compare)
	if dataSources.LiveLogRowCount > 0 {
		for _, partition := range partitions {
			partitionParquetSources := make(map[string]bool)
			if partitionFileStats, exists := dataSources.ParquetFiles[partition]; exists {
				partitionParquetSources = comp.engine.extractParquetSourceFiles(partitionFileStats)
			}

			liveLogMin, _, err := comp.engine.computeLiveLogMinMax(partition, spec.Column, partitionParquetSources)
			if err != nil {
				continue // Skip partitions with errors
			}

			if liveLogMin != nil {
				if globalMin == nil {
					globalMin = liveLogMin
				} else {
					liveLogSchemaValue := comp.engine.convertRawValueToSchemaValue(liveLogMin)
					if liveLogSchemaValue != nil && comp.engine.compareValues(liveLogSchemaValue, globalMinValue) < 0 {
						globalMin = liveLogMin
						globalMinValue = liveLogSchemaValue
					}
				}
			}
		}
	}

	// Step 3: Handle system columns if no regular data found
	if globalMin == nil && !hasParquetStats {
		globalMin = comp.engine.getSystemColumnGlobalMin(spec.Column, dataSources.ParquetFiles)
	}

	return globalMin, nil
}

// computeGlobalMax computes the global maximum value across all data sources
func (comp *AggregationComputer) computeGlobalMax(spec AggregationSpec, dataSources *TopicDataSources, partitions []string) (interface{}, error) {
	var globalMax interface{}
	var globalMaxValue *schema_pb.Value
	hasParquetStats := false

	// Step 1: Get maximum from parquet statistics
	for _, fileStats := range dataSources.ParquetFiles {
		for _, fileStat := range fileStats {
			// Try case-insensitive column lookup
			var colStats *ParquetColumnStats
			var found bool

			// First try exact match
			if stats, exists := fileStat.ColumnStats[spec.Column]; exists {
				colStats = stats
				found = true
			} else {
				// Try case-insensitive lookup
				for colName, stats := range fileStat.ColumnStats {
					if strings.EqualFold(colName, spec.Column) {
						colStats = stats
						found = true
						break
					}
				}
			}

			if found && colStats != nil && colStats.MaxValue != nil {
				if globalMaxValue == nil || comp.engine.compareValues(colStats.MaxValue, globalMaxValue) > 0 {
					globalMaxValue = colStats.MaxValue
					extractedValue := comp.engine.extractRawValue(colStats.MaxValue)
					if extractedValue != nil {
						globalMax = extractedValue
						hasParquetStats = true
					}
				}
			}
		}
	}

	// Step 2: Get maximum from live log data (only if live logs exist)
	if dataSources.LiveLogRowCount > 0 {
		for _, partition := range partitions {
			partitionParquetSources := make(map[string]bool)
			if partitionFileStats, exists := dataSources.ParquetFiles[partition]; exists {
				partitionParquetSources = comp.engine.extractParquetSourceFiles(partitionFileStats)
			}

			_, liveLogMax, err := comp.engine.computeLiveLogMinMax(partition, spec.Column, partitionParquetSources)
			if err != nil {
				continue // Skip partitions with errors
			}

			if liveLogMax != nil {
				if globalMax == nil {
					globalMax = liveLogMax
				} else {
					liveLogSchemaValue := comp.engine.convertRawValueToSchemaValue(liveLogMax)
					if liveLogSchemaValue != nil && comp.engine.compareValues(liveLogSchemaValue, globalMaxValue) > 0 {
						globalMax = liveLogMax
						globalMaxValue = liveLogSchemaValue
					}
				}
			}
		}
	}

	// Step 3: Handle system columns if no regular data found
	if globalMax == nil && !hasParquetStats {
		globalMax = comp.engine.getSystemColumnGlobalMax(spec.Column, dataSources.ParquetFiles)
	}

	return globalMax, nil
}

// executeAggregationQuery handles SELECT queries with aggregation functions
func (e *SQLEngine) executeAggregationQuery(ctx context.Context, hybridScanner *HybridMessageScanner, aggregations []AggregationSpec, stmt *SelectStatement) (*QueryResult, error) {
	return e.executeAggregationQueryWithPlan(ctx, hybridScanner, aggregations, stmt, nil)
}

// executeAggregationQueryWithPlan handles SELECT queries with aggregation functions and populates execution plan
func (e *SQLEngine) executeAggregationQueryWithPlan(ctx context.Context, hybridScanner *HybridMessageScanner, aggregations []AggregationSpec, stmt *SelectStatement, plan *QueryExecutionPlan) (*QueryResult, error) {
	// Parse LIMIT and OFFSET for aggregation results (do this first)
	// Use -1 to distinguish "no LIMIT" from "LIMIT 0"
	limit := -1
	offset := 0
	if stmt.Limit != nil && stmt.Limit.Rowcount != nil {
		if limitExpr, ok := stmt.Limit.Rowcount.(*SQLVal); ok && limitExpr.Type == IntVal {
			if limit64, err := strconv.ParseInt(string(limitExpr.Val), 10, 64); err == nil {
				if limit64 > int64(math.MaxInt) || limit64 < 0 {
					return nil, fmt.Errorf("LIMIT value %d is out of range", limit64)
				}
				// Safe conversion after bounds check
				limit = int(limit64)
			}
		}
	}
	if stmt.Limit != nil && stmt.Limit.Offset != nil {
		if offsetExpr, ok := stmt.Limit.Offset.(*SQLVal); ok && offsetExpr.Type == IntVal {
			if offset64, err := strconv.ParseInt(string(offsetExpr.Val), 10, 64); err == nil {
				if offset64 > int64(math.MaxInt) || offset64 < 0 {
					return nil, fmt.Errorf("OFFSET value %d is out of range", offset64)
				}
				// Safe conversion after bounds check
				offset = int(offset64)
			}
		}
	}

	// Parse WHERE clause for filtering
	var predicate func(*schema_pb.RecordValue) bool
	var err error
	if stmt.Where != nil {
		predicate, err = e.buildPredicate(stmt.Where.Expr)
		if err != nil {
			return &QueryResult{Error: err}, err
		}
	}

	// Extract time filters and validate that WHERE clause contains only time-based predicates
	startTimeNs, stopTimeNs := int64(0), int64(0)
	onlyTimePredicates := true
	if stmt.Where != nil {
		startTimeNs, stopTimeNs, onlyTimePredicates = e.extractTimeFiltersWithValidation(stmt.Where.Expr)
	}

	// FAST PATH WITH TIME-BASED OPTIMIZATION:
	// Allow fast path only for queries without WHERE clause or with time-only WHERE clauses
	// This prevents incorrect results when non-time predicates are present
	canAttemptFastPath := stmt.Where == nil || onlyTimePredicates

	if canAttemptFastPath {
		if isDebugMode(ctx) {
			if stmt.Where == nil {
				fmt.Printf("\nFast path optimization attempt (no WHERE clause)...\n")
			} else {
				fmt.Printf("\nFast path optimization attempt (time-only WHERE clause)...\n")
			}
		}
		fastResult, canOptimize := e.tryFastParquetAggregationWithPlan(ctx, hybridScanner, aggregations, plan, startTimeNs, stopTimeNs, stmt)
		if canOptimize {
			if isDebugMode(ctx) {
				fmt.Printf("Fast path optimization succeeded!\n")
			}
			return fastResult, nil
		} else {
			if isDebugMode(ctx) {
				fmt.Printf("Fast path optimization failed, falling back to slow path\n")
			}
		}
	} else {
		if isDebugMode(ctx) {
			fmt.Printf("Fast path not applicable due to complex WHERE clause\n")
		}
	}

	// SLOW PATH: Fall back to full table scan
	if isDebugMode(ctx) {
		fmt.Printf("Using full table scan for aggregation (parquet optimization not applicable)\n")
	}

	// Extract columns needed for aggregations
	columnsNeeded := make(map[string]bool)
	for _, spec := range aggregations {
		if spec.Column != "*" {
			columnsNeeded[spec.Column] = true
		}
	}

	// Convert to slice
	var scanColumns []string
	if len(columnsNeeded) > 0 {
		scanColumns = make([]string, 0, len(columnsNeeded))
		for col := range columnsNeeded {
			scanColumns = append(scanColumns, col)
		}
	}
	// If no specific columns needed (COUNT(*) only), don't specify columns (scan all)

	// Build scan options for full table scan (aggregations need all data during scanning)
	hybridScanOptions := HybridScanOptions{
		StartTimeNs: startTimeNs,
		StopTimeNs:  stopTimeNs,
		Limit:       -1, // Use -1 to mean "no limit" - need all data for aggregation
		Offset:      0,  // No offset during scanning - OFFSET applies to final results
		Predicate:   predicate,
		Columns:     scanColumns, // Include columns needed for aggregation functions
	}

	// DEBUG: Log scan options for aggregation
	debugHybridScanOptions(ctx, hybridScanOptions, "AGGREGATION")

	// Execute the hybrid scan to get all matching records
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

	// DEBUG: Log scan results
	if isDebugMode(ctx) {
		fmt.Printf("AGGREGATION SCAN RESULTS: %d rows returned\n", len(results))
	}

	// Compute aggregations
	aggResults := e.computeAggregations(results, aggregations)

	// Build result set
	columns := make([]string, len(aggregations))
	row := make([]sqltypes.Value, len(aggregations))

	for i, spec := range aggregations {
		columns[i] = spec.Alias
		row[i] = e.formatAggregationResult(spec, aggResults[i])
	}

	// Apply OFFSET and LIMIT to aggregation results
	// Limit semantics: -1 = no limit, 0 = LIMIT 0 (empty), >0 = limit to N rows
	rows := [][]sqltypes.Value{row}
	if offset > 0 || limit >= 0 {
		// Handle LIMIT 0 first
		if limit == 0 {
			rows = [][]sqltypes.Value{}
		} else {
			// Apply OFFSET first
			if offset > 0 {
				if offset >= len(rows) {
					rows = [][]sqltypes.Value{}
				} else {
					rows = rows[offset:]
				}
			}

			// Apply LIMIT after OFFSET (only if limit > 0)
			if limit > 0 && len(rows) > limit {
				rows = rows[:limit]
			}
		}
	}

	result := &QueryResult{
		Columns: columns,
		Rows:    rows,
	}

	// Build execution tree for aggregation queries if plan is provided
	if plan != nil {
		// Populate detailed plan information for full scan (similar to fast path)
		e.populateFullScanPlanDetails(ctx, plan, hybridScanner, stmt)
		plan.RootNode = e.buildExecutionTree(plan, stmt)
	}

	return result, nil
}

// populateFullScanPlanDetails populates detailed plan information for full scan queries
// This provides consistency with fast path execution plan details
func (e *SQLEngine) populateFullScanPlanDetails(ctx context.Context, plan *QueryExecutionPlan, hybridScanner *HybridMessageScanner, stmt *SelectStatement) {
	// plan.Details is initialized at the start of the SELECT execution

	// Extract table information
	var database, tableName string
	if len(stmt.From) == 1 {
		if table, ok := stmt.From[0].(*AliasedTableExpr); ok {
			if tableExpr, ok := table.Expr.(TableName); ok {
				tableName = tableExpr.Name.String()
				if tableExpr.Qualifier != nil && tableExpr.Qualifier.String() != "" {
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

	// Discover partitions and populate file details
	if partitions, discoverErr := e.discoverTopicPartitions(database, tableName); discoverErr == nil {
		// Add partition paths to execution plan details
		plan.Details["partition_paths"] = partitions

		// Populate detailed file information using shared helper
		e.populatePlanFileDetails(ctx, plan, hybridScanner, partitions, stmt)
	} else {
		// Record discovery error to plan for better diagnostics
		plan.Details["error_partition_discovery"] = discoverErr.Error()
	}
}

// tryFastParquetAggregation attempts to compute aggregations using hybrid approach:
// - Use parquet metadata for parquet files
// - Count live log files for live data
// - Combine both for accurate results per partition
// Returns (result, canOptimize) where canOptimize=true means the hybrid fast path was used
func (e *SQLEngine) tryFastParquetAggregation(ctx context.Context, hybridScanner *HybridMessageScanner, aggregations []AggregationSpec) (*QueryResult, bool) {
	return e.tryFastParquetAggregationWithPlan(ctx, hybridScanner, aggregations, nil, 0, 0, nil)
}

// tryFastParquetAggregationWithPlan is the same as tryFastParquetAggregation but also populates execution plan if provided
// startTimeNs, stopTimeNs: optional time range filters for parquet file optimization (0 means no filtering)
// stmt: SELECT statement for column statistics pruning optimization (can be nil)
func (e *SQLEngine) tryFastParquetAggregationWithPlan(ctx context.Context, hybridScanner *HybridMessageScanner, aggregations []AggregationSpec, plan *QueryExecutionPlan, startTimeNs, stopTimeNs int64, stmt *SelectStatement) (*QueryResult, bool) {
	// Use the new modular components
	optimizer := NewFastPathOptimizer(e)
	computer := NewAggregationComputer(e)

	// Step 1: Determine strategy
	strategy := optimizer.DetermineStrategy(aggregations)
	if !strategy.CanUseFastPath {
		return nil, false
	}

	// Step 2: Collect data sources with time filtering for parquet file optimization
	dataSources, err := optimizer.CollectDataSourcesWithTimeFilter(ctx, hybridScanner, startTimeNs, stopTimeNs)
	if err != nil {
		return nil, false
	}

	// Build partition list for aggregation computer
	// Note: discoverTopicPartitions always returns absolute paths
	partitions, err := e.discoverTopicPartitions(hybridScanner.topic.Namespace, hybridScanner.topic.Name)
	if err != nil {
		return nil, false
	}

	// Debug: Show the hybrid optimization results (only in explain mode)
	if isDebugMode(ctx) && (dataSources.ParquetRowCount > 0 || dataSources.LiveLogRowCount > 0 || dataSources.BrokerUnflushedCount > 0) {
		partitionsWithLiveLogs := 0
		if dataSources.LiveLogRowCount > 0 || dataSources.BrokerUnflushedCount > 0 {
			partitionsWithLiveLogs = 1 // Simplified for now
		}
		fmt.Printf("Hybrid fast aggregation with deduplication: %d parquet rows + %d deduplicated live log rows + %d broker buffer rows from %d partitions\n",
			dataSources.ParquetRowCount, dataSources.LiveLogRowCount, dataSources.BrokerUnflushedCount, partitionsWithLiveLogs)
	}

	// Step 3: Compute aggregations using fast path
	aggResults, err := computer.ComputeFastPathAggregations(ctx, aggregations, dataSources, partitions)
	if err != nil {
		return nil, false
	}

	// Step 3.5: Validate fast path results (safety check)
	// For simple COUNT(*) queries, ensure we got a reasonable result
	if len(aggregations) == 1 && aggregations[0].Function == FuncCOUNT && aggregations[0].Column == "*" {
		totalRows := dataSources.ParquetRowCount + dataSources.LiveLogRowCount + dataSources.BrokerUnflushedCount
		countResult := aggResults[0].Count

		if isDebugMode(ctx) {
			fmt.Printf("Validating fast path: COUNT=%d, Sources=%d\n", countResult, totalRows)
		}

		if totalRows == 0 && countResult > 0 {
			// Fast path found data but data sources show 0 - this suggests a bug
			if isDebugMode(ctx) {
				fmt.Printf("Fast path validation failed: COUNT=%d but sources=0\n", countResult)
			}
			return nil, false
		}
		if totalRows > 0 && countResult == 0 {
			// Data sources show data but COUNT is 0 - this also suggests a bug
			if isDebugMode(ctx) {
				fmt.Printf("Fast path validation failed: sources=%d but COUNT=0\n", totalRows)
			}
			return nil, false
		}
		if countResult != totalRows {
			// Counts don't match - this suggests inconsistent logic
			if isDebugMode(ctx) {
				fmt.Printf("Fast path validation failed: COUNT=%d != sources=%d\n", countResult, totalRows)
			}
			return nil, false
		}
		if isDebugMode(ctx) {
			fmt.Printf("Fast path validation passed: COUNT=%d\n", countResult)
		}
	}

	// Step 4: Populate execution plan if provided (for EXPLAIN queries)
	if plan != nil {
		strategy := optimizer.DetermineStrategy(aggregations)
		builder := &ExecutionPlanBuilder{}

		// Create a minimal SELECT statement for the plan builder (avoid nil pointer)
		stmt := &SelectStatement{}

		// Build aggregation plan with fast path strategy
		aggPlan := builder.BuildAggregationPlan(stmt, aggregations, strategy, dataSources)

		// Copy relevant fields to the main plan
		plan.ExecutionStrategy = aggPlan.ExecutionStrategy
		plan.DataSources = aggPlan.DataSources
		plan.OptimizationsUsed = aggPlan.OptimizationsUsed
		plan.PartitionsScanned = aggPlan.PartitionsScanned
		plan.ParquetFilesScanned = aggPlan.ParquetFilesScanned
		plan.LiveLogFilesScanned = aggPlan.LiveLogFilesScanned
		plan.TotalRowsProcessed = aggPlan.TotalRowsProcessed
		plan.Aggregations = aggPlan.Aggregations

		// Indicate broker buffer participation for EXPLAIN tree rendering
		if dataSources.BrokerUnflushedCount > 0 {
			plan.BrokerBufferQueried = true
			plan.BrokerBufferMessages = int(dataSources.BrokerUnflushedCount)
		}

		// Merge details while preserving existing ones
		for key, value := range aggPlan.Details {
			plan.Details[key] = value
		}

		// Add file path information from the data collection
		plan.Details["partition_paths"] = partitions

		// Populate detailed file information using shared helper, including time filters for pruning
		plan.Details[PlanDetailStartTimeNs] = startTimeNs
		plan.Details[PlanDetailStopTimeNs] = stopTimeNs
		e.populatePlanFileDetails(ctx, plan, hybridScanner, partitions, stmt)

		// Update counts to match discovered live log files
		if liveLogFiles, ok := plan.Details["live_log_files"].([]string); ok {
			dataSources.LiveLogFilesCount = len(liveLogFiles)
			plan.LiveLogFilesScanned = len(liveLogFiles)
		}

		// Ensure PartitionsScanned is set so Statistics section appears
		if plan.PartitionsScanned == 0 && len(partitions) > 0 {
			plan.PartitionsScanned = len(partitions)
		}

		if isDebugMode(ctx) {
			fmt.Printf("Populated execution plan with fast path strategy\n")
		}
	}

	// Step 5: Build final query result
	columns := make([]string, len(aggregations))
	row := make([]sqltypes.Value, len(aggregations))

	for i, spec := range aggregations {
		columns[i] = spec.Alias
		row[i] = e.formatAggregationResult(spec, aggResults[i])
	}

	result := &QueryResult{
		Columns: columns,
		Rows:    [][]sqltypes.Value{row},
	}

	return result, true
}

// computeAggregations computes aggregation results from a full table scan
func (e *SQLEngine) computeAggregations(results []HybridScanResult, aggregations []AggregationSpec) []AggregationResult {
	aggResults := make([]AggregationResult, len(aggregations))

	for i, spec := range aggregations {
		switch spec.Function {
		case FuncCOUNT:
			if spec.Column == "*" {
				aggResults[i].Count = int64(len(results))
			} else {
				count := int64(0)
				for _, result := range results {
					if value := e.findColumnValue(result, spec.Column); value != nil && !e.isNullValue(value) {
						count++
					}
				}
				aggResults[i].Count = count
			}

		case FuncSUM:
			sum := float64(0)
			for _, result := range results {
				if value := e.findColumnValue(result, spec.Column); value != nil {
					if numValue := e.convertToNumber(value); numValue != nil {
						sum += *numValue
					}
				}
			}
			aggResults[i].Sum = sum

		case FuncAVG:
			sum := float64(0)
			count := int64(0)
			for _, result := range results {
				if value := e.findColumnValue(result, spec.Column); value != nil {
					if numValue := e.convertToNumber(value); numValue != nil {
						sum += *numValue
						count++
					}
				}
			}
			if count > 0 {
				aggResults[i].Sum = sum / float64(count) // Store average in Sum field
				aggResults[i].Count = count
			}

		case FuncMIN:
			var min interface{}
			var minValue *schema_pb.Value
			for _, result := range results {
				if value := e.findColumnValue(result, spec.Column); value != nil {
					if minValue == nil || e.compareValues(value, minValue) < 0 {
						minValue = value
						min = e.extractRawValue(value)
					}
				}
			}
			aggResults[i].Min = min

		case FuncMAX:
			var max interface{}
			var maxValue *schema_pb.Value
			for _, result := range results {
				if value := e.findColumnValue(result, spec.Column); value != nil {
					if maxValue == nil || e.compareValues(value, maxValue) > 0 {
						maxValue = value
						max = e.extractRawValue(value)
					}
				}
			}
			aggResults[i].Max = max
		}
	}

	return aggResults
}

// canUseParquetStatsForAggregation determines if an aggregation can be optimized with parquet stats
func (e *SQLEngine) canUseParquetStatsForAggregation(spec AggregationSpec) bool {
	switch spec.Function {
	case FuncCOUNT:
		return spec.Column == "*" || e.isSystemColumn(spec.Column) || e.isRegularColumn(spec.Column)
	case FuncMIN, FuncMAX:
		return e.isSystemColumn(spec.Column) || e.isRegularColumn(spec.Column)
	case FuncSUM, FuncAVG:
		// These require scanning actual values, not just min/max
		return false
	default:
		return false
	}
}

// debugHybridScanOptions logs the exact scan options being used
func debugHybridScanOptions(ctx context.Context, options HybridScanOptions, queryType string) {
	if isDebugMode(ctx) {
		fmt.Printf("\n=== HYBRID SCAN OPTIONS DEBUG (%s) ===\n", queryType)
		fmt.Printf("StartTimeNs: %d\n", options.StartTimeNs)
		fmt.Printf("StopTimeNs: %d\n", options.StopTimeNs)
		fmt.Printf("Limit: %d\n", options.Limit)
		fmt.Printf("Offset: %d\n", options.Offset)
		fmt.Printf("Predicate: %v\n", options.Predicate != nil)
		fmt.Printf("Columns: %v\n", options.Columns)
		fmt.Printf("==========================================\n")
	}
}
