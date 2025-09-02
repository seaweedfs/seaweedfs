package engine

import (
	"github.com/seaweedfs/seaweedfs/weed/query/sqltypes"
)

// QueryExecutionPlan contains information about how a query was executed
type QueryExecutionPlan struct {
	QueryType           string
	ExecutionStrategy   string                 `json:"execution_strategy"` // fast_path, full_scan, hybrid
	DataSources         []string               `json:"data_sources"`       // parquet_files, live_logs
	PartitionsScanned   int                    `json:"partitions_scanned"`
	ParquetFilesScanned int                    `json:"parquet_files_scanned"`
	LiveLogFilesScanned int                    `json:"live_log_files_scanned"`
	TotalRowsProcessed  int64                  `json:"total_rows_processed"`
	OptimizationsUsed   []string               `json:"optimizations_used"` // parquet_stats, predicate_pushdown, etc.
	TimeRangeFilters    map[string]interface{} `json:"time_range_filters,omitempty"`
	Aggregations        []string               `json:"aggregations,omitempty"`
	ExecutionTimeMs     float64                `json:"execution_time_ms"`
	Details             map[string]interface{} `json:"details,omitempty"`
}

// QueryResult represents the result of a SQL query execution
type QueryResult struct {
	Columns       []string            `json:"columns"`
	Rows          [][]sqltypes.Value  `json:"rows"`
	Error         error               `json:"error,omitempty"`
	ExecutionPlan *QueryExecutionPlan `json:"execution_plan,omitempty"`
}
