package engine

import (
	"github.com/seaweedfs/seaweedfs/weed/pb/schema_pb"
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

// ParquetColumnStats holds statistics for a single column in a Parquet file
type ParquetColumnStats struct {
	ColumnName string
	MinValue   *schema_pb.Value
	MaxValue   *schema_pb.Value
	NullCount  int64
	RowCount   int64
}

// ParquetFileStats holds statistics for a single Parquet file
type ParquetFileStats struct {
	FileName    string
	RowCount    int64
	ColumnStats map[string]*ParquetColumnStats
}

// HybridScanResult represents a single record from hybrid scanning
type HybridScanResult struct {
	RecordValue *schema_pb.Value
	Source      string // "live_log", "parquet_archive"
	Timestamp   int64
	Key         []byte
}

// HybridScanOptions configures how the hybrid scanner operates
type HybridScanOptions struct {
	StartTimeNs int64
	StopTimeNs  int64
	Limit       int
	Predicate   func(*schema_pb.RecordValue) bool
}
