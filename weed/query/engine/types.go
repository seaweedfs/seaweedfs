package engine

import (
	"errors"
	"fmt"

	"github.com/seaweedfs/seaweedfs/weed/query/sqltypes"
)

// QueryExecutionPlan contains information about how a query was executed
type QueryExecutionPlan struct {
	QueryType           string
	ExecutionStrategy   string                 `json:"execution_strategy"` // fast_path, full_scan, hybrid
	DataSources         []string               `json:"data_sources"`       // parquet_files, live_logs, broker_buffer
	PartitionsScanned   int                    `json:"partitions_scanned"`
	ParquetFilesScanned int                    `json:"parquet_files_scanned"`
	LiveLogFilesScanned int                    `json:"live_log_files_scanned"`
	TotalRowsProcessed  int64                  `json:"total_rows_processed"`
	OptimizationsUsed   []string               `json:"optimizations_used"` // parquet_stats, predicate_pushdown, etc.
	TimeRangeFilters    map[string]interface{} `json:"time_range_filters,omitempty"`
	Aggregations        []string               `json:"aggregations,omitempty"`
	ExecutionTimeMs     float64                `json:"execution_time_ms"`
	Details             map[string]interface{} `json:"details,omitempty"`

	// Broker buffer information
	BrokerBufferQueried  bool  `json:"broker_buffer_queried"`
	BrokerBufferMessages int   `json:"broker_buffer_messages"`
	BufferStartIndex     int64 `json:"buffer_start_index,omitempty"`
}

// QueryResult represents the result of a SQL query execution
type QueryResult struct {
	Columns       []string            `json:"columns"`
	Rows          [][]sqltypes.Value  `json:"rows"`
	Error         error               `json:"error,omitempty"`
	ExecutionPlan *QueryExecutionPlan `json:"execution_plan,omitempty"`
	// Schema information for type inference (optional)
	Database string `json:"database,omitempty"`
	Table    string `json:"table,omitempty"`
}

// NoSchemaError indicates that a topic exists but has no schema defined
// This is a normal condition for quiet topics that haven't received messages yet
type NoSchemaError struct {
	Namespace string
	Topic     string
}

func (e NoSchemaError) Error() string {
	return fmt.Sprintf("topic %s.%s has no schema", e.Namespace, e.Topic)
}

// IsNoSchemaError checks if an error is a NoSchemaError
func IsNoSchemaError(err error) bool {
	var noSchemaErr NoSchemaError
	return errors.As(err, &noSchemaErr)
}
