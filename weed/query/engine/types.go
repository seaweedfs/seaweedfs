package engine

import (
	"errors"
	"fmt"

	"github.com/seaweedfs/seaweedfs/weed/query/sqltypes"
)

// ExecutionNode represents a node in the execution plan tree
type ExecutionNode interface {
	GetNodeType() string
	GetChildren() []ExecutionNode
	GetDescription() string
	GetDetails() map[string]interface{}
}

// FileSourceNode represents a leaf node - an actual data source file
type FileSourceNode struct {
	FilePath         string                 `json:"file_path"`
	SourceType       string                 `json:"source_type"`       // "parquet", "live_log", "broker_buffer"
	Predicates       []string               `json:"predicates"`        // Pushed down predicates
	Operations       []string               `json:"operations"`        // "sequential_scan", "statistics_skip", etc.
	EstimatedRows    int64                  `json:"estimated_rows"`    // Estimated rows to process
	OptimizationHint string                 `json:"optimization_hint"` // "fast_path", "full_scan", etc.
	Details          map[string]interface{} `json:"details"`
}

func (f *FileSourceNode) GetNodeType() string          { return "file_source" }
func (f *FileSourceNode) GetChildren() []ExecutionNode { return nil }
func (f *FileSourceNode) GetDescription() string {
	if f.OptimizationHint != "" {
		return fmt.Sprintf("%s (%s)", f.FilePath, f.OptimizationHint)
	}
	return f.FilePath
}
func (f *FileSourceNode) GetDetails() map[string]interface{} { return f.Details }

// MergeOperationNode represents a branch node - combines data from multiple sources
type MergeOperationNode struct {
	OperationType string                 `json:"operation_type"` // "chronological_merge", "union", etc.
	Children      []ExecutionNode        `json:"children"`
	Description   string                 `json:"description"`
	Details       map[string]interface{} `json:"details"`
}

func (m *MergeOperationNode) GetNodeType() string                { return "merge_operation" }
func (m *MergeOperationNode) GetChildren() []ExecutionNode       { return m.Children }
func (m *MergeOperationNode) GetDescription() string             { return m.Description }
func (m *MergeOperationNode) GetDetails() map[string]interface{} { return m.Details }

// ScanOperationNode represents an intermediate node - a scanning strategy
type ScanOperationNode struct {
	ScanType    string                 `json:"scan_type"` // "parquet_scan", "live_log_scan", "hybrid_scan"
	Children    []ExecutionNode        `json:"children"`
	Predicates  []string               `json:"predicates"` // Predicates applied at this level
	Description string                 `json:"description"`
	Details     map[string]interface{} `json:"details"`
}

func (s *ScanOperationNode) GetNodeType() string                { return "scan_operation" }
func (s *ScanOperationNode) GetChildren() []ExecutionNode       { return s.Children }
func (s *ScanOperationNode) GetDescription() string             { return s.Description }
func (s *ScanOperationNode) GetDetails() map[string]interface{} { return s.Details }

// QueryExecutionPlan contains information about how a query was executed
type QueryExecutionPlan struct {
	QueryType         string
	ExecutionStrategy string        `json:"execution_strategy"`  // fast_path, full_scan, hybrid
	RootNode          ExecutionNode `json:"root_node,omitempty"` // Root of execution tree

	// Legacy fields (kept for compatibility)
	DataSources         []string               `json:"data_sources"` // parquet_files, live_logs, broker_buffer
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

// Plan detail keys
const (
	PlanDetailStartTimeNs = "StartTimeNs"
	PlanDetailStopTimeNs  = "StopTimeNs"
)

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
