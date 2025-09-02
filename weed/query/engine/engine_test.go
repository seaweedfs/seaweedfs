package engine

import (
	"context"
	"errors"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/mq/topic"
	"github.com/seaweedfs/seaweedfs/weed/pb/schema_pb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/xwb1989/sqlparser"
)

// Mock implementations for testing
type MockHybridMessageScanner struct {
	mock.Mock
	topic topic.Topic
}

func (m *MockHybridMessageScanner) ReadParquetStatistics(partitionPath string) ([]*ParquetFileStats, error) {
	args := m.Called(partitionPath)
	return args.Get(0).([]*ParquetFileStats), args.Error(1)
}

type MockSQLEngine struct {
	*SQLEngine
	mockPartitions         map[string][]string
	mockParquetSourceFiles map[string]map[string]bool
	mockLiveLogRowCounts   map[string]int64
	mockColumnStats        map[string]map[string]*ParquetColumnStats
}

func NewMockSQLEngine() *MockSQLEngine {
	return &MockSQLEngine{
		SQLEngine: &SQLEngine{
			catalog: &SchemaCatalog{
				databases:       make(map[string]*DatabaseInfo),
				currentDatabase: "test",
			},
		},
		mockPartitions:         make(map[string][]string),
		mockParquetSourceFiles: make(map[string]map[string]bool),
		mockLiveLogRowCounts:   make(map[string]int64),
		mockColumnStats:        make(map[string]map[string]*ParquetColumnStats),
	}
}

func (m *MockSQLEngine) discoverTopicPartitions(namespace, topicName string) ([]string, error) {
	key := namespace + "." + topicName
	if partitions, exists := m.mockPartitions[key]; exists {
		return partitions, nil
	}
	return []string{"partition-1", "partition-2"}, nil
}

func (m *MockSQLEngine) extractParquetSourceFiles(fileStats []*ParquetFileStats) map[string]bool {
	if len(fileStats) == 0 {
		return make(map[string]bool)
	}
	return map[string]bool{"converted-log-1": true}
}

func (m *MockSQLEngine) countLiveLogRowsExcludingParquetSources(partition string, parquetSources map[string]bool) (int64, error) {
	if count, exists := m.mockLiveLogRowCounts[partition]; exists {
		return count, nil
	}
	return 25, nil
}

func (m *MockSQLEngine) computeLiveLogMinMax(partition, column string, parquetSources map[string]bool) (interface{}, interface{}, error) {
	switch column {
	case "id":
		return int64(1), int64(50), nil
	case "value":
		return 10.5, 99.9, nil
	default:
		return nil, nil, nil
	}
}

func (m *MockSQLEngine) getSystemColumnGlobalMin(column string, allFileStats map[string][]*ParquetFileStats) interface{} {
	return int64(1000000000)
}

func (m *MockSQLEngine) getSystemColumnGlobalMax(column string, allFileStats map[string][]*ParquetFileStats) interface{} {
	return int64(2000000000)
}

func createMockColumnStats(column string, minVal, maxVal interface{}) *ParquetColumnStats {
	return &ParquetColumnStats{
		ColumnName: column,
		MinValue:   convertToSchemaValue(minVal),
		MaxValue:   convertToSchemaValue(maxVal),
		NullCount:  0,
	}
}

func convertToSchemaValue(val interface{}) *schema_pb.Value {
	switch v := val.(type) {
	case int64:
		return &schema_pb.Value{Kind: &schema_pb.Value_Int64Value{Int64Value: v}}
	case float64:
		return &schema_pb.Value{Kind: &schema_pb.Value_DoubleValue{DoubleValue: v}}
	case string:
		return &schema_pb.Value{Kind: &schema_pb.Value_StringValue{StringValue: v}}
	}
	return nil
}

// Test FastPathOptimizer
func TestFastPathOptimizer_DetermineStrategy(t *testing.T) {
	engine := NewMockSQLEngine()
	optimizer := NewFastPathOptimizer(engine.SQLEngine)

	tests := []struct {
		name         string
		aggregations []AggregationSpec
		expected     AggregationStrategy
	}{
		{
			name: "Supported aggregations",
			aggregations: []AggregationSpec{
				{Function: "COUNT", Column: "*"},
				{Function: "MAX", Column: "id"},
				{Function: "MIN", Column: "value"},
			},
			expected: AggregationStrategy{
				CanUseFastPath:   true,
				Reason:           "all_aggregations_supported",
				UnsupportedSpecs: []AggregationSpec{},
			},
		},
		{
			name: "Unsupported aggregation",
			aggregations: []AggregationSpec{
				{Function: "COUNT", Column: "*"},
				{Function: "AVG", Column: "value"}, // Not supported
			},
			expected: AggregationStrategy{
				CanUseFastPath: false,
				Reason:         "unsupported_aggregation_functions",
			},
		},
		{
			name:         "Empty aggregations",
			aggregations: []AggregationSpec{},
			expected: AggregationStrategy{
				CanUseFastPath:   true,
				Reason:           "all_aggregations_supported",
				UnsupportedSpecs: []AggregationSpec{},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			strategy := optimizer.DetermineStrategy(tt.aggregations)

			assert.Equal(t, tt.expected.CanUseFastPath, strategy.CanUseFastPath)
			assert.Equal(t, tt.expected.Reason, strategy.Reason)
			if !tt.expected.CanUseFastPath {
				assert.NotEmpty(t, strategy.UnsupportedSpecs)
			}
		})
	}
}

// Test AggregationComputer
func TestAggregationComputer_ComputeFastPathAggregations(t *testing.T) {
	engine := NewMockSQLEngine()
	computer := NewAggregationComputer(engine.SQLEngine)

	dataSources := &TopicDataSources{
		ParquetFiles: map[string][]*ParquetFileStats{
			"/topics/test/topic1/partition-1": {
				{
					RowCount: 30,
					ColumnStats: map[string]*ParquetColumnStats{
						"id": createMockColumnStats("id", int64(10), int64(40)),
					},
				},
			},
		},
		ParquetRowCount: 30,
		LiveLogRowCount: 25,
		PartitionsCount: 1,
	}

	partitions := []string{"/topics/test/topic1/partition-1"}

	tests := []struct {
		name         string
		aggregations []AggregationSpec
		validate     func(t *testing.T, results []AggregationResult)
	}{
		{
			name: "COUNT aggregation",
			aggregations: []AggregationSpec{
				{Function: "COUNT", Column: "*"},
			},
			validate: func(t *testing.T, results []AggregationResult) {
				assert.Len(t, results, 1)
				assert.Equal(t, int64(55), results[0].Count) // 30 + 25
			},
		},
		{
			name: "MAX aggregation",
			aggregations: []AggregationSpec{
				{Function: "MAX", Column: "id"},
			},
			validate: func(t *testing.T, results []AggregationResult) {
				assert.Len(t, results, 1)
				// Should be max of parquet stats (40) - mock doesn't combine with live log
				assert.Equal(t, int64(40), results[0].Max)
			},
		},
		{
			name: "MIN aggregation",
			aggregations: []AggregationSpec{
				{Function: "MIN", Column: "id"},
			},
			validate: func(t *testing.T, results []AggregationResult) {
				assert.Len(t, results, 1)
				// Should be min of parquet stats (10) - mock doesn't combine with live log
				assert.Equal(t, int64(10), results[0].Min)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			results, err := computer.ComputeFastPathAggregations(ctx, tt.aggregations, dataSources, partitions)

			assert.NoError(t, err)
			tt.validate(t, results)
		})
	}
}

// Test ExecutionPlanBuilder
func TestExecutionPlanBuilder_BuildAggregationPlan(t *testing.T) {
	engine := NewMockSQLEngine()
	builder := NewExecutionPlanBuilder(engine.SQLEngine)

	// Parse a simple SELECT statement
	stmt, err := sqlparser.Parse("SELECT COUNT(*) FROM test_topic")
	assert.NoError(t, err)
	selectStmt := stmt.(*sqlparser.Select)

	aggregations := []AggregationSpec{
		{Function: "COUNT", Column: "*"},
	}

	strategy := AggregationStrategy{
		CanUseFastPath: true,
		Reason:         "all_aggregations_supported",
	}

	dataSources := &TopicDataSources{
		ParquetRowCount: 100,
		LiveLogRowCount: 50,
		PartitionsCount: 3,
		ParquetFiles: map[string][]*ParquetFileStats{
			"partition-1": {{RowCount: 50}},
			"partition-2": {{RowCount: 50}},
		},
	}

	plan := builder.BuildAggregationPlan(selectStmt, aggregations, strategy, dataSources)

	assert.Equal(t, "SELECT", plan.QueryType)
	assert.Equal(t, "hybrid_fast_path", plan.ExecutionStrategy)
	assert.Contains(t, plan.DataSources, "parquet_stats")
	assert.Contains(t, plan.DataSources, "live_logs")
	assert.Equal(t, 3, plan.PartitionsScanned)
	assert.Equal(t, 2, plan.ParquetFilesScanned)
	assert.Contains(t, plan.OptimizationsUsed, "parquet_statistics")
	assert.Equal(t, []string{"COUNT(*)"}, plan.Aggregations)
	assert.Equal(t, int64(50), plan.TotalRowsProcessed) // Only live logs scanned
}

// Test Error Types
func TestErrorTypes(t *testing.T) {
	t.Run("AggregationError", func(t *testing.T) {
		err := AggregationError{
			Operation: "MAX",
			Column:    "id",
			Cause:     errors.New("column not found"),
		}

		expected := "aggregation error in MAX(id): column not found"
		assert.Equal(t, expected, err.Error())
	})

	t.Run("DataSourceError", func(t *testing.T) {
		err := DataSourceError{
			Source: "partition_discovery:test.topic1",
			Cause:  errors.New("network timeout"),
		}

		expected := "data source error in partition_discovery:test.topic1: network timeout"
		assert.Equal(t, expected, err.Error())
	})

	t.Run("OptimizationError", func(t *testing.T) {
		err := OptimizationError{
			Strategy: "fast_path_aggregation",
			Reason:   "unsupported function: AVG",
		}

		expected := "optimization failed for fast_path_aggregation: unsupported function: AVG"
		assert.Equal(t, expected, err.Error())
	})
}

// Integration Tests
func TestIntegration_FastPathOptimization(t *testing.T) {
	engine := NewMockSQLEngine()

	// Setup components
	optimizer := NewFastPathOptimizer(engine.SQLEngine)
	computer := NewAggregationComputer(engine.SQLEngine)

	// Mock data setup
	aggregations := []AggregationSpec{
		{Function: "COUNT", Column: "*"},
		{Function: "MAX", Column: "id"},
	}

	// Step 1: Determine strategy
	strategy := optimizer.DetermineStrategy(aggregations)
	assert.True(t, strategy.CanUseFastPath)

	// Step 2: Mock data sources
	dataSources := &TopicDataSources{
		ParquetFiles: map[string][]*ParquetFileStats{
			"/topics/test/topic1/partition-1": {{
				RowCount: 75,
				ColumnStats: map[string]*ParquetColumnStats{
					"id": createMockColumnStats("id", int64(1), int64(100)),
				},
			}},
		},
		ParquetRowCount: 75,
		LiveLogRowCount: 25,
		PartitionsCount: 1,
	}

	partitions := []string{"/topics/test/topic1/partition-1"}

	// Step 3: Compute aggregations
	ctx := context.Background()
	results, err := computer.ComputeFastPathAggregations(ctx, aggregations, dataSources, partitions)
	assert.NoError(t, err)
	assert.Len(t, results, 2)
	assert.Equal(t, int64(100), results[0].Count) // 75 + 25
	assert.Equal(t, int64(100), results[1].Max)   // From parquet stats mock
}

func TestIntegration_FallbackToFullScan(t *testing.T) {
	engine := NewMockSQLEngine()
	optimizer := NewFastPathOptimizer(engine.SQLEngine)

	// Unsupported aggregations
	aggregations := []AggregationSpec{
		{Function: "AVG", Column: "value"}, // Not supported
	}

	// Step 1: Strategy should reject fast path
	strategy := optimizer.DetermineStrategy(aggregations)
	assert.False(t, strategy.CanUseFastPath)
	assert.Equal(t, "unsupported_aggregation_functions", strategy.Reason)
	assert.NotEmpty(t, strategy.UnsupportedSpecs)
}

// Benchmark Tests
func BenchmarkFastPathOptimizer_DetermineStrategy(b *testing.B) {
	engine := NewMockSQLEngine()
	optimizer := NewFastPathOptimizer(engine.SQLEngine)

	aggregations := []AggregationSpec{
		{Function: "COUNT", Column: "*"},
		{Function: "MAX", Column: "id"},
		{Function: "MIN", Column: "value"},
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		strategy := optimizer.DetermineStrategy(aggregations)
		_ = strategy.CanUseFastPath
	}
}

func BenchmarkAggregationComputer_ComputeFastPathAggregations(b *testing.B) {
	engine := NewMockSQLEngine()
	computer := NewAggregationComputer(engine.SQLEngine)

	dataSources := &TopicDataSources{
		ParquetFiles: map[string][]*ParquetFileStats{
			"partition-1": {{
				RowCount: 1000,
				ColumnStats: map[string]*ParquetColumnStats{
					"id": createMockColumnStats("id", int64(1), int64(1000)),
				},
			}},
		},
		ParquetRowCount: 1000,
		LiveLogRowCount: 100,
	}

	aggregations := []AggregationSpec{
		{Function: "COUNT", Column: "*"},
		{Function: "MAX", Column: "id"},
	}

	partitions := []string{"partition-1"}
	ctx := context.Background()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		results, err := computer.ComputeFastPathAggregations(ctx, aggregations, dataSources, partitions)
		if err != nil {
			b.Fatal(err)
		}
		_ = results
	}
}
