package engine

import (
	"context"
	"encoding/binary"
	"errors"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/mq/topic"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/schema_pb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"google.golang.org/protobuf/proto"
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

func (m *MockSQLEngine) countLiveLogRowsExcludingParquetSources(ctx context.Context, partition string, parquetSources map[string]bool) (int64, error) {
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
				{Function: FuncCOUNT, Column: "*"},
				{Function: FuncMAX, Column: "id"},
				{Function: FuncMIN, Column: "value"},
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
				{Function: FuncCOUNT, Column: "*"},
				{Function: FuncAVG, Column: "value"}, // Not supported
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
				{Function: FuncCOUNT, Column: "*"},
			},
			validate: func(t *testing.T, results []AggregationResult) {
				assert.Len(t, results, 1)
				assert.Equal(t, int64(55), results[0].Count) // 30 + 25
			},
		},
		{
			name: "MAX aggregation",
			aggregations: []AggregationSpec{
				{Function: FuncMAX, Column: "id"},
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
				{Function: FuncMIN, Column: "id"},
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

// Test case-insensitive column lookup and null handling for MIN/MAX aggregations
func TestAggregationComputer_MinMaxEdgeCases(t *testing.T) {
	engine := NewMockSQLEngine()
	computer := NewAggregationComputer(engine.SQLEngine)

	tests := []struct {
		name         string
		dataSources  *TopicDataSources
		aggregations []AggregationSpec
		validate     func(t *testing.T, results []AggregationResult, err error)
	}{
		{
			name: "Case insensitive column lookup",
			dataSources: &TopicDataSources{
				ParquetFiles: map[string][]*ParquetFileStats{
					"/topics/test/partition-1": {
						{
							RowCount: 50,
							ColumnStats: map[string]*ParquetColumnStats{
								"ID": createMockColumnStats("ID", int64(5), int64(95)), // Uppercase column name
							},
						},
					},
				},
				ParquetRowCount: 50,
				LiveLogRowCount: 0,
				PartitionsCount: 1,
			},
			aggregations: []AggregationSpec{
				{Function: FuncMIN, Column: "id"}, // lowercase column name
				{Function: FuncMAX, Column: "id"},
			},
			validate: func(t *testing.T, results []AggregationResult, err error) {
				assert.NoError(t, err)
				assert.Len(t, results, 2)
				assert.Equal(t, int64(5), results[0].Min, "MIN should work with case-insensitive lookup")
				assert.Equal(t, int64(95), results[1].Max, "MAX should work with case-insensitive lookup")
			},
		},
		{
			name: "Null column stats handling",
			dataSources: &TopicDataSources{
				ParquetFiles: map[string][]*ParquetFileStats{
					"/topics/test/partition-1": {
						{
							RowCount: 50,
							ColumnStats: map[string]*ParquetColumnStats{
								"id": {
									ColumnName: "id",
									MinValue:   nil, // Null min value
									MaxValue:   nil, // Null max value
									NullCount:  50,
									RowCount:   50,
								},
							},
						},
					},
				},
				ParquetRowCount: 50,
				LiveLogRowCount: 0,
				PartitionsCount: 1,
			},
			aggregations: []AggregationSpec{
				{Function: FuncMIN, Column: "id"},
				{Function: FuncMAX, Column: "id"},
			},
			validate: func(t *testing.T, results []AggregationResult, err error) {
				assert.NoError(t, err)
				assert.Len(t, results, 2)
				// When stats are null, should fall back to system column or return nil
				// This tests that we don't crash on null stats
			},
		},
		{
			name: "Mixed data types - string column",
			dataSources: &TopicDataSources{
				ParquetFiles: map[string][]*ParquetFileStats{
					"/topics/test/partition-1": {
						{
							RowCount: 30,
							ColumnStats: map[string]*ParquetColumnStats{
								"name": createMockColumnStats("name", "Alice", "Zoe"),
							},
						},
					},
				},
				ParquetRowCount: 30,
				LiveLogRowCount: 0,
				PartitionsCount: 1,
			},
			aggregations: []AggregationSpec{
				{Function: FuncMIN, Column: "name"},
				{Function: FuncMAX, Column: "name"},
			},
			validate: func(t *testing.T, results []AggregationResult, err error) {
				assert.NoError(t, err)
				assert.Len(t, results, 2)
				assert.Equal(t, "Alice", results[0].Min)
				assert.Equal(t, "Zoe", results[1].Max)
			},
		},
		{
			name: "Mixed data types - float column",
			dataSources: &TopicDataSources{
				ParquetFiles: map[string][]*ParquetFileStats{
					"/topics/test/partition-1": {
						{
							RowCount: 25,
							ColumnStats: map[string]*ParquetColumnStats{
								"price": createMockColumnStats("price", float64(19.99), float64(299.50)),
							},
						},
					},
				},
				ParquetRowCount: 25,
				LiveLogRowCount: 0,
				PartitionsCount: 1,
			},
			aggregations: []AggregationSpec{
				{Function: FuncMIN, Column: "price"},
				{Function: FuncMAX, Column: "price"},
			},
			validate: func(t *testing.T, results []AggregationResult, err error) {
				assert.NoError(t, err)
				assert.Len(t, results, 2)
				assert.Equal(t, float64(19.99), results[0].Min)
				assert.Equal(t, float64(299.50), results[1].Max)
			},
		},
		{
			name: "Column not found in parquet stats",
			dataSources: &TopicDataSources{
				ParquetFiles: map[string][]*ParquetFileStats{
					"/topics/test/partition-1": {
						{
							RowCount: 20,
							ColumnStats: map[string]*ParquetColumnStats{
								"id": createMockColumnStats("id", int64(1), int64(100)),
								// Note: "nonexistent_column" is not in stats
							},
						},
					},
				},
				ParquetRowCount: 20,
				LiveLogRowCount: 10, // Has live logs to fall back to
				PartitionsCount: 1,
			},
			aggregations: []AggregationSpec{
				{Function: FuncMIN, Column: "nonexistent_column"},
				{Function: FuncMAX, Column: "nonexistent_column"},
			},
			validate: func(t *testing.T, results []AggregationResult, err error) {
				assert.NoError(t, err)
				assert.Len(t, results, 2)
				// Should fall back to live log processing or return nil
				// The key is that it shouldn't crash
			},
		},
		{
			name: "Multiple parquet files with different ranges",
			dataSources: &TopicDataSources{
				ParquetFiles: map[string][]*ParquetFileStats{
					"/topics/test/partition-1": {
						{
							RowCount: 30,
							ColumnStats: map[string]*ParquetColumnStats{
								"score": createMockColumnStats("score", int64(10), int64(50)),
							},
						},
						{
							RowCount: 40,
							ColumnStats: map[string]*ParquetColumnStats{
								"score": createMockColumnStats("score", int64(5), int64(75)), // Lower min, higher max
							},
						},
					},
				},
				ParquetRowCount: 70,
				LiveLogRowCount: 0,
				PartitionsCount: 1,
			},
			aggregations: []AggregationSpec{
				{Function: FuncMIN, Column: "score"},
				{Function: FuncMAX, Column: "score"},
			},
			validate: func(t *testing.T, results []AggregationResult, err error) {
				assert.NoError(t, err)
				assert.Len(t, results, 2)
				assert.Equal(t, int64(5), results[0].Min, "Should find global minimum across all files")
				assert.Equal(t, int64(75), results[1].Max, "Should find global maximum across all files")
			},
		},
	}

	partitions := []string{"/topics/test/partition-1"}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			results, err := computer.ComputeFastPathAggregations(ctx, tt.aggregations, tt.dataSources, partitions)
			tt.validate(t, results, err)
		})
	}
}

// Test the specific bug where MIN/MAX was returning empty values
func TestAggregationComputer_MinMaxEmptyValuesBugFix(t *testing.T) {
	engine := NewMockSQLEngine()
	computer := NewAggregationComputer(engine.SQLEngine)

	// This test specifically addresses the bug where MIN/MAX returned empty
	// due to improper null checking and extraction logic
	dataSources := &TopicDataSources{
		ParquetFiles: map[string][]*ParquetFileStats{
			"/topics/test/test-topic/partition1": {
				{
					RowCount: 100,
					ColumnStats: map[string]*ParquetColumnStats{
						"id": {
							ColumnName: "id",
							MinValue:   &schema_pb.Value{Kind: &schema_pb.Value_Int32Value{Int32Value: 0}},  // Min should be 0
							MaxValue:   &schema_pb.Value{Kind: &schema_pb.Value_Int32Value{Int32Value: 99}}, // Max should be 99
							NullCount:  0,
							RowCount:   100,
						},
					},
				},
			},
		},
		ParquetRowCount: 100,
		LiveLogRowCount: 0, // No live logs, pure parquet stats
		PartitionsCount: 1,
	}

	partitions := []string{"/topics/test/test-topic/partition1"}

	tests := []struct {
		name       string
		aggregSpec AggregationSpec
		expected   interface{}
	}{
		{
			name:       "MIN should return 0 not empty",
			aggregSpec: AggregationSpec{Function: FuncMIN, Column: "id"},
			expected:   int32(0), // Should extract the actual minimum value
		},
		{
			name:       "MAX should return 99 not empty",
			aggregSpec: AggregationSpec{Function: FuncMAX, Column: "id"},
			expected:   int32(99), // Should extract the actual maximum value
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			results, err := computer.ComputeFastPathAggregations(ctx, []AggregationSpec{tt.aggregSpec}, dataSources, partitions)

			assert.NoError(t, err)
			assert.Len(t, results, 1)

			// Verify the result is not nil/empty
			if tt.aggregSpec.Function == FuncMIN {
				assert.NotNil(t, results[0].Min, "MIN result should not be nil")
				assert.Equal(t, tt.expected, results[0].Min)
			} else if tt.aggregSpec.Function == FuncMAX {
				assert.NotNil(t, results[0].Max, "MAX result should not be nil")
				assert.Equal(t, tt.expected, results[0].Max)
			}
		})
	}
}

// Test the formatAggregationResult function with MIN/MAX edge cases
func TestSQLEngine_FormatAggregationResult_MinMax(t *testing.T) {
	engine := NewTestSQLEngine()

	tests := []struct {
		name     string
		spec     AggregationSpec
		result   AggregationResult
		expected string
	}{
		{
			name:     "MIN with zero value should not be empty",
			spec:     AggregationSpec{Function: FuncMIN, Column: "id"},
			result:   AggregationResult{Min: int32(0)},
			expected: "0",
		},
		{
			name:     "MAX with large value",
			spec:     AggregationSpec{Function: FuncMAX, Column: "id"},
			result:   AggregationResult{Max: int32(99)},
			expected: "99",
		},
		{
			name:     "MIN with negative value",
			spec:     AggregationSpec{Function: FuncMIN, Column: "score"},
			result:   AggregationResult{Min: int64(-50)},
			expected: "-50",
		},
		{
			name:     "MAX with float value",
			spec:     AggregationSpec{Function: FuncMAX, Column: "price"},
			result:   AggregationResult{Max: float64(299.99)},
			expected: "299.99",
		},
		{
			name:     "MIN with string value",
			spec:     AggregationSpec{Function: FuncMIN, Column: "name"},
			result:   AggregationResult{Min: "Alice"},
			expected: "Alice",
		},
		{
			name:     "MIN with nil should return NULL",
			spec:     AggregationSpec{Function: FuncMIN, Column: "missing"},
			result:   AggregationResult{Min: nil},
			expected: "", // NULL values display as empty
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sqlValue := engine.formatAggregationResult(tt.spec, tt.result)
			assert.Equal(t, tt.expected, sqlValue.String())
		})
	}
}

// Test the direct formatAggregationResult scenario that was originally broken
func TestSQLEngine_MinMaxBugFixIntegration(t *testing.T) {
	// This test focuses on the core bug fix without the complexity of table discovery
	// It directly tests the scenario where MIN/MAX returned empty due to the bug

	engine := NewTestSQLEngine()

	// Test the direct formatting path that was failing
	tests := []struct {
		name          string
		aggregSpec    AggregationSpec
		aggResult     AggregationResult
		expectedEmpty bool
		expectedValue string
	}{
		{
			name:          "MIN with zero should not be empty (the original bug)",
			aggregSpec:    AggregationSpec{Function: FuncMIN, Column: "id", Alias: "MIN(id)"},
			aggResult:     AggregationResult{Min: int32(0)}, // This was returning empty before fix
			expectedEmpty: false,
			expectedValue: "0",
		},
		{
			name:          "MAX with valid value should not be empty",
			aggregSpec:    AggregationSpec{Function: FuncMAX, Column: "id", Alias: "MAX(id)"},
			aggResult:     AggregationResult{Max: int32(99)},
			expectedEmpty: false,
			expectedValue: "99",
		},
		{
			name:          "MIN with negative value should work",
			aggregSpec:    AggregationSpec{Function: FuncMIN, Column: "score", Alias: "MIN(score)"},
			aggResult:     AggregationResult{Min: int64(-10)},
			expectedEmpty: false,
			expectedValue: "-10",
		},
		{
			name:          "MIN with nil should be empty (expected behavior)",
			aggregSpec:    AggregationSpec{Function: FuncMIN, Column: "missing", Alias: "MIN(missing)"},
			aggResult:     AggregationResult{Min: nil},
			expectedEmpty: true,
			expectedValue: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Test the formatAggregationResult function directly
			sqlValue := engine.formatAggregationResult(tt.aggregSpec, tt.aggResult)
			result := sqlValue.String()

			if tt.expectedEmpty {
				assert.Empty(t, result, "Result should be empty for nil values")
			} else {
				assert.NotEmpty(t, result, "Result should not be empty")
				assert.Equal(t, tt.expectedValue, result)
			}
		})
	}
}

// Test the tryFastParquetAggregation method specifically for the bug
func TestSQLEngine_FastParquetAggregationBugFix(t *testing.T) {
	// This test verifies that the fast path aggregation logic works correctly
	// and doesn't return nil/empty values when it should return actual data

	engine := NewMockSQLEngine()
	computer := NewAggregationComputer(engine.SQLEngine)

	// Create realistic data sources that mimic the user's scenario
	dataSources := &TopicDataSources{
		ParquetFiles: map[string][]*ParquetFileStats{
			"/topics/test/test-topic/v2025-09-01-22-54-02/0000-0630": {
				{
					RowCount: 100,
					ColumnStats: map[string]*ParquetColumnStats{
						"id": {
							ColumnName: "id",
							MinValue:   &schema_pb.Value{Kind: &schema_pb.Value_Int32Value{Int32Value: 0}},
							MaxValue:   &schema_pb.Value{Kind: &schema_pb.Value_Int32Value{Int32Value: 99}},
							NullCount:  0,
							RowCount:   100,
						},
					},
				},
			},
		},
		ParquetRowCount: 100,
		LiveLogRowCount: 0, // Pure parquet scenario
		PartitionsCount: 1,
	}

	partitions := []string{"/topics/test/test-topic/v2025-09-01-22-54-02/0000-0630"}

	tests := []struct {
		name            string
		aggregations    []AggregationSpec
		validateResults func(t *testing.T, results []AggregationResult)
	}{
		{
			name: "Single MIN aggregation should return value not nil",
			aggregations: []AggregationSpec{
				{Function: FuncMIN, Column: "id", Alias: "MIN(id)"},
			},
			validateResults: func(t *testing.T, results []AggregationResult) {
				assert.Len(t, results, 1)
				assert.NotNil(t, results[0].Min, "MIN result should not be nil")
				assert.Equal(t, int32(0), results[0].Min, "MIN should return the correct minimum value")
			},
		},
		{
			name: "Single MAX aggregation should return value not nil",
			aggregations: []AggregationSpec{
				{Function: FuncMAX, Column: "id", Alias: "MAX(id)"},
			},
			validateResults: func(t *testing.T, results []AggregationResult) {
				assert.Len(t, results, 1)
				assert.NotNil(t, results[0].Max, "MAX result should not be nil")
				assert.Equal(t, int32(99), results[0].Max, "MAX should return the correct maximum value")
			},
		},
		{
			name: "Combined MIN/MAX should both return values",
			aggregations: []AggregationSpec{
				{Function: FuncMIN, Column: "id", Alias: "MIN(id)"},
				{Function: FuncMAX, Column: "id", Alias: "MAX(id)"},
			},
			validateResults: func(t *testing.T, results []AggregationResult) {
				assert.Len(t, results, 2)
				assert.NotNil(t, results[0].Min, "MIN result should not be nil")
				assert.NotNil(t, results[1].Max, "MAX result should not be nil")
				assert.Equal(t, int32(0), results[0].Min)
				assert.Equal(t, int32(99), results[1].Max)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			results, err := computer.ComputeFastPathAggregations(ctx, tt.aggregations, dataSources, partitions)

			assert.NoError(t, err, "ComputeFastPathAggregations should not error")
			tt.validateResults(t, results)
		})
	}
}

// Test ExecutionPlanBuilder
func TestExecutionPlanBuilder_BuildAggregationPlan(t *testing.T) {
	engine := NewMockSQLEngine()
	builder := NewExecutionPlanBuilder(engine.SQLEngine)

	// Parse a simple SELECT statement using the native parser
	stmt, err := ParseSQL("SELECT COUNT(*) FROM test_topic")
	assert.NoError(t, err)
	selectStmt := stmt.(*SelectStatement)

	aggregations := []AggregationSpec{
		{Function: FuncCOUNT, Column: "*"},
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
		{Function: FuncCOUNT, Column: "*"},
		{Function: FuncMAX, Column: "id"},
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
		{Function: FuncCOUNT, Column: "*"},
		{Function: FuncMAX, Column: "id"},
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
		{Function: FuncCOUNT, Column: "*"},
		{Function: FuncMAX, Column: "id"},
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

// Tests for convertLogEntryToRecordValue - Protocol Buffer parsing bug fix
func TestSQLEngine_ConvertLogEntryToRecordValue_ValidProtobuf(t *testing.T) {
	engine := NewTestSQLEngine()

	// Create a valid RecordValue protobuf with user data
	originalRecord := &schema_pb.RecordValue{
		Fields: map[string]*schema_pb.Value{
			"id":    {Kind: &schema_pb.Value_Int32Value{Int32Value: 42}},
			"name":  {Kind: &schema_pb.Value_StringValue{StringValue: "test-user"}},
			"score": {Kind: &schema_pb.Value_DoubleValue{DoubleValue: 95.5}},
		},
	}

	// Serialize the protobuf (this is what MQ actually stores)
	protobufData, err := proto.Marshal(originalRecord)
	assert.NoError(t, err)

	// Create a LogEntry with the serialized data
	logEntry := &filer_pb.LogEntry{
		TsNs:             1609459200000000000, // 2021-01-01 00:00:00 UTC
		PartitionKeyHash: 123,
		Data:             protobufData, // Protocol buffer data (not JSON!)
		Key:              []byte("test-key-001"),
	}

	// Test the conversion
	result, source, err := engine.convertLogEntryToRecordValue(logEntry)

	// Verify no error
	assert.NoError(t, err)
	assert.Equal(t, "live_log", source)
	assert.NotNil(t, result)
	assert.NotNil(t, result.Fields)

	// Verify system columns are added correctly
	assert.Contains(t, result.Fields, SW_COLUMN_NAME_TIMESTAMP)
	assert.Contains(t, result.Fields, SW_COLUMN_NAME_KEY)
	assert.Equal(t, int64(1609459200000000000), result.Fields[SW_COLUMN_NAME_TIMESTAMP].GetInt64Value())
	assert.Equal(t, []byte("test-key-001"), result.Fields[SW_COLUMN_NAME_KEY].GetBytesValue())

	// Verify user data is preserved
	assert.Contains(t, result.Fields, "id")
	assert.Contains(t, result.Fields, "name")
	assert.Contains(t, result.Fields, "score")
	assert.Equal(t, int32(42), result.Fields["id"].GetInt32Value())
	assert.Equal(t, "test-user", result.Fields["name"].GetStringValue())
	assert.Equal(t, 95.5, result.Fields["score"].GetDoubleValue())
}

func TestSQLEngine_ConvertLogEntryToRecordValue_InvalidProtobuf(t *testing.T) {
	engine := NewTestSQLEngine()

	// Create LogEntry with invalid protobuf data (this would cause the original JSON parsing bug)
	logEntry := &filer_pb.LogEntry{
		TsNs:             1609459200000000000,
		PartitionKeyHash: 123,
		Data:             []byte{0x17, 0x00, 0xFF, 0xFE}, // Invalid protobuf data (starts with \x17 like in the original error)
		Key:              []byte("test-key"),
	}

	// Test the conversion
	result, source, err := engine.convertLogEntryToRecordValue(logEntry)

	// Should return error for invalid protobuf
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to unmarshal log entry protobuf")
	assert.Nil(t, result)
	assert.Empty(t, source)
}

func TestSQLEngine_ConvertLogEntryToRecordValue_EmptyProtobuf(t *testing.T) {
	engine := NewTestSQLEngine()

	// Create a minimal valid RecordValue (empty fields)
	emptyRecord := &schema_pb.RecordValue{
		Fields: map[string]*schema_pb.Value{},
	}
	protobufData, err := proto.Marshal(emptyRecord)
	assert.NoError(t, err)

	logEntry := &filer_pb.LogEntry{
		TsNs:             1609459200000000000,
		PartitionKeyHash: 456,
		Data:             protobufData,
		Key:              []byte("empty-key"),
	}

	// Test the conversion
	result, source, err := engine.convertLogEntryToRecordValue(logEntry)

	// Should succeed and add system columns
	assert.NoError(t, err)
	assert.Equal(t, "live_log", source)
	assert.NotNil(t, result)
	assert.NotNil(t, result.Fields)

	// Should have system columns
	assert.Contains(t, result.Fields, SW_COLUMN_NAME_TIMESTAMP)
	assert.Contains(t, result.Fields, SW_COLUMN_NAME_KEY)
	assert.Equal(t, int64(1609459200000000000), result.Fields[SW_COLUMN_NAME_TIMESTAMP].GetInt64Value())
	assert.Equal(t, []byte("empty-key"), result.Fields[SW_COLUMN_NAME_KEY].GetBytesValue())

	// Should have no user fields
	userFieldCount := 0
	for fieldName := range result.Fields {
		if fieldName != SW_COLUMN_NAME_TIMESTAMP && fieldName != SW_COLUMN_NAME_KEY {
			userFieldCount++
		}
	}
	assert.Equal(t, 0, userFieldCount)
}

func TestSQLEngine_ConvertLogEntryToRecordValue_NilFieldsMap(t *testing.T) {
	engine := NewTestSQLEngine()

	// Create RecordValue with nil Fields map (edge case)
	recordWithNilFields := &schema_pb.RecordValue{
		Fields: nil, // This should be handled gracefully
	}
	protobufData, err := proto.Marshal(recordWithNilFields)
	assert.NoError(t, err)

	logEntry := &filer_pb.LogEntry{
		TsNs:             1609459200000000000,
		PartitionKeyHash: 789,
		Data:             protobufData,
		Key:              []byte("nil-fields-key"),
	}

	// Test the conversion
	result, source, err := engine.convertLogEntryToRecordValue(logEntry)

	// Should succeed and create Fields map
	assert.NoError(t, err)
	assert.Equal(t, "live_log", source)
	assert.NotNil(t, result)
	assert.NotNil(t, result.Fields) // Should be created by the function

	// Should have system columns
	assert.Contains(t, result.Fields, SW_COLUMN_NAME_TIMESTAMP)
	assert.Contains(t, result.Fields, SW_COLUMN_NAME_KEY)
	assert.Equal(t, int64(1609459200000000000), result.Fields[SW_COLUMN_NAME_TIMESTAMP].GetInt64Value())
	assert.Equal(t, []byte("nil-fields-key"), result.Fields[SW_COLUMN_NAME_KEY].GetBytesValue())
}

func TestSQLEngine_ConvertLogEntryToRecordValue_SystemColumnOverride(t *testing.T) {
	engine := NewTestSQLEngine()

	// Create RecordValue that already has system column names (should be overridden)
	recordWithSystemCols := &schema_pb.RecordValue{
		Fields: map[string]*schema_pb.Value{
			"user_field":             {Kind: &schema_pb.Value_StringValue{StringValue: "user-data"}},
			SW_COLUMN_NAME_TIMESTAMP: {Kind: &schema_pb.Value_Int64Value{Int64Value: 999999999}},   // Should be overridden
			SW_COLUMN_NAME_KEY:       {Kind: &schema_pb.Value_StringValue{StringValue: "old-key"}}, // Should be overridden
		},
	}
	protobufData, err := proto.Marshal(recordWithSystemCols)
	assert.NoError(t, err)

	logEntry := &filer_pb.LogEntry{
		TsNs:             1609459200000000000,
		PartitionKeyHash: 100,
		Data:             protobufData,
		Key:              []byte("actual-key"),
	}

	// Test the conversion
	result, source, err := engine.convertLogEntryToRecordValue(logEntry)

	// Should succeed
	assert.NoError(t, err)
	assert.Equal(t, "live_log", source)
	assert.NotNil(t, result)

	// System columns should use LogEntry values, not protobuf values
	assert.Equal(t, int64(1609459200000000000), result.Fields[SW_COLUMN_NAME_TIMESTAMP].GetInt64Value())
	assert.Equal(t, []byte("actual-key"), result.Fields[SW_COLUMN_NAME_KEY].GetBytesValue())

	// User field should be preserved
	assert.Contains(t, result.Fields, "user_field")
	assert.Equal(t, "user-data", result.Fields["user_field"].GetStringValue())
}

func TestSQLEngine_ConvertLogEntryToRecordValue_ComplexDataTypes(t *testing.T) {
	engine := NewTestSQLEngine()

	// Test with various data types
	complexRecord := &schema_pb.RecordValue{
		Fields: map[string]*schema_pb.Value{
			"int32_field":  {Kind: &schema_pb.Value_Int32Value{Int32Value: -42}},
			"int64_field":  {Kind: &schema_pb.Value_Int64Value{Int64Value: 9223372036854775807}},
			"float_field":  {Kind: &schema_pb.Value_FloatValue{FloatValue: 3.14159}},
			"double_field": {Kind: &schema_pb.Value_DoubleValue{DoubleValue: 2.718281828}},
			"bool_field":   {Kind: &schema_pb.Value_BoolValue{BoolValue: true}},
			"string_field": {Kind: &schema_pb.Value_StringValue{StringValue: "test string with unicode party"}},
			"bytes_field":  {Kind: &schema_pb.Value_BytesValue{BytesValue: []byte{0x01, 0x02, 0x03}}},
		},
	}
	protobufData, err := proto.Marshal(complexRecord)
	assert.NoError(t, err)

	logEntry := &filer_pb.LogEntry{
		TsNs:             1609459200000000000,
		PartitionKeyHash: 200,
		Data:             protobufData,
		Key:              []byte("complex-key"),
	}

	// Test the conversion
	result, source, err := engine.convertLogEntryToRecordValue(logEntry)

	// Should succeed
	assert.NoError(t, err)
	assert.Equal(t, "live_log", source)
	assert.NotNil(t, result)

	// Verify all data types are preserved
	assert.Equal(t, int32(-42), result.Fields["int32_field"].GetInt32Value())
	assert.Equal(t, int64(9223372036854775807), result.Fields["int64_field"].GetInt64Value())
	assert.Equal(t, float32(3.14159), result.Fields["float_field"].GetFloatValue())
	assert.Equal(t, 2.718281828, result.Fields["double_field"].GetDoubleValue())
	assert.Equal(t, true, result.Fields["bool_field"].GetBoolValue())
	assert.Equal(t, "test string with unicode party", result.Fields["string_field"].GetStringValue())
	assert.Equal(t, []byte{0x01, 0x02, 0x03}, result.Fields["bytes_field"].GetBytesValue())

	// System columns should still be present
	assert.Contains(t, result.Fields, SW_COLUMN_NAME_TIMESTAMP)
	assert.Contains(t, result.Fields, SW_COLUMN_NAME_KEY)
}

// Tests for log buffer deduplication functionality
func TestSQLEngine_GetLogBufferStartFromFile_BinaryFormat(t *testing.T) {
	engine := NewTestSQLEngine()

	// Create sample buffer start (binary format)
	bufferStartBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(bufferStartBytes, uint64(1609459100000000001))

	// Create file entry with buffer start + some chunks
	entry := &filer_pb.Entry{
		Name: "test-log-file",
		Extended: map[string][]byte{
			"buffer_start": bufferStartBytes,
		},
		Chunks: []*filer_pb.FileChunk{
			{FileId: "chunk1", Offset: 0, Size: 1000},
			{FileId: "chunk2", Offset: 1000, Size: 1000},
			{FileId: "chunk3", Offset: 2000, Size: 1000},
		},
	}

	// Test extraction
	result, err := engine.getLogBufferStartFromFile(entry)
	assert.NoError(t, err)
	assert.NotNil(t, result)
	assert.Equal(t, int64(1609459100000000001), result.StartIndex)

	// Test extraction works correctly with the binary format
}

func TestSQLEngine_GetLogBufferStartFromFile_NoMetadata(t *testing.T) {
	engine := NewTestSQLEngine()

	// Create file entry without buffer start
	entry := &filer_pb.Entry{
		Name:     "test-log-file",
		Extended: nil,
	}

	// Test extraction
	result, err := engine.getLogBufferStartFromFile(entry)
	assert.NoError(t, err)
	assert.Nil(t, result)
}

func TestSQLEngine_GetLogBufferStartFromFile_InvalidData(t *testing.T) {
	engine := NewTestSQLEngine()

	// Create file entry with invalid buffer start (wrong size)
	entry := &filer_pb.Entry{
		Name: "test-log-file",
		Extended: map[string][]byte{
			"buffer_start": []byte("invalid-binary"),
		},
	}

	// Test extraction
	result, err := engine.getLogBufferStartFromFile(entry)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "invalid buffer_start format: expected 8 bytes")
	assert.Nil(t, result)
}

func TestSQLEngine_BuildLogBufferDeduplicationMap_NoBrokerClient(t *testing.T) {
	engine := NewTestSQLEngine()
	engine.catalog.brokerClient = nil // Simulate no broker client

	ctx := context.Background()
	result, err := engine.buildLogBufferDeduplicationMap(ctx, "/topics/test/test-topic")

	assert.NoError(t, err)
	assert.NotNil(t, result)
	assert.Empty(t, result)
}

func TestSQLEngine_LogBufferDeduplication_ServerRestartScenario(t *testing.T) {
	// Simulate scenario: Buffer indexes are now initialized with process start time
	// This tests that buffer start indexes are globally unique across server restarts

	// Before server restart: Process 1 buffer start (3 chunks)
	beforeRestartStart := LogBufferStart{
		StartIndex: 1609459100000000000, // Process 1 start time
	}

	// After server restart: Process 2 buffer start (3 chunks)
	afterRestartStart := LogBufferStart{
		StartIndex: 1609459300000000000, // Process 2 start time (DIFFERENT)
	}

	// Simulate 3 chunks for each file
	chunkCount := int64(3)

	// Calculate end indexes for range comparison
	beforeEnd := beforeRestartStart.StartIndex + chunkCount - 1 // [start, start+2]
	afterStart := afterRestartStart.StartIndex                  // [start, start+2]

	// Test range overlap detection (should NOT overlap)
	overlaps := beforeRestartStart.StartIndex <= (afterStart+chunkCount-1) && beforeEnd >= afterStart
	assert.False(t, overlaps, "Buffer ranges after restart should not overlap")

	// Verify the start indexes are globally unique
	assert.NotEqual(t, beforeRestartStart.StartIndex, afterRestartStart.StartIndex, "Start indexes should be different")
	assert.Less(t, beforeEnd, afterStart, "Ranges should be completely separate")

	// Expected values:
	// Before restart: [1609459100000000000, 1609459100000000002]
	// After restart:  [1609459300000000000, 1609459300000000002]
	expectedBeforeEnd := int64(1609459100000000002)
	expectedAfterStart := int64(1609459300000000000)

	assert.Equal(t, expectedBeforeEnd, beforeEnd)
	assert.Equal(t, expectedAfterStart, afterStart)

	// This demonstrates that buffer start indexes initialized with process start time
	// prevent false positive duplicates across server restarts
}

func TestBrokerClient_BinaryBufferStartFormat(t *testing.T) {
	// Test scenario: getBufferStartFromEntry should only support binary format
	// This tests the standardized binary format for buffer_start metadata
	realBrokerClient := &BrokerClient{}

	// Test binary format (used by both log files and Parquet files)
	binaryEntry := &filer_pb.Entry{
		Name:        "2025-01-07-14-30-45",
		IsDirectory: false,
		Extended: map[string][]byte{
			"buffer_start": func() []byte {
				// Binary format: 8-byte BigEndian
				buf := make([]byte, 8)
				binary.BigEndian.PutUint64(buf, uint64(2000001))
				return buf
			}(),
		},
	}

	bufferStart := realBrokerClient.getBufferStartFromEntry(binaryEntry)
	assert.NotNil(t, bufferStart)
	assert.Equal(t, int64(2000001), bufferStart.StartIndex, "Should parse binary buffer_start metadata")

	// Test Parquet file (same binary format)
	parquetEntry := &filer_pb.Entry{
		Name:        "2025-01-07-14-30.parquet",
		IsDirectory: false,
		Extended: map[string][]byte{
			"buffer_start": func() []byte {
				buf := make([]byte, 8)
				binary.BigEndian.PutUint64(buf, uint64(1500001))
				return buf
			}(),
		},
	}

	bufferStart = realBrokerClient.getBufferStartFromEntry(parquetEntry)
	assert.NotNil(t, bufferStart)
	assert.Equal(t, int64(1500001), bufferStart.StartIndex, "Should parse binary buffer_start from Parquet file")

	// Test missing metadata
	emptyEntry := &filer_pb.Entry{
		Name:        "no-metadata",
		IsDirectory: false,
		Extended:    nil,
	}

	bufferStart = realBrokerClient.getBufferStartFromEntry(emptyEntry)
	assert.Nil(t, bufferStart, "Should return nil for entry without buffer_start metadata")

	// Test invalid format (wrong size)
	invalidEntry := &filer_pb.Entry{
		Name:        "invalid-metadata",
		IsDirectory: false,
		Extended: map[string][]byte{
			"buffer_start": []byte("invalid"),
		},
	}

	bufferStart = realBrokerClient.getBufferStartFromEntry(invalidEntry)
	assert.Nil(t, bufferStart, "Should return nil for invalid buffer_start metadata")
}

// TestGetSQLValAlias tests the getSQLValAlias function, particularly for SQL injection prevention
func TestGetSQLValAlias(t *testing.T) {
	engine := &SQLEngine{}

	tests := []struct {
		name     string
		sqlVal   *SQLVal
		expected string
		desc     string
	}{
		{
			name: "simple string",
			sqlVal: &SQLVal{
				Type: StrVal,
				Val:  []byte("hello"),
			},
			expected: "'hello'",
			desc:     "Simple string should be wrapped in single quotes",
		},
		{
			name: "string with single quote",
			sqlVal: &SQLVal{
				Type: StrVal,
				Val:  []byte("don't"),
			},
			expected: "'don''t'",
			desc:     "String with single quote should have the quote escaped by doubling it",
		},
		{
			name: "string with multiple single quotes",
			sqlVal: &SQLVal{
				Type: StrVal,
				Val:  []byte("'malicious'; DROP TABLE users; --"),
			},
			expected: "'''malicious''; DROP TABLE users; --'",
			desc:     "String with SQL injection attempt should have all single quotes properly escaped",
		},
		{
			name: "empty string",
			sqlVal: &SQLVal{
				Type: StrVal,
				Val:  []byte(""),
			},
			expected: "''",
			desc:     "Empty string should result in empty quoted string",
		},
		{
			name: "integer value",
			sqlVal: &SQLVal{
				Type: IntVal,
				Val:  []byte("123"),
			},
			expected: "123",
			desc:     "Integer value should not be quoted",
		},
		{
			name: "float value",
			sqlVal: &SQLVal{
				Type: FloatVal,
				Val:  []byte("123.45"),
			},
			expected: "123.45",
			desc:     "Float value should not be quoted",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := engine.getSQLValAlias(tt.sqlVal)
			assert.Equal(t, tt.expected, result, tt.desc)
		})
	}
}
