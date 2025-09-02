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
				{Function: "MIN", Column: "id"}, // lowercase column name
				{Function: "MAX", Column: "id"},
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
				{Function: "MIN", Column: "id"},
				{Function: "MAX", Column: "id"},
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
				{Function: "MIN", Column: "name"},
				{Function: "MAX", Column: "name"},
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
				{Function: "MIN", Column: "price"},
				{Function: "MAX", Column: "price"},
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
				{Function: "MIN", Column: "nonexistent_column"},
				{Function: "MAX", Column: "nonexistent_column"},
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
				{Function: "MIN", Column: "score"},
				{Function: "MAX", Column: "score"},
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
			aggregSpec: AggregationSpec{Function: "MIN", Column: "id"},
			expected:   int32(0), // Should extract the actual minimum value
		},
		{
			name:       "MAX should return 99 not empty",
			aggregSpec: AggregationSpec{Function: "MAX", Column: "id"},
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
			if tt.aggregSpec.Function == "MIN" {
				assert.NotNil(t, results[0].Min, "MIN result should not be nil")
				assert.Equal(t, tt.expected, results[0].Min)
			} else if tt.aggregSpec.Function == "MAX" {
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
			spec:     AggregationSpec{Function: "MIN", Column: "id"},
			result:   AggregationResult{Min: int32(0)},
			expected: "0",
		},
		{
			name:     "MAX with large value",
			spec:     AggregationSpec{Function: "MAX", Column: "id"},
			result:   AggregationResult{Max: int32(99)},
			expected: "99",
		},
		{
			name:     "MIN with negative value",
			spec:     AggregationSpec{Function: "MIN", Column: "score"},
			result:   AggregationResult{Min: int64(-50)},
			expected: "-50",
		},
		{
			name:     "MAX with float value",
			spec:     AggregationSpec{Function: "MAX", Column: "price"},
			result:   AggregationResult{Max: float64(299.99)},
			expected: "299.99",
		},
		{
			name:     "MIN with string value",
			spec:     AggregationSpec{Function: "MIN", Column: "name"},
			result:   AggregationResult{Min: "Alice"},
			expected: "Alice",
		},
		{
			name:     "MIN with nil should return NULL",
			spec:     AggregationSpec{Function: "MIN", Column: "missing"},
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
			aggregSpec:    AggregationSpec{Function: "MIN", Column: "id", Alias: "MIN(id)"},
			aggResult:     AggregationResult{Min: int32(0)}, // This was returning empty before fix
			expectedEmpty: false,
			expectedValue: "0",
		},
		{
			name:          "MAX with valid value should not be empty",
			aggregSpec:    AggregationSpec{Function: "MAX", Column: "id", Alias: "MAX(id)"},
			aggResult:     AggregationResult{Max: int32(99)},
			expectedEmpty: false,
			expectedValue: "99",
		},
		{
			name:          "MIN with negative value should work",
			aggregSpec:    AggregationSpec{Function: "MIN", Column: "score", Alias: "MIN(score)"},
			aggResult:     AggregationResult{Min: int64(-10)},
			expectedEmpty: false,
			expectedValue: "-10",
		},
		{
			name:          "MIN with nil should be empty (expected behavior)",
			aggregSpec:    AggregationSpec{Function: "MIN", Column: "missing", Alias: "MIN(missing)"},
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
				{Function: "MIN", Column: "id", Alias: "MIN(id)"},
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
				{Function: "MAX", Column: "id", Alias: "MAX(id)"},
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
				{Function: "MIN", Column: "id", Alias: "MIN(id)"},
				{Function: "MAX", Column: "id", Alias: "MAX(id)"},
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
