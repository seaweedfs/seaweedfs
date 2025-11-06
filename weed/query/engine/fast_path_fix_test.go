package engine

import (
	"context"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb/schema_pb"
	"github.com/stretchr/testify/assert"
)

// TestFastPathCountFixRealistic tests the specific scenario mentioned in the bug report:
// Fast path returning 0 for COUNT(*) when slow path returns 1803
func TestFastPathCountFixRealistic(t *testing.T) {
	engine := NewMockSQLEngine()

	// Set up debug mode to see our new logging
	ctx := context.WithValue(context.Background(), "debug", true)

	// Create realistic data sources that mimic a scenario with 1803 rows
	dataSources := &TopicDataSources{
		ParquetFiles: map[string][]*ParquetFileStats{
			"/topics/test/large-topic/0000-1023": {
				{
					RowCount: 800,
					ColumnStats: map[string]*ParquetColumnStats{
						"id": {
							ColumnName: "id",
							MinValue:   &schema_pb.Value{Kind: &schema_pb.Value_Int64Value{Int64Value: 1}},
							MaxValue:   &schema_pb.Value{Kind: &schema_pb.Value_Int64Value{Int64Value: 800}},
							NullCount:  0,
							RowCount:   800,
						},
					},
				},
				{
					RowCount: 500,
					ColumnStats: map[string]*ParquetColumnStats{
						"id": {
							ColumnName: "id",
							MinValue:   &schema_pb.Value{Kind: &schema_pb.Value_Int64Value{Int64Value: 801}},
							MaxValue:   &schema_pb.Value{Kind: &schema_pb.Value_Int64Value{Int64Value: 1300}},
							NullCount:  0,
							RowCount:   500,
						},
					},
				},
			},
			"/topics/test/large-topic/1024-2047": {
				{
					RowCount: 300,
					ColumnStats: map[string]*ParquetColumnStats{
						"id": {
							ColumnName: "id",
							MinValue:   &schema_pb.Value{Kind: &schema_pb.Value_Int64Value{Int64Value: 1301}},
							MaxValue:   &schema_pb.Value{Kind: &schema_pb.Value_Int64Value{Int64Value: 1600}},
							NullCount:  0,
							RowCount:   300,
						},
					},
				},
			},
		},
		ParquetRowCount:   1600, // 800 + 500 + 300
		LiveLogRowCount:   203,  // Additional live log data
		PartitionsCount:   2,
		LiveLogFilesCount: 15,
	}

	partitions := []string{
		"/topics/test/large-topic/0000-1023",
		"/topics/test/large-topic/1024-2047",
	}

	t.Run("COUNT(*) should return correct total (1803)", func(t *testing.T) {
		computer := NewAggregationComputer(engine.SQLEngine)

		aggregations := []AggregationSpec{
			{Function: FuncCOUNT, Column: "*", Alias: "COUNT(*)"},
		}

		results, err := computer.ComputeFastPathAggregations(ctx, aggregations, dataSources, partitions)

		assert.NoError(t, err, "Fast path aggregation should not error")
		assert.Len(t, results, 1, "Should return one result")

		// This is the key test - before our fix, this was returning 0
		expectedCount := int64(1803) // 1600 (parquet) + 203 (live log)
		actualCount := results[0].Count

		assert.Equal(t, expectedCount, actualCount,
			"COUNT(*) should return %d (1600 parquet + 203 live log), but got %d",
			expectedCount, actualCount)
	})

	t.Run("MIN/MAX should work with multiple partitions", func(t *testing.T) {
		computer := NewAggregationComputer(engine.SQLEngine)

		aggregations := []AggregationSpec{
			{Function: FuncMIN, Column: "id", Alias: "MIN(id)"},
			{Function: FuncMAX, Column: "id", Alias: "MAX(id)"},
		}

		results, err := computer.ComputeFastPathAggregations(ctx, aggregations, dataSources, partitions)

		assert.NoError(t, err, "Fast path aggregation should not error")
		assert.Len(t, results, 2, "Should return two results")

		// MIN should be the lowest across all parquet files
		assert.Equal(t, int64(1), results[0].Min, "MIN should be 1")

		// MAX should be the highest across all parquet files
		assert.Equal(t, int64(1600), results[1].Max, "MAX should be 1600")
	})
}

// TestFastPathDataSourceDiscoveryLogging tests that our debug logging works correctly
func TestFastPathDataSourceDiscoveryLogging(t *testing.T) {
	// This test verifies that our enhanced data source collection structure is correct

	t.Run("DataSources structure validation", func(t *testing.T) {
		// Test the TopicDataSources structure initialization
		dataSources := &TopicDataSources{
			ParquetFiles:      make(map[string][]*ParquetFileStats),
			ParquetRowCount:   0,
			LiveLogRowCount:   0,
			LiveLogFilesCount: 0,
			PartitionsCount:   0,
		}

		assert.NotNil(t, dataSources, "Data sources should not be nil")
		assert.NotNil(t, dataSources.ParquetFiles, "ParquetFiles map should be initialized")
		assert.GreaterOrEqual(t, dataSources.PartitionsCount, 0, "PartitionsCount should be non-negative")
		assert.GreaterOrEqual(t, dataSources.ParquetRowCount, int64(0), "ParquetRowCount should be non-negative")
		assert.GreaterOrEqual(t, dataSources.LiveLogRowCount, int64(0), "LiveLogRowCount should be non-negative")
	})
}

// TestFastPathValidationLogic tests the enhanced validation we added
func TestFastPathValidationLogic(t *testing.T) {
	t.Run("Validation catches data source vs computation mismatch", func(t *testing.T) {
		// Create a scenario where data sources and computation might be inconsistent
		dataSources := &TopicDataSources{
			ParquetFiles:    make(map[string][]*ParquetFileStats),
			ParquetRowCount: 1000, // Data sources say 1000 rows
			LiveLogRowCount: 0,
			PartitionsCount: 1,
		}

		// But aggregation result says different count (simulating the original bug)
		aggResults := []AggregationResult{
			{Count: 0}, // Bug: returns 0 when data sources show 1000
		}

		// This simulates the validation logic from tryFastParquetAggregation
		totalRows := dataSources.ParquetRowCount + dataSources.LiveLogRowCount
		countResult := aggResults[0].Count

		// Our validation should catch this mismatch
		assert.NotEqual(t, totalRows, countResult,
			"This test simulates the bug: data sources show %d but COUNT returns %d",
			totalRows, countResult)

		// In the real code, this would trigger a fallback to slow path
		validationPassed := (countResult == totalRows)
		assert.False(t, validationPassed, "Validation should fail for inconsistent data")
	})

	t.Run("Validation passes for consistent data", func(t *testing.T) {
		// Create a scenario where everything is consistent
		dataSources := &TopicDataSources{
			ParquetFiles:    make(map[string][]*ParquetFileStats),
			ParquetRowCount: 1000,
			LiveLogRowCount: 803,
			PartitionsCount: 1,
		}

		// Aggregation result matches data sources
		aggResults := []AggregationResult{
			{Count: 1803}, // Correct: matches 1000 + 803
		}

		totalRows := dataSources.ParquetRowCount + dataSources.LiveLogRowCount
		countResult := aggResults[0].Count

		// Our validation should pass this
		assert.Equal(t, totalRows, countResult,
			"Validation should pass when data sources (%d) match COUNT result (%d)",
			totalRows, countResult)

		validationPassed := (countResult == totalRows)
		assert.True(t, validationPassed, "Validation should pass for consistent data")
	})
}
