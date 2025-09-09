package engine

import (
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb/schema_pb"
	"github.com/stretchr/testify/assert"
)

// TestExecutionPlanFastPathDisplay tests that the execution plan correctly shows
// "Parquet Statistics (fast path)" when fast path is used, not "Parquet Files (full scan)"
func TestExecutionPlanFastPathDisplay(t *testing.T) {
	engine := NewMockSQLEngine()

	// Create realistic data sources for fast path scenario
	dataSources := &TopicDataSources{
		ParquetFiles: map[string][]*ParquetFileStats{
			"/topics/test/topic/partition-1": {
				{
					RowCount: 500,
					ColumnStats: map[string]*ParquetColumnStats{
						"id": {
							ColumnName: "id",
							MinValue:   &schema_pb.Value{Kind: &schema_pb.Value_Int64Value{Int64Value: 1}},
							MaxValue:   &schema_pb.Value{Kind: &schema_pb.Value_Int64Value{Int64Value: 500}},
							NullCount:  0,
							RowCount:   500,
						},
					},
				},
			},
		},
		ParquetRowCount: 500,
		LiveLogRowCount: 0, // Pure parquet scenario - ideal for fast path
		PartitionsCount: 1,
	}

	t.Run("Fast path execution plan shows correct data sources", func(t *testing.T) {
		optimizer := NewFastPathOptimizer(engine.SQLEngine)

		aggregations := []AggregationSpec{
			{Function: FuncCOUNT, Column: "*", Alias: "COUNT(*)"},
		}

		// Test the strategy determination
		strategy := optimizer.DetermineStrategy(aggregations)
		assert.True(t, strategy.CanUseFastPath, "Strategy should allow fast path for COUNT(*)")
		assert.Equal(t, "all_aggregations_supported", strategy.Reason)

		// Test data source list building
		builder := &ExecutionPlanBuilder{}
		dataSources := &TopicDataSources{
			ParquetFiles: map[string][]*ParquetFileStats{
				"/topics/test/topic/partition-1": {
					{RowCount: 500},
				},
			},
			ParquetRowCount: 500,
			LiveLogRowCount: 0,
			PartitionsCount: 1,
		}

		dataSourcesList := builder.buildDataSourcesList(strategy, dataSources)

		// When fast path is used, should show "parquet_stats" not "parquet_files"
		assert.Contains(t, dataSourcesList, "parquet_stats",
			"Data sources should contain 'parquet_stats' when fast path is used")
		assert.NotContains(t, dataSourcesList, "parquet_files",
			"Data sources should NOT contain 'parquet_files' when fast path is used")

		// Test that the formatting works correctly
		formattedSource := engine.SQLEngine.formatDataSource("parquet_stats")
		assert.Equal(t, "Parquet Statistics (fast path)", formattedSource,
			"parquet_stats should format to 'Parquet Statistics (fast path)'")

		formattedFullScan := engine.SQLEngine.formatDataSource("parquet_files")
		assert.Equal(t, "Parquet Files (full scan)", formattedFullScan,
			"parquet_files should format to 'Parquet Files (full scan)'")
	})

	t.Run("Slow path execution plan shows full scan data sources", func(t *testing.T) {
		builder := &ExecutionPlanBuilder{}

		// Create strategy that cannot use fast path
		strategy := AggregationStrategy{
			CanUseFastPath: false,
			Reason:         "unsupported_aggregation_functions",
		}

		dataSourcesList := builder.buildDataSourcesList(strategy, dataSources)

		// When slow path is used, should show "parquet_files" and "live_logs"
		assert.Contains(t, dataSourcesList, "parquet_files",
			"Slow path should contain 'parquet_files'")
		assert.Contains(t, dataSourcesList, "live_logs",
			"Slow path should contain 'live_logs'")
		assert.NotContains(t, dataSourcesList, "parquet_stats",
			"Slow path should NOT contain 'parquet_stats'")
	})

	t.Run("Data source formatting works correctly", func(t *testing.T) {
		// Test just the data source formatting which is the key fix

		// Test parquet_stats formatting (fast path)
		fastPathFormatted := engine.SQLEngine.formatDataSource("parquet_stats")
		assert.Equal(t, "Parquet Statistics (fast path)", fastPathFormatted,
			"parquet_stats should format to show fast path usage")

		// Test parquet_files formatting (slow path)
		slowPathFormatted := engine.SQLEngine.formatDataSource("parquet_files")
		assert.Equal(t, "Parquet Files (full scan)", slowPathFormatted,
			"parquet_files should format to show full scan")

		// Test that data sources list is built correctly for fast path
		builder := &ExecutionPlanBuilder{}
		fastStrategy := AggregationStrategy{CanUseFastPath: true}

		fastSources := builder.buildDataSourcesList(fastStrategy, dataSources)
		assert.Contains(t, fastSources, "parquet_stats",
			"Fast path should include parquet_stats")
		assert.NotContains(t, fastSources, "parquet_files",
			"Fast path should NOT include parquet_files")

		// Test that data sources list is built correctly for slow path
		slowStrategy := AggregationStrategy{CanUseFastPath: false}

		slowSources := builder.buildDataSourcesList(slowStrategy, dataSources)
		assert.Contains(t, slowSources, "parquet_files",
			"Slow path should include parquet_files")
		assert.NotContains(t, slowSources, "parquet_stats",
			"Slow path should NOT include parquet_stats")
	})
}
