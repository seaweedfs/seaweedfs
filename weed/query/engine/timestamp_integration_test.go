package engine

import (
	"strconv"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb/schema_pb"
	"github.com/stretchr/testify/assert"
)

// TestTimestampIntegrationScenarios tests complete end-to-end scenarios
func TestTimestampIntegrationScenarios(t *testing.T) {
	engine := NewTestSQLEngine()

	// Simulate the exact timestamps that were failing in production
	timestamps := []struct {
		timestamp int64
		id        int64
		name      string
	}{
		{1756947416566456262, 897795, "original_failing_1"},
		{1756947416566439304, 715356, "original_failing_2"},
		{1756913789829292386, 82460, "current_data"},
	}

	t.Run("EndToEndTimestampEquality", func(t *testing.T) {
		for _, ts := range timestamps {
			t.Run(ts.name, func(t *testing.T) {
				// Create a test record
				record := &schema_pb.RecordValue{
					Fields: map[string]*schema_pb.Value{
						"_ts_ns": {Kind: &schema_pb.Value_Int64Value{Int64Value: ts.timestamp}},
						"id":     {Kind: &schema_pb.Value_Int64Value{Int64Value: ts.id}},
					},
				}

				// Build SQL query
				sql := "SELECT id, _ts_ns FROM test WHERE _ts_ns = " + strconv.FormatInt(ts.timestamp, 10)
				stmt, err := ParseSQL(sql)
				assert.NoError(t, err)

				selectStmt := stmt.(*SelectStatement)

				// Test time filter extraction (Fix #2 and #5)
				startTimeNs, stopTimeNs := engine.extractTimeFilters(selectStmt.Where.Expr)
				assert.Equal(t, ts.timestamp-1, startTimeNs, "Should set startTimeNs to avoid scan boundary bug")
				assert.Equal(t, int64(0), stopTimeNs, "Should not set stopTimeNs to avoid premature termination")

				// Test predicate building (Fix #1)
				predicate, err := engine.buildPredicate(selectStmt.Where.Expr)
				assert.NoError(t, err)

				// Test predicate evaluation (Fix #1 - precision)
				result := predicate(record)
				assert.True(t, result, "Should match exact timestamp without precision loss")

				// Test that close but different timestamps don't match
				closeRecord := &schema_pb.RecordValue{
					Fields: map[string]*schema_pb.Value{
						"_ts_ns": {Kind: &schema_pb.Value_Int64Value{Int64Value: ts.timestamp + 1}},
						"id":     {Kind: &schema_pb.Value_Int64Value{Int64Value: ts.id}},
					},
				}
				result = predicate(closeRecord)
				assert.False(t, result, "Should not match timestamp that differs by 1 nanosecond")
			})
		}
	})

	t.Run("ComplexRangeQueries", func(t *testing.T) {
		// Test range queries that combine multiple fixes
		testCases := []struct {
			name      string
			sql       string
			shouldSet struct{ start, stop bool }
		}{
			{
				name:      "RangeWithDifferentBounds",
				sql:       "SELECT * FROM test WHERE _ts_ns >= 1756913789829292386 AND _ts_ns <= 1756947416566456262",
				shouldSet: struct{ start, stop bool }{true, true},
			},
			{
				name:      "RangeWithSameBounds",
				sql:       "SELECT * FROM test WHERE _ts_ns >= 1756913789829292386 AND _ts_ns <= 1756913789829292386",
				shouldSet: struct{ start, stop bool }{true, false}, // Fix #4: equal bounds should not set stop
			},
			{
				name:      "OpenEndedRange",
				sql:       "SELECT * FROM test WHERE _ts_ns >= 1756913789829292386",
				shouldSet: struct{ start, stop bool }{true, false},
			},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				stmt, err := ParseSQL(tc.sql)
				assert.NoError(t, err)

				selectStmt := stmt.(*SelectStatement)
				startTimeNs, stopTimeNs := engine.extractTimeFilters(selectStmt.Where.Expr)

				if tc.shouldSet.start {
					assert.NotEqual(t, int64(0), startTimeNs, "Should set startTimeNs for range query")
				} else {
					assert.Equal(t, int64(0), startTimeNs, "Should not set startTimeNs")
				}

				if tc.shouldSet.stop {
					assert.NotEqual(t, int64(0), stopTimeNs, "Should set stopTimeNs for bounded range")
				} else {
					assert.Equal(t, int64(0), stopTimeNs, "Should not set stopTimeNs")
				}
			})
		}
	})

	t.Run("ProductionScenarioReproduction", func(t *testing.T) {
		// This test reproduces the exact production scenario that was failing

		// Original failing query: WHERE _ts_ns = 1756947416566456262
		sql := "SELECT id, _ts_ns FROM ecommerce.user_events WHERE _ts_ns = 1756947416566456262"
		stmt, err := ParseSQL(sql)
		assert.NoError(t, err, "Should parse the production query that was failing")

		selectStmt := stmt.(*SelectStatement)

		// Verify time filter extraction works correctly (fixes scan termination issue)
		startTimeNs, stopTimeNs := engine.extractTimeFilters(selectStmt.Where.Expr)
		assert.Equal(t, int64(1756947416566456261), startTimeNs, "Should set startTimeNs to target-1") // Fix #5
		assert.Equal(t, int64(0), stopTimeNs, "Should not set stopTimeNs")                             // Fix #2

		// Verify predicate handles the large timestamp correctly
		predicate, err := engine.buildPredicate(selectStmt.Where.Expr)
		assert.NoError(t, err, "Should build predicate for production query")

		// Test with the actual record that exists in production
		productionRecord := &schema_pb.RecordValue{
			Fields: map[string]*schema_pb.Value{
				"_ts_ns": {Kind: &schema_pb.Value_Int64Value{Int64Value: 1756947416566456262}},
				"id":     {Kind: &schema_pb.Value_Int64Value{Int64Value: 897795}},
			},
		}

		result := predicate(productionRecord)
		assert.True(t, result, "Should match the production record that was failing before") // Fix #1

		// Verify precision - test that a timestamp differing by just 1 nanosecond doesn't match
		slightlyDifferentRecord := &schema_pb.RecordValue{
			Fields: map[string]*schema_pb.Value{
				"_ts_ns": {Kind: &schema_pb.Value_Int64Value{Int64Value: 1756947416566456263}},
				"id":     {Kind: &schema_pb.Value_Int64Value{Int64Value: 897795}},
			},
		}

		result = predicate(slightlyDifferentRecord)
		assert.False(t, result, "Should NOT match record with timestamp differing by 1 nanosecond")
	})
}

// TestRegressionPrevention ensures the fixes don't break normal cases
func TestRegressionPrevention(t *testing.T) {
	engine := NewTestSQLEngine()

	t.Run("SmallTimestamps", func(t *testing.T) {
		// Ensure small timestamps still work normally
		smallTimestamp := int64(1234567890)

		record := &schema_pb.RecordValue{
			Fields: map[string]*schema_pb.Value{
				"_ts_ns": {Kind: &schema_pb.Value_Int64Value{Int64Value: smallTimestamp}},
			},
		}

		result := engine.valuesEqual(record.Fields["_ts_ns"], smallTimestamp)
		assert.True(t, result, "Small timestamps should continue to work")
	})

	t.Run("NonTimestampColumns", func(t *testing.T) {
		// Ensure non-timestamp columns aren't affected by timestamp fixes
		sql := "SELECT * FROM test WHERE id = 12345"
		stmt, err := ParseSQL(sql)
		assert.NoError(t, err)

		selectStmt := stmt.(*SelectStatement)
		startTimeNs, stopTimeNs := engine.extractTimeFilters(selectStmt.Where.Expr)

		assert.Equal(t, int64(0), startTimeNs, "Non-timestamp queries should not set startTimeNs")
		assert.Equal(t, int64(0), stopTimeNs, "Non-timestamp queries should not set stopTimeNs")
	})

	t.Run("StringComparisons", func(t *testing.T) {
		// Ensure string comparisons aren't affected
		record := &schema_pb.RecordValue{
			Fields: map[string]*schema_pb.Value{
				"name": {Kind: &schema_pb.Value_StringValue{StringValue: "test"}},
			},
		}

		result := engine.valuesEqual(record.Fields["name"], "test")
		assert.True(t, result, "String comparisons should continue to work")
	})
}
