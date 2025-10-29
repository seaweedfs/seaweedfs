package engine

import (
	"strconv"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb/schema_pb"
	"github.com/stretchr/testify/assert"
)

// TestTimestampQueryFixes tests all the timestamp query fixes comprehensively
func TestTimestampQueryFixes(t *testing.T) {
	engine := NewTestSQLEngine()

	// Test timestamps from the actual failing cases
	largeTimestamp1 := int64(1756947416566456262) // Original failing query
	largeTimestamp2 := int64(1756947416566439304) // Second failing query
	largeTimestamp3 := int64(1756913789829292386) // Current data timestamp

	t.Run("Fix1_PrecisionLoss", func(t *testing.T) {
		// Test that large int64 timestamps don't lose precision in comparisons
		testRecord := &schema_pb.RecordValue{
			Fields: map[string]*schema_pb.Value{
				"_ts_ns": {Kind: &schema_pb.Value_Int64Value{Int64Value: largeTimestamp1}},
				"id":     {Kind: &schema_pb.Value_Int64Value{Int64Value: 12345}},
			},
		}

		// Test equality comparison
		result := engine.valuesEqual(testRecord.Fields["_ts_ns"], largeTimestamp1)
		assert.True(t, result, "Large timestamp equality should work without precision loss")

		// Test inequality comparison
		result = engine.valuesEqual(testRecord.Fields["_ts_ns"], largeTimestamp1+1)
		assert.False(t, result, "Large timestamp inequality should be detected accurately")

		// Test less than comparison
		result = engine.valueLessThan(testRecord.Fields["_ts_ns"], largeTimestamp1+1)
		assert.True(t, result, "Large timestamp less-than should work without precision loss")

		// Test greater than comparison
		result = engine.valueGreaterThan(testRecord.Fields["_ts_ns"], largeTimestamp1-1)
		assert.True(t, result, "Large timestamp greater-than should work without precision loss")
	})

	t.Run("Fix2_TimeFilterExtraction", func(t *testing.T) {
		// Test that equality queries don't set stopTimeNs (which causes premature termination)
		equalitySQL := "SELECT * FROM test WHERE _ts_ns = " + strconv.FormatInt(largeTimestamp2, 10)
		stmt, err := ParseSQL(equalitySQL)
		assert.NoError(t, err)

		selectStmt := stmt.(*SelectStatement)
		startTimeNs, stopTimeNs := engine.extractTimeFilters(selectStmt.Where.Expr)

		assert.Equal(t, largeTimestamp2-1, startTimeNs, "Equality query should set startTimeNs to target-1")
		assert.Equal(t, int64(0), stopTimeNs, "Equality query should NOT set stopTimeNs to avoid early termination")
	})

	t.Run("Fix3_RangeBoundaryFix", func(t *testing.T) {
		// Test that range queries with equal boundaries don't cause premature termination
		rangeSQL := "SELECT * FROM test WHERE _ts_ns >= " + strconv.FormatInt(largeTimestamp3, 10) +
			" AND _ts_ns <= " + strconv.FormatInt(largeTimestamp3, 10)
		stmt, err := ParseSQL(rangeSQL)
		assert.NoError(t, err)

		selectStmt := stmt.(*SelectStatement)
		startTimeNs, stopTimeNs := engine.extractTimeFilters(selectStmt.Where.Expr)

		// Should be treated like an equality query to avoid premature termination
		assert.NotEqual(t, int64(0), startTimeNs, "Range with equal boundaries should set startTimeNs")
		assert.Equal(t, int64(0), stopTimeNs, "Range with equal boundaries should NOT set stopTimeNs")
	})

	t.Run("Fix4_DifferentRangeBoundaries", func(t *testing.T) {
		// Test that normal range queries still work correctly
		rangeSQL := "SELECT * FROM test WHERE _ts_ns >= " + strconv.FormatInt(largeTimestamp1, 10) +
			" AND _ts_ns <= " + strconv.FormatInt(largeTimestamp2, 10)
		stmt, err := ParseSQL(rangeSQL)
		assert.NoError(t, err)

		selectStmt := stmt.(*SelectStatement)
		startTimeNs, stopTimeNs := engine.extractTimeFilters(selectStmt.Where.Expr)

		assert.Equal(t, largeTimestamp1, startTimeNs, "Range query should set correct startTimeNs")
		assert.Equal(t, largeTimestamp2, stopTimeNs, "Range query should set correct stopTimeNs")
	})

	t.Run("Fix5_PredicateAccuracy", func(t *testing.T) {
		// Test that predicates correctly evaluate large timestamp equality
		equalitySQL := "SELECT * FROM test WHERE _ts_ns = " + strconv.FormatInt(largeTimestamp1, 10)
		stmt, err := ParseSQL(equalitySQL)
		assert.NoError(t, err)

		selectStmt := stmt.(*SelectStatement)
		predicate, err := engine.buildPredicate(selectStmt.Where.Expr)
		assert.NoError(t, err)

		// Test with matching record
		matchingRecord := &schema_pb.RecordValue{
			Fields: map[string]*schema_pb.Value{
				"_ts_ns": {Kind: &schema_pb.Value_Int64Value{Int64Value: largeTimestamp1}},
				"id":     {Kind: &schema_pb.Value_Int64Value{Int64Value: 897795}},
			},
		}

		result := predicate(matchingRecord)
		assert.True(t, result, "Predicate should match record with exact timestamp")

		// Test with non-matching record
		nonMatchingRecord := &schema_pb.RecordValue{
			Fields: map[string]*schema_pb.Value{
				"_ts_ns": {Kind: &schema_pb.Value_Int64Value{Int64Value: largeTimestamp1 + 1}},
				"id":     {Kind: &schema_pb.Value_Int64Value{Int64Value: 12345}},
			},
		}

		result = predicate(nonMatchingRecord)
		assert.False(t, result, "Predicate should NOT match record with different timestamp")
	})

	t.Run("Fix6_ComparisonOperators", func(t *testing.T) {
		// Test all comparison operators work correctly with large timestamps
		testRecord := &schema_pb.RecordValue{
			Fields: map[string]*schema_pb.Value{
				"_ts_ns": {Kind: &schema_pb.Value_Int64Value{Int64Value: largeTimestamp2}},
			},
		}

		operators := []struct {
			sql      string
			expected bool
		}{
			{"_ts_ns = " + strconv.FormatInt(largeTimestamp2, 10), true},
			{"_ts_ns = " + strconv.FormatInt(largeTimestamp2+1, 10), false},
			{"_ts_ns > " + strconv.FormatInt(largeTimestamp2-1, 10), true},
			{"_ts_ns > " + strconv.FormatInt(largeTimestamp2, 10), false},
			{"_ts_ns >= " + strconv.FormatInt(largeTimestamp2, 10), true},
			{"_ts_ns >= " + strconv.FormatInt(largeTimestamp2+1, 10), false},
			{"_ts_ns < " + strconv.FormatInt(largeTimestamp2+1, 10), true},
			{"_ts_ns < " + strconv.FormatInt(largeTimestamp2, 10), false},
			{"_ts_ns <= " + strconv.FormatInt(largeTimestamp2, 10), true},
			{"_ts_ns <= " + strconv.FormatInt(largeTimestamp2-1, 10), false},
		}

		for _, op := range operators {
			sql := "SELECT * FROM test WHERE " + op.sql
			stmt, err := ParseSQL(sql)
			assert.NoError(t, err, "Should parse SQL: %s", op.sql)

			selectStmt := stmt.(*SelectStatement)
			predicate, err := engine.buildPredicate(selectStmt.Where.Expr)
			assert.NoError(t, err, "Should build predicate for: %s", op.sql)

			result := predicate(testRecord)
			assert.Equal(t, op.expected, result, "Operator test failed for: %s", op.sql)
		}
	})

	t.Run("Fix7_EdgeCases", func(t *testing.T) {
		// Test edge cases and boundary conditions

		// Maximum int64 value
		maxInt64 := int64(9223372036854775807)
		testRecord := &schema_pb.RecordValue{
			Fields: map[string]*schema_pb.Value{
				"_ts_ns": {Kind: &schema_pb.Value_Int64Value{Int64Value: maxInt64}},
			},
		}

		// Test equality with maximum int64
		result := engine.valuesEqual(testRecord.Fields["_ts_ns"], maxInt64)
		assert.True(t, result, "Should handle maximum int64 value correctly")

		// Test with zero timestamp
		zeroRecord := &schema_pb.RecordValue{
			Fields: map[string]*schema_pb.Value{
				"_ts_ns": {Kind: &schema_pb.Value_Int64Value{Int64Value: 0}},
			},
		}

		result = engine.valuesEqual(zeroRecord.Fields["_ts_ns"], int64(0))
		assert.True(t, result, "Should handle zero timestamp correctly")
	})
}

// TestOriginalFailingQueries tests the specific queries that were failing before the fixes
func TestOriginalFailingQueries(t *testing.T) {
	engine := NewTestSQLEngine()

	failingQueries := []struct {
		name      string
		sql       string
		timestamp int64
		id        int64
	}{
		{
			name:      "OriginalQuery1",
			sql:       "select id, _ts_ns from ecommerce.user_events where _ts_ns = 1756947416566456262",
			timestamp: 1756947416566456262,
			id:        897795,
		},
		{
			name:      "OriginalQuery2",
			sql:       "select id, _ts_ns from ecommerce.user_events where _ts_ns = 1756947416566439304",
			timestamp: 1756947416566439304,
			id:        715356,
		},
		{
			name:      "CurrentDataQuery",
			sql:       "select id, _ts_ns from ecommerce.user_events where _ts_ns = 1756913789829292386",
			timestamp: 1756913789829292386,
			id:        82460,
		},
	}

	for _, query := range failingQueries {
		t.Run(query.name, func(t *testing.T) {
			// Parse the SQL
			stmt, err := ParseSQL(query.sql)
			assert.NoError(t, err, "Should parse the failing query")

			selectStmt := stmt.(*SelectStatement)

			// Test time filter extraction
			startTimeNs, stopTimeNs := engine.extractTimeFilters(selectStmt.Where.Expr)
			assert.Equal(t, query.timestamp-1, startTimeNs, "Should set startTimeNs to timestamp-1")
			assert.Equal(t, int64(0), stopTimeNs, "Should not set stopTimeNs for equality")

			// Test predicate building and evaluation
			predicate, err := engine.buildPredicate(selectStmt.Where.Expr)
			assert.NoError(t, err, "Should build predicate")

			// Test with matching record
			matchingRecord := &schema_pb.RecordValue{
				Fields: map[string]*schema_pb.Value{
					"_ts_ns": {Kind: &schema_pb.Value_Int64Value{Int64Value: query.timestamp}},
					"id":     {Kind: &schema_pb.Value_Int64Value{Int64Value: query.id}},
				},
			}

			result := predicate(matchingRecord)
			assert.True(t, result, "Predicate should match the target record for query: %s", query.name)
		})
	}
}
