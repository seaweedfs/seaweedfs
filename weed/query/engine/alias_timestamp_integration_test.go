package engine

import (
	"strconv"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb/schema_pb"
	"github.com/stretchr/testify/assert"
)

// TestAliasTimestampIntegration tests that SQL aliases work correctly with timestamp query fixes
func TestAliasTimestampIntegration(t *testing.T) {
	engine := NewTestSQLEngine()

	// Use the exact timestamps from the original failing production queries
	originalFailingTimestamps := []int64{
		1756947416566456262, // Original failing query 1
		1756947416566439304, // Original failing query 2
		1756913789829292386, // Current data timestamp
	}

	t.Run("AliasWithLargeTimestamps", func(t *testing.T) {
		for i, timestamp := range originalFailingTimestamps {
			t.Run("Timestamp_"+strconv.Itoa(i+1), func(t *testing.T) {
				// Create test record
				testRecord := &schema_pb.RecordValue{
					Fields: map[string]*schema_pb.Value{
						"_ts_ns": {Kind: &schema_pb.Value_Int64Value{Int64Value: timestamp}},
						"id":     {Kind: &schema_pb.Value_Int64Value{Int64Value: int64(1000 + i)}},
					},
				}

				// Test equality with alias (this was the originally failing pattern)
				sql := "SELECT _ts_ns AS ts, id FROM test WHERE ts = " + strconv.FormatInt(timestamp, 10)
				stmt, err := ParseSQL(sql)
				assert.NoError(t, err, "Should parse alias equality query for timestamp %d", timestamp)

				selectStmt := stmt.(*SelectStatement)
				predicate, err := engine.buildPredicateWithContext(selectStmt.Where.Expr, selectStmt.SelectExprs)
				assert.NoError(t, err, "Should build predicate for large timestamp with alias")

				result := predicate(testRecord)
				assert.True(t, result, "Should match exact large timestamp using alias")

				// Test precision - off by 1 nanosecond should not match
				sqlOffBy1 := "SELECT _ts_ns AS ts, id FROM test WHERE ts = " + strconv.FormatInt(timestamp+1, 10)
				stmt2, err := ParseSQL(sqlOffBy1)
				assert.NoError(t, err)
				selectStmt2 := stmt2.(*SelectStatement)
				predicate2, err := engine.buildPredicateWithContext(selectStmt2.Where.Expr, selectStmt2.SelectExprs)
				assert.NoError(t, err)

				result2 := predicate2(testRecord)
				assert.False(t, result2, "Should not match timestamp off by 1 nanosecond with alias")
			})
		}
	})

	t.Run("AliasWithTimestampRangeQueries", func(t *testing.T) {
		timestamp := int64(1756947416566456262)

		testRecords := []*schema_pb.RecordValue{
			{
				Fields: map[string]*schema_pb.Value{
					"_ts_ns": {Kind: &schema_pb.Value_Int64Value{Int64Value: timestamp - 2}}, // Before range
				},
			},
			{
				Fields: map[string]*schema_pb.Value{
					"_ts_ns": {Kind: &schema_pb.Value_Int64Value{Int64Value: timestamp}}, // In range
				},
			},
			{
				Fields: map[string]*schema_pb.Value{
					"_ts_ns": {Kind: &schema_pb.Value_Int64Value{Int64Value: timestamp + 2}}, // After range
				},
			},
		}

		// Test range query with alias
		sql := "SELECT _ts_ns AS ts FROM test WHERE ts >= " +
			strconv.FormatInt(timestamp-1, 10) + " AND ts <= " +
			strconv.FormatInt(timestamp+1, 10)
		stmt, err := ParseSQL(sql)
		assert.NoError(t, err, "Should parse range query with alias")

		selectStmt := stmt.(*SelectStatement)
		predicate, err := engine.buildPredicateWithContext(selectStmt.Where.Expr, selectStmt.SelectExprs)
		assert.NoError(t, err, "Should build range predicate with alias")

		// Test each record
		assert.False(t, predicate(testRecords[0]), "Should not match record before range")
		assert.True(t, predicate(testRecords[1]), "Should match record in range")
		assert.False(t, predicate(testRecords[2]), "Should not match record after range")
	})

	t.Run("AliasWithTimestampPrecisionEdgeCases", func(t *testing.T) {
		// Test maximum int64 value
		maxInt64 := int64(9223372036854775807)
		testRecord := &schema_pb.RecordValue{
			Fields: map[string]*schema_pb.Value{
				"_ts_ns": {Kind: &schema_pb.Value_Int64Value{Int64Value: maxInt64}},
			},
		}

		// Test with alias
		sql := "SELECT _ts_ns AS ts FROM test WHERE ts = " + strconv.FormatInt(maxInt64, 10)
		stmt, err := ParseSQL(sql)
		assert.NoError(t, err, "Should parse max int64 with alias")

		selectStmt := stmt.(*SelectStatement)
		predicate, err := engine.buildPredicateWithContext(selectStmt.Where.Expr, selectStmt.SelectExprs)
		assert.NoError(t, err, "Should build predicate for max int64 with alias")

		result := predicate(testRecord)
		assert.True(t, result, "Should handle max int64 value correctly with alias")

		// Test minimum value
		minInt64 := int64(-9223372036854775808)
		testRecord2 := &schema_pb.RecordValue{
			Fields: map[string]*schema_pb.Value{
				"_ts_ns": {Kind: &schema_pb.Value_Int64Value{Int64Value: minInt64}},
			},
		}

		sql2 := "SELECT _ts_ns AS ts FROM test WHERE ts = " + strconv.FormatInt(minInt64, 10)
		stmt2, err := ParseSQL(sql2)
		assert.NoError(t, err)
		selectStmt2 := stmt2.(*SelectStatement)
		predicate2, err := engine.buildPredicateWithContext(selectStmt2.Where.Expr, selectStmt2.SelectExprs)
		assert.NoError(t, err)

		result2 := predicate2(testRecord2)
		assert.True(t, result2, "Should handle min int64 value correctly with alias")
	})

	t.Run("MultipleAliasesWithTimestamps", func(t *testing.T) {
		// Test multiple aliases including timestamps
		timestamp1 := int64(1756947416566456262)
		timestamp2 := int64(1756913789829292386)

		testRecord := &schema_pb.RecordValue{
			Fields: map[string]*schema_pb.Value{
				"_ts_ns":     {Kind: &schema_pb.Value_Int64Value{Int64Value: timestamp1}},
				"created_at": {Kind: &schema_pb.Value_Int64Value{Int64Value: timestamp2}},
				"id":         {Kind: &schema_pb.Value_Int64Value{Int64Value: 12345}},
			},
		}

		// Use multiple timestamp aliases in WHERE
		sql := "SELECT _ts_ns AS event_time, created_at AS created_time, id AS record_id FROM test " +
			"WHERE event_time = " + strconv.FormatInt(timestamp1, 10) +
			" AND created_time = " + strconv.FormatInt(timestamp2, 10) +
			" AND record_id = 12345"

		stmt, err := ParseSQL(sql)
		assert.NoError(t, err, "Should parse complex query with multiple timestamp aliases")

		selectStmt := stmt.(*SelectStatement)
		predicate, err := engine.buildPredicateWithContext(selectStmt.Where.Expr, selectStmt.SelectExprs)
		assert.NoError(t, err, "Should build predicate for multiple timestamp aliases")

		result := predicate(testRecord)
		assert.True(t, result, "Should match complex query with multiple timestamp aliases")
	})

	t.Run("CompatibilityWithExistingTimestampFixes", func(t *testing.T) {
		// Verify that all the timestamp fixes (precision, scan boundaries, etc.) still work with aliases
		largeTimestamp := int64(1756947416566456262)

		// Test all comparison operators with aliases
		operators := []struct {
			sql      string
			value    int64
			expected bool
		}{
			{"ts = " + strconv.FormatInt(largeTimestamp, 10), largeTimestamp, true},
			{"ts = " + strconv.FormatInt(largeTimestamp+1, 10), largeTimestamp, false},
			{"ts > " + strconv.FormatInt(largeTimestamp-1, 10), largeTimestamp, true},
			{"ts > " + strconv.FormatInt(largeTimestamp, 10), largeTimestamp, false},
			{"ts >= " + strconv.FormatInt(largeTimestamp, 10), largeTimestamp, true},
			{"ts >= " + strconv.FormatInt(largeTimestamp+1, 10), largeTimestamp, false},
			{"ts < " + strconv.FormatInt(largeTimestamp+1, 10), largeTimestamp, true},
			{"ts < " + strconv.FormatInt(largeTimestamp, 10), largeTimestamp, false},
			{"ts <= " + strconv.FormatInt(largeTimestamp, 10), largeTimestamp, true},
			{"ts <= " + strconv.FormatInt(largeTimestamp-1, 10), largeTimestamp, false},
		}

		for _, op := range operators {
			t.Run(op.sql, func(t *testing.T) {
				testRecord := &schema_pb.RecordValue{
					Fields: map[string]*schema_pb.Value{
						"_ts_ns": {Kind: &schema_pb.Value_Int64Value{Int64Value: op.value}},
					},
				}

				sql := "SELECT _ts_ns AS ts FROM test WHERE " + op.sql
				stmt, err := ParseSQL(sql)
				assert.NoError(t, err, "Should parse: %s", op.sql)

				selectStmt := stmt.(*SelectStatement)
				predicate, err := engine.buildPredicateWithContext(selectStmt.Where.Expr, selectStmt.SelectExprs)
				assert.NoError(t, err, "Should build predicate for: %s", op.sql)

				result := predicate(testRecord)
				assert.Equal(t, op.expected, result, "Alias operator test failed for: %s", op.sql)
			})
		}
	})

	t.Run("ProductionScenarioReproduction", func(t *testing.T) {
		// Reproduce the exact production scenario that was originally failing

		// This was the original failing pattern from the user
		originalFailingSQL := "select id, _ts_ns as ts from ecommerce.user_events where ts = 1756913789829292386"

		testRecord := &schema_pb.RecordValue{
			Fields: map[string]*schema_pb.Value{
				"_ts_ns": {Kind: &schema_pb.Value_Int64Value{Int64Value: 1756913789829292386}},
				"id":     {Kind: &schema_pb.Value_Int64Value{Int64Value: 82460}},
			},
		}

		stmt, err := ParseSQL(originalFailingSQL)
		assert.NoError(t, err, "Should parse the exact originally failing production query")

		selectStmt := stmt.(*SelectStatement)
		predicate, err := engine.buildPredicateWithContext(selectStmt.Where.Expr, selectStmt.SelectExprs)
		assert.NoError(t, err, "Should build predicate for original failing query")

		result := predicate(testRecord)
		assert.True(t, result, "The originally failing production query should now work perfectly")

		// Also test the other originally failing timestamp
		originalFailingSQL2 := "select id, _ts_ns as ts from ecommerce.user_events where ts = 1756947416566456262"
		testRecord2 := &schema_pb.RecordValue{
			Fields: map[string]*schema_pb.Value{
				"_ts_ns": {Kind: &schema_pb.Value_Int64Value{Int64Value: 1756947416566456262}},
				"id":     {Kind: &schema_pb.Value_Int64Value{Int64Value: 897795}},
			},
		}

		stmt2, err := ParseSQL(originalFailingSQL2)
		assert.NoError(t, err)
		selectStmt2 := stmt2.(*SelectStatement)
		predicate2, err := engine.buildPredicateWithContext(selectStmt2.Where.Expr, selectStmt2.SelectExprs)
		assert.NoError(t, err)

		result2 := predicate2(testRecord2)
		assert.True(t, result2, "The second originally failing production query should now work perfectly")
	})
}
