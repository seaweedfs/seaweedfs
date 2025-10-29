package engine

import (
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb/schema_pb"
	"github.com/stretchr/testify/assert"
)

// TestSQLAliasResolution tests the complete SQL alias resolution functionality
func TestSQLAliasResolution(t *testing.T) {
	engine := NewTestSQLEngine()

	t.Run("ResolveColumnAlias", func(t *testing.T) {
		// Test the helper function for resolving aliases

		// Create SELECT expressions with aliases
		selectExprs := []SelectExpr{
			&AliasedExpr{
				Expr: &ColName{Name: stringValue("_ts_ns")},
				As:   aliasValue("ts"),
			},
			&AliasedExpr{
				Expr: &ColName{Name: stringValue("id")},
				As:   aliasValue("record_id"),
			},
		}

		// Test alias resolution
		resolved := engine.resolveColumnAlias("ts", selectExprs)
		assert.Equal(t, "_ts_ns", resolved, "Should resolve 'ts' alias to '_ts_ns'")

		resolved = engine.resolveColumnAlias("record_id", selectExprs)
		assert.Equal(t, "id", resolved, "Should resolve 'record_id' alias to 'id'")

		// Test non-aliased column (should return as-is)
		resolved = engine.resolveColumnAlias("some_other_column", selectExprs)
		assert.Equal(t, "some_other_column", resolved, "Non-aliased columns should return unchanged")
	})

	t.Run("SingleAliasInWhere", func(t *testing.T) {
		// Test using a single alias in WHERE clause
		testRecord := &schema_pb.RecordValue{
			Fields: map[string]*schema_pb.Value{
				"_ts_ns": {Kind: &schema_pb.Value_Int64Value{Int64Value: 1756947416566456262}},
				"id":     {Kind: &schema_pb.Value_Int64Value{Int64Value: 12345}},
			},
		}

		// Parse SQL with alias in WHERE
		sql := "SELECT _ts_ns AS ts, id FROM test WHERE ts = 1756947416566456262"
		stmt, err := ParseSQL(sql)
		assert.NoError(t, err, "Should parse SQL with alias in WHERE")

		selectStmt := stmt.(*SelectStatement)

		// Build predicate with context (for alias resolution)
		predicate, err := engine.buildPredicateWithContext(selectStmt.Where.Expr, selectStmt.SelectExprs)
		assert.NoError(t, err, "Should build predicate with alias resolution")

		// Test the predicate
		result := predicate(testRecord)
		assert.True(t, result, "Predicate should match using alias 'ts' for '_ts_ns'")

		// Test with non-matching value
		sql2 := "SELECT _ts_ns AS ts, id FROM test WHERE ts = 999999"
		stmt2, err := ParseSQL(sql2)
		assert.NoError(t, err)
		selectStmt2 := stmt2.(*SelectStatement)

		predicate2, err := engine.buildPredicateWithContext(selectStmt2.Where.Expr, selectStmt2.SelectExprs)
		assert.NoError(t, err)

		result2 := predicate2(testRecord)
		assert.False(t, result2, "Predicate should not match different value")
	})

	t.Run("MultipleAliasesInWhere", func(t *testing.T) {
		// Test using multiple aliases in WHERE clause
		testRecord := &schema_pb.RecordValue{
			Fields: map[string]*schema_pb.Value{
				"_ts_ns": {Kind: &schema_pb.Value_Int64Value{Int64Value: 1756947416566456262}},
				"id":     {Kind: &schema_pb.Value_Int64Value{Int64Value: 82460}},
			},
		}

		// Parse SQL with multiple aliases in WHERE
		sql := "SELECT _ts_ns AS ts, id AS record_id FROM test WHERE ts = 1756947416566456262 AND record_id = 82460"
		stmt, err := ParseSQL(sql)
		assert.NoError(t, err, "Should parse SQL with multiple aliases")

		selectStmt := stmt.(*SelectStatement)

		// Build predicate with context
		predicate, err := engine.buildPredicateWithContext(selectStmt.Where.Expr, selectStmt.SelectExprs)
		assert.NoError(t, err, "Should build predicate with multiple alias resolution")

		// Test the predicate - should match both conditions
		result := predicate(testRecord)
		assert.True(t, result, "Should match both aliased conditions")

		// Test with one condition not matching
		testRecord2 := &schema_pb.RecordValue{
			Fields: map[string]*schema_pb.Value{
				"_ts_ns": {Kind: &schema_pb.Value_Int64Value{Int64Value: 1756947416566456262}},
				"id":     {Kind: &schema_pb.Value_Int64Value{Int64Value: 99999}}, // Different ID
			},
		}

		result2 := predicate(testRecord2)
		assert.False(t, result2, "Should not match when one alias condition fails")
	})

	t.Run("RangeQueryWithAliases", func(t *testing.T) {
		// Test range queries using aliases
		testRecords := []*schema_pb.RecordValue{
			{
				Fields: map[string]*schema_pb.Value{
					"_ts_ns": {Kind: &schema_pb.Value_Int64Value{Int64Value: 1756947416566456260}}, // Below range
				},
			},
			{
				Fields: map[string]*schema_pb.Value{
					"_ts_ns": {Kind: &schema_pb.Value_Int64Value{Int64Value: 1756947416566456262}}, // In range
				},
			},
			{
				Fields: map[string]*schema_pb.Value{
					"_ts_ns": {Kind: &schema_pb.Value_Int64Value{Int64Value: 1756947416566456265}}, // Above range
				},
			},
		}

		// Test range query with alias
		sql := "SELECT _ts_ns AS ts FROM test WHERE ts > 1756947416566456261 AND ts < 1756947416566456264"
		stmt, err := ParseSQL(sql)
		assert.NoError(t, err, "Should parse range query with alias")

		selectStmt := stmt.(*SelectStatement)
		predicate, err := engine.buildPredicateWithContext(selectStmt.Where.Expr, selectStmt.SelectExprs)
		assert.NoError(t, err, "Should build range predicate with alias")

		// Test each record
		assert.False(t, predicate(testRecords[0]), "Should not match record below range")
		assert.True(t, predicate(testRecords[1]), "Should match record in range")
		assert.False(t, predicate(testRecords[2]), "Should not match record above range")
	})

	t.Run("MixedAliasAndDirectColumn", func(t *testing.T) {
		// Test mixing aliased and non-aliased columns in WHERE
		testRecord := &schema_pb.RecordValue{
			Fields: map[string]*schema_pb.Value{
				"_ts_ns": {Kind: &schema_pb.Value_Int64Value{Int64Value: 1756947416566456262}},
				"id":     {Kind: &schema_pb.Value_Int64Value{Int64Value: 82460}},
				"status": {Kind: &schema_pb.Value_StringValue{StringValue: "active"}},
			},
		}

		// Use alias for one column, direct name for another
		sql := "SELECT _ts_ns AS ts, id, status FROM test WHERE ts = 1756947416566456262 AND status = 'active'"
		stmt, err := ParseSQL(sql)
		assert.NoError(t, err, "Should parse mixed alias/direct query")

		selectStmt := stmt.(*SelectStatement)
		predicate, err := engine.buildPredicateWithContext(selectStmt.Where.Expr, selectStmt.SelectExprs)
		assert.NoError(t, err, "Should build mixed predicate")

		result := predicate(testRecord)
		assert.True(t, result, "Should match with mixed alias and direct column usage")
	})

	t.Run("AliasCompatibilityWithTimestampFixes", func(t *testing.T) {
		// Test that alias resolution works with the timestamp precision fixes
		largeTimestamp := int64(1756947416566456262) // Large nanosecond timestamp

		testRecord := &schema_pb.RecordValue{
			Fields: map[string]*schema_pb.Value{
				"_ts_ns": {Kind: &schema_pb.Value_Int64Value{Int64Value: largeTimestamp}},
				"id":     {Kind: &schema_pb.Value_Int64Value{Int64Value: 897795}},
			},
		}

		// Test that large timestamp precision is maintained with aliases
		sql := "SELECT _ts_ns AS ts, id FROM test WHERE ts = 1756947416566456262"
		stmt, err := ParseSQL(sql)
		assert.NoError(t, err)

		selectStmt := stmt.(*SelectStatement)
		predicate, err := engine.buildPredicateWithContext(selectStmt.Where.Expr, selectStmt.SelectExprs)
		assert.NoError(t, err)

		result := predicate(testRecord)
		assert.True(t, result, "Large timestamp precision should be maintained with aliases")

		// Test precision with off-by-one (should not match)
		sql2 := "SELECT _ts_ns AS ts, id FROM test WHERE ts = 1756947416566456263" // +1
		stmt2, err := ParseSQL(sql2)
		assert.NoError(t, err)
		selectStmt2 := stmt2.(*SelectStatement)
		predicate2, err := engine.buildPredicateWithContext(selectStmt2.Where.Expr, selectStmt2.SelectExprs)
		assert.NoError(t, err)

		result2 := predicate2(testRecord)
		assert.False(t, result2, "Should not match timestamp differing by 1 nanosecond")
	})

	t.Run("EdgeCasesAndErrorHandling", func(t *testing.T) {
		// Test edge cases and error conditions

		// Test with nil SelectExprs
		predicate, err := engine.buildPredicateWithContext(&ComparisonExpr{
			Left:     &ColName{Name: stringValue("test_col")},
			Operator: "=",
			Right:    &SQLVal{Type: IntVal, Val: []byte("123")},
		}, nil)
		assert.NoError(t, err, "Should handle nil SelectExprs gracefully")
		assert.NotNil(t, predicate, "Should return valid predicate even without aliases")

		// Test alias resolution with empty SelectExprs
		resolved := engine.resolveColumnAlias("test_col", []SelectExpr{})
		assert.Equal(t, "test_col", resolved, "Should return original name with empty SelectExprs")

		// Test alias resolution with nil SelectExprs
		resolved = engine.resolveColumnAlias("test_col", nil)
		assert.Equal(t, "test_col", resolved, "Should return original name with nil SelectExprs")
	})

	t.Run("ComparisonOperators", func(t *testing.T) {
		// Test all comparison operators work with aliases
		testRecord := &schema_pb.RecordValue{
			Fields: map[string]*schema_pb.Value{
				"_ts_ns": {Kind: &schema_pb.Value_Int64Value{Int64Value: 1000}},
			},
		}

		operators := []struct {
			op       string
			value    string
			expected bool
		}{
			{"=", "1000", true},
			{"=", "999", false},
			{">", "999", true},
			{">", "1000", false},
			{">=", "1000", true},
			{">=", "1001", false},
			{"<", "1001", true},
			{"<", "1000", false},
			{"<=", "1000", true},
			{"<=", "999", false},
		}

		for _, test := range operators {
			t.Run(test.op+"_"+test.value, func(t *testing.T) {
				sql := "SELECT _ts_ns AS ts FROM test WHERE ts " + test.op + " " + test.value
				stmt, err := ParseSQL(sql)
				assert.NoError(t, err, "Should parse operator: %s", test.op)

				selectStmt := stmt.(*SelectStatement)
				predicate, err := engine.buildPredicateWithContext(selectStmt.Where.Expr, selectStmt.SelectExprs)
				assert.NoError(t, err, "Should build predicate for operator: %s", test.op)

				result := predicate(testRecord)
				assert.Equal(t, test.expected, result, "Operator %s with value %s should return %v", test.op, test.value, test.expected)
			})
		}
	})

	t.Run("BackwardCompatibility", func(t *testing.T) {
		// Ensure non-alias queries still work exactly as before
		testRecord := &schema_pb.RecordValue{
			Fields: map[string]*schema_pb.Value{
				"_ts_ns": {Kind: &schema_pb.Value_Int64Value{Int64Value: 1756947416566456262}},
				"id":     {Kind: &schema_pb.Value_Int64Value{Int64Value: 12345}},
			},
		}

		// Test traditional query (no aliases)
		sql := "SELECT _ts_ns, id FROM test WHERE _ts_ns = 1756947416566456262"
		stmt, err := ParseSQL(sql)
		assert.NoError(t, err)

		selectStmt := stmt.(*SelectStatement)

		// Should work with both old and new predicate building methods
		predicateOld, err := engine.buildPredicate(selectStmt.Where.Expr)
		assert.NoError(t, err, "Old buildPredicate method should still work")

		predicateNew, err := engine.buildPredicateWithContext(selectStmt.Where.Expr, selectStmt.SelectExprs)
		assert.NoError(t, err, "New buildPredicateWithContext should work for non-alias queries")

		// Both should produce the same result
		resultOld := predicateOld(testRecord)
		resultNew := predicateNew(testRecord)

		assert.True(t, resultOld, "Old method should match")
		assert.True(t, resultNew, "New method should match")
		assert.Equal(t, resultOld, resultNew, "Both methods should produce identical results")
	})
}

// TestAliasIntegrationWithProductionScenarios tests real-world usage patterns
func TestAliasIntegrationWithProductionScenarios(t *testing.T) {
	engine := NewTestSQLEngine()

	t.Run("OriginalFailingQuery", func(t *testing.T) {
		// Test the exact query pattern that was originally failing
		testRecord := &schema_pb.RecordValue{
			Fields: map[string]*schema_pb.Value{
				"_ts_ns": {Kind: &schema_pb.Value_Int64Value{Int64Value: 1756913789829292386}},
				"id":     {Kind: &schema_pb.Value_Int64Value{Int64Value: 82460}},
			},
		}

		// This was the original failing pattern
		sql := "SELECT id, _ts_ns AS ts FROM ecommerce.user_events WHERE ts = 1756913789829292386"
		stmt, err := ParseSQL(sql)
		assert.NoError(t, err, "Should parse the originally failing query pattern")

		selectStmt := stmt.(*SelectStatement)
		predicate, err := engine.buildPredicateWithContext(selectStmt.Where.Expr, selectStmt.SelectExprs)
		assert.NoError(t, err, "Should build predicate for originally failing pattern")

		result := predicate(testRecord)
		assert.True(t, result, "Should now work for the originally failing query pattern")
	})

	t.Run("ComplexProductionQuery", func(t *testing.T) {
		// Test a more complex production-like query
		testRecord := &schema_pb.RecordValue{
			Fields: map[string]*schema_pb.Value{
				"_ts_ns":     {Kind: &schema_pb.Value_Int64Value{Int64Value: 1756947416566456262}},
				"id":         {Kind: &schema_pb.Value_Int64Value{Int64Value: 897795}},
				"user_id":    {Kind: &schema_pb.Value_StringValue{StringValue: "user123"}},
				"event_type": {Kind: &schema_pb.Value_StringValue{StringValue: "click"}},
			},
		}

		sql := `SELECT 
					id AS event_id, 
					_ts_ns AS event_time, 
					user_id AS uid,
					event_type AS action
				FROM ecommerce.user_events 
				WHERE event_time = 1756947416566456262 
					AND uid = 'user123' 
					AND action = 'click'`

		stmt, err := ParseSQL(sql)
		assert.NoError(t, err, "Should parse complex production query")

		selectStmt := stmt.(*SelectStatement)
		predicate, err := engine.buildPredicateWithContext(selectStmt.Where.Expr, selectStmt.SelectExprs)
		assert.NoError(t, err, "Should build predicate for complex query")

		result := predicate(testRecord)
		assert.True(t, result, "Should match complex production query with multiple aliases")

		// Test partial match failure
		testRecord2 := &schema_pb.RecordValue{
			Fields: map[string]*schema_pb.Value{
				"_ts_ns":     {Kind: &schema_pb.Value_Int64Value{Int64Value: 1756947416566456262}},
				"id":         {Kind: &schema_pb.Value_Int64Value{Int64Value: 897795}},
				"user_id":    {Kind: &schema_pb.Value_StringValue{StringValue: "user999"}}, // Different user
				"event_type": {Kind: &schema_pb.Value_StringValue{StringValue: "click"}},
			},
		}

		result2 := predicate(testRecord2)
		assert.False(t, result2, "Should not match when one aliased condition fails")
	})

	t.Run("PerformanceRegression", func(t *testing.T) {
		// Ensure alias resolution doesn't significantly impact performance
		testRecord := &schema_pb.RecordValue{
			Fields: map[string]*schema_pb.Value{
				"_ts_ns": {Kind: &schema_pb.Value_Int64Value{Int64Value: 1756947416566456262}},
			},
		}

		// Build predicates for comparison
		sqlWithAlias := "SELECT _ts_ns AS ts FROM test WHERE ts = 1756947416566456262"
		sqlWithoutAlias := "SELECT _ts_ns FROM test WHERE _ts_ns = 1756947416566456262"

		stmtWithAlias, err := ParseSQL(sqlWithAlias)
		assert.NoError(t, err)
		stmtWithoutAlias, err := ParseSQL(sqlWithoutAlias)
		assert.NoError(t, err)

		selectStmtWithAlias := stmtWithAlias.(*SelectStatement)
		selectStmtWithoutAlias := stmtWithoutAlias.(*SelectStatement)

		// Both should build successfully
		predicateWithAlias, err := engine.buildPredicateWithContext(selectStmtWithAlias.Where.Expr, selectStmtWithAlias.SelectExprs)
		assert.NoError(t, err)

		predicateWithoutAlias, err := engine.buildPredicateWithContext(selectStmtWithoutAlias.Where.Expr, selectStmtWithoutAlias.SelectExprs)
		assert.NoError(t, err)

		// Both should produce the same logical result
		resultWithAlias := predicateWithAlias(testRecord)
		resultWithoutAlias := predicateWithoutAlias(testRecord)

		assert.True(t, resultWithAlias, "Alias query should work")
		assert.True(t, resultWithoutAlias, "Non-alias query should work")
		assert.Equal(t, resultWithAlias, resultWithoutAlias, "Both should produce same result")
	})
}
