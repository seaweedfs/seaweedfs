package catalog_trino

import (
	"fmt"
	"strings"
	"testing"
)

// TestTrinoCreateDropRecreateTable is a regression test for
// https://github.com/seaweedfs/seaweedfs/issues/9074
//
// Trino CTAS was failing with "Cannot create a table on a non-empty
// location" because Trino's pre-write check saw leftover data files from
// an earlier table at the same S3 path — DROP TABLE removed the catalog
// entry but left the underlying files behind. The Iceberg REST server now
// treats the S3 Tables catalog as the authority on table existence: when a
// DROP succeeds, it purges the storage under the table's location so that
// a subsequent CREATE at the same path finds it empty.
//
// This test pins an explicit location (so the recreated table lands at the
// exact same path, matching what the bug reporter saw), then walks through:
//  1. CREATE TABLE with explicit location
//  2. INSERT rows (produces data files under that location)
//  3. DROP TABLE (catalog entry removed, data files should also be purged)
//  4. CREATE TABLE again at the same explicit location — previously failed
//     with "Cannot create a table on a non-empty location"
//  5. CREATE TABLE AS SELECT on top of the recreated table, which is the
//     exact operation reported in the issue.
func TestTrinoCreateDropRecreateTable(t *testing.T) {
	env := setupTrinoTest(t)
	defer env.Cleanup(t)

	tableBucket := "iceberg-tables"
	schemaName := "recreate_" + randomString(6)
	tableName := "events_" + randomString(6)
	ctasName := "events_ctas_" + randomString(6)
	tableLocation := fmt.Sprintf("s3://%s/%s/%s", tableBucket, schemaName, tableName)
	ctasLocation := fmt.Sprintf("s3://%s/%s/%s", tableBucket, schemaName, ctasName)
	qualified := fmt.Sprintf("iceberg.%s.%s", schemaName, tableName)
	ctasQualified := fmt.Sprintf("iceberg.%s.%s", schemaName, ctasName)

	runTrinoSQL(t, env.trinoContainer, fmt.Sprintf("CREATE SCHEMA IF NOT EXISTS iceberg.%s", schemaName))
	defer runTrinoSQLAllowNamespaceNotEmpty(t, env.trinoContainer, fmt.Sprintf("DROP SCHEMA IF EXISTS iceberg.%s", schemaName))

	createSQL := fmt.Sprintf(`CREATE TABLE %s (
		id INTEGER,
		label VARCHAR
	) WITH (
		format = 'PARQUET',
		location = '%s'
	)`, qualified, tableLocation)

	t.Logf(">>> CREATE #1: %s (location=%s)", qualified, tableLocation)
	runTrinoSQL(t, env.trinoContainer, createSQL)

	t.Logf(">>> INSERT into %s", qualified)
	runTrinoSQL(t, env.trinoContainer, fmt.Sprintf(
		"INSERT INTO %s VALUES (1, 'alpha'), (2, 'beta')", qualified))

	countOutput := runTrinoSQL(t, env.trinoContainer, fmt.Sprintf("SELECT count(*) FROM %s", qualified))
	if got := mustParseCSVInt64(t, countOutput); got != 2 {
		t.Fatalf("after first insert: expected 2 rows, got %d", got)
	}

	t.Logf(">>> DROP %s", qualified)
	runTrinoSQL(t, env.trinoContainer, fmt.Sprintf("DROP TABLE %s", qualified))

	// The recreate must succeed at the exact same location; DROP is expected
	// to have purged the data files so Trino's pre-write check sees an empty
	// path.
	t.Logf(">>> CREATE #2 at the same location: %s", qualified)
	runTrinoSQL(t, env.trinoContainer, createSQL)

	t.Logf(">>> INSERT into recreated %s", qualified)
	runTrinoSQL(t, env.trinoContainer, fmt.Sprintf(
		"INSERT INTO %s VALUES (10, 'gamma')", qualified))

	countOutput = runTrinoSQL(t, env.trinoContainer, fmt.Sprintf("SELECT count(*) FROM %s", qualified))
	if got := mustParseCSVInt64(t, countOutput); got != 1 {
		t.Fatalf("after recreate+insert: expected 1 row (not %d) — recreated table should not see dropped data", got)
	}

	labelOutput := runTrinoSQL(t, env.trinoContainer, fmt.Sprintf("SELECT label FROM %s WHERE id = 10", qualified))
	if !strings.Contains(labelOutput, "gamma") {
		t.Fatalf("recreated table missing expected row; got:\n%s", labelOutput)
	}

	// CTAS on top of the recreated table is the exact operation from #9074.
	ctasSQL := fmt.Sprintf(`CREATE TABLE %s
WITH (
	format = 'PARQUET',
	location = '%s'
)
AS SELECT * FROM %s`, ctasQualified, ctasLocation, qualified)

	t.Logf(">>> CTAS %s FROM %s", ctasQualified, qualified)
	runTrinoSQL(t, env.trinoContainer, ctasSQL)
	defer runTrinoSQL(t, env.trinoContainer, fmt.Sprintf("DROP TABLE IF EXISTS %s", ctasQualified))

	countOutput = runTrinoSQL(t, env.trinoContainer, fmt.Sprintf("SELECT count(*) FROM %s", ctasQualified))
	if got := mustParseCSVInt64(t, countOutput); got != 1 {
		t.Fatalf("CTAS target: expected 1 row, got %d", got)
	}

	// Drop the CTAS target and recreate it at the same path to make sure
	// the drop-cleanup path is exercised for CTAS too.
	t.Logf(">>> DROP then CTAS-recreate %s at the same location", ctasQualified)
	runTrinoSQL(t, env.trinoContainer, fmt.Sprintf("DROP TABLE %s", ctasQualified))
	runTrinoSQL(t, env.trinoContainer, ctasSQL)

	countOutput = runTrinoSQL(t, env.trinoContainer, fmt.Sprintf("SELECT count(*) FROM %s", ctasQualified))
	if got := mustParseCSVInt64(t, countOutput); got != 1 {
		t.Fatalf("CTAS after drop-recreate: expected 1 row, got %d", got)
	}

	runTrinoSQL(t, env.trinoContainer, fmt.Sprintf("DROP TABLE IF EXISTS %s", ctasQualified))
	runTrinoSQL(t, env.trinoContainer, fmt.Sprintf("DROP TABLE IF EXISTS %s", qualified))
}
