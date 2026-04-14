package catalog_trino

import (
	"fmt"
	"strings"
	"testing"
)

// TestTrinoCreateDropRecreateTable is a regression test for
// https://github.com/seaweedfs/seaweedfs/issues/9074
//
// The scenario:
//  1. CREATE TABLE without an explicit location so the Iceberg REST catalog
//     assigns the deterministic <schema>/<table> path.
//  2. INSERT rows (writes data files under that path).
//  3. DROP TABLE (catalog entry removed; stale data files may remain).
//  4. CREATE TABLE again with the same name — used to fail with
//     "Cannot create a table on a non-empty location" because Trino's
//     pre-write check saw the leftover data files. The catalog should now
//     detect the table as absent and purge the stale location before
//     proceeding, so the recreate succeeds and reads cleanly.
//  5. CREATE TABLE AS (CTAS) on top of the recreated table, which is the
//     exact operation reported in the issue.
func TestTrinoCreateDropRecreateTable(t *testing.T) {
	env := setupTrinoTest(t)
	defer env.Cleanup(t)

	schemaName := "recreate_" + randomString(6)
	tableName := "events_" + randomString(6)
	ctasName := "events_ctas_" + randomString(6)
	qualified := fmt.Sprintf("iceberg.%s.%s", schemaName, tableName)
	ctasQualified := fmt.Sprintf("iceberg.%s.%s", schemaName, ctasName)

	runTrinoSQL(t, env.trinoContainer, fmt.Sprintf("CREATE SCHEMA IF NOT EXISTS iceberg.%s", schemaName))
	defer runTrinoSQL(t, env.trinoContainer, fmt.Sprintf("DROP SCHEMA IF EXISTS iceberg.%s", schemaName))

	createSQL := fmt.Sprintf(`CREATE TABLE %s (
		id INTEGER,
		label VARCHAR
	) WITH (format = 'PARQUET')`, qualified)

	t.Logf(">>> CREATE #1: %s", qualified)
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

	// The second CREATE must succeed even though data files from the
	// dropped table may still live at the deterministic location.
	t.Logf(">>> CREATE #2 (same name, no explicit location): %s", qualified)
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
	t.Logf(">>> CTAS %s FROM %s", ctasQualified, qualified)
	runTrinoSQL(t, env.trinoContainer, fmt.Sprintf(
		"CREATE TABLE %s AS SELECT * FROM %s", ctasQualified, qualified))
	defer runTrinoSQL(t, env.trinoContainer, fmt.Sprintf("DROP TABLE IF EXISTS %s", ctasQualified))

	countOutput = runTrinoSQL(t, env.trinoContainer, fmt.Sprintf("SELECT count(*) FROM %s", ctasQualified))
	if got := mustParseCSVInt64(t, countOutput); got != 1 {
		t.Fatalf("CTAS target: expected 1 row, got %d", got)
	}

	t.Logf(">>> DROP then CTAS-recreate %s to exercise the CTAS-on-non-empty-location path", ctasQualified)
	runTrinoSQL(t, env.trinoContainer, fmt.Sprintf("DROP TABLE %s", ctasQualified))
	runTrinoSQL(t, env.trinoContainer, fmt.Sprintf(
		"CREATE TABLE %s AS SELECT * FROM %s", ctasQualified, qualified))

	countOutput = runTrinoSQL(t, env.trinoContainer, fmt.Sprintf("SELECT count(*) FROM %s", ctasQualified))
	if got := mustParseCSVInt64(t, countOutput); got != 1 {
		t.Fatalf("CTAS after drop-recreate: expected 1 row, got %d", got)
	}

	runTrinoSQL(t, env.trinoContainer, fmt.Sprintf("DROP TABLE %s", qualified))
}
